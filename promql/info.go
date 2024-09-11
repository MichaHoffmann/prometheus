// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/grafana/regexp"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
)

const targetInfo = "target_info"

// identifyingLabels are the labels we consider as identifying for info metrics.
// Currently hard coded, so we don't need knowledge of individual info metrics.
var identifyingLabels = []string{"instance", "job"}

// evalInfo implements the info PromQL function.
func (ev *evaluator) evalInfo(ctx context.Context, args parser.Expressions) (parser.Value, annotations.Annotations) {
	val, annots := ev.eval(ctx, args[0])
	mat := val.(Matrix)
	// Map from data label name to matchers.
	dataLabelMatchers := map[string][]*labels.Matcher{}
	var infoNameMatchers []*labels.Matcher
	if len(args) > 1 {
		// TODO: Introduce a dedicated LabelSelector type.
		labelSelector := args[1].(*parser.VectorSelector)
		for _, m := range labelSelector.LabelMatchers {
			dataLabelMatchers[m.Name] = append(dataLabelMatchers[m.Name], m)
			if m.Name == labels.MetricName {
				infoNameMatchers = append(infoNameMatchers, m)
			}
		}
	} else {
		infoNameMatchers = []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, targetInfo)}
	}

	// Don't try to enrich info series.
	ignoreSeries := map[int]struct{}{}
loop:
	for i, s := range mat {
		lblMap := s.Metric.Map()
		name := lblMap[labels.MetricName]
		for _, m := range infoNameMatchers {
			if m.Matches(name) {
				ignoreSeries[i] = struct{}{}
				continue loop
			}
		}
	}

	infoSeries, ws, err := ev.fetchInfoSeries(ctx, mat, ignoreSeries, dataLabelMatchers)
	annots.Merge(ws)
	if err != nil {
		annots.Add(err)
		return nil, annots
	}

	res, ws := ev.combineWithInfoSeries(ctx, mat, infoSeries, ignoreSeries, dataLabelMatchers)
	annots.Merge(ws)
	return res, annots
}

// fetchInfoSeries fetches info series given ev.selectHints and matching identifying labels in mat.
// Series in ignoreSeries are not fetched.
func (ev *evaluator) fetchInfoSeries(ctx context.Context, mat Matrix, ignoreSeries map[int]struct{}, dataLabelMatchers map[string][]*labels.Matcher) (Matrix, annotations.Annotations, error) {
	if ev.selectHints == nil {
		// ev.selectHints should have been set.
		var annots annotations.Annotations
		annots.Add(fmt.Errorf("ev.selectHints not set"))
		return nil, annots, nil
	}

	// A map of values for all identifying labels we are interested in.
	idLblValues := map[string]map[string]struct{}{}
	for i, s := range mat {
		if _, exists := ignoreSeries[i]; exists {
			continue
		}

		// Register relevant values per identifying label for this series.
		lblMap := s.Metric.Map()
		for _, l := range identifyingLabels {
			val := lblMap[l]
			if val == "" {
				continue
			}

			if idLblValues[l] == nil {
				idLblValues[l] = map[string]struct{}{}
			}
			idLblValues[l][val] = struct{}{}
		}
	}
	if len(idLblValues) == 0 {
		return nil, nil, nil
	}

	// Generate regexps for every interesting value per identifying label.
	var sb strings.Builder
	idLblRegexps := make(map[string]string, len(idLblValues))
	for name, vals := range idLblValues {
		sb.Reset()
		i := 0
		for v := range vals {
			if i > 0 {
				sb.WriteRune('|')
			}
			sb.WriteString(regexp.QuoteMeta(v))
			i++
		}
		idLblRegexps[name] = sb.String()
	}

	var infoLabelMatchers []*labels.Matcher
	for name, re := range idLblRegexps {
		infoLabelMatchers = append(infoLabelMatchers, labels.MustNewMatcher(labels.MatchRegexp, name, re))
	}
	var nameMatcher *labels.Matcher
	for name, ms := range dataLabelMatchers {
		for i, m := range ms {
			if m.Name == labels.MetricName {
				nameMatcher = m
				ms = slices.Delete(ms, i, i+1)
			}
			infoLabelMatchers = append(infoLabelMatchers, m)
		}
		if len(ms) > 0 {
			dataLabelMatchers[name] = ms
		} else {
			delete(dataLabelMatchers, name)
		}
	}
	if nameMatcher == nil {
		// Default to using the target_info metric.
		infoLabelMatchers = append([]*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, labels.MetricName, targetInfo)}, infoLabelMatchers...)
	}

	infoIt := ev.querier.Select(ctx, false, ev.selectHints, infoLabelMatchers...)
	annots := infoIt.Warnings()
	if infoIt.Err() != nil {
		return nil, annots, infoIt.Err()
	}
	var infoSeries []storage.Series
	for infoIt.Next() {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		infoSeries = append(infoSeries, infoIt.At())
	}
	infoMat := ev.expandSeriesToMatrix(ctx, infoSeries, 0, true)
	return infoMat, annots, infoIt.Err()
}

// combineWithInfoSeries combines mat with select data labels from infoMat.
func (ev *evaluator) combineWithInfoSeries(ctx context.Context, mat, infoMat Matrix, ignoreSeries map[int]struct{}, dataLabelMatchers map[string][]*labels.Matcher) (Matrix, annotations.Annotations) {
	buf := make([]byte, 0, 1024)
	lb := labels.NewScratchBuilder(0)
	sigFunction := func(name string) func(labels.Labels) string {
		return func(lset labels.Labels) string {
			lb.Reset()
			lb.Add(labels.MetricName, name)
			lset.MatchLabels(true, identifyingLabels...).Range(func(l labels.Label) {
				lb.Add(l.Name, l.Value)
			})
			lb.Sort()
			return string(lb.Labels().Bytes(buf))
		}
	}

	infoMetrics := map[string]struct{}{}
	for _, is := range infoMat {
		lblMap := is.Metric.Map()
		infoMetrics[lblMap[labels.MetricName]] = struct{}{}
	}
	sigfs := make(map[string]func(labels.Labels) string, len(infoMetrics))
	for name := range infoMetrics {
		sigfs[name] = sigFunction(name)
	}

	// Keep a copy of the original point slices so they can be returned to the pool.
	origMatrices := []Matrix{
		make(Matrix, len(mat)),
		make(Matrix, len(infoMat)),
	}
	copy(origMatrices[0], mat)
	copy(origMatrices[1], infoMat)

	numSteps := int((ev.endTimestamp-ev.startTimestamp)/ev.interval) + 1
	originalNumSamples := ev.currentSamples

	// Create an output vector that is as big as the input matrix with
	// the most time series.
	biggestLen := max(len(mat), len(infoMat))
	baseVector := make(Vector, 0, len(mat))
	infoVector := make(Vector, 0, len(infoMat))
	enh := &EvalNodeHelper{
		Out:          make(Vector, 0, biggestLen),
		labelBuilder: &lb,
	}
	type seriesAndTimestamp struct {
		Series
		ts int64
	}
	seriess := make(map[uint64]seriesAndTimestamp, biggestLen) // Output series by series hash.
	tempNumSamples := ev.currentSamples

	// For every base series, compute signature per info metric.
	baseSigs := make([]map[string]string, 0, len(mat))
	for _, s := range mat {
		sigs := make(map[string]string, len(infoMetrics))
		for infoName := range infoMetrics {
			sigs[infoName] = sigfs[infoName](s.Metric)
		}
		baseSigs = append(baseSigs, sigs)
	}

	infoSigs := make([]string, 0, len(infoMat))
	for _, s := range infoMat {
		name := s.Metric.Map()[labels.MetricName]
		infoSigs = append(infoSigs, sigfs[name](s.Metric))
	}

	var warnings annotations.Annotations
	for ts := ev.startTimestamp; ts <= ev.endTimestamp; ts += ev.interval {
		if err := contextDone(ctx, "expression evaluation"); err != nil {
			ev.error(err)
		}

		// Reset number of samples in memory after each timestamp.
		ev.currentSamples = tempNumSamples
		// Gather input vectors for this timestamp.
		baseVector = ev.gatherVector(ts, mat, baseVector)
		infoVector = ev.gatherVector(ts, infoMat, infoVector)

		enh.Ts = ts
		result, err := ev.combineWithInfoVector(baseVector, infoVector, ignoreSeries, baseSigs, infoSigs, enh, dataLabelMatchers)
		if err != nil {
			warnings.Add(err)
		}
		enh.Out = result[:0] // Reuse result vector.

		vecNumSamples := result.TotalSamples()
		ev.currentSamples += vecNumSamples
		// When we reset currentSamples to tempNumSamples during the next iteration of the loop it also
		// needs to include the samples from the result here, as they're still in memory.
		tempNumSamples += vecNumSamples
		ev.samplesStats.UpdatePeak(ev.currentSamples)
		if ev.currentSamples > ev.maxSamples {
			ev.error(ErrTooManySamples(env))
		}

		// Add samples in result vector to output series.
		for _, sample := range result {
			h := sample.Metric.Hash()
			ss, exists := seriess[h]
			if exists {
				if ss.ts == ts { // If we've seen this output series before at this timestamp, it's a duplicate.
					ev.errorf("vector cannot contain metrics with the same labelset")
				}
				ss.ts = ts
			} else {
				ss = seriesAndTimestamp{Series{Metric: sample.Metric}, ts}
			}
			addToSeries(&ss.Series, enh.Ts, sample.F, sample.H, numSteps)
			seriess[h] = ss
		}
	}

	// Reuse the original point slices.
	for _, m := range origMatrices {
		for _, s := range m {
			putFPointSlice(s.Floats)
			putHPointSlice(s.Histograms)
		}
	}
	// Assemble the output matrix. By the time we get here we know we don't have too many samples.
	numSamples := 0
	output := make(Matrix, 0, len(seriess))
	for _, ss := range seriess {
		numSamples += len(ss.Floats) + totalHPointSize(ss.Histograms)
		output = append(output, ss.Series)
	}
	ev.currentSamples = originalNumSamples + numSamples
	ev.samplesStats.UpdatePeak(ev.currentSamples)
	return output, warnings
}

func (ev *evaluator) gatherVector(ts int64, input Matrix, output Vector) Vector {
	output = output[:0]
	for i, series := range input {
		switch {
		case len(series.Floats) > 0 && series.Floats[0].T == ts:
			output = append(output, Sample{Metric: series.Metric, F: series.Floats[0].F, T: ts, OrigT: series.Floats[0].OrigT})
			// Move input vectors forward so we don't have to re-scan the same
			// past points at the next step.
			input[i].Floats = series.Floats[1:]
		case len(series.Histograms) > 0 && series.Histograms[0].T == ts:
			output = append(output, Sample{Metric: series.Metric, H: series.Histograms[0].H, T: ts, OrigT: series.Histograms[0].OrigT})
			input[i].Histograms = series.Histograms[1:]
		default:
			continue
		}

		// Don't add histogram size here because we only
		// copy the pointer above, not the whole
		// histogram.
		ev.currentSamples++
		if ev.currentSamples > ev.maxSamples {
			ev.error(ErrTooManySamples(env))
		}
	}
	ev.samplesStats.UpdatePeak(ev.currentSamples)

	return output
}

// combineWithInfoVector combines base and info Vectors.
// Base series in ignoreSeries are not combined.
func (ev *evaluator) combineWithInfoVector(base, info Vector, ignoreSeries map[int]struct{}, baseSigs []map[string]string, infoSigs []string, enh *EvalNodeHelper, dataLabelMatchers map[string][]*labels.Matcher) (Vector, error) {
	if len(base) == 0 {
		return nil, nil // Short-circuit: nothing is going to match.
	}

	// All samples from the info Vectors hashed by the matching label/values.
	if enh.infoSamplesBySig == nil {
		enh.infoSamplesBySig = make(map[string]Sample, len(enh.Out))
	} else {
		clear(enh.infoSamplesBySig)
	}

	for i, s := range info {
		sig := infoSigs[i]
		if existing, exists := enh.infoSamplesBySig[sig]; exists {
			switch {
			case existing.OrigT > s.OrigT:
				// Keep the other info sample, since it's newer.
			case existing.OrigT < s.OrigT:
				// Keep this info sample, since it's newer.
				enh.infoSamplesBySig[sig] = s
			default:
				// The two info samples have the same timestamp - conflict.
				name := s.Metric.Map()[labels.MetricName]
				ev.errorf("found duplicate series for info metric %s", name)
			}
		} else {
			enh.infoSamplesBySig[sig] = s
		}
	}

	lb := enh.labelBuilder
	for i, bs := range base {
		if _, exists := ignoreSeries[i]; exists {
			// This series should not be enriched with info metric data labels.
			enh.Out = append(enh.Out, Sample{
				Metric: bs.Metric,
				F:      bs.F,
				H:      bs.H,
			})
			continue
		}

		baseLabels := bs.Metric.Map()
		infoLblMap := map[string]string{}

		// For every info metric name, try to find an info series with the same signature.
		seenInfoMetrics := map[string]struct{}{}
		for infoName, sig := range baseSigs[i] {
			is, exists := enh.infoSamplesBySig[sig]
			if !exists {
				continue
			}
			if _, exists := seenInfoMetrics[infoName]; exists {
				continue
			}

			var err error
			is.Metric.Range(func(l labels.Label) {
				if err != nil {
					return
				}
				if l.Name == labels.MetricName {
					return
				}
				if _, exists := dataLabelMatchers[l.Name]; len(dataLabelMatchers) > 0 && !exists {
					// Not among the specified data label matchers.
					return
				}

				if v, exists := infoLblMap[l.Name]; exists && v != l.Value {
					err = fmt.Errorf("conflicting label: %s", l.Name)
					return
				}
				if _, exists := baseLabels[l.Name]; exists {
					// Skip labels already on the base metric.
					return
				}

				infoLblMap[l.Name] = l.Value
			})
			if err != nil {
				return nil, err
			}
			seenInfoMetrics[infoName] = struct{}{}
		}

		lb.Reset()
		for n, v := range infoLblMap {
			lb.Add(n, v)
		}
		lb.Sort()
		infoLbls := lb.Labels()

		if infoLbls.Len() == 0 {
			// If there's at least one data label matcher not matching the empty string,
			// we have to ignore this series as there are no matching info series.
			allMatchersMatchEmpty := true
			for _, ms := range dataLabelMatchers {
				for _, m := range ms {
					if !m.Matches("") {
						allMatchersMatchEmpty = false
						break
					}
				}
			}
			if !allMatchersMatchEmpty {
				continue
			}
		}

		lb.Reset()
		bs.Metric.Range(func(l labels.Label) {
			lb.Add(l.Name, l.Value)
		})
		infoLbls.Range(func(l labels.Label) {
			lb.Add(l.Name, l.Value)
		})
		lb.Sort()

		enh.Out = append(enh.Out, Sample{
			Metric: lb.Labels(),
			F:      bs.F,
			H:      bs.H,
		})
	}
	return enh.Out, nil
}
