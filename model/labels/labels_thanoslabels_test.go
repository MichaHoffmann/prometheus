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

//go:build thanoslabels

package labels

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// expectedSizeOfLabels and expectedByteSize are the same as slicelabels
// because thanoslabels uses a similar pointer-based slice structure.
var expectedSizeOfLabels = []uint64{ // Values must line up with testCaseLabels.
	72,
	0,
	97,
	326,
	327,
	549,
}

var expectedByteSize = expectedSizeOfLabels // They are identical

func TestScratchBuilderAdd_Strings(t *testing.T) {
	t.Run("safe", func(t *testing.T) {
		n := []byte("__name__")
		v := []byte("metric1")

		l := NewScratchBuilder(0)
		l.Add(unsafeString(n), unsafeString(v))
		ret := l.Labels()

		// For thanoslabels, in default mode strings are reused, so modifying the
		// input will cause `ret` labels to change too.
		n[1] = byte('?')
		v[2] = byte('?')

		require.Empty(t, ret.Get("__name__"))
		require.Equal(t, "me?ric1", ret.Get("_?name__"))
	})
	t.Run("unsafe", func(t *testing.T) {
		n := []byte("__name__")
		v := []byte("metric1")

		l := NewScratchBuilder(0)
		l.SetUnsafeAdd(true)
		l.Add(unsafeString(n), unsafeString(v))
		ret := l.Labels()

		// Changing input strings should be now safe, because we marked adds as unsafe.
		n[1] = byte('?')
		v[2] = byte('?')

		require.Equal(t, "metric1", ret.Get("__name__"))
	})
}

func TestLabelpbLabelConversion(t *testing.T) {
	// Test that we can convert to/from labelpb.Label
	ls := FromStrings("__name__", "test_metric", "job", "prometheus")

	// Get the labelpb labels
	labelpbLabels := ls.ToLabelpbLabels()
	require.Len(t, labelpbLabels, 2)
	require.Equal(t, "__name__", labelpbLabels[0].Name)
	require.Equal(t, "test_metric", labelpbLabels[0].Value)
	require.Equal(t, "job", labelpbLabels[1].Name)
	require.Equal(t, "prometheus", labelpbLabels[1].Value)

	// Convert back
	ls2 := FromLabelpbLabels(labelpbLabels)
	require.True(t, Equal(ls, ls2))
}

func unsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}
