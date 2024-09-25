// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package promql_test

import (
	"testing"

	"github.com/prometheus/prometheus/promql/promqltest"
)

// The "info" function is experimental. This is why we write those tests here for now instead of promqltest/testdata/info.test
func TestInfo(t *testing.T) {
	engine := promqltest.NewTestEngine(t, false, 0, promqltest.DefaultMaxSamplesPerQuery)
	promqltest.RunTest(t, `
load 5m
  metric{instance="a", job="1", label="value"} 0 1 2
  metric_not_matching_target_info{instance="a", job="2", label="value"} 0 1 2
  metric_with_overlapping_label{instance="a", job="1", label="value", data="base"} 0 1 2
  target_info{instance="a", job="1", data="info", another_data="another info"} 1 1 1
  build_info{instance="a", job="1", build_data="build"} 1 1 1

eval range from 0m to 10m step 5m info(metric, {data=~".+"})
  metric{data="info", instance="a", job="1", label="value"} 0 1 2

eval range from 0m to 10m step 5m info(metric)
  metric{data="info", instance="a", job="1", label="value", another_data="another info"} 0 1 2

eval range from 0m to 10m step 5m info(metric_not_matching_target_info)
  metric_not_matching_target_info{instance="a", job="2", label="value"} 0 1 2

eval range from 0m to 10m step 5m info(metric, {non_existent=~".+"})

eval range from 0m to 10m step 5m info(metric, {data=~".+", non_existent=~".*"})
  metric{data="info", instance="a", job="1", label="value"} 0 1 2

eval range from 0m to 10m step 5m info(metric_with_overlapping_label)
  metric_with_overlapping_label{data="base", instance="a", job="1", label="value", another_data="another info"} 0 1 2

eval range from 0m to 10m step 5m info(metric, {__name__="target_info"})
  metric{data="info", instance="a", job="1", label="value", another_data="another info"} 0 1 2

eval range from 0m to 10m step 5m info(metric, {__name__="non_existent"})
  metric{instance="a", job="1", label="value"} 0 1 2

eval range from 0m to 10m step 5m info(metric, {__name__="non_existent", data=~".+"})

eval range from 0m to 10m step 5m info(metric, {__name__="build_info"})
  metric{instance="a", job="1", label="value", build_data="build"} 0 1 2

eval range from 0m to 10m step 5m info(metric, {__name__=~".+_info"})
  metric{instance="a", job="1", label="value", build_data="build", data="info", another_data="another info"} 0 1 2

eval range from 0m to 10m step 5m info(build_info, {__name__=~".+_info", build_data=~".+"})
  build_info{instance="a", job="1", build_data="build"} 1 1 1

clear

load 5m
  metric{instance="a", job="1", label="value"} 0 1 2
  target_info{instance="a", job="1", data="info", another_data="another info"} 1 1 _
  target_info{instance="a", job="1", data="updated info", another_data="another info"} _ _ 1

eval range from 0m to 10m step 5m info(metric)
  metric{data="info", instance="a", job="1", label="value", another_data="another info"} 0 1 _
  metric{data="updated info", instance="a", job="1", label="value", another_data="another info"} _ _ 2

clear

load 5m
  metric{instance="a", job="1", label="value"} 0 1 2
  target_info{instance="a", job="1", data="info"} 1 1 stale
  target_info{instance="a", job="1", data="updated info"} _ _ 1

eval range from 0m to 10m step 5m info(metric)
  metric{data="info", instance="a", job="1", label="value"} 0 1 _
  metric{data="updated info", instance="a", job="1", label="value"} _ _ 2
`, engine)
}
