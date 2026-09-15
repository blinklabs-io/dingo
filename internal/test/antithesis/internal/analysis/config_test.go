// Copyright 2026 Blink Labs Software
//
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

package analysis

import (
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseAnalysisDurationBounds(t *testing.T) {
	t.Parallel()

	max := strconv.FormatInt(maxAnalysisDurationSeconds, 10)
	got, err := parseAnalysisDuration("ANALYSIS_INITIAL_WAIT", max, true)
	require.NoError(t, err)
	require.Equal(t, time.Duration(maxAnalysisDurationSeconds)*time.Second, got)

	for _, tc := range []struct {
		name      string
		value     string
		allowZero bool
	}{
		{name: "negative", value: "-1", allowZero: true},
		{name: "zero interval", value: "0", allowZero: false},
		{name: "overflow", value: strconv.FormatInt(math.MaxInt64, 10), allowZero: true},
		{name: "too large", value: strconv.FormatInt(maxAnalysisDurationSeconds+1, 10), allowZero: true},
		{name: "malformed", value: "not-a-duration", allowZero: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseAnalysisDuration(
				"ANALYSIS_CHECK_INTERVAL", tc.value, tc.allowZero,
			)
			require.Error(t, err)
		})
	}

	got, err = parseAnalysisDuration("ANALYSIS_INITIAL_WAIT", "0", true)
	require.NoError(t, err)
	require.Zero(t, got)
}

func TestLoadConfigAnalysisDurations(t *testing.T) {
	t.Setenv("ANALYSIS_INITIAL_WAIT", "0")
	t.Setenv("ANALYSIS_CHECK_INTERVAL", "1")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Zero(t, cfg.InitialWait)
	require.Equal(t, time.Second, cfg.CheckInterval)
}
