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
	"fmt"
	"math"
	"os"
	"path/filepath"
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

// Not t.Parallel: t.Setenv makes this test process-global.
func TestLoadConfigAnalysisDurations(t *testing.T) {
	clearAnalysisEnv(t)
	t.Setenv("ANALYSIS_INITIAL_WAIT", "0")
	t.Setenv("ANALYSIS_CHECK_INTERVAL", "1")

	cfg, err := LoadConfig()
	require.NoError(t, err)
	require.Zero(t, cfg.InitialWait)
	require.Equal(t, time.Second, cfg.CheckInterval)
}

// clearAnalysisEnv isolates a LoadConfig test from ANALYSIS_* values in the
// caller's environment; LoadConfig fails on any one it cannot parse.
func clearAnalysisEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"ANALYSIS_LOG_DIR",
		"ANALYSIS_GENESIS_FILE",
		"ANALYSIS_INITIAL_WAIT",
		"ANALYSIS_CHECK_INTERVAL",
		"ANALYSIS_MAX_FORK_DEPTH",
		"ANALYSIS_POOLS",
		"ANALYSIS_MIN_BLOCKS_SAMPLE",
	} {
		t.Setenv(key, "")
	}
}

// The genesis security parameter k counts blocks, while MaxForkDepth bounds a
// slot distance; at active slot coefficient f a k-block window spans k/f slots,
// rounded up. 21/0.7 is exactly 30, but float64 division gives
// 30.000000000000004.
// Not t.Parallel: t.Setenv makes this test process-global.
func TestLoadConfigForkDepthIsGenesisKInSlots(t *testing.T) {
	for _, tc := range []struct {
		k    int
		f    string
		want int
	}{
		{k: 40, f: "0.4", want: 100},
		{k: 21, f: "0.7", want: 30},
		{k: 10, f: "0.3", want: 34},
	} {
		t.Run(fmt.Sprintf("k=%d,f=%s", tc.k, tc.f), func(t *testing.T) {
			genesisFile := filepath.Join(t.TempDir(), "testnet.yaml")
			require.NoError(t, os.WriteFile(genesisFile, fmt.Appendf(nil, `---
poolCount: 5
---
protocolConsts:
  k: %d
---
epochLength: 500
slotLength: 1
activeSlotsCoeff: %s
securityParam: %d
`, tc.k, tc.f, tc.k), 0o600))
			clearAnalysisEnv(t)
			t.Setenv("ANALYSIS_GENESIS_FILE", genesisFile)

			cfg, err := LoadConfig()
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.MaxForkDepth)
		})
	}
}
