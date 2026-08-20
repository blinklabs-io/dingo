// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBuildStatusSummaryChecksEpochZero guards against CheckedMin using 0 as
// an "unset" sentinel: epoch 0 is a legitimate checked epoch (pre-staking
// epochs 0/1 are always in scope by default), so it must not be mistaken for
// "no minimum recorded yet" and overwritten by the next epoch checked.
func TestBuildStatusSummaryChecksEpochZero(t *testing.T) {
	statuses := []CheckEpochStatus{
		{Epoch: 0, Status: StatusPass},
		{Epoch: 1, Status: StatusPass},
		{Epoch: 2, Status: StatusPass},
	}
	summary := BuildStatusSummary("preview", nil, statuses)
	require.Equal(t, uint64(0), summary.CheckedMin,
		"epoch 0 is a real checked epoch and must remain the reported minimum")
	require.Equal(t, uint64(2), summary.CheckedMax)
}

func TestReportsMakeCoverageScopeExplicit(t *testing.T) {
	report, err := BuildJSONReport("preview", "2026-08-17", nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, KoiosCoverageMatrix(), report.Coverage)
	require.Contains(t, report.Coverage, KoiosFieldCoverage{
		Endpoint: "/totals",
		Field:    "reward",
		Class:    CoverageIntentionallyIncomparable,
		Reason:   "Koios exposes a lagged cumulative accumulator; Dingo stores a per-epoch reward flow",
	})

	var output bytes.Buffer
	PrintStatus(&output, StatusSummary{Network: "preview"}, false, nil)
	require.Contains(t, output.String(), "intentionally incomparable")
	require.Contains(t, output.String(), "unsupported")

	output.Reset()
	require.NoError(t, WriteJSONReport(&output, report))
	require.True(t, strings.Contains(output.String(), `"coverage"`))
	require.True(
		t,
		strings.Contains(output.String(), `"intentionally-incomparable"`),
	)
}
