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

package ledger

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Deferring RATIFY past the boundary commit must not change any governance
// outcome: the ratified, enacted, expired and dropped marks, the treasury,
// reserves and refunded deposits, the committee, the protocol parameters,
// the mark snapshot and DRep power all match RATIFY run inside the boundary
// transaction, boundary for boundary. DINGO_GOV_DIFF_OUT writes the deferred
// run's dumps for comparison against another tree.
func TestDeferredRatificationMatchesBoundaryRatification(t *testing.T) {
	t.Parallel()

	const boundaries = 4
	atBoundary := newGovDiffScenario(t)
	atBoundary.ls.ratifyAtBoundary = true
	want := atBoundary.run(t, boundaries, func(*LedgerState) {})

	deferred := newGovDiffScenario(t)
	got := deferred.run(t, boundaries, func(ls *LedgerState) {
		require.NoError(t, ls.WaitGovernanceRatification(t.Context()))
	})
	require.Equal(t, want, got)
	if out := os.Getenv("DINGO_GOV_DIFF_OUT"); out != "" {
		raw, err := json.MarshalIndent(got, "", " ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(out, raw, 0o600))
	}

	// The scenario must reach every outcome it claims to compare.
	last := got[len(got)-1]
	enacted := 0
	for _, p := range last.Proposals {
		if p.EnactedIn != nil {
			enacted++
		}
	}
	require.Equal(t, 3, enacted, "committee, parameter and treasury actions")
	require.Len(t, last.Committee, 2)
	require.NotEqual(t, got[0].PParams, last.PParams)
	require.NotEqual(t, got[0].Treasury, last.Treasury)
	require.NotZero(t, last.DRepPower)
	require.NotEmpty(t, last.Mark)

}
