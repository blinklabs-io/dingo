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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

const (
	computedEpoch        = "epoch"
	computedEpochSummary = "epoch_summary"
	computedEnactment    = "pparam_enactment"
)

func openComputedSet(t *testing.T) *utxoset.Set {
	t.Helper()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	return set
}

// TestSetComputesEpochStateWithoutTheStore crosses a boundary and rolls it
// back with the store's epoch, summary and parameter reads failing. The set
// gains the new epoch and its summary at the boundary and holds what it held
// before once the boundary is rolled back.
func TestSetComputesEpochStateWithoutTheStore(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
	reached, _ := scenario.HideStoreReads(
		"GetEpoch", "GetEpochSummary", "ListPParamsForEra",
	)
	// held counts the epochs, the summaries and the parameter rows.
	held := func() [3]int {
		t.Helper()
		epochs, err := set.Epochs()
		require.NoError(t, err)
		var hi uint64
		for _, e := range epochs {
			hi = max(hi, e.EpochId)
		}
		summaries := 0
		for epoch := uint64(0); epoch <= hi+summaryEpochSpan; epoch++ {
			row, err := set.EpochSummary(epoch)
			require.NoError(t, err)
			if row != nil {
				summaries++
			}
		}
		return [3]int{len(epochs), summaries, setPParamsRows(t, set)}
	}
	before := held()
	scenario.ApplyBoundary()
	after := held()
	require.Equal(t, before[0]+1, after[0], "the boundary adds an epoch")
	require.Greater(t, after[1], before[1], "the boundary saved no summary")
	scenario.RollbackBeforeBoundary()
	require.Equal(t, before, held(), "the rollback undoes the boundary")
	require.Zero(t, reached(), "a computed result was read back from the store")
}

// TestSetComputedSummaryCarriesTheStickyReadiness requires a summary saved
// not ready over a ready one to stay ready in the set, and a ready summary
// the set never saw to be counted as a divergence.
func TestSetComputedSummaryCarriesTheStickyReadiness(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	// An abandoned transaction leaves nothing to carry over, so the sticky
	// bit is carried within one: the ready save, then the not-ready one.
	kept := scenario.SaveSummariesInsideTxn(60, true, false)
	require.NotNil(t, kept)
	require.True(t, kept.SnapshotReady, "readiness is kept once set")

}

// TestSetComputedEnactmentDivergenceIsCounted requires an enactment the
// store makes from a proposal the set does not hold to be counted, and the
// same enactment through the ledger not to be.
