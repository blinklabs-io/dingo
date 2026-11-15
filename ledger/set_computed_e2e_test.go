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
	"github.com/blinklabs-io/dingo/database/models"
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

// requireComputed requires result to have been computed and compared in
// verify mode, with no difference from the metadata store.
func requireComputed(
	t *testing.T,
	counts func(string) (float64, float64),
	result string,
) {
	t.Helper()
	verified, mismatches := counts(result)
	require.Positive(t, verified, "no %s result was compared", result)
	require.Zero(t, mismatches, "%s results differ from the store", result)
}

// TestSetComputedEpochRowsEqualTheStores requires the epoch and summary rows
// the set computes at a boundary to be the rows the metadata store wrote,
// and to survive a rollback the same way, with every result compared.
func TestSetComputedEpochRowsEqualTheStores(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsVerify,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	match := func() int {
		t.Helper()
		txn := store.Begin(t.Context(), false)
		defer txn.Release()
		requireEpochStateMatches(t, set, store, txn)
		return requireSummariesMatch(t, set, store, txn)
	}
	scenario.ApplyBoundary()
	require.Positive(t, match(), "the boundary saved no summary")
	scenario.RollbackBeforeBoundary()
	match()
	scenario.ApplyBoundary()
	require.Positive(t, match(), "the second boundary saved no summary")
	requireComputed(t, scenario.ComputedResults, computedEpoch)
	requireComputed(t, scenario.ComputedResults, computedEpochSummary)
}

// TestSetComputedEpochRowsAcrossAMidEpochEraSwitch requires the epoch rows
// the set computes through a mid-epoch era switch and its rollback to be the
// store's.
func TestSetComputedEpochRowsAcrossAMidEpochEraSwitch(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewMidEpochEraSwitchScenarioForTest(
		t, set, ledger.UtxoSetReadsVerify,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	match := func() {
		t.Helper()
		txn := store.Begin(t.Context(), false)
		defer txn.Release()
		requireEpochStateMatches(t, set, store, txn)
		require.Positive(t, requirePParamsMatch(t, set, store, txn))
	}
	scenario.ApplySwitch()
	match()
	scenario.RollbackBeforeSwitch()
	match()
	requireComputed(t, scenario.ComputedResults, computedEpoch)
}

// TestSetComputedEnactmentEqualsTheStores requires the protocol parameter
// rows the set enacts from its own proposals at a boundary to be the rows the
// metadata store wrote, and the rollback to remove them.
func TestSetComputedEnactmentEqualsTheStores(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set, ledger.UtxoSetReadsVerify,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	match := func() int {
		t.Helper()
		txn := store.Begin(t.Context(), false)
		defer txn.Release()
		return requirePParamsMatch(t, set, store, txn)
	}
	before := match()
	scenario.ApplyBoundary()
	// The enacted Babbage row and the translated Conway row.
	require.Equal(t, before+2, match())
	scenario.RollbackBeforeBoundary()
	require.Equal(t, before, match())
	requireComputed(t, scenario.ComputedResults, computedEnactment)
}

// TestSetComputesEpochStateWithoutTheStore requires a boundary and its
// rollback, in set mode, to give the set the rows the store holds while the
// store's epoch, summary and parameter reads fail.
func TestSetComputesEpochStateWithoutTheStore(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
	scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
	reached, _ := scenario.HideStoreReads(
		"GetEpoch", "GetEpochSummary", "ListPParamsForEra",
	)
	match := func() int {
		t.Helper()
		txn := store.Begin(t.Context(), false)
		defer txn.Release()
		requireEpochStateMatches(t, set, store, txn)
		requirePParamsMatch(t, set, store, txn)
		return requireSummariesMatch(t, set, store, txn)
	}
	scenario.ApplyBoundary()
	require.Positive(t, match(), "the boundary saved no summary")
	scenario.RollbackBeforeBoundary()
	match()
	require.Zero(t, reached(), "a computed result was read back from the store")
}

// TestSetEnactsWithoutTheStore requires the boundary that enacts a classic
// update, in set mode, to stage the store's rows while the store's parameter
// and proposal reads fail.
func TestSetEnactsWithoutTheStore(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	reached := scenario.HideStoreReads(
		"GetEpoch", "ListPParamsForEra", "GetPParamUpdates",
	)
	match := func() int {
		t.Helper()
		txn := store.Begin(t.Context(), false)
		defer txn.Release()
		return requirePParamsMatch(t, set, store, txn)
	}
	before := match()
	scenario.ApplyBoundary()
	require.Equal(t, before+2, match())
	scenario.RollbackBeforeBoundary()
	require.Equal(t, before, match())
	require.Zero(t, reached(), "the enactment was read back from the store")
}

// TestSetComputedEpochDivergenceIsCounted requires an epoch row the store
// keeps differently from the set's to be counted in verify mode.
func TestSetComputedEpochDivergenceIsCounted(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsVerify,
	)
	require.NoError(t, set.LoadLedgerState(
		t.Context(), dbadapter.New(scenario.DB),
	))
	scenario.CorruptEpochWrites()
	scenario.ApplyBoundary()
	verified, mismatches := scenario.ComputedResults(computedEpoch)
	require.Positive(t, verified)
	require.Positive(t, mismatches, "the stored nonce differs from the set's")
}

// TestSetComputedSummaryCarriesTheStickyReadiness requires a summary saved
// not ready over a ready one to stay ready in the set, and a ready summary
// the set never saw to be counted as a divergence.
func TestSetComputedSummaryCarriesTheStickyReadiness(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsVerify,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	saveReady := func(epoch uint64, ready bool) {
		t.Helper()
		sm := models.EpochSummary{
			Epoch: epoch, TotalActiveStake: 5, SnapshotReady: ready,
		}
		scenario.SummaryInsideTxn(sm)
	}
	// An abandoned transaction leaves nothing to carry over, so the sticky
	// bit is carried within one: the ready save, then the not-ready one.
	kept := scenario.SaveSummariesInsideTxn(60, true, false)
	require.NotNil(t, kept)
	require.True(t, kept.SnapshotReady, "readiness is kept once set")
	verified, mismatches := scenario.ComputedResults(computedEpochSummary)
	require.Equal(t, float64(2), verified)
	require.Zero(t, mismatches)

	// Ready in the store alone: the set computes not ready.
	require.NoError(t, store.SaveEpochSummary(&models.EpochSummary{
		Epoch: 61, TotalActiveStake: 5, SnapshotReady: true,
	}, nil))
	saveReady(61, false)
	verified, mismatches = scenario.ComputedResults(computedEpochSummary)
	require.Equal(t, float64(3), verified)
	require.Equal(t, float64(1), mismatches)
}

// TestSetComputedEnactmentDivergenceIsCounted requires an enactment the
// store makes from a proposal the set does not hold to be counted, and the
// same enactment through the ledger not to be.
func TestSetComputedEnactmentDivergenceIsCounted(t *testing.T) {
	t.Parallel()
	for _, around := range []bool{false, true} {
		name := "through the ledger"
		if around {
			name = "around the ledger"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			set := openComputedSet(t)
			scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
				t, set, ledger.UtxoSetReadsVerify,
			)
			require.NoError(t, set.LoadLedgerState(
				t.Context(), dbadapter.New(scenario.DB),
			))
			p := ledger.UpdateProposal{
				Slot: scenario.RollbackTarget.Slot + 1, Epoch: 1,
				Keys: []byte{1, 2}, MaxTxSize: 21_000,
			}
			if around {
				scenario.ProposeAroundTheLedger(p)
			} else {
				scenario.Propose(p)
			}
			require.NoError(t, scenario.EnactInsideTxn(2))
			verified, mismatches := scenario.ComputedResults(computedEnactment)
			require.Equal(t, float64(1), verified)
			if around {
				require.Equal(t, float64(1), mismatches,
					"the store enacted a proposal the set does not hold")
			} else {
				require.Zero(t, mismatches)
			}
		})
	}
}
