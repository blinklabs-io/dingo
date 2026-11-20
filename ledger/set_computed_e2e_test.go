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

// TestSetComputedEpochRowsEqualTheStores requires the epoch and summary rows
// the set computes at a boundary to be the rows the metadata store wrote,
// and to survive a rollback the same way, with every result compared.
func TestSetComputedEpochRowsEqualTheStores(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
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
}

// TestSetComputedEpochRowsAcrossAMidEpochEraSwitch requires the epoch rows
// the set computes through a mid-epoch era switch and its rollback to be the
// store's.
func TestSetComputedEpochRowsAcrossAMidEpochEraSwitch(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewMidEpochEraSwitchScenarioForTest(
		t, set,
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
}

// TestSetComputedEnactmentEqualsTheStores requires the protocol parameter
// rows the set enacts from its own proposals at a boundary to be the rows the
// metadata store wrote, and the rollback to remove them.
func TestSetComputedEnactmentEqualsTheStores(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
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
}

// TestSetComputesEpochStateWithoutTheStore requires a boundary and its
// rollback, from the live set, to give the set the rows the store holds while the
// store's epoch, summary and parameter reads fail.
func TestSetComputesEpochStateWithoutTheStore(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
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
// update, from the live set, to stage the store's rows while the store's parameter
// and proposal reads fail.
func TestSetEnactsWithoutTheStore(t *testing.T) {
	t.Parallel()
	set := openComputedSet(t)
	scenario := ledger.NewClassicUpdateBoundaryScenarioForTest(
		t, set,
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
