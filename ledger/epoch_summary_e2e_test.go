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

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/stretchr/testify/require"
)

// summaryEpochSpan is how far past the newest epoch row a summary can sit.
const summaryEpochSpan = 4

// requireSummariesMatch requires the set's epoch summary rows to be the
// metadata store's, row IDs aside, and returns how many there are.
func requireSummariesMatch(
	t *testing.T,
	set *utxoset.Set,
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
) int {
	t.Helper()
	epochs, err := ldb.GetEpochs(txn)
	require.NoError(t, err)
	var hi uint64
	for _, e := range epochs {
		hi = max(hi, e.EpochId)
	}
	n := 0
	for epoch := uint64(0); epoch <= hi+summaryEpochSpan; epoch++ {
		want, err := ldb.GetEpochSummary(epoch, txn)
		require.NoError(t, err)
		got, err := set.EpochSummary(epoch)
		require.NoError(t, err)
		if want == nil {
			require.Nil(t, got, "summary %d", epoch)
			continue
		}
		n++
		require.NotNil(t, got, "summary %d", epoch)
		w, g := *want, *got
		w.ID, g.ID = 0, 0
		require.Equal(t, w, g, "summary %d", epoch)
	}
	return n
}

// The set keeps the summary the snapshot manager saves at a boundary and
// drops it, with the snapshots, when a rollback leaves the epoch behind.
func TestLiveUtxoSetEpochSummariesFollowABoundary(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set,
	)
	ldb := dbadapter.New(scenario.DB)
	require.NoError(t, set.LoadLedgerState(t.Context(), ldb))
	count := func() int {
		t.Helper()
		txn := ldb.Begin(t.Context(), false)
		defer txn.Release()
		return requireSummariesMatch(t, set, ldb, txn)
	}
	before := count()
	scenario.ApplyBoundary()
	after := count()
	require.Greater(t, after, before, "the boundary saved no summary")
	scenario.RollbackBeforeBoundary()
	require.Equal(t, before, count(), "the rollback keeps the boundary's summary")
}

// requireSummaryReads reads every epoch's summary and total active stake
// through the ledger's LedgerDB and requires the metadata store's answers.
// It returns the number of summaries that exist and the number of mark
// totals the set answered from a ready summary.
func requireSummaryReads(
	t *testing.T,
	ldb, store ledgerdb.LedgerDB,
	stage string,
) (summaries, ready int) {
	t.Helper()
	epochs, err := store.GetEpochs(nil)
	require.NoError(t, err)
	var hi uint64
	for _, e := range epochs {
		hi = max(hi, e.EpochId)
	}
	for epoch := uint64(0); epoch <= hi+summaryEpochSpan; epoch++ {
		want, err := store.GetEpochSummary(epoch, nil)
		require.NoError(t, err)
		got, err := ldb.GetEpochSummary(epoch, nil)
		require.NoError(t, err)
		if want == nil {
			require.Nil(t, got, "%s: summary %d", stage, epoch)
		} else {
			summaries++
			require.NotNil(t, got, "%s: summary %d", stage, epoch)
			w, g := *want, *got
			w.ID, g.ID = 0, 0
			require.Equal(t, w, g, "%s: summary %d", stage, epoch)
			if want.SnapshotReady {
				ready++
			}
		}
		for _, typ := range []string{"mark", "set", "go", "actv"} {
			wt, err := store.GetTotalActiveStake(epoch, typ, nil)
			require.NoError(t, err)
			gt, err := ldb.GetTotalActiveStake(epoch, typ, nil)
			require.NoError(t, err)
			require.Equal(t, wt, gt, "%s: total %d/%s", stage, epoch, typ)
		}
	}
	return summaries, ready
}

// With metadata reads available every summary and total active stake read agrees with the
// metadata store; from the live set the set answers them the same, across a
// boundary, a rollback staged in an open transaction (set mode), a save staged in one,
// and the committed rollback.
func TestEpochSummaryReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	for _, reads := range []string{"set"} {
		t.Run(reads, func(t *testing.T) {
			t.Parallel()
			set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { _ = set.Close() })
			scenario := ledger.NewBoundaryEpochStateScenarioForTest(t, set)
			store := dbadapter.New(scenario.DB)
			require.NoError(t, set.LoadLedgerState(t.Context(), store))
			// The harness's stake is rows the set never saw; from the live set the
			// boundary's snapshot reads live stake from the outputs the set
			// holds.
			scenario.AddSetStake(bytes.Repeat([]byte{0x71}, 28), 20_000_000)
			scenario.AddSetStake(bytes.Repeat([]byte{0x72}, 28), 20_000_000)
			ldb := scenario.LedgerDB()
			before, _ := requireSummaryReads(t, ldb, store, "before the boundary")
			scenario.ApplyBoundary()
			after, ready := requireSummaryReads(t, ldb, store, "after the boundary")
			require.Greater(t, after, before, "the boundary saved no summary")
			require.Positive(t, ready, "the boundary's summary is not ready")

			var saved *models.EpochSummary
			for epoch := uint64(0); epoch < 64 && saved == nil; epoch++ {
				sm, err := store.GetEpochSummary(epoch, nil)
				require.NoError(t, err)
				if sm != nil && sm.SnapshotReady && sm.TotalActiveStake > 0 {
					saved = sm
				}
			}
			require.NotNil(t, saved, "no ready summary with stake")
			inside, total := scenario.SummaryInsideRollback(saved.Epoch)
			require.Nil(t, inside, "the staged rollback hides the summary")
			require.Zero(t, total, "the staged rollback hides the stake")

			// A save staged in an open transaction is read back from it.
			next := models.EpochSummary{
				Epoch: saved.Epoch + 40, TotalActiveStake: 777,
				TotalPoolCount: 3, SnapshotReady: true,
			}
			staged, stagedTotal := scenario.SummaryInsideTxn(next)
			require.NotNil(t, staged)
			require.Equal(t, next.TotalActiveStake, staged.TotalActiveStake)
			require.Equal(t, uint64(777), stagedTotal)
			gone, err := ldb.GetEpochSummary(next.Epoch, nil)
			require.NoError(t, err)
			require.Nil(t, gone, "an abandoned transaction leaves no summary")

			scenario.RollbackBeforeBoundary()
			requireSummaryReads(t, ldb, store, "after the rollback")
		})
	}
}

// A set from before it kept summaries loads them from the metadata store.
func TestLiveUtxoSetEpochSummariesLoadFromTheMetadataStore(t *testing.T) {
	t.Parallel()
	first, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, first,
	)
	store := dbadapter.New(scenario.DB)
	require.NoError(t, first.LoadLedgerState(t.Context(), store))
	scenario.ApplyBoundary()

	loaded, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.Close() })
	ok, err := loaded.EpochSummariesLoaded()
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, loaded.LoadLedgerState(t.Context(), store))
	ok, err = loaded.EpochSummariesLoaded()
	require.NoError(t, err)
	require.True(t, ok)
	txn := store.Begin(t.Context(), false)
	defer txn.Release()
	require.Positive(t, requireSummariesMatch(t, loaded, store, txn))
}

// A summary the metadata store holds and the set does not is a counted
// divergence, and the read still answers from the store.
