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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

var opCertSlotReadNames = []string{
	"PoolOpCertSequencesExistAtSlot", "LatestPoolOpCertSequencesAtOrBefore",
}

// TestOpCertSlotReadsFromTheLiveUtxoSet reads whether a counter was observed
// at a slot, and every pool's counters as of a slot, in set mode with the
// store's reads failing, over counters that rise, repeat, drop and are
// batched, and a rollback, and requires the answers to be the store's.
func TestOpCertSlotReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, store := setModeScenario(t)
	reached, _ := scenario.HideStoreReads(opCertSlotReadNames...)
	ldb := scenario.LedgerDB()
	slots := scenario.OpCerts()
	check := func(stage string) {
		t.Helper()
		for _, slot := range []uint64{
			slots.Base - 1, slots.Base, slots.Base + 1, slots.Raised,
			slots.Repeated, slots.Lowered, slots.Batched, slots.Batched + 1,
		} {
			wantE, err := store.PoolOpCertSequencesExistAtSlot(
				t.Context(),
				slot,
				nil,
			)
			require.NoError(t, err)
			gotE, err := ldb.PoolOpCertSequencesExistAtSlot(
				t.Context(),
				slot,
				nil,
			)
			require.NoError(t, err, "%s exist at %d", stage, slot)
			require.Equal(t, wantE, gotE, "%s exist at %d", stage, slot)
			want, err := store.LatestPoolOpCertSequencesAtOrBefore(
				t.Context(),
				slot,
				nil,
			)
			require.NoError(t, err)
			got, err := ldb.LatestPoolOpCertSequencesAtOrBefore(
				t.Context(),
				slot,
				nil,
			)
			require.NoError(t, err, "%s counters at %d", stage, slot)
			require.Equal(t, want, got, "%s counters at %d", stage, slot)
		}
	}
	check("before the rollback")
	scenario.RollbackBeforeBoundary()
	check("after the rollback")
	require.Zero(t, reached(), "a slot read reached the metadata store")
	verified, mismatches := scenario.OpCertReads()
	require.Zero(t, verified, "set mode compares nothing")
	require.Zero(t, mismatches)
}

// TestOpCertSlotReadsStayOnTheStoreForSlotsTheSetHasNoRecordOf requires the
// slot reads to reach the metadata store for a slot before the set's record,
// which a set loaded from the store holds only as each pool's latest
// observation.
func TestOpCertSlotReadsStayOnTheStoreForSlotsTheSetHasNoRecordOf(
	t *testing.T,
) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	scenario := ledger.NewBoundaryEpochStateScenarioForTest(
		t, set, ledger.UtxoSetReadsSet,
	)
	store := dbadapter.New(scenario.DB)
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x44}, 28))
	for _, w := range []struct{ sequence, slot uint64 }{{4, 100}, {6, 200}} {
		require.NoError(t, store.UpdatePoolOpCertSequence(
			t.Context(), pool, w.sequence, w.slot, nil,
		))
	}
	require.NoError(t, set.LoadLedgerState(t.Context(), store))
	reached, _ := scenario.HideStoreReads(opCertSlotReadNames...)
	ldb := scenario.LedgerDB()
	ctx := t.Context()

	ok, err := ldb.PoolOpCertSequencesExistAtSlot(ctx, 200, nil)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = ldb.PoolOpCertSequencesExistAtSlot(ctx, 250, nil)
	require.NoError(t, err)
	require.False(t, ok)
	got, err := ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, 250, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{string(pool[:]): 6}, got)
	require.Zero(t, reached())

	_, err = ldb.PoolOpCertSequencesExistAtSlot(ctx, 100, nil)
	require.Error(t, err, "slot 100 is before the set's record")
	_, err = ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, 100, nil)
	require.Error(t, err)
	require.Equal(t, int64(2), reached())
}
