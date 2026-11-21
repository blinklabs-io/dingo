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

	"github.com/blinklabs-io/dingo/database"
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
// at a slot, and every pool's counters as of a slot, over counters that rise,
// repeat, drop and are batched, and again after a rollback below all but the
// first of them.
func TestOpCertSlotReadsFromTheLiveUtxoSet(t *testing.T) {
	t.Parallel()
	scenario, _, _ := setModeScenario(t)
	ldb := scenario.LedgerDB()
	slots := scenario.OpCerts()
	pool, other := string(slots.Pool), string(slots.Other)
	check := func(
		stage string,
		slot uint64,
		wantExists bool,
		want map[string]uint64,
	) {
		t.Helper()
		exists, err := ldb.PoolOpCertSequencesExistAtSlot(t.Context(), slot, nil)
		require.NoError(t, err, "%s exist at %d", stage, slot)
		require.Equal(t, wantExists, exists, "%s exist at %d", stage, slot)
		got, err := ldb.LatestPoolOpCertSequencesAtOrBefore(
			t.Context(), slot, nil,
		)
		require.NoError(t, err, "%s counters at %d", stage, slot)
		require.Equal(t, want, got, "%s counters at %d", stage, slot)
	}
	none := map[string]uint64{}
	first := map[string]uint64{pool: 3}
	raised := map[string]uint64{pool: 5}
	both := map[string]uint64{pool: 5, other: 2}
	batched := map[string]uint64{pool: 6, other: 2}
	const before = "before the rollback"
	check(before, slots.Base-1, false, none)
	check(before, slots.Base, true, first)
	check(before, slots.Base+1, false, first)
	check(before, slots.Raised, true, raised)
	check(before, slots.Repeated, true, both)
	check(before, slots.Lowered, true, both)
	check(before, slots.Batched, true, batched)
	check(before, slots.Batched+1, false, batched)

	scenario.RollbackBeforeBoundary()
	const after = "after the rollback"
	check(after, slots.Base-1, false, none)
	check(after, slots.Base, true, first)
	for _, slot := range []uint64{
		slots.Raised, slots.Repeated, slots.Lowered, slots.Batched,
	} {
		check(after, slot, false, first)
	}
}

// TestOpCertSlotReadsBeforeTheSetsRecordUseItsBaseline requires the slot
// reads for a slot before the set's record to be answered from the counters
// the set was loaded with, each pool's latest observation.
func TestOpCertSlotReadsBeforeTheSetsRecordUseItsBaseline(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	pool := lcommon.PoolKeyHash(bytes.Repeat([]byte{0x44}, 28))
	scenario := ledger.NewSeededBoundaryEpochStateScenarioForTest(
		t, set, nil, func(db *database.Database) {
			for _, w := range []struct{ sequence, slot uint64 }{
				{4, 100}, {6, 200},
			} {
				require.NoError(t, dbadapter.New(db).UpdatePoolOpCertSequence(
					t.Context(), pool, w.sequence, w.slot, nil,
				))
			}
		},
	)
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

	ok, err = ldb.PoolOpCertSequencesExistAtSlot(ctx, 100, nil)
	require.NoError(t, err, "slot 100 is before the set's record")
	require.False(t, ok)
	got, err = ldb.LatestPoolOpCertSequencesAtOrBefore(ctx, 100, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]uint64{string(pool[:]): 6}, got)
}
