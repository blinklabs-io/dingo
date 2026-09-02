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

package sqlite

import (
	"bytes"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mithrilTrustBoundarySyncKey mirrors database.mithrilLedgerSlotSyncKey. The
// database package keeps its own unexported copy for the same reason this
// test does: nothing lower than it may import the ledger that writes the key.
const mithrilTrustBoundarySyncKey = "mithril_ledger_slot"

// TestCountPoolBlocksInSlotRangeExcludesMithrilImportedCounters covers the
// two row kinds pool_opcert_sequence carries. A block-apply writes one row per
// block minted, but a Mithril restore also writes one row per pool in the
// certified HeaderState counter map, all at the snapshot's anchor slot
// (ledgerstate.importOpCertCounters). Those rows are counters, not blocks: the
// node never applied a block for them, and the anchor slot cannot hold more
// than one block in any case.
//
// Reward performance (ledger/reward_calculation.go),
// reward_pool_input seeding (ledger/snapshot/rotation.go) and Blockfrost's
// blocks_minted all count these rows as minted blocks, so a bootstrapped node
// credits every pool holding a certified counter with a block it never made
// and inflates the epoch denominator by the size of the pool set.
func TestCountPoolBlocksInSlotRangeExcludesMithrilImportedCounters(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const boundarySlot = 1000
	require.NoError(t, store.SetSyncState(
		mithrilTrustBoundarySyncKey, "1000", nil,
	))

	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)
	pkhB := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xB2}, 28)),
	)
	pkhC := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xC3}, 28)),
	)

	// The certified counter map imported at the anchor: three pools, no
	// blocks applied for any of them.
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 5, boundarySlot, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 7, boundarySlot, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhC, 2, boundarySlot, nil))

	// One block the node actually applied after the boundary.
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 5, 1200, nil))

	pools := []lcommon.PoolKeyHash{pkhA, pkhB, pkhC}
	counts, total, err := store.CountPoolBlocksInSlotRange(
		pools, 900, 1300, nil,
	)
	require.NoError(t, err)

	assert.Equal(t, map[string]uint64{
		string(pkhA.Bytes()): 1,
		string(pkhB.Bytes()): 0,
		string(pkhC.Bytes()): 0,
	}, counts, "only the post-boundary block counts as minted")
	assert.Equal(t, uint64(1), total,
		"epoch denominator must not include imported counter rows")
}

// TestGetPoolBlockIssuersInSlotRangeExcludesMithrilImportedCounters covers the
// ordered-row path the decentralization-aware reward count uses
// (ledger.rewardBlockCountsExcludingOverlaySlots), which reads the same table
// directly and so needs the same discrimination.
func TestGetPoolBlockIssuersInSlotRangeExcludesMithrilImportedCounters(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	const boundarySlot = 1000
	require.NoError(t, store.SetSyncState(
		mithrilTrustBoundarySyncKey, "1000", nil,
	))

	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)
	pkhB := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xB2}, 28)),
	)

	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 5, boundarySlot, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 7, boundarySlot, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhB, 7, 1100, nil))

	rows, err := store.GetPoolBlockIssuersInSlotRange(900, 1300, nil)
	require.NoError(t, err)

	require.Len(t, rows, 1, "only the post-boundary block is an issued block")
	assert.Equal(t, uint64(1100), rows[0].Slot)
	assert.Equal(t, pkhB.Bytes(), []byte(rows[0].PoolKeyHash))
}

// TestPoolBlockCountsWithoutMithrilBoundaryCountEveryRow pins the genesis-sync
// case: with no boundary recorded every row in the table is a block the node
// applied, including rows at low slots, so the filter must not narrow them.
func TestPoolBlockCountsWithoutMithrilBoundaryCountEveryRow(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)

	pkhA := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(bytes.Repeat([]byte{0xA1}, 28)),
	)

	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 1, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(pkhA, 1, 20, nil))

	counts, total, err := store.CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{pkhA}, 0, 100, nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), counts[string(pkhA.Bytes())])
	assert.Equal(t, uint64(2), total)

	rows, err := store.GetPoolBlockIssuersInSlotRange(0, 100, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
}
