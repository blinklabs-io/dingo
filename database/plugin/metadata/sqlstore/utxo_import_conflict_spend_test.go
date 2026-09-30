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

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// importConflictHash32 builds a distinct 32-byte hash for use as a tx_id or
// spender reference in these tests, keyed by a single byte so fixtures stay
// readable while remaining unique per call site.
func importConflictHash32(b byte) []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = b
	}
	return h
}

// markUtxoSpentDirect records a spend exactly as the block-apply path would
// leave it on the utxo row (deleted_slot + spent_at_tx_id), without needing a
// fully constructed ledger.TransactionInput. This mirrors the setup pattern
// already used by this package's other conflict-path tests
// (seedRollbackUtxos inserts rows with raw SQL rather than through the
// store's own write methods).
func markUtxoSpentDirect(
	tb testing.TB,
	store *Store,
	txID []byte,
	outputIdx uint32,
	spenderTxID []byte,
	slot uint64,
) {
	tb.Helper()
	_, err := store.writeDB.Exec(
		"UPDATE utxo SET deleted_slot = ?, spent_at_tx_id = ? "+
			"WHERE tx_id = ? AND output_idx = ?",
		int64(slot), spenderTxID, txID, int64(outputIdx),
	)
	require.NoError(tb, err)
}

// utxoDeletedAndSpender reads back the raw deleted_slot/spent_at_tx_id
// columns for a reference, bypassing GetUtxo's "live only" filtering so a
// spent row's state can be inspected directly.
func utxoDeletedAndSpender(
	tb testing.TB,
	store *Store,
	txID []byte,
	outputIdx uint32,
) (deletedSlot int64, spentAtTxID []byte) {
	tb.Helper()
	row := store.writeDB.QueryRow(
		"SELECT deleted_slot, spent_at_tx_id FROM utxo "+
			"WHERE tx_id = ? AND output_idx = ?",
		txID, int64(outputIdx),
	)
	require.NoError(tb, row.Scan(&deletedSlot, &spentAtTxID))
	return deletedSlot, spentAtTxID
}

// TestImportUtxosClearsPostAnchorSpendOnLiveReimport is the dingo#4770
// regression: a Mithril catch-up/reward-repair import re-inserts the
// snapshot's live UTxO set through ON CONFLICT (tx_id, output_idx) DO
// NOTHING. Before the fix, hydrateImportedUtxo updated transaction_id,
// collateral_return_for_tx_id, and added_slot on conflict but left
// deleted_slot/spent_at_tx_id untouched, so an output the certified snapshot
// declares live at its anchor stayed marked spent if the local database had
// already recorded a spend for it after the anchor. The first later block
// that actually spends the output then fails with "utxo not found" and the
// ledger pipeline halts.
func TestImportUtxosClearsPostAnchorSpendOnLiveReimport(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	txID := importConflictHash32(0xaa)
	spender := importConflictHash32(0xbb)

	// 1. The output exists locally, exactly as the snapshot holds it: live,
	// created at slot 100 (the anchor).
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txID, OutputIdx: 0, AddedSlot: 100, Amount: 5_000_000,
	}}, nil))
	liveBefore, err := store.GetUtxo(txID, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, liveBefore, "precondition: live after first import")

	// 2. A real post-anchor block spends it at slot 150.
	markUtxoSpentDirect(t, store, txID, 0, spender, 150)
	spentUtxo, err := store.GetUtxo(txID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentUtxo, "precondition: not live after the post-anchor spend")

	// 3. The catch-up/repair import re-inserts the snapshot's live UTxO set.
	// This output is in it: the snapshot says it is live at the anchor
	// (slot 100), which predates the slot-150 spend the local database
	// recorded after the anchor. The insert conflicts on (tx_id,
	// output_idx), taking the hydrateImportedUtxo path.
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txID, OutputIdx: 0, AddedSlot: 100, Amount: 5_000_000,
	}}, nil))

	liveAfter, err := store.GetUtxo(txID, 0, nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		liveAfter,
		"the snapshot declares this output live at the anchor; the "+
			"post-anchor spend must not survive the conflict-tolerant "+
			"snapshot re-import, or replaying the real block that spends "+
			"it fails with 'utxo not found'",
	)

	deletedSlot, spentAtTxID := utxoDeletedAndSpender(t, store, txID, 0)
	require.Zero(t, deletedSlot, "deleted_slot must be cleared")
	require.Nil(t, spentAtTxID, "spent_at_tx_id must be cleared")
}

// TestImportUtxosLeavesSettledPreAnchorSpendAlone proves the fix is scoped to
// the specific (tx_id, output_idx) reference a conflicting live row names,
// not a blanket unspend. A conflicting live re-import must clear only the
// row it names (B): an unrelated row (A), settled before its own anchor and
// not part of this import batch at all, must not be touched -- even though
// the same call does hit the new clause for B, proving the clause runs and
// still respects scope.
func TestImportUtxosLeavesSettledPreAnchorSpendAlone(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	settledTxID := importConflictHash32(0xcc)
	settledSpender := importConflictHash32(0xdd)
	liveTxID := importConflictHash32(0xee)
	liveSpender := importConflictHash32(0xff)

	// A: created at slot 50, spent at slot 60 -- both comfortably before A's
	// own anchor. This spend is settled: a snapshot at A's anchor correctly
	// does not carry this output as live.
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: settledTxID, OutputIdx: 0, AddedSlot: 50, Amount: 1_000_000,
	}}, nil))
	markUtxoSpentDirect(t, store, settledTxID, 0, settledSpender, 60)
	spentBefore, err := store.GetUtxo(settledTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(t, spentBefore, "precondition: settled spend is not live")

	// B: created at slot 100 (its anchor), then spent post-anchor at 150 --
	// exactly the dingo#4770 shape, seeded here only so the re-import below
	// actually conflicts and the new clause has a row to act on.
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: liveTxID, OutputIdx: 0, AddedSlot: 100, Amount: 2_000_000,
	}}, nil))
	markUtxoSpentDirect(t, store, liveTxID, 0, liveSpender, 150)
	require.Nil(t, mustGetUtxo(t, store, liveTxID, 0), "precondition: B spent")

	// Re-import only B as live. This conflicts on B's (tx_id, output_idx),
	// so hydrateImportedUtxo's new clause actually runs -- unlike a fresh
	// insert, which never reaches it. A is not part of this batch at all.
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: liveTxID, OutputIdx: 0, AddedSlot: 100, Amount: 2_000_000,
	}}, nil))
	require.NotNil(
		t,
		mustGetUtxo(t, store, liveTxID, 0),
		"precondition: B's own conflict must have been cleared, or this "+
			"test proves nothing about scope",
	)

	spentAfter, err := store.GetUtxo(settledTxID, 0, nil)
	require.NoError(t, err)
	require.Nil(
		t,
		spentAfter,
		"an unrelated live import must not resurrect a settled pre-anchor spend",
	)
	deletedSlot, spentAtTxID := utxoDeletedAndSpender(t, store, settledTxID, 0)
	require.Equal(t, int64(60), deletedSlot)
	require.Equal(t, settledSpender, spentAtTxID)
}

// mustGetUtxo is GetUtxo with the error assertion inlined, for preconditions
// that only care about liveness.
func mustGetUtxo(
	t *testing.T,
	store *Store,
	txID []byte,
	outputIdx uint32,
) *models.Utxo {
	t.Helper()
	utxo, err := store.GetUtxo(txID, outputIdx, nil)
	require.NoError(t, err)
	return utxo
}

// TestImportUtxosConflictWithDeletedIncomingLeavesSpendAlone covers the other
// side of the DeletedSlot == 0 gate added for dingo#4770: a conflicting
// import whose own incoming row is itself not live (DeletedSlot != 0, e.g. a
// gap-closure re-import of an output already known consumed) must not clear
// an existing spend either. Only an incoming row that declares the output
// live triggers the clear.
func TestImportUtxosConflictWithDeletedIncomingLeavesSpendAlone(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	txID := importConflictHash32(0x11)
	spender := importConflictHash32(0x22)

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txID, OutputIdx: 0, AddedSlot: 100, Amount: 3_000_000,
	}}, nil))
	markUtxoSpentDirect(t, store, txID, 0, spender, 150)

	// Re-import the same reference, but the incoming row itself is not
	// live: DeletedSlot is set, matching the existing spend. This must take
	// the ON CONFLICT DO NOTHING path without clearing anything.
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: txID, OutputIdx: 0, AddedSlot: 100, Amount: 3_000_000,
		DeletedSlot: 150,
	}}, nil))

	deletedSlot, spentAtTxID := utxoDeletedAndSpender(t, store, txID, 0)
	require.Equal(
		t,
		int64(150),
		deletedSlot,
		"an incoming row that is itself not live must not clear an "+
			"existing spend",
	)
	require.Equal(t, spender, spentAtTxID)
}
