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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// openRetainedStakeWindow seeds a store with retainedStakeChain's input and
// opens a closure window over the chain, with the UTxO overlay attached when
// overlay is set and on the immediate-write path otherwise.
func openRetainedStakeWindow(
	t *testing.T,
	n int,
	overlay bool,
) (*Store, types.Txn, types.MetadataBatchAccumulator, lcommon.Transaction, []lcommon.Transaction) {
	t.Helper()
	seed, txs := retainedStakeChain(t, n)
	store := retainedStakeStore(t, seed, false)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	t.Cleanup(acc.Reset)
	if overlay {
		enabled, err := store.EnableLeiosClosureUtxoOverlay(acc, txn, txs)
		require.NoError(t, err)
		require.True(t, enabled)
	}
	return store, txn, acc, seed, txs
}

func applyRetainedStakeTransaction(
	store *Store,
	txn types.Txn,
	acc types.MetadataBatchAccumulator,
	tx lcommon.Transaction,
	index uint32,
) error {
	return store.SetTransactionLeiosClosureBatched(
		tx, ocommon.Point{Slot: 100, Hash: overlayTxID(0x63, 0)},
		index, nil, true, nil, acc, txn, 0,
	)
}

// TestLeiosClosureUtxoOverlayReadBarrier reads, with plain SQL inside the
// window, rows the overlay has not written yet: an output it created, the
// spend of an output stored before the window, and the live stake both
// change. Each read must see what immediate writes leave at that point.
func TestLeiosClosureUtxoOverlayReadBarrier(t *testing.T) {
	t.Parallel()
	const outputQuery = `SELECT u.deleted_slot, u.spent_at_tx_id IS NOT NULL
FROM utxo AS u JOIN "transaction" AS tx ON tx.id = u.tx_id
WHERE tx.hash = ? AND u.output_idx = 0`
	type observation struct {
		createdSlot, spentSlot int64
		createdSpent, spent    bool
		stake                  string
	}
	observe := func(overlay bool) (string, observation) {
		store, txn, acc, seed, txs := openRetainedStakeWindow(t, 2, overlay)
		initial, _, err := store.dbFromTxn(txn)
		require.NoError(t, err)
		before := retainedStakeState(t, initial)
		require.NoError(
			t,
			applyRetainedStakeTransaction(store, txn, acc, txs[0], 0),
		)
		if overlay {
			require.Zero(
				t,
				acc.(*transactionBatchAccumulator).overlay.stats.BarrierFlushes,
				"the reads below must be the first to write the overlay",
			)
		}
		db, ctx, err := store.dbFromTxn(txn)
		require.NoError(t, err)
		var got observation
		require.NoError(
			t,
			db.QueryRowContext(ctx, outputQuery, txs[0].Hash().Bytes()).
				Scan(&got.createdSlot, &got.createdSpent),
			"a reader missed an output the window created",
		)
		require.NoError(
			t,
			db.QueryRowContext(ctx, outputQuery, seed.Hash().Bytes()).
				Scan(&got.spentSlot, &got.spent),
		)
		got.stake = retainedStakeState(t, db)
		require.NoError(t, store.FlushBatch(acc, txn))
		require.NoError(t, txn.Commit())
		return before, got
	}
	before, want := observe(false)
	_, got := observe(true)
	require.Equal(t, observation{
		spentSlot: 100, spent: true, stake: want.stake,
	}, want)
	require.NotEqual(t, before, want.stake,
		"the transaction must change the live stake the reads compare")
	require.Equal(t, want, got)
}

// The overlay keeps no undo log. Rolling back to a savepoint across overlay
// changes must fail the window, not continue from wrong state, and a read
// barrier's write is such a change: SQLite undoes the rows it wrote, while
// the overlay would still count them as written.
func TestLeiosClosureUtxoOverlaySavepointRollbackFailsClosed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		// barrier applies the transaction before the savepoint and makes
		// the read barrier write it after; otherwise the transaction is
		// staged after the savepoint.
		barrier bool
	}{
		{name: "staged_write"},
		{name: "barrier_write", barrier: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store, txn, acc, _, txs := openRetainedStakeWindow(t, 2, true)
			if tc.barrier {
				require.NoError(
					t,
					applyRetainedStakeTransaction(store, txn, acc, txs[0], 0),
				)
			}
			savepointer := txn.(*sqlTxn)
			require.NoError(t, savepointer.SavePoint("overlay_test"))
			if tc.barrier {
				db, ctx, err := store.dbFromTxn(txn)
				require.NoError(t, err)
				var rows int
				require.NoError(t, db.QueryRowContext(
					ctx, `SELECT COUNT(*) FROM utxo`,
				).Scan(&rows))
				require.Equal(t, 2, rows, "the read must reach the barrier")
			} else {
				require.NoError(
					t,
					applyRetainedStakeTransaction(store, txn, acc, txs[0], 0),
				)
			}
			require.NoError(t, savepointer.RollbackTo("overlay_test"))
			require.ErrorIs(
				t,
				store.FlushBatch(acc, txn),
				errOverlaySavepoint,
			)
		})
	}
}

// An output row stored behind the window's admission check makes the
// overlay's insert fail with ErrBatchUtxoOverlayConflict rather than a bare
// storage error, so the caller can tell a conflict from a failed write.
func TestLeiosClosureUtxoOverlayReportsConflict(t *testing.T) {
	t.Parallel()
	store, txn, acc, _, txs := openRetainedStakeWindow(t, 2, true)
	raw := txn.(*sqlTxn).tx
	hash := txs[1].Hash().Bytes()
	_, err := raw.ExecContext(
		t.Context(), `INSERT INTO "transaction" (hash) VALUES (?)`, hash,
	)
	require.NoError(t, err)
	_, err = raw.ExecContext(t.Context(),
		`INSERT INTO utxo (id, tx_id, output_idx, credential_tag, added_slot,
		   deleted_slot, amount, payment_script)
		 VALUES (1000000, (SELECT id FROM "transaction" WHERE hash = ?),
		   0, 0, 5, 0, '1', 0)`,
		hash,
	)
	require.NoError(t, err)
	for i, tx := range txs {
		require.NoError(
			t,
			applyRetainedStakeTransaction(store, txn, acc, tx, uint32(i)),
		)
	}
	err = store.FlushBatch(acc, txn)
	require.ErrorIs(t, err, types.ErrBatchUtxoOverlayConflict)
	require.ErrorContains(t, err, "utxo.tx_id, utxo.output_idx")
}

// The live-stake replay reads each account once for the whole window, so a
// delete of an account row inside it must fail the window rather than pass.
func TestLeiosClosureUtxoOverlayRefusesAccountDelete(t *testing.T) {
	t.Parallel()
	store, txn, acc, _, _ := openRetainedStakeWindow(t, 2, true)
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		"DELETE FROM account WHERE credential_tag = ? AND staking_key = ?",
		0, bytes.Repeat([]byte{0x41}, 28),
	)
	require.ErrorIs(t, err, errOverlayAccountDelete)
	require.ErrorIs(t, store.FlushBatch(acc, txn), errOverlayAccountDelete)
}
