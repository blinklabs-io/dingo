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
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func blockIdentityTestHash(b byte) []byte {
	return bytes.Repeat([]byte{b}, 32)
}

// insertBlockIdentityTx writes one applied transaction through the write path
// every storage mode shares.
func insertBlockIdentityTx(
	ctx context.Context,
	t *testing.T,
	store *Store,
	db queryer,
	txHash byte,
	blockHash []byte,
	index int,
) {
	t.Helper()
	var block any
	if blockHash != nil {
		block = blockHash
	}
	_, _, err := store.insertTransactionUnbatched(ctx, db,
		blockIdentityTestHash(txHash), block, nil, 10, 0,
		"1", "0", "0", index, true,
	)
	require.NoError(t, err)
}

func appliedTransactionColumns(t *testing.T, store *Store) map[string]bool {
	t.Helper()
	rows, err := store.readDB.QueryContext(t.Context(),
		`SELECT name FROM pragma_table_info('applied_transaction')`)
	require.NoError(t, err)
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		columns[name] = true
	}
	require.NoError(t, rows.Err())
	return columns
}

// TestAppliedTransactionHoldsBlockHashOnce pins that a block's hash is stored
// once in the block dictionary and every transaction row of the block refers to
// it by integer id, instead of repeating the 32-byte hash per row.
func TestAppliedTransactionHoldsBlockHashOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	columns := appliedTransactionColumns(t, store)
	require.False(t, columns["block_hash"],
		"applied_transaction must not repeat the block hash per row")
	require.True(t, columns["block_id"])

	blockA, blockB := blockIdentityTestHash(0xa1), blockIdentityTestHash(0xb2)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	for i := range 3 {
		insertBlockIdentityTx(ctx, t, store, db, byte(i+1), blockA, i)
	}
	insertBlockIdentityTx(ctx, t, store, db, 4, blockB, 0)
	insertBlockIdentityTx(ctx, t, store, db, 5, nil, 0)
	require.NoError(t, txn.Commit())

	var blocks int
	require.NoError(t, store.readDB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM block_identity`).Scan(&blocks))
	require.Equal(t, 2, blocks, "one dictionary row per distinct block hash")

	inA, err := store.GetTransactionsByBlockHash(blockA, nil)
	require.NoError(t, err)
	require.Len(t, inA, 3)
	for i, tx := range inA {
		require.Equal(t, blockIdentityTestHash(byte(i+1)), tx.Hash)
		require.Equal(t, blockA, tx.BlockHash)
		require.Equal(t, uint32(i), tx.BlockIndex)
	}
	inB, err := store.GetTransactionsByBlockHash(blockB, nil)
	require.NoError(t, err)
	require.Len(t, inB, 1)
	require.Equal(t, blockB, inB[0].BlockHash)
	unknown, err := store.GetTransactionsByBlockHash(
		blockIdentityTestHash(0xee), nil,
	)
	require.NoError(t, err)
	require.Empty(t, unknown)

	byHash, err := store.GetTransactionByHash(blockIdentityTestHash(5), nil)
	require.NoError(t, err)
	require.NotNil(t, byHash)
	require.Empty(t, byHash.BlockHash)

	// A replay of an applied transaction moves it to the block it is now in.
	txn = store.Transaction(t.Context())
	db, ctx, err = store.dbFromTxn(txn)
	require.NoError(t, err)
	insertBlockIdentityTx(ctx, t, store, db, 1, blockB, 1)
	require.NoError(t, txn.Commit())
	inA, err = store.GetTransactionsByBlockHash(blockA, nil)
	require.NoError(t, err)
	require.Len(t, inA, 2)
	inB, err = store.GetTransactionsByBlockHash(blockB, nil)
	require.NoError(t, err)
	require.Len(t, inB, 2)
}

// TestBlockIdentityRollbackKeepsLaterWritesReadable pins that a rolled-back
// dictionary insert leaves nothing a later write can mistake for a resolved
// block: the same hash written again resolves to the id the new insert took.
func TestBlockIdentityRollbackKeepsLaterWritesReadable(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	block := blockIdentityTestHash(0xc3)
	for _, mode := range []string{"unbatched", "batch"} {
		acc := store.NewBatchAccumulator().(*transactionBatchAccumulator)
		defer acc.Reset()
		insert := func(ctx context.Context, db queryer, tx byte, idx int) {
			if mode == "unbatched" {
				insertBlockIdentityTx(ctx, t, store, db, tx, block, idx)
				return
			}
			_, _, err := acc.insertTransaction(ctx, db,
				blockIdentityTestHash(tx), block, nil, 10, 0,
				"1", "0", "0", idx, true,
			)
			require.NoError(t, err)
		}
		// Padding rows move the dictionary's next id so a stale id from the
		// rolled-back insert cannot coincide with the new one.
		pad := store.Transaction(t.Context())
		db, ctx, err := store.dbFromTxn(pad)
		require.NoError(t, err)
		insertBlockIdentityTx(ctx, t, store, db, 0x70, blockIdentityTestHash(0x70), 0)
		require.NoError(t, pad.Commit())

		rolledBack := store.Transaction(t.Context())
		db, ctx, err = store.dbFromTxn(rolledBack)
		require.NoError(t, err)
		insert(ctx, db, 0x71, 0)
		require.NoError(t, rolledBack.Rollback())
		// The accumulator's prepared statement belongs to the rolled-back
		// transaction; the write path resets it with the transaction.
		acc.Reset()

		pad = store.Transaction(t.Context())
		db, ctx, err = store.dbFromTxn(pad)
		require.NoError(t, err)
		insertBlockIdentityTx(ctx, t, store, db, 0x72, blockIdentityTestHash(0x72), 0)
		require.NoError(t, pad.Commit())

		committed := store.Transaction(t.Context())
		db, ctx, err = store.dbFromTxn(committed)
		require.NoError(t, err)
		insert(ctx, db, 0x73, 0)
		require.NoError(t, committed.Commit())

		got, err := store.GetTransactionsByBlockHash(block, nil)
		require.NoError(t, err)
		require.Len(t, got, 1, mode)
		require.Equal(t, blockIdentityTestHash(0x73), got[0].Hash, mode)
		require.Equal(t, block, got[0].BlockHash, mode)
		_, err = store.writeDB.ExecContext(t.Context(),
			`DELETE FROM applied_transaction`)
		require.NoError(t, err)
		_, err = store.writeDB.ExecContext(t.Context(),
			`DELETE FROM block_identity`)
		require.NoError(t, err)
	}
}

// TestBlockTransactionLookupUsesBlockIdIndex pins that finding a block's
// transactions by hash resolves the dictionary row through its unique key and
// reads the block's rows in block_index order from idx_applied_transaction_
// block_id, with no sort and no scan of applied_transaction.
func TestBlockTransactionLookupUsesBlockIdIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	block := blockIdentityTestHash(0xd4)
	txn := store.Transaction(t.Context())
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	for i := range 4 {
		insertBlockIdentityTx(ctx, t, store, db, byte(i+1), block, 3-i)
	}
	require.NoError(t, txn.Commit())

	plan := blockIdentityPlan(t, store,
		`SELECT a.tx_id FROM applied_transaction AS a
JOIN block_identity AS b ON b.id = a.block_id
WHERE b.hash = ? ORDER BY a.block_index ASC`, block)
	require.Contains(t, plan, "idx_applied_transaction_block_id")
	require.NotContains(t, plan, "TEMP B-TREE")
	require.NotContains(t, plan, "SCAN a")
	require.NotContains(t, plan, "SCAN applied_transaction")
}

func blockIdentityPlan(
	t *testing.T,
	store *Store,
	query string,
	args ...any,
) string {
	t.Helper()
	rows, err := store.writeDB.QueryContext(t.Context(),
		"EXPLAIN QUERY PLAN "+query, args...)
	require.NoError(t, err)
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan.WriteString(detail + "\n")
	}
	require.NoError(t, rows.Err())
	return plan.String()
}

// TestResolveBlockIDsAssignsOneIdPerDistinctHash resolves more hashes than one
// statement binds, with repeats, a block that already has a row, and hashes
// that name no block.
func TestResolveBlockIDsAssignsOneIdPerDistinctHash(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	const distinct = 1200
	hashes := make([]any, 0, 2*distinct+2)
	for i := range distinct {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, uint64(i+1))
		hashes = append(hashes, hash, hash)
	}
	hashes = append(hashes, nil, []byte{})
	existing := hashes[0].([]byte)
	res, err := store.writeDB.ExecContext(t.Context(),
		`INSERT INTO block_identity (hash) VALUES (?)`, existing)
	require.NoError(t, err)
	existingID, err := res.LastInsertId()
	require.NoError(t, err)

	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	ids, err := resolveBlockIDs(ctx, db, hashes)
	require.NoError(t, err)
	require.Len(t, ids, distinct)
	seen := map[int64]bool{}
	for _, hash := range hashes {
		id, err := ids.id(hash)
		require.NoError(t, err)
		if value, _ := hash.([]byte); len(value) == 0 {
			require.Nil(t, id)
			continue
		}
		require.Positive(t, id)
		seen[id.(int64)] = true
	}
	require.Len(t, seen, distinct, "each distinct hash has its own id")
	id, err := ids.id(existing)
	require.NoError(t, err)
	require.Equal(t, existingID, id, "an existing row keeps its id")
	require.NoError(t, txn.Commit())
	var rows int
	require.NoError(t, store.readDB.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM block_identity`).Scan(&rows))
	require.Equal(t, distinct, rows)
}
