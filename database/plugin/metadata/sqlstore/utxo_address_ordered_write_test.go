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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

type utxoAddressPosition struct {
	TxID       int64
	OutputIdx  int64
	Slot       int64
	BlockIndex int64
}

func readUtxoAddressPosition(
	t *testing.T,
	store *Store,
	utxoID int64,
) utxoAddressPosition {
	t.Helper()
	var pos utxoAddressPosition
	require.NoError(t, store.writeDB.QueryRowContext(
		context.Background(),
		`SELECT tx_id, output_idx, slot, block_index
FROM utxo_address WHERE utxo_id = ?`, utxoID,
	).Scan(&pos.TxID, &pos.OutputIdx, &pos.Slot, &pos.BlockIndex))
	return pos
}

func queuedShapeNames(rows *rowBatch) []string {
	var names []string
	for _, entry := range rows.entries {
		if len(entry.rows) > 0 {
			names = append(names, entry.shape.name)
		}
	}
	return names
}

// seedProducedOutput stores a transaction applied at slot/blockIndex with one
// output at outputIdx, as the writer has it when it queues the address row.
func seedProducedOutput(
	t *testing.T,
	store *Store,
	txID, slot, blockIndex, outputIdx int64,
	appliedRow bool,
) int64 {
	t.Helper()
	ctx := context.Background()
	_, err := store.writeDB.ExecContext(ctx,
		`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`,
		txID, []byte{byte(txID), 1, 2, 3},
	)
	require.NoError(t, err)
	if appliedRow {
		_, err = store.writeDB.ExecContext(ctx,
			`INSERT INTO applied_transaction (tx_id, slot, block_index)
VALUES (?, ?, ?)`, txID, slot, blockIndex,
		)
		require.NoError(t, err)
	}
	result, err := store.writeDB.ExecContext(ctx,
		`INSERT INTO utxo (tx_id, output_idx, amount, added_slot, deleted_slot)
VALUES (?, ?, '1', ?, 0)`, txID, outputIdx, slot,
	)
	require.NoError(t, err)
	id, err := result.LastInsertId()
	require.NoError(t, err)
	return id
}

func TestQueueUtxoAddressWritesFillPosition(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	address := []byte("addr_test1ordered")
	blockIndex := uint32(7)
	txID := uint(11)
	producerID := int64(11)
	producing := seedProducedOutput(t, store, producerID, 300, 7, 2, true)
	collateral := seedProducedOutput(t, store, 12, 301, 7, 3, true)
	collateralTx := uint(12)

	var rows rowBatch
	queueUtxoAddress(&rows, producing, address, &models.Utxo{
		TransactionID: &txID, OutputIdx: 2, AddedSlot: 300,
		BlockIndex: &blockIndex,
	})
	queueUtxoAddress(&rows, collateral, address, &models.Utxo{
		CollateralReturnForTxID: &collateralTx, OutputIdx: 3,
		AddedSlot: 301, BlockIndex: &blockIndex,
	})
	require.Equal(
		t, []string{utxoAddressOrderedShape.name},
		queuedShapeNames(&rows),
		"both rows carry their position and share the ordered shape",
	)
	require.NoError(t, rows.flush(ctx, store.writeDB, store, 500))

	written := map[int64]utxoAddressPosition{
		producing:  readUtxoAddressPosition(t, store, producing),
		collateral: readUtxoAddressPosition(t, store, collateral),
	}
	require.NoError(t, fillUtxoAddressOrderByIDs(
		ctx, store.writeDB, []int64{producing, collateral},
	))
	for id, pos := range written {
		require.Equal(
			t, readUtxoAddressPosition(t, store, id), pos,
			"the written position must equal the one the fill derives",
		)
	}
	require.Equal(
		t, utxoAddressPosition{TxID: 11, OutputIdx: 2, Slot: 300, BlockIndex: 7},
		written[producing],
	)
}

func TestQueueUtxoAddressFallsBackWithoutPosition(t *testing.T) {
	t.Parallel()
	address := []byte("addr_test1plain")
	txID := uint(5)
	blockIndex := uint32(1)
	tests := map[string]*models.Utxo{
		"no producing transaction id": {BlockIndex: &blockIndex},
		"no block index":              {TransactionID: &txID},
	}
	for name, utxo := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var rows rowBatch
			queueUtxoAddress(&rows, 9, address, utxo)
			require.Equal(
				t, []string{utxoAddressShape.name}, queuedShapeNames(&rows),
			)
		})
	}
}

func TestFlushFillsOnlyPlainUtxoAddressRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	// The utxo has no application row, so a fill would write block_index 0.
	// The ordered row's own value must survive the flush.
	id := seedProducedOutput(t, store, 21, 400, 0, 0, false)
	txID := uint(21)
	blockIndex := uint32(9)
	var rows rowBatch
	queueUtxoAddress(&rows, id, []byte("addr_test1nofill"), &models.Utxo{
		TransactionID: &txID, AddedSlot: 400, BlockIndex: &blockIndex,
	})
	require.NoError(t, rows.flush(ctx, store.writeDB, store, 500))
	require.Equal(
		t, utxoAddressPosition{TxID: 21, Slot: 400, BlockIndex: 9},
		readUtxoAddressPosition(t, store, id),
	)

	plain := seedProducedOutput(t, store, 22, 410, 3, 0, true)
	rows = rowBatch{}
	rows.add(utxoAddressShape, plain, []byte("addr_test1fill"))
	require.NoError(t, rows.flush(ctx, store.writeDB, store, 500))
	require.Equal(
		t, utxoAddressPosition{TxID: 22, Slot: 410, BlockIndex: 3},
		readUtxoAddressPosition(t, store, plain),
		"a plain row still takes its position from the fill",
	)
}

func TestDropUtxoAddressRemovesEveryShape(t *testing.T) {
	t.Parallel()
	var rows rowBatch
	rows.add(utxoAddressShape, int64(5), []byte("a"))
	rows.add(utxoAddressShape, int64(6), []byte("b"))
	rows.add(
		utxoAddressOrderedShape, int64(5), []byte("a"),
		int64(1), int64(0), int64(2), int64(3),
	)
	rows.add(
		utxoAddressOrderedShape, int64(7), []byte("c"),
		int64(1), int64(1), int64(2), int64(3),
	)
	require.Equal(t, 4, rows.rowCount)

	rows.dropUtxoAddress(5)

	require.Equal(t, 2, rows.rowCount)
	for _, entry := range rows.entries {
		for _, row := range entry.rows {
			require.NotEqual(t, int64(5), row[0], "shape %s", entry.shape.name)
		}
	}
}

func TestSetTransactionQueuesOrderedUtxoAddressRows(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	tx, point, _, _ := assetOutputTx(t, 90)
	const blockIndex = 3
	require.NoError(t, store.SetTransactionLeiosClosureBatched(
		tx, point, blockIndex, nil, true, nil, acc, txn, 0,
	))
	rows := &acc.(*transactionBatchAccumulator).rows
	queued := queuedShapeNames(rows)
	require.Contains(t, queued, utxoAddressOrderedShape.name)
	require.NotContains(t, queued, utxoAddressShape.name)
	require.NoError(t, store.FlushBatch(acc, txn))

	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var id int64
	var got utxoAddressPosition
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT utxo_id, tx_id, output_idx, slot, block_index
FROM utxo_address`).Scan(
		&id, &got.TxID, &got.OutputIdx, &got.Slot, &got.BlockIndex,
	))
	require.Equal(t, int64(blockIndex), got.BlockIndex)
	require.NoError(t, fillUtxoAddressOrderByIDs(ctx, db, []int64{id}))
	var filled utxoAddressPosition
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT tx_id, output_idx, slot, block_index
FROM utxo_address WHERE utxo_id = ?`, id).Scan(
		&filled.TxID, &filled.OutputIdx, &filled.Slot, &filled.BlockIndex,
	))
	require.Equal(t, filled, got)
}
