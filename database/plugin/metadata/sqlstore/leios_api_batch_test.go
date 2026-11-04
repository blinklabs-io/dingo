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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestLeiosAPIBatchKeepsDatumAndAssetsImmediate(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	require.NotNil(t, acc)
	defer acc.Reset()
	tx, point := apiDetailTx(t, 31)
	contextSlot := point.Slot - 1
	require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, &contextSlot, acc, txn, 0))
	require.Equal(t, map[string]int{"datum": 1, "key_witness": 0, "address_transaction": 0, "transaction_metadata_label": 0}, tableCounts(t, store, txn, "datum", "key_witness", "address_transaction", "transaction_metadata_label"))
	assetTx, assetPoint, _, _ := assetOutputTx(t, 32)
	require.NoError(t, store.SetTransactionLeiosClosureBatched(assetTx, assetPoint, 1, nil, true, nil, acc, txn, 0))
	require.Equal(t, 1, tableCounts(t, store, txn, "asset")["asset"])
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, 1, keyWitnessCount(t, store, txn))
	require.Equal(t, 2, tableCounts(t, store, txn, "transaction_metadata_label")["transaction_metadata_label"])
}

func TestLeiosAPIBatchReplayReplacesQueuedAndFlushedWitnesses(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	for _, witnesses := range []int{2, 1} {
		tx, point := witnessTx(t, 33, witnesses)
		require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, nil, acc, txn, 0))
	}
	require.Zero(t, keyWitnessCount(t, store, txn))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, 1, keyWitnessCount(t, store, txn))
	tx, point := witnessTx(t, 33, 3)
	require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, 1, nil, true, nil, acc, txn, 0))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, 3, keyWitnessCount(t, store, txn))
}

func TestLeiosAPIBatchPreservesConsumedInputTolerance(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	fx := buildSharedCredentialTx(t, 0x43)
	seedConsumedUtxo(t, store, fx)
	other := []byte{0xfe}
	_, err := store.writeDB.Exec(`UPDATE utxo SET deleted_slot = 1, spent_at_tx_id = ? WHERE tx_id = ? AND output_idx = 0`, other, fx.consumedTxID)
	require.NoError(t, err)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	require.NoError(t, store.SetTransactionLeiosClosureBatched(fx.tx, fx.point, 0, fx.certDeposits, false, nil, acc, txn, 0))
	stored, err := store.GetUtxoIncludingSpent(fx.consumedTxID, 0, txn)
	require.NoError(t, err)
	require.Equal(t, other, []byte(stored.SpentAtTxId))
	require.NoError(t, store.FlushBatch(acc, txn))
}

func TestLeiosAPIBatchStakeChangesAreImmediate(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	fx := buildSharedCredentialTx(t, 0x45)
	seedConsumedUtxo(t, store, fx)
	establishRunningTotal(t, store, fx.ref, 1)
	txn := store.Transaction(context.Background())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	require.NoError(t, store.SetTransactionLeiosClosureBatched(fx.tx, fx.point, 0, fx.certDeposits, false, nil, acc, txn, 0))
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	var got types.Uint64
	require.NoError(t, db.QueryRowContext(ctx, `SELECT utxo_stake FROM reward_live_stake WHERE credential_tag = ? AND staking_key = ?`, fx.ref.Tag, fx.ref.Key).Scan(&got))
	require.Equal(t, types.Uint64(fx.producedAmount), got)
	require.Empty(t, acc.(*transactionBatchAccumulator).stakeDeltas)
}

func TestLeiosAPIBatchDefersOutputAddresses(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newAPIModeSQLiteStore(t, reg)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	acc := store.NewLeiosClosureBatchAccumulator()
	defer acc.Reset()
	var previous lcommon.Transaction
	for i := range 3 {
		tx, point, _, _ := assetOutputTx(t, byte(70+i))
		if previous != nil {
			input, err := mockledger.NewSimpleTransactionInput(previous.Hash().Bytes(), 0)
			require.NoError(t, err)
			tx, err = mockledger.NewTransactionBuilder().WithId(tx.Hash().Bytes()).WithInputs(input).WithOutputs(tx.Outputs()...).WithValid(true).Build()
			require.NoError(t, err)
		}
		require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, uint32(i), nil, true, nil, acc, txn, 0))
		// The following transaction validates against these outputs and assets,
		// while exact-address lookup rows are needed only after the closure.
		stored, err := store.GetUtxoIncludingSpent(tx.Hash().Bytes(), 0, txn)
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.Len(t, stored.Assets, 1)
		previous = tx
	}
	require.Zero(t, tableCounts(t, store, txn, "utxo_address")["utxo_address"], "output addresses must wait for the closure flush")
	insertsBefore := counterValue(t, reg, "insert")
	require.NoError(t, store.FlushBatch(acc, txn))
	require.Equal(t, float64(2), counterValue(t, reg, "insert")-insertsBefore, "one insert for all output addresses and one for all address transactions")
	require.Equal(t, 3, tableCounts(t, store, txn, "utxo_address")["utxo_address"])
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	rows, err := db.QueryContext(ctx, "SELECT ua.address FROM utxo_address ua JOIN utxo u ON u.id = ua.utxo_id ORDER BY u.id")
	require.NoError(t, err)
	var addresses []string
	for rows.Next() {
		var address string
		require.NoError(t, rows.Scan(&address))
		addresses = append(addresses, address)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	for i, address := range addresses {
		tx, _, _, _ := assetOutputTx(t, byte(70+i))
		require.Equal(t, tx.Outputs()[0].Address().String(), address)
	}
	require.NoError(t, txn.Commit())
	require.Equal(t, 3, tableCounts(t, store, nil, "utxo_address")["utxo_address"])
}

func TestLeiosAPIBatchRollbackDropsOutputAddresses(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	txn := store.Transaction(t.Context())
	acc := store.NewLeiosClosureBatchAccumulator()
	tx, point, _, _ := assetOutputTx(t, 74)
	require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, nil, acc, txn, 0))
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Rollback())
	acc.Reset()
	require.Zero(t, tableCounts(t, store, nil, "utxo_address")["utxo_address"])
	txn = store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	require.NoError(t, store.FlushBatch(acc, txn))
	require.NoError(t, txn.Commit())
	require.Zero(t, tableCounts(t, store, nil, "utxo_address")["utxo_address"])
}

func BenchmarkLeiosClosureAddressBatch(b *testing.B) {
	store := newAPIModeSQLiteStore(b, nil)
	output, err := mockledger.NewTransactionOutputBuilder().WithAddress("addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp").WithLovelace(1_000_000).Build()
	require.NoError(b, err)
	const count = 1035
	txs := make([]lcommon.Transaction, count)
	for i := range txs {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash[24:], uint64(i+1))
		builder := mockledger.NewTransactionBuilder().WithId(hash).WithOutputs(output).WithValid(true)
		inputHash := make([]byte, 32)
		inputHash[0] = 0xee
		if i > 0 {
			inputHash = txs[i-1].Hash().Bytes()
		}
		input, err := mockledger.NewSimpleTransactionInput(inputHash, 0)
		require.NoError(b, err)
		builder.WithInputs(input)
		txs[i], err = builder.Build()
		require.NoError(b, err)
	}
	b.ReportAllocs()
	for b.Loop() {
		txn := store.Transaction(b.Context())
		acc := store.NewLeiosClosureBatchAccumulator()
		for i, tx := range txs {
			if err := store.SetTransactionLeiosClosureBatched(tx, ocommon.Point{Slot: 100, Hash: []byte("ranking-block")}, uint32(i), nil, true, nil, acc, txn, 0); err != nil {
				b.Fatal(err)
			}
		}
		if err := store.FlushBatch(acc, txn); err != nil {
			b.Fatal(err)
		}
		if err := txn.Rollback(); err != nil {
			b.Fatal(err)
		}
		acc.Reset()
	}
}

func TestLeiosAPIBatchDefersRepairedOutputAddresses(t *testing.T) {
	t.Parallel()
	for _, flushed := range []bool{false, true} {
		name := "queued"
		if flushed {
			name = "flushed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newAPIModeSQLiteStore(t, nil)
			txn := store.Transaction(t.Context())
			t.Cleanup(func() { _ = txn.Rollback() })
			acc := store.NewLeiosClosureBatchAccumulator()
			defer acc.Reset()
			tx, point, _, _ := assetOutputTx(t, 85)
			require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, nil, acc, txn, 0))
			if flushed {
				require.NoError(t, store.FlushBatch(acc, txn))
			}
			db, ctx, err := store.dbFromTxn(txn)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "DELETE FROM utxo_address")
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "DELETE FROM asset")
			require.NoError(t, err)
			require.NoError(t, store.SetTransactionLeiosClosureBatched(tx, point, 0, nil, true, nil, acc, txn, 0))
			require.Zero(t, tableCounts(t, store, txn, "utxo_address")["utxo_address"], "repaired output addresses must wait for the closure flush")
			stored, err := store.GetUtxoIncludingSpent(tx.Hash().Bytes(), 0, txn)
			require.NoError(t, err)
			require.NotNil(t, stored)
			require.Len(t, stored.Assets, 1, "asset repair must be immediately visible")
			for _, entry := range acc.(*transactionBatchAccumulator).rows.entries {
				if entry.shape.table == "utxo_address" {
					require.Len(t, entry.rows, 1, "one queued upsert per repaired output ID")
				}
			}
			require.NoError(t, store.FlushBatch(acc, txn))
			require.Equal(t, 1, tableCounts(t, store, txn, "utxo_address")["utxo_address"])
		})
	}
}
