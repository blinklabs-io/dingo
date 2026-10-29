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

	"github.com/blinklabs-io/dingo/database/types"
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
