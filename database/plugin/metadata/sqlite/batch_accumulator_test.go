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

	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchAccumulator_AddAndReset(t *testing.T) {
	t.Parallel()
	ba := NewBatchAccumulator()
	require.NotNil(t, ba)

	// --- Add one record of each type ---

	ba.AddKeyWitness(models.KeyWitness{
		Vkey:          []byte{0x01},
		Signature:     []byte{0x02},
		TransactionID: 1,
		Type:          0,
	})
	ba.AddWitnessScript(models.WitnessScripts{
		ScriptHash:    []byte{0x03},
		TransactionID: 1,
		Type:          1,
	})
	ba.AddScript(models.Script{
		Hash:        []byte{0x04},
		Content:     []byte{0x05},
		CreatedSlot: 100,
		Type:        2,
	})
	ba.AddPlutusData(models.PlutusData{
		Data:          []byte{0x06},
		TransactionID: 2,
	})
	ba.AddRedeemer(models.Redeemer{
		Data:          []byte{0x07},
		TransactionID: 2,
		Index:         0,
		Tag:           1,
	})
	ba.AddAddressTx(models.AddressTransaction{
		PaymentKey:    []byte{0x08},
		TransactionID: 3,
		Slot:          200,
	})
	ba.AddUtxoOutput(models.Utxo{
		TxId:      []byte{0x09},
		OutputIdx: 0,
		AddedSlot: 200,
	})
	ba.AddUtxoSpend(utxoSpend{
		TxId:          []byte{0x0a},
		OutputIdx:     1,
		Slot:          200,
		SpentByTxHash: []byte{0x0b},
	})
	ba.AddCollateralReturn(models.Utxo{
		TxId:      []byte{0x0c},
		OutputIdx: 0,
		AddedSlot: 200,
	})
	ba.AddDeleteTxID(42)

	// --- Verify counts ---

	assert.Len(t, ba.KeyWitnesses, 1)
	assert.Len(t, ba.WitnessScripts, 1)
	assert.Len(t, ba.Scripts, 1)
	assert.Len(t, ba.PlutusData, 1)
	assert.Len(t, ba.Redeemers, 1)
	assert.Len(t, ba.AddressTxs, 1)
	assert.Len(t, ba.UtxoOutputs, 1)
	assert.Len(t, ba.UtxoSpends, 1)
	assert.Len(t, ba.CollateralRets, 1)
	assert.Len(t, ba.DeleteTxIDs, 1)

	// --- Spot-check values ---

	assert.Equal(t, []byte{0x01}, ba.KeyWitnesses[0].Vkey)
	assert.Equal(t, uint32(1), ba.UtxoSpends[0].OutputIdx)
	assert.Equal(t, uint(42), ba.DeleteTxIDs[0])

	keyWitnessesCap := cap(ba.KeyWitnesses)
	witnessScriptsCap := cap(ba.WitnessScripts)
	scriptsCap := cap(ba.Scripts)
	plutusDataCap := cap(ba.PlutusData)
	redeemersCap := cap(ba.Redeemers)
	addressTxsCap := cap(ba.AddressTxs)
	utxoOutputsCap := cap(ba.UtxoOutputs)
	utxoSpendsCap := cap(ba.UtxoSpends)
	collateralRetsCap := cap(ba.CollateralRets)
	deleteTxIDsCap := cap(ba.DeleteTxIDs)

	// --- Reset and verify all slices are empty ---

	ba.Reset()

	assert.Empty(t, ba.KeyWitnesses)
	assert.Empty(t, ba.WitnessScripts)
	assert.Empty(t, ba.Scripts)
	assert.Empty(t, ba.PlutusData)
	assert.Empty(t, ba.Redeemers)
	assert.Empty(t, ba.AddressTxs)
	assert.Empty(t, ba.UtxoOutputs)
	assert.Empty(t, ba.UtxoSpends)
	assert.Empty(t, ba.CollateralRets)
	assert.Empty(t, ba.DeleteTxIDs)

	// --- Verify backing arrays are reused ---

	assert.Equal(t, keyWitnessesCap, cap(ba.KeyWitnesses))
	assert.Equal(t, witnessScriptsCap, cap(ba.WitnessScripts))
	assert.Equal(t, scriptsCap, cap(ba.Scripts))
	assert.Equal(t, plutusDataCap, cap(ba.PlutusData))
	assert.Equal(t, redeemersCap, cap(ba.Redeemers))
	assert.Equal(t, addressTxsCap, cap(ba.AddressTxs))
	assert.Equal(t, utxoOutputsCap, cap(ba.UtxoOutputs))
	assert.Equal(t, utxoSpendsCap, cap(ba.UtxoSpends))
	assert.Equal(t, collateralRetsCap, cap(ba.CollateralRets))
	assert.Equal(t, deleteTxIDsCap, cap(ba.DeleteTxIDs))

	// --- Verify re-add after reset works ---

	ba.AddKeyWitness(models.KeyWitness{
		Vkey:          []byte{0xff},
		TransactionID: 99,
	})
	assert.Len(t, ba.KeyWitnesses, 1)
	assert.Equal(t, []byte{0xff}, ba.KeyWitnesses[0].Vkey)
}

func TestBatchAccumulator_InFlightProducerIndex(t *testing.T) {
	t.Parallel()
	ba := NewBatchAccumulator()

	producerTx := bytes.Repeat([]byte{0xaa}, 32)
	paymentKey := []byte{0x11}
	stakingKey := []byte{0x22}
	ba.AddUtxoOutput(models.Utxo{
		TxId:       producerTx,
		OutputIdx:  3,
		PaymentKey: paymentKey,
		StakingKey: stakingKey,
		AddedSlot:  100,
	})

	colRetTx := bytes.Repeat([]byte{0xcc}, 32)
	ba.AddCollateralReturn(models.Utxo{
		TxId:       colRetTx,
		OutputIdx:  0,
		PaymentKey: []byte{0x33},
		StakingKey: []byte{0x44},
		AddedSlot:  100,
	})

	// Produced output is resolvable with its address keys.
	assert.True(t, ba.HasInFlightProducer(producerTx, 3))
	keys, ok := ba.InFlightAddressKeys(producerTx, 3)
	require.True(t, ok)
	assert.Equal(t, producerTx, keys.TxId)
	assert.Equal(t, uint32(3), keys.OutputIdx)
	assert.Equal(t, paymentKey, keys.PaymentKey)
	assert.Equal(t, stakingKey, keys.StakingKey)

	// Collateral returns are indexed too.
	assert.True(t, ba.HasInFlightProducer(colRetTx, 0))

	// Misses: wrong index, wrong tx id.
	assert.False(t, ba.HasInFlightProducer(producerTx, 4))
	assert.False(t, ba.HasInFlightProducer(bytes.Repeat([]byte{0x99}, 32), 3))
	_, ok = ba.InFlightAddressKeys(producerTx, 4)
	assert.False(t, ok)

	// Reset clears the index along with the slices.
	ba.Reset()
	assert.False(t, ba.HasInFlightProducer(producerTx, 3))
	assert.False(t, ba.HasInFlightProducer(colRetTx, 0))

	// The index is reusable after reset.
	ba.AddUtxoOutput(models.Utxo{TxId: producerTx, OutputIdx: 3})
	assert.True(t, ba.HasInFlightProducer(producerTx, 3))
}

func TestBatchAccumulator_MergeFromIndex(t *testing.T) {
	t.Parallel()
	dst := NewBatchAccumulator()
	src := NewBatchAccumulator()

	dstTx := bytes.Repeat([]byte{0x01}, 32)
	dst.AddUtxoOutput(
		models.Utxo{TxId: dstTx, OutputIdx: 0, PaymentKey: []byte{0x10}},
	)

	srcTx := bytes.Repeat([]byte{0x02}, 32)
	src.AddUtxoOutput(
		models.Utxo{TxId: srcTx, OutputIdx: 1, PaymentKey: []byte{0x20}},
	)

	dst.MergeFrom(src)

	// Both the destination's own and the merged-in producers resolve.
	assert.True(t, dst.HasInFlightProducer(dstTx, 0))
	assert.True(t, dst.HasInFlightProducer(srcTx, 1))
	keys, ok := dst.InFlightAddressKeys(srcTx, 1)
	require.True(t, ok)
	assert.Equal(t, []byte{0x20}, keys.PaymentKey)
}

func TestFlushBatch_Witnesses(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	batch := NewBatchAccumulator()

	require.NoError(t, createSequentialTestTransactions(store.DB(), 5, 1))
	for i := uint(1); i <= 5; i++ {
		batch.AddKeyWitness(models.KeyWitness{
			TransactionID: i,
			Type:          models.KeyWitnessTypeVkey,
			Vkey:          []byte{byte(i)},
			Signature:     []byte{byte(i + 10)},
		})
	}

	require.NoError(t, store.FlushBatch(batch, nil))

	var count int64
	require.NoError(
		t,
		store.DB().Model(&models.KeyWitness{}).Count(&count).Error,
	)
	assert.Equal(t, int64(5), count)
}

func TestFlushBatch_UtxoOutputsAndSpends(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	batch := NewBatchAccumulator()

	// Use exactly 32-byte hashes to match the column's size:32 annotation.
	txID := bytes.Repeat([]byte{0xAA}, 32)
	spentBy := bytes.Repeat([]byte{0xBB}, 32)

	// spent_at_tx_id references transactions.hash (FK enforced in SQLite).
	// Create the spending transaction first so the FK constraint is satisfied.
	spenderTx := models.Transaction{Hash: spentBy, Slot: 120, Valid: true}
	require.NoError(t, store.DB().Create(&spenderTx).Error)

	batch.AddUtxoOutput(models.Utxo{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    10,
	})
	batch.AddUtxoOutput(models.Utxo{
		TxId:      txID,
		OutputIdx: 1,
		AddedSlot: 100,
		Amount:    20,
	})
	batch.AddUtxoSpend(utxoSpend{
		TxId:          txID,
		OutputIdx:     1,
		Slot:          120,
		SpentByTxHash: spentBy,
	})

	require.NoError(t, store.FlushBatch(batch, nil))

	var outputs []models.Utxo
	require.NoError(
		t,
		store.DB().Order("output_idx ASC").
			Where("tx_id = ?", txID).
			Find(&outputs).Error,
	)
	require.Len(t, outputs, 2)
	assert.Equal(t, uint64(0), outputs[0].DeletedSlot)
	assert.Empty(t, outputs[0].SpentAtTxId)
	assert.Equal(t, uint64(120), outputs[1].DeletedSlot)
	assert.Equal(t, spentBy, []byte(outputs[1].SpentAtTxId))
}

func TestFlushBatch_MultipleSpends(t *testing.T) {
	t.Parallel()
	// This test specifically guards against the args-ordering bug in
	// batchSpendUtxos: with N>1 spends the SQL has three separate CASE
	// sections whose bindings must be grouped (all deletedSlot args, then
	// all spentAt args, then all WHERE args), not interleaved per-spend.
	store := setupTestDBWithMode(t, "api")
	batch := NewBatchAccumulator()

	txID1 := bytes.Repeat([]byte{0x11}, 32)
	txID2 := bytes.Repeat([]byte{0x22}, 32)
	spentBy1 := bytes.Repeat([]byte{0xA1}, 32)
	spentBy2 := bytes.Repeat([]byte{0xA2}, 32)

	// Pre-insert spending transactions to satisfy the FK constraint.
	require.NoError(
		t,
		store.DB().Create(
			&models.Transaction{Hash: spentBy1, Slot: 200, Valid: true},
		).Error,
	)
	require.NoError(
		t,
		store.DB().Create(
			&models.Transaction{Hash: spentBy2, Slot: 201, Valid: true},
		).Error,
	)

	// Two outputs from two different tx hashes.
	batch.AddUtxoOutput(
		models.Utxo{TxId: txID1, OutputIdx: 0, AddedSlot: 100, Amount: 1},
	)
	batch.AddUtxoOutput(
		models.Utxo{TxId: txID2, OutputIdx: 0, AddedSlot: 100, Amount: 2},
	)
	// Spend both, with deliberately different slots and spentBy hashes.
	batch.AddUtxoSpend(utxoSpend{
		TxId:          txID1,
		OutputIdx:     0,
		Slot:          200,
		SpentByTxHash: spentBy1,
	})
	batch.AddUtxoSpend(utxoSpend{
		TxId:          txID2,
		OutputIdx:     0,
		Slot:          201,
		SpentByTxHash: spentBy2,
	})

	require.NoError(t, store.FlushBatch(batch, nil))

	var u1, u2 models.Utxo
	require.NoError(
		t,
		store.DB().Where("tx_id = ? AND output_idx = 0", txID1).First(&u1).Error,
	)
	require.NoError(
		t,
		store.DB().Where("tx_id = ? AND output_idx = 0", txID2).First(&u2).Error,
	)

	// Each UTxO must carry its own slot and spentBy — wrong if args were interleaved.
	assert.Equal(t, uint64(200), u1.DeletedSlot)
	assert.Equal(t, spentBy1, []byte(u1.SpentAtTxId))
	assert.Equal(t, uint64(201), u2.DeletedSlot)
	assert.Equal(t, spentBy2, []byte(u2.SpentAtTxId))
}

func TestFlushBatch_Idempotent(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")
	batch := NewBatchAccumulator()

	txID := uint(77)
	require.NoError(t, createTestTransaction(store.DB(), txID, 300))
	batch.AddDeleteTxID(txID)
	batch.AddKeyWitness(models.KeyWitness{
		TransactionID: txID,
		Type:          models.KeyWitnessTypeVkey,
		Vkey:          []byte{0xaa},
		Signature:     []byte{0xbb},
	})
	batch.AddAddressTx(models.AddressTransaction{
		TransactionID: txID,
		PaymentKey:    []byte{0x01, 0x02},
		Slot:          300,
		TxIndex:       4,
	})
	batch.AddScript(models.Script{
		Hash:        bytes.Repeat([]byte{0xCC}, 28),
		Content:     []byte{0x10, 0x11},
		CreatedSlot: 300,
		Type:        1,
	})
	batch.AddUtxoOutput(models.Utxo{
		TxId:      bytes.Repeat([]byte{0xDD}, 32),
		OutputIdx: 0,
		AddedSlot: 300,
		Amount:    50,
	})

	require.NoError(t, store.FlushBatch(batch, nil))
	require.NoError(t, store.FlushBatch(batch, nil))

	var witnessCount int64
	require.NoError(
		t,
		store.DB().Model(&models.KeyWitness{}).Count(&witnessCount).Error,
	)
	assert.Equal(t, int64(1), witnessCount)

	var addrCount int64
	require.NoError(
		t,
		store.DB().Model(&models.AddressTransaction{}).Count(&addrCount).Error,
	)
	assert.Equal(t, int64(1), addrCount)

	var scriptCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Script{}).Count(&scriptCount).Error,
	)
	assert.Equal(t, int64(1), scriptCount)

	var utxoCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Utxo{}).Count(&utxoCount).Error,
	)
	assert.Equal(t, int64(1), utxoCount)
}

// TestSetTransactionBatched_AccumulatesCorrectly verifies that
// SetTransactionBatched writes the transaction record to the DB immediately
// (so it can be queried) and routes key witnesses and address-transaction rows
// to the accumulator rather than the database.
// A subsequent FlushBatch call must materialise those rows correctly.
func TestSetTransactionBatched_AccumulatesCorrectly(t *testing.T) {
	t.Parallel()
	store := setupTestDBWithMode(t, "api")

	// ------------------------------------------------------------------ //
	// Build a mock transaction with one VKey witness.                    //
	// (uses the same helper pattern as TestStorageMode_APIStoresWitnesses)//
	// ------------------------------------------------------------------ //
	tx := newTestWitnessTransaction(
		"batched_test_hash_1234567890123456789012345678901234567890",
	)
	point := ocommon.Point{
		Hash: []byte("block_hash_batched_12345678901234"),
		Slot: 999,
	}
	acc := NewBatchAccumulator()
	var stats dbtypes.BackfillHotPathStats
	acc.SetBackfillStats(&stats)

	require.NoError(
		t,
		store.SetTransactionBatched(tx, point, 0, nil, acc, nil),
	)

	// ------------------------------------------------------------------ //
	// 1. Transaction record must exist in the DB immediately.             //
	// ------------------------------------------------------------------ //
	var txCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Transaction{}).Count(&txCount).Error,
	)
	assert.Equal(t, int64(1), txCount, "transaction record must be written immediately")

	// ------------------------------------------------------------------ //
	// 2. Key witness must be in the accumulator, NOT yet in the DB.      //
	// ------------------------------------------------------------------ //
	assert.Len(t, acc.KeyWitnesses, 1, "one vkey witness expected in accumulator")
	assert.Equal(t, models.KeyWitnessTypeVkey, acc.KeyWitnesses[0].Type)
	assert.Equal(t, uint64(1), stats.Witnesses)

	var witnessCount int64
	require.NoError(
		t,
		store.DB().Model(&models.KeyWitness{}).Count(&witnessCount).Error,
	)
	assert.Equal(t, int64(0), witnessCount, "witness must not yet be flushed to DB")

	// ------------------------------------------------------------------ //
	// 3. FlushBatch must write the deferred rows to the DB.              //
	// ------------------------------------------------------------------ //
	require.NoError(t, store.FlushBatch(acc, nil))

	require.NoError(
		t,
		store.DB().Model(&models.KeyWitness{}).Count(&witnessCount).Error,
	)
	assert.Equal(t, int64(1), witnessCount, "witness must be present after flush")
}
