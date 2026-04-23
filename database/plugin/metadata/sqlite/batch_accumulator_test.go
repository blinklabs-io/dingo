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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchAccumulator_AddAndReset(t *testing.T) {
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

	// --- Verify backing arrays are reused (cap > 0) ---

	assert.Greater(t, cap(ba.KeyWitnesses), 0)
	assert.Greater(t, cap(ba.WitnessScripts), 0)
	assert.Greater(t, cap(ba.Scripts), 0)
	assert.Greater(t, cap(ba.PlutusData), 0)
	assert.Greater(t, cap(ba.Redeemers), 0)
	assert.Greater(t, cap(ba.AddressTxs), 0)
	assert.Greater(t, cap(ba.UtxoOutputs), 0)
	assert.Greater(t, cap(ba.UtxoSpends), 0)
	assert.Greater(t, cap(ba.CollateralRets), 0)
	assert.Greater(t, cap(ba.DeleteTxIDs), 0)

	// --- Verify re-add after reset works ---

	ba.AddKeyWitness(models.KeyWitness{
		Vkey:          []byte{0xff},
		TransactionID: 99,
	})
	assert.Len(t, ba.KeyWitnesses, 1)
	assert.Equal(t, []byte{0xff}, ba.KeyWitnesses[0].Vkey)
}

func TestFlushBatch_Witnesses(t *testing.T) {
	store := setupTestDBWithMode(t, "api")
	batch := NewBatchAccumulator()

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
	assert.Equal(t, spentBy, outputs[1].SpentAtTxId)
}

func TestFlushBatch_Idempotent(t *testing.T) {
	store := setupTestDBWithMode(t, "api")
	batch := NewBatchAccumulator()

	txID := uint(77)
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
		Hash:        []byte("script-hash-000000000000000001"),
		Content:     []byte{0x10, 0x11},
		CreatedSlot: 300,
		Type:        1,
	})
	batch.AddUtxoOutput(models.Utxo{
		TxId:      []byte("utxo-hash-000000000000000000000001"),
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
