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
