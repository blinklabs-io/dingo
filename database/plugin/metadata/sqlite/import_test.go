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

func TestImportUtxosAddsMissingAssetsForExistingUtxo(t *testing.T) {
	store := setupTestDB(t)

	txID := bytes.Repeat([]byte{0x31}, 32)
	policyID := bytes.Repeat([]byte{0x42}, 28)
	assetA := models.Asset{
		PolicyId:    policyID,
		Name:        []byte("asset-a"),
		NameHex:     []byte("61737365742d61"),
		Fingerprint: []byte("fingerprint-a"),
		Amount:      1,
	}
	assetB := models.Asset{
		PolicyId:    policyID,
		Name:        []byte("asset-b"),
		NameHex:     []byte("61737365742d62"),
		Fingerprint: []byte("fingerprint-b"),
		Amount:      2,
	}

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    10,
		Assets:    []models.Asset{assetA},
	}}, nil))

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    10,
		Assets:    []models.Asset{assetA, assetB},
	}}, nil))

	var utxo models.Utxo
	require.NoError(
		t,
		store.DB().
			Where("tx_id = ? AND output_idx = ?", txID, 0).
			First(&utxo).Error,
	)

	var assets []models.Asset
	require.NoError(
		t,
		store.DB().
			Where("utxo_id = ?", utxo.ID).
			Order("name ASC").
			Find(&assets).Error,
	)
	require.Len(t, assets, 2)

	assert.Equal(t, []byte("asset-a"), assets[0].Name)
	assert.Equal(t, assetA.Amount, assets[0].Amount)
	assert.Equal(t, policyID, assets[0].PolicyId)
	assert.Equal(t, []byte("61737365742d61"), assets[0].NameHex)
	assert.Equal(t, []byte("fingerprint-a"), assets[0].Fingerprint)
	assert.Equal(t, []byte("asset-b"), assets[1].Name)
	assert.Equal(t, assetB.Amount, assets[1].Amount)
	assert.Equal(t, policyID, assets[1].PolicyId)
	assert.Equal(t, []byte("61737365742d62"), assets[1].NameHex)
	assert.Equal(t, []byte("fingerprint-b"), assets[1].Fingerprint)
}
