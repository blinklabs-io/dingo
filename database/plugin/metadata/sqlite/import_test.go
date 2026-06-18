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
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

type importSQLRecorder struct {
	mu         sync.Mutex
	statements []string
}

func (r *importSQLRecorder) LogMode(
	gormlogger.LogLevel,
) gormlogger.Interface {
	return r
}

func (*importSQLRecorder) Info(
	context.Context,
	string,
	...any,
) {
}

func (*importSQLRecorder) Warn(
	context.Context,
	string,
	...any,
) {
}

func (*importSQLRecorder) Error(
	context.Context,
	string,
	...any,
) {
}

func (r *importSQLRecorder) Trace(
	_ context.Context,
	_ time.Time,
	fc func() (string, int64),
	_ error,
) {
	sql, _ := fc()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.statements = append(r.statements, sql)
}

func (r *importSQLRecorder) containsUpdate(table string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, stmt := range r.statements {
		normalized := strings.ToUpper(stmt)
		if strings.Contains(normalized, "UPDATE") &&
			strings.Contains(normalized, strings.ToUpper(table)) {
			return true
		}
	}
	return false
}

func TestImportUtxosAddsMissingAssetsForExistingUtxo(t *testing.T) {
	t.Parallel()
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

func TestImportUtxosSkipsHydrationForFreshInsert(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	txID := bytes.Repeat([]byte{0x41}, 32)
	tx := models.Transaction{
		Hash:       txID,
		Slot:       100,
		BlockIndex: 1,
		Valid:      true,
	}
	require.NoError(t, store.DB().Create(&tx).Error)

	recorder := &importSQLRecorder{}
	db := store.DB().Session(&gorm.Session{
		Logger: recorder,
	})
	require.NoError(t, importUtxosWithDB(db, []models.Utxo{{
		TransactionID: &tx.ID,
		TxId:          txID,
		OutputIdx:     0,
		AddedSlot:     tx.Slot,
		Amount:        10,
	}}))

	assert.False(t, recorder.containsUpdate("utxo"))
}

func TestImportUtxosHydratesSnapshotProvenance(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	txID := bytes.Repeat([]byte{0x51}, 32)
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
		AddedSlot: 200,
		Amount:    10,
		Assets:    []models.Asset{assetA},
	}}, nil))

	tx := models.Transaction{
		Hash:       txID,
		Slot:       100,
		BlockIndex: 2,
		Valid:      true,
	}
	require.NoError(t, store.DB().Create(&tx).Error)

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TransactionID: &tx.ID,
		TxId:          txID,
		OutputIdx:     0,
		AddedSlot:     tx.Slot,
		Amount:        10,
		Assets:        []models.Asset{assetA, assetB},
	}}, nil))

	var got models.Utxo
	require.NoError(
		t,
		store.DB().
			Where("tx_id = ? AND output_idx = ?", txID, 0).
			First(&got).Error,
	)
	require.NotNil(t, got.TransactionID)
	assert.Equal(t, tx.ID, *got.TransactionID)
	assert.Equal(t, tx.Slot, got.AddedSlot)

	var assets []models.Asset
	require.NoError(
		t,
		store.DB().
			Where("utxo_id = ?", got.ID).
			Order("name ASC").
			Find(&assets).Error,
	)
	require.Len(t, assets, 2)
	assert.Equal(t, []byte("asset-a"), assets[0].Name)
	assert.Equal(t, []byte("asset-b"), assets[1].Name)

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 300,
		Amount:    10,
		Assets:    []models.Asset{assetA},
	}}, nil))

	require.NoError(
		t,
		store.DB().
			Where("tx_id = ? AND output_idx = ?", txID, 0).
			First(&got).Error,
	)
	require.NotNil(t, got.TransactionID)
	assert.Equal(t, tx.ID, *got.TransactionID)
	assert.Equal(t, tx.Slot, got.AddedSlot)
}

func TestImportUtxosHydratesSnapshotCollateralReturnProvenance(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	txID := bytes.Repeat([]byte{0x61}, 32)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 400,
		Amount:    10,
	}}, nil))

	tx := models.Transaction{
		Hash:       txID,
		Slot:       250,
		BlockIndex: 1,
		Valid:      false,
	}
	require.NoError(t, store.DB().Create(&tx).Error)

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		CollateralReturnForTxID: &tx.ID,
		TxId:                    txID,
		OutputIdx:               0,
		AddedSlot:               tx.Slot,
		Amount:                  10,
	}}, nil))

	var got models.Utxo
	require.NoError(
		t,
		store.DB().
			Where("tx_id = ? AND output_idx = ?", txID, 0).
			First(&got).Error,
	)
	require.NotNil(t, got.CollateralReturnForTxID)
	assert.Equal(t, tx.ID, *got.CollateralReturnForTxID)
	assert.Equal(t, tx.Slot, got.AddedSlot)
}
