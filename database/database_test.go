// Copyright 2025 Blink Labs Software
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

package database_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type TestTable struct {
	gorm.Model
}

var dbConfig = &database.Config{
	BlobCacheSize: 1 << 20,
	Logger:        nil,
	PromRegistry:  nil,
	DataDir:       "",
}

// TestInMemorySqliteMultipleTransaction tests that our sqlite connection allows multiple
// concurrent transactions when using in-memory mode. This requires special URI flags, and
// this is mostly making sure that we don't lose them
func TestInMemorySqliteMultipleTransaction(t *testing.T) {
	var db *database.Database
	doQuery := func(sleep time.Duration) error {
		txn := db.Metadata().Transaction()
		if result := txn.First(&TestTable{}); result.Error != nil {
			return result.Error
		}
		time.Sleep(sleep)
		if result := txn.Commit(); result.Error != nil {
			return result.Error
		}
		return nil
	}
	db, err := database.New(dbConfig)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := db.Metadata().DB().AutoMigrate(&TestTable{}); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if result := db.Metadata().DB().Create(&TestTable{}); result.Error != nil {
		t.Fatalf("unexpected error: %s", result.Error)
	}
	// The linter calls us on the lack of error checking, but it's a goroutine...
	//nolint:errcheck
	go doQuery(5 * time.Second)
	time.Sleep(1 * time.Second)
	if err := doQuery(0); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
}

func TestUtxosByAddressCollateralReturnFlag(t *testing.T) {
	// Test that UtxosByAddress properly returns UTXOs with the IsCollateralReturn flag set
	// This tests the actual database query and mapping logic, not just copy operations

	// Setup database
	db, err := database.New(dbConfig)
	require.NoError(t, err)
	defer db.Close()

	// Test data - use 28-byte hashes for address parts
	paymentKeyBytes := make([]byte, 28)
	copy(
		paymentKeyBytes,
		[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A},
	)
	stakingKeyBytes := make([]byte, 28)
	copy(
		stakingKeyBytes,
		[]byte{0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1A},
	)

	// Create hashes from the key bytes
	paymentKeyHash := lcommon.NewBlake2b224(paymentKeyBytes)
	stakingKeyHash := lcommon.NewBlake2b224(stakingKeyBytes)

	// Create a test address that matches our UTXO
	testAddr, err := lcommon.NewAddressFromParts(
		0x00, // Shelley address type
		0x00, // Mainnet network ID
		paymentKeyBytes,
		stakingKeyBytes,
	)
	require.NoError(t, err)

	// Insert a UTXO directly into the metadata store
	utxo := models.Utxo{
		TxId:               []byte{0x10, 0x11, 0x12, 0x13},
		OutputIdx:          0,
		AddedSlot:          types.Uint64{Val: 1000},
		DeletedSlot:        types.Uint64{Val: 0},
		PaymentKey:         paymentKeyHash.Bytes(),
		StakingKey:         stakingKeyHash.Bytes(),
		Amount:             types.Uint64{Val: 1000000},
		Assets:             nil,
		IsCollateralReturn: true, // This flag should be preserved
	}

	// Insert the UTXO using the metadata store
	txn := db.Metadata().Transaction()
	err = txn.Create(&utxo).Error
	require.NoError(t, err)
	err = txn.Commit().Error
	require.NoError(t, err)

	// Query UTXOs by address using the metadata API (this tests the core query logic)
	metadataUtxos, err := db.Metadata().GetUtxosByAddress(testAddr, nil)
	require.NoError(t, err)
	require.Len(t, metadataUtxos, 1, "Metadata should return exactly one UTXO")

	// Verify the IsCollateralReturn flag is properly preserved at the metadata level
	assert.True(
		t,
		metadataUtxos[0].IsCollateralReturn,
		"IsCollateralReturn flag should be true in metadata result",
	)
	assert.Equal(t, utxo.TxId, metadataUtxos[0].TxId)
	assert.Equal(t, utxo.OutputIdx, metadataUtxos[0].OutputIdx)
	assert.Equal(t, utxo.PaymentKey, metadataUtxos[0].PaymentKey)
	assert.Equal(t, utxo.StakingKey, metadataUtxos[0].StakingKey)
	assert.Equal(t, utxo.Amount.Val, metadataUtxos[0].Amount.Val)
	assert.Equal(t, utxo.Assets, metadataUtxos[0].Assets)
}
