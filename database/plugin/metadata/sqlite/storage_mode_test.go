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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// setupTestDBWithMode creates and initializes a test SQLite database with the
// specified storage mode.
func setupTestDBWithMode(t *testing.T, mode string) *MetadataStoreSqlite {
	t.Helper()
	store, err := NewWithOptions(WithStorageMode(mode))
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

// newTestWitnessTransaction builds a mock transaction with a VKey witness
// using ouroboros-mock builders, for exercising the witness storage guards.
func newTestWitnessTransaction(
	hashSeed string,
) *mockledger.MockTransaction {
	ws := mockledger.NewMockTransactionWitnessSet().
		WithVkeyWitnesses(lcommon.VkeyWitness{
			Vkey:      make([]byte, 32),
			Signature: make([]byte, 64),
		})
	tx := mockledger.NewTransactionBuilder().
		WithWitnesses(ws)
	tx.WithId([]byte(hashSeed))
	return tx
}

func TestStorageMode_CoreSkipsWitnesses(t *testing.T) {
	store := setupTestDBWithMode(t, "core")

	tx := newTestWitnessTransaction(
		"core_test_hash_12345678901234567890" +
			"12345678901234567890",
	)
	point := ocommon.Point{
		Hash: []byte(
			"block_hash_1234567890123456789012",
		),
		Slot: 100,
	}

	err := store.SetTransaction(tx, point, 0, nil, nil)
	require.NoError(t, err)

	// Transaction record should still be stored
	var txCount int64
	store.DB().Model(&models.Transaction{}).Count(&txCount)
	assert.Equal(t, int64(1), txCount,
		"transaction record should be stored in core mode",
	)

	// Witnesses should NOT be stored in core mode
	var witnessCount int64
	store.DB().Model(&models.KeyWitness{}).Count(&witnessCount)
	assert.Equal(t, int64(0), witnessCount,
		"witnesses should not be stored in core mode",
	)
}

func TestStorageMode_APIStoresWitnesses(t *testing.T) {
	store := setupTestDBWithMode(t, "api")

	tx := newTestWitnessTransaction(
		"api_test_hash_123456789012345678901" +
			"2345678901234567890",
	)
	point := ocommon.Point{
		Hash: []byte(
			"block_hash_1234567890123456789012",
		),
		Slot: 200,
	}

	err := store.SetTransaction(tx, point, 0, nil, nil)
	require.NoError(t, err)

	// Transaction record should be stored
	var txCount int64
	store.DB().Model(&models.Transaction{}).Count(&txCount)
	assert.Equal(t, int64(1), txCount,
		"transaction record should be stored in api mode",
	)

	// Witnesses SHOULD be stored in API mode
	var witnessCount int64
	store.DB().Model(&models.KeyWitness{}).Count(&witnessCount)
	assert.Equal(t, int64(1), witnessCount,
		"vkey witness should be stored in api mode",
	)
}

func TestStorageMode_CoreSkipsDatumIndex(t *testing.T) {
	store := setupTestDBWithMode(t, "core")

	tx := newTestWitnessTransaction(
		"datum_core_hash_1234567890123456789" +
			"012345678901234567890",
	)
	point := ocommon.Point{
		Hash: []byte(
			"block_hash_1234567890123456789012",
		),
		Slot: 300,
	}

	err := store.SetTransaction(tx, point, 0, nil, nil)
	require.NoError(t, err)

	// Datum index table should be empty in core mode
	var datumCount int64
	store.DB().Model(&models.Datum{}).Count(&datumCount)
	assert.Equal(t, int64(0), datumCount,
		"datum index should not be stored in core mode",
	)
}

func TestStorageMode_DefaultIsCore(t *testing.T) {
	// A store created without specifying storage mode should default to "core"
	store, err := NewWithOptions()
	require.NoError(t, err)
	assert.Equal(t, "core", store.storageMode,
		"default storage mode should be core",
	)
}

func TestStorageMode_InvalidRejected(t *testing.T) {
	_, err := NewWithOptions(WithStorageMode("invalid"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid storage mode")
}

func TestStorageMode_CaseNormalized(t *testing.T) {
	store, err := NewWithOptions(WithStorageMode("API"))
	require.NoError(t, err)
	assert.Equal(t, "api", store.storageMode, "storage mode should be lowercased")
}
