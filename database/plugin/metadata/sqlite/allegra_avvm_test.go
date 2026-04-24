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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Shelley→Allegra HARDFORK rule (pv3): cardano-ledger
// Allegra/Translation.hs returnRedeemAddrsToReserves. See
// database/plugin/metadata/store.go for the interface contract.

// seedAvvmFixtures inserts one UTxO of each Byron inner-address type
// (pubkey, script, redeem), one modern/non-Byron UTxO, and one
// already-spent redeem UTxO. Only the live redeem UTxO should be
// removed by RemoveByronAvvmUtxos.
func seedAvvmFixtures(t *testing.T, store *MetadataStoreSqlite) {
	t.Helper()
	rows := []models.Utxo{
		{
			TxId:             bytes.Repeat([]byte{0x01}, 32),
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xA1}, 28),
			Amount:           types.Uint64(1_000_000),
			AddedSlot:        100,
			ByronAddressType: uint8(lcommon.ByronAddressTypeRedeem),
		},
		{
			TxId:             bytes.Repeat([]byte{0x02}, 32),
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xA2}, 28),
			Amount:           types.Uint64(2_500_000),
			AddedSlot:        110,
			ByronAddressType: uint8(lcommon.ByronAddressTypeRedeem),
		},
		{
			TxId:             bytes.Repeat([]byte{0x03}, 32),
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xB1}, 28),
			Amount:           types.Uint64(9_999_999),
			AddedSlot:        120,
			ByronAddressType: uint8(lcommon.ByronAddressTypePubkey),
		},
		{
			TxId:             bytes.Repeat([]byte{0x04}, 32),
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xB2}, 28),
			Amount:           types.Uint64(8_888_888),
			AddedSlot:        130,
			ByronAddressType: uint8(lcommon.ByronAddressTypeScript),
		},
		{
			TxId:       bytes.Repeat([]byte{0x05}, 32),
			OutputIdx:  0,
			PaymentKey: bytes.Repeat([]byte{0xC1}, 28),
			StakingKey: bytes.Repeat([]byte{0xC2}, 28),
			Amount:     types.Uint64(7_777_777),
			AddedSlot:  140,
			// ByronAddressType == 0 (non-Byron; or Byron pubkey — both
			// are excluded from the redeem filter anyway).
		},
		{
			// Already-spent redeem: must not be touched.
			TxId:             bytes.Repeat([]byte{0x06}, 32),
			OutputIdx:        0,
			PaymentKey:       bytes.Repeat([]byte{0xD1}, 28),
			Amount:           types.Uint64(100),
			AddedSlot:        50,
			DeletedSlot:      200,
			ByronAddressType: uint8(lcommon.ByronAddressTypeRedeem),
		},
	}
	for i := range rows {
		require.NoError(t, store.DB().Create(&rows[i]).Error)
	}
}

// Exactly the two live redeem UTxOs get cleared; their value is summed;
// everything else is untouched.
func TestRemoveByronAvvmUtxos_ClearsLiveRedeemersOnly(t *testing.T) {
	store := setupTestDB(t)
	seedAvvmFixtures(t, store)

	const boundarySlot uint64 = 500_000
	count, total, err := store.RemoveByronAvvmUtxos(boundarySlot, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, uint64(3_500_000), total)

	// The two live redeem UTxOs (tx_id 0x01, 0x02) are now deleted at the
	// boundary slot.
	for _, id := range [][]byte{
		bytes.Repeat([]byte{0x01}, 32),
		bytes.Repeat([]byte{0x02}, 32),
	} {
		var r models.Utxo
		require.NoError(t, store.DB().
			Where("tx_id = ?", id).First(&r).Error)
		assert.Equal(t, boundarySlot, r.DeletedSlot,
			"live redeem UTxO must be deleted at the boundary slot")
	}

	// Non-redeem UTxOs stay live (anything with byron_address_type != 2).
	var live int64
	require.NoError(t, store.DB().
		Model(&models.Utxo{}).
		Where("byron_address_type != ?",
			uint8(lcommon.ByronAddressTypeRedeem),
		).
		Where("deleted_slot = 0").
		Count(&live).Error)
	assert.Equal(t, int64(3), live,
		"pubkey/script/non-Byron UTxOs must remain live")

	// The pre-existing spent redeem row stays at its original DeletedSlot.
	var preSpent models.Utxo
	require.NoError(t, store.DB().
		Where("tx_id = ?", bytes.Repeat([]byte{0x06}, 32)).
		First(&preSpent).Error)
	assert.Equal(t, uint64(200), preSpent.DeletedSlot,
		"already-spent UTxO must keep its prior DeletedSlot")
}

// Running the rule on a chain with no AVVM UTxOs is a no-op.
func TestRemoveByronAvvmUtxos_EmptyIsNoOp(t *testing.T) {
	store := setupTestDB(t)
	// One non-redeem UTxO only.
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:             bytes.Repeat([]byte{0xE1}, 32),
		OutputIdx:        0,
		PaymentKey:       bytes.Repeat([]byte{0xE2}, 28),
		Amount:           types.Uint64(1_234_567),
		AddedSlot:        100,
		ByronAddressType: uint8(lcommon.ByronAddressTypePubkey),
	}).Error)

	count, total, err := store.RemoveByronAvvmUtxos(999_999, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.Equal(t, uint64(0), total)

	var live int64
	require.NoError(t, store.DB().
		Model(&models.Utxo{}).
		Where("deleted_slot = 0").
		Count(&live).Error)
	assert.Equal(t, int64(1), live)
}

// A second call after the boundary must find nothing to do.
func TestRemoveByronAvvmUtxos_Idempotent(t *testing.T) {
	store := setupTestDB(t)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:             bytes.Repeat([]byte{0xF1}, 32),
		OutputIdx:        0,
		PaymentKey:       bytes.Repeat([]byte{0xF2}, 28),
		Amount:           types.Uint64(42),
		AddedSlot:        100,
		ByronAddressType: uint8(lcommon.ByronAddressTypeRedeem),
	}).Error)

	count, total, err := store.RemoveByronAvvmUtxos(500_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, uint64(42), total)

	count, total, err = store.RemoveByronAvvmUtxos(600_000, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	assert.Equal(t, uint64(0), total,
		"re-running after the boundary must be a no-op")
}
