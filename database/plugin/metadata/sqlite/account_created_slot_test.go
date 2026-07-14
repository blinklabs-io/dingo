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
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestBackfillAccountCreatedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	registeredKey := createdSlotTestHash(0x71)
	genesisKey := createdSlotTestHash(0x72)
	require.NoError(t, db.Create(&[]models.Account{
		{StakingKey: registeredKey, CredentialTag: 0, Active: true},
		{StakingKey: genesisKey, CredentialTag: 0, Active: true},
	}).Error)
	seedCreatedSlotRegistration(t, db, 1, registeredKey, 500)
	seedCreatedSlotRegistration(t, db, 2, registeredKey, 300)

	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	var registered models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, registeredKey,
	).First(&registered).Error)
	assert.Equal(t, uint64(300), registered.CreatedSlot)

	var genesis models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, genesisKey,
	).First(&genesis).Error)
	assert.Zero(t, genesis.CreatedSlot)
}

func TestBackfillAccountCreatedSlotIsCheckpointGated(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	firstKey := createdSlotTestHash(0x94)
	require.NoError(t, db.Create(&models.Account{
		StakingKey: firstKey, CredentialTag: 0, Active: true,
	}).Error)
	seedCreatedSlotRegistration(t, db, 1, firstKey, 400)
	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	var first models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, firstKey,
	).First(&first).Error)
	assert.Equal(t, uint64(400), first.CreatedSlot)

	var checkpoint models.BackfillCheckpoint
	require.NoError(t, db.Where(
		"phase = ?", "account_created_slot",
	).First(&checkpoint).Error)
	assert.True(t, checkpoint.Completed)

	secondKey := createdSlotTestHash(0x95)
	require.NoError(t, db.Create(&models.Account{
		StakingKey: secondKey, CredentialTag: 0, Active: true,
	}).Error)
	seedCreatedSlotRegistration(t, db, 2, secondKey, 500)
	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	var second models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, secondKey,
	).First(&second).Error)
	assert.Zero(t, second.CreatedSlot)
}

func TestSaveAccountStampsCreatedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()
	key := createdSlotTestHash(0x81)

	account, err := store.getOrCreateAccount(0, key, nil)
	require.NoError(t, err)
	account.AddedSlot = 700
	require.NoError(t, saveAccount(account, db))

	var reloaded models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, key,
	).First(&reloaded).Error)
	require.Equal(t, uint64(700), reloaded.CreatedSlot)
	require.Equal(t, uint64(700), reloaded.AddedSlot)

	account, err = store.getOrCreateAccount(0, key, nil)
	require.NoError(t, err)
	account.AddedSlot = 900
	require.NoError(t, saveAccount(account, db))
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, key,
	).First(&reloaded).Error)
	assert.Equal(t, uint64(700), reloaded.CreatedSlot)
	assert.Equal(t, uint64(900), reloaded.AddedSlot)
}

func TestSetTransactionStampsCreatedSlotForPhantomDeregistration(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := createdSlotTestHash(0x93)
	certificate := &lcommon.StakeDeregistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(stakeKey),
		},
	}
	tx := &mockTransaction{
		hash:         lcommon.NewBlake2b256(createdSlotTestHash(0x01)),
		isValid:      true,
		certificates: []lcommon.Certificate{certificate},
	}
	const slot = uint64(4200)
	point := ocommon.Point{Slot: slot, Hash: createdSlotTestHash(0xbb)}
	require.NoError(t, store.SetTransaction(tx, point, 0, nil, nil))

	var account models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).First(&account).Error)
	assert.False(t, account.Active)
	assert.Equal(t, slot, account.CreatedSlot)
}

func createdSlotTestHash(fill byte) []byte {
	ret := make([]byte, 28)
	for i := range ret {
		ret[i] = fill
	}
	return ret
}

func seedCreatedSlotRegistration(
	t *testing.T,
	db *gorm.DB,
	id uint,
	stakingKey []byte,
	slot uint64,
) {
	t.Helper()
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	require.NoError(t, db.Create(&models.Transaction{
		ID: id, Hash: hash, Slot: slot,
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID: id, TransactionID: id, Slot: slot,
		CertType: uint(lcommon.CertificateTypeStakeRegistration),
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistration{
		ID: id, StakingKey: stakingKey, CredentialTag: 0,
		CertificateID: id, AddedSlot: slot,
	}).Error)
}
