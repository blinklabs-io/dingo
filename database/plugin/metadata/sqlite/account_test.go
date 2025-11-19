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

package sqlite_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetAccount(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store and cast to concrete type
	metadataStore := db.Metadata().(*sqlite.MetadataStoreSqlite)

	// Test data
	stakeKey := []byte{0x01, 0x02, 0x03, 0x04}
	poolKeyHash := []byte{0x05, 0x06, 0x07, 0x08}
	drepKeyHash := []byte{0x09, 0x0A, 0x0B, 0x0C}
	slot := uint64(1000)

	// Set account for the first time
	err = metadataStore.SetAccount(
		stakeKey,
		poolKeyHash,
		drepKeyHash,
		slot,
		true,
		0, // certificateID
		nil,
	)
	require.NoError(t, err)

	// Verify account was created
	account, err := metadataStore.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, stakeKey, account.StakingKey)
	assert.Equal(t, poolKeyHash, account.Pool)
	assert.Equal(t, drepKeyHash, account.Drep)
	assert.Equal(t, slot, account.AddedSlot)
	assert.True(t, account.Active)

	// Get the ID of the first account
	firstAccountID := account.ID

	// Update the same account with different values and set inactive
	newPoolKeyHash := []byte{0x0D, 0x0E, 0x0F, 0x10}
	newDrepKeyHash := []byte{0x11, 0x12, 0x13, 0x14}
	newSlot := uint64(2000)

	err = metadataStore.SetAccount(
		stakeKey,
		newPoolKeyHash,
		newDrepKeyHash,
		newSlot,
		false,
		0, // certificateID
		nil,
	)
	require.NoError(t, err)

	// Verify account is no longer returned by GetAccount (since it's inactive)
	account, err = metadataStore.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	assert.Nil(
		t,
		account,
		"Inactive account should not be returned by GetAccount",
	)

	// But verify the record still exists in the database with updated values
	var accounts []models.Account
	err = metadataStore.DB().
		Where("staking_key = ?", stakeKey).
		Find(&accounts).
		Error
	require.NoError(t, err)
	assert.Len(t, accounts, 1, "Should have exactly one account record")
	assert.Equal(
		t,
		firstAccountID,
		accounts[0].ID,
		"Should be the same account record",
	)
	assert.Equal(t, stakeKey, accounts[0].StakingKey)
	assert.Equal(t, newPoolKeyHash, accounts[0].Pool)
	assert.Equal(t, newDrepKeyHash, accounts[0].Drep)
	assert.Equal(t, newSlot, accounts[0].AddedSlot)
	assert.False(t, accounts[0].Active)

	// Test GetAccount with includeInactive=true can find the inactive account
	inactiveAccount, err := metadataStore.GetAccount(stakeKey, true, nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		inactiveAccount,
		"Should be able to retrieve inactive account when includeInactive=true",
	)
	assert.Equal(t, firstAccountID, inactiveAccount.ID)
	assert.False(t, inactiveAccount.Active)
}

func TestSetAccountWithTransaction(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store and cast to concrete type
	metadataStore := db.Metadata().(*sqlite.MetadataStoreSqlite)

	// Test data
	stakeKey := []byte{0x21, 0x22, 0x23, 0x24}
	poolKeyHash := []byte{0x25, 0x26, 0x27, 0x28}
	drepKeyHash := []byte{0x29, 0x2A, 0x2B, 0x2C}
	slot := uint64(3000)

	// Start a transaction
	txn := metadataStore.DB().Begin()
	require.NoError(t, txn.Error)

	// Set account within transaction
	err = metadataStore.SetAccount(
		stakeKey,
		poolKeyHash,
		drepKeyHash,
		slot,
		true,
		0, // certificateID
		txn,
	)
	require.NoError(t, err)

	// Verify account exists within transaction
	account, err := metadataStore.GetAccount(stakeKey, false, txn)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, stakeKey, account.StakingKey)
	assert.Equal(t, poolKeyHash, account.Pool)
	assert.Equal(t, drepKeyHash, account.Drep)
	assert.Equal(t, slot, account.AddedSlot)
	assert.True(t, account.Active)

	// Rollback transaction
	txn.Rollback()

	// Verify account does not exist after rollback
	account, err = metadataStore.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	assert.Nil(
		t,
		account,
		"Account should not exist after transaction rollback",
	)
}

func TestDeleteCertificate(t *testing.T) {
	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store and cast to concrete type
	metadataStore := db.Metadata().(*sqlite.MetadataStoreSqlite)

	// Create a transaction and certificate manually for testing
	txn := metadataStore.DB().Begin()
	require.NoError(t, txn.Error)
	defer txn.Rollback()

	// Create a transaction record
	txHash := []byte{0x11, 0x22, 0x33}
	tmpTx := models.Transaction{
		Hash:       txHash,
		Type:       0,
		BlockHash:  []byte{0x01, 0x02, 0x03},
		BlockIndex: 0,
		Fee:        types.Uint64{Val: 1000},
		TTL:        types.Uint64{Val: 1000000},
	}
	err = txn.Create(&tmpTx).Error
	require.NoError(t, err)

	// Create unified certificate record
	unifiedCert := models.Certificate{
		BlockHash:     []byte{0x01, 0x02, 0x03},
		TransactionID: tmpTx.ID,
		CertificateID: 0,
		Slot:          1000,
		CertIndex:     0,
		CertType:      models.CertificateTypeStakeRegistration,
	}
	err = txn.Create(&unifiedCert).Error
	require.NoError(t, err)

	// Create stake registration record
	stakeReg := models.StakeRegistration{
		StakingKey:    []byte{0xAA, 0xBB, 0xCC},
		CertificateID: unifiedCert.ID,
		DepositAmount: types.Uint64{Val: 2000000},
		AddedSlot:     1000,
	}
	err = txn.Create(&stakeReg).Error
	require.NoError(t, err)

	// Update the unified certificate with the specific model ID
	unifiedCert.CertificateID = stakeReg.ID
	err = txn.Save(&unifiedCert).Error
	require.NoError(t, err)

	// Verify certificate exists
	var certCount int64
	err = txn.Model(&models.Certificate{}).
		Where("transaction_id = ? AND cert_index = ?", tmpTx.ID, 0).
		Count(&certCount).
		Error
	require.NoError(t, err)
	assert.Equal(t, int64(1), certCount, "Certificate should exist")

	// Verify stake registration exists
	var regCount int64
	err = txn.Model(&models.StakeRegistration{}).Count(&regCount).Error
	require.NoError(t, err)
	assert.Equal(t, int64(1), regCount, "Stake registration should exist")

	// Delete certificate
	err = metadataStore.DeleteCertificate(tmpTx.ID, 0, txn)
	require.NoError(t, err)

	// Verify certificate is deleted
	err = txn.Model(&models.Certificate{}).
		Where("transaction_id = ? AND cert_index = ?", tmpTx.ID, 0).
		Count(&certCount).
		Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), certCount, "Certificate should be deleted")

	// Verify stake registration is deleted
	err = txn.Model(&models.StakeRegistration{}).Count(&regCount).Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), regCount, "Stake registration should be deleted")

	// Test deleting non-existent certificate (should not error)
	err = metadataStore.DeleteCertificate(99999, 0, txn)
	require.NoError(
		t,
		err,
		"Deleting non-existent certificate should not error",
	)
}

func TestSetAccountWithCertificateLinkage(t *testing.T) {
	// Test that SetAccount properly links accounts to certificates when certificateID is non-zero
	// This tests the real certificate flow integration

	// Setup database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		Logger:        nil,
		PromRegistry:  nil,
		DataDir:       "",
	})
	require.NoError(t, err)
	defer db.Close()

	// Get metadata store and cast to concrete type
	metadataStore := db.Metadata().(*sqlite.MetadataStoreSqlite)

	// Test data
	stakeKey := []byte{0x31, 0x32, 0x33, 0x34}
	poolKeyHash := []byte{0x35, 0x36, 0x37, 0x38}
	slot := uint64(4000)

	// Start a transaction to simulate certificate processing
	txn := metadataStore.DB().Begin()
	require.NoError(t, txn.Error)

	// Create a transaction record (simulating certificate processing)
	txHash := []byte{0x21, 0x22, 0x33}
	tmpTx := models.Transaction{
		Hash:       txHash,
		Type:       0,
		BlockHash:  []byte{0x11, 0x12, 0x13},
		BlockIndex: 0,
		Fee:        types.Uint64{Val: 2000},
		TTL:        types.Uint64{Val: 2000000},
	}
	err = txn.Create(&tmpTx).Error
	require.NoError(t, err)

	// Create unified certificate record (simulating certificate storage)
	unifiedCert := models.Certificate{
		BlockHash:     []byte{0x11, 0x12, 0x13},
		TransactionID: tmpTx.ID,
		CertificateID: 0, // Will be updated after specific model creation
		Slot:          4000,
		CertIndex:     0,
		CertType:      models.CertificateTypeStakeRegistration,
	}
	err = txn.Create(&unifiedCert).Error
	require.NoError(t, err)

	// Create stake registration record (specific certificate model)
	stakeReg := models.StakeRegistration{
		StakingKey:    stakeKey,
		CertificateID: unifiedCert.ID,
		DepositAmount: types.Uint64{Val: 3000000},
		AddedSlot:     4000,
	}
	err = txn.Create(&stakeReg).Error
	require.NoError(t, err)

	// Update the unified certificate with the specific model ID
	unifiedCert.CertificateID = stakeReg.ID
	err = txn.Save(&unifiedCert).Error
	require.NoError(t, err)

	// Now call SetAccount with the unified certificate ID (simulating certificate processing)
	err = metadataStore.SetAccount(
		stakeKey,
		poolKeyHash,
		nil, // drep
		slot,
		true,           // active
		unifiedCert.ID, // certificateID - this is the key test: non-zero certificate linkage
		txn,
	)
	require.NoError(t, err)

	// Commit the transaction
	err = txn.Commit().Error
	require.NoError(t, err)

	// Verify account was created and is properly linked to the certificate
	account, err := metadataStore.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	assert.Equal(t, stakeKey, account.StakingKey)
	assert.Equal(t, poolKeyHash, account.Pool)
	assert.True(t, account.Active)
	assert.Equal(t, slot, account.AddedSlot)

	// This is the key assertion: account should be linked to the certificate
	assert.Equal(
		t,
		unifiedCert.ID,
		account.CertificateID,
		"Account should be linked to the originating certificate",
	)

	// Verify we can query the account through the certificate relationship
	var linkedAccount models.Account
	err = metadataStore.DB().
		Where("certificate_id = ?", unifiedCert.ID).
		First(&linkedAccount).
		Error
	require.NoError(t, err)
	assert.Equal(
		t,
		account.ID,
		linkedAccount.ID,
		"Should find account by certificate ID",
	)
}
