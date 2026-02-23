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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlite

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	pdata "github.com/blinklabs-io/plutigo/data"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// --- Mock transaction input with consumed support ---

type mockInput struct {
	txId  []byte
	index uint32
}

func (m *mockInput) Id() lcommon.Blake2b256 {
	return lcommon.NewBlake2b256(m.txId)
}

func (m *mockInput) Index() uint32 {
	return m.index
}

func (m *mockInput) String() string {
	return fmt.Sprintf("%x#%d", m.txId, m.index)
}

func (m *mockInput) Utxorpc() (*cardano.TxInput, error) {
	return nil, nil
}

func (m *mockInput) ToPlutusData() pdata.PlutusData {
	return nil
}

// mockTransactionWithInputs extends mockTransaction with consumed
// inputs for testing the optimistic locking UTxO spend path.
type mockTransactionWithInputs struct {
	mockTransaction
	consumed []mockInput
}

func (m *mockTransactionWithInputs) Consumed() []lcommon.TransactionInput {
	result := make([]lcommon.TransactionInput, len(m.consumed))
	for i := range m.consumed {
		result[i] = &m.consumed[i]
	}
	return result
}

func (m *mockTransactionWithInputs) Inputs() []lcommon.TransactionInput {
	return m.Consumed()
}

// --- Optimistic UTxO locking tests ---

// TestOptimisticLockingConflictDetection verifies that the optimistic
// locking mechanism in SetTransaction correctly detects when a UTxO
// has already been spent by another transaction.
func TestOptimisticLockingConflictDetection(t *testing.T) {
	store := setupTestDB(t)

	// Create a UTxO that both transactions will try to spend
	utxoTxId := bytes.Repeat([]byte{0xAA}, 32)
	utxo := models.Utxo{
		TxId:      utxoTxId,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    types.Uint64(5000000),
	}
	require.NoError(t, store.DB().Create(&utxo).Error)

	// First transaction spends the UTxO successfully
	tx1Hash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x01}, 32))
	tx1 := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    tx1Hash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: utxoTxId, index: 0},
		},
	}

	point1 := ocommon.Point{
		Hash: bytes.Repeat([]byte{0xBB}, 32),
		Slot: 200,
	}

	err := store.SetTransaction(tx1, point1, 0, nil, nil)
	require.NoError(t, err, "first spend should succeed")

	// Verify the UTxO is now marked as spent
	var spentUtxo models.Utxo
	result := store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxoTxId,
		0,
	).First(&spentUtxo)
	require.NoError(t, result.Error)
	require.Equal(
		t,
		uint64(200),
		spentUtxo.DeletedSlot,
		"UTxO should be marked as spent at slot 200",
	)
	require.NotNil(
		t,
		spentUtxo.SpentAtTxId,
		"UTxO should have spent_at_tx_id set",
	)

	// Second transaction tries to spend the same UTxO -- should get conflict
	tx2Hash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x02}, 32))
	tx2 := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    tx2Hash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: utxoTxId, index: 0},
		},
	}

	point2 := ocommon.Point{
		Hash: bytes.Repeat([]byte{0xCC}, 32),
		Slot: 300,
	}

	err = store.SetTransaction(tx2, point2, 0, nil, nil)
	require.Error(t, err, "second spend should fail")
	require.True(
		t,
		errors.Is(err, types.ErrUtxoConflict),
		"error should be ErrUtxoConflict, got: %v",
		err,
	)
}

// TestOptimisticLockingIdempotentRetry verifies that retrying a
// transaction that already successfully consumed a UTxO is safe
// (idempotent retry detection).
func TestOptimisticLockingIdempotentRetry(t *testing.T) {
	store := setupTestDB(t)

	// Create a UTxO
	utxoTxId := bytes.Repeat([]byte{0xDD}, 32)
	utxo := models.Utxo{
		TxId:      utxoTxId,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    types.Uint64(5000000),
	}
	require.NoError(t, store.DB().Create(&utxo).Error)

	// Spend the UTxO
	txHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x03}, 32))
	tx := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    txHash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: utxoTxId, index: 0},
		},
	}

	point := ocommon.Point{
		Hash: bytes.Repeat([]byte{0xEE}, 32),
		Slot: 200,
	}

	err := store.SetTransaction(tx, point, 0, nil, nil)
	require.NoError(t, err, "first spend should succeed")

	// Retry the same transaction -- should succeed (idempotent)
	err = store.SetTransaction(tx, point, 0, nil, nil)
	require.NoError(t, err, "idempotent retry should succeed")
}

// TestOptimisticLockingMissingUtxo verifies that attempting to spend a
// non-existent UTxO logs a warning but does not return an error.
func TestOptimisticLockingMissingUtxo(t *testing.T) {
	store := setupTestDB(t)

	// Transaction consumes a UTxO that doesn't exist
	missingTxId := bytes.Repeat([]byte{0xFF}, 32)
	txHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0x04}, 32))
	tx := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    txHash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: missingTxId, index: 0},
		},
	}

	point := ocommon.Point{
		Hash: bytes.Repeat([]byte{0x11}, 32),
		Slot: 200,
	}

	// Missing UTxO should not return error (logged as warning)
	err := store.SetTransaction(tx, point, 0, nil, nil)
	require.NoError(
		t,
		err,
		"spending missing UTxO should not error (warn only)",
	)
}

// Validates AddressTransaction populates the data from transaction(input/output) participant
// Validates whether it inserts a record per unique address (payment & staking key pair)
func TestSetTransactionIndexesAddressTransactions(t *testing.T) {
	store := setupTestDB(t)

	utxoTxId := bytes.Repeat([]byte{0xA1}, 32)
	paymentKey := bytes.Repeat([]byte{0x11}, 28)
	stakingKey := bytes.Repeat([]byte{0x22}, 28)
	utxo := models.Utxo{
		TxId:       utxoTxId,
		OutputIdx:  0,
		AddedSlot:  100,
		PaymentKey: paymentKey,
		StakingKey: stakingKey,
		Amount:     types.Uint64(5000000),
	}
	require.NoError(t, store.DB().Create(&utxo).Error)

	txHash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0xB1}, 32))
	tx := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    txHash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: utxoTxId, index: 0},
			{txId: utxoTxId, index: 0}, // duplicate input reference
		},
	}
	point := ocommon.Point{
		Hash: bytes.Repeat([]byte{0xC1}, 32),
		Slot: 200,
	}
	require.NoError(t, store.SetTransaction(tx, point, 3, nil, nil))

	var dbTx models.Transaction
	require.NoError(
		t,
		store.DB().Where("hash = ?", txHash.Bytes()).First(&dbTx).Error,
	)

	var rows []models.AddressTransaction
	require.NoError(
		t,
		store.DB().Where("transaction_id = ?", dbTx.ID).Find(&rows).Error,
	)
	require.Len(t, rows, 1, "duplicate addresses in one tx should be deduplicated")
	require.Equal(t, paymentKey, rows[0].PaymentKey)
	require.Equal(t, stakingKey, rows[0].StakingKey)
	require.Equal(t, point.Slot, rows[0].Slot)
	require.Equal(t, uint32(3), rows[0].TxIndex)
}

// --- Rollback state restoration tests ---

// TestRollbackAccountState tests that RestoreAccountStateAtSlot correctly
// restores account delegation state to a prior slot.
func TestRollbackAccountState(t *testing.T) {
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0x01}, 28)
	poolKey1 := bytes.Repeat([]byte{0xA1}, 28)
	poolKey2 := bytes.Repeat([]byte{0xA2}, 28)

	// Create account with initial pool delegation at slot 100
	account := &models.Account{
		StakingKey: stakeKey,
		Pool:       poolKey1,
		Active:     true,
		AddedSlot:  100,
	}
	require.NoError(t, store.DB().Create(account).Error)

	// Create transaction at slot 100 for the certificate FK
	require.NoError(t, createTestTransaction(store.DB(), 1, 100))

	// Add a stake registration certificate at slot 100
	// (RestoreAccountStateAtSlot requires a registration cert to keep the account)
	regCert := models.Certificate{
		TransactionID: 1,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeRegistration),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&regCert).Error)

	stakeReg := models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: regCert.ID,
	}
	require.NoError(t, store.DB().Create(&stakeReg).Error)

	// Add a stake delegation certificate at slot 100 (pool delegation)
	delegCert1 := models.Certificate{
		TransactionID: 1,
		CertIndex:     1,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&delegCert1).Error)

	stakeDelegation1 := models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKey1,
		AddedSlot:     100,
		CertificateID: delegCert1.ID,
	}
	require.NoError(t, store.DB().Create(&stakeDelegation1).Error)

	// Create transaction at slot 200 for the certificate FK
	require.NoError(t, createTestTransaction(store.DB(), 2, 200))

	// Add a new delegation at slot 200 (changes pool)
	delegCert2 := models.Certificate{
		TransactionID: 2,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          200,
	}
	require.NoError(t, store.DB().Create(&delegCert2).Error)

	stakeDelegation2 := models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKey2,
		AddedSlot:     200,
		CertificateID: delegCert2.ID,
	}
	require.NoError(t, store.DB().Create(&stakeDelegation2).Error)

	// Update account to reflect new delegation
	store.DB().Model(account).Updates(map[string]any{
		"pool":       poolKey2,
		"added_slot": 200,
	})

	// Rollback to slot 150 (between cert1 and cert2)
	err := store.RestoreAccountStateAtSlot(150, nil)
	require.NoError(t, err, "RestoreAccountStateAtSlot should succeed")

	// Verify account was restored to pool1
	restored, err := store.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	require.NotNil(t, restored, "account should still exist after rollback")
	require.True(
		t,
		bytes.Equal(restored.Pool, poolKey1),
		"account pool should be restored to poolKey1",
	)
	require.True(t, restored.Active, "account should remain active")
}

// TestRollbackDeletesAccountRegisteredAfterSlot tests that accounts
// registered only after the rollback slot are deleted.
func TestRollbackDeletesAccountRegisteredAfterSlot(t *testing.T) {
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0x02}, 28)

	// Create account registered at slot 300 (after rollback point)
	account := &models.Account{
		StakingKey: stakeKey,
		Active:     true,
		AddedSlot:  300,
	}
	require.NoError(t, store.DB().Create(account).Error)

	// Rollback to slot 200
	err := store.RestoreAccountStateAtSlot(200, nil)
	require.NoError(t, err)

	// Account should be deleted since it was registered after rollback slot
	restored, err := store.GetAccount(stakeKey, true, nil)
	require.NoError(t, err)
	require.Nil(
		t,
		restored,
		"account registered after rollback should be deleted",
	)
}

// TestRollbackPoolState tests that RestorePoolStateAtSlot correctly
// restores pool state to a prior slot.
func TestRollbackPoolState(t *testing.T) {
	store := setupTestDB(t)

	poolKeyHash := bytes.Repeat([]byte{0x10}, 28)
	vrfKeyHash := bytes.Repeat([]byte{0x20}, 32)

	// Create pool at slot 100
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  vrfKeyHash,
		Pledge:      types.Uint64(1000000),
		Cost:        types.Uint64(340000000),
		Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	// Create transaction at slot 100 for registration FK
	require.NoError(t, createTestTransaction(store.DB(), 10, 100))

	// Create initial registration at slot 100
	cert1 := models.Certificate{
		TransactionID: 10,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&cert1).Error)

	reg1 := models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   poolKeyHash,
		VrfKeyHash:    vrfKeyHash,
		Pledge:        types.Uint64(1000000),
		Cost:          types.Uint64(340000000),
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		AddedSlot:     100,
		CertificateID: cert1.ID,
	}
	require.NoError(t, store.DB().Create(&reg1).Error)

	// Create transaction at slot 250 for second registration
	require.NoError(t, createTestTransaction(store.DB(), 11, 250))

	// Create updated registration at slot 250 (changes pledge)
	cert2 := models.Certificate{
		TransactionID: 11,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
		Slot:          250,
	}
	require.NoError(t, store.DB().Create(&cert2).Error)

	reg2 := models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   poolKeyHash,
		VrfKeyHash:    vrfKeyHash,
		Pledge:        types.Uint64(5000000),
		Cost:          types.Uint64(340000000),
		Margin:        &types.Rat{Rat: big.NewRat(1, 50)},
		AddedSlot:     250,
		CertificateID: cert2.ID,
	}
	require.NoError(t, store.DB().Create(&reg2).Error)

	// Update pool denormalized fields
	store.DB().Model(pool).Updates(map[string]any{
		"pledge": types.Uint64(5000000),
		"margin": &types.Rat{Rat: big.NewRat(1, 50)},
	})

	// Rollback to slot 200
	err := store.RestorePoolStateAtSlot(200, nil)
	require.NoError(t, err, "RestorePoolStateAtSlot should succeed")

	// Verify pool was restored to original pledge
	restored, err := store.GetPool(
		lcommon.PoolKeyHash(poolKeyHash),
		true,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, restored, "pool should still exist after rollback")
	require.Equal(
		t,
		types.Uint64(1000000),
		restored.Pledge,
		"pool pledge should be restored to original value",
	)
}

// TestRollbackDrepState tests that RestoreDrepStateAtSlot correctly
// restores DRep state to a prior slot.
func TestRollbackDrepState(t *testing.T) {
	store := setupTestDB(t)

	drepCred := bytes.Repeat([]byte{0x30}, 28)

	// Create DRep at slot 100
	drep := &models.Drep{
		Credential: drepCred,
		AnchorUrl:  "https://drep.example.com/initial",
		AnchorHash: bytes.Repeat([]byte{0x40}, 32),
		Active:     true,
		AddedSlot:  100,
	}
	require.NoError(t, store.DB().Create(drep).Error)

	// Create transaction at slot 100
	require.NoError(t, createTestTransaction(store.DB(), 20, 100))

	// Registration at slot 100
	cert1 := models.Certificate{
		TransactionID: 20,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeRegistrationDrep),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&cert1).Error)

	reg1 := models.RegistrationDrep{
		DrepCredential: drepCred,
		AnchorUrl:      "https://drep.example.com/initial",
		AnchorHash:     bytes.Repeat([]byte{0x40}, 32),
		AddedSlot:      100,
		CertificateID:  cert1.ID,
	}
	require.NoError(t, store.DB().Create(&reg1).Error)

	// Create transaction at slot 250
	require.NoError(t, createTestTransaction(store.DB(), 21, 250))

	// Update at slot 250 (changes anchor)
	cert2 := models.Certificate{
		TransactionID: 21,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeUpdateDrep),
		Slot:          250,
	}
	require.NoError(t, store.DB().Create(&cert2).Error)

	update := models.UpdateDrep{
		Credential:    drepCred,
		AnchorUrl:     "https://drep.example.com/updated",
		AnchorHash:    bytes.Repeat([]byte{0x50}, 32),
		AddedSlot:     250,
		CertificateID: cert2.ID,
	}
	require.NoError(t, store.DB().Create(&update).Error)

	// Update DRep to reflect new anchor
	store.DB().Model(drep).Updates(map[string]any{
		"anchor_url":  "https://drep.example.com/updated",
		"anchor_hash": bytes.Repeat([]byte{0x50}, 32),
		"added_slot":  250,
	})

	// Rollback to slot 200
	err := store.RestoreDrepStateAtSlot(200, nil)
	require.NoError(t, err, "RestoreDrepStateAtSlot should succeed")

	// Verify DRep was restored
	restored, err := store.GetDrep(drepCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, restored, "DRep should still exist after rollback")
	require.Equal(
		t,
		"https://drep.example.com/initial",
		restored.AnchorUrl,
		"DRep anchor URL should be restored",
	)
	require.True(t, restored.Active, "DRep should remain active")
}

// TestRollbackDeletesCertificatesAfterSlot tests that
// DeleteCertificatesAfterSlot removes certificates added after the slot.
func TestRollbackDeletesCertificatesAfterSlot(t *testing.T) {
	store := setupTestDB(t)

	// Create transactions for FK constraints
	require.NoError(t, createTestTransaction(store.DB(), 30, 100))
	require.NoError(t, createTestTransaction(store.DB(), 31, 200))
	require.NoError(t, createTestTransaction(store.DB(), 32, 300))

	// Create certificates at different slots
	stakeKey := bytes.Repeat([]byte{0x60}, 28)

	cert1 := models.Certificate{
		TransactionID: 30,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeRegistration),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&cert1).Error)

	stakeReg := models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: cert1.ID,
	}
	require.NoError(t, store.DB().Create(&stakeReg).Error)

	cert2 := models.Certificate{
		TransactionID: 31,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeRegistration),
		Slot:          200,
	}
	require.NoError(t, store.DB().Create(&cert2).Error)

	cert3 := models.Certificate{
		TransactionID: 32,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeRegistration),
		Slot:          300,
	}
	require.NoError(t, store.DB().Create(&cert3).Error)

	// Delete certificates after slot 150
	err := store.DeleteCertificatesAfterSlot(150, nil)
	require.NoError(t, err)

	// Verify only the first certificate remains
	var remaining []models.Certificate
	store.DB().Find(&remaining)
	require.Len(
		t,
		remaining,
		1,
		"only certificates at or before slot 150 should remain",
	)
	require.Equal(
		t,
		uint64(100),
		remaining[0].Slot,
		"remaining certificate should be at slot 100",
	)
}

// TestRollbackUtxoRestoration tests that UTxOs are correctly unspent
// and removed during rollback.
func TestRollbackUtxoRestoration(t *testing.T) {
	store := setupTestDB(t)

	// Create a transaction that will be referenced as the spender
	spenderTxHash := bytes.Repeat([]byte{0x71}, 32)
	spenderTx := models.Transaction{
		Hash:  spenderTxHash,
		Slot:  200,
		Valid: true,
		Type:  0,
	}
	require.NoError(t, store.DB().Create(&spenderTx).Error)

	// Create UTxOs at different slots
	// UTxO 1: created at slot 100, spent at slot 200
	utxo1TxId := bytes.Repeat([]byte{0x70}, 32)
	utxo1 := models.Utxo{
		TxId:        utxo1TxId,
		OutputIdx:   0,
		AddedSlot:   100,
		DeletedSlot: 200,
		Amount:      types.Uint64(2000000),
		SpentAtTxId: spenderTxHash,
	}
	require.NoError(t, store.DB().Create(&utxo1).Error)

	// UTxO 2: created at slot 250 (after rollback point)
	utxo2TxId := bytes.Repeat([]byte{0x72}, 32)
	utxo2 := models.Utxo{
		TxId:      utxo2TxId,
		OutputIdx: 0,
		AddedSlot: 250,
		Amount:    types.Uint64(3000000),
	}
	require.NoError(t, store.DB().Create(&utxo2).Error)

	// UTxO 3: created at slot 50, not spent (should remain unchanged)
	utxo3TxId := bytes.Repeat([]byte{0x73}, 32)
	utxo3 := models.Utxo{
		TxId:      utxo3TxId,
		OutputIdx: 0,
		AddedSlot: 50,
		Amount:    types.Uint64(1000000),
	}
	require.NoError(t, store.DB().Create(&utxo3).Error)

	// Rollback: unspend UTxOs spent after slot 150
	err := store.SetUtxosNotDeletedAfterSlot(150, nil)
	require.NoError(t, err, "SetUtxosNotDeletedAfterSlot should succeed")

	// Verify UTxO 1 is now unspent
	var restored1 models.Utxo
	store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxo1TxId,
		0,
	).First(&restored1)
	require.Equal(
		t,
		uint64(0),
		restored1.DeletedSlot,
		"UTxO 1 should be unspent after rollback",
	)

	// Rollback: delete UTxOs added after slot 150
	err = store.DeleteUtxosAfterSlot(150, nil)
	require.NoError(t, err, "DeleteUtxosAfterSlot should succeed")

	// Verify UTxO 2 is deleted
	var count int64
	store.DB().Model(&models.Utxo{}).Where(
		"tx_id = ?",
		utxo2TxId,
	).Count(&count)
	require.Equal(
		t,
		int64(0),
		count,
		"UTxO 2 should be deleted (added after rollback)",
	)

	// Verify UTxO 3 is unchanged
	var unchanged models.Utxo
	result := store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxo3TxId,
		0,
	).First(&unchanged)
	require.NoError(t, result.Error)
	require.Equal(
		t,
		uint64(50),
		unchanged.AddedSlot,
		"UTxO 3 should be unchanged",
	)
}

// TestRollbackTransactionDeletion tests that
// DeleteTransactionsAfterSlot removes transactions and restores
// UTxO state correctly.
func TestRollbackTransactionDeletion(t *testing.T) {
	store := setupTestDB(t)

	spendingTxHash := bytes.Repeat([]byte{0x81}, 32)

	// Create the spending transaction at slot 200 FIRST (FK target)
	spendingTx := models.Transaction{
		Hash:  spendingTxHash,
		Slot:  200,
		Valid: true,
		Type:  0,
	}
	require.NoError(t, store.DB().Create(&spendingTx).Error)

	// Create a transaction at slot 100 (should be kept)
	keepTx := models.Transaction{
		Hash:  bytes.Repeat([]byte{0x82}, 32),
		Slot:  100,
		Valid: true,
		Type:  0,
	}
	require.NoError(t, store.DB().Create(&keepTx).Error)

	// Create UTxO that was spent by the transaction at slot 200
	utxoTxId := bytes.Repeat([]byte{0x80}, 32)
	utxo := models.Utxo{
		TxId:        utxoTxId,
		OutputIdx:   0,
		AddedSlot:   50,
		DeletedSlot: 200,
		Amount:      types.Uint64(5000000),
		SpentAtTxId: spendingTxHash,
	}
	require.NoError(t, store.DB().Create(&utxo).Error)

	// Delete transactions after slot 150
	err := store.DeleteTransactionsAfterSlot(150, nil)
	require.NoError(t, err)

	// Verify the UTxO was restored (spent_at_tx_id cleared, deleted_slot = 0)
	var restored models.Utxo
	result := store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxoTxId,
		0,
	).First(&restored)
	require.NoError(t, result.Error)
	require.Equal(
		t,
		uint64(0),
		restored.DeletedSlot,
		"UTxO deleted_slot should be reset to 0",
	)
	require.Nil(
		t,
		restored.SpentAtTxId,
		"UTxO spent_at_tx_id should be nil after rollback",
	)

	// Verify the spending transaction was deleted
	var txCount int64
	store.DB().Model(&models.Transaction{}).Where(
		"hash = ?",
		spendingTxHash,
	).Count(&txCount)
	require.Equal(
		t,
		int64(0),
		txCount,
		"spending transaction should be deleted",
	)

	// Verify the kept transaction still exists
	var keptCount int64
	store.DB().Model(&models.Transaction{}).Where(
		"hash = ?",
		keepTx.Hash,
	).Count(&keptCount)
	require.Equal(
		t,
		int64(1),
		keptCount,
		"transaction at slot 100 should be kept",
	)
}

// --- Full rollback and replay tests ---

// TestRollbackAndReplay processes state through slots, rolls back, then
// replays the same operations and verifies the final state matches.
func TestRollbackAndReplay(t *testing.T) {
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0xD1}, 28)
	poolKey := bytes.Repeat([]byte{0xD2}, 28)

	// --- Phase 1: Build initial state at slot 100 ---
	account := &models.Account{
		StakingKey: stakeKey,
		Pool:       poolKey,
		Active:     true,
		AddedSlot:  100,
	}
	require.NoError(t, store.DB().Create(account).Error)

	// Create certificate infrastructure at slot 100
	require.NoError(t, createTestTransaction(store.DB(), 40, 100))

	// Registration cert (needed by RestoreAccountStateAtSlot to keep the account)
	regCert := models.Certificate{
		TransactionID: 40,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeRegistration),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&regCert).Error)
	stakeReg := models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: regCert.ID,
	}
	require.NoError(t, store.DB().Create(&stakeReg).Error)

	// Pool delegation cert at slot 100
	delegCert100 := models.Certificate{
		TransactionID: 40,
		CertIndex:     1,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          100,
	}
	require.NoError(t, store.DB().Create(&delegCert100).Error)
	sd100 := models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKey,
		AddedSlot:     100,
		CertificateID: delegCert100.ID,
	}
	require.NoError(t, store.DB().Create(&sd100).Error)

	// --- Phase 2: Advance to slot 200 with new delegation ---
	poolKey2 := bytes.Repeat([]byte{0xD3}, 28)
	require.NoError(t, createTestTransaction(store.DB(), 41, 200))
	delegCert200 := models.Certificate{
		TransactionID: 41,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          200,
	}
	require.NoError(t, store.DB().Create(&delegCert200).Error)
	sd200 := models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKey2,
		AddedSlot:     200,
		CertificateID: delegCert200.ID,
	}
	require.NoError(t, store.DB().Create(&sd200).Error)
	store.DB().Model(account).Updates(map[string]any{
		"pool":       poolKey2,
		"added_slot": 200,
	})

	// Capture pre-rollback state at slot 200
	preRollback, err := store.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	require.True(
		t,
		bytes.Equal(preRollback.Pool, poolKey2),
		"pre-rollback pool should be poolKey2",
	)

	// --- Phase 3: Rollback to slot 150 ---
	// Delete certificates after slot 150
	err = store.DeleteCertificatesAfterSlot(150, nil)
	require.NoError(t, err)

	// Restore account state
	err = store.RestoreAccountStateAtSlot(150, nil)
	require.NoError(t, err)

	// Verify rollback state
	midRollback, err := store.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		midRollback,
		"account should exist after rollback (has registration at slot 100)",
	)
	require.True(
		t,
		bytes.Equal(midRollback.Pool, poolKey),
		"after rollback, pool should be original poolKey",
	)

	// --- Phase 4: Replay slot 200 with same data ---
	require.NoError(t, createTestTransaction(store.DB(), 42, 200))
	cert200Replay := models.Certificate{
		TransactionID: 42,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          200,
	}
	require.NoError(t, store.DB().Create(&cert200Replay).Error)
	sd200Replay := models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolKey2,
		AddedSlot:     200,
		CertificateID: cert200Replay.ID,
	}
	require.NoError(t, store.DB().Create(&sd200Replay).Error)
	store.DB().Model(&models.Account{}).Where(
		"staking_key = ?",
		stakeKey,
	).Updates(map[string]any{
		"pool":       poolKey2,
		"added_slot": 200,
	})

	// Verify final state matches pre-rollback state
	postReplay, err := store.GetAccount(stakeKey, false, nil)
	require.NoError(t, err)
	require.NotNil(t, postReplay, "account should exist after replay")
	require.True(
		t,
		bytes.Equal(postReplay.Pool, poolKey2),
		"after replay, pool should be poolKey2 again",
	)
}

// TestRollbackPParamsDeletion tests that protocol parameters added
// after the rollback slot are removed.
func TestRollbackPParamsDeletion(t *testing.T) {
	store := setupTestDB(t)

	// Create protocol parameters at different slots
	pparams1 := models.PParams{
		Cbor:      []byte("pparams_at_slot_100"),
		AddedSlot: 100,
		Epoch:     10,
		EraId:     5,
	}
	require.NoError(t, store.DB().Create(&pparams1).Error)

	pparams2 := models.PParams{
		Cbor:      []byte("pparams_at_slot_300"),
		AddedSlot: 300,
		Epoch:     15,
		EraId:     5,
	}
	require.NoError(t, store.DB().Create(&pparams2).Error)

	// Delete after slot 200
	err := store.DeletePParamsAfterSlot(200, nil)
	require.NoError(t, err)

	// Verify only pparams at slot 100 remain
	var remaining []models.PParams
	store.DB().Find(&remaining)
	require.Len(
		t,
		remaining,
		1,
		"only pparams at or before slot 200 should remain",
	)
	require.Equal(
		t,
		uint64(100),
		remaining[0].AddedSlot,
		"remaining pparams should be at slot 100",
	)
}

// TestRollbackGovernanceProposalDeletion tests that governance proposals
// added after the rollback slot are removed.
func TestRollbackGovernanceProposalDeletion(t *testing.T) {
	store := setupTestDB(t)

	prop1 := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x90}, 32),
		ActionIndex:   0,
		ActionType:    1,
		AddedSlot:     100,
		ExpiresEpoch:  50,
		AnchorUrl:     "https://example.com/prop1",
		AnchorHash:    bytes.Repeat([]byte{0x92}, 32),
		ReturnAddress: bytes.Repeat([]byte{0x93}, 29),
	}
	require.NoError(t, store.SetGovernanceProposal(prop1, nil))

	prop2 := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x91}, 32),
		ActionIndex:   0,
		ActionType:    2,
		AddedSlot:     300,
		ExpiresEpoch:  70,
		AnchorUrl:     "https://example.com/prop2",
		AnchorHash:    bytes.Repeat([]byte{0x94}, 32),
		ReturnAddress: bytes.Repeat([]byte{0x95}, 29),
	}
	require.NoError(t, store.SetGovernanceProposal(prop2, nil))

	// Delete after slot 200
	err := store.DeleteGovernanceProposalsAfterSlot(200, nil)
	require.NoError(t, err)

	// Verify only first proposal remains
	var remaining []models.GovernanceProposal
	store.DB().Where("deleted_slot IS NULL").Find(&remaining)
	require.Len(
		t,
		remaining,
		1,
		"only proposals at or before slot 200 should remain",
	)
}

// TestRollbackConcurrentUtxoSpend simulates the TOCTOU race scenario:
// 1. Spend a UTxO
// 2. Rollback restores it to unspent
// 3. New transaction successfully re-spends it
// The optimistic locking should allow the second spend to succeed since
// the rollback restored the UTxO to unspent state.
func TestRollbackConcurrentUtxoSpend(t *testing.T) {
	store := setupTestDB(t)

	// Create UTxO at slot 50
	utxoTxId := bytes.Repeat([]byte{0xC0}, 32)
	utxo := models.Utxo{
		TxId:      utxoTxId,
		OutputIdx: 0,
		AddedSlot: 50,
		Amount:    types.Uint64(10000000),
	}
	require.NoError(t, store.DB().Create(&utxo).Error)

	// Spend it at slot 100
	tx1Hash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0xC1}, 32))
	tx1 := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    tx1Hash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: utxoTxId, index: 0},
		},
	}
	point1 := ocommon.Point{
		Hash: bytes.Repeat([]byte{0xC2}, 32),
		Slot: 100,
	}
	err := store.SetTransaction(tx1, point1, 0, nil, nil)
	require.NoError(t, err, "initial spend should succeed")

	// Verify UTxO is spent
	var spentCheck models.Utxo
	store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxoTxId,
		0,
	).First(&spentCheck)
	require.Equal(t, uint64(100), spentCheck.DeletedSlot, "should be spent")

	// Rollback: unspend UTxOs spent after slot 80
	err = store.SetUtxosNotDeletedAfterSlot(80, nil)
	require.NoError(t, err)

	// Verify UTxO is now unspent
	var unspentCheck models.Utxo
	store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxoTxId,
		0,
	).First(&unspentCheck)
	require.Equal(
		t,
		uint64(0),
		unspentCheck.DeletedSlot,
		"should be unspent after rollback",
	)

	// Clear spent_at_tx_id so optimistic locking allows re-spending
	// (this is what DeleteTransactionsAfterSlot does for the full rollback path)
	store.DB().Model(&models.Utxo{}).Where(
		"tx_id = ? AND output_idx = ?",
		utxoTxId,
		0,
	).Updates(map[string]any{
		"spent_at_tx_id": nil,
	})

	// Now a new transaction should be able to spend it
	tx2Hash := lcommon.NewBlake2b256(bytes.Repeat([]byte{0xC3}, 32))
	tx2 := &mockTransactionWithInputs{
		mockTransaction: mockTransaction{
			hash:    tx2Hash,
			isValid: true,
		},
		consumed: []mockInput{
			{txId: utxoTxId, index: 0},
		},
	}
	point2 := ocommon.Point{
		Hash: bytes.Repeat([]byte{0xC4}, 32),
		Slot: 150,
	}
	err = store.SetTransaction(tx2, point2, 0, nil, nil)
	require.NoError(t, err, "re-spend after rollback should succeed")

	// Verify the UTxO is now spent by tx2
	var finalCheck models.Utxo
	store.DB().Where(
		"tx_id = ? AND output_idx = ?",
		utxoTxId,
		0,
	).First(&finalCheck)
	require.Equal(
		t,
		uint64(150),
		finalCheck.DeletedSlot,
		"UTxO should now be spent at slot 150",
	)
	require.True(
		t,
		bytes.Equal(finalCheck.SpentAtTxId, tx2Hash.Bytes()),
		"UTxO should be spent by tx2",
	)
}
