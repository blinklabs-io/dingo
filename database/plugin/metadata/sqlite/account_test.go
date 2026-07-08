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
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestGetAccount_ExcludesInactiveWhenIncludeInactiveFalse pins the
// includeInactive=false branch of GetAccountByCredential: a row with
// active=false must not be returned. This is the semantic the
// queryShelleyFilteredDelegationAndRewardAccounts handler relies on when
// it passes includeInactive=false; testing it here keeps the assertion
// next to the SQL clause it actually tests, without forcing the ledger
// layer to import this package or to depend on the GORM default-tag
// behavior of CreateAccount.
func TestGetAccount_ExcludesInactiveWhenIncludeInactiveFalse(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xAB
	}

	// Insert active, then deactivate via UPDATE. GORM's `default:true`
	// on Account.Active swallows the false value on INSERT (whether via
	// Create, Save, or Select("*").Create — the DEFAULT clause applies
	// at the SQL level). UPDATE has no analogous fallback, so flipping
	// Active in a follow-up statement is the reliable path.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeKey,
		Reward:     types.Uint64(123_456),
		Active:     true,
	}).Error)
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("staking_key = ?", stakeKey).
		Update("active", false).Error)

	got, err := store.GetAccountByCredential(0, stakeKey, false /* includeInactive */, nil)
	require.NoError(t, err)
	assert.Nil(t, got, "inactive account must not be returned when includeInactive=false")

	got, err = store.GetAccountByCredential(0, stakeKey, true /* includeInactive */, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "inactive account must be returned when includeInactive=true")
	assert.False(t, got.Active, "returned account should have Active=false")
}

// TestAccountHistoryQueriesAreCredentialTagAware verifies that account
// registration/delegation history queries do not merge key and script
// credential rows that share the same 28-byte hash.
func TestAccountHistoryQueriesAreCredentialTagAware(t *testing.T) {
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xA1
	}
	keyPool := make([]byte, 28)
	for i := range keyPool {
		keyPool[i] = 0xB1
	}
	scriptPool := make([]byte, 28)
	for i := range scriptPool {
		scriptPool[i] = 0xC1
	}

	require.NoError(t, db.Create(&models.Transaction{
		ID:         100,
		Hash:       []byte("tx_key_history_hash_123456789012"),
		Slot:       10,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, db.Create(&models.Transaction{
		ID:         101,
		Hash:       []byte("tx_script_history_hash_123456789"),
		Slot:       20,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            1000,
		TransactionID: 100,
		CertIndex:     0,
		Slot:          10,
		CertType:      11, // StakeRegistrationDelegation
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            1001,
		TransactionID: 101,
		CertIndex:     0,
		Slot:          20,
		CertType:      11, // StakeRegistrationDelegation
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistrationDelegation{
		ID:            1000,
		StakingKey:    stakeKey,
		CredentialTag: 0,
		PoolKeyHash:   keyPool,
		CertificateID: 1000,
		AddedSlot:     10,
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistrationDelegation{
		ID:            1001,
		StakingKey:    stakeKey,
		CredentialTag: 1,
		PoolKeyHash:   scriptPool,
		CertificateID: 1001,
		AddedSlot:     20,
	}).Error)

	keyDelegations, err := store.GetAccountDelegationHistoryByCredential(
		0,
		stakeKey,
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, keyDelegations, 1)
	assert.Equal(t, keyPool, keyDelegations[0].PoolKeyHash)

	scriptDelegations, err := store.GetAccountDelegationHistoryByCredential(
		1,
		stakeKey,
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, scriptDelegations, 1)
	assert.Equal(t, scriptPool, scriptDelegations[0].PoolKeyHash)

	keyDelegationCount, err := store.CountAccountDelegationHistoryByCredential(
		0,
		stakeKey,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, keyDelegationCount)
	scriptDelegationCount, err := store.CountAccountDelegationHistoryByCredential(
		1,
		stakeKey,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, scriptDelegationCount)

	keyRegistrations, err := store.GetAccountRegistrationHistoryByCredential(
		0,
		stakeKey,
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, keyRegistrations, 1)
	assert.Equal(t, uint64(10), keyRegistrations[0].AddedSlot)

	scriptRegistrations, err := store.GetAccountRegistrationHistoryByCredential(
		1,
		stakeKey,
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, scriptRegistrations, 1)
	assert.Equal(t, uint64(20), scriptRegistrations[0].AddedSlot)

	keyRegistrationCount, err := store.CountAccountRegistrationHistoryByCredential(
		0,
		stakeKey,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, keyRegistrationCount)
	scriptRegistrationCount, err := store.CountAccountRegistrationHistoryByCredential(
		1,
		stakeKey,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, 1, scriptRegistrationCount)
}

// TestAccountHistoryExposesTxSlotBlockHashAndDeposit pins the
// OpenAPI-0.1.88 enrichment of the delegation and registration history
// queries: each row must carry the transaction slot, the containing block's
// hash (the adapter resolves the height from the block store), and (for
// registrations) the deposit recorded on the certificate.
func TestAccountHistoryExposesTxSlotBlockHashAndDeposit(t *testing.T) {
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xA2
	}
	pool := make([]byte, 28)
	for i := range pool {
		pool[i] = 0xB2
	}
	blockHash := []byte("block_hash_for_account_history12")

	require.NoError(t, db.Create(&models.Transaction{
		ID:         100,
		Hash:       []byte("tx_enriched_history_hash_1234567"),
		BlockHash:  blockHash,
		Slot:       10,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            1000,
		TransactionID: 100,
		CertIndex:     0,
		Slot:          10,
		CertType:      11, // StakeRegistrationDelegation
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistrationDelegation{
		ID:            1000,
		StakingKey:    stakeKey,
		CredentialTag: 0,
		PoolKeyHash:   pool,
		CertificateID: 1000,
		AddedSlot:     10,
		DepositAmount: 2_000_000,
	}).Error)

	delegations, err := store.GetAccountDelegationHistoryByCredential(
		0, stakeKey, 10, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, delegations, 1)
	assert.Equal(t, uint64(10), delegations[0].TxSlot)
	assert.Equal(t, blockHash, delegations[0].BlockHash)

	registrations, err := store.GetAccountRegistrationHistoryByCredential(
		0, stakeKey, 10, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	assert.Equal(t, "registered", registrations[0].Action)
	assert.Equal(t, uint64(2_000_000), registrations[0].Deposit)
	assert.Equal(t, uint64(10), registrations[0].TxSlot)
	assert.Equal(t, blockHash, registrations[0].BlockHash)
}

// TestGetAccountSumsByCredential verifies the account-sum aggregation that
// backs the Blockfrost account withdrawals_sum/reserves_sum/treasury_sum
// fields: withdrawals come from rollback-aware reward deltas, while the
// reserves and treasury totals come from MIR distributions split by source
// pot. Credits (non-withdrawal deltas) and the other credential tag must be
// excluded.
func TestGetAccountSumsByCredential(t *testing.T) {
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xA3
	}
	otherKey := make([]byte, 28)
	for i := range otherKey {
		otherKey[i] = 0xF3
	}

	// Two withdrawals (summed) plus a credit (ignored) for the credential.
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey: stakeKey, CredentialTag: 0,
		TxHash: []byte("withdrawal_tx_hash_aaaaaaaaaaaa1"),
		Amount: 30, Withdrawal: true, AddedSlot: 10,
	}).Error)
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey: stakeKey, CredentialTag: 0,
		TxHash: []byte("withdrawal_tx_hash_aaaaaaaaaaaa2"),
		Amount: 15, Withdrawal: true, AddedSlot: 20,
	}).Error)
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey: stakeKey, CredentialTag: 0,
		TxHash: []byte("credit_tx_hash_bbbbbbbbbbbbbbbb1"),
		Amount: 99, Withdrawal: false, AddedSlot: 30,
	}).Error)
	// A withdrawal for a different credential must not leak in.
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey: otherKey, CredentialTag: 0,
		TxHash: []byte("withdrawal_tx_hash_ccccccccccc01"),
		Amount: 100, Withdrawal: true, AddedSlot: 40,
	}).Error)

	// MIR distributions: pot 0 = reserves, pot 1 = treasury.
	reservesMIR := &models.MoveInstantaneousRewards{ID: 1, Pot: 0, AddedSlot: 10}
	require.NoError(t, db.Create(reservesMIR).Error)
	treasuryMIR := &models.MoveInstantaneousRewards{ID: 2, Pot: 1, AddedSlot: 10}
	require.NoError(t, db.Create(treasuryMIR).Error)
	require.NoError(t, db.Create(&models.MoveInstantaneousRewardsReward{
		MIRID: 1, Credential: stakeKey, CredentialTag: 0, Amount: 6,
	}).Error)
	require.NoError(t, db.Create(&models.MoveInstantaneousRewardsReward{
		MIRID: 2, Credential: stakeKey, CredentialTag: 0, Amount: 7,
	}).Error)
	// Treasury MIR for a different credential must not leak in.
	require.NoError(t, db.Create(&models.MoveInstantaneousRewardsReward{
		MIRID: 2, Credential: otherKey, CredentialTag: 0, Amount: 50,
	}).Error)

	sums, err := store.GetAccountSumsByCredential(0, stakeKey, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(45), sums.WithdrawalsSum)
	assert.Equal(t, uint64(6), sums.ReservesSum)
	assert.Equal(t, uint64(7), sums.TreasurySum)

	// An unknown credential returns zeroed sums, not an error.
	empty, err := store.GetAccountSumsByCredential(1, stakeKey, nil)
	require.NoError(t, err)
	assert.Equal(t, models.AccountSums{}, empty)
}

// TestGetStakeRegistrationsByCredentialIsTagAware verifies that certificate
// reconstruction keeps key and script stake registrations with the same hash
// separate and restores the correct on-chain credential type.
func TestGetStakeRegistrationsByCredentialIsTagAware(t *testing.T) {
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xD1
	}

	require.NoError(t, db.Create(&models.StakeRegistration{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		CertificateID: 2000,
		AddedSlot:     10,
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistration{
		StakingKey:    stakeKey,
		CredentialTag: 1,
		CertificateID: 2001,
		AddedSlot:     20,
	}).Error)

	keyCerts, err := store.GetStakeRegistrationsByCredential(0, stakeKey, nil)
	require.NoError(t, err)
	require.Len(t, keyCerts, 1)
	assert.Equal(
		t,
		uint(lcommon.CredentialTypeAddrKeyHash),
		keyCerts[0].StakeCredential.CredType,
	)
	assert.Equal(t, stakeKey, keyCerts[0].StakeCredential.Credential.Bytes())

	scriptCerts, err := store.GetStakeRegistrationsByCredential(1, stakeKey, nil)
	require.NoError(t, err)
	require.Len(t, scriptCerts, 1)
	assert.Equal(
		t,
		uint(lcommon.CredentialTypeScriptHash),
		scriptCerts[0].StakeCredential.CredType,
	)
	assert.Equal(t, stakeKey, scriptCerts[0].StakeCredential.Credential.Bytes())
}

func TestGetAccountsActiveAtSlotUsesCertificateHistory(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	activeKey := accountTestHash(0x41)
	inactiveKey := accountTestHash(0x42)
	sameSlotKey := accountTestHash(0x43)
	missingKey := accountTestHash(0x44)
	genesisKey := accountTestHash(0x45)
	futureGenesisKey := accountTestHash(0x46)
	inactiveGenesisKey := accountTestHash(0x47)

	require.NoError(t, db.Create(&[]models.Account{
		{
			StakingKey:    genesisKey,
			CredentialTag: 0,
			Active:        true,
			AddedSlot:     0,
		},
		{
			StakingKey:    futureGenesisKey,
			CredentialTag: 0,
			Active:        true,
			AddedSlot:     200,
		},
		{
			StakingKey:    inactiveGenesisKey,
			CredentialTag: 0,
			Active:        false,
			AddedSlot:     0,
		},
	}).Error)
	require.NoError(t, db.Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", 0, inactiveGenesisKey).
		Update("active", false).Error)

	seedAccountActiveAtSlotCert(
		t,
		db,
		1,
		activeKey,
		0,
		100,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		2,
		activeKey,
		0,
		200,
		0,
		0,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		3,
		activeKey,
		0,
		300,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		4,
		inactiveKey,
		0,
		100,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		5,
		inactiveKey,
		0,
		250,
		0,
		0,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		6,
		sameSlotKey,
		0,
		400,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		7,
		sameSlotKey,
		0,
		400,
		1,
		0,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	// inactiveGenesisKey is a genesis-delegated account (no registration cert)
	// deregistered at slot 50; a genesis account is deactivated only by a
	// deregistration certificate, not by its row's active/added_slot columns.
	seedAccountActiveAtSlotCert(
		t,
		db,
		8,
		inactiveGenesisKey,
		0,
		50,
		0,
		0,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, activeKey),
		models.NewStakeCredentialRef(0, inactiveKey),
		models.NewStakeCredentialRef(1, activeKey),
		models.NewStakeCredentialRef(0, sameSlotKey),
		models.NewStakeCredentialRef(0, missingKey),
		models.NewStakeCredentialRef(0, genesisKey),
		models.NewStakeCredentialRef(0, futureGenesisKey),
		models.NewStakeCredentialRef(0, inactiveGenesisKey),
	}

	got, err := store.GetAccountsActiveAtSlot(refs, 150, nil)
	require.NoError(t, err)
	assert.Contains(t, got, models.NewStakeCredentialRef(0, activeKey).MapKey())
	assert.Contains(t, got, models.NewStakeCredentialRef(0, inactiveKey).MapKey())
	assert.NotContains(t, got, models.NewStakeCredentialRef(1, activeKey).MapKey())
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, missingKey).MapKey())
	assert.Contains(t, got, models.NewStakeCredentialRef(0, genesisKey).MapKey())
	// A genesis account whose row added_slot was bumped to 200 by a later
	// re-delegation is still active at slot 150: activity comes from certificate
	// history, not the mutable added_slot column.
	assert.Contains(t, got, models.NewStakeCredentialRef(0, futureGenesisKey).MapKey())
	// A genesis account deregistered at slot 50 is inactive at slot 150.
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, inactiveGenesisKey).MapKey())

	got, err = store.GetAccountsActiveAtSlot(refs, 260, nil)
	require.NoError(t, err)
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, activeKey).MapKey())
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, inactiveKey).MapKey())
	assert.Contains(t, got, models.NewStakeCredentialRef(0, futureGenesisKey).MapKey())

	got, err = store.GetAccountsActiveAtSlot(refs, 350, nil)
	require.NoError(t, err)
	assert.Contains(t, got, models.NewStakeCredentialRef(0, activeKey).MapKey())
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, inactiveKey).MapKey())

	got, err = store.GetAccountsActiveAtSlot(refs, 400, nil)
	require.NoError(t, err)
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, sameSlotKey).MapKey())
}

// TestGetAccountsActiveAtSlotGenesisMutations pins the #1959 fallback fix: a
// genesis-delegated account (no registration certificate) is active from chain
// start and is deactivated only by a deregistration certificate. Its activity
// at a past slot must not depend on the account row's mutable added_slot/active
// columns, which a later re-delegation bumps forward without changing activity.
func TestGetAccountsActiveAtSlotGenesisMutations(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	reDelegatedKey := accountTestHash(0x51)
	deregisteredKey := accountTestHash(0x52)

	// Both are genesis-delegated (no registration cert). reDelegatedKey later
	// re-delegated at slot 500, bumping its row added_slot to 500 without
	// affecting activity. deregisteredKey was deregistered at slot 300, which
	// also flips its row active flag to false.
	require.NoError(t, db.Create(&[]models.Account{
		{
			StakingKey:    reDelegatedKey,
			CredentialTag: 0,
			Active:        true,
			AddedSlot:     500,
		},
		{
			StakingKey:    deregisteredKey,
			CredentialTag: 0,
			Active:        false,
			AddedSlot:     300,
		},
	}).Error)
	require.NoError(t, db.Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", 0, deregisteredKey).
		Update("active", false).Error)
	seedAccountActiveAtSlotCert(
		t,
		db,
		1,
		deregisteredKey,
		0,
		300,
		0,
		0,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, reDelegatedKey),
		models.NewStakeCredentialRef(0, deregisteredKey),
	}

	// Before the re-delegation slot the account is still active even though its
	// row added_slot (500) is greater than the query slot; before its
	// deregistration slot the other account is active too.
	got, err := store.GetAccountsActiveAtSlot(refs, 200, nil)
	require.NoError(t, err)
	assert.Contains(
		t,
		got,
		models.NewStakeCredentialRef(0, reDelegatedKey).MapKey(),
	)
	assert.Contains(
		t,
		got,
		models.NewStakeCredentialRef(0, deregisteredKey).MapKey(),
	)

	// After the re-delegation the account is still active; after the
	// deregistration the other account is inactive.
	got, err = store.GetAccountsActiveAtSlot(refs, 600, nil)
	require.NoError(t, err)
	assert.Contains(
		t,
		got,
		models.NewStakeCredentialRef(0, reDelegatedKey).MapKey(),
	)
	assert.NotContains(
		t,
		got,
		models.NewStakeCredentialRef(0, deregisteredKey).MapKey(),
	)
}

// TestGetAccountsActiveAtSlotRespectsCreatedSlot pins the finding #2 follow-up:
// an account whose row was first created after the queried slot (e.g. a reward
// account registered after an earlier reward's RUPD slot) must not be reported
// active for that earlier slot, even though it is currently active with no
// deregistration history. created_slot is immutable, so it distinguishes this
// case from a genesis-delegated account (created_slot 0, active from slot 0).
func TestGetAccountsActiveAtSlotRespectsCreatedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	lateKey := accountTestHash(0x61)
	require.NoError(t, db.Create(&models.Account{
		StakingKey:    lateKey,
		CredentialTag: 0,
		Active:        true,
		AddedSlot:     500,
		CreatedSlot:   500,
	}).Error)
	seedAccountActiveAtSlotCert(
		t,
		db,
		1,
		lateKey,
		0,
		500,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, lateKey),
	}

	// At slot 200 the registration is not yet visible, so the account falls into
	// the no-registration fallback; created_slot (500) shows it did not exist
	// yet, so it must be inactive rather than reported active-since-genesis.
	got, err := store.GetAccountsActiveAtSlot(refs, 200, nil)
	require.NoError(t, err)
	assert.NotContains(t, got, models.NewStakeCredentialRef(0, lateKey).MapKey())

	// At slot 600 the registration certificate is visible and it is active.
	got, err = store.GetAccountsActiveAtSlot(refs, 600, nil)
	require.NoError(t, err)
	assert.Contains(t, got, models.NewStakeCredentialRef(0, lateKey).MapKey())
}

// TestBackfillAccountCreatedSlot verifies the one-time migration stamps
// created_slot from the earliest registration certificate for pre-existing
// accounts (genesis-delegated accounts with no registration certificate keep 0).
func TestBackfillAccountCreatedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	registeredKey := accountTestHash(0x71)
	genesisKey := accountTestHash(0x72)
	require.NoError(t, db.Create(&[]models.Account{
		{StakingKey: registeredKey, CredentialTag: 0, Active: true, CreatedSlot: 0},
		{StakingKey: genesisKey, CredentialTag: 0, Active: true, CreatedSlot: 0},
	}).Error)
	// registeredKey has two registrations; the earliest (300) must win.
	seedAccountActiveAtSlotCert(
		t, db, 1, registeredKey, 0, 500, 0, 0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	seedAccountActiveAtSlotCert(
		t, db, 2, registeredKey, 0, 300, 0, 0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

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
	assert.Equal(t, uint64(0), genesis.CreatedSlot)
}

// TestSetTransactionStampsCreatedSlotForPhantomDeregistration pins the finding
// #3 fix: deregistering a credential that has no account row materializes a
// phantom (inactive) account, and that row must record the certificate's slot
// as created_slot, not the genesis-era default of 0. Slot-active reward lookups
// treat created_slot == 0 as "existed since chain start", so a phantom stranded
// at 0 would be wrongly counted active for reward snapshots before it appeared.
func TestSetTransactionStampsCreatedSlotForPhantomDeregistration(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := accountTestHash(0x93)
	cert := &lcommon.StakeDeregistrationCertificate{
		CertType: uint(lcommon.CertificateTypeStakeDeregistration),
		StakeCredential: lcommon.Credential{
			CredType:   0,
			Credential: lcommon.NewBlake2b224(stakeKey),
		},
	}
	tx := &mockTransaction{
		hash:         collateralFeeTestHash(0x01),
		isValid:      true,
		certificates: []lcommon.Certificate{cert},
	}
	const slot = uint64(4200)
	point := ocommon.Point{Slot: slot, Hash: accountTestHash(0xbb)}
	require.NoError(t, store.SetTransaction(tx, point, 0, nil, nil))

	var acct models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).First(&acct).Error)
	assert.False(t, acct.Active, "phantom deregistered account is inactive")
	assert.Equal(t, slot, acct.CreatedSlot,
		"phantom account must record the certificate slot, not genesis 0")
}

// TestBackfillAccountCreatedSlotIsCheckpointGated pins the finding #1 fix: the
// one-time created_slot backfill records durable completion in
// backfill_checkpoint and is skipped on later runs. This makes it crash-safe
// (an interrupted run leaves no checkpoint, so the next startup retries the
// created_slot = 0 guarded scan) and stops it re-scanning registration history
// on every startup.
func TestBackfillAccountCreatedSlotIsCheckpointGated(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	firstKey := accountTestHash(0x94)
	require.NoError(t, db.Create(&models.Account{
		StakingKey: firstKey, CredentialTag: 0, Active: true, CreatedSlot: 0,
	}).Error)
	seedAccountActiveAtSlotCert(
		t, db, 1, firstKey, 0, 400, 0, 0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	// The first run stamps the account and durably records completion.
	var first models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, firstKey,
	).First(&first).Error)
	assert.Equal(t, uint64(400), first.CreatedSlot)

	// Phase literal mirrors the unexported accountCreatedSlotBackfillPhase.
	var cp models.BackfillCheckpoint
	require.NoError(t, db.Where(
		"phase = ?", "account_created_slot",
	).First(&cp).Error)
	assert.True(t, cp.Completed)

	// A second row that predates the now-recorded backfill must NOT be stamped
	// by a later run: completion is durable, so the scan is skipped.
	secondKey := accountTestHash(0x95)
	require.NoError(t, db.Create(&models.Account{
		StakingKey: secondKey, CredentialTag: 0, Active: true, CreatedSlot: 0,
	}).Error)
	seedAccountActiveAtSlotCert(
		t, db, 2, secondKey, 0, 500, 0, 0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	require.NoError(t, models.BackfillAccountCreatedSlot(db, nil))

	var second models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, secondKey,
	).First(&second).Error)
	assert.Equal(t, uint64(0), second.CreatedSlot,
		"backfill must be skipped once durably completed")
}

// TestSaveAccountStampsCreatedSlot verifies the create/save helpers set
// Account.CreatedSlot to the creation slot on first insert (resolving the
// AccountCreatedSlotUnset sentinel) and keep it immutable across a later
// re-delegation that bumps added_slot.
func TestSaveAccountStampsCreatedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()
	key := accountTestHash(0x81)

	acct, err := store.getOrCreateAccount(0, key, nil)
	require.NoError(t, err)
	acct.AddedSlot = 700
	require.NoError(t, saveAccount(acct, db))

	var reloaded models.Account
	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, key,
	).First(&reloaded).Error)
	require.Equal(t, uint64(700), reloaded.CreatedSlot)
	require.Equal(t, uint64(700), reloaded.AddedSlot)

	// Re-delegate at a later slot: added_slot is bumped, created_slot immutable.
	acct2, err := store.getOrCreateAccount(0, key, nil)
	require.NoError(t, err)
	acct2.AddedSlot = 900
	require.NoError(t, saveAccount(acct2, db))

	require.NoError(t, db.Where(
		"credential_tag = ? AND staking_key = ?", 0, key,
	).First(&reloaded).Error)
	assert.Equal(t, uint64(700), reloaded.CreatedSlot)
	assert.Equal(t, uint64(900), reloaded.AddedSlot)
}

func TestRestoreAccountStateClearsDelegationAfterDeregistration(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := accountTestHash(0x45)
	oldPool := accountTestHash(0x46)
	currentPool := accountTestHash(0x47)

	require.NoError(t, db.Create(&models.Account{
		StakingKey: stakeKey,
		Pool:       currentPool,
		AddedSlot:  80,
		Active:     true,
	}).Error)
	seedAccountActiveAtSlotCert(
		t,
		db,
		10,
		stakeKey,
		0,
		10,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	require.NoError(t, db.Create(&models.Transaction{
		ID:         11,
		Hash:       accountTestHash(0x48),
		Slot:       20,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            11,
		TransactionID: 11,
		CertIndex:     0,
		Slot:          20,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
	}).Error)
	require.NoError(t, db.Create(&models.StakeDelegation{
		ID:            11,
		StakingKey:    stakeKey,
		PoolKeyHash:   oldPool,
		CertificateID: 11,
		AddedSlot:     20,
	}).Error)
	seedAccountActiveAtSlotCert(
		t,
		db,
		12,
		stakeKey,
		0,
		30,
		0,
		0,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	seedAccountActiveAtSlotCert(
		t,
		db,
		13,
		stakeKey,
		0,
		40,
		0,
		0,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	require.NoError(t, store.RestoreAccountStateAtSlot(50, nil))

	account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.True(t, account.Active)
	require.Empty(t, account.Pool)
}

// TestBatchFetchCerts_SameSlotTiebreakByBlockIndex pins the cert ordering
// invariant documented in CLAUDE.md: when two certs for the same staking
// key live in the same slot but in different transactions of the same
// block, batchFetchCerts must pick the one with the higher block_index.
//
// cert_index resets per transaction, so without block_index the SQL
// ORDER BY is ambiguous and the DB is free to return either row, breaking
// RestoreAccountStateAtSlot's determinism. Pool queries already follow
// "added_slot DESC, block_index DESC, cert_index DESC"; the account-side
// queries must too.
func TestBatchFetchCerts_SameSlotTiebreakByBlockIndex(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()
	const slot = uint64(1000)

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xCD
	}
	earlyPool := make([]byte, 28)
	for i := range earlyPool {
		earlyPool[i] = 0x01
	}
	latePool := make([]byte, 28)
	for i := range latePool {
		latePool[i] = 0x02
	}

	// Two transactions in the same slot; tx1 is earlier in the block
	// (block_index=0), tx2 is later (block_index=1). Both contain a
	// stake_delegation cert at cert_index=0 (cert_index resets per tx),
	// pointing to different pools.
	require.NoError(t, db.Create(&models.Transaction{
		ID:         1,
		Hash:       []byte("tx_block_index_0_hash_xxxxxxxxxx"),
		Slot:       slot,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, db.Create(&models.Transaction{
		ID:         2,
		Hash:       []byte("tx_block_index_1_hash_xxxxxxxxxx"),
		Slot:       slot,
		BlockIndex: 1,
	}).Error)

	require.NoError(t, db.Create(&models.Certificate{
		ID:            10,
		TransactionID: 1,
		CertIndex:     0,
		Slot:          slot,
		CertType:      2, // StakeDelegation
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            11,
		TransactionID: 2,
		CertIndex:     0,
		Slot:          slot,
		CertType:      2,
	}).Error)

	require.NoError(t, db.Create(&models.StakeDelegation{
		ID:            100,
		StakingKey:    stakeKey,
		PoolKeyHash:   earlyPool,
		CertificateID: 10,
		AddedSlot:     slot,
	}).Error)
	require.NoError(t, db.Create(&models.StakeDelegation{
		ID:            101,
		StakingKey:    stakeKey,
		PoolKeyHash:   latePool,
		CertificateID: 11,
		AddedSlot:     slot,
	}).Error)

	cache, err := batchFetchCerts(db, []models.StakeCredentialRef{{Tag: 0, Key: stakeKey}}, slot)
	require.NoError(t, err)

	rec, ok := cache.poolDelegation[accountCertCacheKey(0, stakeKey)]
	require.True(t, ok, "expected pool delegation for staking key")
	assert.Equal(
		t,
		latePool,
		rec.pool,
		"pool delegation must come from the cert in the higher-block_index transaction; "+
			"without block_index in the ORDER BY this picks an ambiguous row",
	)
}

func accountTestHash(fill byte) []byte {
	ret := make([]byte, 28)
	for i := range ret {
		ret[i] = fill
	}
	return ret
}

func seedAccountActiveAtSlotCert(
	t *testing.T,
	db *gorm.DB,
	id uint,
	stakingKey []byte,
	credentialTag uint8,
	slot uint64,
	blockIndex uint32,
	certIndex uint,
	certType uint,
) {
	t.Helper()
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	require.NoError(t, db.Create(&models.Transaction{
		ID:         id,
		Hash:       hash,
		Slot:       slot,
		BlockIndex: blockIndex,
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            id,
		TransactionID: id,
		CertIndex:     certIndex,
		Slot:          slot,
		CertType:      certType,
	}).Error)
	switch certType {
	case uint(lcommon.CertificateTypeStakeRegistration):
		require.NoError(t, db.Create(&models.StakeRegistration{
			ID:            id,
			StakingKey:    stakingKey,
			CredentialTag: credentialTag,
			CertificateID: id,
			AddedSlot:     slot,
		}).Error)
	case uint(lcommon.CertificateTypeStakeDeregistration):
		require.NoError(t, db.Create(&models.StakeDeregistration{
			ID:            id,
			StakingKey:    stakingKey,
			CredentialTag: credentialTag,
			CertificateID: id,
			AddedSlot:     slot,
		}).Error)
	default:
		t.Fatalf("unsupported cert type %d", certType)
	}
}

// TestBatchFetchCerts_CrossTableTiebreakByBlockIndex pins the cross-table
// merge contract behind certRecord.isMoreRecent: when same-slot pool
// delegations exist in two different certificate tables (here
// stake_delegation vs stake_vote_delegation), the in-memory cache merge
// must use block_index, not the iteration order of batchFetch* helpers,
// to pick the winner. Without block_index in certRecord/isMoreRecent the
// chosen pool depends purely on which batchFetch* runs last, which is
// non-deterministic across refactors and makes the restore path silently
// drift from the on-chain ordering.
func TestBatchFetchCerts_CrossTableTiebreakByBlockIndex(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()
	const slot = uint64(2000)

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xEF
	}
	earlyPool := make([]byte, 28)
	for i := range earlyPool {
		earlyPool[i] = 0x03
	}
	latePool := make([]byte, 28)
	for i := range latePool {
		latePool[i] = 0x04
	}
	drep := make([]byte, 28)
	for i := range drep {
		drep[i] = 0x05
	}

	// tx1 (block_index=0) carries a stake_delegation pointing at earlyPool;
	// tx2 (block_index=1) carries a stake_vote_delegation pointing at
	// latePool. Both share the same slot. The on-chain ordering is tx2,
	// so latePool must win the cross-table cache merge.
	require.NoError(t, db.Create(&models.Transaction{
		ID:         3,
		Hash:       []byte("tx_cross_table_block0_xxxxxxxxxx"),
		Slot:       slot,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, db.Create(&models.Transaction{
		ID:         4,
		Hash:       []byte("tx_cross_table_block1_xxxxxxxxxx"),
		Slot:       slot,
		BlockIndex: 1,
	}).Error)

	require.NoError(t, db.Create(&models.Certificate{
		ID:            20,
		TransactionID: 3,
		CertIndex:     0,
		Slot:          slot,
		CertType:      2, // StakeDelegation
	}).Error)
	require.NoError(t, db.Create(&models.Certificate{
		ID:            21,
		TransactionID: 4,
		CertIndex:     0,
		Slot:          slot,
		CertType:      9, // StakeVoteDelegation
	}).Error)

	require.NoError(t, db.Create(&models.StakeDelegation{
		ID:            200,
		StakingKey:    stakeKey,
		PoolKeyHash:   earlyPool,
		CertificateID: 20,
		AddedSlot:     slot,
	}).Error)
	require.NoError(t, db.Create(&models.StakeVoteDelegation{
		ID:            201,
		StakingKey:    stakeKey,
		PoolKeyHash:   latePool,
		Drep:          drep,
		DrepType:      0,
		CertificateID: 21,
		AddedSlot:     slot,
	}).Error)

	cache, err := batchFetchCerts(db, []models.StakeCredentialRef{{Tag: 0, Key: stakeKey}}, slot)
	require.NoError(t, err)

	rec, ok := cache.poolDelegation[accountCertCacheKey(0, stakeKey)]
	require.True(t, ok, "expected pool delegation for staking key")
	assert.Equal(
		t,
		latePool,
		rec.pool,
		"cross-table cache merge must pick the cert from the higher-block_index "+
			"transaction; without block_index in certRecord/isMoreRecent this "+
			"falls through to batchFetch* iteration order",
	)
}
