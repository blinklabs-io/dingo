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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAccountExpirationEpochRoundTrip pins that Account.ExpirationEpoch (the
// CIP-0163 delegator-inactivity column) persists through GORM's AutoMigrate
// and survives a create/reload round trip like the other plain-column
// account fields (e.g. DrepType).
func TestAccountExpirationEpochRoundTrip(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xEE
	}

	acct := &models.Account{
		StakingKey:      stakeKey,
		CredentialTag:   0,
		ExpirationEpoch: 123,
	}
	require.NoError(t, db.Create(acct).Error)

	var got models.Account
	require.NoError(t, db.First(&got, acct.ID).Error)
	assert.Equal(t, uint64(123), got.ExpirationEpoch)
}

// TestRenewAccountExpirations pins the CIP-0163 write primitive that sets
// expiration_epoch for a set of reward-account credentials: matching rows
// are updated in place, and a ref with no matching account row is silently
// ignored (no error, no row created), since an account must already be
// registered to have an expiration.
func TestRenewAccountExpirations(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	keyA := make([]byte, 28)
	for i := range keyA {
		keyA[i] = 0xA4
	}
	keyB := make([]byte, 28)
	for i := range keyB {
		keyB[i] = 0xB4
	}
	keyMissing := make([]byte, 28)
	for i := range keyMissing {
		keyMissing[i] = 0xC4
	}

	acctA := &models.Account{StakingKey: keyA, CredentialTag: 0}
	require.NoError(t, db.Create(acctA).Error)
	acctB := &models.Account{StakingKey: keyB, CredentialTag: 0}
	require.NoError(t, db.Create(acctB).Error)

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, keyA),
		models.NewStakeCredentialRef(0, keyB),
		models.NewStakeCredentialRef(0, keyMissing), // no row => ignored
	}
	require.NoError(t, store.RenewAccountExpirations(refs, 150, nil))

	var gotA models.Account
	require.NoError(t, db.First(&gotA, acctA.ID).Error)
	assert.Equal(t, uint64(150), gotA.ExpirationEpoch)

	var gotB models.Account
	require.NoError(t, db.First(&gotB, acctB.ID).Error)
	assert.Equal(t, uint64(150), gotB.ExpirationEpoch)

	var count int64
	require.NoError(
		t,
		db.Model(&models.Account{}).
			Where("credential_tag = ? AND staking_key = ?", 0, keyMissing).
			Count(&count).Error,
	)
	assert.Equal(t, int64(0), count, "no row should be created for a missing credential")
}

// TestRenewAccountExpirations_EmptyRefsIsNoop verifies that an empty refs
// slice is a no-op that returns nil without touching the store.
func TestRenewAccountExpirations_EmptyRefsIsNoop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	require.NoError(t, store.RenewAccountExpirations(nil, 150, nil))
}

// TestRenewAccountExpirations_Chunking exercises the set-based batch UPDATE
// across chunk boundaries: it renews more credentials than fit in a single
// chunk (renewAccountExpirationsChunkSize) in one call, interleaving both
// credential tags, and confirms every matching row is updated while a missing
// credential threaded through the batch is still silently ignored.
func TestRenewAccountExpirations_Chunking(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	// Span more than two chunks so the batching loop runs at least three times.
	const total = renewAccountExpirationsChunkSize*2 + 5
	mkKey := func(i int) []byte {
		key := make([]byte, 28)
		key[0] = byte(i >> 8)
		key[1] = byte(i)
		key[2] = 0xEE
		return key
	}

	refs := make([]models.StakeCredentialRef, 0, total+1)
	ids := make([]uint, 0, total)
	for i := range total {
		tag := uint8(i % 2) // interleave key- and script-credential tags
		key := mkKey(i)
		acct := &models.Account{StakingKey: key, CredentialTag: tag}
		require.NoError(t, db.Create(acct).Error)
		ids = append(ids, acct.ID)
		refs = append(refs, models.NewStakeCredentialRef(tag, key))
	}
	// Thread a missing credential through the batch to confirm it is ignored.
	missing := mkKey(total + 1)
	refs = append(refs, models.NewStakeCredentialRef(0, missing))

	require.NoError(t, store.RenewAccountExpirations(refs, 777, nil))

	for _, id := range ids {
		var got models.Account
		require.NoError(t, db.First(&got, id).Error)
		assert.Equal(t, uint64(777), got.ExpirationEpoch)
	}
	var count int64
	require.NoError(t,
		db.Model(&models.Account{}).
			Where("credential_tag = ? AND staking_key = ?", 0, missing).
			Count(&count).Error,
	)
	assert.Equal(t, int64(0), count, "missing credential must not be created")
}

// TestStampAllActiveAccountExpirations pins the CIP-0163 one-time activation
// write primitive: every active account is stamped to expirationEpoch,
// including an account whose pre-activation witness already started its
// inactivity clock. An inactive account is never stamped regardless of its
// expiration_epoch. Returns the count of rows actually stamped.
func TestStampAllActiveAccountExpirations(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	keyUnsetA := make([]byte, 28)
	for i := range keyUnsetA {
		keyUnsetA[i] = 0xD1
	}
	keyUnsetB := make([]byte, 28)
	for i := range keyUnsetB {
		keyUnsetB[i] = 0xD2
	}
	keyPreset := make([]byte, 28)
	for i := range keyPreset {
		keyPreset[i] = 0xD3
	}
	keyInactive := make([]byte, 28)
	for i := range keyInactive {
		keyInactive[i] = 0xD4
	}

	acctUnsetA := &models.Account{
		StakingKey:    keyUnsetA,
		CredentialTag: 0,
		Active:        true,
	}
	require.NoError(t, db.Create(acctUnsetA).Error)
	acctUnsetB := &models.Account{
		StakingKey:    keyUnsetB,
		CredentialTag: 0,
		Active:        true,
	}
	require.NoError(t, db.Create(acctUnsetB).Error)
	acctPreset := &models.Account{
		StakingKey:      keyPreset,
		CredentialTag:   0,
		Active:          true,
		ExpirationEpoch: 5,
	}
	require.NoError(t, db.Create(acctPreset).Error)
	acctInactive := &models.Account{
		StakingKey:    keyInactive,
		CredentialTag: 0,
		Active:        true,
	}
	require.NoError(t, db.Create(acctInactive).Error)
	// GORM's `default:true` on Account.Active swallows an explicit false on
	// INSERT, so deactivate via a follow-up UPDATE (see
	// TestGetAccount_ExcludesInactiveWhenIncludeInactiveFalse below).
	require.NoError(t, db.Model(&models.Account{}).
		Where("staking_key = ?", keyInactive).
		Update("active", false).Error)

	rows, err := store.StampAllActiveAccountExpirations(150, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), rows, "all three active accounts should be stamped")

	var gotA models.Account
	require.NoError(t, db.First(&gotA, acctUnsetA.ID).Error)
	assert.Equal(t, uint64(150), gotA.ExpirationEpoch)

	var gotB models.Account
	require.NoError(t, db.First(&gotB, acctUnsetB.ID).Error)
	assert.Equal(t, uint64(150), gotB.ExpirationEpoch)

	var gotPreset models.Account
	require.NoError(t, db.First(&gotPreset, acctPreset.ID).Error)
	assert.Equal(
		t,
		uint64(150),
		gotPreset.ExpirationEpoch,
		"pre-activation witness expiration must be replaced",
	)

	var gotInactive models.Account
	require.NoError(t, db.First(&gotInactive, acctInactive.ID).Error)
	assert.Equal(
		t,
		uint64(0),
		gotInactive.ExpirationEpoch,
		"inactive account must not be stamped",
	)
}

// TestStampAllActiveAccountExpirations_OverwritesExistingExpiration verifies
// that activation replaces an existing expiration so every active account gets
// the same activation window.
func TestStampAllActiveAccountExpirations_OverwritesExistingExpiration(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	stakeKey := make([]byte, 28)
	for i := range stakeKey {
		stakeKey[i] = 0xD5
	}
	require.NoError(t, db.Create(&models.Account{
		StakingKey:      stakeKey,
		CredentialTag:   0,
		Active:          true,
		ExpirationEpoch: 200,
	}).Error)

	rows, err := store.StampAllActiveAccountExpirations(150, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)

	var got models.Account
	require.NoError(t, db.Where("staking_key = ?", stakeKey).First(&got).Error)
	assert.Equal(t, uint64(150), got.ExpirationEpoch)
}

func TestResetAccountExpirationActivationReturnsClearedCredentials(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	resetKey := bytes.Repeat([]byte{0xD6}, 28)
	inactiveKey := bytes.Repeat([]byte{0xD7}, 28)
	postActivationKey := bytes.Repeat([]byte{0xD8}, 28)
	require.NoError(t, db.Create(&[]models.Account{
		{
			StakingKey: resetKey, CredentialTag: 1, Active: true,
			ExpirationEpoch: 149,
		},
		{
			StakingKey: inactiveKey, CredentialTag: 0, Active: true,
			ExpirationEpoch: 149,
		},
	}).Error)
	require.NoError(t, db.Model(&models.Account{}).
		Where("staking_key = ?", inactiveKey).
		Update("active", false).Error)
	rows, err := store.StampAllActiveAccountExpirations(150, nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)

	// This account happens to carry the activation value but was created after
	// activation, so it is not in the durable membership set and must not reset.
	require.NoError(t, db.Create(&models.Account{
		StakingKey: postActivationKey, Active: true, ExpirationEpoch: 150,
	}).Error)

	refs, err := store.ResetAccountExpirationActivation(nil)
	require.NoError(t, err)
	require.Equal(t, []models.StakeCredentialRef{
		models.NewStakeCredentialRef(1, resetKey),
	}, refs)

	var reset, inactive, postActivation models.Account
	require.NoError(t, db.Where("staking_key = ?", resetKey).First(&reset).Error)
	require.NoError(t, db.Where("staking_key = ?", inactiveKey).First(&inactive).Error)
	require.NoError(t, db.Where("staking_key = ?", postActivationKey).
		First(&postActivation).Error)
	assert.Zero(t, reset.ExpirationEpoch)
	assert.Equal(t, uint64(149), inactive.ExpirationEpoch)
	assert.Equal(t, uint64(150), postActivation.ExpirationEpoch)
}

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

// witnessTestKey builds a 28-byte credential hash filled with b.
func witnessTestKey(b byte) []byte {
	out := make([]byte, 28)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestAccountLastWitnessSlots pins the CIP-0163 rollback query surface: for each
// requested credential it returns the greatest witnessing added_slot <= maxSlot
// across the stake-witnessing certificate tables AND the reward-withdrawal
// history (account_reward_delta where withdrawal = TRUE). The brief example:
// a delegation cert at slot s1 and a reward withdrawal at s2 > s1. maxSlot=s2
// must return s2 (the withdrawal wins); maxSlot=s1 must return s1 (the later
// withdrawal is excluded); a maxSlot before any witness leaves the credential
// absent from the map.
func TestAccountLastWitnessSlots(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	key := witnessTestKey(0xA1)
	ref := models.NewStakeCredentialRef(0, key)
	const (
		s1 = uint64(1000)
		s2 = uint64(2000)
	)

	// Delegation cert (a stake-witnessing cert) at slot s1.
	require.NoError(t, db.Create(&models.StakeDelegation{
		StakingKey:    key,
		CredentialTag: 0,
		PoolKeyHash:   witnessTestKey(0x0A),
		AddedSlot:     s1,
	}).Error)
	// Reward withdrawal (also a CIP witnessing action) at slot s2 > s1.
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey:    key,
		CredentialTag: 0,
		TxHash:        make([]byte, 32),
		Amount:        types.Uint64(1_000),
		AddedSlot:     s2,
		Withdrawal:    true,
	}).Error)

	refs := []models.StakeCredentialRef{ref}

	// maxSlot = s2: the withdrawal at s2 is the latest witness <= s2.
	got, err := store.AccountLastWitnessSlots(refs, s2, nil)
	require.NoError(t, err)
	assert.Equal(t, s2, got[ref.MapKey()])

	// maxSlot = s1: the withdrawal at s2 is excluded; the cert at s1 wins.
	got, err = store.AccountLastWitnessSlots(refs, s1, nil)
	require.NoError(t, err)
	assert.Equal(t, s1, got[ref.MapKey()])

	// maxSlot before any witness: absent from the map.
	got, err = store.AccountLastWitnessSlots(refs, s1-1, nil)
	require.NoError(t, err)
	_, present := got[ref.MapKey()]
	assert.False(t, present, "no witness <= maxSlot means the credential is absent")
}

// TestAccountLastWitnessSlots_TagIsolation verifies that two credentials sharing
// a 28-byte hash but differing in tag (key vs script) keep separate last-witness
// slots, matching the composite (tag, key) identity used throughout the account
// tables.
func TestAccountLastWitnessSlots_TagIsolation(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	hash := witnessTestKey(0xB2)
	keyRef := models.NewStakeCredentialRef(0, hash)
	scriptRef := models.NewStakeCredentialRef(1, hash)

	require.NoError(t, db.Create(&models.StakeRegistration{
		StakingKey:    hash,
		CredentialTag: 0,
		AddedSlot:     100,
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistration{
		StakingKey:    hash,
		CredentialTag: 1,
		AddedSlot:     200,
	}).Error)

	got, err := store.AccountLastWitnessSlots(
		[]models.StakeCredentialRef{keyRef, scriptRef},
		1000,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(100), got[keyRef.MapKey()])
	assert.Equal(t, uint64(200), got[scriptRef.MapKey()])
}

// TestAccountLastWitnessSlots_EmptyRefsIsNoop verifies an empty refs slice
// returns an empty map without error.
func TestAccountLastWitnessSlots_EmptyRefsIsNoop(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	got, err := store.AccountLastWitnessSlots(nil, 1000, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestZeroAmountWithdrawalWitnessHistory(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()
	key := witnessTestKey(0xBC)
	ref := models.NewStakeCredentialRef(0, key)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey, lcommon.AddressNetworkTestnet, nil, key,
	)
	require.NoError(t, err)
	require.NoError(t, store.applyTransactionRewardWithdrawals(
		map[*lcommon.Address]*big.Int{&addr: big.NewInt(0)},
		2000, make([]byte, 32), nil,
	))
	var witnessCount int64
	require.NoError(t, db.Model(&models.AccountWithdrawalWitness{}).Count(&witnessCount).Error)
	require.Equal(t, int64(1), witnessCount)

	last, err := store.AccountLastWitnessSlots([]models.StakeCredentialRef{ref}, 2000, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2000), last[ref.MapKey()])
	affected, err := store.AccountsWitnessedAfterSlot(1500, nil)
	require.NoError(t, err)
	require.Equal(t, []models.StakeCredentialRef{ref}, affected)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(1500, nil))
	affected, err = store.AccountsWitnessedAfterSlot(1500, nil)
	require.NoError(t, err)
	require.Empty(t, affected)
}

// TestAccountsWitnessedAfterSlot pins the CIP-0163 rollback affected-set query:
// it returns every credential with a stake-witnessing cert OR a reward
// withdrawal at added_slot > slot. A cert-only credential, a withdrawal-only
// credential, and a credential witnessed only at/before the slot exercise the
// three cases.
func TestAccountsWitnessedAfterSlot(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	db := store.DB()

	certKey := witnessTestKey(0xC1) // cert at slot 2000
	wdrKey := witnessTestKey(0xC2)  // withdrawal at slot 2500
	oldKey := witnessTestKey(0xC3)  // cert at slot 500 only
	certRef := models.NewStakeCredentialRef(0, certKey)
	wdrRef := models.NewStakeCredentialRef(0, wdrKey)

	require.NoError(t, db.Create(&models.StakeDelegation{
		StakingKey:    certKey,
		CredentialTag: 0,
		PoolKeyHash:   witnessTestKey(0x0A),
		AddedSlot:     2000,
	}).Error)
	require.NoError(t, db.Create(&models.AccountRewardDelta{
		StakingKey:    wdrKey,
		CredentialTag: 0,
		TxHash:        make([]byte, 32),
		Amount:        types.Uint64(5),
		AddedSlot:     2500,
		Withdrawal:    true,
	}).Error)
	require.NoError(t, db.Create(&models.StakeRegistration{
		StakingKey:    oldKey,
		CredentialTag: 0,
		AddedSlot:     500,
	}).Error)

	// Slot 1500: both the cert (2000) and the withdrawal (2500) are after it;
	// the old cert (500) is not.
	got, err := store.AccountsWitnessedAfterSlot(1500, nil)
	require.NoError(t, err)
	keys := make(map[string]struct{}, len(got))
	for _, r := range got {
		keys[r.MapKey()] = struct{}{}
	}
	_, hasCert := keys[certRef.MapKey()]
	_, hasWdr := keys[wdrRef.MapKey()]
	_, hasOld := keys[models.NewStakeCredentialRef(0, oldKey).MapKey()]
	assert.True(t, hasCert, "cert witnessed at 2000 must be in the affected set for slot 1500")
	assert.True(t, hasWdr, "withdrawal witnessed at 2500 must be in the affected set for slot 1500")
	assert.False(t, hasOld, "cert witnessed at 500 must not be in the affected set for slot 1500")

	// Slot 2500: nothing is strictly after it.
	got, err = store.AccountsWitnessedAfterSlot(2500, nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}
