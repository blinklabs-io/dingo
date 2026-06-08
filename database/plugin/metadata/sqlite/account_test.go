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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAccount_ExcludesInactiveWhenIncludeInactiveFalse pins the
// includeInactive=false branch of GetAccount: a row with active=false must
// not be returned. This is the semantic the
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

	got, err := store.GetAccount(stakeKey, false /* includeInactive */, nil)
	require.NoError(t, err)
	assert.Nil(t, got, "inactive account must not be returned when includeInactive=false")

	got, err = store.GetAccount(stakeKey, true /* includeInactive */, nil)
	require.NoError(t, err)
	require.NotNil(t, got, "inactive account must be returned when includeInactive=true")
	assert.False(t, got.Active, "returned account should have Active=false")
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

	cache, err := batchFetchCerts(db, [][]byte{stakeKey}, slot)
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

	cache, err := batchFetchCerts(db, [][]byte{stakeKey}, slot)
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
