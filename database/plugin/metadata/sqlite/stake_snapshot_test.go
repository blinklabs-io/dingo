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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func setupStakeSnapshotTestStore(t *testing.T) *MetadataStoreSqlite {
	t.Helper()
	sqliteStore, err := New("", nil, nil)
	require.NoError(t, err, "failed to create sqlite store")
	require.NoError(t, sqliteStore.Start(), "failed to start sqlite store")
	require.NoError(
		t,
		sqliteStore.DB().AutoMigrate(models.MigrateModels...),
		"failed to auto-migrate",
	)
	return sqliteStore
}

// TestPoolStakeSnapshotSave tests saving a single pool stake snapshot
func TestPoolStakeSnapshotSave(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	poolKeyHash := []byte("pool_key_hash_12345678901234")
	snapshot := &models.PoolStakeSnapshot{
		Epoch:          100,
		SnapshotType:   "go",
		PoolKeyHash:    poolKeyHash,
		TotalStake:     1000000000000,
		DelegatorCount: 500,
		CapturedSlot:   4320000,
	}

	err := store.SavePoolStakeSnapshot(snapshot, nil)
	require.NoError(t, err, "failed to save pool stake snapshot")
	assert.NotZero(t, snapshot.ID, "expected snapshot ID to be set after save")
}

// TestPoolStakeSnapshotGet tests retrieving a specific pool stake snapshot
func TestPoolStakeSnapshotGet(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	poolKeyHash := []byte("pool_key_hash_12345678901234")
	snapshot := &models.PoolStakeSnapshot{
		Epoch:          100,
		SnapshotType:   "go",
		PoolKeyHash:    poolKeyHash,
		TotalStake:     1000000000000,
		DelegatorCount: 500,
		CapturedSlot:   4320000,
	}
	require.NoError(t, store.SavePoolStakeSnapshot(snapshot, nil))

	retrieved, err := store.GetPoolStakeSnapshot(100, "go", poolKeyHash, nil)
	require.NoError(t, err, "failed to get pool stake snapshot")
	require.NotNil(t, retrieved, "expected to retrieve snapshot")
	assert.Equal(t, uint64(1000000000000), uint64(retrieved.TotalStake))
	assert.Equal(t, uint64(500), retrieved.DelegatorCount)
	assert.Equal(t, uint64(4320000), retrieved.CapturedSlot)
}

// TestPoolStakeSnapshotGetNotFound tests retrieval when snapshot does not exist
func TestPoolStakeSnapshotGetNotFound(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	poolKeyHash := []byte("pool_key_hash_12345678901234")
	notFound, err := store.GetPoolStakeSnapshot(999, "go", poolKeyHash, nil)
	require.NoError(t, err, "unexpected error for not found")
	assert.Nil(t, notFound, "expected nil for not found snapshot")
}

// TestPoolStakeSnapshotsSaveBatch tests saving multiple snapshots in batch
func TestPoolStakeSnapshotsSaveBatch(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_11111111111111"),
			TotalStake:     500000000000,
			DelegatorCount: 200,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_22222222222222"),
			TotalStake:     750000000000,
			DelegatorCount: 300,
			CapturedSlot:   4320000,
		},
	}

	err := store.SavePoolStakeSnapshots(snapshots, nil)
	require.NoError(t, err, "failed to save pool stake snapshots batch")

	for _, s := range snapshots {
		assert.NotZero(t, s.ID, "expected snapshot ID to be set after batch save")
	}
}

// TestPoolStakeSnapshotsSaveBatchEmpty tests that empty batch save works
func TestPoolStakeSnapshotsSaveBatchEmpty(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	err := store.SavePoolStakeSnapshots([]*models.PoolStakeSnapshot{}, nil)
	require.NoError(t, err, "empty batch should not error")
}

// TestPoolStakeSnapshotsGetByEpoch tests retrieving all snapshots for an epoch
func TestPoolStakeSnapshotsGetByEpoch(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_11111111111111"),
			TotalStake:     500000000000,
			DelegatorCount: 200,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_22222222222222"),
			TotalStake:     750000000000,
			DelegatorCount: 300,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "set",
			PoolKeyHash:    []byte("pool_key_hash_33333333333333"),
			TotalStake:     600000000000,
			DelegatorCount: 250,
			CapturedSlot:   4320000,
		},
	}
	require.NoError(t, store.SavePoolStakeSnapshots(snapshots, nil))

	// Get "go" snapshots for epoch 100
	goSnapshots, err := store.GetPoolStakeSnapshotsByEpoch(100, "go", nil)
	require.NoError(t, err, "failed to get pool stake snapshots by epoch")
	assert.Len(t, goSnapshots, 2, "expected 2 'go' snapshots")

	// Get "set" snapshots for epoch 100
	setSnapshots, err := store.GetPoolStakeSnapshotsByEpoch(100, "set", nil)
	require.NoError(t, err)
	assert.Len(t, setSnapshots, 1, "expected 1 'set' snapshot")
}

// TestGetTotalActiveStake tests summing all pool stakes for an epoch
func TestGetTotalActiveStake(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_11111111111111"),
			TotalStake:     1000000000000,
			DelegatorCount: 100,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_22222222222222"),
			TotalStake:     500000000000,
			DelegatorCount: 200,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_33333333333333"),
			TotalStake:     750000000000,
			DelegatorCount: 300,
			CapturedSlot:   4320000,
		},
	}
	require.NoError(t, store.SavePoolStakeSnapshots(snapshots, nil))

	total, err := store.GetTotalActiveStake(100, "go", nil)
	require.NoError(t, err, "failed to get total active stake")
	// 1000000000000 + 500000000000 + 750000000000 = 2250000000000
	assert.Equal(t, uint64(2250000000000), total)
}

// TestGetTotalActiveStakeNoSnapshots tests GetTotalActiveStake when no snapshots exist
func TestGetTotalActiveStakeNoSnapshots(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	total, err := store.GetTotalActiveStake(999, "go", nil)
	require.NoError(t, err, "unexpected error for empty epoch")
	assert.Equal(t, uint64(0), total, "expected 0 for empty epoch")
}

// TestEpochSummarySave tests saving an epoch summary
func TestEpochSummarySave(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	summary := &models.EpochSummary{
		Epoch:            100,
		TotalActiveStake: 30000000000000000,
		TotalPoolCount:   3000,
		TotalDelegators:  1200000,
		EpochNonce:       []byte("nonce_123456789012345678901234"),
		BoundarySlot:     4320000,
		SnapshotReady:    true,
	}

	err := store.SaveEpochSummary(summary, nil)
	require.NoError(t, err, "failed to save epoch summary")
	assert.NotZero(t, summary.ID, "expected summary ID to be set after save")
}

// TestEpochSummaryGet tests retrieving an epoch summary by epoch number
func TestEpochSummaryGet(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	summary := &models.EpochSummary{
		Epoch:            100,
		TotalActiveStake: 30000000000000000,
		TotalPoolCount:   3000,
		TotalDelegators:  1200000,
		EpochNonce:       []byte("nonce_123456789012345678901234"),
		BoundarySlot:     4320000,
		SnapshotReady:    true,
	}
	require.NoError(t, store.SaveEpochSummary(summary, nil))

	retrieved, err := store.GetEpochSummary(100, nil)
	require.NoError(t, err, "failed to get epoch summary")
	require.NotNil(t, retrieved, "expected to retrieve summary")
	assert.Equal(t, uint64(3000), retrieved.TotalPoolCount)
	assert.Equal(t, uint64(1200000), retrieved.TotalDelegators)
	assert.True(t, retrieved.SnapshotReady)
}

// TestEpochSummaryGetNotFound tests retrieval when summary does not exist
func TestEpochSummaryGetNotFound(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	notFound, err := store.GetEpochSummary(999, nil)
	require.NoError(t, err, "unexpected error for not found")
	assert.Nil(t, notFound, "expected nil for not found summary")
}

// TestEpochSummaryGetLatest tests retrieving the most recent epoch summary
func TestEpochSummaryGetLatest(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	summaries := []*models.EpochSummary{
		{
			Epoch:            100,
			TotalActiveStake: 30000000000000000,
			TotalPoolCount:   3000,
			TotalDelegators:  1200000,
			BoundarySlot:     4320000,
			SnapshotReady:    true,
		},
		{
			Epoch:            101,
			TotalActiveStake: 31000000000000000,
			TotalPoolCount:   3050,
			TotalDelegators:  1210000,
			BoundarySlot:     4363200,
			SnapshotReady:    false,
		},
		{
			Epoch:            102,
			TotalActiveStake: 32000000000000000,
			TotalPoolCount:   3100,
			TotalDelegators:  1220000,
			BoundarySlot:     4406400,
			SnapshotReady:    true,
		},
	}
	for _, s := range summaries {
		require.NoError(t, store.SaveEpochSummary(s, nil))
	}

	latest, err := store.GetLatestEpochSummary(nil)
	require.NoError(t, err, "failed to get latest epoch summary")
	require.NotNil(t, latest, "expected to retrieve latest summary")
	assert.Equal(t, uint64(102), latest.Epoch)
}

// TestEpochSummaryGetLatestEmpty tests GetLatestEpochSummary when no summaries exist
func TestEpochSummaryGetLatestEmpty(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	latest, err := store.GetLatestEpochSummary(nil)
	require.NoError(t, err, "unexpected error for empty table")
	assert.Nil(t, latest, "expected nil for empty table")
}

// TestDeletePoolStakeSnapshotsForEpoch tests deleting snapshots for a specific epoch
func TestDeletePoolStakeSnapshotsForEpoch(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_11111111111111"),
			TotalStake:     500000000000,
			DelegatorCount: 200,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_22222222222222"),
			TotalStake:     750000000000,
			DelegatorCount: 300,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          100,
			SnapshotType:   "set",
			PoolKeyHash:    []byte("pool_key_hash_33333333333333"),
			TotalStake:     600000000000,
			DelegatorCount: 250,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          101,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_44444444444444"),
			TotalStake:     800000000000,
			DelegatorCount: 400,
			CapturedSlot:   4363200,
		},
	}
	require.NoError(t, store.SavePoolStakeSnapshots(snapshots, nil))

	// Delete "go" snapshots for epoch 100
	err := store.DeletePoolStakeSnapshotsForEpoch(100, "go", nil)
	require.NoError(t, err, "failed to delete pool stake snapshots for epoch")

	// Verify "go" snapshots for epoch 100 are deleted
	goSnapshots100, err := store.GetPoolStakeSnapshotsByEpoch(100, "go", nil)
	require.NoError(t, err)
	assert.Len(t, goSnapshots100, 0, "expected 0 'go' snapshots for epoch 100")

	// Verify "set" snapshot for epoch 100 still exists
	setSnapshots100, err := store.GetPoolStakeSnapshotsByEpoch(100, "set", nil)
	require.NoError(t, err)
	assert.Len(t, setSnapshots100, 1, "expected 1 'set' snapshot for epoch 100")

	// Verify "go" snapshot for epoch 101 still exists
	goSnapshots101, err := store.GetPoolStakeSnapshotsByEpoch(101, "go", nil)
	require.NoError(t, err)
	assert.Len(t, goSnapshots101, 1, "expected 1 'go' snapshot for epoch 101")
}

// TestDeletePoolStakeSnapshotsAfterEpoch tests deleting snapshots after a given epoch
func TestDeletePoolStakeSnapshotsAfterEpoch(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	snapshots := []*models.PoolStakeSnapshot{
		{
			Epoch:          100,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_11111111111111"),
			TotalStake:     500000000000,
			DelegatorCount: 200,
			CapturedSlot:   4320000,
		},
		{
			Epoch:          101,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_22222222222222"),
			TotalStake:     600000000000,
			DelegatorCount: 250,
			CapturedSlot:   4363200,
		},
		{
			Epoch:          102,
			SnapshotType:   "go",
			PoolKeyHash:    []byte("pool_key_hash_33333333333333"),
			TotalStake:     700000000000,
			DelegatorCount: 300,
			CapturedSlot:   4406400,
		},
		{
			Epoch:          103,
			SnapshotType:   "set",
			PoolKeyHash:    []byte("pool_key_hash_44444444444444"),
			TotalStake:     800000000000,
			DelegatorCount: 350,
			CapturedSlot:   4449600,
		},
	}
	require.NoError(t, store.SavePoolStakeSnapshots(snapshots, nil))

	// Delete all snapshots after epoch 100
	err := store.DeletePoolStakeSnapshotsAfterEpoch(100, nil)
	require.NoError(t, err, "failed to delete pool stake snapshots after epoch")

	// Verify epoch 100 snapshots still exist
	snapshots100, err := store.GetPoolStakeSnapshotsByEpoch(100, "go", nil)
	require.NoError(t, err)
	assert.Len(t, snapshots100, 1, "expected 1 snapshot for epoch 100")

	// Verify epoch 101, 102, 103 snapshots are deleted
	snapshots101, err := store.GetPoolStakeSnapshotsByEpoch(101, "go", nil)
	require.NoError(t, err)
	assert.Len(t, snapshots101, 0, "expected 0 snapshots for epoch 101")

	snapshots102, err := store.GetPoolStakeSnapshotsByEpoch(102, "go", nil)
	require.NoError(t, err)
	assert.Len(t, snapshots102, 0, "expected 0 snapshots for epoch 102")

	snapshots103, err := store.GetPoolStakeSnapshotsByEpoch(103, "set", nil)
	require.NoError(t, err)
	assert.Len(t, snapshots103, 0, "expected 0 snapshots for epoch 103")
}

// TestDeleteEpochSummariesAfterEpoch tests deleting summaries after a given epoch
func TestDeleteEpochSummariesAfterEpoch(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	summaries := []*models.EpochSummary{
		{
			Epoch:            100,
			TotalActiveStake: 30000000000000000,
			TotalPoolCount:   3000,
			TotalDelegators:  1200000,
			BoundarySlot:     4320000,
			SnapshotReady:    true,
		},
		{
			Epoch:            101,
			TotalActiveStake: 31000000000000000,
			TotalPoolCount:   3050,
			TotalDelegators:  1210000,
			BoundarySlot:     4363200,
			SnapshotReady:    true,
		},
		{
			Epoch:            102,
			TotalActiveStake: 32000000000000000,
			TotalPoolCount:   3100,
			TotalDelegators:  1220000,
			BoundarySlot:     4406400,
			SnapshotReady:    true,
		},
		{
			Epoch:            103,
			TotalActiveStake: 33000000000000000,
			TotalPoolCount:   3150,
			TotalDelegators:  1230000,
			BoundarySlot:     4449600,
			SnapshotReady:    false,
		},
	}
	for _, s := range summaries {
		require.NoError(t, store.SaveEpochSummary(s, nil))
	}

	// Delete all summaries after epoch 101
	err := store.DeleteEpochSummariesAfterEpoch(101, nil)
	require.NoError(t, err, "failed to delete epoch summaries after epoch")

	// Verify epoch 100 and 101 still exist
	summary100, err := store.GetEpochSummary(100, nil)
	require.NoError(t, err)
	assert.NotNil(t, summary100, "expected epoch 100 summary to exist")

	summary101, err := store.GetEpochSummary(101, nil)
	require.NoError(t, err)
	assert.NotNil(t, summary101, "expected epoch 101 summary to exist")

	// Verify epoch 102 and 103 are deleted
	summary102, err := store.GetEpochSummary(102, nil)
	require.NoError(t, err)
	assert.Nil(t, summary102, "expected epoch 102 summary to be deleted")

	summary103, err := store.GetEpochSummary(103, nil)
	require.NoError(t, err)
	assert.Nil(t, summary103, "expected epoch 103 summary to be deleted")

	// Verify GetLatestEpochSummary returns epoch 101
	latest, err := store.GetLatestEpochSummary(nil)
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, uint64(101), latest.Epoch)
}

// TestGetStakeByPoolsAggregatesUtxos tests that GetStakeByPools correctly
// sums live UTxO amounts per pool by joining accounts with their UTxOs.
func TestGetStakeByPoolsAggregatesUtxos(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()

	// Pool key hashes (28 bytes each)
	poolA := []byte("pool_A_1234567890123456789012")
	poolB := []byte("pool_B_1234567890123456789012")

	// Staking keys (28 bytes each)
	stakeKeyAlice := []byte("alice_stakekey_1234567890123")
	stakeKeyBob := []byte("bob___stakekey_1234567890123")
	stakeKeyCarol := []byte("carol_stakekey_1234567890123")

	// Create accounts delegated to pools
	accounts := []models.Account{
		{StakingKey: stakeKeyAlice, Pool: poolA, AddedSlot: 100, Active: true},
		{StakingKey: stakeKeyBob, Pool: poolA, AddedSlot: 100, Active: true},
		{StakingKey: stakeKeyCarol, Pool: poolB, AddedSlot: 100, Active: true},
	}
	for i := range accounts {
		require.NoError(t, db.Create(&accounts[i]).Error, "create account")
	}

	// Create live UTxOs for these staking keys (deleted_slot = 0 means unspent)
	utxos := []models.Utxo{
		// Alice has 2 UTxOs: 5 ADA + 3 ADA = 8 ADA total
		{TxId: []byte("tx01_234567890123456789012345678901"), OutputIdx: 0, StakingKey: stakeKeyAlice, Amount: 5000000, AddedSlot: 100, DeletedSlot: 0},
		{TxId: []byte("tx02_234567890123456789012345678901"), OutputIdx: 0, StakingKey: stakeKeyAlice, Amount: 3000000, AddedSlot: 200, DeletedSlot: 0},
		// Bob has 1 UTxO: 10 ADA
		{TxId: []byte("tx03_234567890123456789012345678901"), OutputIdx: 0, StakingKey: stakeKeyBob, Amount: 10000000, AddedSlot: 100, DeletedSlot: 0},
		// Bob also has a spent UTxO that should NOT be counted
		{TxId: []byte("tx04_234567890123456789012345678901"), OutputIdx: 0, StakingKey: stakeKeyBob, Amount: 7000000, AddedSlot: 50, DeletedSlot: 150},
		// Carol has 1 UTxO: 20 ADA
		{TxId: []byte("tx05_234567890123456789012345678901"), OutputIdx: 0, StakingKey: stakeKeyCarol, Amount: 20000000, AddedSlot: 100, DeletedSlot: 0},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error, "create utxo")
	}

	// Query stake for both pools
	stakes, delegators, err := store.GetStakeByPools(
		[][]byte{poolA, poolB},
		nil,
	)
	require.NoError(t, err, "GetStakeByPools failed")

	// Pool A: Alice (5M + 3M) + Bob (10M) = 18M lovelace
	require.Equal(t, uint64(18000000), stakes[string(poolA)],
		"pool A stake should sum live UTxOs for Alice and Bob")

	// Pool B: Carol (20M) = 20M lovelace
	require.Equal(t, uint64(20000000), stakes[string(poolB)],
		"pool B stake should sum live UTxOs for Carol")

	// Delegator counts
	require.Equal(t, uint64(2), delegators[string(poolA)],
		"pool A should have 2 delegators")
	require.Equal(t, uint64(1), delegators[string(poolB)],
		"pool B should have 1 delegator")
}

// TestGetStakeByPoolsExcludesInactiveAccounts tests that inactive accounts
// are excluded from stake aggregation.
func TestGetStakeByPoolsExcludesInactiveAccounts(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	db := store.DB()

	poolA := []byte("pool_A_1234567890123456789012")
	stakeActive := []byte("active_stakekey_123456789012")
	stakeInactive := []byte("inactv_stakekey_123456789012")

	// One active account, one inactive.
	// Note: GORM's Create skips zero-value fields when the model has
	// a `default` tag, so Active: false would be stored as true.
	// Use a map-based update after creation to set Active = false.
	require.NoError(t, db.Create(&models.Account{
		StakingKey: stakeActive, Pool: poolA, AddedSlot: 100, Active: true,
	}).Error)
	inactiveAccount := models.Account{
		StakingKey: stakeInactive, Pool: poolA, AddedSlot: 100, Active: true,
	}
	require.NoError(t, db.Create(&inactiveAccount).Error)
	require.NoError(t, db.Model(&inactiveAccount).Update("active", false).Error)

	// Both have UTxOs
	require.NoError(t, db.Create(&models.Utxo{
		TxId: []byte("tx01_active_678901234567890123456789"), OutputIdx: 0,
		StakingKey: stakeActive, Amount: 5000000, AddedSlot: 100,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId: []byte("tx02_inactv_678901234567890123456789"), OutputIdx: 0,
		StakingKey: stakeInactive, Amount: 9000000, AddedSlot: 100,
	}).Error)

	stakes, delegators, err := store.GetStakeByPools([][]byte{poolA}, nil)
	require.NoError(t, err)

	// Only active account's UTxO counted
	require.Equal(t, uint64(5000000), stakes[string(poolA)],
		"only active account stake should be counted")
	require.Equal(t, uint64(1), delegators[string(poolA)],
		"only active account should be counted as delegator")
}

// TestGetStakeByPoolsNoDelegators tests that pools with no delegators
// return zero stake.
func TestGetStakeByPoolsNoDelegators(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	poolEmpty := []byte("pool_E_1234567890123456789012")

	stakes, delegators, err := store.GetStakeByPools(
		[][]byte{poolEmpty},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), stakes[string(poolEmpty)])
	require.Equal(t, uint64(0), delegators[string(poolEmpty)])
}

// TestSnapshotTypesMarkSetGo tests all three snapshot types
func TestSnapshotTypesMarkSetGo(t *testing.T) {
	store := setupStakeSnapshotTestStore(t)
	defer store.Close() //nolint:errcheck

	poolKeyHash := []byte("pool_key_hash_12345678901234")

	// Create snapshots for all three types
	snapshotTypes := []string{"mark", "set", "go"}
	for i, snapshotType := range snapshotTypes {
		snapshot := &models.PoolStakeSnapshot{
			Epoch:          100,
			SnapshotType:   snapshotType,
			PoolKeyHash:    poolKeyHash,
			TotalStake:     types.Uint64((i + 1) * 1000000000000),
			DelegatorCount: uint64((i + 1) * 100),
			CapturedSlot:   4320000,
		}
		require.NoError(t, store.SavePoolStakeSnapshot(snapshot, nil))
	}

	// Verify each type can be retrieved independently
	for i, snapshotType := range snapshotTypes {
		retrieved, err := store.GetPoolStakeSnapshot(
			100,
			snapshotType,
			poolKeyHash,
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, retrieved, "expected snapshot for type %s", snapshotType)
		expectedStake := uint64((i + 1) * 1000000000000)
		assert.Equal(
			t,
			expectedStake,
			uint64(retrieved.TotalStake),
			"stake mismatch for type %s",
			snapshotType,
		)
	}
}
