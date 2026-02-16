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

package snapshot

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// setupTestDB creates a database.Database backed by in-memory SQLite for
// testing, and returns the database along with the underlying SQLite store
// for direct data seeding. The caller should defer db.Close().
func setupTestDB(t *testing.T) (*database.Database, *sqlite.MetadataStoreSqlite) {
	t.Helper()
	tmpDir := t.TempDir()

	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
	})
	require.NoError(t, err, "create database")
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	// Get the underlying SQLite store for direct data seeding
	meta := db.Metadata()
	sqliteStore, ok := meta.(*sqlite.MetadataStoreSqlite)
	require.True(t, ok, "metadata store should be SQLite")

	return db, sqliteStore
}

// seedPoolAndDelegations creates a pool, accounts, and UTxOs for testing
// stake distribution calculations.
func seedPoolAndDelegations(
	t *testing.T,
	sqliteStore *sqlite.MetadataStoreSqlite,
	poolKeyHash []byte,
	delegations []struct {
		stakingKey []byte
		utxoAmounts []types.Uint64
	},
	slot uint64,
) {
	t.Helper()
	gormDB := sqliteStore.DB()

	// Create pool and registration
	pool := models.Pool{
		PoolKeyHash: poolKeyHash,
	}
	require.NoError(t, gormDB.Create(&pool).Error, "create pool")

	reg := models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKeyHash,
		AddedSlot:   slot,
		Pledge:      1000000,
		Cost:        340000000,
		Margin: &types.Rat{
			Rat: big.NewRat(1, 100),
		},
		VrfKeyHash:    make([]byte, 32),
		RewardAccount: make([]byte, 28),
	}
	require.NoError(t, gormDB.Create(&reg).Error, "create pool registration")

	// Create accounts and their UTxOs
	for i, d := range delegations {
		account := models.Account{
			StakingKey: d.stakingKey,
			Pool:       poolKeyHash,
			AddedSlot:  slot,
			Active:     true,
		}
		require.NoError(t, gormDB.Create(&account).Error, "create account %d", i)

		for j, amount := range d.utxoAmounts {
			// Construct a unique 32-byte tx hash using pool hash,
			// delegator index, and utxo index for uniqueness
			txId := make([]byte, 32)
			copy(txId, poolKeyHash[:min(len(poolKeyHash), 28)])
			txId[28] = byte(i)
			txId[29] = byte(j)
			txId[30] = byte(i ^ j)
			txId[31] = byte(i + j + 1)

			utxo := models.Utxo{
				TxId:       txId,
				OutputIdx:  uint32(j),
				StakingKey: d.stakingKey,
				Amount:     amount,
				AddedSlot:  slot,
				// DeletedSlot = 0 means live/unspent
			}
			require.NoError(t, gormDB.Create(&utxo).Error, "create utxo")
		}
	}
}

// TestCalculateStakeDistribution_NonZeroStake verifies that the calculator
// returns non-zero stake values when delegation data and live UTxOs exist.
// This is a regression test for the critical bug where GetStakeByPools
// returned zero for all pools, blocking block production.
func TestCalculateStakeDistribution_NonZeroStake(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Seed epoch data (required for GetActivePoolKeyHashesAtSlot)
	epoch := models.Epoch{
		EpochId:       10,
		StartSlot:     0,
		LengthInSlots: 432000,
	}
	require.NoError(t, gormDB.Create(&epoch).Error, "create epoch")

	// Pool A: 28-byte key hash
	poolAHash := []byte("poolA_12345678901234567890AB")

	// Seed Pool A with two delegators
	seedPoolAndDelegations(t, sqliteStore, poolAHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			// Alice: 5 ADA + 3 ADA = 8 ADA
			stakingKey:  []byte("alice_staking_key_1234567890"),
			utxoAmounts: []types.Uint64{5000000, 3000000},
		},
		{
			// Bob: 10 ADA
			stakingKey:  []byte("bob___staking_key_1234567890"),
			utxoAmounts: []types.Uint64{10000000},
		},
	}, 500)

	// Pool B: 28-byte key hash
	poolBHash := []byte("poolB_12345678901234567890AB")

	// Seed Pool B with one delegator
	seedPoolAndDelegations(t, sqliteStore, poolBHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			// Carol: 20 ADA
			stakingKey:  []byte("carol_staking_key_1234567890"),
			utxoAmounts: []types.Uint64{20000000},
		},
	}, 500)

	// Calculate stake distribution at slot 1000
	calc := NewCalculator(db)
	dist, err := calc.CalculateStakeDistribution(context.Background(), 1000)
	require.NoError(t, err, "CalculateStakeDistribution should not fail")

	// Verify non-zero total stake (the critical assertion)
	require.NotZero(t, dist.TotalStake,
		"CRITICAL: TotalStake must not be zero when delegations exist")

	// Verify total pools
	require.Equal(t, uint64(2), dist.TotalPools,
		"expected 2 active pools")

	// Verify per-pool stakes
	var poolAKey lcommon.PoolKeyHash
	copy(poolAKey[:], poolAHash)
	var poolBKey lcommon.PoolKeyHash
	copy(poolBKey[:], poolBHash)

	// Pool A: Alice (5M + 3M) + Bob (10M) = 18M lovelace
	require.Equal(t, uint64(18000000), dist.PoolStakes[poolAKey],
		"pool A stake should be sum of Alice and Bob UTxOs")

	// Pool B: Carol (20M) = 20M lovelace
	require.Equal(t, uint64(20000000), dist.PoolStakes[poolBKey],
		"pool B stake should be Carol's UTxO")

	// Total: 18M + 20M = 38M
	require.Equal(t, uint64(38000000), dist.TotalStake,
		"total stake should be sum of all pool stakes")

	// Verify delegator counts
	require.Equal(t, uint64(2), dist.DelegatorCount[poolAKey],
		"pool A should have 2 delegators")
	require.Equal(t, uint64(1), dist.DelegatorCount[poolBKey],
		"pool B should have 1 delegator")
}

// TestCalculateStakeDistribution_SpentUtxosExcluded verifies that spent
// UTxOs (deleted_slot != 0) are not counted in the stake distribution.
func TestCalculateStakeDistribution_SpentUtxosExcluded(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Seed epoch data
	epoch := models.Epoch{
		EpochId:       10,
		StartSlot:     0,
		LengthInSlots: 432000,
	}
	require.NoError(t, gormDB.Create(&epoch).Error, "create epoch")

	poolHash := []byte("poolC_12345678901234567890AB")
	stakeKey := []byte("dave__staking_key_1234567890")

	// Create pool and registration
	pool := models.Pool{PoolKeyHash: poolHash}
	require.NoError(t, gormDB.Create(&pool).Error)

	reg := models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   poolHash,
		AddedSlot:     100,
		Pledge:        1000000,
		Cost:          340000000,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:    make([]byte, 32),
		RewardAccount: make([]byte, 28),
	}
	require.NoError(t, gormDB.Create(&reg).Error)

	// Create account
	account := models.Account{
		StakingKey: stakeKey,
		Pool:       poolHash,
		AddedSlot:  100,
		Active:     true,
	}
	require.NoError(t, gormDB.Create(&account).Error)

	// Create one live UTxO (5 ADA) and one spent UTxO (10 ADA)
	liveUtxo := models.Utxo{
		TxId:        []byte("tx_live_34567890123456789012345678901234"),
		OutputIdx:   0,
		StakingKey:  stakeKey,
		Amount:      5000000,
		AddedSlot:   100,
		DeletedSlot: 0, // live
	}
	require.NoError(t, gormDB.Create(&liveUtxo).Error)

	spentUtxo := models.Utxo{
		TxId:        []byte("tx_spent_4567890123456789012345678901234"),
		OutputIdx:   0,
		StakingKey:  stakeKey,
		Amount:      10000000,
		AddedSlot:   100,
		DeletedSlot: 500, // spent at slot 500
	}
	require.NoError(t, gormDB.Create(&spentUtxo).Error)

	// Calculate stake distribution
	calc := NewCalculator(db)
	dist, err := calc.CalculateStakeDistribution(context.Background(), 1000)
	require.NoError(t, err)

	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)

	// Only the live UTxO (5 ADA) should be counted
	require.Equal(t, uint64(5000000), dist.PoolStakes[poolKey],
		"only live UTxOs should contribute to stake")
	require.Equal(t, uint64(5000000), dist.TotalStake,
		"total stake should exclude spent UTxOs")
}

// TestCalculateStakeDistribution_InactiveAccountsExcluded verifies that
// inactive accounts (deregistered) are not counted in the distribution.
func TestCalculateStakeDistribution_InactiveAccountsExcluded(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Seed epoch data
	epoch := models.Epoch{
		EpochId:       10,
		StartSlot:     0,
		LengthInSlots: 432000,
	}
	require.NoError(t, gormDB.Create(&epoch).Error, "create epoch")

	poolHash := []byte("poolD_12345678901234567890AB")
	activeKey := []byte("activ_staking_key_1234567890")
	inactiveKey := []byte("inact_staking_key_1234567890")

	// Create pool and registration
	pool := models.Pool{PoolKeyHash: poolHash}
	require.NoError(t, gormDB.Create(&pool).Error)

	reg := models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   poolHash,
		AddedSlot:     100,
		Pledge:        1000000,
		Cost:          340000000,
		Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		VrfKeyHash:    make([]byte, 32),
		RewardAccount: make([]byte, 28),
	}
	require.NoError(t, gormDB.Create(&reg).Error)

	// Active account with 7 ADA UTxO
	require.NoError(t, gormDB.Create(&models.Account{
		StakingKey: activeKey, Pool: poolHash, AddedSlot: 100, Active: true,
	}).Error)
	require.NoError(t, gormDB.Create(&models.Utxo{
		TxId: []byte("tx_activ_567890123456789012345678901234"),
		OutputIdx: 0, StakingKey: activeKey,
		Amount: 7000000, AddedSlot: 100,
	}).Error)

	// Inactive account with 15 ADA UTxO.
	// Note: GORM's Create skips zero-value fields when the model has a
	// `default` tag, so Active: false would be stored as true. Create
	// the account first, then explicitly set Active = false via Update.
	inactiveAcct := models.Account{
		StakingKey: inactiveKey, Pool: poolHash, AddedSlot: 100, Active: true,
	}
	require.NoError(t, gormDB.Create(&inactiveAcct).Error)
	require.NoError(t, gormDB.Model(&inactiveAcct).Update("active", false).Error)
	require.NoError(t, gormDB.Create(&models.Utxo{
		TxId: []byte("tx_inact_567890123456789012345678901234"),
		OutputIdx: 0, StakingKey: inactiveKey,
		Amount: 15000000, AddedSlot: 100,
	}).Error)

	// Calculate stake distribution
	calc := NewCalculator(db)
	dist, err := calc.CalculateStakeDistribution(context.Background(), 1000)
	require.NoError(t, err)

	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)

	// Only active account's stake should be counted
	require.Equal(t, uint64(7000000), dist.PoolStakes[poolKey],
		"only active accounts should contribute to stake")
	require.Equal(t, uint64(1), dist.DelegatorCount[poolKey],
		"only active account should be counted as delegator")
}

// TestCalculateStakeDistribution_EmptyDatabase verifies that the calculator
// handles the case where no pools exist gracefully.
func TestCalculateStakeDistribution_EmptyDatabase(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Seed epoch data so we don't get ErrNoEpochData
	epoch := models.Epoch{
		EpochId:       10,
		StartSlot:     0,
		LengthInSlots: 432000,
	}
	require.NoError(t, gormDB.Create(&epoch).Error)

	calc := NewCalculator(db)
	dist, err := calc.CalculateStakeDistribution(context.Background(), 1000)
	require.NoError(t, err)

	require.Zero(t, dist.TotalStake, "empty database should have zero stake")
	require.Zero(t, dist.TotalPools, "empty database should have zero pools")
	require.Empty(t, dist.PoolStakes, "empty database should have no pool stakes")
}
