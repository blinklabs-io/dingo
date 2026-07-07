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
	"gorm.io/gorm"
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
		stakingKey  []byte
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

func seedCertificate(
	t *testing.T,
	gormDB *gorm.DB,
	slot uint64,
	blockIndex uint32,
	certIndex uint,
	certType lcommon.CertificateType,
) uint {
	t.Helper()
	txHash := make([]byte, 32)
	txHash[0] = byte(slot)
	txHash[1] = byte(slot >> 8)
	txHash[2] = byte(blockIndex)
	txHash[3] = byte(certIndex)
	txHash[4] = byte(certType)
	tx := models.Transaction{
		Hash:       txHash,
		Slot:       slot,
		BlockIndex: blockIndex,
	}
	require.NoError(t, gormDB.Create(&tx).Error, "create tx for cert")
	cert := models.Certificate{
		TransactionID: tx.ID,
		Slot:          slot,
		CertIndex:     certIndex,
		CertType:      uint(certType),
	}
	require.NoError(t, gormDB.Create(&cert).Error, "create cert")
	return cert.ID
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

func TestCalculateStakeDistribution_UsesHistoricalDelegationAndRegistration(
	t *testing.T,
) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	require.NoError(t, gormDB.Create(&models.Epoch{
		EpochId:       10,
		StartSlot:     0,
		LengthInSlots: 432000,
	}).Error, "create epoch")

	poolAHash := []byte("poolA_hist_12345678901234567")
	poolBHash := []byte("poolB_hist_12345678901234567")
	stakeKey := []byte("hist__staking_key_1234567890")

	for _, poolHash := range [][]byte{poolAHash, poolBHash} {
		pool := models.Pool{PoolKeyHash: poolHash}
		require.NoError(t, gormDB.Create(&pool).Error, "create pool")
		require.NoError(t, gormDB.Create(&models.PoolRegistration{
			PoolID:        pool.ID,
			PoolKeyHash:   poolHash,
			AddedSlot:     50,
			Pledge:        1000000,
			Cost:          340000000,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
			VrfKeyHash:    make([]byte, 32),
			RewardAccount: make([]byte, 28),
		}).Error, "create pool registration")
	}

	regCertID := seedCertificate(
		t,
		gormDB,
		100,
		0,
		0,
		lcommon.CertificateTypeStakeRegistration,
	)
	require.NoError(t, gormDB.Create(&models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     100,
		CertificateID: regCertID,
	}).Error, "create stake registration")

	delegationACertID := seedCertificate(
		t,
		gormDB,
		100,
		0,
		1,
		lcommon.CertificateTypeStakeDelegation,
	)
	require.NoError(t, gormDB.Create(&models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolAHash,
		AddedSlot:     100,
		CertificateID: delegationACertID,
	}).Error, "create pool A delegation")

	delegationBCertID := seedCertificate(
		t,
		gormDB,
		300,
		0,
		0,
		lcommon.CertificateTypeStakeDelegation,
	)
	require.NoError(t, gormDB.Create(&models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   poolBHash,
		AddedSlot:     300,
		CertificateID: delegationBCertID,
	}).Error, "create pool B delegation")

	deregCertID := seedCertificate(
		t,
		gormDB,
		500,
		0,
		0,
		lcommon.CertificateTypeStakeDeregistration,
	)
	require.NoError(t, gormDB.Create(&models.StakeDeregistration{
		StakingKey:    stakeKey,
		AddedSlot:     500,
		CertificateID: deregCertID,
	}).Error, "create deregistration")

	reregCertID := seedCertificate(
		t,
		gormDB,
		600,
		0,
		0,
		lcommon.CertificateTypeStakeRegistration,
	)
	require.NoError(t, gormDB.Create(&models.StakeRegistration{
		StakingKey:    stakeKey,
		AddedSlot:     600,
		CertificateID: reregCertID,
	}).Error, "create plain re-registration")

	require.NoError(t, gormDB.Create(&models.Utxo{
		TxId:       []byte("tx_hist_123456789012345678901234"),
		OutputIdx:  0,
		StakingKey: stakeKey,
		Amount:     10000000,
		AddedSlot:  100,
	}).Error, "create delegated utxo")
	require.NoError(t, gormDB.Create(&models.Account{
		StakingKey: stakeKey,
		Pool:       poolBHash,
		AddedSlot:  600,
		Active:     true,
	}).Error, "create current account row")

	bootstrapKey := []byte("hist_bootstrap_key_123456789")
	bootstrapRegCertID := seedCertificate(
		t,
		gormDB,
		610,
		0,
		0,
		lcommon.CertificateTypeStakeRegistration,
	)
	require.NoError(t, gormDB.Create(&models.StakeRegistration{
		StakingKey:    bootstrapKey,
		AddedSlot:     610,
		CertificateID: bootstrapRegCertID,
	}).Error, "create bootstrap plain registration")
	require.NoError(t, gormDB.Create(&models.Account{
		StakingKey: bootstrapKey,
		Pool:       poolBHash,
		AddedSlot:  610,
		Active:     true,
	}).Error, "create bootstrap current account row")
	require.NoError(t, gormDB.Create(&models.Utxo{
		TxId:       []byte("tx_boot_123456789012345678901234"),
		OutputIdx:  0,
		StakingKey: bootstrapKey,
		Amount:     20000000,
		AddedSlot:  100,
	}).Error, "create bootstrap utxo")

	calc := NewCalculator(db)

	dist, err := calc.CalculateStakeDistribution(context.Background(), 200)
	require.NoError(t, err)
	var poolAKey lcommon.PoolKeyHash
	copy(poolAKey[:], poolAHash)
	require.Equal(t, uint64(10000000), dist.PoolStakes[poolAKey])
	require.Equal(t, uint64(10000000), dist.TotalStake)

	dist, err = calc.CalculateStakeDistribution(context.Background(), 400)
	require.NoError(t, err)
	var poolBKey lcommon.PoolKeyHash
	copy(poolBKey[:], poolBHash)
	require.Equal(t, uint64(10000000), dist.PoolStakes[poolBKey])
	require.Equal(t, uint64(10000000), dist.TotalStake)

	dist, err = calc.CalculateStakeDistribution(context.Background(), 650)
	require.NoError(t, err)
	require.Zero(t, dist.TotalStake)
	require.Empty(t, dist.PoolStakes)
}

// TestCalculateStakeDistribution_HistoricalUtxoLiveness verifies that stake
// distribution uses UTxO liveness at the snapshot slot rather than today's live
// UTxO set.
func TestCalculateStakeDistribution_HistoricalUtxoLiveness(t *testing.T) {
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

	// Create one live UTxO (5 ADA), one UTxO spent after the earlier
	// snapshot slot (10 ADA), and one UTxO created after that slot (20 ADA).
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

	lateUtxo := models.Utxo{
		TxId:        []byte("tx_late_4567890123456789012345678901234"),
		OutputIdx:   0,
		StakingKey:  stakeKey,
		Amount:      20000000,
		AddedSlot:   700,
		DeletedSlot: 0,
	}
	require.NoError(t, gormDB.Create(&lateUtxo).Error)

	calc := NewCalculator(db)
	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)

	dist, err := calc.CalculateStakeDistribution(context.Background(), 400)
	require.NoError(t, err)

	// At slot 400 the spent-at-500 UTxO is still live, while the
	// added-at-700 UTxO does not exist yet.
	require.Equal(t, uint64(15000000), dist.PoolStakes[poolKey],
		"snapshot before spend should include UTxOs live at that slot only")
	require.Equal(t, uint64(15000000), dist.TotalStake,
		"total stake should use snapshot-slot UTxO liveness")

	dist, err = calc.CalculateStakeDistribution(context.Background(), 1000)
	require.NoError(t, err)

	// At slot 1000 the spent UTxO is gone and the late UTxO is live.
	require.Equal(t, uint64(25000000), dist.PoolStakes[poolKey],
		"later snapshot should exclude spent UTxOs and include later outputs")
	require.Equal(t, uint64(25000000), dist.TotalStake,
		"total stake should reflect the later slot")
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
		TxId:      []byte("tx_activ_567890123456789012345678901234"),
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
		TxId:      []byte("tx_inact_567890123456789012345678901234"),
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
