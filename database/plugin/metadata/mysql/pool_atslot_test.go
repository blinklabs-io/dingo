//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestGetStakeByPoolsAtSlotAggregatesFallbackAccountsMysql exercises the
// historical stake query on mysql. The query text includes every
// delegation/registration UNION branch and the SUM(CAST(utxo.amount AS
// UNSIGNED)) aggregate over the text-encoded amount column, so this catches
// backend-specific regressions that the sqlite tests cannot.
func TestGetStakeByPoolsAtSlotAggregatesFallbackAccountsMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()

	poolA := bytes.Repeat([]byte{0xA1}, 28)
	poolB := bytes.Repeat([]byte{0xB1}, 28)
	poolEmpty := bytes.Repeat([]byte{0xE1}, 28)
	stakeA1 := bytes.Repeat([]byte{0x71}, 28)
	stakeA2 := bytes.Repeat([]byte{0x72}, 28)
	stakeB1 := bytes.Repeat([]byte{0x73}, 28)

	cleanup := func() {
		for _, k := range [][]byte{stakeA1, stakeA2, stakeB1} {
			_ = db.Where("staking_key = ?", k).Delete(&models.Account{}).Error
			_ = db.Where("staking_key = ?", k).Delete(&models.Utxo{}).Error
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	accounts := []models.Account{
		{StakingKey: stakeA1, Pool: poolA, AddedSlot: 10, Active: true},
		{StakingKey: stakeA2, Pool: poolA, AddedSlot: 10, Active: true},
		{StakingKey: stakeB1, Pool: poolB, AddedSlot: 10, Active: true},
	}
	for i := range accounts {
		require.NoError(t, db.Create(&accounts[i]).Error)
	}
	utxos := []models.Utxo{
		{TxId: bytes.Repeat([]byte{0x11}, 32), OutputIdx: 0, StakingKey: stakeA1, Amount: 5, AddedSlot: 20},
		{TxId: bytes.Repeat([]byte{0x12}, 32), OutputIdx: 0, StakingKey: stakeA1, Amount: 7, AddedSlot: 30, DeletedSlot: 90},
		{TxId: bytes.Repeat([]byte{0x13}, 32), OutputIdx: 0, StakingKey: stakeA1, Amount: 11, AddedSlot: 90},
		{TxId: bytes.Repeat([]byte{0x14}, 32), OutputIdx: 0, StakingKey: stakeA1, Amount: 13, AddedSlot: 5, DeletedSlot: 70},
		{TxId: bytes.Repeat([]byte{0x15}, 32), OutputIdx: 0, StakingKey: stakeB1, Amount: 17, AddedSlot: 20},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error)
	}

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{poolA, poolB, poolEmpty}, 80, 0, 0, nil,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(12), stakes[string(poolA)])
	require.Equal(t, uint64(2), delegators[string(poolA)])
	require.Equal(t, uint64(17), stakes[string(poolB)])
	require.Equal(t, uint64(1), delegators[string(poolB)])
	require.Equal(t, uint64(0), stakes[string(poolEmpty)])
	require.Equal(t, uint64(0), delegators[string(poolEmpty)])
}

// TestGetStakeByPoolsAtSlotIncludesRewardBalanceMysql covers issue #2813 on
// mysql: the reconstructed reward-account balance is cast to UNSIGNED and
// added to live UTxO lovelace, and a reward-only credential with no live UTxO
// yields non-zero stake.
func TestGetStakeByPoolsAtSlotIncludesRewardBalanceMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()

	poolA := bytes.Repeat([]byte{0xA2}, 28)
	poolB := bytes.Repeat([]byte{0xB2}, 28)
	stakeUtxoReward := bytes.Repeat([]byte{0x75}, 28)
	stakeRewardOnly := bytes.Repeat([]byte{0x76}, 28)
	stakeUtxoNoReward := bytes.Repeat([]byte{0x77}, 28)

	cleanup := func() {
		for _, k := range [][]byte{stakeUtxoReward, stakeRewardOnly, stakeUtxoNoReward} {
			_ = db.Where("staking_key = ?", k).Delete(&models.Account{}).Error
			_ = db.Where("staking_key = ?", k).Delete(&models.Utxo{}).Error
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	accounts := []models.Account{
		{StakingKey: stakeUtxoReward, Pool: poolA, AddedSlot: 10, Active: true, Reward: types.Uint64(50)},
		{StakingKey: stakeRewardOnly, Pool: poolA, AddedSlot: 10, Active: true, Reward: types.Uint64(30)},
		{StakingKey: stakeUtxoNoReward, Pool: poolB, AddedSlot: 10, Active: true},
	}
	for i := range accounts {
		require.NoError(t, db.Create(&accounts[i]).Error)
	}
	utxos := []models.Utxo{
		{TxId: bytes.Repeat([]byte{0x41}, 32), OutputIdx: 0, StakingKey: stakeUtxoReward, Amount: 100, AddedSlot: 20},
		{TxId: bytes.Repeat([]byte{0x42}, 32), OutputIdx: 0, StakingKey: stakeUtxoNoReward, Amount: 40, AddedSlot: 20},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error)
	}

	stakes, delegators, err := store.GetStakeByPoolsAtSlot(
		[][]byte{poolA, poolB}, 80, 0, 0, nil,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(180), stakes[string(poolA)])
	require.Equal(t, uint64(2), delegators[string(poolA)])
	require.Equal(t, uint64(40), stakes[string(poolB)])
	require.Equal(t, uint64(1), delegators[string(poolB)])
}

// TestGetStakeByPoolsPreservesIntegerPrecisionMysql verifies that the live
// stake query casts the text-encoded amount before summing it. Values above
// 2^53 expose MySQL's lossy implicit DOUBLE conversion.
func TestGetStakeByPoolsPreservesIntegerPrecisionMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()

	pool := bytes.Repeat([]byte{0xC1}, 28)
	stakeKey := bytes.Repeat([]byte{0x74}, 28)

	cleanup := func() {
		_ = db.Where("staking_key = ?", stakeKey).Delete(&models.Account{}).Error
		_ = db.Where("staking_key = ?", stakeKey).Delete(&models.Utxo{}).Error
	}
	cleanup()
	t.Cleanup(cleanup)

	require.NoError(t, db.Create(&models.Account{
		StakingKey: stakeKey,
		Pool:       pool,
		AddedSlot:  10,
		Active:     true,
	}).Error)

	utxos := []models.Utxo{
		{
			TxId:        bytes.Repeat([]byte{0x21}, 32),
			OutputIdx:   0,
			StakingKey:  stakeKey,
			Amount:      9007199254740993,
			AddedSlot:   20,
			DeletedSlot: 0,
		},
		{
			TxId:        bytes.Repeat([]byte{0x22}, 32),
			OutputIdx:   0,
			StakingKey:  stakeKey,
			Amount:      2,
			AddedSlot:   20,
			DeletedSlot: 0,
		},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error)
	}

	stakes, delegators, err := store.GetStakeByPools([][]byte{pool}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(9007199254740995), stakes[string(pool)])
	require.Equal(t, uint64(1), delegators[string(pool)])
}

// TestGetRewardStakeInputsForPoolsUsesHistoricalExpirationMysql exercises the
// CIP-0163 gated reward-input path on mysql. With the gate on it must
// reconstruct expiration at slot from the shared historical CTE (not the live
// account.expiration_epoch column) so the reward-basis inputs agree with
// GetStakeByPoolsAtSlot. Mysql type-checks the full query at parse time,
// catching backend-specific regressions the sqlite test cannot.
func TestGetRewardStakeInputsForPoolsUsesHistoricalExpirationMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	defer store.Close() //nolint:errcheck
	db := store.DB()

	pool := bytes.Repeat([]byte{0xD5}, 28)
	active := bytes.Repeat([]byte{0x2A}, 28)
	expired := bytes.Repeat([]byte{0x2B}, 28)

	cleanup := func() {
		for _, k := range [][]byte{active, expired} {
			_ = db.Where("staking_key = ?", k).Delete(&models.Account{}).Error
			_ = db.Where("staking_key = ?", k).Delete(&models.Utxo{}).Error
			_ = db.Where("staking_key = ?", k).
				Delete(&models.AccountWithdrawalWitness{}).Error
			_ = db.Where("staking_key = ?", k).
				Delete(&models.RewardLiveStake{}).Error
		}
		_ = db.Where("epoch_id <= ?", 5).Delete(&models.Epoch{}).Error
	}
	cleanup()
	t.Cleanup(cleanup)

	for epoch := uint64(0); epoch <= 5; epoch++ {
		require.NoError(t, db.Create(&models.Epoch{
			EpochId: epoch, StartSlot: epoch * 100, LengthInSlots: 100,
		}).Error)
	}
	accounts := []models.Account{
		{
			StakingKey: active, Pool: pool, AddedSlot: 10, CreatedSlot: 10,
			Active: true, ExpirationEpoch: 7,
		},
		{
			StakingKey: expired, Pool: pool, AddedSlot: 10, CreatedSlot: 10,
			Active: true, ExpirationEpoch: 7,
		},
	}
	for i := range accounts {
		require.NoError(t, db.Create(&accounts[i]).Error)
	}
	utxos := []models.Utxo{
		{TxId: bytes.Repeat([]byte{0x63}, 32), StakingKey: active, Amount: 100, AddedSlot: 20},
		{TxId: bytes.Repeat([]byte{0x64}, 32), StakingKey: expired, Amount: 40, AddedSlot: 20},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error)
	}
	require.NoError(t, db.Create(&models.AccountWithdrawalWitness{
		StakingKey: active, AddedSlot: 250, TxHash: bytes.Repeat([]byte{0x73}, 32),
	}).Error)
	require.NoError(t, db.Create(&models.AccountWithdrawalWitness{
		StakingKey: expired, AddedSlot: 50, TxHash: bytes.Repeat([]byte{0x74}, 32),
	}).Error)
	live := []models.RewardLiveStake{
		{
			PoolKeyHash: pool, StakingKey: active, CredentialTag: 0,
			TotalStake: types.Uint64(100), Registered: true,
		},
		{
			PoolKeyHash: pool, StakingKey: expired, CredentialTag: 0,
			TotalStake: types.Uint64(40), Registered: true,
		},
	}
	for i := range live {
		require.NoError(t, db.Create(&live[i]).Error)
	}

	// Gate off: live aggregate returns both inputs.
	inputs, err := store.GetRewardStakeInputsForPools([][]byte{pool}, 250, 0, 0, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 2)

	// Gate on: expiration reconstructed at slot 250 excludes the expired-at-slot
	// credential; a live-column filter would have kept both (both live epoch 7).
	inputs, err = store.GetRewardStakeInputsForPools([][]byte{pool}, 250, 3, 2, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, active, inputs[0].StakingKey)
	require.Equal(t, uint64(100), uint64(inputs[0].Stake))
}
