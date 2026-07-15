//go:build dingo_extra_plugins

package postgres

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestGetStakeByPoolsAtSlotAggregatesFallbackAccountsPostgres exercises the
// historical stake query on postgres. The query text includes every
// delegation/registration UNION branch and the SUM(CAST(utxo.amount ...))
// aggregate, all of which postgres type-checks at parse time, so this catches
// backend-specific regressions (text/integer UNION mismatch, sum over the
// text-encoded amount column) that the sqlite tests cannot.
func TestGetStakeByPoolsAtSlotAggregatesFallbackAccountsPostgres(t *testing.T) {
	store := newTestPostgresStore(t)
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
		[][]byte{poolA, poolB, poolEmpty}, 80, nil,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(12), stakes[string(poolA)])
	require.Equal(t, uint64(2), delegators[string(poolA)])
	require.Equal(t, uint64(17), stakes[string(poolB)])
	require.Equal(t, uint64(1), delegators[string(poolB)])
	require.Equal(t, uint64(0), stakes[string(poolEmpty)])
	require.Equal(t, uint64(0), delegators[string(poolEmpty)])
}

// TestGetStakeByPoolsAggregatesTextAmountsPostgres verifies that the live
// stake query casts the text-encoded amount to BIGINT before SUM.
func TestGetStakeByPoolsAggregatesTextAmountsPostgres(t *testing.T) {
	store := newTestPostgresStore(t)
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
			Amount:      5,
			AddedSlot:   20,
			DeletedSlot: 0,
		},
		{
			TxId:        bytes.Repeat([]byte{0x22}, 32),
			OutputIdx:   0,
			StakingKey:  stakeKey,
			Amount:      7,
			AddedSlot:   20,
			DeletedSlot: 0,
		},
	}
	for i := range utxos {
		require.NoError(t, db.Create(&utxos[i]).Error)
	}

	stakes, delegators, err := store.GetStakeByPools([][]byte{pool}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(12), stakes[string(pool)])
	require.Equal(t, uint64(1), delegators[string(pool)])
}

// TestGetStakeByPoolsAtSlotIncludesRewardBalancePostgres covers issue #2813 on
// postgres: the reconstructed reward-account balance is cast to BIGINT and
// added to live UTxO lovelace, and a reward-only credential with no live UTxO
// yields non-zero stake.
func TestGetStakeByPoolsAtSlotIncludesRewardBalancePostgres(t *testing.T) {
	store := newTestPostgresStore(t)
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
		[][]byte{poolA, poolB}, 80, nil,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(180), stakes[string(poolA)])
	require.Equal(t, uint64(2), delegators[string(poolA)])
	require.Equal(t, uint64(40), stakes[string(poolB)])
	require.Equal(t, uint64(1), delegators[string(poolB)])
}
