//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
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
