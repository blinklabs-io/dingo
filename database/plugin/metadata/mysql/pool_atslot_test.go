//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestGetStakeByPoolsAtSlotAggregatesFallbackAccountsMysql exercises the
// historical stake query on mysql. The query text includes every
// delegation/registration UNION branch and the SUM(CAST(utxo.amount AS
// UNSIGNED)) aggregate over the text-encoded amount column, so this catches
// backend-specific regressions that the sqlite tests cannot.
func TestGetStakeByPoolsAtSlotAggregatesFallbackAccountsMysql(t *testing.T) {
	store := newTestMysqlStore(t)
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
	t.Cleanup(func() {
		cleanup()
		_ = store.Close()
	})

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

func TestCountPoolBlocksInSlotRangeTotalIncludesUnrequestedPoolsMysql(
	t *testing.T,
) {
	store := newTestMysqlStore(t)
	db := store.DB()

	poolA := bytes.Repeat([]byte{0xCA}, 28)
	poolB := bytes.Repeat([]byte{0xCB}, 28)
	var poolAHash lcommon.PoolKeyHash
	copy(poolAHash[:], poolA)

	cleanup := func() {
		for _, pool := range [][]byte{poolA, poolB} {
			_ = db.Where(
				"pool_key_hash = ?",
				pool,
			).Delete(&models.PoolOpCertSequence{}).Error
		}
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = store.Close()
	})

	for _, row := range []struct {
		pool []byte
		seq  uint64
		slot uint64
	}{
		{pool: poolA, seq: 1, slot: 10},
		{pool: poolA, seq: 2, slot: 11},
		{pool: poolB, seq: 1, slot: 12},
		{pool: poolB, seq: 2, slot: 30},
	} {
		var poolHash lcommon.PoolKeyHash
		copy(poolHash[:], row.pool)
		require.NoError(t, store.UpdatePoolOpCertSequence(
			poolHash,
			row.seq,
			row.slot,
			nil,
		))
	}

	counts, total, err := store.CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{poolAHash},
		10,
		20,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), total)
	require.Equal(t, uint64(2), counts[string(poolAHash.Bytes())])
	_, ok := counts[string(poolB)]
	require.False(t, ok)
}

func TestRewardLiveStakeRebuildAggregatesCurrentStateMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	db := store.DB()

	poolA := bytes.Repeat([]byte{0xD1}, 28)
	stakeA := bytes.Repeat([]byte{0xD2}, 28)
	txHash := bytes.Repeat([]byte{0xD3}, 32)
	utxoHash := bytes.Repeat([]byte{0xD4}, 32)

	cleanup := func() {
		_ = db.Where("credential_tag = ? AND staking_key = ?", 0, stakeA).
			Delete(&models.RewardLiveStake{}).Error
		_ = db.Where("staking_key = ?", stakeA).
			Delete(&models.StakeDelegation{}).Error
		_ = db.Where("staking_key = ?", stakeA).
			Delete(&models.Account{}).Error
		_ = db.Where("staking_key = ?", stakeA).
			Delete(&models.Utxo{}).Error
		_ = db.Where("pool_key_hash = ?", poolA).
			Delete(&models.PoolRegistration{}).Error
		_ = db.Where("pool_key_hash = ?", poolA).
			Delete(&models.Pool{}).Error
		_ = db.Where("slot = ?", uint64(901959)).
			Delete(&models.Certificate{}).Error
		_ = db.Where("hash = ?", txHash).
			Delete(&models.Transaction{}).Error
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = store.Close()
	})

	pool := models.Pool{PoolKeyHash: poolA}
	require.NoError(t, db.Create(&pool).Error)
	require.NoError(t, db.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolA,
		AddedSlot:   10,
	}).Error)
	tx := models.Transaction{
		Hash:       txHash,
		Slot:       901959,
		BlockIndex: 3,
		Valid:      true,
	}
	require.NoError(t, db.Create(&tx).Error)
	cert := models.Certificate{
		TransactionID: tx.ID,
		Slot:          901959,
		CertIndex:     2,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
	}
	require.NoError(t, db.Create(&cert).Error)
	require.NoError(t, db.Create(&models.Account{
		StakingKey: stakeA,
		Pool:       poolA,
		Reward:     3,
		Active:     true,
		AddedSlot:  10,
	}).Error)
	require.NoError(t, db.Create(&models.StakeDelegation{
		StakingKey:    stakeA,
		PoolKeyHash:   poolA,
		CertificateID: cert.ID,
		AddedSlot:     11,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       utxoHash,
		OutputIdx:  0,
		StakingKey: stakeA,
		Amount:     5,
		AddedSlot:  12,
	}).Error)

	require.NoError(t, store.RebuildRewardLiveStake(20, nil))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 20, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint64(8), uint64(inputs[0].Stake))
}

func TestRewardLiveStakeTracksRewardChangesMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	db := store.DB()

	poolA := bytes.Repeat([]byte{0xF1}, 28)
	stakeA := bytes.Repeat([]byte{0xF2}, 28)
	utxoHash := bytes.Repeat([]byte{0xF3}, 32)
	creditHash := bytes.Repeat([]byte{0xF4}, 32)
	withdrawHash := bytes.Repeat([]byte{0xF5}, 32)

	cleanup := func() {
		_ = db.Where("credential_tag = ? AND staking_key = ?", 0, stakeA).
			Delete(&models.RewardLiveStake{}).Error
		_ = db.Where("credential_tag = ? AND staking_key = ?", 0, stakeA).
			Delete(&models.AccountRewardDelta{}).Error
		_ = db.Where("staking_key = ?", stakeA).
			Delete(&models.Account{}).Error
		_ = db.Where("staking_key = ?", stakeA).
			Delete(&models.Utxo{}).Error
		_ = db.Where("pool_key_hash = ?", poolA).
			Delete(&models.PoolRegistration{}).Error
		_ = db.Where("pool_key_hash = ?", poolA).
			Delete(&models.Pool{}).Error
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = store.Close()
	})

	pool := models.Pool{PoolKeyHash: poolA}
	require.NoError(t, db.Create(&pool).Error)
	require.NoError(t, db.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolA,
		AddedSlot:   10,
	}).Error)
	require.NoError(t, db.Create(&models.Account{
		StakingKey: stakeA,
		Pool:       poolA,
		Reward:     3,
		Active:     true,
		AddedSlot:  10,
	}).Error)
	require.NoError(t, db.Create(&models.Utxo{
		TxId:       utxoHash,
		OutputIdx:  0,
		StakingKey: stakeA,
		Amount:     5,
		AddedSlot:  11,
	}).Error)

	require.NoError(t, store.RebuildRewardLiveStake(11, nil))
	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(8), uint64(inputs[0].Stake))

	require.NoError(t, store.AddAccountRewardByCredential(
		0,
		stakeA,
		7,
		12,
		creditHash,
		nil,
	))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 12, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(15), uint64(inputs[0].Stake))

	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0,
		stakeA,
		10,
		13,
		withdrawHash,
		nil,
	))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 13, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(5), uint64(inputs[0].Stake))

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(11, nil))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(8), uint64(inputs[0].Stake))
}

func TestRewardStakeInputsAtSlotComparesDelegationOrderMysql(t *testing.T) {
	store := newTestMysqlStore(t)
	db := store.DB()

	poolA := bytes.Repeat([]byte{0xE1}, 28)
	stakeBefore := bytes.Repeat([]byte{0xE2}, 28)
	stakeEqual := bytes.Repeat([]byte{0xE3}, 28)
	stakeAfter := bytes.Repeat([]byte{0xE4}, 28)
	txHash := bytes.Repeat([]byte{0xE5}, 32)

	cleanup := func() {
		for _, stake := range [][]byte{stakeBefore, stakeEqual, stakeAfter} {
			_ = db.Where("credential_tag = ? AND staking_key = ?", 0, stake).
				Delete(&models.RewardLiveStake{}).Error
		}
		_ = db.Where("pool_key_hash = ?", poolA).
			Delete(&models.PoolRegistration{}).Error
		_ = db.Where("pool_key_hash = ?", poolA).
			Delete(&models.Pool{}).Error
		_ = db.Where("slot = ?", uint64(901960)).
			Delete(&models.Certificate{}).Error
		_ = db.Where("hash = ?", txHash).
			Delete(&models.Transaction{}).Error
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = store.Close()
	})

	pool := models.Pool{PoolKeyHash: poolA}
	require.NoError(t, db.Create(&pool).Error)
	tx := models.Transaction{
		Hash:       txHash,
		Slot:       901960,
		BlockIndex: 2,
		Valid:      true,
	}
	require.NoError(t, db.Create(&tx).Error)
	cert := models.Certificate{
		TransactionID: tx.ID,
		Slot:          901960,
		CertIndex:     5,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
	}
	require.NoError(t, db.Create(&cert).Error)
	require.NoError(t, db.Create(&models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   poolA,
		CertificateID: cert.ID,
		AddedSlot:     30,
	}).Error)

	require.NoError(t, db.Create(&[]models.RewardLiveStake{
		{
			PoolKeyHash:              poolA,
			StakingKey:               stakeBefore,
			Registered:               true,
			TotalStake:               11,
			PoolDelegationSlot:       30,
			PoolDelegationBlockIndex: 2,
			PoolDelegationCertIndex:  4,
		},
		{
			PoolKeyHash:              poolA,
			StakingKey:               stakeEqual,
			Registered:               true,
			TotalStake:               13,
			PoolDelegationSlot:       30,
			PoolDelegationBlockIndex: 2,
			PoolDelegationCertIndex:  5,
		},
		{
			PoolKeyHash:              poolA,
			StakingKey:               stakeAfter,
			Registered:               true,
			TotalStake:               17,
			PoolDelegationSlot:       30,
			PoolDelegationBlockIndex: 3,
			PoolDelegationCertIndex:  0,
		},
	}).Error)

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 30, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 3)
	require.Equal(t, stakeBefore, inputs[0].StakingKey)
	require.Equal(t, uint64(11), uint64(inputs[0].Stake))
	require.Equal(t, stakeEqual, inputs[1].StakingKey)
	require.Equal(t, uint64(13), uint64(inputs[1].Stake))
	require.Equal(t, stakeAfter, inputs[2].StakingKey)
	require.Equal(t, uint64(17), uint64(inputs[2].Stake))
}
