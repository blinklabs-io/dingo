// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build dingo_extra_plugins

package mysql

import (
	"bytes"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	dbtestutil "github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestMysqlCreditRewardLiveStakeDeltaPreservesLargeIntegers(t *testing.T) {
	store := newTestMysqlStore(t)
	t.Cleanup(func() { _ = store.Close() })

	stakeKey := bytes.Repeat([]byte{0xD4}, 28)
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).Delete(&models.RewardLiveStake{}).Error)
	const initial = uint64(9_007_199_254_740_993)
	require.NoError(t, store.DB().Create(&models.RewardLiveStake{
		CredentialTag: 0,
		StakingKey:    stakeKey,
		RewardStake:   types.Uint64(initial),
		TotalStake:    types.Uint64(initial),
	}).Error)

	require.NoError(t, store.DB().Transaction(func(tx *gorm.DB) error {
		updated, err := creditRewardLiveStakeDelta(
			tx,
			models.NewStakeCredentialRef(0, stakeKey),
			36_000_000_000_000_007,
			42,
		)
		if err != nil {
			return err
		}
		require.True(t, updated)
		return nil
	}))

	var got models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).First(&got).Error)
	require.Equal(t, types.Uint64(45_007_199_254_741_000), got.RewardStake)
	require.Equal(t, got.RewardStake, got.TotalStake)
	require.Equal(t, uint64(42), got.UpdatedSlot)
}

func TestMysqlRebuildRewardLiveStake(t *testing.T) {
	store := newTestMysqlStore(t)
	t.Cleanup(func() { _ = store.Close() })
	stakeKey := bytes.Repeat([]byte{0xD5}, 28)
	poolKey := bytes.Repeat([]byte{0xC5}, 28)
	txID := bytes.Repeat([]byte{0xB5}, 32)
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).Delete(&models.RewardLiveStake{}).Error)
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).Delete(&models.Account{}).Error)
	require.NoError(t, store.DB().Where(
		"tx_id = ? AND output_idx = ?", txID, 0,
	).Delete(&models.Utxo{}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		CredentialTag: 0, StakingKey: stakeKey, Pool: poolKey,
		Reward: types.Uint64(9_007_199_254_740_993), Active: true,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId: txID, OutputIdx: 0, CredentialTag: 0, StakingKey: stakeKey,
		Amount: types.Uint64(36_000_000_000_000_007),
	}).Error)

	require.NoError(t, store.RebuildRewardLiveStake(99, nil))

	var got models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).First(&got).Error)
	require.Equal(t, types.Uint64(45_007_199_254_741_000), got.TotalStake)
	require.Equal(t, uint64(99), got.UpdatedSlot)
}

func TestRewardStakeRefsFromUtxoRewardStakeCredentialsUsesBoundarySlot(
	t *testing.T,
) {
	stakeKey := bytes.Repeat([]byte{0xA5}, 28)
	refs := rewardStakeRefsFromUtxoRewardStakeCredentials(
		[]utxoRewardStakeCredential{{
			CredentialTag: 1,
			StakingKey:    stakeKey,
		}},
		123,
	)

	ref := models.NewStakeCredentialRef(1, stakeKey)
	got, ok := refs[ref.MapKey()]
	require.True(t, ok)
	require.Equal(t, ref, got.ref)
	require.Equal(t, uint64(123), got.slot)
}

func TestMysqlConcurrentWithdrawalReplayAtDifferentSlots(t *testing.T) {
	store := newTestMysqlStore(t)
	t.Cleanup(func() { _ = store.Close() })

	stakeKey := bytes.Repeat([]byte{0xE4}, 28)
	txHash := bytes.Repeat([]byte{0x74}, 32)
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).Delete(&models.AccountRewardDelta{}).Error)
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeKey,
	).Delete(&models.Account{}).Error)
	account := &models.Account{
		StakingKey: stakeKey, CredentialTag: 0,
		Reward: types.Uint64(100), Active: true,
	}
	require.NoError(t, store.DB().Create(account).Error)

	blocker := store.DB().Begin()
	require.NoError(t, blocker.Error)
	t.Cleanup(func() { _ = blocker.Rollback().Error })
	var locked models.Account
	require.NoError(t, blocker.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("id = ?", account.ID).First(&locked).Error)

	started := make(chan struct{}, 2)
	errCh := make(chan error, 2)
	for _, slot := range []uint64{100, 300} {
		go func(slot uint64) {
			started <- struct{}{}
			errCh <- store.ApplyAccountRewardWithdrawal(
				0, stakeKey, 100, slot, txHash, nil,
			)
		}(slot)
	}
	for range 2 {
		dbtestutil.RequireReceive(t, started, 5*time.Second, "withdrawal start")
	}
	require.NoError(t, blocker.Commit().Error)
	for range 2 {
		require.NoError(t, dbtestutil.RequireReceive(
			t, errCh, 5*time.Second, "concurrent withdrawal result",
		))
	}

	var count int64
	require.NoError(t, store.DB().Model(&models.AccountRewardDelta{}).
		Where(
			"withdrawal = ? AND tx_hash = ? AND credential_tag = ? AND staking_key = ?",
			true, txHash, 0, stakeKey,
		).Count(&count).Error)
	require.Equal(t, int64(1), count)
}
