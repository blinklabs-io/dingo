// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//go:build dingo_extra_plugins

package postgres

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestPostgresRebuildRewardLiveStake(t *testing.T) {
	store := newTestPostgresStore(t)
	t.Cleanup(func() { _ = store.Close() })
	stakeKey := bytes.Repeat([]byte{0xD6}, 28)
	poolKey := bytes.Repeat([]byte{0xC6}, 28)
	txID := bytes.Repeat([]byte{0xB6}, 32)
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
