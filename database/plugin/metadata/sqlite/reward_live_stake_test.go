// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqlite

import (
	"bytes"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestRewardLiveStakeNeedsBackfillAcceptsReadTransaction(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	key := bytes.Repeat([]byte{0xB4}, 28)
	require.NoError(t, store.DB().Create(&models.Account{
		CredentialTag: 0,
		StakingKey:    key,
		Active:        true,
	}).Error)
	txn := store.ReadTransaction()
	t.Cleanup(func() { require.NoError(t, txn.Rollback()) })

	needed, err := store.RewardLiveStakeNeedsBackfill(txn)

	require.NoError(t, err)
	require.True(t, needed)
}

func TestRebuildRewardLiveStakeRejectsNullAccountCredential(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)
	require.NoError(t, store.DB().Exec(`
		INSERT INTO account (credential_tag, staking_key, reward, active)
		VALUES (?, NULL, ?, ?)
	`, 0, "0", true).Error)

	err := store.RebuildRewardLiveStake(1, nil)

	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "invalid account credentials"))
}
