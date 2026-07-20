// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqlite

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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

// TestRebuildRewardLiveStakeToleratesDroppedStakingIndex reproduces the
// Mithril sync crash where the ledger-state import runs RebuildRewardLiveStake
// while idx_utxo_staking_deleted_amount is dropped by the deferred-index
// bulk-load optimization. SQLite treats INDEXED BY as a hard directive and
// aborts with "no such index" when the hinted index is absent, so the rebuild
// must fall back to a planner-chosen plan and still produce the correct
// aggregate.
func TestRebuildRewardLiveStakeToleratesDroppedStakingIndex(t *testing.T) {
	t.Parallel()
	store := setupTestStore(t)

	stakingKey := bytes.Repeat([]byte{0xA1}, 28)
	pool := bytes.Repeat([]byte{0xC2}, 28)
	require.NoError(t, store.DB().Create(&models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Pool:          pool,
		Reward:        types.Uint64(500),
		Active:        true,
		AddedSlot:     10,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          bytes.Repeat([]byte{0x01}, 32),
		OutputIdx:     0,
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Amount:        types.Uint64(1_000),
		DeletedSlot:   0,
		AddedSlot:     10,
	}).Error)

	// Simulate the deferred-index bulk-load window: the composite staking
	// index the rebuild hints is dropped during Mithril ledger-state import.
	migrator := store.DB().Migrator()
	require.NoError(
		t,
		migrator.DropIndex(&models.Utxo{}, utxoStakingLiveAmountIndex),
	)
	require.False(
		t,
		migrator.HasIndex(&models.Utxo{}, utxoStakingLiveAmountIndex),
		"precondition: staking index must be absent",
	)

	require.NoError(t, store.RebuildRewardLiveStake(20, nil))

	var row models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?",
		0,
		stakingKey,
	).First(&row).Error)
	require.Equal(t, uint64(1_000), uint64(row.UtxoStake))
	require.Equal(t, uint64(500), uint64(row.RewardStake))
	require.Equal(t, uint64(1_500), uint64(row.TotalStake))
}

// TestRebuildRewardLiveStakeWithinWriteTxnToleratesDroppedIndex reproduces the
// Mithril import deadlock: the ledger-state import calls RebuildRewardLiveStake
// inside a write transaction (database.RebuildRewardLiveStake -> Txn.Do) while
// idx_utxo_staking_deleted_amount is dropped for bulk load. The file-based write
// pool is capped at a single connection, so probing index presence on d.DB()
// (rather than the transaction's own connection) requests a second handle and
// deadlocks against the transaction waiting on the rebuild. A file-based store
// is required — the in-memory store uses a multi-connection pool and cannot
// reproduce the single-writer contention.
func TestRebuildRewardLiveStakeWithinWriteTxnToleratesDroppedIndex(t *testing.T) {
	t.Parallel()
	store, err := New(t.TempDir(), nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	t.Cleanup(func() { store.Close() }) //nolint:errcheck

	stakingKey := bytes.Repeat([]byte{0xA2}, 28)
	require.NoError(t, store.DB().Create(&models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Reward:        types.Uint64(700),
		Active:        true,
		AddedSlot:     5,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:          bytes.Repeat([]byte{0x02}, 32),
		OutputIdx:     0,
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Amount:        types.Uint64(2_000),
		DeletedSlot:   0,
		AddedSlot:     5,
	}).Error)

	require.NoError(
		t,
		store.DB().Migrator().DropIndex(&models.Utxo{}, utxoStakingLiveAmountIndex),
	)

	txn := store.Transaction()
	done := make(chan error, 1)
	go func() { done <- store.RebuildRewardLiveStake(30, txn) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(15 * time.Second):
		t.Fatal("RebuildRewardLiveStake deadlocked inside a write transaction")
	}
	require.NoError(t, txn.Commit())

	var row models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?",
		0,
		stakingKey,
	).First(&row).Error)
	require.Equal(t, uint64(2_000), uint64(row.UtxoStake))
	require.Equal(t, uint64(700), uint64(row.RewardStake))
	require.Equal(t, uint64(2_700), uint64(row.TotalStake))
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
