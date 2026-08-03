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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// importedAccountFixture mirrors what a Mithril bootstrap leaves on disk for a
// reward account that was registered on-chain long before the snapshot: a live
// account row stamped with the snapshot slot and NO certificate history at all.
// ImportAccount writes exactly this shape (see ImportAccount and
// ledgerstate.importAccounts), and unlike ImportPool it synthesizes no
// registration record, so the account's real registration certificate is
// simply absent from this database.
func importedAccountFixture(
	t *testing.T,
	store *MetadataStoreSqlite,
	stakeKey, pool []byte,
	boundarySlot uint64,
	reward uint64,
) *models.Account {
	t.Helper()
	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Pool:          pool,
		Active:        true,
		AddedSlot:     boundarySlot,
		// CreatedSlot stays 0: the true registration slot predates the
		// snapshot and is unknowable from it.
		CreatedSlot: 0,
		Reward:      types.Uint64(reward),
	}
	require.NoError(t, store.ImportAccount(account, nil))
	return account
}

// redelegateAt records a plain stake-delegation certificate at slot and bumps
// the account's added_slot to match, the way applying a re-delegation
// transaction does. This is a delegation certificate, not a registration one.
func redelegateAt(
	t *testing.T,
	store *MetadataStoreSqlite,
	accountID uint,
	stakeKey, pool []byte,
	txID uint,
	slot uint64,
) {
	t.Helper()
	require.NoError(t, createTestTransaction(store.DB(), txID, slot))
	cert := models.Certificate{
		TransactionID: txID,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          slot,
	}
	require.NoError(t, store.DB().Create(&cert).Error)
	require.NoError(t, store.DB().Create(&models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   pool,
		AddedSlot:     slot,
		CertificateID: cert.ID,
	}).Error)
	require.NoError(t, store.DB().Model(&models.Account{}).
		Where("id = ?", accountID).
		Updates(map[string]any{
			"pool":       pool,
			"added_slot": slot,
		}).Error)
}

// TestRollbackKeepsImportedAccountWithoutRegistrationHistory covers the
// rollback path for an account that exists on-chain but whose registration
// certificate is not in this database, which is every reward account on a
// Mithril-bootstrapped node. Rollback must not infer "registered after the
// rollback slot" from missing certificate history: the account demonstrably
// existed at the rollback slot, so deleting it destroys live ledger state that
// no later block will restore.
func TestRollbackKeepsImportedAccountWithoutRegistrationHistory(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0x51}, 28)
	snapshotPool := bytes.Repeat([]byte{0xB1}, 28)
	laterPool := bytes.Repeat([]byte{0xB2}, 28)

	const (
		boundarySlot     = uint64(100)
		redelegationSlot = uint64(200)
		rollbackSlot     = uint64(150)
	)

	account := importedAccountFixture(
		t, store, stakeKey, snapshotPool, boundarySlot, 0,
	)
	redelegateAt(
		t, store, account.ID, stakeKey, laterPool, 1, redelegationSlot,
	)

	// Rollback above the trust boundary but below the re-delegation, which is
	// the shape of any routine fork switch after a Mithril bootstrap.
	require.NoError(t, store.RestoreAccountStateAtSlot(rollbackSlot, nil))

	restored, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.NotNil(
		t,
		restored,
		"account that existed at the rollback slot must not be deleted just because its registration certificate predates the imported snapshot",
	)
	require.True(
		t,
		restored.Active,
		"surviving account should remain active",
	)
	require.NotEmpty(
		t,
		restored.Pool,
		"pool delegation must not be cleared when no certificate history is available to restore from",
	)
}

// TestRollbackDeletesAccountCreatedAfterSlotWithCreatedSlot pins the behavior
// that must survive the fix above: an account whose row was genuinely created
// after the rollback slot is still deleted.
func TestRollbackDeletesAccountCreatedAfterSlotWithCreatedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0x52}, 28)
	require.NoError(t, store.SetAccount(
		0, stakeKey, nil, nil, 300, true, nil,
	))

	require.NoError(t, store.RestoreAccountStateAtSlot(200, nil))

	restored, err := store.GetAccountByCredential(0, stakeKey, true, nil)
	require.NoError(t, err)
	require.Nil(
		t,
		restored,
		"account first created after the rollback slot should be deleted",
	)
}

// TestDeleteAccountRewardsAfterSlotToleratesMissingAccount covers recovery on a
// database already damaged by the deletion described above: reward-delta
// journal rows survive whose account row is gone. Startup reconciliation rolls
// the ledger back to the chain tip through this path, so a hard failure here is
// an unrecoverable boot crash-loop -- the rollback can never complete, and no
// operator action short of discarding the database clears it. Reverting a delta
// for an absent account is a no-op (there is no balance to restore), so the
// rollback must drop the stale journal rows and continue.
func TestDeleteAccountRewardsAfterSlotToleratesMissingAccount(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakeKey := bytes.Repeat([]byte{0x53}, 28)
	txHash := bytes.Repeat([]byte{0x7a}, 32)

	const (
		boundarySlot   = uint64(100)
		withdrawalSlot = uint64(120)
	)

	// A withdrawal journal row with no surviving account row.
	require.NoError(t, store.DB().Create(&models.AccountRewardDelta{
		StakingKey:     stakeKey,
		CredentialTag:  0,
		TxHash:         txHash,
		Amount:         types.Uint64(5_000_000),
		PreviousReward: types.Uint64(5_000_000),
		AddedSlot:      withdrawalSlot,
		Withdrawal:     true,
	}).Error)

	require.NoError(
		t,
		store.DeleteAccountRewardsAfterSlot(boundarySlot, nil),
		"rollback must complete despite a reward delta whose account row is missing",
	)

	var remaining int64
	require.NoError(t, store.DB().Model(&models.AccountRewardDelta{}).
		Where("staking_key = ?", stakeKey).
		Count(&remaining).Error)
	require.Zero(
		t,
		remaining,
		"stale reward-delta journal rows should be removed by the rollback",
	)
}
