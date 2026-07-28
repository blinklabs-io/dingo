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

//go:build dingo_extra_plugins

package mysql

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// cleanImportedAccountTables clears every table these rollback tests touch, so
// they are independent of whatever else ran against the shared test database.
func cleanImportedAccountTables(store *MetadataStoreMysql) {
	db := store.DB()
	db.Where("1 = 1").Delete(&models.AccountRewardDelta{})
	db.Where("1 = 1").Delete(&models.AccountWithdrawalWitness{})
	db.Where("1 = 1").Delete(&models.StakeDelegation{})
	db.Where("1 = 1").Delete(&models.StakeRegistration{})
	db.Where("1 = 1").Delete(&models.Certificate{})
	db.Where("1 = 1").Delete(&models.Account{})
	db.Where("1 = 1").Delete(&models.Transaction{})
}

// TestMysqlRollbackKeepsImportedAccountWithoutRegistrationHistory mirrors the
// sqlite coverage: an account imported by a Mithril bootstrap has no
// certificate history in this database, and rollback must not read that absence
// as "registered after the rollback slot" and delete a live reward account.
func TestMysqlRollbackKeepsImportedAccountWithoutRegistrationHistory(
	t *testing.T,
) {
	myStore := newTestMysqlStore(t)
	defer myStore.Close() //nolint:errcheck
	cleanImportedAccountTables(myStore)

	stakeKey := testHash28("imported_no_reg_history")
	snapshotPool := testHash28("snapshot_pool")
	laterPool := testHash28("later_pool")

	const (
		boundarySlot     = uint64(100)
		redelegationSlot = uint64(200)
		rollbackSlot     = uint64(150)
	)

	// ImportAccount writes the live row with added_slot at the snapshot slot,
	// created_slot 0, and no registration record of any kind.
	account := &models.Account{
		StakingKey:    stakeKey,
		CredentialTag: 0,
		Pool:          snapshotPool,
		Active:        true,
		AddedSlot:     boundarySlot,
		CreatedSlot:   0,
	}
	require.NoError(t, myStore.ImportAccount(account, nil))

	// A later re-delegation: a plain delegation certificate, not a registration.
	require.NoError(t, createTestTransactionMysql(myStore.DB(), 1, redelegationSlot))
	cert := models.Certificate{
		TransactionID: 1,
		CertIndex:     0,
		CertType:      uint(lcommon.CertificateTypeStakeDelegation),
		Slot:          redelegationSlot,
	}
	require.NoError(t, myStore.DB().Create(&cert).Error)
	require.NoError(t, myStore.DB().Create(&models.StakeDelegation{
		StakingKey:    stakeKey,
		PoolKeyHash:   laterPool,
		AddedSlot:     redelegationSlot,
		CertificateID: cert.ID,
	}).Error)
	require.NoError(t, myStore.DB().Model(&models.Account{}).
		Where("id = ?", account.ID).
		Updates(map[string]any{
			"pool":       laterPool,
			"added_slot": redelegationSlot,
		}).Error)

	require.NoError(t, myStore.RestoreAccountStateAtSlot(rollbackSlot, nil))

	var count int64
	require.NoError(t, myStore.DB().Model(&models.Account{}).
		Where("staking_key = ?", stakeKey).
		Count(&count).Error)
	require.EqualValues(
		t,
		1,
		count,
		"account that existed at the rollback slot must not be deleted just because its registration certificate predates the imported snapshot",
	)
}

// TestMysqlDeleteAccountRewardsAfterSlotToleratesMissingAccount mirrors the
// sqlite coverage for recovery on an already-damaged database: a reward delta
// whose account row is gone must not abort the rollback, because startup
// reconciliation rolls the ledger back through this path and a hard failure
// there is an unrecoverable boot crash-loop.
func TestMysqlDeleteAccountRewardsAfterSlotToleratesMissingAccount(
	t *testing.T,
) {
	myStore := newTestMysqlStore(t)
	defer myStore.Close() //nolint:errcheck
	cleanImportedAccountTables(myStore)

	stakeKey := testHash28("orphan_reward_delta")
	txHash := testHash32("orphan_withdrawal_tx")

	require.NoError(t, myStore.DB().Create(&models.AccountRewardDelta{
		StakingKey:     stakeKey,
		CredentialTag:  0,
		TxHash:         txHash,
		Amount:         types.Uint64(5_000_000),
		PreviousReward: types.Uint64(5_000_000),
		AddedSlot:      120,
		Withdrawal:     true,
	}).Error)

	require.NoError(
		t,
		myStore.DeleteAccountRewardsAfterSlot(100, nil),
		"rollback must complete despite a reward delta whose account row is missing",
	)

	var remaining int64
	require.NoError(t, myStore.DB().Model(&models.AccountRewardDelta{}).
		Where("staking_key = ?", stakeKey).
		Count(&remaining).Error)
	require.Zero(
		t,
		remaining,
		"stale reward-delta journal rows should be removed by the rollback",
	)
}
