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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type accountStore interface {
	CreateAccount(types.Txn, *models.Account) error
	ImportAccount(*models.Account, types.Txn) error
	GetAccountByCredential(
		uint8,
		[]byte,
		bool,
		types.Txn,
	) (*models.Account, error)
	GetAccountsByCredential(
		[]models.StakeCredentialRef,
		bool,
		types.Txn,
	) (map[string]*models.Account, error)
	RenewAccountExpirations(
		[]models.StakeCredentialRef,
		uint64,
		types.Txn,
	) error
	StampAllActiveAccountExpirations(uint64, types.Txn) (int64, error)
	AccountInactivityActivationMembership(
		[]models.StakeCredentialRef,
		types.Txn,
	) (map[string]struct{}, error)
	ResetAccountExpirationActivation(
		types.Txn,
	) ([]models.StakeCredentialRef, error)
	GetActiveAccountCredentials(
		types.Txn,
	) ([]models.StakeCredentialRef, error)
	DeactivateAccounts(types.Txn, []models.StakeCredentialRef) error
	AddAccountRewardByCredential(
		uint8,
		[]byte,
		uint64,
		uint64,
		[]byte,
		types.Txn,
	) error
	ApplyAccountRewardWithdrawal(
		uint8,
		[]byte,
		uint64,
		uint64,
		[]byte,
		types.Txn,
	) error
	DeleteAccountRewardsAfterSlot(uint64, types.Txn) error
	GetAccountSumsByCredential(
		uint8,
		[]byte,
		types.Txn,
	) (models.AccountSums, error)
}

type accountState struct {
	active                  *models.Account
	inactiveHidden          *models.Account
	inactive                *models.Account
	activeBatch             map[string]*models.Account
	allBatch                map[string]*models.Account
	renewed                 *models.Account
	activeRefs              []models.StakeCredentialRef
	stamped                 int64
	membership              map[string]struct{}
	resetRefs               []models.StakeCredentialRef
	afterReset              *models.Account
	deactivated             *models.Account
	afterCredit             *models.Account
	afterWithdrawal         *models.Account
	afterWithdrawalRollback *models.Account
	afterCreditRollback     *models.Account
	accountSums             models.AccountSums
}

func TestSharedSQLStoreAccountParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseAccountStore(t, store)
}

func exerciseAccountStore(t *testing.T, store accountStore) accountState {
	t.Helper()
	activeKey := bytes.Repeat([]byte{0x11}, 28)
	inactiveKey := bytes.Repeat([]byte{0x22}, 28)
	require.NoError(t, store.CreateAccount(
		nil,
		&models.Account{
			StakingKey: activeKey, CredentialTag: 0,
			Pool: []byte("pool-a"), AddedSlot: 10, CreatedSlot: 5,
			CertificateID: 2, Reward: 30, Active: true,
			ExpirationEpoch: 100,
		},
	))
	require.NoError(t, store.CreateAccount(
		nil,
		&models.Account{
			StakingKey: inactiveKey, CredentialTag: 1,
			AddedSlot: 20, CreatedSlot: 15, Reward: 40,
		},
	))
	require.NoError(t, store.ImportAccount(
		&models.Account{
			StakingKey: activeKey, CredentialTag: 0,
			Pool: []byte("pool-b"), Drep: []byte("drep-a"),
			AddedSlot: 999, CreatedSlot: 999, CertificateID: 999,
			Reward: 50, DrepType: 1, Active: true,
			ExpirationEpoch: 999,
		},
		nil,
	))

	refs := []models.StakeCredentialRef{
		models.NewStakeCredentialRef(0, activeKey),
		models.NewStakeCredentialRef(1, inactiveKey),
		models.NewStakeCredentialRef(0, []byte("missing")),
	}
	var ret accountState
	var err error
	ret.active, err = store.GetAccountByCredential(0, activeKey, false, nil)
	require.NoError(t, err)
	ret.inactiveHidden, err = store.GetAccountByCredential(
		1,
		inactiveKey,
		false,
		nil,
	)
	require.NoError(t, err)
	ret.inactive, err = store.GetAccountByCredential(
		1,
		inactiveKey,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.activeBatch, err = store.GetAccountsByCredential(refs, false, nil)
	require.NoError(t, err)
	ret.allBatch, err = store.GetAccountsByCredential(refs, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.RenewAccountExpirations(refs, 55, nil))
	ret.renewed, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	ret.activeRefs, err = store.GetActiveAccountCredentials(nil)
	require.NoError(t, err)
	ret.stamped, err = store.StampAllActiveAccountExpirations(77, nil)
	require.NoError(t, err)
	ret.membership, err = store.AccountInactivityActivationMembership(
		refs,
		nil,
	)
	require.NoError(t, err)
	ret.resetRefs, err = store.ResetAccountExpirationActivation(nil)
	require.NoError(t, err)
	ret.afterReset, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, activeKey, 10, 80, []byte("credit"), nil,
	))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, activeKey, 10, 80, []byte("credit"), nil,
	))
	ret.afterCredit, err = store.GetAccountByCredential(0, activeKey, true, nil)
	require.NoError(t, err)
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, activeKey, 60, 90, []byte("withdraw"), nil,
	))
	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0, activeKey, 60, 90, []byte("withdraw"), nil,
	))
	ret.afterWithdrawal, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(85, nil))
	ret.afterWithdrawalRollback, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeleteAccountRewardsAfterSlot(75, nil))
	ret.afterCreditRollback, err = store.GetAccountByCredential(
		0, activeKey, true, nil,
	)
	require.NoError(t, err)
	ret.accountSums, err = store.GetAccountSumsByCredential(0, activeKey, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeactivateAccounts(
		nil,
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(1, inactiveKey),
		},
	))
	ret.deactivated, err = store.GetAccountByCredential(
		1,
		inactiveKey,
		true,
		nil,
	)
	require.NoError(t, err)
	return ret
}
