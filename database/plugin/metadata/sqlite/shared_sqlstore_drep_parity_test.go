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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type drepStore interface {
	CreateDrep(types.Txn, *models.Drep) error
	ImportDrep(*models.Drep, *models.RegistrationDrep, types.Txn) error
	GetDrep([]byte, bool, types.Txn) (*models.Drep, error)
	GetDrepByCredential(
		uint8,
		[]byte,
		bool,
		types.Txn,
	) (*models.Drep, error)
	GetActiveDreps(types.Txn) ([]*models.Drep, error)
	SetDrep(uint8, []byte, uint64, string, []byte, bool, types.Txn) error
	InsertDrepIfAbsent(
		uint8,
		[]byte,
		uint64,
		string,
		[]byte,
		bool,
		types.Txn,
	) error
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetDRepDelegators(
		uint8,
		[]byte,
		types.Txn,
	) ([]models.StakeCredentialRef, error)
	UpdateDRepActivity(uint8, []byte, uint64, uint64, types.Txn) error
	GetExpiredDReps(uint64, types.Txn) ([]*models.Drep, error)
	GetDrepLastRegistrationSlot(uint8, []byte, types.Txn) (uint64, error)
	GetDRepVotingPower(uint8, []byte, uint64, types.Txn) (uint64, error)
	GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef,
		uint64,
		types.Txn,
	) (map[string]uint64, error)
	GetDRepVotingPowerByType(
		[]uint64,
		uint64,
		types.Txn,
	) (map[uint64]uint64, error)
	GetDreps(types.Txn) ([]models.DrepListRow, error)
	GetPredefinedDrepFirstSeenSlots(types.Txn) (map[uint64]uint64, error)
	DeactivateDreps(types.Txn, []models.StakeCredentialRef) error
	ClearDanglingDRepDelegations(uint64, types.Txn) (int, error)
	GetLiveStakeInputsForPools(
		[][]byte,
		uint64,
		types.Txn,
	) ([]*models.RewardStakeInput, error)
	RewardLiveStakeNeedsBackfill(types.Txn) (bool, error)
	RebuildRewardLiveStake(uint64, types.Txn) error
}

type drepState struct {
	Created                 *models.Drep
	Imported                *models.Drep
	InactiveHidden          *models.Drep
	Inactive                *models.Drep
	Active                  []*models.Drep
	Delegators              []models.StakeCredentialRef
	Expired                 []*models.Drep
	LastRegistrationSlot    uint64
	MissingRegistrationSlot uint64
	MissingActivityError    string
	VotingPower             uint64
	VotingPowerBatch        map[string]uint64
	VotingPowerByType       map[uint64]uint64
	Dreps                   []models.DrepListRow
	PredefinedFirstSeen     map[uint64]uint64
	DanglingCleared         int
	Deactivated             *models.Drep
	LiveStake               []*models.RewardStakeInput
	LiveStakeNeedsBackfill  bool
	LiveStakeAfterRebuild   []*models.RewardStakeInput
}

func TestSharedSQLStoreDrepParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseDrepStore(t, store)
}

func exerciseDrepStore(t *testing.T, store drepStore) drepState {
	t.Helper()
	createdCredential := bytes.Repeat([]byte{0x41}, 28)
	importedCredential := bytes.Repeat([]byte{0x42}, 28)
	missingCredential := bytes.Repeat([]byte{0x43}, 28)

	created := &models.Drep{
		Credential: createdCredential, AddedSlot: 10,
		AnchorURL: "created", AnchorHash: []byte("created-hash"),
	}
	require.NoError(t, store.CreateDrep(nil, created))
	require.NoError(t, store.SetDrep(
		0,
		createdCredential,
		12,
		"inactive",
		[]byte("inactive-hash"),
		false,
		nil,
	))
	require.NoError(t, store.InsertDrepIfAbsent(
		0,
		createdCredential,
		99,
		"ignored",
		[]byte("ignored"),
		true,
		nil,
	))

	imported := &models.Drep{
		CredentialTag: 1, Credential: importedCredential,
		AddedSlot: 20, AnchorURL: "imported",
		AnchorHash: []byte("imported-hash"), Active: true,
	}
	registration := &models.RegistrationDrep{
		CredentialTag: 1, DrepCredential: importedCredential,
		AddedSlot: 21, CertificateID: 7, AnchorURL: "registered",
		AnchorHash: []byte("registration-hash"), DepositAmount: 500,
	}
	require.NoError(t, store.ImportDrep(imported, registration, nil))
	require.NoError(t, store.UpdateDRepActivity(
		1,
		importedCredential,
		30,
		5,
		nil,
	))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: bytes.Repeat([]byte{0x51}, 28),
		Pool:       []byte("pool-a"), Drep: importedCredential,
		DrepType: 1, Active: true, Reward: 100,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    bytes.Repeat([]byte{0x50}, 28),
		CredentialTag: 1, Drep: importedCredential,
		DrepType: 1, Active: true, Reward: 200,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x61}, 32),
		StakingKey: bytes.Repeat([]byte{0x51}, 28),
		Amount:     400,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: bytes.Repeat([]byte{0x52}, 28),
		DrepType:   models.DrepTypeAlwaysAbstain,
		Active:     true, Reward: 30, AddedSlot: 22,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x62}, 32),
		StakingKey: bytes.Repeat([]byte{0x52}, 28),
		Amount:     20,
	}))

	var ret drepState
	var err error
	ret.Created, err = store.GetDrep(createdCredential, true, nil)
	require.NoError(t, err)
	ret.Imported, err = store.GetDrepByCredential(
		1,
		importedCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.InactiveHidden, err = store.GetDrepByCredential(
		0,
		createdCredential,
		false,
		nil,
	)
	require.NoError(t, err)
	ret.Inactive, err = store.GetDrepByCredential(
		0,
		createdCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	ret.Active, err = store.GetActiveDreps(nil)
	require.NoError(t, err)
	ret.Delegators, err = store.GetDRepDelegators(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	ret.Expired, err = store.GetExpiredDReps(35, nil)
	require.NoError(t, err)
	ret.LastRegistrationSlot, err = store.GetDrepLastRegistrationSlot(
		1,
		importedCredential,
		nil,
	)
	require.NoError(t, err)
	ret.MissingRegistrationSlot, err = store.GetDrepLastRegistrationSlot(
		0,
		missingCredential,
		nil,
	)
	require.NoError(t, err)
	err = store.UpdateDRepActivity(0, missingCredential, 1, 1, nil)
	require.Error(t, err)
	require.True(t, errors.Is(err, models.ErrDrepActivityNotUpdated))
	ret.MissingActivityError = err.Error()
	ret.VotingPower, err = store.GetDRepVotingPower(
		1,
		importedCredential,
		0,
		nil,
	)
	require.NoError(t, err)
	ret.VotingPowerBatch, err = store.GetDRepVotingPowerBatch(
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(1, importedCredential),
			models.NewStakeCredentialRef(0, missingCredential),
		},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.VotingPowerByType, err = store.GetDRepVotingPowerByType(
		[]uint64{
			models.DrepTypeAlwaysAbstain,
			models.DrepTypeAlwaysNoConfidence,
		},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.Dreps, err = store.GetDreps(nil)
	require.NoError(t, err)
	ret.PredefinedFirstSeen, err = store.GetPredefinedDrepFirstSeenSlots(nil)
	require.NoError(t, err)
	ret.LiveStake, err = store.GetLiveStakeInputsForPools(
		[][]byte{[]byte("pool-a"), []byte("pool-a")},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.LiveStakeNeedsBackfill, err = store.RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.NoError(t, store.RebuildRewardLiveStake(88, nil))
	ret.LiveStakeAfterRebuild, err = store.GetLiveStakeInputsForPools(
		[][]byte{[]byte("pool-a")},
		0,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.DeactivateDreps(
		nil,
		[]models.StakeCredentialRef{
			models.NewStakeCredentialRef(1, importedCredential),
		},
	))
	ret.DanglingCleared, err = store.ClearDanglingDRepDelegations(99, nil)
	require.NoError(t, err)
	ret.Deactivated, err = store.GetDrepByCredential(
		1,
		importedCredential,
		true,
		nil,
	)
	require.NoError(t, err)
	return ret
}
