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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type midnightStore interface {
	Transaction(ctx context.Context) types.Txn
	CreateMidnightAssetCreate(types.Txn, *models.MidnightAssetCreate) error
	CreateMidnightAssetSpend(types.Txn, *models.MidnightAssetSpend) error
	CreateMidnightRegistration(types.Txn, *models.MidnightRegistration) error
	CreateMidnightDeregistration(
		types.Txn,
		*models.MidnightDeregistration,
	) error
	FindUnspentMidnightAssetCreates() ([]models.MidnightAssetCreate, error)
	FindUnspentMidnightRegistrations() ([]models.MidnightRegistration, error)
	FindMidnightAssetCreatesFrom(
		uint64,
		uint32,
		int,
		types.Txn,
	) ([]models.MidnightAssetCreate, error)
	DeleteMidnightAssetCreatesByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAssetCreate, error)
	DeleteMidnightAssetSpendsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAssetSpend, error)
	DeleteMidnightRegistrationsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightRegistration, error)
	DeleteMidnightDeregistrationsByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightDeregistration, error)
	InsertMidnightGovernanceDatum(
		types.Txn,
		*models.MidnightGovernanceDatum,
	) error
	GetLatestMidnightGovernanceDatum(
		string,
		uint64,
		types.Txn,
	) (*models.MidnightGovernanceDatum, error)
	UpsertMidnightAriadneParams(
		types.Txn,
		*models.MidnightAriadneParams,
	) error
	GetLatestMidnightAriadneParams(
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	GetMidnightAriadneParamsAtOrBeforeEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightAriadneParams, error)
	CreateMidnightAriadneRollback(
		types.Txn,
		*models.MidnightAriadneRollback,
	) error
	FindMidnightAriadneRollbacksByBlock(
		types.Txn,
		uint64,
	) ([]models.MidnightAriadneRollback, error)
	UpsertMidnightEpochCandidates(
		types.Txn,
		*models.MidnightEpochCandidates,
	) error
	GetMidnightEpochCandidatesByEpoch(
		uint64,
		types.Txn,
	) (*models.MidnightEpochCandidates, error)
	InsertMidnightCommitteeCandidateRegistration(
		types.Txn,
		*models.MidnightCommitteeCandidateRegistration,
	) error
	GetMidnightCommitteeCandidateRegistrationsByTxHashes(
		[][]byte,
		types.Txn,
	) ([]models.MidnightCommitteeCandidateRegistration, error)
}

type midnightState struct {
	unspentAssets          []models.MidnightAssetCreate
	unspentRegistrations   []models.MidnightRegistration
	page                   []models.MidnightAssetCreate
	deletedCreates         []models.MidnightAssetCreate
	deletedSpends          []models.MidnightAssetSpend
	deletedRegistrations   []models.MidnightRegistration
	deletedDeregistrations []models.MidnightDeregistration
	governance             *models.MidnightGovernanceDatum
	latestAriadne          *models.MidnightAriadneParams
	historicalAriadne      *models.MidnightAriadneParams
	rollbacks              []models.MidnightAriadneRollback
	candidates             *models.MidnightEpochCandidates
	registrations          []models.MidnightCommitteeCandidateRegistration
}

func TestSharedSQLStoreMidnightParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseMidnightStore(t, store)
}

func exerciseMidnightStore(t *testing.T, store midnightStore) midnightState {
	t.Helper()
	txn := store.Transaction(t.Context())
	for _, row := range []*models.MidnightAssetCreate{
		{
			Address: []byte("address-a"), Quantity: 10,
			TxHash: []byte("create-a"), OutputIndex: 0,
			BlockNumber: 10, BlockHash: []byte("block-10"), TxIndex: 1,
			BlockTimestampMs: 1000,
		},
		{
			Address: []byte("address-b"), Quantity: 20,
			TxHash: []byte("create-b"), OutputIndex: 0,
			BlockNumber: 10, BlockHash: []byte("block-10"), TxIndex: 1,
			BlockTimestampMs: 1000,
		},
		{
			Address: []byte("address-c"), Quantity: 30,
			TxHash: []byte("create-c"), OutputIndex: 0,
			BlockNumber: 11, BlockHash: []byte("block-11"), TxIndex: 0,
			BlockTimestampMs: 1100,
		},
	} {
		require.NoError(t, store.CreateMidnightAssetCreate(txn, row))
	}
	require.NoError(t, store.CreateMidnightAssetSpend(
		txn,
		&models.MidnightAssetSpend{
			Address: []byte("address-a"), Quantity: 10,
			SpendingTxHash: []byte("spend-a"),
			UtxoTxHash:     []byte("create-a"), UtxoIndex: 0,
			BlockNumber: 12, BlockHash: []byte("block-12"),
			TxIndex: 0, BlockTimestampMs: 1200,
		},
	))
	for _, row := range []*models.MidnightRegistration{
		{
			FullDatum: []byte("registration-a"), TxHash: []byte("reg-a"),
			OutputIndex: 0, BlockNumber: 10, BlockHash: []byte("block-10"),
			TxIndex: 1, BlockTimestampMs: 1000,
		},
		{
			FullDatum: []byte("registration-b"), TxHash: []byte("reg-b"),
			OutputIndex: 0, BlockNumber: 11, BlockHash: []byte("block-11"),
			TxIndex: 0, BlockTimestampMs: 1100,
		},
	} {
		require.NoError(t, store.CreateMidnightRegistration(txn, row))
	}
	require.NoError(t, store.CreateMidnightDeregistration(
		txn,
		&models.MidnightDeregistration{
			FullDatum: []byte("deregistration-a"),
			TxHash:    []byte("dereg-a"), UtxoTxHash: []byte("reg-a"),
			UtxoIndex: 0, BlockNumber: 12, BlockHash: []byte("block-12"),
			TxIndex: 0, BlockTimestampMs: 1200,
		},
	))
	require.NoError(t, store.InsertMidnightGovernanceDatum(
		txn,
		&models.MidnightGovernanceDatum{
			DatumType: models.MidnightGovernanceDatumTypeCouncil,
			TxHash:    []byte("gov-a"), Datum: []byte("datum-a"),
			BlockNumber: 10,
		},
	))
	require.NoError(t, store.UpsertMidnightAriadneParams(
		txn,
		&models.MidnightAriadneParams{Epoch: 1, Datum: []byte("ariadne-a")},
	))
	require.NoError(t, store.UpsertMidnightAriadneParams(
		txn,
		&models.MidnightAriadneParams{Epoch: 2, Datum: []byte("ariadne-b")},
	))
	require.NoError(t, store.CreateMidnightAriadneRollback(
		txn,
		&models.MidnightAriadneRollback{
			BlockNumber: 12, Epoch: 2, PreviousExists: true,
			PreviousDatum: []byte("ariadne-a"),
		},
	))
	require.NoError(t, store.UpsertMidnightEpochCandidates(
		txn,
		&models.MidnightEpochCandidates{
			Epoch: 2, BlockNumber: 12, CandidatesCbor: []byte("candidates"),
		},
	))
	require.NoError(t, store.InsertMidnightCommitteeCandidateRegistration(
		txn,
		&models.MidnightCommitteeCandidateRegistration{
			TxHash: []byte("candidate-a"), BlockNumber: 12, SlotNumber: 120,
			TxIndex: 1, TxInputsCbor: []byte("inputs"),
		},
	))
	require.NoError(t, txn.Commit())

	var ret midnightState
	var err error
	ret.unspentAssets, err = store.FindUnspentMidnightAssetCreates()
	require.NoError(t, err)
	ret.unspentRegistrations, err = store.FindUnspentMidnightRegistrations()
	require.NoError(t, err)
	ret.page, err = store.FindMidnightAssetCreatesFrom(0, 0, 1, nil)
	require.NoError(t, err)
	ret.governance, err = store.GetLatestMidnightGovernanceDatum(
		models.MidnightGovernanceDatumTypeCouncil,
		11,
		nil,
	)
	require.NoError(t, err)
	ret.latestAriadne, err = store.GetLatestMidnightAriadneParams(nil)
	require.NoError(t, err)
	ret.historicalAriadne, err =
		store.GetMidnightAriadneParamsAtOrBeforeEpoch(1, nil)
	require.NoError(t, err)
	ret.rollbacks, err = store.FindMidnightAriadneRollbacksByBlock(nil, 12)
	require.NoError(t, err)
	ret.candidates, err = store.GetMidnightEpochCandidatesByEpoch(2, nil)
	require.NoError(t, err)
	ret.registrations, err =
		store.GetMidnightCommitteeCandidateRegistrationsByTxHashes(
			[][]byte{[]byte("candidate-a"), []byte("missing")},
			nil,
		)
	require.NoError(t, err)

	rollback := store.Transaction(t.Context())
	ret.deletedCreates, err = store.DeleteMidnightAssetCreatesByBlock(
		rollback,
		11,
	)
	require.NoError(t, err)
	ret.deletedSpends, err = store.DeleteMidnightAssetSpendsByBlock(
		rollback,
		12,
	)
	require.NoError(t, err)
	ret.deletedRegistrations, err = store.DeleteMidnightRegistrationsByBlock(
		rollback,
		11,
	)
	require.NoError(t, err)
	ret.deletedDeregistrations, err =
		store.DeleteMidnightDeregistrationsByBlock(rollback, 12)
	require.NoError(t, err)
	require.NoError(t, rollback.Commit())
	return ret
}
