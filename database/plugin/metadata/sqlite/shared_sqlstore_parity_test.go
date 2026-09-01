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
	"math/big"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type operationalStore interface {
	Transaction(ctx context.Context) types.Txn
	GetTip(types.Txn) (ochainsync.Tip, error)
	SetTip(ochainsync.Tip, types.Txn) error
	SetNetworkState(uint64, uint64, uint64, types.Txn) error
	GetNetworkState(types.Txn) (*models.NetworkState, error)
	DeleteNetworkStateAfterSlot(uint64, types.Txn) error
	GetSyncState(string, types.Txn) (string, error)
	SetSyncState(string, string, types.Txn) error
	DeleteSyncState(string, types.Txn) error
	ClearSyncState(types.Txn) error
	SetEpoch(
		uint64,
		uint64,
		[]byte,
		[]byte,
		[]byte,
		[]byte,
		uint,
		uint,
		uint,
		types.Txn,
	) error
	GetEpoch(uint64, types.Txn) (*models.Epoch, error)
	GetEpochs(types.Txn) ([]models.Epoch, error)
	GetEpochsByEra(uint, types.Txn) ([]models.Epoch, error)
	GetEpochBySlot(uint64, types.Txn) (*models.Epoch, error)
	DeleteEpochsAfterSlot(uint64, types.Txn) error
	SetBlockNonce([]byte, uint64, []byte, bool, types.Txn) error
	GetBlockNonce(ocommon.Point, types.Txn) ([]byte, error)
	GetBlockNoncesInSlotRange(
		uint64,
		uint64,
		types.Txn,
	) ([]models.BlockNonce, error)
	GetLastBlockNonceInRange(uint64, uint64, types.Txn) ([]byte, error)
	DeleteBlockNoncesBeforeSlotWithoutCheckpoints(uint64, types.Txn) error
	DeleteBlockNoncesAfterPoint(ocommon.Point, types.Txn) error
	SetDatum(lcommon.Blake2b256, []byte, uint64, types.Txn) error
	GetDatum(lcommon.Blake2b256, types.Txn) (*models.Datum, error)
	SetPParams([]byte, uint64, uint64, uint, types.Txn) error
	GetPParams(uint64, uint, types.Txn) ([]models.PParams, error)
	SetPParamUpdate([]byte, []byte, uint64, uint64, types.Txn) error
	GetPParamUpdates(uint64, types.Txn) ([]models.PParamUpdate, error)
	DeletePParamsAfterSlot(uint64, types.Txn) error
	DeletePParamUpdatesAfterSlot(uint64, types.Txn) error
	AddNetworkDonation(uint64, uint64, uint64, types.Txn) error
	SumNetworkDonationsForEpoch(uint64, types.Txn) (uint64, error)
	DeleteNetworkDonationsAfterSlot(uint64, types.Txn) error
	GetImportCheckpoint(
		string,
		types.Txn,
	) (*models.ImportCheckpoint, error)
	SetImportCheckpoint(*models.ImportCheckpoint, types.Txn) error
	GetBackfillCheckpoint(string, types.Txn) (*models.BackfillCheckpoint, error)
	SetBackfillCheckpoint(*models.BackfillCheckpoint, types.Txn) error
	GetConstitution(types.Txn) (*models.Constitution, error)
	SetConstitution(*models.Constitution, types.Txn) error
	DeleteConstitutionsAfterSlot(uint64, types.Txn) error
	SetCommitteeMembers([]*models.CommitteeMember, types.Txn) error
	SetCommitteeQuorum(*types.Rat, uint64, types.Txn) error
	ClearCommitteeQuorum(uint64, types.Txn) error
	GetCommitteeQuorum(types.Txn) (*types.Rat, error)
	GetCommitteeMembers(types.Txn) ([]*models.CommitteeMember, error)
	GetCommitteeMembersIncludeDeleted(
		types.Txn,
	) ([]*models.CommitteeMember, error)
	SoftDeleteCommitteeMembers(
		[]models.CommitteeCredential,
		uint64,
		types.Txn,
	) error
	DeleteCommitteeMembersAfterSlot(uint64, types.Txn) error
}

type operationalSnapshot struct {
	tip                  ochainsync.Tip
	network              *models.NetworkState
	syncValue            string
	deletedSync          string
	epoch                *models.Epoch
	epochs               []models.Epoch
	epochsByEra          []models.Epoch
	epochBySlot          *models.Epoch
	nonce                []byte
	lastNonce            []byte
	nonces               []models.BlockNonce
	datum                *models.Datum
	pparams              []models.PParams
	pparamUpdates        []models.PParamUpdate
	donations            uint64
	importCheckpoint     *models.ImportCheckpoint
	backfillCheckpoint   *models.BackfillCheckpoint
	constitution         *models.Constitution
	committeeQuorum      *types.Rat
	committeeMembers     []*models.CommitteeMember
	allCommitteeMembers  []*models.CommitteeMember
	networkRollback      *models.NetworkState
	epochsRollback       []models.Epoch
	noncesRollback       []models.BlockNonce
	pparamsRollback      []models.PParams
	updatesRollback      []models.PParamUpdate
	donationsRollback    uint64
	constitutionRollback *models.Constitution
	quorumRollback       *types.Rat
	membersRollback      []*models.CommitteeMember
}

func TestSharedSQLStoreOperationalParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	snapshot := exerciseOperationalStore(t, store)
	require.Equal(t, uint64(7), snapshot.tip.BlockNumber)
	require.Equal(t, ^uint64(0), uint64(snapshot.network.Treasury))
	require.Equal(t, "value", snapshot.syncValue)
	require.Empty(t, snapshot.deletedSync)
	require.NotNil(t, snapshot.epoch)
	require.Len(t, snapshot.epochs, 2)
	require.Len(t, snapshot.nonces, 3)
	require.Equal(t, []byte("nonce-1-updated"), snapshot.nonce)
	require.Equal(t, uint64(16), snapshot.donations)
	require.NotNil(t, snapshot.datum)
	require.Len(t, snapshot.pparams, 1)
	require.Len(t, snapshot.pparamUpdates, 2)
	require.Nil(t, snapshot.committeeQuorum)
	require.Len(t, snapshot.committeeMembers, 1)
	require.Len(t, snapshot.allCommitteeMembers, 2)
	require.NotNil(t, snapshot.networkRollback)
	require.Len(t, snapshot.epochsRollback, 1)
	require.Equal(t, uint64(7), snapshot.donationsRollback)
}

func exerciseOperationalStore(
	t *testing.T,
	store operationalStore,
) operationalSnapshot {
	t.Helper()
	var datumHash lcommon.Blake2b256
	copy(datumHash[:], []byte("datum-hash"))
	hash1 := []byte("block-one")
	hash2 := []byte("block-two")
	hash3 := []byte("block-three")
	startedAt := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	updatedAt := startedAt.Add(time.Minute)
	deletedAt := uint64(30)

	txn := store.Transaction(t.Context())
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 42, Hash: []byte("tip-hash")},
		BlockNumber: 7,
	}, txn))
	require.NoError(t, store.SetNetworkState(10, 20, 5, txn))
	require.NoError(t, store.SetNetworkState(^uint64(0), 30, 10, txn))
	require.NoError(t, store.SetSyncState("keep", "value", txn))
	require.NoError(t, store.SetSyncState("delete", "value", txn))
	require.NoError(t, store.DeleteSyncState("delete", txn))
	require.NoError(t, store.SetEpoch(
		0,
		0,
		[]byte("nonce-0"),
		[]byte("evolving-0"),
		[]byte("candidate-0"),
		nil,
		1,
		1,
		100,
		txn,
	))
	require.NoError(t, store.SetEpoch(
		100,
		1,
		[]byte("nonce-1"),
		[]byte("evolving-1"),
		[]byte("candidate-1"),
		[]byte("lab-1"),
		2,
		2,
		200,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash1,
		1,
		[]byte("nonce-1"),
		false,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash1,
		1,
		[]byte("nonce-1-updated"),
		true,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash2,
		2,
		[]byte("nonce-2"),
		false,
		txn,
	))
	require.NoError(t, store.SetBlockNonce(
		hash3,
		3,
		[]byte("nonce-3"),
		false,
		txn,
	))
	require.NoError(t, store.SetDatum(
		datumHash,
		[]byte("datum"),
		42,
		txn,
	))
	require.NoError(t, store.SetPParams(
		[]byte("params-1"),
		20,
		1,
		2,
		txn,
	))
	require.NoError(t, store.SetPParams(
		[]byte("params-2"),
		30,
		2,
		2,
		txn,
	))
	require.NoError(t, store.SetPParamUpdate(
		[]byte("genesis"),
		[]byte("update-1"),
		25,
		1,
		txn,
	))
	require.NoError(t, store.SetPParamUpdate(
		[]byte("genesis"),
		[]byte("update-2"),
		35,
		2,
		txn,
	))
	require.NoError(t, store.AddNetworkDonation(20, 3, 7, txn))
	require.NoError(t, store.AddNetworkDonation(21, 3, 8, txn))
	require.NoError(t, store.AddNetworkDonation(21, 3, 9, txn))
	require.NoError(t, store.SetImportCheckpoint(
		&models.ImportCheckpoint{
			ImportKey: "snapshot:42",
			Phase:     models.ImportPhaseUTxO,
		},
		txn,
	))
	require.NoError(t, store.SetImportCheckpoint(
		&models.ImportCheckpoint{
			ImportKey: "snapshot:42",
			Phase:     models.ImportPhasePParams,
		},
		txn,
	))
	require.NoError(t, store.SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      "metadata",
			LastSlot:   10,
			TotalSlots: 100,
			StartedAt:  startedAt,
			UpdatedAt:  startedAt,
		},
		txn,
	))
	require.NoError(t, store.SetBackfillCheckpoint(
		&models.BackfillCheckpoint{
			Phase:      "metadata",
			LastSlot:   20,
			TotalSlots: 100,
			StartedAt:  startedAt.Add(time.Hour),
			UpdatedAt:  updatedAt,
			Completed:  true,
		},
		txn,
	))
	require.NoError(t, store.SetConstitution(
		&models.Constitution{
			AnchorURL:  "https://example.test/one",
			AnchorHash: []byte("constitution-one"),
			PolicyHash: []byte("policy-one"),
			AddedSlot:  10,
		},
		txn,
	))
	require.NoError(t, store.SetConstitution(
		&models.Constitution{
			AnchorURL:   "https://example.test/two",
			AnchorHash:  []byte("constitution-two"),
			PolicyHash:  []byte("policy-two"),
			AddedSlot:   20,
			DeletedSlot: &deletedAt,
		},
		txn,
	))
	require.NoError(t, store.SetCommitteeMembers(
		[]*models.CommitteeMember{
			{
				ColdCredHash: []byte("committee-one"),
				ExpiresEpoch: 100,
				AddedSlot:    10,
			},
			{
				ColdCredHash: []byte("committee-two"),
				ExpiresEpoch: 200,
				AddedSlot:    20,
			},
		},
		txn,
	))
	require.NoError(t, store.SetCommitteeQuorum(
		&types.Rat{Rat: big.NewRat(2, 3)},
		40,
		txn,
	))
	require.NoError(t, store.ClearCommitteeQuorum(50, txn))
	require.NoError(t, store.SoftDeleteCommitteeMembers(
		[]models.CommitteeCredential{{
			Credential: []byte("committee-two"),
		}},
		60,
		txn,
	))
	require.NoError(t, txn.Commit())

	var snapshot operationalSnapshot
	var err error
	snapshot.tip, err = store.GetTip(nil)
	require.NoError(t, err)
	snapshot.network, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	snapshot.syncValue, err = store.GetSyncState("keep", nil)
	require.NoError(t, err)
	snapshot.deletedSync, err = store.GetSyncState("delete", nil)
	require.NoError(t, err)
	snapshot.epoch, err = store.GetEpoch(1, nil)
	require.NoError(t, err)
	snapshot.epochs, err = store.GetEpochs(nil)
	require.NoError(t, err)
	snapshot.epochsByEra, err = store.GetEpochsByEra(2, nil)
	require.NoError(t, err)
	snapshot.epochBySlot, err = store.GetEpochBySlot(150, nil)
	require.NoError(t, err)
	snapshot.nonce, err = store.GetBlockNonce(
		ocommon.Point{Slot: 1, Hash: hash1},
		nil,
	)
	require.NoError(t, err)
	snapshot.lastNonce, err = store.GetLastBlockNonceInRange(0, 4, nil)
	require.NoError(t, err)
	snapshot.nonces, err = store.GetBlockNoncesInSlotRange(0, 4, nil)
	require.NoError(t, err)
	snapshot.datum, err = store.GetDatum(datumHash, nil)
	require.NoError(t, err)
	snapshot.pparams, err = store.GetPParams(2, 2, nil)
	require.NoError(t, err)
	snapshot.pparamUpdates, err = store.GetPParamUpdates(2, nil)
	require.NoError(t, err)
	snapshot.donations, err = store.SumNetworkDonationsForEpoch(3, nil)
	require.NoError(t, err)
	snapshot.importCheckpoint, err = store.GetImportCheckpoint(
		"snapshot:42",
		nil,
	)
	require.NoError(t, err)
	snapshot.backfillCheckpoint, err = store.GetBackfillCheckpoint(
		"metadata",
		nil,
	)
	require.NoError(t, err)
	snapshot.constitution, err = store.GetConstitution(nil)
	require.NoError(t, err)
	snapshot.committeeQuorum, err = store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	snapshot.committeeMembers, err = store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	snapshot.allCommitteeMembers, err = store.GetCommitteeMembersIncludeDeleted(
		nil,
	)
	require.NoError(t, err)

	rollbackTxn := store.Transaction(t.Context())
	require.NoError(t, store.DeleteNetworkStateAfterSlot(5, rollbackTxn))
	require.NoError(t, store.DeleteEpochsAfterSlot(50, rollbackTxn))
	require.NoError(t, store.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
		3,
		rollbackTxn,
	))
	require.NoError(t, store.DeleteBlockNoncesAfterPoint(
		ocommon.Point{Slot: 1, Hash: hash1},
		rollbackTxn,
	))
	require.NoError(t, store.DeletePParamsAfterSlot(25, rollbackTxn))
	require.NoError(t, store.DeletePParamUpdatesAfterSlot(30, rollbackTxn))
	require.NoError(t, store.DeleteNetworkDonationsAfterSlot(20, rollbackTxn))
	require.NoError(t, store.DeleteConstitutionsAfterSlot(15, rollbackTxn))
	require.NoError(t, store.DeleteCommitteeMembersAfterSlot(40, rollbackTxn))
	require.NoError(t, rollbackTxn.Commit())
	snapshot.networkRollback, err = store.GetNetworkState(nil)
	require.NoError(t, err)
	snapshot.epochsRollback, err = store.GetEpochs(nil)
	require.NoError(t, err)
	snapshot.noncesRollback, err = store.GetBlockNoncesInSlotRange(0, 4, nil)
	require.NoError(t, err)
	snapshot.pparamsRollback, err = store.GetPParams(2, 2, nil)
	require.NoError(t, err)
	snapshot.updatesRollback, err = store.GetPParamUpdates(2, nil)
	require.NoError(t, err)
	snapshot.donationsRollback, err = store.SumNetworkDonationsForEpoch(3, nil)
	require.NoError(t, err)
	snapshot.constitutionRollback, err = store.GetConstitution(nil)
	require.NoError(t, err)
	snapshot.quorumRollback, err = store.GetCommitteeQuorum(nil)
	require.NoError(t, err)
	snapshot.membersRollback, err = store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	return snapshot
}
