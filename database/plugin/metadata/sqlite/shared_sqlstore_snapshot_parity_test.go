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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type snapshotStore interface {
	Transaction() types.Txn
	SavePoolStakeSnapshot(*models.PoolStakeSnapshot, types.Txn) error
	SavePoolStakeSnapshots([]*models.PoolStakeSnapshot, types.Txn) error
	GetPoolStakeSnapshot(
		uint64,
		string,
		[]byte,
		types.Txn,
	) (*models.PoolStakeSnapshot, error)
	GetPoolStakeSnapshotsByEpoch(
		uint64,
		string,
		types.Txn,
	) ([]*models.PoolStakeSnapshot, error)
	GetTotalActiveStake(uint64, string, types.Txn) (uint64, error)
	SaveEpochSummary(*models.EpochSummary, types.Txn) error
	GetEpochSummary(uint64, types.Txn) (*models.EpochSummary, error)
	GetLatestEpochSummary(types.Txn) (*models.EpochSummary, error)
	DeletePoolStakeSnapshotsForEpoch(uint64, string, types.Txn) error
	DeletePoolStakeSnapshotsAfterEpoch(uint64, types.Txn) error
	DeletePoolStakeSnapshotsBeforeEpoch(uint64, types.Txn) error
	DeleteEpochSummariesAfterEpoch(uint64, types.Txn) error
}

type snapshotState struct {
	pool             *models.PoolStakeSnapshot
	pools            []*models.PoolStakeSnapshot
	totalBeforeReady uint64
	totalAfterReady  uint64
	summary          *models.EpochSummary
	latest           *models.EpochSummary
	remaining        []*models.PoolStakeSnapshot
}

func TestSharedSQLStoreSnapshotParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseSnapshotStore(t, store)
}

func exerciseSnapshotStore(t *testing.T, store snapshotStore) snapshotState {
	t.Helper()
	poolA := []byte("pool-a")
	poolB := []byte("pool-b")

	txn := store.Transaction()
	require.NoError(t, store.SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:            1,
			SnapshotType:     models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:      poolA,
			TotalStake:       10,
			StakeDenominator: 100,
			DelegatorCount:   1,
			CapturedSlot:     10,
		},
		txn,
	))
	require.NoError(t, store.SavePoolStakeSnapshots(
		[]*models.PoolStakeSnapshot{
			{
				Epoch:                         1,
				SnapshotType:                  models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:                   poolA,
				TotalStake:                    20,
				StakeDenominator:              999,
				DelegatorCount:                2,
				CapturedSlot:                  20,
				RewardAccountAutoVote:         2,
				RewardAccountAutoVoteResolved: true,
			},
			{
				Epoch:            1,
				SnapshotType:     models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:      poolB,
				TotalStake:       30,
				StakeDenominator: 200,
				DelegatorCount:   3,
				CapturedSlot:     20,
			},
			{
				Epoch:        0,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  poolA,
				TotalStake:   5,
			},
			{
				Epoch:        2,
				SnapshotType: models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:  poolA,
				TotalStake:   40,
			},
			{
				Epoch:        1,
				SnapshotType: models.PoolStakeSnapshotTypeGo,
				PoolKeyHash:  poolA,
				TotalStake:   15,
			},
		},
		txn,
	))
	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            1,
			TotalActiveStake: 999,
			TotalPoolCount:   2,
			TotalDelegators:  5,
			EpochNonce:       []byte("nonce-one"),
			BoundarySlot:     20,
		},
		txn,
	))
	require.NoError(t, txn.Commit())

	var ret snapshotState
	var err error
	ret.pool, err = store.GetPoolStakeSnapshot(
		1,
		models.PoolStakeSnapshotTypeMark,
		poolA,
		nil,
	)
	require.NoError(t, err)
	ret.pools, err = store.GetPoolStakeSnapshotsByEpoch(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	ret.totalBeforeReady, err = store.GetTotalActiveStake(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)

	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            1,
			TotalActiveStake: 777,
			TotalPoolCount:   2,
			TotalDelegators:  5,
			EpochNonce:       []byte("nonce-two"),
			BoundarySlot:     21,
			SnapshotReady:    true,
		},
		nil,
	))
	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            1,
			TotalActiveStake: 888,
			TotalPoolCount:   2,
			TotalDelegators:  5,
			EpochNonce:       []byte("nonce-three"),
			BoundarySlot:     22,
		},
		nil,
	))
	require.NoError(t, store.SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            2,
			TotalActiveStake: 40,
			TotalPoolCount:   1,
			TotalDelegators:  1,
			BoundarySlot:     30,
			SnapshotReady:    true,
		},
		nil,
	))
	ret.totalAfterReady, err = store.GetTotalActiveStake(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	ret.summary, err = store.GetEpochSummary(1, nil)
	require.NoError(t, err)

	rollback := store.Transaction()
	require.NoError(t, store.DeletePoolStakeSnapshotsForEpoch(
		1,
		models.PoolStakeSnapshotTypeGo,
		rollback,
	))
	require.NoError(t, store.DeletePoolStakeSnapshotsAfterEpoch(1, rollback))
	require.NoError(t, store.DeletePoolStakeSnapshotsBeforeEpoch(1, rollback))
	require.NoError(t, store.DeleteEpochSummariesAfterEpoch(1, rollback))
	require.NoError(t, rollback.Commit())
	ret.remaining, err = store.GetPoolStakeSnapshotsByEpoch(
		1,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	ret.latest, err = store.GetLatestEpochSummary(nil)
	require.NoError(t, err)
	return ret
}
