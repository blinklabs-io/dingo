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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type rewardStore interface {
	Transaction(ctx context.Context) types.Txn
	SaveRewardAdaPots(*models.RewardAdaPots, types.Txn) error
	GetRewardAdaPots(uint64, types.Txn) (*models.RewardAdaPots, error)
	SaveRewardSnapshot(*models.RewardSnapshot, types.Txn) error
	ClaimFallbackRewardSnapshot(
		*models.RewardSnapshot,
		types.Txn,
	) (bool, error)
	ClaimFallbackRewardSnapshotGuard(
		uint64,
		string,
		types.Txn,
	) (bool, uint, error)
	ReleaseFallbackRewardSnapshotGuard(uint, types.Txn) error
	GetRewardSnapshot(
		uint64,
		string,
		types.Txn,
	) (*models.RewardSnapshot, error)
	SaveRewardPoolInputs([]*models.RewardPoolInput, types.Txn) error
	GetRewardPoolInputs(
		uint64,
		types.Txn,
	) ([]*models.RewardPoolInput, error)
	SaveRewardStakeInputs([]*models.RewardStakeInput, types.Txn) error
	GetRewardStakeInputs(
		uint64,
		types.Txn,
	) ([]*models.RewardStakeInput, error)
	DeleteRewardInputsForEpoch(uint64, types.Txn) error
	SaveRewardPoolOutputs([]*models.RewardPoolOutput, types.Txn) error
	GetRewardPoolOutputs(
		uint64,
		types.Txn,
	) ([]*models.RewardPoolOutput, error)
	SaveRewardAccountOutputs([]*models.RewardAccountOutput, types.Txn) error
	GetRewardAccountOutputs(
		uint64,
		types.Txn,
	) ([]*models.RewardAccountOutput, error)
	DeleteRewardOutputsForEpoch(uint64, types.Txn) error
	DeleteRewardStateAfterSlot(uint64, types.Txn) error
	DeleteRewardStateBeforeEpoch(uint64, types.Txn) error
}

type rewardState struct {
	pots                    *models.RewardAdaPots
	authoritativeClaim      bool
	fallbackClaim           bool
	fallback                *models.RewardSnapshot
	guardCreated            bool
	guardRemoved            *models.RewardSnapshot
	provisionalGuard        bool
	provisionalGuardID      uint
	authoritativeGuard      bool
	poolInputs              []*models.RewardPoolInput
	stakeInputs             []*models.RewardStakeInput
	poolOutputs             []*models.RewardPoolOutput
	accountOutputs          []*models.RewardAccountOutput
	oldStakeInputs          []*models.RewardStakeInput
	recentStakeInputs       []*models.RewardStakeInput
	rolledBackPoolOutputs   []*models.RewardPoolOutput
	rolledBackAccountOutput []*models.RewardAccountOutput
}

func TestSharedSQLStoreRewardStateParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseRewardStore(t, store)
}

func exerciseRewardStore(t *testing.T, store rewardStore) rewardState {
	t.Helper()
	blocks := uint64(4)
	totalBlocks := uint64(10)

	require.NoError(t, store.SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        5,
			Treasury:     1,
			Reserves:     2,
			Fees:         3,
			Rewards:      4,
			CapturedSlot: 50,
		},
		nil,
	))
	require.NoError(t, store.SaveRewardAdaPots(
		&models.RewardAdaPots{
			Epoch:        5,
			Treasury:     10,
			Reserves:     20,
			Fees:         30,
			Rewards:      40,
			CapturedSlot: 55,
		},
		nil,
	))
	require.NoError(t, store.SaveRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:            5,
			SnapshotType:     "mark",
			TotalActiveStake: 100,
			TotalPoolCount:   1,
			TotalDelegators:  2,
			CapturedSlot:     50,
			BoundarySlot:     49,
			EpochNonce:       []byte("authoritative"),
			ProtocolVersion:  10,
			Authoritative:    true,
		},
		nil,
	))
	authoritativeClaim, err := store.ClaimFallbackRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:        5,
			SnapshotType: "mark",
		},
		nil,
	)
	require.NoError(t, err)
	fallbackClaim, err := store.ClaimFallbackRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:            6,
			SnapshotType:     "mark",
			TotalActiveStake: 200,
			TotalPoolCount:   2,
			TotalDelegators:  3,
			CapturedSlot:     60,
			BoundarySlot:     59,
			EpochNonce:       []byte("fallback-one"),
			ProtocolVersion:  10,
		},
		nil,
	)
	require.NoError(t, err)
	fallbackClaim, err = store.ClaimFallbackRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:            6,
			SnapshotType:     "mark",
			TotalActiveStake: 250,
			TotalPoolCount:   3,
			TotalDelegators:  4,
			CapturedSlot:     61,
			BoundarySlot:     60,
			EpochNonce:       []byte("fallback-two"),
			ProtocolVersion:  11,
		},
		nil,
	)
	require.NoError(t, err)

	guardTxn := store.Transaction(t.Context())
	guardCreated, guardID, err := store.ClaimFallbackRewardSnapshotGuard(
		7,
		"mark",
		guardTxn,
	)
	require.NoError(t, err)
	require.NoError(t, store.ReleaseFallbackRewardSnapshotGuard(
		guardID,
		guardTxn,
	))
	require.NoError(t, guardTxn.Commit())

	require.NoError(t, store.SaveRewardSnapshot(
		&models.RewardSnapshot{
			Epoch:        8,
			SnapshotType: "mark",
		},
		nil,
	))
	provisionalTxn := store.Transaction(t.Context())
	provisionalGuard, provisionalGuardID, err :=
		store.ClaimFallbackRewardSnapshotGuard(
			8,
			"mark",
			provisionalTxn,
		)
	require.NoError(t, err)
	require.NoError(t, provisionalTxn.Commit())

	authoritativeTxn := store.Transaction(t.Context())
	authoritativeGuard, _, err := store.ClaimFallbackRewardSnapshotGuard(
		5,
		"mark",
		authoritativeTxn,
	)
	require.NoError(t, err)
	require.NoError(t, authoritativeTxn.Commit())

	require.NoError(t, store.SaveRewardPoolInputs(
		[]*models.RewardPoolInput{
			{
				Margin:                     &types.Rat{Rat: big.NewRat(1, 5)},
				PoolKeyHash:                []byte("pool-a"),
				RewardAccount:              []byte("reward-a"),
				BlocksProduced:             &blocks,
				TotalBlocksInEpoch:         &totalBlocks,
				Epoch:                      5,
				Pledge:                     10,
				DelegatedStake:             100,
				OwnerStake:                 20,
				Cost:                       3,
				DelegatorCount:             2,
				RewardAccountCredentialTag: 1,
				CapturedSlot:               50,
				BoundarySlot:               49,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveRewardStakeInputs(
		[]*models.RewardStakeInput{
			{
				PoolKeyHash:   []byte("pool-a"),
				StakingKey:    []byte("stake-a"),
				Epoch:         5,
				CredentialTag: 1,
				Stake:         75,
				Owner:         true,
				Registered:    true,
				CapturedSlot:  50,
				BoundarySlot:  49,
			},
			{
				PoolKeyHash:  []byte("pool-old"),
				StakingKey:   []byte("stake-old"),
				Epoch:        1,
				Stake:        1,
				CapturedSlot: 10,
				BoundarySlot: 9,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveRewardPoolOutputs(
		[]*models.RewardPoolOutput{
			{
				ApparentPerformance: &types.Rat{Rat: big.NewRat(3, 4)},
				PoolKeyHash:         []byte("pool-a"),
				Epoch:               5,
				OptimalReward:       100,
				TotalReward:         90,
				LeaderReward:        10,
				MemberRewardTotal:   80,
				OwnerStake:          20,
				Undistributed:       5,
				Unspendable:         1,
				CapturedSlot:        50,
				BoundarySlot:        49,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveRewardAccountOutputs(
		[]*models.RewardAccountOutput{
			{
				StakingKey:    []byte("stake-a"),
				PoolKeyHash:   []byte("pool-a"),
				RewardType:    "member",
				Epoch:         5,
				CredentialTag: 1,
				Amount:        80,
				Spendable:     true,
				CapturedSlot:  50,
				BoundarySlot:  49,
			},
			{
				StakingKey:   []byte("stake-old"),
				PoolKeyHash:  []byte("pool-old"),
				RewardType:   "member",
				Epoch:        1,
				Amount:       1,
				CapturedSlot: 10,
				BoundarySlot: 9,
			},
		},
		nil,
	))

	ret := rewardState{
		authoritativeClaim: authoritativeClaim,
		fallbackClaim:      fallbackClaim,
		guardCreated:       guardCreated,
		provisionalGuard:   provisionalGuard,
		provisionalGuardID: provisionalGuardID,
		authoritativeGuard: authoritativeGuard,
	}
	ret.pots, err = store.GetRewardAdaPots(5, nil)
	require.NoError(t, err)
	ret.fallback, err = store.GetRewardSnapshot(6, "mark", nil)
	require.NoError(t, err)
	ret.guardRemoved, err = store.GetRewardSnapshot(7, "mark", nil)
	require.NoError(t, err)
	ret.poolInputs, err = store.GetRewardPoolInputs(5, nil)
	require.NoError(t, err)
	ret.stakeInputs, err = store.GetRewardStakeInputs(5, nil)
	require.NoError(t, err)
	ret.poolOutputs, err = store.GetRewardPoolOutputs(5, nil)
	require.NoError(t, err)
	ret.accountOutputs, err = store.GetRewardAccountOutputs(5, nil)
	require.NoError(t, err)

	require.NoError(t, store.DeleteRewardStateBeforeEpoch(5, nil))
	ret.oldStakeInputs, err = store.GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	ret.recentStakeInputs, err = store.GetRewardStakeInputs(5, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteRewardStateAfterSlot(49, nil))
	ret.rolledBackPoolOutputs, err = store.GetRewardPoolOutputs(5, nil)
	require.NoError(t, err)
	ret.rolledBackAccountOutput, err = store.GetRewardAccountOutputs(5, nil)
	require.NoError(t, err)
	return ret
}
