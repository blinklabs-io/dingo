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

package ledger

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/stretchr/testify/require"
)

func TestStakeRewardEpochsForInitialApplication(t *testing.T) {
	for _, epoch := range []uint64{0, 1} {
		_, ok := stakeRewardEpochsForApplication(epoch)
		require.False(t, ok, "epoch %d must not apply stake rewards", epoch)
	}

	epochs, ok := stakeRewardEpochsForApplication(2)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 0,
		pots:        1,
		bootstrap:   true,
	}, epochs)

	epochs, ok = stakeRewardEpochsForApplication(3)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    0,
		performance: 1,
		pots:        2,
	}, epochs)
}

func TestSuppressBootstrapStakeRewardsReturnsAvailableRewardsToReserves(t *testing.T) {
	result := &rewards.Result{
		PoolRewards:      []rewards.PoolReward{{PoolReward: 600}},
		AccountRewards:   []rewards.AccountReward{{Amount: 600}},
		TotalRewardPot:   1_000,
		AvailableRewards: 800,
		EffectiveRewards: 600,
		Unspendable:      50,
		Undistributed:    150,
	}
	suppressBootstrapStakeRewards(result)

	require.Empty(t, result.PoolRewards)
	require.Empty(t, result.AccountRewards)
	require.Zero(t, result.EffectiveRewards)
	require.Zero(t, result.Unspendable)
	require.Equal(t, uint64(800), result.Undistributed)

	app := &stakeRewardApplication{
		params: rewards.Parameters{
			TreasuryExpansion: big.NewRat(1, 5),
		},
		pots: &models.RewardAdaPots{
			Reserves: types.Uint64(10_000),
			Treasury: types.Uint64(10),
		},
		totalRewardPot:   result.TotalRewardPot,
		availableRewards: result.AvailableRewards,
		undistributed:    result.Undistributed,
	}
	reserves, treasury, err := stakeRewardUpdatedPots(app)
	require.NoError(t, err)
	require.Equal(t, uint64(9_800), reserves)
	require.Equal(t, uint64(210), treasury)
}

func TestBootstrapStakeRewardsRejectStalePrecompute(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	require.NoError(t, db.Metadata().SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:   1,
		Rewards: types.Uint64(1_000),
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(txn, 2, 200)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)

	app, ok, err = ls.precomputeStakeRewardsCalculate(txn, 2, 100, 200)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}
