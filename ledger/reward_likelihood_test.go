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
	"bytes"
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestRewardLikelihoodReferenceArithmetic(t *testing.T) {
	current := rewardPoolLikelihood(3, 1_000, 0.0005)
	require.Len(t, current, rewardLikelihoodSampleCount)
	require.Equal(t, uint32(0xc17e5bef), math.Float32bits(current[0]))
	require.Equal(t, uint32(0xc016cef1), math.Float32bits(current[49]))
	require.Equal(t, uint32(0xbf02dbf1), math.Float32bits(current[99]))

	previous := make([]float32, rewardLikelihoodSampleCount)
	for i := range previous {
		previous[i] = float32(i) / 10
	}
	combined, err := combineRewardLikelihood(previous, current)
	require.NoError(t, err)
	require.Equal(t, float32(0), combined[0])
	require.Equal(t, uint32(0x418f9bc7), math.Float32bits(combined[49]))
	require.Equal(t, uint32(0x41c25ec6), math.Float32bits(combined[99]))

	estimate, err := rewardPerformanceEstimate(combined)
	require.NoError(t, err)
	require.Equal(t, 0.915, estimate)
}

func TestRewardLikelihoodEncodingRejectsWrongShape(t *testing.T) {
	_, err := encodeRewardLikelihood(make([]float32, 99))
	require.ErrorContains(t, err, "99 samples")
	_, err = decodeRewardLikelihood(make([]byte, 399))
	require.ErrorContains(t, err, "399 bytes")
}

func TestRewardLikelihoodHistoryCompleteness(t *testing.T) {
	bootstrap := stakeRewardEpochs{snapshot: 4, pots: 4, bootstrap: true}
	require.True(t, rewardLikelihoodHistoryComplete(nil, 5, bootstrap))
	require.True(
		t,
		rewardLikelihoodHistoryComplete(nil, 1, stakeRewardEpochs{}),
	)
	require.False(
		t,
		rewardLikelihoodHistoryComplete(nil, 5, stakeRewardEpochs{}),
	)
	require.False(t, rewardLikelihoodHistoryComplete(
		&models.RewardLikelihoodState{Complete: false},
		6,
		bootstrap,
	))
	require.True(t, rewardLikelihoodHistoryComplete(
		&models.RewardLikelihoodState{Complete: true},
		6,
		stakeRewardEpochs{},
	))
}

func TestNonMyopicMemberRewardUsesReferenceStakeChoice(t *testing.T) {
	poolKey := lcommon.PoolKeyHash(lcommon.NewBlake2b224(make([]byte, 28)))
	pool := nonMyopicPool{
		key:         poolKey,
		stake:       100_000_000,
		ownerStake:  10_000_000,
		pledge:      10_000_000,
		margin:      new(big.Rat),
		performance: 1,
	}
	state := &nonMyopicQueryState{
		params: rewards.Parameters{
			OptimalPoolCount: 10,
			PledgeInfluence:  new(big.Rat),
		},
		rewardPot:  1_000_000_000,
		totalStake: 1_000_000_000,
		topPools:   map[lcommon.PoolKeyHash]struct{}{poolKey: {}},
	}
	reward, err := nonMyopicMemberReward(pool, 10_000_000, state)
	require.NoError(t, err)
	require.Equal(t, uint64(9_090_909), reward)

	state.topPools = nil
	reward, err = nonMyopicMemberReward(pool, 10_000_000, state)
	require.NoError(t, err)
	require.Equal(t, uint64(10_000_000), reward)
}

func TestRewardLikelihoodStakeShareUsesCirculation(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	pool := bytes.Repeat([]byte{0x31}, 28)
	app := &stakeRewardApplication{
		params: rewards.Parameters{
			Decentralization: big.NewRat(0, 1),
			ActiveSlotsCoeff: big.NewRat(1, 10),
			EpochLength:      100,
		},
		pots:                     &models.RewardAdaPots{},
		epochs:                   stakeRewardEpochs{pots: 0},
		snapshotTotalActiveStake: 100_000,
		totalCirculation:         1_000_000,
		likelihoodPoolInputs: []*models.RewardPoolInput{{
			PoolKeyHash:    pool,
			DelegatedStake: 10_000,
		}},
		likelihoodBlockCounts: map[string]uint64{string(pool): 1},
	}
	require.NoError(t, ls.saveRewardLikelihoodForApplication(app, 10, nil))

	rows, err := ls.rewardLikelihoodDB().GetRewardPoolLikelihoods(1, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	got, err := decodeRewardLikelihood(rows[0].LogWeights)
	require.NoError(t, err)
	want, err := combineRewardLikelihood(
		make([]float32, rewardLikelihoodSampleCount),
		rewardPoolLikelihood(1, 100, rewardLeaderProbability(0.1, 0.01, 0)),
	)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// Two reward updates applied in one transaction chain their likelihoods: the
// second reads the first's pools from the transaction's staged changes, before
// either reaches the LedgerDB set.
func TestRewardLikelihoodChainsWithinOneTransaction(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	pool := bytes.Repeat([]byte{0x32}, 28)
	app := func(pots uint64) *stakeRewardApplication {
		return &stakeRewardApplication{
			params: rewards.Parameters{
				Decentralization: big.NewRat(0, 1),
				ActiveSlotsCoeff: big.NewRat(1, 10),
				EpochLength:      100,
			},
			pots:             &models.RewardAdaPots{CapturedSlot: pots * 100},
			epochs:           stakeRewardEpochs{pots: pots},
			totalCirculation: 1_000_000,
			likelihoodPoolInputs: []*models.RewardPoolInput{{
				PoolKeyHash:    pool,
				DelegatedStake: 10_000,
			}},
			likelihoodBlockCounts: map[string]uint64{string(pool): 1},
		}
	}
	txn := ls.ledgerDB().Begin(t.Context(), true)
	require.NoError(t, ls.saveRewardLikelihoodForApplication(app(0), 100, txn))
	require.NoError(t, ls.saveRewardLikelihoodForApplication(app(1), 200, txn))
	require.NoError(t, txn.Commit())
	txn.Release()

	rows, err := ls.rewardLikelihoodDB().GetRewardPoolLikelihoods(2, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	got, err := decodeRewardLikelihood(rows[0].LogWeights)
	require.NoError(t, err)
	current := rewardPoolLikelihood(1, 100, rewardLeaderProbability(0.1, 0.01, 0))
	first, err := combineRewardLikelihood(
		make([]float32, rewardLikelihoodSampleCount), current,
	)
	require.NoError(t, err)
	want, err := combineRewardLikelihood(first, current)
	require.NoError(t, err)
	require.Equal(t, want, got)
	state, err := ls.rewardLikelihoodDB().GetRewardLikelihoodStateAtSlot(150, nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(1), state.Epoch)
}
