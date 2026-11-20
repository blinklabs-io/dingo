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
	"encoding/hex"
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
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
	require.Equal(t, uint32(0x418f9bc8), math.Float32bits(combined[49]))
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
	require.True(t, rewardLikelihoodHistoryComplete(nil, 1, stakeRewardEpochs{}))
	require.False(t, rewardLikelihoodHistoryComplete(nil, 5, stakeRewardEpochs{}))
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

func TestNonMyopicQueryResultReferenceEncoding(t *testing.T) {
	pool := gledger.NewBlake2b224(make([]byte, 28))
	rewards := olocalstatequery.NonMyopicMemberRewardsResult{
		olocalstatequery.NewNonMyopicCoinKey(100): {pool: 3},
	}
	encoded, err := cbor.Encode([]any{rewards})
	require.NoError(t, err)
	require.Equal(
		t,
		"81a182001864a1581c0000000000000000000000000000000000000000000000000000000003",
		hex.EncodeToString(encoded),
	)

	rewardInfo := olocalstatequery.RewardInfoPoolsResult{
		Params: olocalstatequery.RewardParams{
			NOpt:       1,
			A0:         &cbor.Rat{Rat: big.NewRat(0, 1)},
			RPot:       2,
			TotalStake: 3,
		},
		Pools: map[gledger.Blake2b224]olocalstatequery.RewardInfoPool{},
	}
	encoded, err = cbor.Encode(rewardInfo)
	require.NoError(t, err)
	require.Equal(
		t,
		"81828401d81e8200010203a0",
		hex.EncodeToString(encoded),
	)
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
