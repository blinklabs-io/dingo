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
	"encoding/hex"
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func TestRewardLikelihoodReferenceArithmetic(t *testing.T) {
	current := epochengine.RewardPoolLikelihood(3, 1_000, 0.0005)
	require.Len(t, current, epochengine.RewardLikelihoodSampleCount)
	require.Equal(t, uint32(0xc17e5bef), math.Float32bits(current[0]))
	require.Equal(t, uint32(0xc016cef1), math.Float32bits(current[49]))
	require.Equal(t, uint32(0xbf02dbf1), math.Float32bits(current[99]))

	previous := make([]float32, epochengine.RewardLikelihoodSampleCount)
	for i := range previous {
		previous[i] = float32(i) / 10
	}
	combined, err := epochengine.CombineRewardLikelihood(previous, current)
	require.NoError(t, err)
	require.Equal(t, float32(0), combined[0])
	require.Equal(t, uint32(0x418f9bc7), math.Float32bits(combined[49]))
	require.Equal(t, uint32(0x41c25ec6), math.Float32bits(combined[99]))

	estimate, err := epochengine.RewardPerformanceEstimate(combined)
	require.NoError(t, err)
	require.Equal(t, 0.915, estimate)
}

func TestRewardLikelihoodEncodingRejectsWrongShape(t *testing.T) {
	_, err := epochengine.EncodeRewardLikelihood(make([]float32, 99))
	require.ErrorContains(t, err, "99 samples")
	_, err = epochengine.DecodeRewardLikelihood(make([]byte, 399))
	require.ErrorContains(t, err, "399 bytes")
}

func TestRewardLikelihoodHistoryCompleteness(t *testing.T) {
	bootstrap := epochengine.StakeRewardEpochs{Snapshot: 4, Pots: 4, Bootstrap: true}
	require.True(t, epochengine.RewardLikelihoodHistoryComplete(nil, 5, bootstrap))
	require.True(
		t,
		epochengine.RewardLikelihoodHistoryComplete(nil, 1, epochengine.StakeRewardEpochs{}),
	)
	require.False(
		t,
		epochengine.RewardLikelihoodHistoryComplete(nil, 5, epochengine.StakeRewardEpochs{}),
	)
	require.False(t, epochengine.RewardLikelihoodHistoryComplete(
		&models.RewardLikelihoodState{Complete: false},
		6,
		bootstrap,
	))
	require.True(t, epochengine.RewardLikelihoodHistoryComplete(
		&models.RewardLikelihoodState{Complete: true},
		6,
		epochengine.StakeRewardEpochs{},
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

func TestRewardLikelihoodStakeShareUsesCirculation(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	pool := bytes.Repeat([]byte{0x31}, 28)
	app := &epochengine.StakeRewardApplication{
		Params: rewards.Parameters{
			Decentralization: big.NewRat(0, 1),
			ActiveSlotsCoeff: big.NewRat(1, 10),
			EpochLength:      100,
		},
		Pots:                     &models.RewardAdaPots{},
		Epochs:                   epochengine.StakeRewardEpochs{Pots: 0},
		SnapshotTotalActiveStake: 100_000,
		TotalCirculation:         1_000_000,
		LikelihoodPoolInputs: []*models.RewardPoolInput{{
			PoolKeyHash:    pool,
			DelegatedStake: 10_000,
		}},
		LikelihoodBlockCounts: map[string]uint64{string(pool): 1},
	}
	require.NoError(t, ls.engine().SaveRewardLikelihoodForApplication(app, 10, nil))

	rows, err := ls.rewardLikelihoodDB().GetRewardPoolLikelihoods(1, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	got, err := epochengine.DecodeRewardLikelihood(rows[0].LogWeights)
	require.NoError(t, err)
	want, err := epochengine.CombineRewardLikelihood(
		make([]float32, epochengine.RewardLikelihoodSampleCount),
		epochengine.RewardPoolLikelihood(1, 100, epochengine.RewardLeaderProbability(0.1, 0.01, 0)),
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
	app := func(pots uint64) *epochengine.StakeRewardApplication {
		return &epochengine.StakeRewardApplication{
			Params: rewards.Parameters{
				Decentralization: big.NewRat(0, 1),
				ActiveSlotsCoeff: big.NewRat(1, 10),
				EpochLength:      100,
			},
			Pots:             &models.RewardAdaPots{CapturedSlot: pots * 100},
			Epochs:           epochengine.StakeRewardEpochs{Pots: pots},
			TotalCirculation: 1_000_000,
			LikelihoodPoolInputs: []*models.RewardPoolInput{{
				PoolKeyHash:    pool,
				DelegatedStake: 10_000,
			}},
			LikelihoodBlockCounts: map[string]uint64{string(pool): 1},
		}
	}
	txn := ls.ledgerDB().Begin(t.Context(), true)
	require.NoError(t, ls.engine().SaveRewardLikelihoodForApplication(app(0), 100, txn))
	require.NoError(t, ls.engine().SaveRewardLikelihoodForApplication(app(1), 200, txn))
	require.NoError(t, txn.Commit())
	txn.Release()

	rows, err := ls.rewardLikelihoodDB().GetRewardPoolLikelihoods(2, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	got, err := epochengine.DecodeRewardLikelihood(rows[0].LogWeights)
	require.NoError(t, err)
	current := epochengine.RewardPoolLikelihood(1, 100, epochengine.RewardLeaderProbability(0.1, 0.01, 0))
	first, err := epochengine.CombineRewardLikelihood(
		make([]float32, epochengine.RewardLikelihoodSampleCount), current,
	)
	require.NoError(t, err)
	want, err := epochengine.CombineRewardLikelihood(first, current)
	require.NoError(t, err)
	require.Equal(t, want, got)
	state, err := ls.rewardLikelihoodDB().GetRewardLikelihoodStateAtSlot(150, nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(1), state.Epoch)
}
