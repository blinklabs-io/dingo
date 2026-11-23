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
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

const (
	nonMyopicFirstPot  = uint64(777_000_000)
	nonMyopicSecondPot = uint64(555_000_000)
)

type nonMyopicFixture struct {
	ls    *LedgerState
	db    *database.Database
	poolA lcommon.PoolKeyHash
	poolB lcommon.PoolKeyHash
	// perfA is the performance estimate stored for pool A; pool B has no
	// stored likelihood and takes the default estimate.
	perfA       float64
	perfDefault float64
	tipHash     []byte
	firstHash   []byte
}

// newNonMyopicFixture seeds two registered pools with live stake and two
// completed likelihood states: epoch 0 installed at slot 1 and epoch 1
// installed at slot 60, with a different reward pot each.
func newNonMyopicFixture(t *testing.T, complete bool) *nonMyopicFixture {
	t.Helper()
	db := newTestDB(t)
	poolA := bytes.Repeat([]byte{0x11}, 28)
	poolB := bytes.Repeat([]byte{0x22}, 28)
	pkhA := seedLiveStakeFixtureWithPledge(
		t, db, poolA, bytes.Repeat([]byte{0xAA}, 32), 3_000_000, 1, 0, 0,
	)
	pkhB := seedLiveStakeFixtureWithPledge(
		t, db, poolB, bytes.Repeat([]byte{0xBB}, 32), 1_000_000, 1, 0, 0,
	)

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(
		t,
		completeTestShelleyGenesis(
			cfg,
		).LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 4000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)),
	)
	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 1, nil))

	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cfg
	pp := &conway.ConwayProtocolParameters{}
	pp.ProtocolVersion.Major = 9
	pp.NOpt = 2
	pp.A0 = &cbor.Rat{Rat: big.NewRat(3, 10)}
	pp.Rho = &cbor.Rat{Rat: big.NewRat(3, 1000)}
	pp.Tau = &cbor.Rat{Rat: big.NewRat(1, 5)}
	ls.currentPParams = pp
	ls.publishSnapshotsLocked()
	seedEpochs(t, ls, map[uint64]uint64{0: 0, 50: 1})

	firstHash := bytes.Repeat([]byte{0xE1}, 32)
	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 1, firstHash)
	seedBlockAtSlot(t, ls, 60, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(60, tipHash),
	}, nil))

	likelihood := rewardPoolLikelihood(1, 100, 0.05)
	encoded, err := encodeRewardLikelihood(likelihood)
	require.NoError(t, err)
	perfA, err := rewardPerformanceEstimate(likelihood)
	require.NoError(t, err)
	perfDefault, err := rewardPerformanceEstimate(
		make([]float32, rewardLikelihoodSampleCount),
	)
	require.NoError(t, err)
	require.NotEqual(t, perfA, perfDefault)

	require.NoError(t, ls.rewardLikelihoodDB().SaveRewardLikelihoodState(
		&models.RewardLikelihoodState{
			Epoch:        0,
			RewardPot:    dbtypes.Uint64(nonMyopicFirstPot),
			CapturedSlot: 0,
			BoundarySlot: 1,
			Complete:     true,
		},
		[]*models.RewardPoolLikelihood{{
			Epoch:               0,
			PoolKeyHash:         poolA,
			LogWeights:          encoded,
			PerformanceEstimate: perfA,
		}},
		nil,
	))
	require.NoError(t, ls.rewardLikelihoodDB().SaveRewardLikelihoodState(
		&models.RewardLikelihoodState{
			Epoch:        1,
			RewardPot:    dbtypes.Uint64(nonMyopicSecondPot),
			CapturedSlot: 50,
			BoundarySlot: 60,
			Complete:     complete,
		},
		[]*models.RewardPoolLikelihood{{
			Epoch:               1,
			PoolKeyHash:         poolA,
			LogWeights:          encoded,
			PerformanceEstimate: perfA,
		}},
		nil,
	))
	return &nonMyopicFixture{
		ls: ls, db: db, poolA: pkhA, poolB: pkhB,
		perfA: perfA, perfDefault: perfDefault,
		tipHash: tipHash, firstHash: firstHash,
	}
}

func rewardInfoPoolsQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyRewardInfoPoolsQuery{},
		},
	}
}

func nonMyopicMemberRewardsQuery(
	t *testing.T,
	keys ...olocalstatequery.NonMyopicMemberRewardsKey,
) *olocalstatequery.BlockQuery {
	t.Helper()
	raws := make([]cbor.RawMessage, 0, len(keys))
	for _, key := range keys {
		encoded, err := cbor.Encode(key)
		require.NoError(t, err)
		raws = append(raws, cbor.RawMessage(encoded))
	}
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyNonMyopicMemberRewardsQuery{
				Type:   olocalstatequery.QueryTypeShelleyNonMyopicMemberRewards,
				Stakes: cbor.NewSetType(raws, false),
			},
		},
	}
}

func decodeRewardInfoPools(
	t *testing.T,
	result any,
) olocalstatequery.RewardInfoPoolsResult {
	t.Helper()
	encoded, err := cbor.Encode(&result)
	require.NoError(t, err)
	var decoded olocalstatequery.RewardInfoPoolsResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	return decoded
}

func TestQueryRewardInfoPoolsAnswersFromTheAcquiredPointState(t *testing.T) {
	t.Parallel()
	f := newNonMyopicFixture(t, true)

	result, err := f.ls.Query(t.Context(), rewardInfoPoolsQuery(), QueryPoint{})
	require.NoError(t, err)
	info := decodeRewardInfoPools(t, result)
	require.Equal(t, nonMyopicSecondPot, info.Params.RPot)
	require.Equal(t, uint64(4_000_000), info.Params.TotalStake)
	require.Equal(t, uint16(2), info.Params.NOpt)
	require.Len(t, info.Pools, 2)
	a := info.Pools[lcommon.NewBlake2b224(f.poolA.Bytes())]
	b := info.Pools[lcommon.NewBlake2b224(f.poolB.Bytes())]
	require.Equal(t, uint64(3_000_000), a.Stake)
	require.Equal(t, uint64(1_000_000), b.Stake)
	require.Equal(t, f.perfA, a.PerformanceEstimate)
	require.Equal(t, f.perfDefault, b.PerformanceEstimate)

	// A pinned point before the second state was installed sees the first.
	pinned := QueryPoint{Slot: 1, Hash: f.firstHash}
	result, err = f.ls.Query(t.Context(), rewardInfoPoolsQuery(), pinned)
	require.NoError(t, err)
	require.Equal(
		t,
		nonMyopicFirstPot,
		decodeRewardInfoPools(t, result).Params.RPot,
	)
}

func TestQueryRewardInfoPoolsRefusesAnIncompleteHistory(t *testing.T) {
	t.Parallel()
	f := newNonMyopicFixture(t, false)

	_, err := f.ls.Query(t.Context(), rewardInfoPoolsQuery(), QueryPoint{})
	require.ErrorIs(t, err, ErrQueryNotAnswered)

	// The earlier, complete state still answers a point before it.
	result, err := f.ls.Query(
		t.Context(),
		rewardInfoPoolsQuery(),
		QueryPoint{Slot: 1, Hash: f.firstHash},
	)
	require.NoError(t, err)
	require.Equal(
		t,
		nonMyopicFirstPot,
		decodeRewardInfoPools(t, result).Params.RPot,
	)
}

func TestQueryNonMyopicMemberRewardsRanksPoolsByStoredPerformance(
	t *testing.T,
) {
	t.Parallel()
	f := newNonMyopicFixture(t, true)

	coin := olocalstatequery.NewNonMyopicCoinKey(1_000_000)
	query := nonMyopicMemberRewardsQuery(t, coin)
	result, err := f.ls.Query(t.Context(), query, QueryPoint{})
	require.NoError(t, err)
	values, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, values, 1)
	rewardsByKey, ok := values[0].(olocalstatequery.NonMyopicMemberRewardsResult)
	require.True(t, ok)
	byPool, ok := rewardsByKey[coin]
	require.True(t, ok)
	require.Len(t, byPool, 2)
	rewardA := byPool[lcommon.NewBlake2b224(f.poolA.Bytes())]
	rewardB := byPool[lcommon.NewBlake2b224(f.poolB.Bytes())]
	require.Positive(t, rewardA)
	require.NotEqual(t, rewardA, rewardB)

	// The same query at the earlier point uses the earlier reward pot, so
	// the rewards differ.
	earlier, err := f.ls.Query(
		t.Context(), query, QueryPoint{Slot: 1, Hash: f.firstHash},
	)
	require.NoError(t, err)
	earlierByPool := earlier.([]any)[0].(olocalstatequery.NonMyopicMemberRewardsResult)[coin]
	require.NotEqual(
		t,
		rewardA,
		earlierByPool[lcommon.NewBlake2b224(f.poolA.Bytes())],
	)

	f2 := newNonMyopicFixture(t, false)
	_, err = f2.ls.Query(t.Context(), query, QueryPoint{})
	require.ErrorIs(t, err, ErrQueryNotAnswered)
}

func TestQueryRewardInfoPoolsViaGetCBORReturnsBareContent(t *testing.T) {
	t.Parallel()
	f := newNonMyopicFixture(t, true)

	direct, err := f.ls.Query(t.Context(), rewardInfoPoolsQuery(), QueryPoint{})
	require.NoError(t, err)
	info := decodeRewardInfoPools(t, direct)

	result, err := f.ls.Query(
		t.Context(),
		&olocalstatequery.BlockQuery{
			Query: &olocalstatequery.ShelleyQuery{
				Query: &olocalstatequery.ShelleyCborQuery{
					Query: &olocalstatequery.ShelleyRewardInfoPoolsQuery{},
				},
			},
		},
		QueryPoint{},
	)
	require.NoError(t, err)
	values, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, values, 1)
	tag, ok := values[0].(cbor.Tag)
	require.True(t, ok)
	require.Equal(t, uint64(cbor.CborTagCbor), tag.Number)
	content, ok := tag.Content.([]byte)
	require.True(t, ok)

	var bare struct {
		cbor.StructAsArray
		Params olocalstatequery.RewardParams
		Pools  map[lcommon.Blake2b224]olocalstatequery.RewardInfoPool
	}
	_, err = cbor.Decode(content, &bare)
	require.NoError(t, err)
	require.Equal(t, info.Params.RPot, bare.Params.RPot)
	require.Len(t, bare.Pools, len(info.Pools))
}
