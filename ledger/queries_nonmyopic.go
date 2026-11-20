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
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

type nonMyopicPool struct {
	key           lcommon.PoolKeyHash
	stake         uint64
	ownerStake    uint64
	pledge        uint64
	cost          uint64
	margin        *big.Rat
	performance   float64
	desirability  float64
	hasLikelihood bool
}

type nonMyopicQueryState struct {
	pools           []nonMyopicPool
	credentialStake map[string]uint64
	params          rewards.Parameters
	rewardPot       uint64
	totalStake      uint64
	minPoolCost     uint64
	topPools        map[lcommon.PoolKeyHash]struct{}
}

func nonMyopicCredentialKey(tag uint8, hash []byte) string {
	return string(append([]byte{tag}, hash...))
}

func nonMyopicRatOrZero(value *big.Rat) *big.Rat {
	if value == nil {
		return new(big.Rat)
	}
	return new(big.Rat).Set(value)
}

func nonMyopicShare(value, total uint64) *big.Rat {
	if total == 0 {
		return new(big.Rat)
	}
	return new(big.Rat).SetFrac(
		new(big.Int).SetUint64(value),
		new(big.Int).SetUint64(total),
	)
}

func minNonMyopicRat(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) <= 0 {
		return new(big.Rat).Set(a)
	}
	return new(big.Rat).Set(b)
}

func maxNonMyopicRat(a, b *big.Rat) *big.Rat {
	if a.Cmp(b) >= 0 {
		return new(big.Rat).Set(a)
	}
	return new(big.Rat).Set(b)
}

func floorNonMyopicRat(value *big.Rat) *big.Int {
	quotient, remainder := new(big.Int).QuoRem(
		value.Num(),
		value.Denom(),
		new(big.Int),
	)
	if value.Sign() < 0 && remainder.Sign() != 0 {
		quotient.Sub(quotient, big.NewInt(1))
	}
	return quotient
}

func nonMyopicMaxPool(
	rewardPot uint64,
	nOpt uint64,
	a0 *big.Rat,
	sigma *big.Rat,
	pledge *big.Rat,
	leverage *big.Rat,
) *big.Int {
	if nOpt == 0 {
		nOpt = 1
	}
	z0 := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).SetUint64(nOpt))
	s := minNonMyopicRat(sigma, z0)
	if leverage != nil {
		s = minNonMyopicRat(s, new(big.Rat).Mul(leverage, pledge))
	}
	p := minNonMyopicRat(pledge, z0)
	a0 = nonMyopicRatOrZero(a0)
	left := new(big.Rat).Quo(
		new(big.Rat).SetInt(new(big.Int).SetUint64(rewardPot)),
		new(big.Rat).Add(big.NewRat(1, 1), a0),
	)
	pledgeDiscount := new(big.Rat).Quo(
		new(big.Rat).Mul(p, new(big.Rat).Sub(z0, s)),
		z0,
	)
	right := new(big.Rat).Add(
		s,
		new(big.Rat).Mul(
			new(big.Rat).Mul(p, a0),
			new(big.Rat).Quo(new(big.Rat).Sub(s, pledgeDiscount), z0),
		),
	)
	return floorNonMyopicRat(new(big.Rat).Mul(left, right))
}

func nonMyopicPoolDesirability(
	pool nonMyopicPool,
	state *nonMyopicQueryState,
) float64 {
	nOpt := state.params.OptimalPoolCount
	if nOpt == 0 {
		nOpt = 1
	}
	z0 := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).SetUint64(nOpt))
	pledge := nonMyopicShare(pool.pledge, state.totalStake)
	inner := new(big.Rat).Add(
		z0,
		new(big.Rat).Mul(
			minNonMyopicRat(pledge, z0),
			nonMyopicRatOrZero(state.params.PledgeInfluence),
		),
	)
	fTildeRat := new(big.Rat).Quo(
		new(big.Rat).Mul(
			new(big.Rat).SetInt(new(big.Int).SetUint64(state.rewardPot)),
			inner,
		),
		new(big.Rat).Add(
			big.NewRat(1, 1),
			nonMyopicRatOrZero(state.params.PledgeInfluence),
		),
	)
	fTilde, _ := fTildeRat.Float64()
	fTilde *= pool.performance
	if fTilde <= float64(pool.cost) {
		return 0
	}
	margin, _ := nonMyopicRatOrZero(pool.margin).Float64()
	return (fTilde - float64(pool.cost)) * (1 - margin)
}

func nonMyopicMemberReward(
	pool nonMyopicPool,
	memberStake uint64,
	state *nonMyopicQueryState,
) (uint64, error) {
	if pool.pledge > pool.ownerStake {
		return 0, nil
	}
	pledge := nonMyopicShare(pool.pledge, state.totalStake)
	sigma := nonMyopicShare(pool.stake, state.totalStake)
	member := nonMyopicShare(memberStake, state.totalStake)
	nOpt := state.params.OptimalPoolCount
	if nOpt == 0 {
		nOpt = 1
	}
	z0 := new(big.Rat).SetFrac(big.NewInt(1), new(big.Int).SetUint64(nOpt))
	var nonMyopicStake *big.Rat
	if _, top := state.topPools[pool.key]; top {
		nonMyopicStake = maxNonMyopicRat(new(big.Rat).Add(sigma, member), z0)
	} else {
		nonMyopicStake = new(big.Rat).Add(pledge, member)
	}
	leverage := (*big.Rat)(nil)
	if state.params.PledgeLeverageEnabled {
		leverage = state.params.PledgeLeverage
	}
	maxPool := nonMyopicMaxPool(
		state.rewardPot,
		state.params.OptimalPoolCount,
		state.params.PledgeInfluence,
		nonMyopicStake,
		pledge,
		leverage,
	)
	maxPoolFloat, _ := new(big.Float).SetInt(maxPool).Float64()
	poolReward := uint64(0)
	if value := mathFloorToBigInt(pool.performance * maxPoolFloat); value.Sign() > 0 {
		if !value.IsUint64() {
			return 0, errors.New("non-myopic pool reward overflows uint64")
		}
		poolReward = value.Uint64()
	}
	cost := pool.cost
	margin := nonMyopicRatOrZero(pool.margin)
	if state.params.ProtocolMajorVersion >= 12 {
		if cost < state.minPoolCost {
			cost = state.minPoolCost
		}
		if state.params.MinPoolMargin != nil && margin.Cmp(state.params.MinPoolMargin) < 0 {
			margin = new(big.Rat).Set(state.params.MinPoolMargin)
		}
	}
	if poolReward == 0 || poolReward <= cost || nonMyopicStake.Sign() == 0 {
		return 0, nil
	}
	variable := new(big.Rat).SetInt(
		new(big.Int).SetUint64(poolReward - cost),
	)
	reward := new(big.Rat).Mul(variable, new(big.Rat).Sub(big.NewRat(1, 1), margin))
	reward.Mul(reward, member)
	reward.Quo(reward, nonMyopicStake)
	result := floorNonMyopicRat(reward)
	if result.Sign() <= 0 {
		return 0, nil
	}
	if !result.IsUint64() {
		return 0, errors.New("non-myopic member reward overflows uint64")
	}
	return result.Uint64(), nil
}

func mathFloorToBigInt(value float64) *big.Int {
	ret, _ := new(big.Float).SetFloat64(value).Int(nil)
	if value < 0 && new(big.Float).SetInt(ret).Cmp(new(big.Float).SetFloat64(value)) != 0 {
		ret.Sub(ret, big.NewInt(1))
	}
	return ret
}

func (ls *LedgerState) nonMyopicState(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (*nonMyopicQueryState, error) {
	txn, release := ls.readTxn(ctx, txn)
	defer release()
	consensus := ls.loadConsensusSnapshot()
	tip, epoch, err := ls.epochAtPoint(at, txn)
	if err != nil {
		return nil, err
	}
	if epoch == nil && at.pinned() {
		return nil, errEpochNotResolved(at)
	}
	epochRow := consensus.currentEpoch
	if epoch != nil {
		epochRow = *epoch
	}
	pparams, err := ls.snapshotProtocolParameters(consensus, epoch, txn)
	if err != nil {
		return nil, err
	}
	params, err := rewardParametersFromPParams(
		pparams,
		ls.config.CardanoNodeConfig,
		uint64(epochRow.LengthInSlots),
	)
	if err != nil {
		return nil, err
	}
	if pp, ok := pparams.(*dijkstra.DijkstraProtocolParameters); ok {
		params.MinPoolMargin = cloneCBORRat(pp.MinPoolMargin)
		params.PledgeLeverage = cloneCBORRat(pp.MaxPledgeLeverage)
		params.PledgeLeverageEnabled = params.PledgeLeverage != nil
	}
	var minPoolCost uint64
	if pp, ok := pparams.(interface{ MinPoolCostValue() uint64 }); ok {
		minPoolCost = pp.MinPoolCostValue()
	}
	calc := snapshot.NewCalculator(ls.ledgerDB(), ls.config.Logger)
	distribution, err := calc.CalculateStakeDistributionWithInputsInTxn(
		ctx,
		txn,
		tip.Point.Slot,
	)
	if err != nil {
		return nil, err
	}
	likelihoodState, err := ls.ledgerDB().GetRewardLikelihoodStateAtSlot(
		tip.Point.Slot,
		txn,
	)
	if err != nil {
		return nil, err
	}
	if likelihoodState == nil {
		completeDefault := epochRow.EpochId == 0
		if !completeDefault && epochRow.EraId == eras.ShelleyEraDesc.Id {
			previousEpoch, err := ls.ledgerDB().GetEpoch(epochRow.EpochId-1, txn)
			if err != nil {
				return nil, err
			}
			completeDefault = previousEpoch != nil &&
				previousEpoch.EraId == eras.ByronEraDesc.Id
		}
		if completeDefault {
			likelihoodState = &models.RewardLikelihoodState{Complete: true}
		}
	}
	if likelihoodState == nil || !likelihoodState.Complete {
		return nil, queryNotAnswered(
			"non-myopic reward queries",
			"a complete likelihood history through the acquired point",
		)
	}
	likelihoods, err := ls.ledgerDB().GetRewardPoolLikelihoods(
		likelihoodState.Epoch,
		txn,
	)
	if err != nil {
		return nil, err
	}
	performance := make(map[string]float64, len(likelihoods))
	for _, item := range likelihoods {
		performance[string(item.PoolKeyHash)] = item.PerformanceEstimate
	}
	defaultPerformance, err := rewardPerformanceEstimate(
		make([]float32, rewardLikelihoodSampleCount),
	)
	if err != nil {
		return nil, err
	}
	active, err := ls.ledgerDB().GetActivePoolKeyHashesAtSlot(tip.Point.Slot, txn)
	if err != nil {
		return nil, err
	}
	keys := make([]lcommon.PoolKeyHash, 0, len(active))
	for _, key := range active {
		keys = append(keys, lcommon.PoolKeyHash(lcommon.NewBlake2b224(key)))
	}
	registrations, err := ls.ledgerDB().GetPoolRegistrationsEffectiveForEpoch(
		keys,
		epochRow.StartSlot,
		epochRow.EpochId,
		tip.Point.Slot,
		txn,
	)
	if err != nil {
		return nil, err
	}
	credentialStake := make(map[string]uint64, len(distribution.StakeInputs))
	for _, input := range distribution.StakeInputs {
		if input.Registered {
			credentialStake[nonMyopicCredentialKey(input.CredentialTag, input.StakingKey)] = input.Stake
		}
	}
	var networkStateAsOfSlot *uint64
	if at.pinned() {
		s := at.Slot
		networkStateAsOfSlot = &s
	}
	totalStake, err := ls.totalCirculatingSupply(
		epochRow.EpochId,
		networkStateAsOfSlot,
		true,
		txn,
	)
	if err != nil {
		return nil, err
	}
	state := &nonMyopicQueryState{
		pools:           make([]nonMyopicPool, 0, len(registrations)),
		credentialStake: credentialStake,
		params:          params,
		rewardPot:       uint64(likelihoodState.RewardPot),
		totalStake:      totalStake,
		minPoolCost:     minPoolCost,
	}
	for i := range registrations {
		reg := &registrations[i]
		key := lcommon.PoolKeyHash(lcommon.NewBlake2b224(reg.PoolKeyHash))
		ownerStake := uint64(0)
		owners := make(map[string]struct{}, len(reg.Owners))
		for _, owner := range reg.Owners {
			owners[string(owner.KeyHash)] = struct{}{}
		}
		for _, input := range distribution.StakeInputs {
			if !input.Registered || input.CredentialTag != 0 ||
				!bytes.Equal(input.PoolKeyHash, reg.PoolKeyHash) {
				continue
			}
			if _, ok := owners[string(input.StakingKey)]; ok {
				if ownerStake > ^uint64(0)-input.Stake {
					return nil, errors.New("non-myopic owner stake overflow")
				}
				ownerStake += input.Stake
			}
		}
		poolPerformance, ok := performance[string(reg.PoolKeyHash)]
		hasLikelihood := ok
		if !ok {
			poolPerformance = defaultPerformance
		}
		margin := new(big.Rat)
		if reg.Margin != nil && reg.Margin.Rat != nil {
			margin.Set(reg.Margin.Rat)
		}
		state.pools = append(state.pools, nonMyopicPool{
			key:           key,
			stake:         distribution.PoolStakes[key],
			ownerStake:    ownerStake,
			pledge:        uint64(reg.Pledge),
			cost:          uint64(reg.Cost),
			margin:        margin,
			performance:   poolPerformance,
			hasLikelihood: hasLikelihood,
		})
	}
	for i := range state.pools {
		state.pools[i].desirability = nonMyopicPoolDesirability(state.pools[i], state)
	}
	slices.SortStableFunc(state.pools, func(a, b nonMyopicPool) int {
		if a.desirability > b.desirability {
			return -1
		}
		if a.desirability < b.desirability {
			return 1
		}
		return bytes.Compare(a.key.Bytes(), b.key.Bytes())
	})
	if state.params.OptimalPoolCount > math.MaxInt {
		return nil, errors.New("non-myopic optimal pool count exceeds int range")
	}
	//nolint:gosec // bounded by math.MaxInt above
	limit := int(state.params.OptimalPoolCount)
	if limit == 0 {
		limit = 1
	}
	limit = min(limit, len(state.pools))
	state.topPools = make(map[lcommon.PoolKeyHash]struct{}, limit)
	for _, pool := range state.pools {
		if !pool.hasLikelihood {
			continue
		}
		state.topPools[pool.key] = struct{}{}
		if len(state.topPools) == limit {
			break
		}
	}
	return state, nil
}

func (ls *LedgerState) queryShelleyRewardInfoPools(
	ctx context.Context,
	at QueryPoint,
	txn ledgerdb.Txn,
) (any, error) {
	state, err := ls.nonMyopicState(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	if state.params.OptimalPoolCount > math.MaxUint16 {
		return nil, errors.New("non-myopic optimal pool count exceeds uint16 range")
	}
	result := olocalstatequery.RewardInfoPoolsResult{
		Params: olocalstatequery.RewardParams{
			//nolint:gosec // bounded by math.MaxUint16 above
			NOpt:       uint16(state.params.OptimalPoolCount),
			A0:         &cbor.Rat{Rat: nonMyopicRatOrZero(state.params.PledgeInfluence)},
			RPot:       state.rewardPot,
			TotalStake: state.totalStake,
		},
		Pools: make(map[gledger.Blake2b224]olocalstatequery.RewardInfoPool, len(state.pools)),
	}
	for _, pool := range state.pools {
		result.Pools[gledger.NewBlake2b224(pool.key.Bytes())] = olocalstatequery.RewardInfoPool{
			Stake:               pool.stake,
			OwnerPledge:         pool.pledge,
			OwnerStake:          pool.ownerStake,
			Cost:                pool.cost,
			Margin:              &cbor.Rat{Rat: nonMyopicRatOrZero(pool.margin)},
			PerformanceEstimate: pool.performance,
		}
	}
	return result, nil
}

func (ls *LedgerState) queryShelleyNonMyopicMemberRewards(
	ctx context.Context,
	q *olocalstatequery.ShelleyNonMyopicMemberRewardsQuery,
	at QueryPoint,
	txn ledgerdb.Txn,
) (any, error) {
	state, err := ls.nonMyopicState(ctx, at, txn)
	if err != nil {
		return nil, err
	}
	result := make(olocalstatequery.NonMyopicMemberRewardsResult, len(q.Stakes.Items()))
	for _, raw := range q.Stakes.Items() {
		var key olocalstatequery.NonMyopicMemberRewardsKey
		if _, err := cbor.Decode(raw, &key); err != nil {
			return nil, fmt.Errorf("decode non-myopic member rewards key: %w", err)
		}
		stake := key.Coin
		if key.IsCredential {
			var credentialTag uint8
			switch key.Credential.Tag {
			case 0:
				credentialTag = 0
			case 1:
				credentialTag = 1
			default:
				return nil, fmt.Errorf(
					"invalid non-myopic staking credential tag %d",
					key.Credential.Tag,
				)
			}
			stake = state.credentialStake[nonMyopicCredentialKey(
				credentialTag,
				key.Credential.Bytes.Bytes(),
			)]
		}
		rewardsByPool := make(map[gledger.Blake2b224]uint64, len(state.pools))
		for _, pool := range state.pools {
			reward, err := nonMyopicMemberReward(pool, stake, state)
			if err != nil {
				return nil, err
			}
			rewardsByPool[gledger.NewBlake2b224(pool.key.Bytes())] = reward
		}
		result[key] = rewardsByPool
	}
	return []any{result}, nil
}
