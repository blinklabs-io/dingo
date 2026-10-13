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

package rewards

import (
	"fmt"
	"math/big"
	"sort"
)

// RoundTotals holds the reward round's pot-level scalars: the values Calculate
// derives once from pool-level aggregates alone, before any per-pool member
// split runs. CalculateBaseRewards computes them without reading a single
// delegator row, so a caller can obtain them from a mainnet-scale pool set
// (thousands of rows) without holding the round's whole delegator population
// (millions of rows) in memory at the same time.
type RoundTotals struct {
	UpdatedPots      Pots
	Incentives       uint64
	TotalRewardPot   uint64
	TreasuryTax      uint64
	AvailableRewards uint64
	Efficiency       *big.Rat
	TotalCirculation uint64
	TotalBlocks      uint64
	ExpectedBlocks   *big.Rat
	// Degenerate is true when the round has nothing to split (zero available
	// rewards, zero active stake, or zero circulation): every pool's share is
	// zero, UpdatedPots already carries the whole AvailableRewards back to
	// reserves as Undistributed, and CalculateBaseRewards returns no
	// per-pool results. A caller must not call ApplyPoolMemberRewards for a
	// degenerate round.
	Degenerate    bool
	Undistributed uint64
}

// CalculateBaseRewards computes Pass 1 of Calculate's two-pass reward split:
// each pool's base reward (or, when params.FullPotRewardsEnabled, its
// CIP-0163 full-pot-scaled reward) and the round's pot-level scalars, from
// pool-level snapshot inputs alone. poolSummaries' Delegators and Owners
// fields are not read and may be left nil.
//
// The returned map is keyed by PoolID so a caller can look up one pool's
// PoolReward when it later processes that pool's delegators in its own batch
// via ApplyPoolMemberRewards, without holding every pool's delegators in
// memory for a single Calculate call. Pass 1 needs every pool's base reward at
// once only for the CIP-0163 apportionment (ApportionFullPot); it is
// otherwise a pure per-pool computation, so a caller who does not need to
// change the pool set between Pass 1 and Pass 2 may call this once and reuse
// the result across many Pass-2 batches.
//
// The result is nil when RoundTotals.Degenerate is true: there is nothing for
// Pass 2 to do, and UpdatedPots/Undistributed already reflect the whole round.
func CalculateBaseRewards(
	pots Pots,
	poolSummaries []Pool,
	totalActiveStake uint64,
	excludedActiveStake *uint64,
	params Parameters,
) (RoundTotals, map[PoolID]PoolReward, error) {
	if err := validateParameters(params); err != nil {
		return RoundTotals{}, nil, err
	}
	if err := validatePoolSummaries(
		poolSummaries,
		totalActiveStake,
		excludedActiveStake,
	); err != nil {
		return RoundTotals{}, nil, err
	}
	if pots.Reserves > params.MaxLovelaceSupply {
		return RoundTotals{}, nil, fmt.Errorf(
			"%w: reserves %d exceed max supply %d",
			ErrInvalidParameters,
			pots.Reserves,
			params.MaxLovelaceSupply,
		)
	}

	totalBlocksProduced, err := totalBlocks(poolSummaries)
	if err != nil {
		return RoundTotals{}, nil, err
	}
	expected := expectedBlocks(params)
	if expected.Sign() == 0 &&
		params.Decentralization.Cmp(new(big.Rat).SetFrac64(4, 5)) < 0 {
		return RoundTotals{}, nil, fmt.Errorf(
			"%w: expected blocks is zero",
			ErrInvalidParameters,
		)
	}
	efficiency := Efficiency(totalBlocksProduced, params)
	incentives, err := floorMulChecked(
		minRat(oneRat(), efficiency),
		params.MonetaryExpansion,
		uintRat(pots.Reserves),
	)
	if err != nil {
		return RoundTotals{}, nil, fmt.Errorf("calculate incentives: %w", err)
	}
	totalRewardPot, overflow := addUint64(incentives, pots.Fees)
	if overflow {
		return RoundTotals{}, nil, fmt.Errorf(
			"%w: reward pot overflow",
			ErrInvalidParameters,
		)
	}
	treasuryTax, err := floorMulChecked(
		params.TreasuryExpansion,
		uintRat(totalRewardPot),
	)
	if err != nil {
		return RoundTotals{}, nil, fmt.Errorf("calculate treasury tax: %w", err)
	}
	availableRewards := totalRewardPot - treasuryTax
	treasuryAfterTax, overflow := addUint64(pots.Treasury, treasuryTax)
	if overflow {
		return RoundTotals{}, nil, fmt.Errorf(
			"%w: treasury tax overflow",
			ErrInvalidParameters,
		)
	}
	totalCirculation := params.MaxLovelaceSupply - pots.Reserves

	totals := RoundTotals{
		UpdatedPots: Pots{
			Reserves: pots.Reserves - incentives,
			Treasury: treasuryAfterTax,
			Fees:     0,
		},
		Incentives:       incentives,
		TotalRewardPot:   totalRewardPot,
		TreasuryTax:      treasuryTax,
		AvailableRewards: availableRewards,
		Efficiency:       efficiency,
		TotalCirculation: totalCirculation,
		TotalBlocks:      totalBlocksProduced,
		ExpectedBlocks:   expected,
	}

	if availableRewards == 0 || totalActiveStake == 0 || totalCirculation == 0 {
		totals.Degenerate = true
		totals.Undistributed = availableRewards
		reserves, overflow := addUint64(
			totals.UpdatedPots.Reserves,
			totals.Undistributed,
		)
		if overflow {
			return RoundTotals{}, nil, fmt.Errorf(
				"%w: reserve refund overflow",
				ErrInvalidParameters,
			)
		}
		totals.UpdatedPots.Reserves = reserves
		return totals, nil, nil
	}

	pools := append([]Pool(nil), poolSummaries...)
	sort.Slice(pools, func(i, j int) bool {
		return pools[i].ID.String() < pools[j].ID.String()
	})
	poolRewards := make([]PoolReward, len(pools))
	baseTotals := make([]uint64, len(pools))
	for i, pool := range pools {
		poolReward, err := calculatePoolRewards(
			pool,
			availableRewards,
			totalActiveStake,
			totalCirculation,
			totalBlocksProduced,
			params,
		)
		if err != nil {
			return RoundTotals{}, nil, fmt.Errorf(
				"calculate reward for pool %s: %w",
				pool.ID.String(),
				err,
			)
		}
		poolRewards[i] = poolReward
		baseTotals[i] = poolReward.PoolReward
	}
	if params.FullPotRewardsEnabled {
		scaled := ApportionFullPot(baseTotals, availableRewards)
		for i := range poolRewards {
			if scaled[i] == poolRewards[i].PoolReward {
				continue
			}
			poolRewards[i].PoolReward = scaled[i]
			leader, err := leaderRewardChecked(
				scaled[i],
				pools[i].Cost,
				params.effectiveMargin(pools[i].Margin),
				pools[i].OwnerStake,
				pools[i].DelegatedStake,
			)
			if err != nil {
				return RoundTotals{}, nil, fmt.Errorf(
					"calculate leader reward for pool %s: %w",
					pools[i].ID.String(),
					err,
				)
			}
			poolRewards[i].LeaderReward = leader
		}
	}
	byPool := make(map[PoolID]PoolReward, len(poolRewards))
	for _, pr := range poolRewards {
		byPool[pr.PoolID] = pr
	}
	return totals, byPool, nil
}

// PoolMemberRewardsResult is Pass 2's outcome for a single pool: the leader
// and member AccountRewards its already-finalized PoolReward credits, plus
// the running totals a caller accumulates across pools to reconcile the
// round the way Calculate's finalization tail does (see
// ReconcileRoundTotals).
type PoolMemberRewardsResult struct {
	Rewards           []AccountReward
	MemberRewardTotal uint64
	// Accounted is the total actually credited from this pool's PoolReward
	// (leader plus member, whether spendable or not); PoolReward.PoolReward
	// minus Accounted is this pool's Undistributed remainder.
	Accounted   uint64
	Unspendable uint64
	Effective   uint64
}

// ApplyPoolMemberRewards computes Pass 2 of Calculate's two-pass reward split
// for a single pool: crediting its leader reward and splitting its member
// reward total across its delegators proportional to stake, using the same
// checked arithmetic Calculate applies per pool
// (memberRewardChecked/leaderRewardChecked). poolReward is this pool's
// already-finalized Pass 1 result (from CalculateBaseRewards's returned map),
// so this call needs no visibility into any other pool.
//
// It requires params.aggregateRewards() (ProtocolMajorVersion >= 3): below
// that version, cardano-ledger dedups a reward credential across the WHOLE
// pool set (Calculate's pendingRewards/finalizeRewards -- the earliest-era
// credential can appear as, for example, both a pool's reward account and
// another pool's delegator, and only one of those rewards is paid), which a
// single-pool call cannot see. Calculate itself remains the correct entry
// point for those pre-Allegra epochs.
func ApplyPoolMemberRewards(
	pool Pool,
	poolReward PoolReward,
	params Parameters,
) (PoolMemberRewardsResult, error) {
	var out PoolMemberRewardsResult
	if !params.aggregateRewards() {
		return out, fmt.Errorf(
			"%w: ApplyPoolMemberRewards requires ProtocolMajorVersion >= 3"+
				" (aggregateRewards); pre-Allegra epochs need Calculate's"+
				" whole-snapshot credential dedup",
			ErrInvalidParameters,
		)
	}
	if pool.OwnerStake > pool.DelegatedStake {
		return out, fmt.Errorf(
			"%w: pool %s owner stake %d exceeds delegated stake %d",
			ErrInvalidParameters,
			pool.ID.String(),
			pool.OwnerStake,
			pool.DelegatedStake,
		)
	}
	if err := validatePoolDelegators(pool); err != nil {
		return out, err
	}
	var computedOwnerStake uint64
	for owner := range pool.Owners {
		if owner.Tag != 0 {
			return out, fmt.Errorf(
				"%w: pool %s owner %x has non-key credential tag %d",
				ErrInvalidParameters,
				pool.ID.String(),
				owner.Hash,
				owner.Tag,
			)
		}
		ownerStake, found := poolDelegatorStake(pool.Delegators, owner)
		if !found {
			return out, fmt.Errorf(
				"%w: pool %s owner %x is not a delegator",
				ErrInvalidParameters,
				pool.ID.String(),
				owner.Hash,
			)
		}
		var overflow bool
		computedOwnerStake, overflow = addUint64(computedOwnerStake, ownerStake)
		if overflow {
			return out, fmt.Errorf(
				"%w: pool %s owner stake overflow",
				ErrInvalidParameters,
				pool.ID.String(),
			)
		}
	}
	if computedOwnerStake != pool.OwnerStake {
		return out, fmt.Errorf(
			"%w: pool %s computed owner stake %d does not match owner stake %d",
			ErrInvalidParameters,
			pool.ID.String(),
			computedOwnerStake,
			pool.OwnerStake,
		)
	}

	credit := func(reward AccountReward) error {
		out.Rewards = append(out.Rewards, reward)
		accounted, overflow := addUint64(out.Accounted, reward.Amount)
		if overflow {
			return fmt.Errorf(
				"%w: pool accounted reward overflow",
				ErrInvalidParameters,
			)
		}
		out.Accounted = accounted
		if reward.Type == RewardTypeMember {
			memberTotal, overflow := addUint64(
				out.MemberRewardTotal,
				reward.Amount,
			)
			if overflow {
				return fmt.Errorf(
					"%w: pool member reward overflow",
					ErrInvalidParameters,
				)
			}
			out.MemberRewardTotal = memberTotal
		}
		if reward.Spendable {
			effective, overflow := addUint64(out.Effective, reward.Amount)
			if overflow {
				return fmt.Errorf(
					"%w: effective reward overflow",
					ErrInvalidParameters,
				)
			}
			out.Effective = effective
			return nil
		}
		unspendable, overflow := addUint64(out.Unspendable, reward.Amount)
		if overflow {
			return fmt.Errorf(
				"%w: pool unspendable reward overflow",
				ErrInvalidParameters,
			)
		}
		out.Unspendable = unspendable
		return nil
	}

	if poolReward.LeaderReward > 0 &&
		params.rewardPassesPrefilter(pool.RewardAccountRegistered) {
		if err := credit(AccountReward{
			Credential: pool.RewardAccount,
			PoolID:     pool.ID,
			Amount:     poolReward.LeaderReward,
			Type:       RewardTypeLeader,
			Spendable:  pool.RewardAccountEligible,
		}); err != nil {
			return PoolMemberRewardsResult{}, err
		}
	}

	delegators := append([]Delegator(nil), pool.Delegators...)
	sort.Slice(delegators, func(i, j int) bool {
		return delegators[i].Credential.Key() < delegators[j].Credential.Key()
	})
	for _, delegator := range delegators {
		if _, owner := pool.Owners[delegator.Credential]; owner {
			continue
		}
		amount, err := memberRewardChecked(
			poolReward.PoolReward,
			pool.Cost,
			params.effectiveMargin(pool.Margin),
			delegator.Stake,
			pool.DelegatedStake,
		)
		if err != nil {
			return PoolMemberRewardsResult{}, fmt.Errorf(
				"calculate member reward for pool %s: %w",
				pool.ID.String(),
				err,
			)
		}
		if amount == 0 {
			continue
		}
		if !params.rewardPassesPrefilter(delegator.Registered) {
			continue
		}
		if err := credit(AccountReward{
			Credential: delegator.Credential,
			PoolID:     pool.ID,
			Amount:     amount,
			Type:       RewardTypeMember,
			Spendable:  delegator.Eligible,
		}); err != nil {
			return PoolMemberRewardsResult{}, err
		}
	}
	return out, nil
}

// ReconcileRoundTotals folds every pool's Pass-2 running totals
// (effectiveRewards, unspendableRewards: the sums of PoolMemberRewardsResult.
// Effective and .Unspendable across every pool in the round) into the
// round's final pot state, exactly matching Calculate's finalization tail
// (Result.finalizeRewards, addUndistributedToReserves, addUnspendableToTreasury).
// Call it once, after every pool in a non-degenerate round has been processed
// by ApplyPoolMemberRewards.
func ReconcileRoundTotals(
	totals RoundTotals,
	effectiveRewards, unspendableRewards uint64,
) (Pots, uint64, error) {
	accounted, overflow := addUint64(effectiveRewards, unspendableRewards)
	if overflow || accounted > totals.AvailableRewards {
		return Pots{}, 0, fmt.Errorf(
			"%w: rewards exceed available pot",
			ErrInvalidParameters,
		)
	}
	undistributed := totals.AvailableRewards - accounted
	pots := totals.UpdatedPots
	reserves, overflow := addUint64(pots.Reserves, undistributed)
	if overflow {
		return Pots{}, 0, fmt.Errorf(
			"%w: reserve refund overflow",
			ErrInvalidParameters,
		)
	}
	pots.Reserves = reserves
	treasury, overflow := addUint64(pots.Treasury, unspendableRewards)
	if overflow {
		return Pots{}, 0, fmt.Errorf(
			"%w: unspendable treasury overflow",
			ErrInvalidParameters,
		)
	}
	pots.Treasury = treasury
	return pots, undistributed, nil
}

// validatePoolSummaries applies the subset of validateSnapshot's checks that
// do not require a pool's delegator list: unique pool IDs, reward-account
// credential shape, margin bounds, owner-stake not exceeding delegated
// stake, and the active-stake accounting identity. Owner-stake
// reconciliation and within-pool delegator validation live in
// ApplyPoolMemberRewards, because both need a pool's actual delegator rows.
//
// Cross-pool delegator duplication -- the same stake credential appearing as
// a delegator of two different pools -- is not checked here or in
// ApplyPoolMemberRewards. validateSnapshot catches it because it holds every
// pool's delegator list at once; a chunked caller processes one pool's rows
// per batch and cannot see across batches without an accumulator of its own.
// The condition is structurally excluded upstream instead: reward_stake_input
// rows are captured from reward_live_stake, whose (credential_tag,
// staking_key) primary key and single pool_key_hash column make a credential
// belonging to two pools simultaneously impossible to represent.
func validatePoolSummaries(
	pools []Pool,
	totalActiveStake uint64,
	excludedActiveStake *uint64,
) error {
	seen := make(map[PoolID]struct{}, len(pools))
	var totalDelegated uint64
	for _, pool := range pools {
		if _, ok := seen[pool.ID]; ok {
			return fmt.Errorf(
				"%w: duplicate pool %s in reward snapshot",
				ErrInvalidParameters,
				pool.ID.String(),
			)
		}
		seen[pool.ID] = struct{}{}
		if err := validateCredential(
			pool.RewardAccount,
			fmt.Sprintf("pool %s reward account", pool.ID.String()),
		); err != nil {
			return err
		}
		if pool.Margin != nil {
			if pool.Margin.Sign() < 0 || pool.Margin.Cmp(oneRat()) > 0 {
				return fmt.Errorf(
					"%w: pool %s margin outside [0,1]",
					ErrInvalidParameters,
					pool.ID.String(),
				)
			}
		}
		if pool.OwnerStake > pool.DelegatedStake {
			return fmt.Errorf(
				"%w: pool %s owner stake %d exceeds delegated stake %d",
				ErrInvalidParameters,
				pool.ID.String(),
				pool.OwnerStake,
				pool.DelegatedStake,
			)
		}
		var overflow bool
		totalDelegated, overflow = addUint64(
			totalDelegated,
			pool.DelegatedStake,
		)
		if overflow {
			return fmt.Errorf(
				"%w: total delegated stake overflow",
				ErrInvalidParameters,
			)
		}
	}
	if excludedActiveStake == nil {
		if totalDelegated > totalActiveStake {
			return fmt.Errorf(
				"%w: total delegated stake %d exceeds active stake %d",
				ErrInvalidParameters,
				totalDelegated,
				totalActiveStake,
			)
		}
		return nil
	}
	total, overflow := addUint64(totalDelegated, *excludedActiveStake)
	if overflow {
		return fmt.Errorf(
			"%w: total delegated stake plus excluded active stake overflow",
			ErrInvalidParameters,
		)
	}
	if total != totalActiveStake {
		return fmt.Errorf(
			"%w: total delegated stake %d does not match active stake %d",
			ErrInvalidParameters,
			totalDelegated,
			totalActiveStake,
		)
	}
	return nil
}
