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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// denominatorSnapshot returns the fixture of
// TestCalculateMatchesShelleyPoolRewardFormula with a caller-chosen
// TotalActiveStake, so the only variable across the cases below is the sigma_a
// denominator.
func denominatorSnapshot(totalActiveStake uint64) Snapshot {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	return Snapshot{
		TotalActiveStake: totalActiveStake,
		Pools: []Pool{
			{
				ID:                      testPoolID(1),
				RewardAccount:           testCredential(0, 4),
				Margin:                  big.NewRat(1, 10),
				Pledge:                  500,
				Cost:                    1_000,
				DelegatedStake:          1_000,
				OwnerStake:              500,
				BlocksProduced:          10,
				TotalBlocks:             10,
				RewardAccountRegistered: true,
				RewardAccountEligible:   true,
				Owners: map[Credential]struct{}{
					owner: {},
				},
				Delegators: []Delegator{
					{
						Credential: owner,
						Stake:      500,
						Registered: true,
						Eligible:   true,
					},
					{
						Credential: member,
						Stake:      500,
						Registered: true,
						Eligible:   true,
					},
				},
			},
		},
	}
}

func denominatorParameters() Parameters {
	return Parameters{
		MonetaryExpansion: big.NewRat(1, 100),
		TreasuryExpansion: big.NewRat(0, 1),
		Decentralization:  big.NewRat(0, 1),
		PledgeInfluence:   big.NewRat(1, 2),
		ActiveSlotsCoeff:  big.NewRat(1, 10),
		OptimalPoolCount:  10,
		EpochLength:       100,
		MaxLovelaceSupply: 100_010_000,
	}
}

// TestCalculateAcceptsActiveStakeAboveThePoolSet covers the reward-input shape
// produced when snapshot capture excludes a pool for degraded registration
// data: the pool contributes no Pool entry, but its delegators' stake is still
// part of the epoch's active stake.
//
// cardano-ledger builds ssTotalActiveStake from every registered credential
// that carries a delegation and never intersects it with ssStakePoolsSnapShot
// (Cardano.Ledger.State.SnapShots.mkSnapShot over resolveInstantStake), so
// sum(Pools) < TotalActiveStake is a representable state, not a contradiction.
// Rejecting it forced the caller to shrink the denominator to match the pool
// set, which is what under-credits every surviving pool.
func TestCalculateAcceptsActiveStakeAboveThePoolSet(t *testing.T) {
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		denominatorSnapshot(1_250),
		denominatorParameters(),
	)
	require.NoError(t, err)
	require.Len(t, result.PoolRewards, 1)

	// sigma_a = 1000/1250 = 4/5, beta = 10/10 = 1, so the apparent performance
	// is beta/sigma_a = 5/4 and the pool reward is floor(5/4 * 83333).
	require.Zero(
		t,
		big.NewRat(5, 4).Cmp(result.PoolRewards[0].ApparentPerformance),
	)
	require.Equal(t, uint64(83_333), result.PoolRewards[0].OptimalReward)
	require.Equal(t, uint64(104_166), result.PoolRewards[0].PoolReward)
	require.Equal(t, uint64(57_741), result.PoolRewards[0].LeaderReward)
	require.Equal(t, uint64(46_424), result.PoolRewards[0].MemberRewardTotal)
}

// TestShrunkActiveStakeUnderCreditsEveryReward quantifies the divergence the
// bound above exists to prevent. Both cases describe the same epoch: one pool
// holding 1000 stake and one degraded pool holding 250. Collapsing the
// denominator onto the surviving pool set raises sigma_a from 4/5 to 1, drops
// the apparent performance from 5/4 to 1, and costs the single member 9375 of
// its 46424 lovelace — a shortfall in proportion to the excluded pool's share
// of active stake, repeated every epoch the exclusion holds.
func TestShrunkActiveStakeUnderCreditsEveryReward(t *testing.T) {
	params := denominatorParameters()

	shrunk, err := Calculate(
		Pots{Reserves: 100_000_000},
		denominatorSnapshot(1_000),
		params,
	)
	require.NoError(t, err)

	full, err := Calculate(
		Pots{Reserves: 100_000_000},
		denominatorSnapshot(1_250),
		params,
	)
	require.NoError(t, err)

	require.Len(t, shrunk.AccountRewards, 2)
	require.Len(t, full.AccountRewards, 2)

	require.Equal(t, RewardTypeLeader, shrunk.AccountRewards[0].Type)
	require.Equal(t, uint64(46_283), shrunk.AccountRewards[0].Amount)
	require.Equal(t, uint64(57_741), full.AccountRewards[0].Amount)

	require.Equal(t, RewardTypeMember, shrunk.AccountRewards[1].Type)
	require.Equal(
		t,
		shrunk.AccountRewards[1].Credential,
		full.AccountRewards[1].Credential,
	)
	require.Equal(t, uint64(37_049), shrunk.AccountRewards[1].Amount)
	require.Equal(t, uint64(46_424), full.AccountRewards[1].Amount)
}

// TestValidateSnapshotTrackedExcludedActiveStake covers dingo #4025:
// TestCalculateAcceptsActiveStakeAboveThePoolSet's non-exceeding bound
// (sum(Pools) <= TotalActiveStake) tolerates one legitimately excluded pool's
// stake going missing, but tolerates just as well a Pools set proportionally
// shrunk by some unrelated bug -- pool count, delegator count, and per-pool
// cross-sums all stay internally consistent, so nothing else catches it. A
// caller that tracks ExcludedActiveStake (even as zero) gets an exact check
// instead: Pools must sum to precisely TotalActiveStake minus the tracked
// exclusion.
func TestValidateSnapshotTrackedExcludedActiveStake(t *testing.T) {
	// 1000 (Pools) + 250 (tracked excluded) == 1250 (declared total): exact
	// match passes, matching the legitimately-excluded-pool case.
	exact := denominatorSnapshot(1_250)
	tracked := uint64(250)
	exact.ExcludedActiveStake = &tracked
	require.NoError(t, validateSnapshot(exact))

	// The same 250 tracked as excluded, but Pools is proportionally shrunk to
	// 900 instead of 1000 (as if every pool's stake had been scaled down).
	// 900+250=1150 != 1250, so this must be rejected even though 900 <= 1250
	// would have passed the old non-exceeding bound silently.
	shrunk := denominatorSnapshot(1_250)
	shrunk.Pools[0].DelegatedStake = 900
	shrunk.Pools[0].OwnerStake = 450
	shrunk.Pools[0].Delegators[0].Stake = 450
	shrunk.Pools[0].Delegators[1].Stake = 450
	shrunkExcluded := uint64(250)
	shrunk.ExcludedActiveStake = &shrunkExcluded
	err := validateSnapshot(shrunk)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "does not match active stake")

	// A tracked exclusion of exactly zero still demands an exact match: no
	// slack remains once the caller affirmatively says nothing was excluded.
	noExclusion := denominatorSnapshot(1_250)
	zero := uint64(0)
	noExclusion.ExcludedActiveStake = &zero
	err = validateSnapshot(noExclusion)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "does not match active stake")
}
