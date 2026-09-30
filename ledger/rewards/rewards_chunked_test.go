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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// chunkedCredential deterministically derives a unique 28-byte credential
// hash from an index, so a generated snapshot can hold thousands of distinct
// credentials without collisions.
func chunkedCredential(tag uint8, index uint64) Credential {
	var hash [CredentialHashSize]byte
	for i := range 8 {
		hash[CredentialHashSize-1-i] = byte(index >> (8 * i))
	}
	return Credential{Tag: tag, Hash: hash}
}

func chunkedPoolID(index uint64) PoolID {
	var id PoolID
	for i := range 8 {
		id[len(id)-1-i] = byte(index >> (8 * i))
	}
	return id
}

// genChunkedPool builds one pool with numDelegators random delegators (the
// first is also the sole owner, so the leader/owner-exclusion paths in
// ApplyPoolMemberRewards are always exercised). Every delegator is registered
// and eligible; genChunkedPool's caller may flip individual flags for
// prefilter/spendability coverage.
func genChunkedPool(
	rng *rand.Rand,
	poolIndex uint64,
	numDelegators int,
) Pool {
	poolID := chunkedPoolID(poolIndex)
	rewardAccount := chunkedCredential(0, poolIndex<<32)
	owner := chunkedCredential(0, poolIndex<<32|1)

	delegators := make([]Delegator, 0, numDelegators)
	var delegatedStake uint64
	ownerStake := uint64(1_000 + rng.Int63n(9_000))
	delegators = append(delegators, Delegator{
		Credential: owner,
		Stake:      ownerStake,
		Registered: true,
		Eligible:   true,
	})
	delegatedStake += ownerStake
	for i := 1; i < numDelegators; i++ {
		stake := uint64(rng.Int63n(1_000_000) + 1)
		delegators = append(delegators, Delegator{
			Credential: chunkedCredential(0, poolIndex<<32|uint64(i+1)),
			Stake:      stake,
			Registered: rng.Intn(10) != 0, // ~90% registered
			Eligible:   rng.Intn(20) != 0, // ~95% eligible/spendable
		})
		delegatedStake += stake
	}
	margin := big.NewRat(int64(rng.Intn(101)), 100)
	pledge := ownerStake / 2
	if pledge == 0 {
		pledge = 1
	}
	return Pool{
		ID:                      poolID,
		RewardAccount:           rewardAccount,
		Margin:                  margin,
		Pledge:                  pledge,
		Cost:                    uint64(100 + rng.Int63n(1_000)),
		DelegatedStake:          delegatedStake,
		OwnerStake:              ownerStake,
		BlocksProduced:          uint64(rng.Intn(50)),
		TotalBlocks:             500,
		RewardAccountRegistered: true,
		RewardAccountEligible:   rng.Intn(10) != 0,
		Owners: map[Credential]struct{}{
			owner: {},
		},
		Delegators: delegators,
	}
}

func genChunkedSnapshot(
	rng *rand.Rand,
	numPools, delegatorsPerPool int,
) (Snapshot, []Pool) {
	pools := make([]Pool, numPools)
	var totalActive uint64
	for i := range pools {
		pools[i] = genChunkedPool(rng, uint64(i), delegatorsPerPool)
		totalActive += pools[i].DelegatedStake
	}
	return Snapshot{
		Pools:            pools,
		TotalActiveStake: totalActive,
	}, pools
}

// poolSummaryOf strips Delegators/Owners, matching what a caller reading only
// reward_pool_input rows (no reward_stake_input scan) would have for Pass 1.
func poolSummaryOf(pool Pool) Pool {
	summary := pool
	summary.Delegators = nil
	summary.Owners = nil
	return summary
}

type rewardKey struct {
	credential string
	pool       PoolID
	rewardType RewardType
}

func rewardsToMap(rewards []AccountReward) map[rewardKey]uint64 {
	out := make(map[rewardKey]uint64, len(rewards))
	for _, r := range rewards {
		out[rewardKey{r.Credential.Key(), r.PoolID, r.Type}] = r.Amount
	}
	return out
}

func poolRewardsToMap(rewards []PoolReward) map[PoolID]PoolReward {
	out := make(map[PoolID]PoolReward, len(rewards))
	for _, r := range rewards {
		out[r.PoolID] = r
	}
	return out
}

// runChunked drives CalculateBaseRewards + ApplyPoolMemberRewards over pools
// in fixed-size batches (batchSize == 0 means "all pools in one batch"),
// reconstructing a Result-shaped view for comparison against Calculate.
func runChunked(
	t *testing.T,
	pots Pots,
	pools []Pool,
	totalActiveStake uint64,
	params Parameters,
	batchSize int,
) (Pots, map[rewardKey]uint64, map[PoolID]PoolReward, bool) {
	t.Helper()
	summaries := make([]Pool, len(pools))
	for i, p := range pools {
		summaries[i] = poolSummaryOf(p)
	}
	totals, byPool, err := CalculateBaseRewards(
		pots, summaries, totalActiveStake, nil, params,
	)
	require.NoError(t, err)
	if totals.Degenerate {
		return totals.UpdatedPots, nil, nil, true
	}

	size := batchSize
	if size <= 0 {
		size = len(pools)
	}
	allRewards := make(map[rewardKey]uint64)
	poolResults := make(map[PoolID]PoolReward, len(pools))
	var effective, unspendable uint64
	for start := 0; start < len(pools); start += size {
		end := min(start+size, len(pools))
		for _, pool := range pools[start:end] {
			pr, ok := byPool[pool.ID]
			require.True(t, ok, "missing pass-1 result for pool %x", pool.ID)
			res, err := ApplyPoolMemberRewards(pool, pr, params)
			require.NoError(t, err)
			for _, reward := range res.Rewards {
				allRewards[rewardKey{
					reward.Credential.Key(), reward.PoolID, reward.Type,
				}] = reward.Amount
			}
			pr.MemberRewardTotal = res.MemberRewardTotal
			pr.Unspendable = res.Unspendable
			if pr.PoolReward < res.Accounted {
				t.Fatalf(
					"pool %x accounted %d exceeds pool reward %d",
					pool.ID, res.Accounted, pr.PoolReward,
				)
			}
			pr.Undistributed = pr.PoolReward - res.Accounted
			poolResults[pool.ID] = pr
			effective += res.Effective
			unspendable += res.Unspendable
		}
	}
	finalPots, _, err := ReconcileRoundTotals(totals, effective, unspendable)
	require.NoError(t, err)
	return finalPots, allRewards, poolResults, false
}

// TestChunkedRewardSplitMatchesCalculate is the exactness proof required
// before any epoch-boundary reward path may run Pass 1/Pass 2 in separate
// batches instead of one Calculate call: for every generated snapshot and
// every pool-batch grouping, the chunked path must reproduce Calculate's
// pots, every pool output and every account output exactly -- not
// approximately, to the lovelace.
func TestChunkedRewardSplitMatchesCalculate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name              string
		numPools          int
		delegatorsPerPool int
		fullPot           bool
		pledgeLeverage    bool
	}{
		{"single_pool_few_delegators", 1, 5, false, false},
		{"few_pools_many_delegators", 7, 200, false, false},
		{"many_small_pools", 60, 3, false, false},
		{"full_pot_enabled", 15, 40, true, false},
		{"pledge_leverage_enabled", 15, 40, false, true},
		{"full_pot_and_pledge_leverage", 25, 25, true, true},
		{"zero_stake_pools_mixed_in", 20, 0, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(int64(len(tc.name)) + 1))
			snapshot, pools := genChunkedSnapshot(
				rng, tc.numPools, tc.delegatorsPerPool,
			)
			if tc.delegatorsPerPool == 0 {
				// Every pool has one owner-delegator from genChunkedPool;
				// zero-delegator coverage means BlocksProduced == 0 for a
				// subset of pools instead (calculatePoolRewards's
				// zero-block-count early return), which is the actual
				// zero-contribution case reachable from stored data.
				for i := range pools {
					if i%2 == 0 {
						pools[i].BlocksProduced = 0
					}
				}
			}
			params := Parameters{
				MonetaryExpansion:     big.NewRat(3, 1000),
				TreasuryExpansion:     big.NewRat(1, 5),
				Decentralization:      big.NewRat(1, 2),
				PledgeInfluence:       big.NewRat(3, 10),
				ActiveSlotsCoeff:      big.NewRat(1, 20),
				OptimalPoolCount:      uint64(max(1, tc.numPools/2)),
				EpochLength:           432_000,
				MaxLovelaceSupply:     45_000_000_000_000_000,
				ProtocolMajorVersion:  9,
				FullPotRewardsEnabled: tc.fullPot,
			}
			if tc.pledgeLeverage {
				params.PledgeLeverageEnabled = true
				params.PledgeLeverage = big.NewRat(3, 1)
			}
			pots := Pots{
				Reserves: 13_000_000_000_000_000,
				Treasury: 500_000_000_000,
				Fees:     2_000_000_000,
			}

			reference, err := Calculate(pots, snapshot, params)
			require.NoError(t, err)
			refRewards := rewardsToMap(reference.AccountRewards)
			refPoolRewards := poolRewardsToMap(reference.PoolRewards)

			for _, batchSize := range []int{0, 1, 3, len(pools)} {
				t.Run(
					fmt.Sprintf("batch_%d", batchSize),
					func(t *testing.T) {
						t.Parallel()
						pots, rewards, poolResults, degenerate := runChunked(
							t,
							pots,
							pools,
							snapshot.TotalActiveStake,
							params,
							batchSize,
						)
						require.Equal(
							t,
							reference.AvailableRewards == 0 ||
								snapshot.TotalActiveStake == 0,
							degenerate,
						)
						require.Equal(t, reference.UpdatedPots, pots)
						if degenerate {
							return
						}
						require.Equal(t, refRewards, rewards)
						require.Equal(
							t,
							len(refPoolRewards),
							len(poolResults),
						)
						for id, want := range refPoolRewards {
							got, ok := poolResults[id]
							require.True(
								t, ok, "missing pool result for %x", id,
							)
							require.Equal(t, want, got)
						}
					},
				)
			}
		})
	}
}

// TestChunkedRewardSplitExtremeValues exercises the extremes the accounting
// bar names explicitly: the maximum lovelace supply, a margin of exactly 0
// and exactly 1, and a stake of 1 lovelace competing against a
// billion-lovelace delegator in the same pool.
func TestChunkedRewardSplitExtremeValues(t *testing.T) {
	t.Parallel()
	const maxSupply = 45_000_000_000_000_000
	owner := chunkedCredential(0, 1)
	tiny := chunkedCredential(0, 2)
	whale := chunkedCredential(0, 3)
	rewardAccount := chunkedCredential(0, 4)
	poolID := chunkedPoolID(1)

	for _, margin := range []*big.Rat{big.NewRat(0, 1), big.NewRat(1, 1)} {
		t.Run(fmt.Sprintf("margin_%s", margin.RatString()), func(t *testing.T) {
			t.Parallel()
			pool := Pool{
				ID:                      poolID,
				RewardAccount:           rewardAccount,
				Margin:                  margin,
				Pledge:                  1,
				Cost:                    0,
				DelegatedStake:          1_000_000_002,
				OwnerStake:              1,
				BlocksProduced:          21_600,
				TotalBlocks:             21_600,
				RewardAccountRegistered: true,
				RewardAccountEligible:   true,
				Owners: map[Credential]struct{}{
					owner: {},
				},
				Delegators: []Delegator{
					{
						Credential: owner,
						Stake:      1,
						Registered: true,
						Eligible:   true,
					},
					{
						Credential: tiny,
						Stake:      1,
						Registered: true,
						Eligible:   true,
					},
					{
						Credential: whale,
						Stake:      1_000_000_000,
						Registered: true,
						Eligible:   true,
					},
				},
			}
			snapshot := Snapshot{
				Pools:            []Pool{pool},
				TotalActiveStake: pool.DelegatedStake,
			}
			params := Parameters{
				MonetaryExpansion:    big.NewRat(3, 1000),
				TreasuryExpansion:    big.NewRat(1, 5),
				Decentralization:     big.NewRat(0, 1),
				PledgeInfluence:      big.NewRat(3, 10),
				ActiveSlotsCoeff:     big.NewRat(1, 20),
				OptimalPoolCount:     100,
				EpochLength:          432_000,
				MaxLovelaceSupply:    maxSupply,
				ProtocolMajorVersion: 9,
			}
			pots := Pots{Reserves: maxSupply - 1_000_000_000, Fees: 1}

			reference, err := Calculate(pots, snapshot, params)
			require.NoError(t, err)

			for _, batchSize := range []int{0, 1} {
				resultPots, rewards, poolResults, degenerate := runChunked(
					t, pots, []Pool{pool}, snapshot.TotalActiveStake,
					params, batchSize,
				)
				require.False(t, degenerate)
				require.Equal(t, reference.UpdatedPots, resultPots)
				require.Equal(
					t, rewardsToMap(reference.AccountRewards), rewards,
				)
				require.Equal(
					t,
					poolRewardsToMap(reference.PoolRewards)[poolID],
					poolResults[poolID],
				)
			}
		})
	}
}

// TestApplyPoolMemberRewardsRejectsPreAggregateEra proves the guard names the
// exact reason ApplyPoolMemberRewards refuses ProtocolMajorVersion < 3,
// rather than silently reproducing a wrong (non-deduped) split.
func TestApplyPoolMemberRewardsRejectsPreAggregateEra(t *testing.T) {
	t.Parallel()
	pool := Pool{
		ID:                      chunkedPoolID(1),
		RewardAccount:           chunkedCredential(0, 1),
		Margin:                  big.NewRat(1, 10),
		Cost:                    100,
		DelegatedStake:          1000,
		BlocksProduced:          1,
		TotalBlocks:             1,
		RewardAccountRegistered: true,
		RewardAccountEligible:   true,
	}
	poolReward := PoolReward{
		PoolID:       pool.ID,
		PoolReward:   1000,
		LeaderReward: 1000,
	}
	_, err := ApplyPoolMemberRewards(
		pool, poolReward, Parameters{ProtocolMajorVersion: 2},
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "aggregateRewards")
}
