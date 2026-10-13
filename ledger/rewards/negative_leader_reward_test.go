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

// A negative maxPool' is kept by mkPoolRewardInfo: poolR =
// floor(appPerf * maxP) rounds toward negative infinity, and
// calcStakePoolOperatorReward returns poolR itself because poolR <= cost, so
// the whole negative pot is the leader's and no member is paid.
func TestHaskellReferenceNegativeMaxPoolPoolReward(t *testing.T) {
	const (
		poolStake = 60_000_000_000_000
		pledge    = 1_000_000_000_000
	)
	tests := []struct {
		name             string
		leverage         *big.Rat
		blocks           uint64
		totalBlocks      uint64
		totalActiveStake uint64
		wantDeficit      uint64
	}{
		{
			// maxP = floor(-3750000000/3757) = -998137, appPerf = 1.
			name:     "zero leverage",
			leverage: big.NewRat(0, 1),
			blocks:   1, totalBlocks: 1, totalActiveStake: poolStake,
			wantDeficit: 998_137,
		},
		{
			// maxP = -2038206 with R = 26.44M ADA; appPerf = 10801/10800
			// gives -2038394.72, which floors to -2038395 (truncation would
			// give -2038394).
			name:     "small leverage floors toward negative infinity",
			leverage: big.NewRat(1, 1000),
			blocks:   10_801, totalBlocks: 21_600, totalActiveStake: 2 * poolStake,
			wantDeficit: 2_038_395,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			available := uint64(10_000_000_000_000)
			if tc.blocks != 1 {
				available = 26_440_000_000_000
			}
			got, err := CalculatePoolReward(
				Pool{
					ID:             testPoolID(1),
					Margin:         big.NewRat(1, 100),
					Pledge:         pledge,
					Cost:           170_000_000,
					DelegatedStake: poolStake,
					OwnerStake:     pledge,
					BlocksProduced: tc.blocks,
					TotalBlocks:    tc.totalBlocks,
				},
				available,
				tc.totalActiveStake,
				34_000_000_000_000_000,
				tc.totalBlocks,
				Parameters{
					Decentralization:      big.NewRat(0, 1),
					PledgeInfluence:       big.NewRat(3, 10),
					OptimalPoolCount:      500,
					PledgeLeverageEnabled: true,
					PledgeLeverage:        tc.leverage,
				},
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantDeficit, got.LeaderRewardDeficit)
			require.Zero(t, got.OptimalReward)
			require.Zero(t, got.PoolReward)
			require.Zero(t, got.LeaderReward)
		})
	}
}

// negativeLeaderRewardSnapshot is a mainnet-scale epoch with L = 1/1000:
// pool X (1M ADA pledge) has maxP = -2038206 and leader reward -2038395,
// pool Y (1000 ADA pledge) has maxP = 595 and leader reward 594. Both hold
// 60M ADA of the 120M ADA active stake; X made 10801 and Y 10799 of the
// 21600 expected blocks.
func negativeLeaderRewardSnapshot(xRegistered bool) (Pots, Snapshot, Parameters) {
	const stake = 60_000_000_000_000
	pool := func(
		id byte,
		pledge uint64,
		blocks uint64,
		account Credential,
		member Credential,
		accountRegistered bool,
	) Pool {
		return Pool{
			ID:                      testPoolID(id),
			RewardAccount:           account,
			Margin:                  big.NewRat(1, 100),
			Pledge:                  pledge,
			Cost:                    170_000_000,
			DelegatedStake:          stake,
			OwnerStake:              pledge,
			BlocksProduced:          blocks,
			TotalBlocks:             21_600,
			RewardAccountRegistered: accountRegistered,
			RewardAccountEligible:   accountRegistered,
			Owners:                  map[Credential]struct{}{account: {}},
			Delegators: []Delegator{
				{
					Credential: account,
					Stake:      pledge,
					Registered: accountRegistered,
					Eligible:   accountRegistered,
				},
				{
					Credential: member,
					Stake:      stake - pledge,
					Registered: true,
					Eligible:   true,
				},
			},
		}
	}
	return Pots{
			Reserves: 11_000_000_000_000_000,
			Treasury: 1_500_000_000_000_000,
			Fees:     50_000_000_000,
		},
		Snapshot{
			TotalActiveStake: 2 * stake,
			Pools: []Pool{
				pool(1, 1_000_000_000_000, 10_801,
					testCredential(0, 0xa1), testCredential(0, 0xb1), xRegistered),
				pool(2, 1_000_000_000, 10_799,
					testCredential(0, 0xa2), testCredential(0, 0xb2), true),
			},
		},
		Parameters{
			MonetaryExpansion:     big.NewRat(3, 1000),
			TreasuryExpansion:     big.NewRat(1, 5),
			Decentralization:      big.NewRat(0, 1),
			PledgeInfluence:       big.NewRat(3, 10),
			ActiveSlotsCoeff:      big.NewRat(1, 20),
			OptimalPoolCount:      500,
			EpochLength:           432_000,
			MaxLovelaceSupply:     45_000_000_000_000_000,
			ProtocolMajorVersion:  12,
			PledgeLeverageEnabled: true,
			PledgeLeverage:        big.NewRat(1, 1000),
		}
}

// An unregistered reward account's negative leader reward reaches the pots
// the way applyRUpdFiltered moves it: completeRupd's deltaR2 = R - sum(rs)
// counts it, so reserves gain its magnitude, and frTotalUnregistered adds it
// to the treasury, so the treasury loses the same amount.
//
// R = 26440000000000, rewards = {X leader -2038395, Y leader 594},
// deltaR2 = 26440002037801, reserves = 11e15 - 33e12 + deltaR2,
// treasury = 1.5e15 + 6.61e12 - 2038395.
func TestHaskellReferenceNegativeLeaderRewardUnregisteredAccount(t *testing.T) {
	pots, snapshot, params := negativeLeaderRewardSnapshot(false)
	result, err := Calculate(pots, snapshot, params)
	require.NoError(t, err)
	require.NoError(t, result.NegativeLeaderRewardError())

	require.Equal(t, uint64(33_000_000_000_000), result.Incentives)
	require.Equal(t, uint64(6_610_000_000_000), result.TreasuryTax)
	require.Equal(t, uint64(26_440_000_000_000), result.AvailableRewards)
	require.Equal(t, uint64(594), result.EffectiveRewards)
	require.Zero(t, result.Unspendable)
	require.Equal(t, uint64(2_038_395), result.UnspendableDeficit)
	require.Equal(t, uint64(26_440_002_037_801), result.Undistributed)
	require.Equal(t, uint64(10_993_440_002_037_801), result.UpdatedPots.Reserves)
	require.Equal(t, uint64(1_506_609_997_961_605), result.UpdatedPots.Treasury)
	require.Equal(t, []NegativeLeaderReward{{
		PoolID:     testPoolID(1),
		Credential: testCredential(0, 0xa1),
		Amount:     2_038_395,
		Spendable:  false,
	}}, result.NegativeLeaderRewards)
	require.Equal(t, []AccountReward{{
		Credential: testCredential(0, 0xa2),
		PoolID:     testPoolID(2),
		Amount:     594,
		Type:       RewardTypeLeader,
		Spendable:  true,
	}}, result.AccountRewards)
	require.Equal(t, uint64(2_038_395), result.PoolRewards[0].LeaderRewardDeficit)
	require.Zero(t, result.PoolRewards[0].LeaderReward)
	require.Equal(t, uint64(594), result.PoolRewards[1].LeaderReward)
}

// A registered reward account cannot hold a negative reward: the reference's
// aggregateCompactRewards applies compactCoinOrError to every reward of a
// registered account, which fails. The result names the reward instead of
// applying it.
func TestHaskellReferenceNegativeLeaderRewardRegisteredAccount(t *testing.T) {
	pots, snapshot, params := negativeLeaderRewardSnapshot(true)
	result, err := Calculate(pots, snapshot, params)
	require.NoError(t, err)
	require.Zero(t, result.UnspendableDeficit)
	require.Equal(t, []NegativeLeaderReward{{
		PoolID:     testPoolID(1),
		Credential: testCredential(0, 0xa1),
		Amount:     2_038_395,
		Spendable:  true,
	}}, result.NegativeLeaderRewards)
	applyErr := result.NegativeLeaderRewardError()
	require.ErrorIs(t, applyErr, ErrNegativeLeaderReward)
	require.ErrorContains(t, applyErr, testPoolID(1).String())
	require.ErrorContains(t, applyErr, "-2038395")
}

// The reference would leave a negative treasury, which Dingo cannot hold.
func TestNegativeLeaderRewardBeyondTreasuryIsRefused(t *testing.T) {
	pots, snapshot, params := negativeLeaderRewardSnapshot(false)
	pots.Treasury = 0
	params.TreasuryExpansion = big.NewRat(0, 1)
	_, err := Calculate(pots, snapshot, params)
	require.ErrorIs(t, err, ErrNegativeLeaderReward)
}

// CIP-0163 apportions the pot across pools that earned a base reward. A pool
// whose leader reward is negative earned none, so it keeps its negative
// outcome and the rest of the pot goes to the other pools.
func TestFullPotLeavesNegativeLeaderRewardPoolOut(t *testing.T) {
	pots, snapshot, params := negativeLeaderRewardSnapshot(false)
	params.FullPotRewardsEnabled = true
	result, err := Calculate(pots, snapshot, params)
	require.NoError(t, err)
	require.Equal(t, uint64(2_038_395), result.PoolRewards[0].LeaderRewardDeficit)
	require.Zero(t, result.PoolRewards[0].PoolReward)
	require.Equal(t, result.AvailableRewards, result.PoolRewards[1].PoolReward)
	require.Equal(t, uint64(2_038_395), result.UnspendableDeficit)
}
