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
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalculateMatchesShelleyPoolRewardFormula(t *testing.T) {
	poolID := testPoolID(1)
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	rewardAccount := testCredential(0, 4)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           rewardAccount,
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
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
						{Credential: member, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		Parameters{
			MonetaryExpansion: big.NewRat(1, 100),
			TreasuryExpansion: big.NewRat(0, 1),
			Decentralization:  big.NewRat(0, 1),
			PledgeInfluence:   big.NewRat(1, 2),
			ActiveSlotsCoeff:  big.NewRat(1, 10),
			OptimalPoolCount:  10,
			EpochLength:       100,
			MaxLovelaceSupply: 100_010_000,
		},
	)
	require.NoError(t, err)

	require.Equal(t, uint64(1_000_000), result.Incentives)
	require.Equal(t, uint64(1_000_000), result.AvailableRewards)
	require.Equal(t, uint64(83333), result.PoolRewards[0].OptimalReward)
	require.Equal(t, uint64(83333), result.PoolRewards[0].PoolReward)
	require.Equal(t, uint64(46283), result.PoolRewards[0].LeaderReward)
	require.Equal(t, uint64(37049), result.PoolRewards[0].MemberRewardTotal)
	require.Equal(t, uint64(1), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(916_668), result.Undistributed)
	require.Equal(t, uint64(99_916_668), result.UpdatedPots.Reserves)

	require.Len(t, result.AccountRewards, 2)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, rewardAccount, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(46283), result.AccountRewards[0].Amount)
	require.Equal(t, RewardTypeMember, result.AccountRewards[1].Type)
	require.Equal(t, member, result.AccountRewards[1].Credential)
	require.Equal(t, uint64(37049), result.AccountRewards[1].Amount)
}

func TestCalculateScriptCredentialWithOwnerHashStillEarnsMemberReward(t *testing.T) {
	poolID := testPoolID(1)
	ownerKey := testCredential(0, 2)
	scriptWithOwnerHash := testCredential(1, 2)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(0, 1),
					Pledge:                  0,
					Cost:                    0,
					DelegatedStake:          1_000,
					OwnerStake:              0,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerKey: {},
					},
					Delegators: []Delegator{
						{Credential: ownerKey, Stake: 0, Registered: true, Eligible: true},
						{
							Credential: scriptWithOwnerHash,
							Stake:      1_000,
							Registered: true,
							Eligible:   true,
						},
					},
				},
			},
		},
		testParams(),
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 1)
	require.Equal(t, RewardTypeMember, result.AccountRewards[0].Type)
	require.Equal(t, scriptWithOwnerHash, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(66_666), result.AccountRewards[0].Amount)
	require.Equal(t, uint64(66_666), result.PoolRewards[0].MemberRewardTotal)
	require.Equal(t, uint64(0), result.PoolRewards[0].LeaderReward)
}

func TestCalculatePledgeFailureZerosPoolReward(t *testing.T) {
	poolID := testPoolID(1)
	owner := testCredential(0, 2)

	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  501,
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
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		testParams(),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(0), result.PoolRewards[0].PoolReward)
	require.Empty(t, result.AccountRewards)
	require.Equal(t, uint64(1_000_000), result.Undistributed)
	require.Equal(t, uint64(100_000_000), result.UpdatedPots.Reserves)
}

func TestCalculatePoolRewardAtOrBelowCostPaysOnlyLeader(t *testing.T) {
	poolID := testPoolID(1)
	leader := testCredential(0, 2)
	member := testCredential(0, 3)

	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           leader,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    100_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						leader: {},
					},
					Delegators: []Delegator{
						{Credential: leader, Stake: 500, Registered: true, Eligible: true},
						{Credential: member, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		testParams(),
	)
	require.NoError(t, err)

	require.Equal(t, uint64(83_333), result.PoolRewards[0].PoolReward)
	require.Equal(t, uint64(83_333), result.PoolRewards[0].LeaderReward)
	require.Equal(t, uint64(0), result.PoolRewards[0].MemberRewardTotal)
	require.Equal(t, uint64(0), result.PoolRewards[0].Undistributed)
	require.Len(t, result.AccountRewards, 1)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, leader, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(83_333), result.AccountRewards[0].Amount)
	require.Equal(t, uint64(83_333), result.EffectiveRewards)
	require.Equal(t, uint64(916_667), result.Undistributed)
	require.Equal(t, uint64(99_916_667), result.UpdatedPots.Reserves)
}

func TestCalculateNoActiveStakeReturnsAvailableRewardsToReserves(t *testing.T) {
	params := testParams()
	params.TreasuryExpansion = big.NewRat(1, 5)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: 10,
			Fees:     2_000,
		},
		Snapshot{},
		params,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(2_000), result.TotalRewardPot)
	require.Equal(t, uint64(400), result.TreasuryTax)
	require.Equal(t, uint64(1_600), result.AvailableRewards)
	require.Equal(t, uint64(1_600), result.Undistributed)
	require.Equal(t, uint64(100_001_600), result.UpdatedPots.Reserves)
	require.Equal(t, uint64(410), result.UpdatedPots.Treasury)
	require.Empty(t, result.PoolRewards)
	require.Empty(t, result.AccountRewards)
}

func TestCalculateTreasuryTaxUsesIncentivesAndFees(t *testing.T) {
	poolID := testPoolID(1)
	owner := testCredential(0, 2)
	params := testParams()
	params.TreasuryExpansion = big.NewRat(1, 5)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: 10,
			Fees:     2_000,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(0, 1),
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
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(1_000_000), result.Incentives)
	require.Equal(t, uint64(1_002_000), result.TotalRewardPot)
	require.Equal(t, uint64(200_400), result.TreasuryTax)
	require.Equal(t, uint64(801_600), result.AvailableRewards)
	require.Equal(t, uint64(200_410), result.UpdatedPots.Treasury)
}

func TestCalculateNetworkEfficiencyIncludesDecentralization(t *testing.T) {
	poolID := testPoolID(1)
	owner := testCredential(0, 2)
	params := testParams()
	params.Decentralization = big.NewRat(1, 2)

	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             5,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Equal(t, big.NewRat(5, 1), result.ExpectedBlocks)
	require.Equal(t, big.NewRat(1, 1), result.Efficiency)
	require.Equal(t, uint64(1_000_000), result.Incentives)
}

func TestCalculateNetworkEfficiencyHonorsDecentralizationThreshold(t *testing.T) {
	poolID := testPoolID(1)
	owner := testCredential(0, 2)
	params := testParams()
	params.Decentralization = big.NewRat(4, 5)
	snapshot := Snapshot{
		TotalActiveStake: 1_000,
		Pools: []Pool{
			{
				ID:                      poolID,
				RewardAccount:           testCredential(0, 4),
				Margin:                  big.NewRat(1, 10),
				Pledge:                  500,
				Cost:                    1_000,
				DelegatedStake:          1_000,
				OwnerStake:              500,
				RewardAccountRegistered: true,
				RewardAccountEligible:   true,
				Owners: map[Credential]struct{}{
					owner: {},
				},
				Delegators: []Delegator{
					{Credential: owner, Stake: 500, Registered: true, Eligible: true},
				},
			},
		},
	}

	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		snapshot,
		params,
	)
	require.NoError(t, err)

	require.Equal(t, big.NewRat(2, 1), result.ExpectedBlocks)
	require.Equal(t, big.NewRat(1, 1), result.Efficiency)
	require.Equal(t, uint64(1_000_000), result.Incentives)
	require.Equal(t, uint64(46_283), result.EffectiveRewards)
	require.Equal(t, uint64(953_717), result.Undistributed)
	require.Equal(t, uint64(99_953_717), result.UpdatedPots.Reserves)

	params.Decentralization = big.NewRat(1, 1)
	result, err = Calculate(Pots{Reserves: 100_000_000}, snapshot, params)
	require.NoError(t, err)
	require.Equal(t, big.NewRat(0, 1), result.ExpectedBlocks)
	require.Equal(t, big.NewRat(1, 1), result.Efficiency)
	require.Equal(t, uint64(1_000_000), result.Incentives)
	require.Equal(t, uint64(46_283), result.EffectiveRewards)
	require.Equal(t, uint64(953_717), result.Undistributed)
}

func TestCalculateRoutesUnspendableRewardsToTreasury(t *testing.T) {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	poolID := testPoolID(1)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: 10,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: false,
					RewardAccountEligible:   false,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: false, Eligible: false},
						{Credential: member, Stake: 500, Registered: false, Eligible: false},
					},
				},
			},
		},
		babbageParams(),
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 2)
	require.False(t, result.AccountRewards[0].Spendable)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, uint64(46283), result.AccountRewards[0].Amount)
	require.False(t, result.AccountRewards[1].Spendable)
	require.Equal(t, RewardTypeMember, result.AccountRewards[1].Type)
	require.Equal(t, uint64(37049), result.AccountRewards[1].Amount)
	require.Equal(t, uint64(83332), result.Unspendable)
	require.Equal(t, uint64(1), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(916_668), result.Undistributed)
	require.Equal(t, uint64(99_916_668), result.UpdatedPots.Reserves)
	require.Equal(t, uint64(83_342), result.UpdatedPots.Treasury)
}

func TestCalculatePotDeltasMatchReferenceSemantics(t *testing.T) {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	poolID := testPoolID(1)
	params := babbageParams()
	params.TreasuryExpansion = big.NewRat(1, 5)
	pots := Pots{
		Reserves: 100_000_000,
		Treasury: 10,
		Fees:     2_000,
	}

	result, err := Calculate(
		pots,
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: false,
					RewardAccountEligible:   false,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
						{Credential: member, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)
	require.Greater(t, result.TreasuryTax, uint64(0))
	require.Greater(t, result.Unspendable, uint64(0))
	require.Greater(t, result.EffectiveRewards, uint64(0))

	// This is the same pot movement expressed by cardano-ledger's
	// oldr <-> sumRewards, Amaru's delta_reserves, and the CF calculator's
	// reserve/treasury adjustment after unspendable rewards are known.
	computedAccountRewards := result.EffectiveRewards + result.Unspendable
	reserveDelta := result.Incentives + computedAccountRewards - result.AvailableRewards
	require.Equal(t, pots.Reserves-reserveDelta, result.UpdatedPots.Reserves)
	require.Equal(
		t,
		pots.Treasury+result.TreasuryTax+result.Unspendable,
		result.UpdatedPots.Treasury,
	)
}

func TestCalculateRejectsRewardsAboveAvailablePot(t *testing.T) {
	poolID := testPoolID(1)
	params := babbageParams()
	params.MaxLovelaceSupply = 1_000
	params.OptimalPoolCount = 1

	// The inconsistent block counts intentionally drive apparent performance
	// above one so the calculated rewards exceed the available pot.
	_, err := Calculate(
		Pots{
			Fees: 1_000_000,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 1),
					Pledge:                  0,
					Cost:                    0,
					DelegatedStake:          1_000,
					OwnerStake:              0,
					BlocksProduced:          20,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners:                  map[Credential]struct{}{},
				},
			},
		},
		params,
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "rewards exceed available pot")
}

func TestCalculatePreBabbageFilteredRewardsReturnToReserves(t *testing.T) {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	poolID := testPoolID(1)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: 10,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: false,
					RewardAccountEligible:   false,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: false, Eligible: false},
						{Credential: member, Stake: 500, Registered: false, Eligible: false},
					},
				},
			},
		},
		testParams(),
	)
	require.NoError(t, err)

	require.Empty(t, result.AccountRewards)
	require.Equal(t, uint64(0), result.Unspendable)
	require.Equal(t, uint64(83333), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(1_000_000), result.Undistributed)
	require.Equal(t, uint64(100_000_000), result.UpdatedPots.Reserves)
	require.Equal(t, uint64(10), result.UpdatedPots.Treasury)
}

func TestCalculatePreBabbageFinalUnregisteredRewardsGoToTreasury(t *testing.T) {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	poolID := testPoolID(1)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: 10,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   false,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: true, Eligible: false},
						{Credential: member, Stake: 500, Registered: true, Eligible: false},
					},
				},
			},
		},
		testParams(),
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 2)
	require.False(t, result.AccountRewards[0].Spendable)
	require.False(t, result.AccountRewards[1].Spendable)
	require.Equal(t, uint64(83_332), result.Unspendable)
	require.Equal(t, uint64(916_668), result.Undistributed)
	require.Equal(t, uint64(99_916_668), result.UpdatedPots.Reserves)
	require.Equal(t, uint64(83_342), result.UpdatedPots.Treasury)
}

func TestCalculatePreBabbagePrefilteredRewardsIgnoreCurrentRegistration(t *testing.T) {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	poolID := testPoolID(1)

	result, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: 10,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: false,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: false, Eligible: true},
						{Credential: member, Stake: 500, Registered: false, Eligible: true},
					},
				},
			},
		},
		testParams(),
	)
	require.NoError(t, err)

	require.Empty(t, result.AccountRewards)
	require.Equal(t, uint64(0), result.Unspendable)
	require.Equal(t, uint64(83333), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(1_000_000), result.Undistributed)
	require.Equal(t, uint64(100_000_000), result.UpdatedPots.Reserves)
	require.Equal(t, uint64(10), result.UpdatedPots.Treasury)
}

func TestCalculateShelleyFiltersMultipleRewardsForSameCredential(t *testing.T) {
	owner := testCredential(0, 2)
	shared := testCredential(0, 3)
	poolID := testPoolID(1)

	params := testParams()
	params.ProtocolMajorVersion = 2
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           shared,
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
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
						{Credential: shared, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 1)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, shared, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, uint64(0), result.PoolRewards[0].MemberRewardTotal)
	require.Equal(t, uint64(37_050), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(953_717), result.Undistributed)
	require.Equal(t, uint64(99_953_717), result.UpdatedPots.Reserves)
}

func TestCalculateShelleyFiltersLeaderRewardsByPoolOrder(t *testing.T) {
	shared := testCredential(0, 3)
	ownerA := testCredential(0, 1)
	ownerB := testCredential(0, 2)
	poolA := testPoolID(1)
	poolB := testPoolID(2)

	params := testParams()
	params.ProtocolMajorVersion = 2
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 2_000,
			Pools: []Pool{
				{
					ID:                      poolB,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerB: {},
					},
					Delegators: []Delegator{
						{Credential: ownerB, Stake: 500, Registered: true, Eligible: true},
					},
				},
				{
					ID:                      poolA,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerA: {},
					},
					Delegators: []Delegator{
						{Credential: ownerA, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 1)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, poolA, result.AccountRewards[0].PoolID)
	require.Equal(t, shared, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, poolA, result.PoolRewards[0].PoolID)
	require.Equal(t, uint64(37_050), result.PoolRewards[0].Undistributed)
	require.Equal(t, poolB, result.PoolRewards[1].PoolID)
	require.Equal(t, uint64(83_333), result.PoolRewards[1].Undistributed)
	require.Equal(t, uint64(953_717), result.Undistributed)
	require.Equal(t, uint64(99_953_717), result.UpdatedPots.Reserves)
}

func TestCalculateShelleyFiltersLeaderBeforeLowerPoolMemberReward(t *testing.T) {
	shared := testCredential(0, 3)
	ownerA := testCredential(0, 1)
	ownerB := testCredential(0, 2)
	poolA := testPoolID(1)
	poolB := testPoolID(2)

	params := testParams()
	params.ProtocolMajorVersion = 2
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 2_000,
			Pools: []Pool{
				{
					ID:                      poolA,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: false,
					RewardAccountEligible:   false,
					Owners: map[Credential]struct{}{
						ownerA: {},
					},
					Delegators: []Delegator{
						{Credential: ownerA, Stake: 500, Registered: false, Eligible: false},
						{Credential: shared, Stake: 500, Registered: true, Eligible: true},
					},
				},
				{
					ID:                      poolB,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerB: {},
					},
					Delegators: []Delegator{
						{Credential: ownerB, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 1)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, poolB, result.AccountRewards[0].PoolID)
	require.Equal(t, shared, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, uint64(0), result.PoolRewards[0].MemberRewardTotal)
	require.Equal(t, uint64(83_333), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(37_050), result.PoolRewards[1].Undistributed)
	require.Equal(t, uint64(953_717), result.Undistributed)
	require.Equal(t, uint64(99_953_717), result.UpdatedPots.Reserves)
}

func TestCalculateShelleyDropsMemberRewardsForSharedRewardCredentialAcrossPools(t *testing.T) {
	shared := testCredential(0, 3)
	ownerA := testCredential(0, 1)
	ownerB := testCredential(0, 2)
	poolA := testPoolID(1)
	poolB := testPoolID(2)

	params := testParams()
	params.ProtocolMajorVersion = 2
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 2_000,
			Pools: []Pool{
				{
					ID:                      poolB,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerB: {},
					},
					Delegators: []Delegator{
						{Credential: ownerB, Stake: 500, Registered: true, Eligible: true},
						{Credential: shared, Stake: 500, Registered: true, Eligible: true},
					},
				},
				{
					ID:                      poolA,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerA: {},
					},
					Delegators: []Delegator{
						{Credential: ownerA, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 1)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, poolA, result.AccountRewards[0].PoolID)
	require.Equal(t, shared, result.AccountRewards[0].Credential)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, uint64(0), result.PoolRewards[1].MemberRewardTotal)
	require.Equal(t, uint64(83_333), result.PoolRewards[1].Undistributed)
	require.Equal(t, uint64(953_717), result.Undistributed)
	require.Equal(t, uint64(99_953_717), result.UpdatedPots.Reserves)
}

func TestCalculateAllegraAggregatesMultipleRewardsForSameCredential(t *testing.T) {
	owner := testCredential(0, 2)
	shared := testCredential(0, 3)
	poolID := testPoolID(1)

	params := testParams()
	params.ProtocolMajorVersion = 3
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           shared,
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
						{Credential: owner, Stake: 500, Registered: true, Eligible: true},
						{Credential: shared, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 2)
	require.Equal(t, RewardTypeLeader, result.AccountRewards[0].Type)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, RewardTypeMember, result.AccountRewards[1].Type)
	require.Equal(t, uint64(37_049), result.AccountRewards[1].Amount)
	require.Equal(t, uint64(37_049), result.PoolRewards[0].MemberRewardTotal)
	require.Equal(t, uint64(1), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(916_668), result.Undistributed)
	require.Equal(t, uint64(99_916_668), result.UpdatedPots.Reserves)
}

func TestCalculateAllegraKeepsSameCredentialRewardsAcrossPools(t *testing.T) {
	shared := testCredential(0, 3)
	ownerA := testCredential(0, 1)
	ownerB := testCredential(0, 2)
	poolA := testPoolID(1)
	poolB := testPoolID(2)

	params := testParams()
	params.ProtocolMajorVersion = 3
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 2_000,
			Pools: []Pool{
				{
					ID:                      poolB,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerB: {},
					},
					Delegators: []Delegator{
						{Credential: ownerB, Stake: 500, Registered: true, Eligible: true},
					},
				},
				{
					ID:                      poolA,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					TotalBlocks:             10,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerA: {},
					},
					Delegators: []Delegator{
						{Credential: ownerA, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Len(t, result.AccountRewards, 2)
	require.Equal(t, poolA, result.AccountRewards[0].PoolID)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, poolB, result.AccountRewards[1].PoolID)
	require.Equal(t, uint64(46_283), result.AccountRewards[1].Amount)
	require.Equal(t, uint64(92_566), result.EffectiveRewards)
	require.Equal(t, uint64(37_050), result.PoolRewards[0].Undistributed)
	require.Equal(t, uint64(37_050), result.PoolRewards[1].Undistributed)
	require.Equal(t, uint64(907_434), result.Undistributed)
	require.Equal(t, uint64(99_907_434), result.UpdatedPots.Reserves)
}

func TestCalculateUsesGlobalBlockTotalWhenPoolTotalsAbsent(t *testing.T) {
	shared := testCredential(0, 3)
	ownerA := testCredential(0, 1)
	ownerB := testCredential(0, 2)
	poolA := testPoolID(1)
	poolB := testPoolID(2)

	params := testParams()
	params.ProtocolMajorVersion = 3
	result, err := Calculate(
		Pots{Reserves: 100_000_000},
		Snapshot{
			TotalActiveStake: 2_000,
			Pools: []Pool{
				{
					ID:                      poolB,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerB: {},
					},
					Delegators: []Delegator{
						{Credential: ownerB, Stake: 500, Registered: true, Eligible: true},
					},
				},
				{
					ID:                      poolA,
					RewardAccount:           shared,
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          5,
					RewardAccountRegistered: true,
					RewardAccountEligible:   true,
					Owners: map[Credential]struct{}{
						ownerA: {},
					},
					Delegators: []Delegator{
						{Credential: ownerA, Stake: 500, Registered: true, Eligible: true},
					},
				},
			},
		},
		params,
	)
	require.NoError(t, err)

	require.Equal(t, uint64(10), result.TotalBlocks)
	require.Len(t, result.AccountRewards, 2)
	require.Equal(t, poolA, result.AccountRewards[0].PoolID)
	require.Equal(t, uint64(46_283), result.AccountRewards[0].Amount)
	require.Equal(t, poolB, result.AccountRewards[1].PoolID)
	require.Equal(t, uint64(46_283), result.AccountRewards[1].Amount)
	require.Equal(t, uint64(907_434), result.Undistributed)
	require.Equal(t, uint64(99_907_434), result.UpdatedPots.Reserves)
}

func TestApparentPerformanceHonorsDecentralizationThreshold(t *testing.T) {
	require.Equal(
		t,
		big.NewRat(1, 1),
		apparentPerformance(big.NewRat(4, 5), 1, 100, 0, 10),
	)
	require.Equal(
		t,
		big.NewRat(1, 1),
		apparentPerformance(big.NewRat(1, 1), 1, 100, 0, 10),
	)
	require.Equal(
		t,
		big.NewRat(2, 1),
		apparentPerformance(big.NewRat(0, 1), 10, 100, 2, 10),
	)
}

func TestRewardEfficiencyHonorsDecentralizationThreshold(t *testing.T) {
	// Mirrors cardano-ledger's eta shortcut in PulsingReward.startStep:
	// when d >= 0.8, the reward pot is not reduced by observed block count.
	require.Equal(
		t,
		big.NewRat(1, 1),
		rewardEfficiency(0, big.NewRat(10, 1), big.NewRat(4, 5)),
	)
	require.Equal(
		t,
		big.NewRat(1, 1),
		rewardEfficiency(0, big.NewRat(10, 1), big.NewRat(1, 1)),
	)
	require.Equal(
		t,
		big.NewRat(1, 5),
		rewardEfficiency(2, big.NewRat(10, 1), big.NewRat(0, 1)),
	)
}

func TestCalculateRewardPotMatchesCFCalculatorEtaFloorVectors(t *testing.T) {
	params := testParams()
	params.MonetaryExpansion = big.NewRat(3, 1000)
	params.TreasuryExpansion = big.NewRat(0, 1)
	params.Decentralization = big.NewRat(0, 1)
	params.ActiveSlotsCoeff = big.NewRat(1, 1)
	params.EpochLength = 60
	params.MaxLovelaceSupply = 45_000_000_000_000_000

	snapshot := Snapshot{
		TotalActiveStake: 0,
		Pools: []Pool{
			{
				ID:             testPoolID(1),
				RewardAccount:  testCredential(0, 4),
				Margin:         big.NewRat(0, 1),
				BlocksProduced: 59,
				TotalBlocks:    59,
			},
		},
	}

	for _, tc := range []struct {
		name   string
		pots   Pots
		reward uint64
	}{
		{
			name: "exact rational floor",
			pots: Pots{
				Reserves: 35_989_500_000_000_000,
			},
			reward: 106_169_025_000_000,
		},
		{
			name: "fractional result floors before adding fees",
			pots: Pots{
				Reserves: 1_000,
				Fees:     7,
			},
			reward: 9,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := Calculate(tc.pots, snapshot, params)
			require.NoError(t, err)
			require.Equal(t, big.NewRat(59, 60), result.Efficiency)
			require.Equal(t, tc.reward, result.TotalRewardPot)
		})
	}
}

func TestCalculateRewardPotMatchesCFCalculatorEtaBoundaryVectors(t *testing.T) {
	snapshot := Snapshot{
		TotalActiveStake: 0,
		Pools: []Pool{
			{
				ID:             testPoolID(1),
				RewardAccount:  testCredential(0, 4),
				Margin:         big.NewRat(0, 1),
				BlocksProduced: 0,
				TotalBlocks:    0,
			},
		},
	}

	for _, tc := range []struct {
		name           string
		totalBlocks    uint64
		decentralized  *big.Rat
		reward         uint64
		treasuryTax    uint64
		available      uint64
		wantEfficiency *big.Rat
	}{
		{
			name:           "eta one at decentralization threshold",
			decentralized:  big.NewRat(4, 5),
			reward:         3,
			wantEfficiency: big.NewRat(1, 1),
		},
		{
			name:           "eta capped at one when blocks meet expectation",
			totalBlocks:    60,
			decentralized:  big.NewRat(0, 1),
			reward:         3,
			wantEfficiency: big.NewRat(1, 1),
		},
		{
			name:           "treasury cut uses exact ratio floor",
			totalBlocks:    59,
			decentralized:  big.NewRat(0, 1),
			reward:         106_169_025_000_000,
			treasuryTax:    21_233_805_000_000,
			available:      84_935_220_000_000,
			wantEfficiency: big.NewRat(59, 60),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			params := testParams()
			params.MonetaryExpansion = big.NewRat(3, 1000)
			params.TreasuryExpansion = big.NewRat(0, 1)
			params.Decentralization = tc.decentralized
			params.ActiveSlotsCoeff = big.NewRat(1, 1)
			params.EpochLength = 60
			params.MaxLovelaceSupply = 45_000_000_000_000_000
			pots := Pots{
				Reserves: 1_000,
			}
			if tc.treasuryTax != 0 {
				params.TreasuryExpansion = big.NewRat(1, 5)
				pots.Reserves = 35_989_500_000_000_000
			}

			testSnapshot := snapshot
			testSnapshot.Pools = append([]Pool(nil), snapshot.Pools...)
			testSnapshot.Pools[0].BlocksProduced = tc.totalBlocks
			testSnapshot.Pools[0].TotalBlocks = tc.totalBlocks

			result, err := Calculate(pots, testSnapshot, params)
			require.NoError(t, err)
			require.Equal(t, tc.wantEfficiency, result.Efficiency)
			require.Equal(t, tc.reward, result.TotalRewardPot)
			require.Equal(t, tc.treasuryTax, result.TreasuryTax)
			if tc.available != 0 {
				require.Equal(t, tc.available, result.AvailableRewards)
			}
		})
	}
}

func TestCalculateKeepsFractionalExpectedBlocksExact(t *testing.T) {
	params := testParams()
	params.EpochLength = 10
	params.ActiveSlotsCoeff = big.NewRat(1, 3)
	params.Decentralization = big.NewRat(0, 1)
	params.MonetaryExpansion = big.NewRat(1, 1)

	result, err := Calculate(Pots{Reserves: 100}, Snapshot{
		Pools: []Pool{{
			ID:             testPoolID(1),
			RewardAccount:  testCredential(0, 2),
			BlocksProduced: 1,
			TotalBlocks:    1,
		}},
	}, params)
	require.NoError(t, err)
	require.Equal(t, big.NewRat(10, 3), result.ExpectedBlocks)
	require.Equal(t, big.NewRat(3, 10), result.Efficiency)
	require.Equal(t, uint64(30), result.Incentives)
}

func TestCalculateRejectsInvalidUnitIntervals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Parameters)
	}{
		{
			name: "monetary expansion",
			mutate: func(params *Parameters) {
				params.MonetaryExpansion = big.NewRat(2, 1)
			},
		},
		{
			name: "treasury expansion",
			mutate: func(params *Parameters) {
				params.TreasuryExpansion = big.NewRat(2, 1)
			},
		},
		{
			name: "decentralization",
			mutate: func(params *Parameters) {
				params.Decentralization = big.NewRat(2, 1)
			},
		},
		{
			name: "active slot coeff",
			mutate: func(params *Parameters) {
				params.ActiveSlotsCoeff = big.NewRat(2, 1)
			},
		},
		{
			name: "active slot coeff is zero",
			mutate: func(params *Parameters) {
				params.ActiveSlotsCoeff = big.NewRat(0, 1)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			params := testParams()
			tc.mutate(&params)
			_, err := Calculate(Pots{}, Snapshot{}, params)
			require.ErrorIs(t, err, ErrInvalidParameters)
			require.ErrorContains(t, err, tc.name)
		})
	}
}

func TestCalculateRejectsInvalidPoolMargins(t *testing.T) {
	cases := []struct {
		name   string
		margin *big.Rat
	}{
		{name: "negative", margin: big.NewRat(-1, 10)},
		{name: "above one", margin: big.NewRat(11, 10)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Calculate(
				Pots{},
				Snapshot{
					Pools: []Pool{
						{
							ID:     testPoolID(1),
							Margin: tc.margin,
						},
					},
				},
				testParams(),
			)
			require.ErrorIs(t, err, ErrInvalidParameters)
			require.ErrorContains(t, err, "margin outside [0,1]")
		})
	}
}

func TestCalculateRejectsOwnerStakeAboveDelegatedStake(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 9,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 9,
					OwnerStake:     10,
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "owner stake 10 exceeds delegated stake 9")
}

func TestCalculateRejectsOwnerMissingFromDelegators(t *testing.T) {
	owner := testCredential(0, 2)
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 1,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 1,
					Owners: map[Credential]struct{}{
						owner: {},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "owner")
	require.ErrorContains(t, err, "is not a delegator")
}

func TestCalculateRejectsIncorrectOwnerStake(t *testing.T) {
	owner := testCredential(0, 2)
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 5,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 5,
					OwnerStake:     5,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 4},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "computed owner stake 4")
	require.ErrorContains(t, err, "does not match owner stake 5")
}

func TestCalculateRejectsConflictingCredentialEligibility(t *testing.T) {
	shared := testCredential(0, 3)
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 1,
			Pools: []Pool{
				{
					ID:                    testPoolID(1),
					RewardAccount:         shared,
					RewardAccountEligible: true,
					DelegatedStake:        1,
					Delegators: []Delegator{
						{Credential: shared, Stake: 1, Eligible: false},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "conflicting reward eligibility")
}

func TestCalculateRejectsScriptPoolOwner(t *testing.T) {
	scriptOwner := testCredential(1, 2)
	_, err := Calculate(
		Pots{},
		Snapshot{
			Pools: []Pool{
				{
					ID: testPoolID(1),
					Owners: map[Credential]struct{}{
						scriptOwner: {},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "non-key credential tag")
}

func TestCalculateRejectsInvalidRewardAccountCredentialTag(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			Pools: []Pool{
				{
					ID:            testPoolID(1),
					RewardAccount: testCredential(2, 3),
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "reward account")
	require.ErrorContains(t, err, "invalid credential tag")
}

func TestCalculateRejectsInvalidDelegatorCredentialTag(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 1,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					RewardAccount:  testCredential(0, 3),
					DelegatedStake: 1,
					Delegators: []Delegator{
						{Credential: testCredential(2, 4), Stake: 1},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "delegator")
	require.ErrorContains(t, err, "invalid credential tag")
}

func TestCalculateRejectsDuplicatePoolSnapshotRows(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			Pools: []Pool{
				{ID: testPoolID(1)},
				{ID: testPoolID(1)},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "duplicate pool")
}

func TestCalculateRejectsActiveStakeMismatch(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 2,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 1,
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(
		t,
		err,
		"total delegated stake 1 does not match active stake 2",
	)
}

func TestCalculateRejectsTotalBlocksOverflow(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: math.MaxUint64,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: math.MaxUint64 - 1,
					BlocksProduced: math.MaxUint64,
				},
				{
					ID:             testPoolID(2),
					DelegatedStake: 1,
					BlocksProduced: 1,
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "total blocks overflow")
}

func TestCalculateRejectsInconsistentExplicitTotalBlocks(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 2,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 1,
					TotalBlocks:    10,
				},
				{
					ID:             testPoolID(2),
					DelegatedStake: 1,
					TotalBlocks:    11,
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "inconsistent total blocks")
}

func TestCalculateRejectsDuplicatePoolDelegators(t *testing.T) {
	credential := testCredential(0, 2)
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 2,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 2,
					Delegators: []Delegator{
						{Credential: credential, Stake: 1},
						{Credential: credential, Stake: 1},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "duplicate delegator")
}

func TestCalculateRejectsDelegatorInMultiplePools(t *testing.T) {
	credential := testCredential(0, 2)
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 2,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 1,
					Delegators: []Delegator{
						{Credential: credential, Stake: 1},
					},
				},
				{
					ID:             testPoolID(2),
					DelegatedStake: 1,
					Delegators: []Delegator{
						{Credential: credential, Stake: 1},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "duplicate delegator")
	require.ErrorContains(t, err, "in pools")
}

func TestCalculateRejectsDelegatorStakeAbovePoolStake(t *testing.T) {
	_, err := Calculate(
		Pots{},
		Snapshot{
			TotalActiveStake: 2,
			Pools: []Pool{
				{
					ID:             testPoolID(1),
					DelegatedStake: 2,
					Delegators: []Delegator{
						{Credential: testCredential(0, 2), Stake: 3},
					},
				},
			},
		},
		testParams(),
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(
		t,
		err,
		"delegator stake 3 exceeds delegated stake 2",
	)
}

func TestCalculateRejectsReservesAboveMaxSupply(t *testing.T) {
	params := testParams()
	params.MaxLovelaceSupply = 1_000

	_, err := Calculate(
		Pots{
			Reserves: 1_001,
		},
		Snapshot{},
		params,
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "reserves 1001 exceed max supply 1000")
}

func TestCalculateRejectsTreasuryTaxOverflow(t *testing.T) {
	params := testParams()
	params.MonetaryExpansion = big.NewRat(0, 1)
	params.TreasuryExpansion = big.NewRat(1, 1)

	_, err := Calculate(
		Pots{
			Treasury: math.MaxUint64,
			Fees:     1,
		},
		Snapshot{},
		params,
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "treasury tax overflow")
}

func TestCalculateRejectsReserveRefundOverflow(t *testing.T) {
	params := testParams()
	params.MonetaryExpansion = big.NewRat(0, 1)
	params.MaxLovelaceSupply = math.MaxUint64

	_, err := Calculate(
		Pots{
			Reserves: math.MaxUint64,
			Fees:     1,
		},
		Snapshot{},
		params,
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "reserve refund overflow")
}

func TestCalculateRejectsUnspendableTreasuryOverflow(t *testing.T) {
	owner := testCredential(0, 2)
	member := testCredential(0, 3)
	poolID := testPoolID(1)
	params := babbageParams()

	_, err := Calculate(
		Pots{
			Reserves: 100_000_000,
			Treasury: math.MaxUint64,
		},
		Snapshot{
			TotalActiveStake: 1_000,
			Pools: []Pool{
				{
					ID:                      poolID,
					RewardAccount:           testCredential(0, 4),
					Margin:                  big.NewRat(1, 10),
					Pledge:                  500,
					Cost:                    1_000,
					DelegatedStake:          1_000,
					OwnerStake:              500,
					BlocksProduced:          10,
					TotalBlocks:             10,
					RewardAccountRegistered: false,
					RewardAccountEligible:   false,
					Owners: map[Credential]struct{}{
						owner: {},
					},
					Delegators: []Delegator{
						{Credential: owner, Stake: 500, Registered: false, Eligible: false},
						{Credential: member, Stake: 500, Registered: false, Eligible: false},
					},
				},
			},
		},
		params,
	)
	require.ErrorIs(t, err, ErrInvalidParameters)
	require.ErrorContains(t, err, "unspendable treasury overflow")
}

func TestFloorRatCheckedRejectsUint64Overflow(t *testing.T) {
	tooLarge := new(big.Rat).SetInt(
		new(big.Int).Add(
			new(big.Int).SetUint64(math.MaxUint64),
			big.NewInt(1),
		),
	)

	_, err := floorRatChecked(tooLarge)
	require.ErrorIs(t, err, ErrRewardAmountOverflow)
}

func testParams() Parameters {
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

func babbageParams() Parameters {
	params := testParams()
	params.ProtocolMajorVersion = 7
	return params
}

func testCredential(tag uint8, fill byte) Credential {
	var hash [CredentialHashSize]byte
	for i := range hash {
		hash[i] = fill
	}
	return Credential{Tag: tag, Hash: hash}
}

func testPoolID(fill byte) PoolID {
	var hash PoolID
	for i := range hash {
		hash[i] = fill
	}
	return hash
}
