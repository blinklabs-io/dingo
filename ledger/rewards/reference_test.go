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
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHaskellReferenceApparentPerformanceBranches(t *testing.T) {
	require.Zero(
		t,
		apparentPerformance(big.NewRat(0, 1), 0, 100, 2, 20).Sign(),
	)
	require.Equal(
		t,
		big.NewRat(1, 2),
		apparentPerformance(big.NewRat(0, 1), 20, 100, 2, 20),
	)
	require.Equal(
		t,
		big.NewRat(1, 1),
		apparentPerformance(big.NewRat(4, 5), 20, 100, 0, 0),
	)
}

func TestHaskellReferenceOperatorRewardBranches(t *testing.T) {
	t.Run("pool reward at or below cost pays all to operator", func(t *testing.T) {
		require.Equal(
			t,
			uint64(90),
			requireLeaderReward(t, 90, 100, big.NewRat(1, 50), 100, 1_000),
		)
		require.Equal(
			t,
			uint64(0),
			requireMemberReward(t, 90, 100, big.NewRat(1, 50), 300, 1_000),
		)
	})

	t.Run("cost plus margin plus owner share uses exact floor", func(t *testing.T) {
		require.Equal(
			t,
			uint64(417),
			requireLeaderReward(t, 1_000, 340, big.NewRat(1, 50), 100, 1_000),
		)
	})
}

func TestHaskellReferenceMemberRewardBranches(t *testing.T) {
	require.Equal(
		t,
		uint64(0),
		requireMemberReward(t, 1_000, 340, big.NewRat(1, 50), 0, 1_000),
	)
	require.Equal(
		t,
		uint64(194),
		requireMemberReward(t, 1_000, 340, big.NewRat(1, 50), 300, 1_000),
	)
}

func TestShelleySpecOptimalPoolRewardVectors(t *testing.T) {
	for _, tc := range []struct {
		name      string
		poolStake uint64
		pledge    uint64
		want      uint64
	}{
		{
			name:      "saturated with half saturation pledge",
			poolStake: 1_000,
			pledge:    500,
			want:      83_333,
		},
		{
			name:      "saturated with saturation pledge",
			poolStake: 2_000,
			pledge:    2_000,
			want:      100_000,
		},
		{
			name:      "saturated with no pledge influence bonus",
			poolStake: 2_000,
			pledge:    0,
			want:      66_666,
		},
		{
			name:      "unsaturated with exact pledge discount floor",
			poolStake: 500,
			pledge:    250,
			want:      36_458,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				tc.want,
				requireOptimalPoolReward(
					t,
					1_000_000,
					10,
					big.NewRat(1, 2),
					tc.poolStake,
					tc.pledge,
					10_000,
				),
			)
		})
	}
}

func TestCFCalculatorPoolRewardFormulaVectors(t *testing.T) {
	require.Equal(
		t,
		uint64(66_800),
		requireOptimalPoolReward(
			t,
			801_600,
			10,
			big.NewRat(1, 2),
			1_000,
			500,
			10_000,
		),
	)

	for _, tc := range []struct {
		name         string
		poolReward   uint64
		cost         uint64
		margin       *big.Rat
		ownerStake   uint64
		memberStake  uint64
		totalStake   uint64
		wantLeader   uint64
		wantMember   uint64
		wantLeftover uint64
	}{
		{
			name:         "medium pool README example with ledger floors",
			poolReward:   4_000,
			cost:         340,
			margin:       big.NewRat(1, 50),
			memberStake:  1_000,
			totalStake:   1_000,
			wantLeader:   413,
			wantMember:   3_586,
			wantLeftover: 1,
		},
		{
			name:         "small pool README example with ledger floors",
			poolReward:   400,
			cost:         340,
			margin:       big.NewRat(1, 50),
			memberStake:  1_000,
			totalStake:   1_000,
			wantLeader:   341,
			wantMember:   58,
			wantLeftover: 1,
		},
		{
			name:         "frontend pledge-return term",
			poolReward:   4_000,
			cost:         340,
			margin:       big.NewRat(1, 50),
			ownerStake:   100,
			memberStake:  900,
			totalStake:   1_000,
			wantLeader:   771,
			wantMember:   3_228,
			wantLeftover: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := requireLeaderReward(
				t,
				tc.poolReward,
				tc.cost,
				tc.margin,
				tc.ownerStake,
				tc.totalStake,
			)
			member := requireMemberReward(
				t,
				tc.poolReward,
				tc.cost,
				tc.margin,
				tc.memberStake,
				tc.totalStake,
			)
			require.Equal(t, tc.wantLeader, leader)
			require.Equal(t, tc.wantMember, member)
			require.Equal(t, tc.wantLeftover, tc.poolReward-leader-member)
		})
	}
}

func TestAmaruReferenceRewardVector(t *testing.T) {
	vector := loadAmaruRewardVector(t)

	result, err := Calculate(
		vector.Pots.toPots(),
		vector.Snapshot.toSnapshot(t),
		vector.Parameters.toParameters(t),
	)
	require.NoError(t, err)

	source := vector.Source.Repository + "@" + vector.Source.Commit +
		":" + vector.Source.Path
	require.Equal(t, uint64(vector.Expected.Incentives), result.Incentives, source)
	require.Equal(t, uint64(vector.Expected.TotalRewardPot), result.TotalRewardPot, source)
	require.Equal(t, uint64(vector.Expected.TreasuryTax), result.TreasuryTax, source)
	require.Equal(t, uint64(vector.Expected.AvailableRewards), result.AvailableRewards, source)
	require.Equal(t, uint64(vector.Expected.EffectiveRewards), result.EffectiveRewards, source)
	require.Equal(t, uint64(vector.Expected.Undistributed), result.Undistributed, source)
	require.Equal(t, vector.Expected.UpdatedPots.toPots(), result.UpdatedPots, source)
	require.Len(t, result.PoolRewards, len(vector.Expected.PoolRewards), source)
	for i, want := range vector.Expected.PoolRewards {
		got := result.PoolRewards[i]
		require.Equal(t, parsePoolID(t, want.PoolID), got.PoolID, source)
		require.Equal(t, uint64(want.OptimalReward), got.OptimalReward, source)
		require.Equal(t, uint64(want.PoolReward), got.PoolReward, source)
		require.Equal(t, uint64(want.LeaderReward), got.LeaderReward, source)
		require.Equal(t, uint64(want.MemberRewardTotal), got.MemberRewardTotal, source)
		require.Equal(t, uint64(want.OwnerStake), got.OwnerStake, source)
		require.Equal(t, uint64(want.Undistributed), got.Undistributed, source)
		require.Equal(t, uint64(want.Unspendable), got.Unspendable, source)
	}
	require.Len(t, result.AccountRewards, len(vector.Expected.AccountRewards), source)
	for i, want := range vector.Expected.AccountRewards {
		got := result.AccountRewards[i]
		require.Equal(t, want.Credential.toCredential(t), got.Credential, source)
		require.Equal(t, parsePoolID(t, want.PoolID), got.PoolID, source)
		require.Equal(t, RewardType(want.Type), got.Type, source)
		require.Equal(t, uint64(want.Amount), got.Amount, source)
		require.Equal(t, want.Spendable, got.Spendable, source)
	}
}

type amaruRewardVector struct {
	Name       string                 `json:"name"`
	Source     rewardVectorSource     `json:"source"`
	Parameters rewardVectorParameters `json:"parameters"`
	Pots       rewardVectorPots       `json:"pots"`
	Snapshot   rewardVectorSnapshot   `json:"snapshot"`
	Expected   rewardVectorExpected   `json:"expected"`
}

type rewardVectorSource struct {
	Implementation string   `json:"implementation"`
	Repository     string   `json:"repository"`
	Commit         string   `json:"commit"`
	Path           string   `json:"path"`
	Functions      []string `json:"functions"`
}

type rewardVectorParameters struct {
	MonetaryExpansion string `json:"monetary_expansion"`
	TreasuryExpansion string `json:"treasury_expansion"`
	Decentralization  string `json:"decentralization"`
	PledgeInfluence   string `json:"pledge_influence"`
	ActiveSlotsCoeff  string `json:"active_slots_coeff"`
	OptimalPoolCount  uint64 `json:"optimal_pool_count"`
	EpochLength       uint64 `json:"epoch_length"`
	MaxLovelaceSupply uint64 `json:"max_lovelace_supply"`
	ProtocolMajorVer  uint64 `json:"protocol_major_version"`
}

type rewardVectorPots struct {
	Reserves uint64 `json:"reserves"`
	Treasury uint64 `json:"treasury"`
	Fees     uint64 `json:"fees"`
}

type rewardVectorSnapshot struct {
	TotalActiveStake uint64             `json:"total_active_stake"`
	Pools            []rewardVectorPool `json:"pools"`
}

type rewardVectorPool struct {
	ID                      string                   `json:"id"`
	RewardAccount           rewardVectorCredential   `json:"reward_account"`
	Margin                  string                   `json:"margin"`
	Pledge                  uint64                   `json:"pledge"`
	Cost                    uint64                   `json:"cost"`
	DelegatedStake          uint64                   `json:"delegated_stake"`
	OwnerStake              uint64                   `json:"owner_stake"`
	BlocksProduced          uint64                   `json:"blocks_produced"`
	TotalBlocks             uint64                   `json:"total_blocks"`
	RewardAccountRegistered bool                     `json:"reward_account_registered"`
	RewardAccountEligible   bool                     `json:"reward_account_eligible"`
	Owners                  []rewardVectorCredential `json:"owners"`
	Delegators              []rewardVectorDelegator  `json:"delegators"`
}

type rewardVectorDelegator struct {
	Credential rewardVectorCredential `json:"credential"`
	Stake      uint64                 `json:"stake"`
	Registered bool                   `json:"registered"`
	Eligible   bool                   `json:"eligible"`
}

type rewardVectorCredential struct {
	Tag  uint8  `json:"tag"`
	Hash string `json:"hash"`
}

type rewardVectorExpected struct {
	Incentives       uint64                      `json:"incentives"`
	TotalRewardPot   uint64                      `json:"total_reward_pot"`
	TreasuryTax      uint64                      `json:"treasury_tax"`
	AvailableRewards uint64                      `json:"available_rewards"`
	EffectiveRewards uint64                      `json:"effective_rewards"`
	Undistributed    uint64                      `json:"undistributed"`
	UpdatedPots      rewardVectorPots            `json:"updated_pots"`
	PoolRewards      []rewardVectorPoolReward    `json:"pool_rewards"`
	AccountRewards   []rewardVectorAccountReward `json:"account_rewards"`
}

type rewardVectorPoolReward struct {
	PoolID            string `json:"pool_id"`
	OptimalReward     uint64 `json:"optimal_reward"`
	PoolReward        uint64 `json:"pool_reward"`
	LeaderReward      uint64 `json:"leader_reward"`
	MemberRewardTotal uint64 `json:"member_reward_total"`
	OwnerStake        uint64 `json:"owner_stake"`
	Undistributed     uint64 `json:"undistributed"`
	Unspendable       uint64 `json:"unspendable"`
}

type rewardVectorAccountReward struct {
	Credential rewardVectorCredential `json:"credential"`
	PoolID     string                 `json:"pool_id"`
	Type       string                 `json:"type"`
	Amount     uint64                 `json:"amount"`
	Spendable  bool                   `json:"spendable"`
}

func loadAmaruRewardVector(t *testing.T) amaruRewardVector {
	t.Helper()
	data, err := os.ReadFile("testdata/amaru_single_pool_reward_vector.json")
	require.NoError(t, err)
	var vector amaruRewardVector
	require.NoError(t, json.Unmarshal(data, &vector))
	require.NotEmpty(t, vector.Source.Repository)
	require.NotEmpty(t, vector.Source.Commit)
	require.NotEmpty(t, vector.Source.Path)
	require.NotZero(t, vector.Parameters.ProtocolMajorVer)
	return vector
}

func (p rewardVectorParameters) toParameters(t *testing.T) Parameters {
	t.Helper()
	return Parameters{
		MonetaryExpansion:    parseRat(t, p.MonetaryExpansion),
		TreasuryExpansion:    parseRat(t, p.TreasuryExpansion),
		Decentralization:     parseRat(t, p.Decentralization),
		PledgeInfluence:      parseRat(t, p.PledgeInfluence),
		ActiveSlotsCoeff:     parseRat(t, p.ActiveSlotsCoeff),
		OptimalPoolCount:     p.OptimalPoolCount,
		EpochLength:          p.EpochLength,
		MaxLovelaceSupply:    p.MaxLovelaceSupply,
		ProtocolMajorVersion: p.ProtocolMajorVer,
	}
}

func (p rewardVectorPots) toPots() Pots {
	return Pots{Reserves: p.Reserves, Treasury: p.Treasury, Fees: p.Fees}
}

func (s rewardVectorSnapshot) toSnapshot(t *testing.T) Snapshot {
	t.Helper()
	pools := make([]Pool, 0, len(s.Pools))
	for _, item := range s.Pools {
		owners := make(map[Credential]struct{}, len(item.Owners))
		for _, owner := range item.Owners {
			owners[owner.toCredential(t)] = struct{}{}
		}
		delegators := make([]Delegator, 0, len(item.Delegators))
		for _, delegator := range item.Delegators {
			delegators = append(delegators, Delegator{
				Credential: delegator.Credential.toCredential(t),
				Stake:      delegator.Stake,
				Registered: delegator.Registered,
				Eligible:   delegator.Eligible,
			})
		}
		pools = append(pools, Pool{
			ID:                      parsePoolID(t, item.ID),
			RewardAccount:           item.RewardAccount.toCredential(t),
			Margin:                  parseRat(t, item.Margin),
			Pledge:                  item.Pledge,
			Cost:                    item.Cost,
			DelegatedStake:          item.DelegatedStake,
			OwnerStake:              item.OwnerStake,
			BlocksProduced:          item.BlocksProduced,
			TotalBlocks:             item.TotalBlocks,
			RewardAccountRegistered: item.RewardAccountRegistered,
			RewardAccountEligible:   item.RewardAccountEligible,
			Owners:                  owners,
			Delegators:              delegators,
		})
	}
	return Snapshot{TotalActiveStake: s.TotalActiveStake, Pools: pools}
}

func (c rewardVectorCredential) toCredential(t *testing.T) Credential {
	t.Helper()
	decoded, err := hex.DecodeString(c.Hash)
	require.NoError(t, err)
	ret, err := NewCredential(c.Tag, decoded)
	require.NoError(t, err)
	return ret
}

func parsePoolID(t *testing.T, value string) PoolID {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	require.NoError(t, err)
	ret, err := NewPoolID(decoded)
	require.NoError(t, err)
	return ret
}

func parseRat(t *testing.T, value string) *big.Rat {
	t.Helper()
	ret, ok := new(big.Rat).SetString(value)
	require.True(t, ok, "invalid rational %q", value)
	return ret
}

func requireOptimalPoolReward(
	t *testing.T,
	availableRewards uint64,
	optimalPoolCount uint64,
	a0 *big.Rat,
	poolStake uint64,
	pledge uint64,
	totalStake uint64,
) uint64 {
	t.Helper()
	ret, err := optimalPoolRewardChecked(
		availableRewards,
		optimalPoolCount,
		a0,
		poolStake,
		pledge,
		totalStake,
	)
	require.NoError(t, err)
	return ret
}

func requireLeaderReward(
	t *testing.T,
	poolReward uint64,
	cost uint64,
	margin *big.Rat,
	ownerStake uint64,
	poolStake uint64,
) uint64 {
	t.Helper()
	ret, err := leaderRewardChecked(poolReward, cost, margin, ownerStake, poolStake)
	require.NoError(t, err)
	return ret
}

func requireMemberReward(
	t *testing.T,
	poolReward uint64,
	cost uint64,
	margin *big.Rat,
	memberStake uint64,
	poolStake uint64,
) uint64 {
	t.Helper()
	ret, err := memberRewardChecked(poolReward, cost, margin, memberStake, poolStake)
	require.NoError(t, err)
	return ret
}
