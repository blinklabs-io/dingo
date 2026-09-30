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

// primeReferencePool builds a one-pool snapshot plus a filler pool carrying
// the rest of the active stake, with all Prime Mainnet reward inputs taken
// from the db-sync-compatible Prime API. The filler makes the active-stake
// denominator match the network total without contributing rewards.
func primeReferencePool(
	t *testing.T,
	poolHex string,
	memberHex string,
	margin *big.Rat,
	cost, poolStake, ownerStake, blocks, totalBlocks, activeStake uint64,
) (Pool, Pool, Credential) {
	t.Helper()
	poolHash, err := hex.DecodeString(poolHex)
	require.NoError(t, err)
	memberHash, err := hex.DecodeString(memberHex)
	require.NoError(t, err)
	pid, err := NewPoolID(poolHash)
	require.NoError(t, err)
	member, err := NewCredential(0, memberHash)
	require.NoError(t, err)
	ownerHash := make([]byte, 28)
	ownerHash[0] = 0xaa
	owner, err := NewCredential(0, ownerHash)
	require.NoError(t, err)
	pool := Pool{
		ID:                      pid,
		RewardAccount:           owner,
		Margin:                  margin,
		Cost:                    cost,
		DelegatedStake:          poolStake,
		OwnerStake:              ownerStake,
		BlocksProduced:          blocks,
		TotalBlocks:             totalBlocks,
		RewardAccountRegistered: true,
		RewardAccountEligible:   true,
		Delegators: []Delegator{
			{Credential: owner, Stake: ownerStake, Registered: true, Eligible: true},
			{Credential: member, Stake: poolStake - ownerStake, Registered: true, Eligible: true},
		},
		Owners: map[Credential]struct{}{owner: {}},
	}
	fillerHash := make([]byte, 28)
	fillerHash[0] = 0xff
	fid, err := NewPoolID(fillerHash)
	require.NoError(t, err)
	fcred, err := NewCredential(0, fillerHash)
	require.NoError(t, err)
	rest := activeStake - poolStake
	filler := Pool{
		ID:                      fid,
		RewardAccount:           fcred,
		Margin:                  new(big.Rat),
		DelegatedStake:          rest,
		TotalBlocks:             totalBlocks,
		RewardAccountRegistered: true,
		RewardAccountEligible:   true,
		Delegators: []Delegator{
			{Credential: fcred, Stake: rest, Registered: true, Eligible: true},
		},
		Owners: map[Credential]struct{}{},
	}
	return pool, filler, member
}

func primeReferenceAmounts(
	t *testing.T,
	pots Pots,
	pool, filler Pool,
	active uint64,
	params Parameters,
	member Credential,
) (leader, memberReward uint64) {
	t.Helper()
	res, err := Calculate(
		pots,
		Snapshot{Pools: []Pool{pool, filler}, TotalActiveStake: active},
		params,
	)
	require.NoError(t, err)
	for _, r := range res.AccountRewards {
		switch {
		case r.Type == RewardTypeLeader && r.PoolID == pool.ID:
			leader = r.Amount
		case r.Type == RewardTypeMember && r.PoolID == pool.ID &&
			r.Credential == member:
			memberReward = r.Amount
		}
	}
	return leader, memberReward
}

// TestPrimeMainnetEpoch39MemberRewardUsesBabbageDecentralization pins the
// canonical Prime Mainnet epoch-39 member reward (821877 lovelace for
// stake1u8qd4rm5...). The round ran in the first Babbage epoch, so d is 0 and
// expectedBlocks is 21600; with the Alonzo d of 7/10 the same inputs pay
// 2735325.
func TestPrimeMainnetEpoch39MemberRewardUsesBabbageDecentralization(
	t *testing.T,
) {
	t.Parallel()

	const (
		activeStake = uint64(33603505000305)
		poolStake   = uint64(4800500038924)
		ownerStake  = uint64(500000425)
	)
	pool, filler, member := primeReferencePool(
		t,
		"4ae106a4361c18f9ac0ed3229d6ea6577c08ab40c329a477a33c34d5",
		"c0da8f74f9978de9f4001c2f8f5996d12b52ebfa7ea75db7fbb0f114",
		big.NewRat(1, 100), 0, poolStake, ownerStake, 213, 1614, activeStake,
	)
	params := func(d *big.Rat) Parameters {
		return Parameters{
			MonetaryExpansion:    big.NewRat(1, 100000),
			TreasuryExpansion:    big.NewRat(1, 1000000),
			Decentralization:     d,
			PledgeInfluence:      new(big.Rat),
			ActiveSlotsCoeff:     big.NewRat(1, 20),
			OptimalPoolCount:     100,
			EpochLength:          432000,
			MaxLovelaceSupply:    3000000000000000,
			ProtocolMajorVersion: 7,
		}
	}
	pots := Pots{Reserves: 600000029754619, Fees: 1000000}

	_, got := primeReferenceAmounts(
		t, pots, pool, filler, activeStake, params(new(big.Rat)), member,
	)
	require.Equal(t, uint64(821877), got)

	_, alonzoD := primeReferenceAmounts(
		t, pots, pool, filler, activeStake, params(big.NewRat(7, 10)), member,
	)
	require.Equal(t, uint64(2735325), alonzoD)
}

// TestPrimeMainnetEpoch53CanonicalRewards pins the canonical Prime Mainnet
// round for pool 4ccbe126... (performance epoch 53, applied at the boundary
// into epoch 55): leader 283404115 and member 634638139 from the canonical
// active stake and a reserves value inside the fitted canonical range.
func TestPrimeMainnetEpoch53CanonicalRewards(t *testing.T) {
	t.Parallel()

	const activeStake = uint64(70370231140939)
	pool, filler, member := primeReferencePool(
		t,
		"4ccbe126e0a6e642cc64c49dd0baaea5ceed27b2a7788b02186b9cb3",
		"f91c40da91fc893b2d0ca89155e3168b4901783c4255341ea8b06f61",
		big.NewRat(1, 20), 250000000, 720002272385, 2272385, 205, 21076,
		activeStake,
	)
	params := Parameters{
		MonetaryExpansion:    big.NewRat(55, 10000),
		TreasuryExpansion:    big.NewRat(1, 1000000),
		Decentralization:     new(big.Rat),
		PledgeInfluence:      new(big.Rat),
		ActiveSlotsCoeff:     big.NewRat(1, 20),
		OptimalPoolCount:     500,
		EpochLength:          432000,
		MaxLovelaceSupply:    3000000000000000,
		ProtocolMajorVersion: 7,
	}
	leader, got := primeReferenceAmounts(
		t,
		Pots{Reserves: 599851994600000, Fees: 38876904},
		pool, filler, activeStake, params, member,
	)
	require.Equal(t, uint64(283404115), leader)
	require.Equal(t, uint64(634638139), got)
}

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
	t.Run(
		"pool reward at or below cost pays all to operator",
		func(t *testing.T) {
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
		},
	)

	t.Run(
		"cost plus margin plus owner share uses exact floor",
		func(t *testing.T) {
			require.Equal(
				t,
				uint64(417),
				requireLeaderReward(
					t,
					1_000,
					340,
					big.NewRat(1, 50),
					100,
					1_000,
				),
			)
		},
	)
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
	require.Equal(
		t,
		uint64(vector.Expected.Incentives),
		result.Incentives,
		source,
	)
	require.Equal(
		t,
		uint64(vector.Expected.TotalRewardPot),
		result.TotalRewardPot,
		source,
	)
	require.Equal(
		t,
		uint64(vector.Expected.TreasuryTax),
		result.TreasuryTax,
		source,
	)
	require.Equal(
		t,
		uint64(vector.Expected.AvailableRewards),
		result.AvailableRewards,
		source,
	)
	require.Equal(
		t,
		uint64(vector.Expected.EffectiveRewards),
		result.EffectiveRewards,
		source,
	)
	require.Equal(
		t,
		uint64(vector.Expected.Undistributed),
		result.Undistributed,
		source,
	)
	require.Equal(
		t,
		vector.Expected.UpdatedPots.toPots(),
		result.UpdatedPots,
		source,
	)
	require.Len(t, result.PoolRewards, len(vector.Expected.PoolRewards), source)
	for i, want := range vector.Expected.PoolRewards {
		got := result.PoolRewards[i]
		require.Equal(t, parsePoolID(t, want.PoolID), got.PoolID, source)
		require.Equal(t, uint64(want.OptimalReward), got.OptimalReward, source)
		require.Equal(t, uint64(want.PoolReward), got.PoolReward, source)
		require.Equal(t, uint64(want.LeaderReward), got.LeaderReward, source)
		require.Equal(
			t,
			uint64(want.MemberRewardTotal),
			got.MemberRewardTotal,
			source,
		)
		require.Equal(t, uint64(want.OwnerStake), got.OwnerStake, source)
		require.Equal(t, uint64(want.Undistributed), got.Undistributed, source)
		require.Equal(t, uint64(want.Unspendable), got.Unspendable, source)
	}
	require.Len(
		t,
		result.AccountRewards,
		len(vector.Expected.AccountRewards),
		source,
	)
	for i, want := range vector.Expected.AccountRewards {
		got := result.AccountRewards[i]
		require.Equal(
			t,
			want.Credential.toCredential(t),
			got.Credential,
			source,
		)
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
		nil,
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
	ret, err := leaderRewardChecked(
		poolReward,
		cost,
		margin,
		ownerStake,
		poolStake,
	)
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
	ret, err := memberRewardChecked(
		poolReward,
		cost,
		margin,
		memberStake,
		poolStake,
	)
	require.NoError(t, err)
	return ret
}

// Mainnet epoch 655 pot and stake state, from dingo #4660.
const (
	shortfallReserves    = uint64(6126859026912852)
	shortfallTreasury    = uint64(1356415272910618)
	shortfallFees        = uint64(32555502387)
	shortfallActiveStake = uint64(21386263525299978)
	shortfallTotalBlocks = uint64(21060)
)

func shortfallParameters() Parameters {
	return Parameters{
		MonetaryExpansion:    big.NewRat(3, 1000),
		TreasuryExpansion:    big.NewRat(2, 10),
		Decentralization:     new(big.Rat),
		PledgeInfluence:      big.NewRat(3, 10),
		ActiveSlotsCoeff:     big.NewRat(5, 100),
		OptimalPoolCount:     500,
		EpochLength:          432000,
		MaxLovelaceSupply:    45000000000000000,
		ProtocolMajorVersion: 10,
	}
}

// shortfallSnapshot builds the three mainnet pools #4660 cross-checked: two
// above k=500 saturation and one comfortably below it, so a lever that acts
// through the saturation cap is distinguishable from one that does not.
func shortfallSnapshot(totalActiveStake, totalBlocks uint64) Snapshot {
	pools := make([]Pool, 0, 3)
	for i, p := range []struct {
		share  float64
		blocks uint64
	}{
		{0.002227, 47},
		{0.002233, 47},
		{0.001628, 34},
	} {
		var id PoolID
		id[0] = byte(i + 1)
		var cred Credential
		cred.Hash[0] = byte(i + 1)
		stake := uint64(float64(totalActiveStake) * p.share)
		pools = append(pools, Pool{
			ID:             id,
			RewardAccount:  cred,
			Margin:         big.NewRat(2, 100),
			Cost:           170000000,
			DelegatedStake: stake,
			BlocksProduced: p.blocks,
			TotalBlocks:    totalBlocks,
			Delegators: []Delegator{{
				Credential: cred,
				Stake:      stake,
				Registered: true,
				Eligible:   true,
			}},
			RewardAccountRegistered: true,
			RewardAccountEligible:   true,
		})
	}
	return Snapshot{Pools: pools, TotalActiveStake: totalActiveStake}
}

func shortfallCalculate(t *testing.T, pots Pots, snapshot Snapshot) *Result {
	t.Helper()
	result, err := Calculate(pots, snapshot, shortfallParameters())
	if err != nil {
		t.Fatalf("calculate rewards: %v", err)
	}
	return result
}

// shortfallPercent returns each pool's reward shortfall against base, in
// percent, positive when the perturbed round under-credits.
func shortfallPercent(base, perturbed *Result) []float64 {
	ret := make([]float64, len(base.PoolRewards))
	for i := range base.PoolRewards {
		want := float64(base.PoolRewards[i].PoolReward)
		got := float64(perturbed.PoolRewards[i].PoolReward)
		ret[i] = (want - got) / want * 100
	}
	return ret
}

// TestTotalBlocksUndercountDoesNotUnderCreditRewards pins the cancellation that
// rules the block count out as the cause of a uniform network-wide reward
// shortfall (dingo #4660, which suspected it). totalBlocks is the numerator of
// the efficiency term that scales the reward pot R and the denominator of every
// pool's beta, so it cancels: undercounting it by 0.135% moves each pool's
// reward by less than 0.001%, and slightly upwards, because only R's fee
// component fails to cancel.
func TestTotalBlocksUndercountDoesNotUnderCreditRewards(t *testing.T) {
	pots := Pots{
		Reserves: shortfallReserves,
		Treasury: shortfallTreasury,
		Fees:     shortfallFees,
	}
	base := shortfallCalculate(
		t, pots, shortfallSnapshot(shortfallActiveStake, shortfallTotalBlocks),
	)
	undercounted := shortfallCalculate(
		t, pots, shortfallSnapshot(shortfallActiveStake, shortfallTotalBlocks-28),
	)
	for i, pct := range shortfallPercent(base, undercounted) {
		if pct > 0.001 || pct < -0.01 {
			t.Errorf(
				"pool %d: totalBlocks undercount moved the reward by %+.5f%%, want no material under-credit",
				i, pct,
			)
		}
	}
}

// TestActiveStakeUndercountUnderCreditsUniformly pins the lever that does
// produce #4660's signature: total active stake is the denominator of sigmaA
// alone, so a shortfall there under-credits every pool by the same percentage
// regardless of pool size or saturation.
func TestActiveStakeUndercountUnderCreditsUniformly(t *testing.T) {
	pots := Pots{
		Reserves: shortfallReserves,
		Treasury: shortfallTreasury,
		Fees:     shortfallFees,
	}
	base := shortfallCalculate(
		t, pots, shortfallSnapshot(shortfallActiveStake, shortfallTotalBlocks),
	)
	short := shortfallActiveStake - shortfallActiveStake/1000
	undercountedSnapshot := shortfallSnapshot(
		shortfallActiveStake, shortfallTotalBlocks,
	)
	undercountedSnapshot.TotalActiveStake = short
	undercounted := shortfallCalculate(t, pots, undercountedSnapshot)
	for i, pct := range shortfallPercent(base, undercounted) {
		if pct < 0.0999 || pct > 0.1001 {
			t.Errorf(
				"pool %d: 0.1%% active-stake shortfall under-credited by %+.5f%%, want 0.1%%",
				i, pct,
			)
		}
	}
}
