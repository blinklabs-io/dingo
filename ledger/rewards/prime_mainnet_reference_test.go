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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

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
