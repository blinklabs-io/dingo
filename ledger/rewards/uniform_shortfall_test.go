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
)

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
