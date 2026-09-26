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

// Expected values are cardano-ledger maxPool' (Cardano.Ledger.State.SnapShots)
// evaluated over exact rationals:
//
//	sigma' = min(L*pR, min(sigma, z0))   -- the L term only when L is set
//	p'     = min(pR, z0)                 -- never capped by L
//	maxP   = floor(R/(1+a0) * (sigma' + p'*a0*(sigma' - p'*(z0-sigma')/z0)/z0))
//
// Boundary case, z0 = 1/2, sigma = 3/5, pR = 3/10, a0 = 1/2, L = 1/2:
// sigma' = 3/20, p' = 3/10, (z0-sigma')/z0 = 7/10,
// (sigma' - p'*7/10)/z0 = -3/25, sigma' + p'*a0*(-3/25) = 33/250.
// R = 1000 gives 1000*(2/3)*(33/250) = 88 exactly; R = 999 gives
// 87.912, which floors to 87.
func TestHaskellReferenceMaxPoolPledgeLeverage(t *testing.T) {
	const (
		mainnetR       = 10_000_000_000_000
		mainnetTotal   = 34_000_000_000_000_000
		mainnetStake   = 60_000_000_000_000
		mainnetPledge  = 1_000_000_000_000
		mainnetNOpt    = 500
		boundaryTotal  = 100
		boundaryStake  = 60
		boundaryPledge = 30
		boundaryNOpt   = 2
	)
	tests := []struct {
		name     string
		r        uint64
		nOpt     uint64
		a0       *big.Rat
		stake    uint64
		pledge   uint64
		total    uint64
		leverage *big.Rat
		want     uint64
	}{
		{
			name: "boundary: exact integer is not rounded down",
			r:    1_000, nOpt: boundaryNOpt, a0: big.NewRat(1, 2),
			stake: boundaryStake, pledge: boundaryPledge, total: boundaryTotal,
			leverage: big.NewRat(1, 2),
			want:     88,
		},
		{
			name: "boundary: one lovelace less floors to the integer below",
			r:    999, nOpt: boundaryNOpt, a0: big.NewRat(1, 2),
			stake: boundaryStake, pledge: boundaryPledge, total: boundaryTotal,
			leverage: big.NewRat(1, 2),
			want:     87,
		},
		{
			name: "boundary: unset leverage",
			r:    1_000, nOpt: boundaryNOpt, a0: big.NewRat(1, 2),
			stake: boundaryStake, pledge: boundaryPledge, total: boundaryTotal,
			want: 433,
		},
		{
			name: "mainnet scale: unset leverage",
			r:    mainnetR, nOpt: mainnetNOpt, a0: big.NewRat(3, 10),
			stake: mainnetStake, pledge: mainnetPledge, total: mainnetTotal,
			want: 13_634_431_414,
		},
		{
			name: "mainnet scale: non-binding leverage equals unset",
			r:    mainnetR, nOpt: mainnetNOpt, a0: big.NewRat(3, 10),
			stake: mainnetStake, pledge: mainnetPledge, total: mainnetTotal,
			leverage: big.NewRat(20_000, 1),
			want:     13_634_431_414,
		},
		{
			name: "mainnet scale: binding integer leverage",
			r:    mainnetR, nOpt: mainnetNOpt, a0: big.NewRat(3, 10),
			stake: mainnetStake, pledge: mainnetPledge, total: mainnetTotal,
			leverage: big.NewRat(10, 1),
			want:     2_271_573_455,
		},
		{
			name: "mainnet scale: fractional leverage below one",
			r:    mainnetR, nOpt: mainnetNOpt, a0: big.NewRat(3, 10),
			stake: mainnetStake, pledge: mainnetPledge, total: mainnetTotal,
			leverage: big.NewRat(1, 2),
			want:     112_630_442,
		},
		{
			name: "zero leverage without pledge influence",
			r:    mainnetR, nOpt: mainnetNOpt, a0: big.NewRat(0, 1),
			stake: mainnetStake, pledge: mainnetPledge, total: mainnetTotal,
			leverage: big.NewRat(0, 1),
			want:     0,
		},
		{
			name: "zero pledge with a binding leverage",
			r:    mainnetR, nOpt: mainnetNOpt, a0: big.NewRat(3, 10),
			stake: mainnetStake, pledge: 0, total: mainnetTotal,
			leverage: big.NewRat(10, 1),
			want:     0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := optimalPoolRewardChecked(
				tc.r,
				tc.nOpt,
				tc.a0,
				tc.stake,
				tc.pledge,
				tc.total,
				tc.leverage,
			)
			require.NoError(t, err)
			require.Equal(t, tc.want, got, "maxPool' = %d, want %d", got, tc.want)
		})
	}
}

// The pool pot, leader and member split for a mainnet-scale pool under a
// binding L = 10, against mkPoolRewardInfo, calcStakePoolOperatorReward and
// calcStakePoolMemberReward (Cardano.Ledger.Shelley.Rewards) evaluated over
// exact rationals. Apparent performance is 1: one block of one, d = 0, and the
// pool holds all active stake.
func TestHaskellReferencePledgeLeveragePoolSplit(t *testing.T) {
	const (
		poolStake   = 60_000_000_000_000
		pledge      = 1_000_000_000_000
		memberStake = poolStake - pledge
		cost        = 170_000_000
	)
	margin := big.NewRat(1, 100)
	params := Parameters{
		Decentralization:      big.NewRat(0, 1),
		PledgeInfluence:       big.NewRat(3, 10),
		OptimalPoolCount:      500,
		PledgeLeverageEnabled: true,
		PledgeLeverage:        big.NewRat(10, 1),
	}
	got, err := CalculatePoolReward(
		Pool{
			ID:             testPoolID(1),
			Margin:         margin,
			Pledge:         pledge,
			Cost:           cost,
			DelegatedStake: poolStake,
			OwnerStake:     pledge,
			BlocksProduced: 1,
			TotalBlocks:    1,
		},
		10_000_000_000_000,
		poolStake,
		34_000_000_000_000_000,
		1,
		params,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2_271_573_455), got.OptimalReward)
	require.Equal(t, uint64(2_271_573_455), got.PoolReward)
	require.Equal(t, uint64(225_691_696), got.LeaderReward)
	member, err := memberRewardChecked(
		got.PoolReward, cost, margin, memberStake, poolStake,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2_045_881_758), member)
}
