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

package ledger

import (
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// queryShelleyPoolDistr2 answers GetPoolDistr2, the stake distribution across
// block-producing pools.
//
// cardano-cli sends this while computing a leadership schedule, having chosen
// it over the deprecated GetPoolDistr once node-to-client protocol version 21
// is negotiated.
//
// The distribution itself is read by PoolStakeDistribution, which the UTxO RPC
// ReadState handler shares; this function is only the adaptation of that result
// into the node-to-client reply shape. Keeping the read in one place is what
// stops the two surfaces reporting different VRF keys or different snapshots
// for the same chain.
func (ls *LedgerState) queryShelleyPoolDistr2(
	q *olocalstatequery.ShelleyPoolDistr2Query,
) (any, error) {
	// PoolFilter reports all=true with a nil pool list when the query's
	// StrictMaybe was SNothing, and all=false otherwise -- including for an
	// explicit SJust of the empty set, which asks for no pools rather than
	// every pool. PoolStakeDistribution draws that same distinction between a
	// nil filter and an empty non-nil one, so the filter is built as a non-nil
	// slice whenever all is false.
	requested, all := q.PoolFilter()
	var poolFilter []lcommon.PoolKeyHash
	if !all {
		poolFilter = make([]lcommon.PoolKeyHash, 0, len(requested))
		for _, poolId := range requested {
			poolFilter = append(poolFilter, lcommon.PoolKeyHash(poolId))
		}
	}

	dist, err := ls.PoolStakeDistribution(poolFilter)
	if err != nil {
		return nil, err
	}

	result := olocalstatequery.PoolDistr2Result{
		Pools: make(
			map[ledger.PoolId]olocalstatequery.PoolDistr2IndividualStake,
			len(dist.Pools),
		),
		TotalActiveStake: dist.TotalActiveStake,
	}
	for _, pool := range dist.Pools {
		result.Pools[ledger.PoolId(pool.PoolKeyHash)] = olocalstatequery.PoolDistr2IndividualStake{
			StakeFraction:  pool.StakeFraction,
			TotalPoolStake: pool.Stake,
			VrfHash:        pool.VrfKeyHash,
		}
	}
	return []any{result}, nil
}
