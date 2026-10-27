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
	"context"
	"math/big"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
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
//
// at is Query's pinned point (unpinned = live); PoolStakeDistribution
// resolves it to the historical epoch whose mark snapshot governed that
// slot.
func (ls *LedgerState) queryShelleyPoolDistr2(ctx context.Context, q *olocalstatequery.ShelleyPoolDistr2Query,
	at QueryPoint,
	txn *database.Txn,
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

	dist, err := ls.PoolStakeDistribution(ctx, poolFilter, at, txn)
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
	// Client.GetPoolDistr2 decodes the wire reply into a
	// StakeDistribution2Result, a one-field struct around the distribution,
	// so the reply is a one-element slice holding the distribution's own two
	// fields. Returning the PoolDistr2Result struct itself would let its own
	// StructAsArray encoding nest inside this slice's, producing a spurious
	// extra array layer that no real NtC client can decode.
	return []any{[]any{result.Pools, result.TotalActiveStake}}, nil
}

// queryShelleyPoolDistr answers the legacy GetPoolDistr query. Its distribution
// is the same point-aware stake snapshot as GetPoolDistr2; the older reply
// carries only each pool's fraction and VRF hash.
func (ls *LedgerState) queryShelleyPoolDistr(
	ctx context.Context,
	q *olocalstatequery.ShelleyPoolDistrQuery,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	var requested []ledger.PoolId
	all := len(q.Pools) == 0
	if !all {
		requested = q.Pools[0].Items()
	}
	var poolFilter []lcommon.PoolKeyHash
	if !all {
		poolFilter = make([]lcommon.PoolKeyHash, 0, len(requested))
		for _, poolID := range requested {
			poolFilter = append(poolFilter, lcommon.PoolKeyHash(poolID))
		}
	}
	dist, err := ls.PoolStakeDistribution(ctx, poolFilter, at, txn)
	if err != nil {
		return nil, err
	}
	result := make(map[ledger.PoolId]struct {
		cbor.StructAsArray
		StakeFraction *cbor.Rat
		VrfHash       ledger.Blake2b256
	}, len(dist.Pools))
	for _, pool := range dist.Pools {
		result[ledger.PoolId(pool.PoolKeyHash)] = struct {
			cbor.StructAsArray
			StakeFraction *cbor.Rat
			VrfHash       ledger.Blake2b256
		}{StakeFraction: pool.StakeFraction, VrfHash: pool.VrfKeyHash}
	}
	// PoolDistrResult is a one-field StructAsArray in gOuroboros, matching the
	// result wrapper consumed by the legacy client method.
	return []any{result}, nil
}

// queryShelleyStakeDistribution2 answers GetStakeDistribution2, which replaces
// GetStakeDistribution from node-to-client protocol version 21. Pools, stakes
// and VRF keys come from the same PoolStakeDistribution read as GetPoolDistr2,
// so both agree about the snapshot.
//
// Unlike GetPoolDistr2, each StakeFraction is taken over total circulating
// supply rather than over the sum of delegated stake, matching cardano-node's
// poolsByTotalStakeFraction. The reply's total remains TotalActiveStake.
//
// Client.GetStakeDistribution2 decodes the reply into a StakeDistribution2Result,
// a one-field struct around the distribution, so the result is a one-element
// slice holding the distribution's own two fields.
func (ls *LedgerState) queryShelleyStakeDistribution2(
	ctx context.Context,
	at QueryPoint,
	txn *database.Txn,
) (any, error) {
	dist, err := ls.PoolStakeDistribution(ctx, nil, at, txn)
	if err != nil {
		return nil, err
	}
	pools := make(
		map[ledger.PoolId]olocalstatequery.IndividualPoolStake,
		len(dist.Pools),
	)
	circulation := new(big.Int).SetUint64(dist.TotalCirculatingSupply)
	for _, pool := range dist.Pools {
		fraction := new(big.Rat).SetFrac(
			new(big.Int).SetUint64(pool.Stake),
			circulation,
		)
		pools[ledger.PoolId(pool.PoolKeyHash)] = olocalstatequery.IndividualPoolStake{
			StakeFraction:  &cbor.Rat{Rat: fraction},
			TotalPoolStake: pool.Stake,
			VrfHash:        pool.VrfKeyHash,
		}
	}
	return []any{[]any{pools, dist.TotalActiveStake}}, nil
}
