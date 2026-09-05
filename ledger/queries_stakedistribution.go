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
	"math/big"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// stakeDistributionEntry mirrors the anonymous struct type
// olocalstatequery.StakeDistributionResult.Results is keyed by. Named here
// only for readability inside this file: Go's struct types are structural
// (not nominal) for anonymous struct literals, so a value of this named
// type is directly assignable into that map without any conversion.
type stakeDistributionEntry = struct {
	cbor.StructAsArray
	StakeFraction *cbor.Rat
	VrfHash       ledger.Blake2b256
}

// queryShelleyStakeDistribution answers GetStakeDistribution: the active
// stake distribution across every block-producing pool in the current mark
// snapshot. Unlike GetPoolDistr2 (queryShelleyPoolDistr2), this query has
// no pool filter on the wire, so every pool that holds stake is reported.
//
// Reads the per-pool stake, pool set, and VRF keys from PoolStakeDistribution,
// the same helper queryShelleyPoolDistr2 and the UTxO RPC ReadState handler
// share, so this query cannot report a different snapshot, pool set, or VRF
// key for the same chain than either of those.
//
// It does NOT reuse PoolStakeDistribution's own StakeFraction (taken over
// TotalActiveStake, the sum of delegated stake): a real cardano-node's
// GetStakeDistribution reply uses total circulating supply as its
// denominator instead, confirmed against real cardano-node's raw wire bytes
// -- see totalCirculatingSupply's doc comment (blinklabs-io/dingo#3824) for
// the full story and why GetPoolDistr2 must not make the same change.
func (ls *LedgerState) queryShelleyStakeDistribution() (any, error) {
	dist, err := ls.PoolStakeDistribution(nil)
	if err != nil {
		return nil, err
	}
	circulation := new(big.Int).SetUint64(dist.TotalCirculatingSupply)
	result := olocalstatequery.StakeDistributionResult{
		Results: make(
			map[ledger.PoolId]stakeDistributionEntry,
			len(dist.Pools),
		),
	}
	for _, pool := range dist.Pools {
		fraction := new(big.Rat).SetFrac(
			new(big.Int).SetUint64(pool.Stake),
			circulation,
		)
		result.Results[ledger.PoolId(pool.PoolKeyHash)] = stakeDistributionEntry{
			StakeFraction: &cbor.Rat{Rat: fraction},
			VrfHash:       pool.VrfKeyHash,
		}
	}
	// Client.GetStakeDistribution decodes the wire reply directly into a
	// StakeDistributionResult (client.go's runQuery does a plain cbor.Decode
	// with no extra wrapping), so the one field must be the top-level array
	// element here. Returning the struct itself would let its own
	// StructAsArray encoding nest inside this slice's, producing a spurious
	// extra array layer that no real NtC client can decode.
	return []any{result.Results}, nil
}
