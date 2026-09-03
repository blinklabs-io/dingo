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
// Reads from PoolStakeDistribution, the same helper queryShelleyPoolDistr2
// and the UTxO RPC ReadState handler share, so this query cannot report a
// different snapshot or VRF key for the same chain than either of those --
// which is exactly what lets the devnet cross-node ledger-state comparison
// (internal/test/devnet, blinklabs-io/dingo#1900) trust this leaf as a
// faithful reflection of Dingo's own leadership-election view.
func (ls *LedgerState) queryShelleyStakeDistribution() (any, error) {
	dist, err := ls.PoolStakeDistribution(nil)
	if err != nil {
		return nil, err
	}
	result := olocalstatequery.StakeDistributionResult{
		Results: make(
			map[ledger.PoolId]stakeDistributionEntry,
			len(dist.Pools),
		),
	}
	for _, pool := range dist.Pools {
		result.Results[ledger.PoolId(pool.PoolKeyHash)] = stakeDistributionEntry{
			StakeFraction: pool.StakeFraction,
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
