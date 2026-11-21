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

package cardano

import (
	"errors"
	"math"
)

// ImmutableChunkGeometry derives the chunk geometry the way cardano-node lays
// out its ImmutableDB: ten security parameters of slots per chunk, with a
// leading relative slot for an epoch boundary block. The security parameter is
// the Byron genesis k, or the Shelley genesis securityParam when a network has
// no Byron genesis. The embedded preview chunk files (4320 slots, 10 * 432,
// with the boundary slot) and the mainnet and preprod chunk sizes (21600,
// 10 * 2160) follow this rule.
func (c *CardanoNodeConfig) ImmutableChunkGeometry() (
	chunkSlots uint64,
	canContainEBB bool,
	err error,
) {
	if c == nil {
		return 0, false, errors.New("no cardano node config")
	}
	var k uint64
	if byronGenesis := c.ByronGenesis(); byronGenesis != nil {
		if byronGenesis.ProtocolConsts.K > 0 {
			k = uint64(byronGenesis.ProtocolConsts.K) //nolint:gosec
		}
	} else if shelleyGenesis := c.ShelleyGenesis(); shelleyGenesis != nil &&
		shelleyGenesis.SecurityParam > 0 {
		k = uint64(shelleyGenesis.SecurityParam) //nolint:gosec
	}
	if k == 0 || k > math.MaxUint64/10 {
		return 0, false, errors.New("invalid security parameter")
	}
	return k * 10, true, nil
}
