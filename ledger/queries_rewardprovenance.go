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
)

// queryShelleyRewardProvenance answers GetRewardProvenance.
//
// The reference node does not report its reward calculation here: consensus
// answers with the second element of the ledger's getRewardProvenance, which
// is the default RewardProvenance whatever the chain state. Every scalar is
// zero, both rationals are 0/1, and the block, pool and desirability maps are
// empty. The reply is the same constant at every point, so the acquired point
// does not matter.
func (ls *LedgerState) queryShelleyRewardProvenance() (any, error) {
	zeroRat := &cbor.Rat{Rat: big.NewRat(0, 1)}
	return []any{[]any{
		uint64(0),     // slots per epoch
		map[any]any{}, // blocks made per pool
		uint64(0),     // maxLL
		uint64(0),     // deltaR1
		uint64(0),     // deltaR2
		uint64(0),     // r
		uint64(0),     // totalStake
		uint64(0),     // blocks count
		zeroRat,       // decentralisation
		uint64(0),     // expected blocks
		zeroRat,       // eta
		uint64(0),     // rPot
		uint64(0),     // deltaT1
		uint64(0),     // activeStake
		map[any]any{}, // per-pool provenance
		map[any]any{}, // desirabilities
	}}, nil
}
