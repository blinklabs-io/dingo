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

package peergov

import (
	"math"
	"math/bits"
	"math/rand/v2"
)

// minSampleWeight is the weight of an entry with no known stake. It keeps
// unregistered or unknown-stake relays discoverable, just rarely.
const minSampleWeight = 1

// weightedSample picks up to n relays without replacement, each draw
// proportional to the relay's Stake among those not yet picked. A relay with
// zero stake carries minSampleWeight. The result is in selection order, so
// a prefix of it is itself a weighted sample. It returns nil for an empty
// input or n <= 0, and every relay when n covers them all.
//
// A Fenwick tree makes each draw and removal O(log len(relays)), which
// matters because mainnet registers thousands of relays.
func weightedSample(relays []PoolRelay, n int) []PoolRelay {
	if len(relays) == 0 || n <= 0 {
		return nil
	}
	count := len(relays)
	n = min(n, count)

	// Clamp each weight so the running total cannot overflow uint64 no matter
	// how large the registered stake values are.
	limit := uint64(math.MaxUint64) / uint64(count)
	weights := make([]uint64, count)
	tree := make([]uint64, count+1)
	var total uint64
	for i, relay := range relays {
		w := min(max(relay.Stake, minSampleWeight), limit)
		weights[i] = w
		total += w
		tree[i+1] += w
		if parent := (i + 1) + ((i + 1) & -(i + 1)); parent <= count {
			tree[parent] += tree[i+1]
		}
	}

	highBit := 1 << (bits.Len(uint(count)) - 1)
	picked := make([]PoolRelay, 0, n)
	for len(picked) < n {
		//nolint:gosec // relay spread, not security-sensitive
		target := rand.Uint64N(total)
		// Descend to the first index whose prefix sum exceeds target.
		pos := 0
		for step := highBit; step > 0; step >>= 1 {
			next := pos + step
			if next <= count && tree[next] <= target {
				pos = next
				target -= tree[next]
			}
		}
		picked = append(picked, relays[pos])
		total -= weights[pos]
		for i := pos + 1; i <= count; i += i & -i {
			tree[i] -= weights[pos]
		}
	}
	return picked
}
