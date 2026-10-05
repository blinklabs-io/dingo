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

// weightedSample picks up to n relays without replacement. Each round draws
// pools without replacement in proportion to their delegated stake and picks
// one relay from each pool. Further relays from a pool become eligible only
// after every other pool with a relay has had the same opportunity. Relays
// without a pool identity are treated independently for compatibility with
// non-ledger providers and restored peer snapshots.
//
// A Fenwick tree makes each draw and removal O(log len(relays)), which
// matters because mainnet registers thousands of relays.
func weightedSample(relays []PoolRelay, n int) []PoolRelay {
	if len(relays) == 0 || n <= 0 {
		return nil
	}
	n = min(n, len(relays))
	groups := groupRelaysByPool(relays)
	picked := make([]PoolRelay, 0, n)
	for len(picked) < n {
		for _, groupIndex := range weightedPoolOrder(groups) {
			group := &groups[groupIndex]
			//nolint:gosec // relay spread within one authenticated pool identity
			relayIndex := int(rand.Uint64N(uint64(len(group.relays))))
			picked = append(picked, group.relays[relayIndex])
			group.relays[relayIndex] = group.relays[len(group.relays)-1]
			group.relays = group.relays[:len(group.relays)-1]
			if len(picked) == n {
				return picked
			}
		}
	}
	return picked
}

type relayPoolGroup struct {
	stake  uint64
	relays []PoolRelay
}

func groupRelaysByPool(relays []PoolRelay) []relayPoolGroup {
	groups := make([]relayPoolGroup, 0, len(relays))
	byPool := make(map[string]int, len(relays))
	for _, relay := range relays {
		if len(relay.PoolKeyHash) == 0 {
			groups = append(groups, relayPoolGroup{
				stake: relay.Stake, relays: []PoolRelay{relay},
			})
			continue
		}
		key := string(relay.PoolKeyHash)
		if index, ok := byPool[key]; ok {
			groups[index].relays = append(groups[index].relays, relay)
			continue
		}
		byPool[key] = len(groups)
		groups = append(groups, relayPoolGroup{
			stake: relay.Stake, relays: []PoolRelay{relay},
		})
	}
	return groups
}

// weightedPoolOrder returns every non-empty group once in stake-weighted
// order. Calling it again forms the next round for groups with relays left.
func weightedPoolOrder(groups []relayPoolGroup) []int {
	active := make([]int, 0, len(groups))
	for i := range groups {
		if len(groups[i].relays) > 0 {
			active = append(active, i)
		}
	}
	count := len(active)
	if count == 0 {
		return nil
	}

	// Keep exact weights whenever their sum fits; otherwise scale every
	// weight together, preserving proportions rather than clipping whales.
	weights := make([]uint64, count)
	tree := make([]uint64, count+1)
	var total uint64
	for shift := uint(0); ; shift++ {
		total = 0
		overflow := false
		for i, groupIndex := range active {
			w := max(groups[groupIndex].stake>>shift, minSampleWeight)
			if math.MaxUint64-total < w {
				overflow = true
				break
			}
			weights[i] = w
			total += w
		}
		if !overflow {
			break
		}
	}
	for i, w := range weights {
		tree[i+1] += w
		if parent := (i + 1) + ((i + 1) & -(i + 1)); parent <= count {
			tree[parent] += tree[i+1]
		}
	}

	highBit := 1 << (bits.Len(uint(count)) - 1)
	ordered := make([]int, 0, count)
	for len(ordered) < count {
		//nolint:gosec // stake-weighted peer spread, not a secret draw
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
		ordered = append(ordered, active[pos])
		total -= weights[pos]
		for i := pos + 1; i <= count; i += i & -i {
			tree[i] -= weights[pos]
		}
	}
	return ordered
}
