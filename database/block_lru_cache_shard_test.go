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

package database

import (
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockLRUShardCount(t *testing.T) {
	tests := []struct {
		maxEntries int
		want       int
	}{
		// Small caches stay single-shard so global-LRU semantics are exact.
		{maxEntries: -5, want: 1},
		{maxEntries: 0, want: 1},
		{maxEntries: 1, want: 1},
		{maxEntries: 3, want: 1},
		{maxEntries: blockLRUMinEntriesPerShard, want: 1},
		// Once capacity comfortably exceeds the per-shard minimum, shard.
		{maxEntries: 32, want: 2},
		{maxEntries: 100, want: 4},
		{maxEntries: 500, want: 16}, // production default
		// Large caches saturate at the shard cap.
		{maxEntries: 100000, want: blockLRUMaxShards},
	}
	for _, tt := range tests {
		got := blockLRUShardCount(tt.maxEntries)
		assert.Equal(
			t,
			tt.want,
			got,
			"blockLRUShardCount(%d)",
			tt.maxEntries,
		)
		// Must always be a power of two and at least 1.
		require.GreaterOrEqual(t, got, 1)
		assert.Equal(
			t,
			1,
			bits.OnesCount(uint(got)),
			"shard count %d must be a power of two",
			got,
		)
		assert.LessOrEqual(t, got, blockLRUMaxShards)
	}
}

func TestBlockLRUShardCapacities(t *testing.T) {
	tests := []struct {
		maxEntries int
		shardCount int
	}{
		{maxEntries: 0, shardCount: 1},
		{maxEntries: 3, shardCount: 1},
		{maxEntries: 500, shardCount: 16},
		{maxEntries: 501, shardCount: 16}, // not evenly divisible
		{maxEntries: 1000, shardCount: 64},
	}
	for _, tt := range tests {
		caps := blockLRUShardCapacities(tt.maxEntries, tt.shardCount)

		// One capacity per shard.
		require.Len(t, caps, tt.shardCount)

		// The per-shard capacities must sum to exactly the requested total,
		// so the sharded cache never holds more than maxEntries blocks.
		sum := 0
		minCap, maxCap := caps[0], caps[0]
		for _, c := range caps {
			assert.GreaterOrEqual(t, c, 0)
			sum += c
			minCap = min(minCap, c)
			maxCap = max(maxCap, c)
		}
		assert.Equal(
			t,
			tt.maxEntries,
			sum,
			"capacities must sum to maxEntries (%d across %d shards)",
			tt.maxEntries,
			tt.shardCount,
		)

		// Distribution must be even to within one entry per shard.
		assert.LessOrEqual(
			t,
			maxCap-minCap,
			1,
			"capacities must be balanced within 1 entry",
		)
	}
}

// totalEntries counts cached blocks across all shards (white-box helper).
func (c *BlockLRUCache) totalEntries() int {
	n := 0
	for _, s := range c.shards {
		s.mu.Lock()
		n += len(s.cache)
		s.mu.Unlock()
	}
	return n
}

func TestBlockLRUCacheShardsScaleWithCapacity(t *testing.T) {
	// Small caches stay single-shard (exact global LRU, no overhead).
	assert.Len(t, NewBlockLRUCache(3).shards, 1)
	// The production default capacity shards.
	assert.Len(t, NewBlockLRUCache(500).shards, 16)
}

func TestBlockLRUCacheTotalCapacityBounded(t *testing.T) {
	const maxEntries = 500
	cache := NewBlockLRUCache(maxEntries)

	// Flood with far more distinct keys than capacity, spread across shards.
	for i := range maxEntries * 20 {
		cache.Put(uint64(i), benchBlockKeyHash(i), &CachedBlock{
			RawBytes: []byte{byte(i)},
		})
	}

	// Sharding makes eviction per-shard, but the aggregate must never exceed
	// the configured capacity.
	assert.LessOrEqual(
		t,
		cache.totalEntries(),
		maxEntries,
		"total cached blocks must not exceed maxEntries",
	)
}
