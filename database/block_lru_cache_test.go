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
	"encoding/binary"
	"math/bits"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// benchBlockKeyHash builds a 32-byte block hash whose leading bytes encode the
// given index. The leading bytes drive shard selection, so encoding the index
// here spreads the working set uniformly across shards once the cache is
// sharded.
func benchBlockKeyHash(idx int) [32]byte {
	var h [32]byte
	binary.LittleEndian.PutUint64(h[:8], uint64(idx))
	return h
}

// benchmarkBlockLRUParallel exercises the cache from many goroutines over a
// working set that fits within capacity (so reads hit). It mirrors the
// methodology of https://strebkov.dev/posts/shard-your-locks/ : a fixed key
// space hammered concurrently, with a tunable read/write mix. Note that the
// hot path Get also mutates the LRU ordering (MoveToFront), so even reads
// contend on the lock — the case where a single mutex scales backwards.
//
// Run with -cpu=1,4,8 to see the scaling curve.
func benchmarkBlockLRUParallel(b *testing.B, workingSet int, writeEvery int) {
	cache := NewBlockLRUCache(workingSet)

	// Pre-populate the full working set so reads are hits.
	for i := range workingSet {
		cache.Put(uint64(i), benchBlockKeyHash(i), &CachedBlock{
			RawBytes: make([]byte, 256),
		})
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Per-goroutine counter avoids shared RNG state; the stride keeps
		// successive ops landing on different keys/shards.
		i := 0
		for pb.Next() {
			idx := int((int64(i) * 2654435761) % int64(workingSet))
			if idx < 0 {
				idx += workingSet
			}
			slot := uint64(idx)
			hash := benchBlockKeyHash(idx)
			if writeEvery > 0 && i%writeEvery == 0 {
				cache.Put(slot, hash, &CachedBlock{RawBytes: make([]byte, 256)})
			} else {
				cache.Get(slot, hash)
			}
			i++
		}
	})
}

// BenchmarkBlockLRUParallelReadHeavy is ~90% Get / ~10% Put.
func BenchmarkBlockLRUParallelReadHeavy(b *testing.B) {
	benchmarkBlockLRUParallel(b, 500, 10)
}

// BenchmarkBlockLRUParallelBalanced is ~50% Get / ~50% Put.
func BenchmarkBlockLRUParallelBalanced(b *testing.B) {
	benchmarkBlockLRUParallel(b, 500, 2)
}

// BenchmarkBlockLRUParallelReadOnly is pure Get (still mutates LRU order).
func BenchmarkBlockLRUParallelReadOnly(b *testing.B) {
	benchmarkBlockLRUParallel(b, 500, 0)
}

func TestBlockLRUShardCount(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	// Small caches stay single-shard (exact global LRU, no overhead).
	assert.Len(t, NewBlockLRUCache(3).shards, 1)
	// The production default capacity shards.
	assert.Len(t, NewBlockLRUCache(500).shards, 16)
}

func TestBlockLRUCacheTotalCapacityBounded(t *testing.T) {
	t.Parallel()

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

func TestBlockLRUCacheGetPut(t *testing.T) {
	t.Parallel()

	cache := NewBlockLRUCache(10)
	require.NotNil(t, cache)

	// Create a test block
	hash1 := [32]byte{1, 2, 3}
	block1 := &CachedBlock{
		RawBytes: []byte("test block data"),
		TxIndex: map[[32]byte]Location{
			{0xaa}: {Offset: 0, Length: 5},
		},
		OutputIndex: map[OutputKey]Location{
			{TxIndex: 0, OutputIndex: 0}: {Offset: 5, Length: 10},
		},
	}

	// Test Put and Get
	cache.Put(100, hash1, block1)

	got, ok := cache.Get(100, hash1)
	require.True(t, ok)
	assert.Equal(t, block1.RawBytes, got.RawBytes)
	assert.Equal(t, block1.TxIndex, got.TxIndex)
	assert.Equal(t, block1.OutputIndex, got.OutputIndex)

	// Test Get for non-existent block
	hash2 := [32]byte{4, 5, 6}
	got, ok = cache.Get(100, hash2)
	assert.False(t, ok)
	assert.Nil(t, got)

	// Test Get with wrong slot but correct hash
	got, ok = cache.Get(101, hash1)
	assert.False(t, ok)
	assert.Nil(t, got)

	// Test overwriting existing entry
	block1Updated := &CachedBlock{
		RawBytes: []byte("updated block data"),
		TxIndex:  map[[32]byte]Location{},
	}
	cache.Put(100, hash1, block1Updated)

	got, ok = cache.Get(100, hash1)
	require.True(t, ok)
	assert.Equal(t, block1Updated.RawBytes, got.RawBytes)
}

func TestBlockLRUCacheEviction(t *testing.T) {
	t.Parallel()

	cache := NewBlockLRUCache(3)

	// Add 3 blocks - LRU order after: 3, 2, 1 (3 is most recent)
	hash1 := [32]byte{1}
	hash2 := [32]byte{2}
	hash3 := [32]byte{3}

	cache.Put(1, hash1, &CachedBlock{RawBytes: []byte("block1")})
	cache.Put(2, hash2, &CachedBlock{RawBytes: []byte("block2")})
	cache.Put(3, hash3, &CachedBlock{RawBytes: []byte("block3")})

	// Add a 4th block - should evict block1 (least recently used)
	// LRU order after: 4, 3, 2
	hash4 := [32]byte{4}
	cache.Put(4, hash4, &CachedBlock{RawBytes: []byte("block4")})

	// block1 should be evicted
	_, ok := cache.Get(1, hash1)
	assert.False(t, ok, "block1 should have been evicted")

	// block2, block3, block4 should still be present
	// Note: these Gets change LRU order to: 4, 3, 2 -> 2, 4, 3 -> 3, 2, 4 -> 4, 3, 2
	_, ok = cache.Get(2, hash2)
	assert.True(t, ok, "block2 should still be present")
	_, ok = cache.Get(3, hash3)
	assert.True(t, ok, "block3 should still be present")
	_, ok = cache.Get(4, hash4)
	assert.True(t, ok, "block4 should still be present")
	// After these gets, LRU order is: 4, 3, 2 (4 most recent, 2 least recent)

	// Add another block - should evict block2 (least recently used)
	hash5 := [32]byte{5}
	cache.Put(5, hash5, &CachedBlock{RawBytes: []byte("block5")})

	_, ok = cache.Get(2, hash2)
	assert.False(t, ok, "block2 should have been evicted")

	// block3, block4, block5 should still be present
	_, ok = cache.Get(3, hash3)
	assert.True(t, ok, "block3 should still be present")
	_, ok = cache.Get(4, hash4)
	assert.True(t, ok, "block4 should still be present")
	_, ok = cache.Get(5, hash5)
	assert.True(t, ok, "block5 should still be present")
}

func TestCachedBlockExtract(t *testing.T) {
	t.Parallel()

	block := &CachedBlock{
		RawBytes: []byte("0123456789abcdef"),
	}

	// Test normal extraction
	result := block.Extract(0, 5)
	assert.Equal(t, []byte("01234"), result)

	// Test extraction from middle
	result = block.Extract(5, 5)
	assert.Equal(t, []byte("56789"), result)

	// Test extraction at end
	result = block.Extract(10, 6)
	assert.Equal(t, []byte("abcdef"), result)

	// Test single byte extraction
	result = block.Extract(0, 1)
	assert.Equal(t, []byte("0"), result)

	// Test zero length extraction
	result = block.Extract(5, 0)
	assert.Equal(t, []byte{}, result)

	// Test extraction beyond bounds returns nil
	result = block.Extract(20, 5)
	assert.Nil(t, result)

	// Test extraction that would overflow returns nil
	result = block.Extract(14, 5)
	assert.Nil(t, result)
}

func TestCachedBlockExtractReturnsCopy(t *testing.T) {
	t.Parallel()

	original := []byte("ABCDEFGHIJKLMNOP")
	block := &CachedBlock{
		RawBytes: make([]byte, len(original)),
	}
	copy(block.RawBytes, original)

	// Extract a range from the middle
	extracted := block.Extract(4, 4)
	require.NotNil(t, extracted)
	require.Equal(t, []byte("EFGH"), extracted)

	// Mutate the returned slice
	extracted[0] = 'X'
	extracted[1] = 'Y'
	extracted[2] = 'Z'
	extracted[3] = 'W'

	// Extract the same range again and verify the cached data is unchanged
	extractedAgain := block.Extract(4, 4)
	require.NotNil(t, extractedAgain)
	assert.Equal(
		t,
		[]byte("EFGH"),
		extractedAgain,
		"cached block data must not be corrupted by caller mutations",
	)

	// Also verify the full RawBytes are intact
	assert.Equal(t, original, block.RawBytes)
}

func TestBlockLRUCacheConcurrent(t *testing.T) {
	t.Parallel()

	cache := NewBlockLRUCache(100)
	var wg sync.WaitGroup
	numGoroutines := 50
	numOperations := 100

	// Spawn multiple goroutines doing concurrent puts and gets
	for i := range numGoroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range numOperations {
				slot := uint64((id*numOperations + j) % 200)
				hash := [32]byte{byte(id), byte(j)}

				// Put
				block := &CachedBlock{
					RawBytes: []byte{byte(id), byte(j)},
					TxIndex:  map[[32]byte]Location{},
				}
				cache.Put(slot, hash, block)

				// Get
				_, _ = cache.Get(slot, hash)
			}
		}(i)
	}

	wg.Wait()

	// Verify cache is still functional after concurrent access
	hash := [32]byte{0xff}
	block := &CachedBlock{RawBytes: []byte("final")}
	cache.Put(999, hash, block)

	got, ok := cache.Get(999, hash)
	assert.True(t, ok)
	assert.Equal(t, block.RawBytes, got.RawBytes)
}

func TestBlockLRUCacheLRUOrdering(t *testing.T) {
	t.Parallel()

	cache := NewBlockLRUCache(3)

	hash1 := [32]byte{1}
	hash2 := [32]byte{2}
	hash3 := [32]byte{3}

	// Add blocks in order: 1, 2, 3
	cache.Put(1, hash1, &CachedBlock{RawBytes: []byte("block1")})
	cache.Put(2, hash2, &CachedBlock{RawBytes: []byte("block2")})
	cache.Put(3, hash3, &CachedBlock{RawBytes: []byte("block3")})

	// Access order: 3, 1, 2
	// This makes LRU order (most to least recent): 2, 1, 3
	cache.Get(3, hash3)
	cache.Get(1, hash1)
	cache.Get(2, hash2)

	// Adding a new block should evict block3 (least recently used)
	hash4 := [32]byte{4}
	cache.Put(4, hash4, &CachedBlock{RawBytes: []byte("block4")})

	_, ok := cache.Get(3, hash3)
	assert.False(t, ok, "block3 should have been evicted as LRU")

	// Others should still be present
	_, ok = cache.Get(1, hash1)
	assert.True(t, ok)
	_, ok = cache.Get(2, hash2)
	assert.True(t, ok)
	_, ok = cache.Get(4, hash4)
	assert.True(t, ok)
}
