// Copyright 2025 Blink Labs Software
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
	"container/list"
	"encoding/binary"
	"sync"
)

// Location represents a byte range within the block's raw CBOR data.
type Location struct {
	Offset uint32
	Length uint32
}

// OutputKey uniquely identifies a transaction output within a block.
type OutputKey struct {
	TxIndex     uint16
	OutputIndex uint16
}

// CachedBlock holds a block's raw CBOR data along with pre-computed indexes
// for fast extraction of transactions and UTxO outputs.
type CachedBlock struct {
	// RawBytes contains the block's raw CBOR data.
	RawBytes []byte
	// TxIndex maps transaction hashes to their location in RawBytes.
	TxIndex map[[32]byte]Location
	// OutputIndex maps UTxO output keys to their location in RawBytes.
	OutputIndex map[OutputKey]Location
}

// Extract returns a copy of RawBytes from offset to offset+length.
// Returns nil if the range is out of bounds.
// The returned slice is a defensive copy so callers may freely modify it
// without corrupting the cached block data.
func (cb *CachedBlock) Extract(offset, length uint32) []byte {
	dataLen := len(cb.RawBytes)
	// Check offset doesn't exceed data length
	if uint64(offset) > uint64(dataLen) {
		return nil
	}
	// Check end doesn't exceed data length (using uint64 to avoid overflow)
	end := uint64(offset) + uint64(length)
	if end > uint64(dataLen) {
		return nil
	}
	result := make([]byte, length)
	copy(result, cb.RawBytes[offset:offset+length])
	return result
}

// blockKey is the composite key for cache entries.
type blockKey struct {
	slot uint64
	hash [32]byte
}

// cacheEntry holds a cached block and its key for LRU tracking.
type cacheEntry struct {
	key   blockKey
	block *CachedBlock
}

// Lock-striping parameters for BlockLRUCache. See
// https://strebkov.dev/posts/shard-your-locks/ : a single mutex around a map +
// LRU list scales backwards under concurrent access because Get itself mutates
// the list (MoveToFront), so even reads serialize on one lock and its cache
// line ping-pongs between cores. Splitting the cache into independent shards,
// each with its own lock, drops contention by roughly the shard count.
const (
	// blockLRUMaxShards bounds the number of lock stripes. It is sized to
	// exceed the realistic count of concurrent accessors (API request handlers
	// plus the validation path) while keeping each shard's LRU large enough to
	// be useful. The blog's shard sweep shows steeply diminishing returns once
	// the stripe count comfortably exceeds the core/goroutine count, so there
	// is no need to chase its 256 (which targeted a million-entry cache).
	blockLRUMaxShards = 64

	// blockLRUMinEntriesPerShard is the smallest per-shard capacity we are
	// willing to create. Sharding splits one global LRU into N independent
	// LRUs, so too-small shards would evict useful blocks and tank the hit
	// rate. Below this threshold we use fewer shards (down to a single shard,
	// which preserves exact global-LRU behavior and adds no overhead).
	blockLRUMinEntriesPerShard = 16
)

// blockLRUShardCount picks a power-of-two shard count for a cache of the given
// total capacity. Tiny caches get a single shard (exact global LRU, zero
// sharding overhead); capacity is sharded only once each shard can still hold
// at least blockLRUMinEntriesPerShard blocks, up to blockLRUMaxShards.
func blockLRUShardCount(maxEntries int) int {
	if maxEntries < 0 {
		maxEntries = 0
	}
	n := 1
	for n < blockLRUMaxShards && maxEntries/(n*2) >= blockLRUMinEntriesPerShard {
		n *= 2
	}
	return n
}

// blockLRUShardCapacities splits maxEntries across shardCount shards so the
// per-shard capacities sum to exactly maxEntries (the cache as a whole never
// holds more than maxEntries blocks). The remainder is spread one-per-shard
// across the leading shards, keeping all shards within one entry of each other.
func blockLRUShardCapacities(maxEntries, shardCount int) []int {
	if maxEntries < 0 {
		maxEntries = 0
	}
	capacities := make([]int, shardCount)
	base := maxEntries / shardCount
	rem := maxEntries % shardCount
	for i := range capacities {
		capacities[i] = base
		if i < rem {
			capacities[i]++
		}
	}
	return capacities
}

// blockLRUShard is a single lock stripe: an independent map + LRU list guarded
// by its own mutex. Shards are heap-allocated separately (the parent holds
// pointers) so adjacent shards' mutexes do not share a cache line, avoiding
// false sharing when different shards are locked concurrently.
type blockLRUShard struct {
	mu         sync.Mutex
	maxEntries int
	cache      map[blockKey]*list.Element
	lruList    *list.List
}

// get retrieves a block from the shard, moving it to the front of the shard's
// LRU list on a hit.
func (s *blockLRUShard) get(key blockKey) (*CachedBlock, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	elem, ok := s.cache[key]
	if !ok {
		return nil, false
	}

	// Move to front (most recently used)
	s.lruList.MoveToFront(elem)

	entry := elem.Value.(*cacheEntry)
	return entry.block, true
}

// put adds or updates a block in the shard, evicting the least recently used
// entry if the shard is over capacity.
func (s *blockLRUShard) put(key blockKey, block *CachedBlock) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if entry already exists
	if elem, ok := s.cache[key]; ok {
		// Update existing entry
		s.lruList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.block = block
		return
	}

	// Add new entry
	entry := &cacheEntry{key: key, block: block}
	elem := s.lruList.PushFront(entry)
	s.cache[key] = elem

	// Evict if over capacity
	for s.lruList.Len() > s.maxEntries {
		s.evictOldest()
	}
}

// evictOldest removes the least recently used entry from the shard.
// Must be called with the shard mutex held.
func (s *blockLRUShard) evictOldest() {
	elem := s.lruList.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*cacheEntry)
	delete(s.cache, entry.key)
	s.lruList.Remove(elem)
}

// BlockLRUCache is a thread-safe, lock-striped LRU cache for recently accessed
// blocks. Blocks are keyed by (slot, hash) and routed to one of N independent
// shards by the block hash, so operations on different blocks rarely contend on
// the same lock. Each shard maintains its own LRU list and capacity; eviction
// is therefore per-shard rather than strictly global (the standard trade-off
// for a sharded cache). The total number of cached blocks never exceeds the
// configured maxEntries.
type BlockLRUCache struct {
	shardMask uint64
	shards    []*blockLRUShard
}

// NewBlockLRUCache creates a new BlockLRUCache with the specified maximum
// number of entries. If maxEntries is negative, it is treated as zero
// (cache disabled). The cache transparently shards its internal storage to
// reduce lock contention; the shard count is derived from maxEntries (small
// caches use a single shard, preserving exact global-LRU behavior).
func NewBlockLRUCache(maxEntries int) *BlockLRUCache {
	if maxEntries < 0 {
		maxEntries = 0
	}
	shardCount := blockLRUShardCount(maxEntries)
	capacities := blockLRUShardCapacities(maxEntries, shardCount)
	shards := make([]*blockLRUShard, shardCount)
	for i := range shards {
		shards[i] = &blockLRUShard{
			maxEntries: capacities[i],
			cache:      make(map[blockKey]*list.Element),
			lruList:    list.New(),
		}
	}
	return &BlockLRUCache{
		// shardCount is always a positive power of two (>=1), so shardCount-1
		// is non-negative and fits in uint64.
		shardMask: uint64(shardCount - 1), //nolint:gosec // shardCount >= 1
		shards:    shards,
	}
}

// shardFor selects the shard for a block hash. The hash is a uniformly
// distributed cryptographic digest, so its low bits make a good shard index
// with no additional hashing. shardMask is shardCount-1 (shardCount is always a
// power of two), so this masks rather than divides.
func (c *BlockLRUCache) shardFor(hash [32]byte) *blockLRUShard {
	idx := binary.LittleEndian.Uint64(hash[:8]) & c.shardMask
	return c.shards[idx]
}

// Get retrieves a cached block by slot and hash.
// Returns the block and true if found, or nil and false if not found.
// Accessing a block moves it to the front of its shard's LRU list.
func (c *BlockLRUCache) Get(slot uint64, hash [32]byte) (*CachedBlock, bool) {
	return c.shardFor(hash).get(blockKey{slot: slot, hash: hash})
}

// Put adds or updates a block in the cache.
// The block is moved to the front of its shard's LRU list.
// If the shard exceeds its capacity, the least recently used block in that
// shard is evicted.
func (c *BlockLRUCache) Put(slot uint64, hash [32]byte, block *CachedBlock) {
	c.shardFor(hash).put(blockKey{slot: slot, hash: hash}, block)
}
