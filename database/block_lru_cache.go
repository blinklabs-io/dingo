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

// BlockLRUCache is a thread-safe LRU cache for recently accessed blocks.
// Blocks are keyed by (slot, hash) and evicted in LRU order when the cache
// exceeds maxEntries.
type BlockLRUCache struct {
	mu         sync.Mutex
	maxEntries int
	cache      map[blockKey]*list.Element
	lruList    *list.List
}

// NewBlockLRUCache creates a new BlockLRUCache with the specified maximum
// number of entries. If maxEntries is negative, it is treated as zero
// (cache disabled).
func NewBlockLRUCache(maxEntries int) *BlockLRUCache {
	if maxEntries < 0 {
		maxEntries = 0
	}
	return &BlockLRUCache{
		maxEntries: maxEntries,
		cache:      make(map[blockKey]*list.Element),
		lruList:    list.New(),
	}
}

// Get retrieves a cached block by slot and hash.
// Returns the block and true if found, or nil and false if not found.
// Accessing a block moves it to the front of the LRU list.
func (c *BlockLRUCache) Get(slot uint64, hash [32]byte) (*CachedBlock, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := blockKey{slot: slot, hash: hash}
	elem, ok := c.cache[key]
	if !ok {
		return nil, false
	}

	// Move to front (most recently used)
	c.lruList.MoveToFront(elem)

	entry := elem.Value.(*cacheEntry)
	return entry.block, true
}

// Put adds or updates a block in the cache.
// The block is moved to the front of the LRU list.
// If the cache exceeds maxEntries, the least recently used block is evicted.
func (c *BlockLRUCache) Put(slot uint64, hash [32]byte, block *CachedBlock) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := blockKey{slot: slot, hash: hash}

	// Check if entry already exists
	if elem, ok := c.cache[key]; ok {
		// Update existing entry
		c.lruList.MoveToFront(elem)
		entry := elem.Value.(*cacheEntry)
		entry.block = block
		return
	}

	// Add new entry
	entry := &cacheEntry{key: key, block: block}
	elem := c.lruList.PushFront(entry)
	c.cache[key] = elem

	// Evict if over capacity
	for c.lruList.Len() > c.maxEntries {
		c.evictOldest()
	}
}

// evictOldest removes the least recently used entry from the cache.
// Must be called with the mutex held.
func (c *BlockLRUCache) evictOldest() {
	elem := c.lruList.Back()
	if elem == nil {
		return
	}

	entry := elem.Value.(*cacheEntry)
	delete(c.cache, entry.key)
	c.lruList.Remove(elem)
}
