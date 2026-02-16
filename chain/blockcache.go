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

package chain

import (
	"container/list"
	"sync"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DefaultBlockCacheCapacity is the default maximum number of
// blocks to cache. At ~20KB per block, 10K blocks uses ~200MB
// of memory.
const DefaultBlockCacheCapacity = 10000

// blockCache is an LRU cache for blocks keyed by block hash.
// It is used to cache rolled-back blocks that may still be
// needed by non-primary chains during reconciliation.
// All methods are thread-safe.
type blockCache struct {
	mu           sync.Mutex
	capacity     int
	items        map[string]*list.Element
	order        *list.List // front = most recent, back = least recent
	cachedBlocks prometheus.Gauge
}

type blockCacheEntry struct {
	hash  string
	block models.Block
}

// newBlockCache creates a new block cache with the given
// capacity. If capacity is <= 0, DefaultBlockCacheCapacity
// is used.
func newBlockCache(
	capacity int,
	promRegistry prometheus.Registerer,
) *blockCache {
	if capacity <= 0 {
		capacity = DefaultBlockCacheCapacity
	}
	c := &blockCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		order:    list.New(),
	}
	if promRegistry != nil {
		c.initMetrics(promRegistry)
	}
	return c
}

func (c *blockCache) initMetrics(
	promRegistry prometheus.Registerer,
) {
	promautoFactory := promauto.With(promRegistry)
	c.cachedBlocks = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_chain_manager_cached_blocks",
			Help: "current number of cached blocks in the chain manager LRU cache",
		},
	)
}

func (c *blockCache) updateMetrics() {
	if c.cachedBlocks != nil {
		c.cachedBlocks.Set(float64(c.order.Len()))
	}
}

// Get retrieves a block from the cache by its hash.
// Returns the block and true if found, or an empty block
// and false if not found. Accessing a block moves it to the
// front of the LRU list.
func (c *blockCache) Get(
	hash string,
) (models.Block, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[hash]; ok {
		c.order.MoveToFront(elem)
		return elem.Value.(*blockCacheEntry).block, true
	}
	return models.Block{}, false
}

// Put adds or updates a block in the cache.
// If the cache is at capacity, the least recently used block
// is evicted.
func (c *blockCache) Put(block models.Block) {
	c.mu.Lock()
	defer c.mu.Unlock()
	hash := string(block.Hash)

	// If already in cache, update and move to front
	if elem, ok := c.items[hash]; ok {
		c.order.MoveToFront(elem)
		elem.Value.(*blockCacheEntry).block = block
		return
	}

	// Evict if at capacity
	if c.order.Len() >= c.capacity {
		c.evictOldest()
	}

	// Add new entry at front
	entry := &blockCacheEntry{
		hash:  hash,
		block: block,
	}
	elem := c.order.PushFront(entry)
	c.items[hash] = elem
	c.updateMetrics()
}

// Delete removes a block from the cache by its hash.
func (c *blockCache) Delete(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[hash]; ok {
		c.order.Remove(elem)
		delete(c.items, hash)
		c.updateMetrics()
	}
}

// evictOldest removes the least recently used entry from
// the cache.
func (c *blockCache) evictOldest() {
	if elem := c.order.Back(); elem != nil {
		c.order.Remove(elem)
		entry := elem.Value.(*blockCacheEntry)
		delete(c.items, entry.hash)
	}
}

// Len returns the number of blocks in the cache.
func (c *blockCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
