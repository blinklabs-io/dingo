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
	"maps"
	"sort"
	"sync/atomic"
)

// hotCacheData holds both entries and access counts together for atomic updates.
// This ensures entries and accessCnt are always consistent.
type hotCacheData struct {
	entries   map[string][]byte
	accessCnt map[string]uint64
}

// accessSampleRate controls how often access counts are updated on Get().
// A value of 4 means ~25% of Gets trigger an access count update.
// This trades LFU accuracy for performance by avoiding map copies on every read.
const accessSampleRate = 4

// HotCache provides a lock-free cache for frequently accessed CBOR data.
// It uses copy-on-write semantics for thread-safe concurrent access without locks.
// Eviction follows a Least-Frequently-Used (LFU) policy with probabilistic counting.
type HotCache struct {
	data      atomic.Pointer[hotCacheData] // combined entries + access counts
	maxSize   int                          // max number of entries (0 = unlimited)
	maxBytes  int64                        // max memory in bytes (0 = unlimited)
	curBytes  atomic.Int64                 // current memory usage
	evicting  atomic.Bool                  // eviction lock
	sampleCnt atomic.Uint64                // counter for probabilistic access tracking
}

// NewHotCache creates a new HotCache with the given size and memory limits.
// Set maxSize to 0 for unlimited entries (limited only by maxBytes).
// Set maxBytes to 0 for unlimited memory (limited only by maxSize).
func NewHotCache(maxSize int, maxBytes int64) *HotCache {
	cache := &HotCache{
		maxSize:  maxSize,
		maxBytes: maxBytes,
	}

	// Initialize combined data structure
	data := &hotCacheData{
		entries:   make(map[string][]byte),
		accessCnt: make(map[string]uint64),
	}
	cache.data.Store(data)

	return cache
}

// Get retrieves a value from the cache by key.
// Returns the value and true if found, nil and false otherwise.
// This operation is lock-free and safe for concurrent use.
// Access counts are updated probabilistically (1 in accessSampleRate calls)
// to reduce overhead while maintaining approximate LFU behavior.
func (c *HotCache) Get(key []byte) ([]byte, bool) {
	data := c.data.Load()
	if data == nil {
		return nil, false
	}

	value, ok := data.entries[string(key)]
	if ok {
		// Probabilistic counting: only update access count 1 in accessSampleRate times
		// This avoids expensive map copies on every read while maintaining approximate LFU
		if c.sampleCnt.Add(1)%accessSampleRate == 0 {
			c.incrementAccess(key)
		}
		// Return a copy to prevent callers from mutating cached data
		return append([]byte(nil), value...), true
	}
	return nil, false
}

// Put adds or updates a value in the cache.
// If maxBytes > 0 and the entry size exceeds maxBytes/10, the entry is skipped.
// This operation uses copy-on-write semantics for thread safety.
func (c *HotCache) Put(key []byte, cbor []byte) {
	keyStr := string(key)
	entrySize := int64(len(key) + len(cbor))

	// Skip entries that are too large (> 10% of max memory)
	if c.maxBytes > 0 && entrySize > c.maxBytes/10 {
		return
	}

	// Copy-on-write: create new combined data with the update
	for {
		oldData := c.data.Load()

		newEntries := make(map[string][]byte, len(oldData.entries)+1)
		maps.Copy(newEntries, oldData.entries)

		// Track memory change
		var memDelta int64
		if oldValue, exists := newEntries[keyStr]; exists {
			memDelta = entrySize - int64(len(keyStr)+len(oldValue))
		} else {
			memDelta = entrySize
		}

		// Copy the value to prevent callers from mutating cached data
		cborCopy := make([]byte, len(cbor))
		copy(cborCopy, cbor)
		newEntries[keyStr] = cborCopy

		// Also update access count map
		newAccessCnt := make(map[string]uint64, len(oldData.accessCnt)+1)
		maps.Copy(newAccessCnt, oldData.accessCnt)
		newAccessCnt[keyStr]++ // increment on put

		newData := &hotCacheData{
			entries:   newEntries,
			accessCnt: newAccessCnt,
		}

		// Try to atomically update both maps together
		if c.data.CompareAndSwap(oldData, newData) {
			if c.maxBytes > 0 {
				c.curBytes.Add(memDelta)
			}
			break
		}
		// CAS failed, retry with fresh data
	}

	c.maybeEvict()
}

// incrementAccess increments the access counter for a key.
// Uses copy-on-write semantics for thread safety.
// Only increments if the key still exists in entries to prevent orphan access counts.
func (c *HotCache) incrementAccess(key []byte) {
	keyStr := string(key)

	for {
		oldData := c.data.Load()
		if oldData == nil {
			return
		}

		// Check if key still exists in entries (may have been evicted)
		if _, exists := oldData.entries[keyStr]; !exists {
			return
		}

		// Copy and update access counts only - entries are unchanged so reuse them
		newAccessCnt := make(map[string]uint64, len(oldData.accessCnt))
		maps.Copy(newAccessCnt, oldData.accessCnt)
		newAccessCnt[keyStr]++

		newData := &hotCacheData{
			entries:   oldData.entries, // reuse immutable entries map
			accessCnt: newAccessCnt,
		}

		if c.data.CompareAndSwap(oldData, newData) {
			return
		}
		// CAS failed, retry
	}
}

// maybeEvict checks if eviction is needed and performs LFU eviction.
// Uses atomic bool to ensure only one goroutine performs eviction at a time.
func (c *HotCache) maybeEvict() {
	// Check if eviction is needed
	data := c.data.Load()
	if data == nil {
		return
	}

	currentSize := len(data.entries)
	currentBytes := c.curBytes.Load()

	needEvictBySize := c.maxSize > 0 && currentSize > c.maxSize
	needEvictByBytes := c.maxBytes > 0 && currentBytes > c.maxBytes

	if !needEvictBySize && !needEvictByBytes {
		return
	}

	// Try to acquire eviction lock
	if !c.evicting.CompareAndSwap(false, true) {
		return // Another goroutine is already evicting
	}
	defer c.evicting.Store(false)

	// Use CAS loop to atomically update while handling concurrent modifications
	for {
		oldData := c.data.Load()
		if oldData == nil {
			return
		}

		currentSize = len(oldData.entries)

		// Calculate target size (evict ~25%, but keep at least 1 entry)
		// Only computed when maxSize > 0; otherwise size-based eviction is disabled
		var targetSize int
		if c.maxSize > 0 {
			targetSize = c.maxSize * 3 / 4
			if targetSize < 1 {
				targetSize = 1
			}
		}

		var targetBytes int64
		if c.maxBytes > 0 {
			targetBytes = c.maxBytes * 3 / 4
		}

		// Build list of entries sorted by access count (ascending = least frequent first)
		type entry struct {
			key   string
			count uint64
			size  int64
		}

		entriesList := make([]entry, 0, currentSize)
		for k, v := range oldData.entries {
			count := oldData.accessCnt[k]
			entriesList = append(entriesList, entry{
				key:   k,
				count: count,
				size:  int64(len(k) + len(v)),
			})
		}

		// Sort by access count ascending (least frequently used first)
		sort.Slice(entriesList, func(i, j int) bool {
			return entriesList[i].count < entriesList[j].count
		})

		// Determine which entries to keep
		keysToRemove := make(map[string]bool)
		keptSize := 0
		var keptBytes int64

		for i := len(entriesList) - 1; i >= 0; i-- {
			e := entriesList[i]

			wouldExceedSize := c.maxSize > 0 && keptSize >= targetSize
			wouldExceedBytes := c.maxBytes > 0 && keptBytes+e.size > targetBytes

			if wouldExceedSize || wouldExceedBytes {
				keysToRemove[e.key] = true
			} else {
				keptSize++
				keptBytes += e.size
			}
		}

		if len(keysToRemove) == 0 {
			return
		}

		// Create new maps without evicted entries
		newEntries := make(map[string][]byte, keptSize)
		newAccessCnt := make(map[string]uint64, keptSize)
		var bytesRemoved int64

		for k, v := range oldData.entries {
			if keysToRemove[k] {
				bytesRemoved += int64(len(k) + len(v))
				continue
			}
			newEntries[k] = v
			newAccessCnt[k] = oldData.accessCnt[k]
		}

		newData := &hotCacheData{
			entries:   newEntries,
			accessCnt: newAccessCnt,
		}

		// Try to atomically update
		if c.data.CompareAndSwap(oldData, newData) {
			if c.maxBytes > 0 {
				c.curBytes.Add(-bytesRemoved)
			}
			return
		}
		// CAS failed, retry with fresh data
	}
}
