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
	"fmt"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockCache_BasicOperations(t *testing.T) {
	cache := newBlockCache(3, nil)

	// Test empty cache
	_, ok := cache.Get("nonexistent")
	assert.False(t, ok)
	assert.Equal(t, 0, cache.Len())

	// Add a block
	block1 := models.Block{
		Hash: []byte("hash1"),
		Slot: 100,
	}
	cache.Put(block1)
	assert.Equal(t, 1, cache.Len())

	// Retrieve it
	got, ok := cache.Get("hash1")
	assert.True(t, ok)
	assert.Equal(t, uint64(100), got.Slot)
}

func TestBlockCache_LRUEviction(t *testing.T) {
	cache := newBlockCache(3, nil)

	// Add 3 blocks
	block1 := models.Block{
		Hash: []byte("hash1"),
		Slot: 1,
	}
	block2 := models.Block{
		Hash: []byte("hash2"),
		Slot: 2,
	}
	block3 := models.Block{
		Hash: []byte("hash3"),
		Slot: 3,
	}

	cache.Put(block1)
	cache.Put(block2)
	cache.Put(block3)
	assert.Equal(t, 3, cache.Len())

	// All should be present
	_, ok1 := cache.Get("hash1")
	_, ok2 := cache.Get("hash2")
	_, ok3 := cache.Get("hash3")
	assert.True(t, ok1)
	assert.True(t, ok2)
	assert.True(t, ok3)

	// Add a 4th block, should evict hash1 (least recently
	// used). After the Gets above, order is hash3, hash2,
	// hash1 (most to least recent), so hash1 is evicted.
	block4 := models.Block{
		Hash: []byte("hash4"),
		Slot: 4,
	}
	cache.Put(block4)
	assert.Equal(t, 3, cache.Len())

	// hash1 should be evicted
	_, ok1 = cache.Get("hash1")
	assert.False(t, ok1)

	// Others should still be present
	_, ok2 = cache.Get("hash2")
	_, ok3 = cache.Get("hash3")
	_, ok4 := cache.Get("hash4")
	assert.True(t, ok2)
	assert.True(t, ok3)
	assert.True(t, ok4)
}

func TestBlockCache_UpdateExisting(t *testing.T) {
	cache := newBlockCache(3, nil)

	// Add a block
	block1 := models.Block{
		Hash: []byte("hash1"),
		Slot: 100,
	}
	cache.Put(block1)

	// Update it
	block1Updated := models.Block{
		Hash: []byte("hash1"),
		Slot: 200,
	}
	cache.Put(block1Updated)

	// Should still have only 1 entry
	assert.Equal(t, 1, cache.Len())

	// Should have updated slot
	got, ok := cache.Get("hash1")
	assert.True(t, ok)
	assert.Equal(t, uint64(200), got.Slot)
}

func TestBlockCache_AccessMovesToFront(t *testing.T) {
	cache := newBlockCache(3, nil)

	// Add 3 blocks in order
	block1 := models.Block{
		Hash: []byte("hash1"),
		Slot: 1,
	}
	block2 := models.Block{
		Hash: []byte("hash2"),
		Slot: 2,
	}
	block3 := models.Block{
		Hash: []byte("hash3"),
		Slot: 3,
	}

	cache.Put(block1)
	cache.Put(block2)
	cache.Put(block3)

	// Access hash1, moving it to front
	cache.Get("hash1")

	// Add block4 - should evict hash2 (now least recently
	// used)
	block4 := models.Block{
		Hash: []byte("hash4"),
		Slot: 4,
	}
	cache.Put(block4)

	// hash1 should still be present (was accessed)
	_, ok1 := cache.Get("hash1")
	assert.True(t, ok1)

	// hash2 should be evicted
	_, ok2 := cache.Get("hash2")
	assert.False(t, ok2)

	// hash3 and hash4 should be present
	_, ok3 := cache.Get("hash3")
	_, ok4 := cache.Get("hash4")
	assert.True(t, ok3)
	assert.True(t, ok4)
}

func TestBlockCache_DefaultCapacity(t *testing.T) {
	// Test that 0 capacity uses default
	cache := newBlockCache(0, nil)
	assert.Equal(
		t,
		DefaultBlockCacheCapacity,
		cache.capacity,
	)

	// Test that negative capacity uses default
	cache = newBlockCache(-1, nil)
	assert.Equal(
		t,
		DefaultBlockCacheCapacity,
		cache.capacity,
	)
}

func TestBlockCache_Delete(t *testing.T) {
	cache := newBlockCache(5, nil)

	// Add blocks
	block1 := models.Block{
		Hash: []byte("hash1"),
		Slot: 1,
	}
	block2 := models.Block{
		Hash: []byte("hash2"),
		Slot: 2,
	}
	block3 := models.Block{
		Hash: []byte("hash3"),
		Slot: 3,
	}
	cache.Put(block1)
	cache.Put(block2)
	cache.Put(block3)
	assert.Equal(t, 3, cache.Len())

	// Delete middle block
	cache.Delete("hash2")
	assert.Equal(t, 2, cache.Len())

	// Deleted block should not be found
	_, ok := cache.Get("hash2")
	assert.False(t, ok)

	// Other blocks should still be present
	_, ok1 := cache.Get("hash1")
	_, ok3 := cache.Get("hash3")
	assert.True(t, ok1)
	assert.True(t, ok3)

	// Deleting a non-existent key should be a no-op
	cache.Delete("nonexistent")
	assert.Equal(t, 2, cache.Len())
}

func TestBlockCache_ConcurrentAccess(t *testing.T) {
	cache := newBlockCache(100, nil)
	const goroutines = 10
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines * 3) // Put, Get, Delete goroutines

	// Concurrent Put operations
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				hash := fmt.Sprintf(
					"hash-%d-%d",
					id,
					i,
				)
				block := models.Block{
					Hash: []byte(hash),
					Slot: uint64(
						id*opsPerGoroutine + i,
					),
				}
				cache.Put(block)
			}
		}(g)
	}

	// Concurrent Get operations
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				hash := fmt.Sprintf(
					"hash-%d-%d",
					id,
					i,
				)
				cache.Get(hash)
			}
		}(g)
	}

	// Concurrent Delete operations
	for g := range goroutines {
		go func(id int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				hash := fmt.Sprintf(
					"hash-%d-%d",
					id,
					i,
				)
				cache.Delete(hash)
			}
		}(g)
	}

	wg.Wait()

	// Cache should be in a consistent state
	cacheLen := cache.Len()
	require.GreaterOrEqual(t, cacheLen, 0)
	require.LessOrEqual(t, cacheLen, 100)
}

func TestBlockCache_PrometheusMetric(t *testing.T) {
	registry := prometheus.NewRegistry()
	cache := newBlockCache(5, registry)

	// Add blocks and verify metric is registered
	block1 := models.Block{
		Hash: []byte("hash1"),
		Slot: 1,
	}
	block2 := models.Block{
		Hash: []byte("hash2"),
		Slot: 2,
	}
	cache.Put(block1)
	cache.Put(block2)

	// Gather metrics from the registry
	metrics, err := registry.Gather()
	require.NoError(t, err)

	// Find our metric
	var found bool
	for _, mf := range metrics {
		if mf.GetName() == "dingo_chain_manager_cached_blocks" {
			found = true
			require.Len(t, mf.GetMetric(), 1)
			assert.Equal(
				t,
				float64(2),
				mf.GetMetric()[0].GetGauge().GetValue(),
			)
		}
	}
	require.True(
		t,
		found,
		"dingo_chain_manager_cached_blocks metric not found",
	)

	// Delete a block and verify metric updates
	cache.Delete("hash1")
	metrics, err = registry.Gather()
	require.NoError(t, err)
	for _, mf := range metrics {
		if mf.GetName() == "dingo_chain_manager_cached_blocks" {
			assert.Equal(
				t,
				float64(1),
				mf.GetMetric()[0].GetGauge().GetValue(),
			)
		}
	}
}
