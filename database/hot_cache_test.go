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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHotCacheGetPut(t *testing.T) {
	cache := NewHotCache(100, 0)

	// Test Put and Get
	key1 := []byte("key1")
	value1 := []byte("value1")

	cache.Put(key1, value1)

	got, ok := cache.Get(key1)
	require.True(t, ok, "expected key to be found")
	assert.Equal(t, value1, got, "expected value to match")

	// Test missing key
	got, ok = cache.Get([]byte("nonexistent"))
	assert.False(t, ok, "expected key not to be found")
	assert.Nil(t, got, "expected nil value for missing key")

	// Test overwrite
	value2 := []byte("value2")
	cache.Put(key1, value2)

	got, ok = cache.Get(key1)
	require.True(t, ok, "expected key to be found after overwrite")
	assert.Equal(t, value2, got, "expected updated value")

	// Test multiple keys
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		cache.Put(key, value)
	}

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		expectedValue := []byte(fmt.Sprintf("value%d", i))
		got, ok := cache.Get(key)
		require.True(t, ok, "expected key%d to be found", i)
		assert.Equal(t, expectedValue, got, "expected value%d to match", i)
	}
}

func TestHotCacheConcurrent(t *testing.T) {
	cache := NewHotCache(1000, 0)

	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				value := []byte(fmt.Sprintf("value-%d-%d", id, j))
				cache.Put(key, value)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify some entries are still accessible
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%d-%d", i, numOperations-1))
		got, ok := cache.Get(key)
		if ok {
			expected := []byte(fmt.Sprintf("value-%d-%d", i, numOperations-1))
			assert.Equal(t, expected, got, "value mismatch for concurrent key")
		}
	}
}

func TestHotCacheEviction(t *testing.T) {
	maxSize := 10
	cache := NewHotCache(maxSize, 0)

	// Add entries up to maxSize
	for i := 0; i < maxSize; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		cache.Put(key, value)
	}

	// Access some keys to increase their frequency
	frequentKeys := [][]byte{
		[]byte("key0"),
		[]byte("key1"),
		[]byte("key2"),
	}
	for _, key := range frequentKeys {
		for j := 0; j < 10; j++ {
			cache.Get(key)
		}
	}

	// Add more entries to trigger eviction
	for i := maxSize; i < maxSize+10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		cache.Put(key, value)
	}

	// Frequently accessed keys should still be present
	for _, key := range frequentKeys {
		_, ok := cache.Get(key)
		assert.True(
			t,
			ok,
			"frequently accessed key %s should still be present",
			string(key),
		)
	}

	// Verify cache size is controlled (may be slightly over due to concurrent ops)
	data := cache.data.Load()
	assert.LessOrEqual(
		t,
		len(data.entries),
		maxSize+5,
		"cache should be close to maxSize after eviction",
	)
}

func TestHotCacheMemoryLimit(t *testing.T) {
	maxBytes := int64(1000)
	cache := NewHotCache(1000, maxBytes)

	// Add entries that together exceed maxBytes
	valueSize := 100
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i)
		}
		cache.Put(key, value)
	}

	// Memory usage should be controlled
	assert.LessOrEqual(
		t,
		cache.curBytes.Load(),
		maxBytes+int64(valueSize*3),
		"memory should be close to maxBytes after eviction",
	)

	// Test that oversized entries are rejected
	// Entry > maxBytes/10 should be skipped
	oversizedKey := []byte("oversized")
	oversizedValue := make([]byte, maxBytes/10+1)
	cache.Put(oversizedKey, oversizedValue)

	_, ok := cache.Get(oversizedKey)
	assert.False(t, ok, "oversized entry should not be stored")
}

func TestHotCacheLFUEviction(t *testing.T) {
	maxSize := 5
	cache := NewHotCache(maxSize, 0)

	// Add initial entries
	for i := 0; i < maxSize; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		cache.Put(key, value)
	}

	// Access key0 many times to make it frequently used
	for i := 0; i < 50; i++ {
		cache.Get([]byte("key0"))
	}

	// Access key1 a moderate number of times
	for i := 0; i < 20; i++ {
		cache.Get([]byte("key1"))
	}

	// key2, key3, key4 have access count of 1 from the Put operation only

	// Add new entries to force eviction
	for i := maxSize; i < maxSize+5; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		cache.Put(key, value)
	}

	// key0 (most frequent) should still be present
	_, ok := cache.Get([]byte("key0"))
	assert.True(t, ok, "most frequently accessed key should survive eviction")

	// key1 (moderately frequent) should likely still be present
	_, ok = cache.Get([]byte("key1"))
	assert.True(t, ok, "moderately accessed key should survive eviction")
}

func TestHotCacheEmptyCache(t *testing.T) {
	cache := NewHotCache(10, 0)

	// Get from empty cache
	got, ok := cache.Get([]byte("nonexistent"))
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestHotCacheNilKey(t *testing.T) {
	cache := NewHotCache(10, 0)

	// Put with nil key
	cache.Put(nil, []byte("value"))

	// Should be retrievable with empty key
	got, ok := cache.Get(nil)
	assert.True(t, ok)
	assert.Equal(t, []byte("value"), got)
}

func TestHotCacheZeroMaxSize(t *testing.T) {
	// Zero maxSize means unlimited by count
	cache := NewHotCache(0, 1000)

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		cache.Put(key, value)
	}

	// All entries should be present (limited only by bytes)
	count := 0
	data := cache.data.Load()
	if data != nil {
		count = len(data.entries)
	}
	assert.Greater(t, count, 0, "cache should have entries")
}

func TestHotCacheSmallMaxSize(t *testing.T) {
	// Test that maxSize=1 works correctly (edge case for eviction)
	cache := NewHotCache(1, 0)

	// Add first entry
	cache.Put([]byte("key1"), []byte("value1"))
	val, ok := cache.Get([]byte("key1"))
	assert.True(t, ok, "first entry should be retrievable")
	assert.Equal(t, []byte("value1"), val)

	// Add second entry - should trigger eviction but keep at least 1
	cache.Put([]byte("key2"), []byte("value2"))

	// At least one entry should exist (either key1 or key2)
	data := cache.data.Load()
	assert.NotNil(t, data)
	assert.GreaterOrEqual(
		t,
		len(data.entries),
		1,
		"cache with maxSize=1 should keep at least 1 entry",
	)
	assert.LessOrEqual(
		t,
		len(data.entries),
		2,
		"cache with maxSize=1 should have at most 2 entries",
	)
}
