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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBlockLRUCacheGetPut(t *testing.T) {
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

func TestBlockLRUCacheConcurrent(t *testing.T) {
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
