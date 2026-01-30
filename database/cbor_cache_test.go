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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTieredCborCache(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  1000,
		HotTxEntries:    500,
		HotTxMaxBytes:   1024 * 1024, // 1 MB
		BlockLRUEntries: 100,
	}

	cache := NewTieredCborCache(config)

	require.NotNil(t, cache)
	require.NotNil(t, cache.hotUtxo)
	require.NotNil(t, cache.hotTx)
	require.NotNil(t, cache.blockLRU)
	require.NotNil(t, cache.metrics)
}

func TestTieredCborCacheHotHitUtxo(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config)

	// Create a test key and CBOR data
	var txId [32]byte
	copy(txId[:], []byte("test-tx-id-0000000000000000000"))
	outputIdx := uint32(0)
	testCbor := []byte{0x82, 0x01, 0x02} // Simple CBOR array [1, 2]

	// Manually populate the hot cache
	key := makeUtxoKey(txId[:], outputIdx)
	cache.hotUtxo.Put(key, testCbor)

	// Resolve should hit the hot cache
	result, err := cache.ResolveUtxoCbor(txId[:], outputIdx)

	require.NoError(t, err)
	assert.Equal(t, testCbor, result)

	// Verify metrics show a hot hit
	metrics := cache.Metrics()
	assert.Equal(t, uint64(1), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(0), metrics.UtxoHotMisses.Load())
}

func TestTieredCborCacheHotHitTx(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config)

	// Create a test key and CBOR data
	var txHash [32]byte
	copy(txHash[:], []byte("test-tx-hash-000000000000000000"))
	testCbor := []byte{0x82, 0x01, 0x02} // Simple CBOR array [1, 2]

	// Manually populate the hot cache
	cache.hotTx.Put(txHash[:], testCbor)

	// Resolve should hit the hot cache
	result, err := cache.ResolveTxCbor(txHash[:])

	require.NoError(t, err)
	assert.Equal(t, testCbor, result)

	// Verify metrics show a hot hit
	metrics := cache.Metrics()
	assert.Equal(t, uint64(1), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(0), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheHotMissUtxo(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config)

	// Create a test key that is NOT in the cache
	var txId [32]byte
	copy(txId[:], []byte("missing-tx-id-0000000000000000"))
	outputIdx := uint32(5)

	// Resolve should miss the hot cache and return ErrNotImplemented
	// (since cold path is not wired up yet)
	result, err := cache.ResolveUtxoCbor(txId[:], outputIdx)

	assert.ErrorIs(t, err, ErrNotImplemented)
	assert.Nil(t, result)

	// Verify metrics show a hot miss
	metrics := cache.Metrics()
	assert.Equal(t, uint64(0), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(1), metrics.UtxoHotMisses.Load())
}

func TestTieredCborCacheHotMissTx(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config)

	// Create a test key that is NOT in the cache
	var txHash [32]byte
	copy(txHash[:], []byte("missing-tx-hash-00000000000000"))

	// Resolve should miss the hot cache and return ErrNotImplemented
	result, err := cache.ResolveTxCbor(txHash[:])

	assert.ErrorIs(t, err, ErrNotImplemented)
	assert.Nil(t, result)

	// Verify metrics show a hot miss
	metrics := cache.Metrics()
	assert.Equal(t, uint64(0), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(1), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheMetrics(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config)

	// Initial metrics should all be zero
	metrics := cache.Metrics()
	assert.Equal(t, uint64(0), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(0), metrics.UtxoHotMisses.Load())
	assert.Equal(t, uint64(0), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(0), metrics.TxHotMisses.Load())
	assert.Equal(t, uint64(0), metrics.BlockLRUHits.Load())
	assert.Equal(t, uint64(0), metrics.BlockLRUMisses.Load())
	assert.Equal(t, uint64(0), metrics.ColdExtractions.Load())

	// Populate some UTxO entries
	var txId1 [32]byte
	copy(txId1[:], []byte("tx-id-1-00000000000000000000000"))
	cache.hotUtxo.Put(makeUtxoKey(txId1[:], 0), []byte{0x01})

	var txId2 [32]byte
	copy(txId2[:], []byte("tx-id-2-00000000000000000000000"))
	// txId2 is NOT in cache

	// Perform some hits and misses
	_, _ = cache.ResolveUtxoCbor(txId1[:], 0) // hit
	_, _ = cache.ResolveUtxoCbor(txId1[:], 0) // hit
	_, _ = cache.ResolveUtxoCbor(txId2[:], 0) // miss

	// Verify counts
	assert.Equal(t, uint64(2), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(1), metrics.UtxoHotMisses.Load())

	// Populate some TX entries
	var txHash1 [32]byte
	copy(txHash1[:], []byte("tx-hash-1-000000000000000000000"))
	cache.hotTx.Put(txHash1[:], []byte{0x02})

	var txHash2 [32]byte
	copy(txHash2[:], []byte("tx-hash-2-000000000000000000000"))
	// txHash2 is NOT in cache

	// Perform some hits and misses
	_, _ = cache.ResolveTxCbor(txHash1[:]) // hit
	_, _ = cache.ResolveTxCbor(txHash2[:]) // miss
	_, _ = cache.ResolveTxCbor(txHash2[:]) // miss

	// Verify counts
	assert.Equal(t, uint64(1), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(2), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheBatchStub(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config)

	// Create some test refs
	refs := []UtxoRef{
		{TxId: [32]byte{1}, OutputIdx: 0},
		{TxId: [32]byte{2}, OutputIdx: 1},
	}

	// Batch resolution returns ErrNotImplemented for now
	result, err := cache.ResolveUtxoCborBatch(refs)

	assert.ErrorIs(t, err, ErrNotImplemented)
	assert.Nil(t, result)
}

func TestUtxoRefEquality(t *testing.T) {
	// Test that UtxoRef works correctly as map keys
	ref1 := UtxoRef{TxId: [32]byte{1, 2, 3}, OutputIdx: 5}
	ref2 := UtxoRef{TxId: [32]byte{1, 2, 3}, OutputIdx: 5}
	ref3 := UtxoRef{TxId: [32]byte{1, 2, 3}, OutputIdx: 6}
	ref4 := UtxoRef{TxId: [32]byte{1, 2, 4}, OutputIdx: 5}

	assert.Equal(t, ref1, ref2)
	assert.NotEqual(t, ref1, ref3)
	assert.NotEqual(t, ref1, ref4)

	// Test map usage
	m := make(map[UtxoRef][]byte)
	m[ref1] = []byte{0x01}

	_, exists := m[ref2]
	assert.True(t, exists, "ref2 should match ref1 as map key")

	_, exists = m[ref3]
	assert.False(t, exists, "ref3 should not match ref1")
}

func TestMakeUtxoKey(t *testing.T) {
	var txId [32]byte
	for i := range txId {
		txId[i] = byte(i)
	}
	outputIdx := uint32(12345)

	key := makeUtxoKey(txId[:], outputIdx)

	// Key should be 36 bytes: 32 (txId) + 4 (outputIdx big-endian)
	assert.Len(t, key, 36)

	// First 32 bytes should be the txId
	assert.Equal(t, txId[:], key[:32])

	// Last 4 bytes should be outputIdx in big-endian
	assert.Equal(t, byte(0x00), key[32])
	assert.Equal(t, byte(0x00), key[33])
	assert.Equal(t, byte(0x30), key[34]) // 12345 = 0x3039
	assert.Equal(t, byte(0x39), key[35])
}

func TestCborCacheConfigDefaults(t *testing.T) {
	// Test with zero config
	config := CborCacheConfig{}

	cache := NewTieredCborCache(config)

	require.NotNil(t, cache)
	require.NotNil(t, cache.hotUtxo)
	require.NotNil(t, cache.hotTx)
	require.NotNil(t, cache.blockLRU)
	require.NotNil(t, cache.metrics)
}
