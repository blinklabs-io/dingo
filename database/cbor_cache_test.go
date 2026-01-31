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

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
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

	cache := NewTieredCborCache(config, nil)

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

	cache := NewTieredCborCache(config, nil)

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

	cache := NewTieredCborCache(config, nil)

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

	cache := NewTieredCborCache(config, nil)

	// Create a test key that is NOT in the cache
	var txId [32]byte
	copy(txId[:], []byte("missing-tx-id-0000000000000000"))
	outputIdx := uint32(5)

	// Resolve should miss the hot cache and return ErrBlobStoreUnavailable
	// (since db is nil)
	result, err := cache.ResolveUtxoCbor(txId[:], outputIdx)

	assert.ErrorIs(t, err, types.ErrBlobStoreUnavailable)
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

	cache := NewTieredCborCache(config, nil)

	// Create a test key that is NOT in the cache
	var txHash [32]byte
	copy(txHash[:], []byte("missing-tx-hash-00000000000000"))

	// Resolve should miss the hot cache and return ErrBlobStoreUnavailable
	// (since db is nil)
	result, err := cache.ResolveTxCbor(txHash[:])

	assert.ErrorIs(t, err, types.ErrBlobStoreUnavailable)
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

	cache := NewTieredCborCache(config, nil)

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

func TestTieredCborCacheBatchHotHits(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config, nil)

	// Populate some UTxOs in hot cache
	ref1 := UtxoRef{TxId: [32]byte{1}, OutputIdx: 0}
	ref2 := UtxoRef{TxId: [32]byte{2}, OutputIdx: 1}
	ref3 := UtxoRef{TxId: [32]byte{3}, OutputIdx: 2} // Not in cache

	cbor1 := []byte{0x01, 0x02}
	cbor2 := []byte{0x03, 0x04}

	cache.hotUtxo.Put(makeUtxoKey(ref1.TxId[:], ref1.OutputIdx), cbor1)
	cache.hotUtxo.Put(makeUtxoKey(ref2.TxId[:], ref2.OutputIdx), cbor2)

	// Batch resolution should return hot cache hits
	refs := []UtxoRef{ref1, ref2, ref3}
	result, err := cache.ResolveUtxoCborBatch(refs)

	// No error - but ref3 won't be in result since db is nil (can't fetch cold)
	assert.ErrorIs(t, err, types.ErrBlobStoreUnavailable)
	assert.Equal(t, cbor1, result[ref1])
	assert.Equal(t, cbor2, result[ref2])
	_, hasRef3 := result[ref3]
	assert.False(t, hasRef3, "ref3 should not be in result (not in cache, no db)")

	// Verify hot hit metrics
	metrics := cache.Metrics()
	assert.Equal(t, uint64(2), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(1), metrics.UtxoHotMisses.Load())
}

func TestTieredCborCacheBatchEmpty(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config, nil)

	// Batch resolution with empty refs returns empty result
	result, err := cache.ResolveUtxoCborBatch(nil)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result, 0)
}

func TestTieredCborCacheTxBatchHotHits(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config, nil)

	// Populate some TXs in hot cache
	hash1 := [32]byte{1}
	hash2 := [32]byte{2}
	hash3 := [32]byte{3} // Not in cache

	cbor1 := []byte{0x01, 0x02}
	cbor2 := []byte{0x03, 0x04}

	cache.hotTx.Put(hash1[:], cbor1)
	cache.hotTx.Put(hash2[:], cbor2)

	// Batch resolution should return hot cache hits
	hashes := [][32]byte{hash1, hash2, hash3}
	result, err := cache.ResolveTxCborBatch(hashes)

	// No error - but hash3 won't be in result since db is nil (can't fetch cold)
	assert.ErrorIs(t, err, types.ErrBlobStoreUnavailable)
	assert.Equal(t, cbor1, result[hash1])
	assert.Equal(t, cbor2, result[hash2])
	_, hasHash3 := result[hash3]
	assert.False(t, hasHash3, "hash3 should not be in result (not in cache, no db)")

	// Verify hot hit metrics
	metrics := cache.Metrics()
	assert.Equal(t, uint64(2), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(1), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheTxBatchEmpty(t *testing.T) {
	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config, nil)

	// Batch resolution with empty hashes returns empty result
	result, err := cache.ResolveTxCborBatch(nil)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result, 0)
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

	cache := NewTieredCborCache(config, nil)

	require.NotNil(t, cache)
	require.NotNil(t, cache.hotUtxo)
	require.NotNil(t, cache.hotTx)
	require.NotNil(t, cache.blockLRU)
	require.NotNil(t, cache.metrics)
}

func TestCacheMetricsPrometheus(t *testing.T) {
	// Create a fresh registry to avoid conflicts
	registry := prometheus.NewRegistry()

	config := CborCacheConfig{
		HotUtxoEntries:  100,
		HotTxEntries:    100,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 10,
	}

	cache := NewTieredCborCache(config, nil)

	// Register Prometheus metrics
	cache.Metrics().Register(registry)

	// Populate hot caches
	var txId [32]byte
	copy(txId[:], []byte("test-tx-id-0000000000000000000"))
	cache.hotUtxo.Put(makeUtxoKey(txId[:], 0), []byte{0x01})

	var txHash [32]byte
	copy(txHash[:], []byte("test-tx-hash-000000000000000000"))
	cache.hotTx.Put(txHash[:], []byte{0x02})

	// Generate some hits
	_, _ = cache.ResolveUtxoCbor(txId[:], 0)      // UTxO hot hit
	_, _ = cache.ResolveTxCbor(txHash[:])         // TX hot hit
	_, _ = cache.ResolveUtxoCbor(txId[:], 99)     // UTxO hot miss (no db)
	_, _ = cache.ResolveTxCbor([]byte("missing")) // TX hot miss (not impl)

	// Verify atomic counters
	metrics := cache.Metrics()
	assert.Equal(t, uint64(1), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(1), metrics.UtxoHotMisses.Load())
	assert.Equal(t, uint64(1), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(1), metrics.TxHotMisses.Load())

	// Gather Prometheus metrics
	mfs, err := registry.Gather()
	require.NoError(t, err)

	// Check that expected metrics are present
	metricNames := make(map[string]float64)
	for _, mf := range mfs {
		if mf.Metric != nil && len(mf.Metric) > 0 {
			metricNames[mf.GetName()] = mf.Metric[0].Counter.GetValue()
		}
	}

	assert.Equal(t, float64(1), metricNames["dingo_cbor_cache_utxo_hot_hits_total"])
	assert.Equal(t, float64(1), metricNames["dingo_cbor_cache_utxo_hot_misses_total"])
	assert.Equal(t, float64(1), metricNames["dingo_cbor_cache_tx_hot_hits_total"])
	assert.Equal(t, float64(1), metricNames["dingo_cbor_cache_tx_hot_misses_total"])
}

func TestCacheMetricsRegisterNil(t *testing.T) {
	// Register with nil registry should not panic
	metrics := &CacheMetrics{}
	metrics.Register(nil)

	// Increment methods should still work (just update atomic counters)
	metrics.IncUtxoHotHit()
	metrics.IncUtxoHotMiss()
	metrics.IncTxHotHit()
	metrics.IncTxHotMiss()
	metrics.IncBlockLRUHit()
	metrics.IncBlockLRUMiss()
	metrics.IncColdExtraction()

	assert.Equal(t, uint64(1), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(1), metrics.UtxoHotMisses.Load())
	assert.Equal(t, uint64(1), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(1), metrics.TxHotMisses.Load())
	assert.Equal(t, uint64(1), metrics.BlockLRUHits.Load())
	assert.Equal(t, uint64(1), metrics.BlockLRUMisses.Load())
	assert.Equal(t, uint64(1), metrics.ColdExtractions.Load())
}
