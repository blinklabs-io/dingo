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
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BenchmarkHotCacheGet measures hot cache read performance
func BenchmarkHotCacheGet(b *testing.B) {
	cache := NewHotCache(10000, 0)

	// Pre-populate cache
	key := make([]byte, 36)
	rand.Read(key) //nolint:errcheck
	cbor := make([]byte, 100)
	rand.Read(cbor) //nolint:errcheck
	cache.Put(key, cbor)

	b.ResetTimer()
	for b.Loop() {
		cache.Get(key)
	}
}

// BenchmarkHotCachePut measures hot cache write performance
func BenchmarkHotCachePut(b *testing.B) {
	cache := NewHotCache(10000, 0)

	key := make([]byte, 36)
	cbor := make([]byte, 100)
	rand.Read(cbor) //nolint:errcheck

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Use different keys to avoid overwriting
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		key[3] = byte(i >> 24)
		cache.Put(key, cbor)
	}
}

// BenchmarkHotCacheGetMiss measures hot cache miss performance
func BenchmarkHotCacheGetMiss(b *testing.B) {
	cache := NewHotCache(10000, 0)

	key := make([]byte, 36)

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Use different keys that are not in cache
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		key[3] = byte(i >> 24)
		cache.Get(key)
	}
}

// BenchmarkHotCacheParallelGet measures concurrent read performance
func BenchmarkHotCacheParallelGet(b *testing.B) {
	cache := NewHotCache(10000, 0)

	// Pre-populate with many entries
	for i := range 1000 {
		key := make([]byte, 36)
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		cbor := make([]byte, 100)
		cache.Put(key, cbor)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		key := make([]byte, 36)
		i := 0
		for pb.Next() {
			key[0] = byte(i % 1000)
			key[1] = byte((i % 1000) >> 8)
			cache.Get(key)
			i++
		}
	})
}

// BenchmarkHotCacheCardinality verifies that routine hit and replacement
// costs stay flat as configured cache cardinality grows. Population happens
// before the timer so the benchmark reports only steady-state operations.
func BenchmarkHotCacheCardinality(b *testing.B) {
	for _, cardinality := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("Get/%d", cardinality), func(b *testing.B) {
			cache, key := populatedHotCache(cardinality)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cache.Get(key)
			}
		})

		b.Run(fmt.Sprintf("Put/%d", cardinality), func(b *testing.B) {
			cache, key := populatedHotCache(cardinality)
			value := []byte("replacement-value")
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				cache.Put(key, value)
			}
		})

		b.Run(fmt.Sprintf("Churn/%d", cardinality), func(b *testing.B) {
			cache, _ := populatedHotCache(cardinality)
			value := []byte("replacement-value")
			key := make([]byte, 0, 32)
			nextKey := cardinality
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				key = fmt.Appendf(key[:0], "key-%08d", nextKey)
				cache.Put(key, value)
				nextKey++
			}
		})
	}
}

func populatedHotCache(cardinality int) (*HotCache, []byte) {
	cache := NewHotCache(cardinality, 0)
	value := []byte("cached-value")
	for i := range cardinality {
		cache.Put(fmt.Appendf(nil, "key-%08d", i), value)
	}
	return cache, []byte("key-00000000")
}

// BenchmarkBlockLRUCacheGet measures block LRU cache hit performance
func BenchmarkBlockLRUCacheGet(b *testing.B) {
	cache := NewBlockLRUCache(100)

	// Pre-populate cache
	var hash [32]byte
	rand.Read(hash[:]) //nolint:errcheck
	block := &CachedBlock{
		RawBytes:    make([]byte, 100000),
		TxIndex:     make(map[[32]byte]Location),
		OutputIndex: make(map[OutputKey]Location),
	}
	cache.Put(12345, hash, block)

	b.ResetTimer()
	for b.Loop() {
		cache.Get(12345, hash)
	}
}

// BenchmarkBlockLRUCachePut measures block LRU cache write performance
func BenchmarkBlockLRUCachePut(b *testing.B) {
	cache := NewBlockLRUCache(100)

	var hash [32]byte
	block := &CachedBlock{
		RawBytes:    make([]byte, 100000),
		TxIndex:     make(map[[32]byte]Location),
		OutputIndex: make(map[OutputKey]Location),
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		hash[0] = byte(i)
		hash[1] = byte(i >> 8)
		cache.Put(uint64(i), hash, block)
	}
}

// BenchmarkCachedBlockExtract measures CBOR extraction from cached block
func BenchmarkCachedBlockExtract(b *testing.B) {
	block := &CachedBlock{
		RawBytes:    make([]byte, 200000),
		TxIndex:     make(map[[32]byte]Location),
		OutputIndex: make(map[OutputKey]Location),
	}
	rand.Read(block.RawBytes) //nolint:errcheck

	b.ResetTimer()
	for b.Loop() {
		block.Extract(1000, 256)
	}
}

// BenchmarkCborOffsetEncode measures offset encoding performance
func BenchmarkCborOffsetEncode(b *testing.B) {
	offset := &CborOffset{
		BlockSlot:  12345678,
		BlockHash:  [32]byte{1, 2, 3, 4, 5},
		ByteOffset: 1000,
		ByteLength: 256,
	}

	b.ResetTimer()
	for b.Loop() {
		offset.Encode()
	}
}

// BenchmarkCborOffsetDecode measures offset decoding performance
func BenchmarkCborOffsetDecode(b *testing.B) {
	offset := &CborOffset{
		BlockSlot:  12345678,
		BlockHash:  [32]byte{1, 2, 3, 4, 5},
		ByteOffset: 1000,
		ByteLength: 256,
	}
	encoded := offset.Encode()

	b.ResetTimer()
	for b.Loop() {
		DecodeUtxoOffset(encoded) //nolint:errcheck
	}
}

// BenchmarkTieredCacheHotHit measures tiered cache hot path performance
func BenchmarkTieredCacheHotHit(b *testing.B) {
	config := CborCacheConfig{
		HotUtxoEntries:  10000,
		HotTxEntries:    1000,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 100,
	}
	cache := NewTieredCborCache(config, nil)

	// Pre-populate hot cache
	var txId [32]byte
	rand.Read(txId[:]) //nolint:errcheck
	cbor := make([]byte, 100)
	rand.Read(cbor) //nolint:errcheck
	cache.hotUtxo.Put(makeUtxoKey(txId[:], 0), cbor)

	b.ResetTimer()
	for b.Loop() {
		cache.ResolveUtxoCbor(txId[:], 0) //nolint:errcheck
	}
}

// BenchmarkMakeUtxoKey measures key generation performance
func BenchmarkMakeUtxoKey(b *testing.B) {
	var txId [32]byte
	rand.Read(txId[:]) //nolint:errcheck

	b.ResetTimer()
	for b.Loop() {
		makeUtxoKey(txId[:], 12345)
	}
}

// BenchmarkMetricsIncrement measures metric increment performance
func BenchmarkMetricsIncrement(b *testing.B) {
	metrics := &CacheMetrics{}

	b.ResetTimer()
	for b.Loop() {
		metrics.IncUtxoHotHit()
	}
}

// BenchmarkMetricsIncrementWithPrometheus measures metric increment with
// Prometheus counters registered. This benchmarks the path where counters
// are non-nil and Prometheus Inc() is called on each increment.
func BenchmarkMetricsIncrementWithPrometheus(b *testing.B) {
	metrics := &CacheMetrics{}

	// Register with a local registry to exercise the Prometheus increment path
	registry := prometheus.NewRegistry()
	metrics.Register(registry)

	b.ResetTimer()
	for b.Loop() {
		metrics.IncUtxoHotHit()
	}
}

// BenchmarkBatchResolutionHotHits measures batch resolution with all hot hits
func BenchmarkBatchResolutionHotHits(b *testing.B) {
	config := CborCacheConfig{
		HotUtxoEntries:  10000,
		HotTxEntries:    1000,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 100,
	}
	cache := NewTieredCborCache(config, nil)

	// Pre-populate hot cache with 100 entries
	refs := make([]UtxoRef, 100)
	for i := range 100 {
		refs[i] = UtxoRef{OutputIdx: uint32(i)}
		refs[i].TxId[0] = byte(i)
		refs[i].TxId[1] = byte(i >> 8)

		cbor := make([]byte, 100)
		cache.hotUtxo.Put(makeUtxoKey(refs[i].TxId[:], refs[i].OutputIdx), cbor)
	}

	b.ResetTimer()
	for b.Loop() {
		cache.ResolveUtxoCborBatch(refs) //nolint:errcheck
	}
}

// BenchmarkTxBatchResolutionHotHits measures TX batch resolution with all hot hits
func BenchmarkTxBatchResolutionHotHits(b *testing.B) {
	config := CborCacheConfig{
		HotUtxoEntries:  10000,
		HotTxEntries:    1000,
		HotTxMaxBytes:   1024 * 1024,
		BlockLRUEntries: 100,
	}
	cache := NewTieredCborCache(config, nil)

	// Pre-populate hot cache with 100 TX entries
	hashes := make([][32]byte, 100)
	for i := range 100 {
		hashes[i][0] = byte(i)
		hashes[i][1] = byte(i >> 8)

		cbor := make([]byte, 500) // TX CBOR is typically larger than UTxO
		cache.hotTx.Put(hashes[i][:], cbor)
	}

	b.ResetTimer()
	for b.Loop() {
		cache.ResolveTxCborBatch(hashes) //nolint:errcheck
	}
}

func TestNewTieredCborCache(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	result, err := cache.ResolveTxCbor(nil, txHash[:])

	require.NoError(t, err)
	assert.Equal(t, testCbor, result)

	// Verify metrics show a hot hit
	metrics := cache.Metrics()
	assert.Equal(t, uint64(1), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(0), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheHotMissUtxo(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	result, err := cache.ResolveTxCbor(nil, txHash[:])

	assert.ErrorIs(t, err, types.ErrBlobStoreUnavailable)
	assert.Nil(t, result)

	// Verify metrics show a hot miss
	metrics := cache.Metrics()
	assert.Equal(t, uint64(0), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(1), metrics.TxHotMisses.Load())
}

func TestResolveTxCborUsesCallerTxn(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	var txHash [32]byte
	copy(txHash[:], []byte("active-txn-visible-tx"))
	var blockHash [32]byte
	copy(blockHash[:], []byte("active-txn-visible-block"))
	txBodyCbor := []byte{0x82, 0x01, 0x02}
	blockCbor := append([]byte("prefix-"), txBodyCbor...)
	offset := CborOffset{
		BlockSlot:  42,
		BlockHash:  blockHash,
		ByteOffset: uint32(len(blockCbor) - len(txBodyCbor)),
		ByteLength: uint32(len(txBodyCbor)),
	}

	txn := db.BlobTxn(true)
	defer txn.Release()
	require.NoError(t, db.Blob().SetBlock(
		txn.Blob(),
		offset.BlockSlot,
		blockHash[:],
		blockCbor,
		1,
		0,
		0,
		nil,
	))
	require.NoError(t, db.Blob().SetTx(
		txn.Blob(),
		txHash[:],
		EncodeTxOffset(&offset),
	))

	result, err := db.CborCache().ResolveTxCbor(txn, txHash[:])

	require.NoError(t, err)
	assert.Equal(t, txBodyCbor, result)
}

func TestTieredCborCacheMetrics(t *testing.T) {
	t.Parallel()

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
	_, _ = cache.ResolveTxCbor(nil, txHash1[:]) // hit
	_, _ = cache.ResolveTxCbor(nil, txHash2[:]) // miss
	_, _ = cache.ResolveTxCbor(nil, txHash2[:]) // miss

	// Verify counts
	assert.Equal(t, uint64(1), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(2), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheBatchHotHits(t *testing.T) {
	t.Parallel()

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
	assert.False(
		t,
		hasRef3,
		"ref3 should not be in result (not in cache, no db)",
	)

	// Verify hot hit metrics
	metrics := cache.Metrics()
	assert.Equal(t, uint64(2), metrics.UtxoHotHits.Load())
	assert.Equal(t, uint64(1), metrics.UtxoHotMisses.Load())
}

func TestTieredCborCacheBatchEmpty(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	assert.False(
		t,
		hasHash3,
		"hash3 should not be in result (not in cache, no db)",
	)

	// Verify hot hit metrics
	metrics := cache.Metrics()
	assert.Equal(t, uint64(2), metrics.TxHotHits.Load())
	assert.Equal(t, uint64(1), metrics.TxHotMisses.Load())
}

func TestTieredCborCacheTxBatchEmpty(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	_, _ = cache.ResolveUtxoCbor(txId[:], 0)           // UTxO hot hit
	_, _ = cache.ResolveTxCbor(nil, txHash[:])         // TX hot hit
	_, _ = cache.ResolveUtxoCbor(txId[:], 99)          // UTxO hot miss (no db)
	_, _ = cache.ResolveTxCbor(nil, []byte("missing")) // TX hot miss (not impl)

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

	assert.Equal(
		t,
		float64(1),
		metricNames["dingo_cbor_cache_utxo_hot_hits_total"],
	)
	assert.Equal(
		t,
		float64(1),
		metricNames["dingo_cbor_cache_utxo_hot_misses_total"],
	)
	assert.Equal(
		t,
		float64(1),
		metricNames["dingo_cbor_cache_tx_hot_hits_total"],
	)
	assert.Equal(
		t,
		float64(1),
		metricNames["dingo_cbor_cache_tx_hot_misses_total"],
	)
}

func TestCacheMetricsRegisterNil(t *testing.T) {
	t.Parallel()

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

func TestSetGenesisCborWarmsCacheForOpenBatchTransaction(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		CacheConfig: CborCacheConfig{
			BlockLRUEntries: 16,
		},
	})
	require.NoError(t, err)
	store := db.Blob()
	require.NotNil(t, store)

	chunkTxn := db.BlobTxn(true)
	t.Cleanup(chunkTxn.Release)
	var txID [32]byte
	txID[0] = 1
	var blockHash [32]byte
	blockHash[0] = 2
	const blockSlot = 42
	want := []byte{0x82, 0x01, 0x02}
	blockCbor := append([]byte{0x00}, want...)
	offset := &CborOffset{
		BlockSlot:  blockSlot,
		BlockHash:  blockHash,
		ByteOffset: 1,
		ByteLength: uint32(len(want)),
	}
	require.NoError(t, store.SetUtxo(
		chunkTxn.Blob(),
		txID[:],
		0,
		EncodeUtxoOffset(offset),
	))

	// The block commits in its own transaction while the chunk transaction
	// remains open. Resolving the offset must not read the new blob key through
	// the older Badger transaction.
	require.NoError(t, db.SetGenesisCbor(blockSlot, blockHash[:], blockCbor, nil))
	blockCbor[1] = 0xff

	got, err := db.CborCache().ResolveUtxoCbor(txID[:], 0, chunkTxn)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, uint64(1), db.CborCache().Metrics().BlockLRUHits.Load())
	require.Zero(t, db.CborCache().Metrics().ColdExtractions.Load())
	require.NoError(t, chunkTxn.Commit())
}

func TestResolveUtxoCborUsesFreshSnapshotAfterLRUEviction(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{
		DataDir: t.TempDir(),
		CacheConfig: CborCacheConfig{
			BlockLRUEntries: 1,
		},
	})
	require.NoError(t, err)
	store := db.Blob()
	require.NotNil(t, store)

	chunkTxn := db.BlobTxn(true)
	t.Cleanup(chunkTxn.Release)
	var txID [32]byte
	txID[0] = 1
	var blockHash [32]byte
	blockHash[0] = 2
	const blockSlot = 42
	want := []byte{0x82, 0x01, 0x02}
	blockCbor := append([]byte{0x00}, want...)
	offset := &CborOffset{
		BlockSlot:  blockSlot,
		BlockHash:  blockHash,
		ByteOffset: 1,
		ByteLength: uint32(len(want)),
	}
	require.NoError(t, store.SetUtxo(
		chunkTxn.Blob(),
		txID[:],
		0,
		EncodeUtxoOffset(offset),
	))
	require.NoError(t, db.SetGenesisCbor(blockSlot, blockHash[:], blockCbor, nil))
	chunkTxn.MarkBlockCborCommittedSeparately(blockSlot, blockHash)

	// A fresh blob snapshot must resolve this after the shared LRU evicts it.
	var otherHash [32]byte
	otherHash[0] = 3
	db.CborCache().blockLRU.Put(blockSlot+1, otherHash, newCachedBlock([]byte{0x01}))
	blockCbor[1] = 0xff
	_, ok := db.CborCache().blockLRU.Get(blockSlot, blockHash)
	require.False(t, ok)

	got, err := db.CborCache().ResolveUtxoCbor(txID[:], 0, chunkTxn)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, uint64(1), db.CborCache().Metrics().ColdExtractions.Load())
	_, ok = db.CborCache().hotUtxo.Get(makeUtxoKey(txID[:], 0))
	require.False(t, ok)
	require.NoError(t, chunkTxn.Commit())
	require.Empty(t, chunkTxn.separatelyCommittedBlocks)
}

func TestResolveUtxoCborDoesNotCacheRolledBackUtxo(t *testing.T) {
	t.Parallel()

	db, err := newTestDatabase(t, &Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	store := db.Blob()
	require.NotNil(t, store)

	txn := db.BlobTxn(true)
	t.Cleanup(txn.Release)
	var txID [32]byte
	txID[0] = 1
	want := []byte{0x82, 0x01, 0x02}
	require.NoError(t, store.SetUtxo(txn.Blob(), txID[:], 0, want))

	got, err := db.CborCache().ResolveUtxoCbor(txID[:], 0, txn)
	require.NoError(t, err)
	require.Equal(t, want, got)
	_, ok := db.CborCache().hotUtxo.Get(makeUtxoKey(txID[:], 0))
	require.False(t, ok)
	require.NoError(t, txn.Rollback())

	_, err = db.CborCache().ResolveUtxoCbor(txID[:], 0)
	require.Error(t, err)
}
