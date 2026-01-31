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
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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
	for i := 0; i < 1000; i++ {
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
	for i := 0; i < 100; i++ {
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
	for i := 0; i < 100; i++ {
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
