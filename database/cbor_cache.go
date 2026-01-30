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
	"encoding/binary"
	"errors"
	"sync/atomic"
)

// ErrNotImplemented is returned when functionality is not yet implemented.
var ErrNotImplemented = errors.New("not implemented")

// UtxoRef represents a reference to a UTxO by transaction ID and output index.
type UtxoRef struct {
	TxId      [32]byte
	OutputIdx uint32
}

// CacheMetrics holds atomic counters for cache performance monitoring.
type CacheMetrics struct {
	UtxoHotHits     atomic.Uint64
	UtxoHotMisses   atomic.Uint64
	TxHotHits       atomic.Uint64
	TxHotMisses     atomic.Uint64
	BlockLRUHits    atomic.Uint64
	BlockLRUMisses  atomic.Uint64
	ColdExtractions atomic.Uint64
}

// CborCacheConfig holds configuration for the TieredCborCache.
type CborCacheConfig struct {
	HotUtxoEntries  int   // Number of UTxO CBOR entries in hot cache
	HotTxEntries    int   // Number of TX CBOR entries in hot cache
	HotTxMaxBytes   int64 // Memory limit for TX hot cache (0 = no limit)
	BlockLRUEntries int   // Number of blocks in LRU cache
}

// TieredCborCache orchestrates the tiered cache system for CBOR data resolution.
// It checks hot caches first, then falls back to block extraction.
//
// Cache tiers:
//   - Tier 1: Hot caches (hotUtxo for UTxO CBOR, hotTx for transaction CBOR)
//   - Tier 2: Block LRU cache (shared block cache with pre-computed indexes)
//   - Tier 3: Cold extraction from blob store (not yet wired up)
type TieredCborCache struct {
	hotUtxo  *HotCache      // Tier 1: frequently accessed UTxO CBOR
	hotTx    *HotCache      // Tier 1: frequently accessed transaction CBOR
	blockLRU *BlockLRUCache // Tier 2: recent blocks with index (shared)
	metrics  *CacheMetrics
}

// NewTieredCborCache creates a new TieredCborCache with the given configuration.
func NewTieredCborCache(config CborCacheConfig) *TieredCborCache {
	return &TieredCborCache{
		hotUtxo:  NewHotCache(config.HotUtxoEntries, 0),
		hotTx:    NewHotCache(config.HotTxEntries, config.HotTxMaxBytes),
		blockLRU: NewBlockLRUCache(config.BlockLRUEntries),
		metrics:  &CacheMetrics{},
	}
}

// ResolveUtxoCbor resolves UTxO CBOR data by transaction ID and output index.
// It first checks the hot UTxO cache. On cache miss, it returns ErrNotImplemented
// since the cold path (fetching offsets and extracting from blocks) is not yet wired up.
func (c *TieredCborCache) ResolveUtxoCbor(
	txId []byte,
	outputIdx uint32,
) ([]byte, error) {
	key := makeUtxoKey(txId, outputIdx)

	// Tier 1: Hot cache check
	if cbor, ok := c.hotUtxo.Get(key); ok {
		c.metrics.UtxoHotHits.Add(1)
		return cbor, nil
	}

	// Hot cache miss
	c.metrics.UtxoHotMisses.Add(1)

	// TODO: Implement cold path
	// 1. Get offset from blob store: db.GetUtxoOffset(txId, outputIdx, nil)
	// 2. Check block LRU cache
	// 3. If miss, fetch block from blob store and extract CBOR
	// 4. Populate hot cache with extracted CBOR

	return nil, ErrNotImplemented
}

// ResolveTxCbor resolves transaction CBOR data by transaction hash.
// It first checks the hot TX cache. On cache miss, it returns ErrNotImplemented
// since the cold path (fetching offsets and extracting from blocks) is not yet wired up.
func (c *TieredCborCache) ResolveTxCbor(txHash []byte) ([]byte, error) {
	// Tier 1: Hot cache check
	if cbor, ok := c.hotTx.Get(txHash); ok {
		c.metrics.TxHotHits.Add(1)
		return cbor, nil
	}

	// Hot cache miss
	c.metrics.TxHotMisses.Add(1)

	// TODO: Implement cold path
	// 1. Get offset from blob store: db.GetTxOffset(txHash, nil)
	// 2. Check block LRU cache
	// 3. If miss, fetch block from blob store and extract CBOR
	// 4. Populate hot cache with extracted CBOR

	return nil, ErrNotImplemented
}

// ResolveUtxoCborBatch resolves multiple UTxO CBOR entries in a single batch.
// This is a stub that returns ErrNotImplemented. The full implementation will
// group requests by block to minimize blob store fetches.
func (c *TieredCborCache) ResolveUtxoCborBatch(
	refs []UtxoRef,
) (map[UtxoRef][]byte, error) {
	// TODO: Implement batch resolution
	// 1. Check hot cache for each ref, collect misses
	// 2. Look up offset structs for all misses
	// 3. Group misses by (block_slot, block_hash)
	// 4. Fetch each unique block once
	// 5. Extract all items from each block
	// 6. Populate hot cache and return combined results
	_ = refs // unused for now
	return nil, ErrNotImplemented
}

// Metrics returns the cache metrics for monitoring and observability.
func (c *TieredCborCache) Metrics() *CacheMetrics {
	return c.metrics
}

// makeUtxoKey creates a cache key from transaction ID and output index.
// The key is 36 bytes: 32 bytes for txId + 4 bytes for outputIdx (big-endian).
func makeUtxoKey(txId []byte, outputIdx uint32) []byte {
	key := make([]byte, 36)
	copy(key[:32], txId)
	binary.BigEndian.PutUint32(key[32:36], outputIdx)
	return key
}
