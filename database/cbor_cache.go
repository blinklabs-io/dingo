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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

	// Prometheus metrics (nil until Register is called)
	utxoHotHitsCounter     prometheus.Counter
	utxoHotMissesCounter   prometheus.Counter
	txHotHitsCounter       prometheus.Counter
	txHotMissesCounter     prometheus.Counter
	blockLRUHitsCounter    prometheus.Counter
	blockLRUMissesCounter  prometheus.Counter
	coldExtractionsCounter prometheus.Counter

	// registerOnce ensures Prometheus metrics are only registered once
	registerOnce sync.Once
}

// Register registers Prometheus metrics with the given registry.
// If registry is nil, this is a no-op. This method is idempotent;
// subsequent calls after the first successful registration are no-ops.
func (m *CacheMetrics) Register(registry prometheus.Registerer) {
	if registry == nil {
		return
	}

	m.registerOnce.Do(func() {
		factory := promauto.With(registry)

		m.utxoHotHitsCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_utxo_hot_hits_total",
			Help: "Total number of UTxO CBOR hot cache hits",
		})

		m.utxoHotMissesCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_utxo_hot_misses_total",
			Help: "Total number of UTxO CBOR hot cache misses",
		})

		m.txHotHitsCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_tx_hot_hits_total",
			Help: "Total number of TX CBOR hot cache hits",
		})

		m.txHotMissesCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_tx_hot_misses_total",
			Help: "Total number of TX CBOR hot cache misses",
		})

		m.blockLRUHitsCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_block_lru_hits_total",
			Help: "Total number of block LRU cache hits",
		})

		m.blockLRUMissesCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_block_lru_misses_total",
			Help: "Total number of block LRU cache misses",
		})

		m.coldExtractionsCounter = factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_cbor_cache_cold_extractions_total",
			Help: "Total number of cold CBOR extractions from blob store",
		})
	})
}

// IncUtxoHotHit increments the UTxO hot cache hit counter.
func (m *CacheMetrics) IncUtxoHotHit() {
	m.UtxoHotHits.Add(1)
	if m.utxoHotHitsCounter != nil {
		m.utxoHotHitsCounter.Inc()
	}
}

// IncUtxoHotMiss increments the UTxO hot cache miss counter.
func (m *CacheMetrics) IncUtxoHotMiss() {
	m.UtxoHotMisses.Add(1)
	if m.utxoHotMissesCounter != nil {
		m.utxoHotMissesCounter.Inc()
	}
}

// IncTxHotHit increments the TX hot cache hit counter.
func (m *CacheMetrics) IncTxHotHit() {
	m.TxHotHits.Add(1)
	if m.txHotHitsCounter != nil {
		m.txHotHitsCounter.Inc()
	}
}

// IncTxHotMiss increments the TX hot cache miss counter.
func (m *CacheMetrics) IncTxHotMiss() {
	m.TxHotMisses.Add(1)
	if m.txHotMissesCounter != nil {
		m.txHotMissesCounter.Inc()
	}
}

// IncBlockLRUHit increments the block LRU cache hit counter.
func (m *CacheMetrics) IncBlockLRUHit() {
	m.BlockLRUHits.Add(1)
	if m.blockLRUHitsCounter != nil {
		m.blockLRUHitsCounter.Inc()
	}
}

// IncBlockLRUMiss increments the block LRU cache miss counter.
func (m *CacheMetrics) IncBlockLRUMiss() {
	m.BlockLRUMisses.Add(1)
	if m.blockLRUMissesCounter != nil {
		m.blockLRUMissesCounter.Inc()
	}
}

// IncColdExtraction increments the cold extraction counter.
func (m *CacheMetrics) IncColdExtraction() {
	m.ColdExtractions.Add(1)
	if m.coldExtractionsCounter != nil {
		m.coldExtractionsCounter.Inc()
	}
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
//   - Tier 3: Cold extraction from blob store
type TieredCborCache struct {
	db       *Database      // Database for blob store access
	hotUtxo  *HotCache      // Tier 1: frequently accessed UTxO CBOR
	hotTx    *HotCache      // Tier 1: frequently accessed transaction CBOR
	blockLRU *BlockLRUCache // Tier 2: recent blocks with index (shared)
	metrics  *CacheMetrics
}

// NewTieredCborCache creates a new TieredCborCache with the given configuration.
// The db parameter provides access to the blob store for cold path resolution.
func NewTieredCborCache(config CborCacheConfig, db *Database) *TieredCborCache {
	return &TieredCborCache{
		db:       db,
		hotUtxo:  NewHotCache(config.HotUtxoEntries, 0),
		hotTx:    NewHotCache(config.HotTxEntries, config.HotTxMaxBytes),
		blockLRU: NewBlockLRUCache(config.BlockLRUEntries),
		metrics:  &CacheMetrics{},
	}
}

// ResolveUtxoCbor resolves UTxO CBOR data by transaction ID and output index.
// It checks caches in order: hot UTxO cache, block LRU cache, then blob store.
// An optional blob transaction can be provided to see uncommitted writes within
// the same transaction (important for intra-batch UTxO lookups during validation).
func (c *TieredCborCache) ResolveUtxoCbor(
	txId []byte,
	outputIdx uint32,
	blobTxn ...types.Txn,
) ([]byte, error) {
	key := makeUtxoKey(txId, outputIdx)

	// Tier 1: Hot cache check
	if cbor, ok := c.hotUtxo.Get(key); ok {
		c.metrics.IncUtxoHotHit()
		return cbor, nil
	}

	// Hot cache miss
	c.metrics.IncUtxoHotMiss()

	// Tier 3: Cold path - fetch from blob store
	if c.db == nil {
		return nil, types.ErrBlobStoreUnavailable
	}

	blob := c.db.Blob()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}

	// Use provided transaction if available, otherwise create a new one
	var txn types.Txn
	var ownedTxn bool
	if len(blobTxn) > 0 && blobTxn[0] != nil {
		txn = blobTxn[0]
	} else {
		txn = blob.NewTransaction(false)
		ownedTxn = true
		defer func() {
			if ownedTxn {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}

	utxoData, err := blob.GetUtxo(txn, txId, outputIdx)
	if err != nil {
		return nil, fmt.Errorf("get utxo from blob: %w", err)
	}

	// Check if this is offset-based storage
	if !IsUtxoOffsetStorage(utxoData) {
		// Legacy format: raw CBOR data - populate hot cache and return
		c.hotUtxo.Put(key, utxoData)
		return utxoData, nil
	}

	// Decode the offset reference
	offset, err := DecodeUtxoOffset(utxoData)
	if err != nil {
		return nil, fmt.Errorf("decode utxo offset: %w", err)
	}

	// Tier 2: Check block LRU cache
	if cachedBlock, ok := c.blockLRU.Get(offset.BlockSlot, offset.BlockHash); ok {
		c.metrics.IncBlockLRUHit()
		cbor := cachedBlock.Extract(offset.ByteOffset, offset.ByteLength)
		if cbor != nil {
			// Populate hot cache
			c.hotUtxo.Put(key, cbor)
			return cbor, nil
		}
	}

	// Block LRU miss
	c.metrics.IncBlockLRUMiss()

	// Fetch block from blob store
	blockCbor, _, err := blob.GetBlock(txn, offset.BlockSlot, offset.BlockHash[:])
	if err != nil {
		return nil, fmt.Errorf("get block for utxo extraction: %w", err)
	}

	c.metrics.IncColdExtraction()

	// Create cached block and add to LRU
	cachedBlock := newCachedBlock(blockCbor)
	c.blockLRU.Put(offset.BlockSlot, offset.BlockHash, cachedBlock)

	// Extract the UTxO CBOR from the block
	cbor := cachedBlock.Extract(offset.ByteOffset, offset.ByteLength)
	if cbor == nil {
		return nil, fmt.Errorf(
			"utxo offset out of bounds: offset=%d, length=%d, block_size=%d",
			offset.ByteOffset,
			offset.ByteLength,
			len(blockCbor),
		)
	}

	// Populate hot cache
	c.hotUtxo.Put(key, cbor)
	return cbor, nil
}

// ResolveTxCbor resolves transaction body CBOR data by transaction hash.
// It checks caches in order: hot TX cache, block LRU cache, then blob store.
//
// NOTE: This returns only the transaction BODY CBOR, not a complete standalone
// transaction. Cardano blocks store bodies and witnesses in separate arrays,
// so reconstructing a complete transaction ([body, witness, is_valid, aux_data])
// would require fetching multiple components. Use tx.Cbor() from the parsed
// block if you need complete transaction CBOR for decoding.
func (c *TieredCborCache) ResolveTxCbor(txHash []byte) ([]byte, error) {
	// Tier 1: Hot cache check
	if cbor, ok := c.hotTx.Get(txHash); ok {
		c.metrics.IncTxHotHit()
		return cbor, nil
	}

	// Hot cache miss
	c.metrics.IncTxHotMiss()

	// Tier 3: Cold path - fetch from blob store
	if c.db == nil {
		return nil, types.ErrBlobStoreUnavailable
	}

	blob := c.db.Blob()
	if blob == nil {
		return nil, types.ErrBlobStoreUnavailable
	}

	// Get TX offset data from blob store
	txn := blob.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	txData, err := blob.GetTx(txn, txHash)
	if err != nil {
		return nil, fmt.Errorf("get tx from blob: %w", err)
	}

	// Check if this is offset-based storage
	if !IsTxOffsetStorage(txData) {
		// Legacy format: raw CBOR data - populate hot cache and return
		c.hotTx.Put(txHash, txData)
		return txData, nil
	}

	// Decode the offset reference
	offset, err := DecodeTxOffset(txData)
	if err != nil {
		return nil, fmt.Errorf("decode tx offset: %w", err)
	}

	// Tier 2: Check block LRU cache
	if cachedBlock, ok := c.blockLRU.Get(offset.BlockSlot, offset.BlockHash); ok {
		c.metrics.IncBlockLRUHit()
		cbor := cachedBlock.Extract(offset.ByteOffset, offset.ByteLength)
		if cbor != nil {
			// Populate hot cache
			c.hotTx.Put(txHash, cbor)
			return cbor, nil
		}
	}

	// Block LRU miss
	c.metrics.IncBlockLRUMiss()

	// Fetch block from blob store
	blockCbor, _, err := blob.GetBlock(txn, offset.BlockSlot, offset.BlockHash[:])
	if err != nil {
		return nil, fmt.Errorf("get block for tx extraction: %w", err)
	}

	c.metrics.IncColdExtraction()

	// Create cached block and add to LRU
	cachedBlock := newCachedBlock(blockCbor)
	c.blockLRU.Put(offset.BlockSlot, offset.BlockHash, cachedBlock)

	// Extract the TX CBOR from the block
	cbor := cachedBlock.Extract(offset.ByteOffset, offset.ByteLength)
	if cbor == nil {
		return nil, fmt.Errorf(
			"tx offset out of bounds: offset=%d, length=%d, block_size=%d",
			offset.ByteOffset,
			offset.ByteLength,
			len(blockCbor),
		)
	}

	// Populate hot cache
	c.hotTx.Put(txHash, cbor)
	return cbor, nil
}

// ResolveUtxoCborBatch resolves multiple UTxO CBOR entries in a single batch.
// It groups requests by block to minimize blob store fetches.
func (c *TieredCborCache) ResolveUtxoCborBatch(
	refs []UtxoRef,
) (map[UtxoRef][]byte, error) {
	result := make(map[UtxoRef][]byte, len(refs))

	if len(refs) == 0 {
		return result, nil
	}

	// Phase 1: Check hot cache for each ref, collect misses
	var misses []UtxoRef
	for _, ref := range refs {
		key := makeUtxoKey(ref.TxId[:], ref.OutputIdx)
		if cbor, ok := c.hotUtxo.Get(key); ok {
			c.metrics.IncUtxoHotHit()
			result[ref] = cbor
		} else {
			c.metrics.IncUtxoHotMiss()
			misses = append(misses, ref)
		}
	}

	if len(misses) == 0 {
		return result, nil
	}

	// Check database availability
	if c.db == nil {
		return result, types.ErrBlobStoreUnavailable
	}

	blob := c.db.Blob()
	if blob == nil {
		return result, types.ErrBlobStoreUnavailable
	}

	txn := blob.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	// Phase 2: Look up offset structs for all misses, group by block
	type blockGroup struct {
		slot uint64
		hash [32]byte
		refs []struct {
			ref    UtxoRef
			offset CborOffset
		}
	}
	blockGroups := make(map[blockKey]*blockGroup)

	for _, ref := range misses {
		utxoData, err := blob.GetUtxo(txn, ref.TxId[:], ref.OutputIdx)
		if err != nil {
			// Skip UTxOs that can't be found, propagate other errors
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				continue
			}
			return result, fmt.Errorf(
				"get utxo %x#%d: %w",
				ref.TxId[:8],
				ref.OutputIdx,
				err,
			)
		}

		// Check if this is offset-based storage
		if !IsUtxoOffsetStorage(utxoData) {
			// Legacy format: raw CBOR data
			key := makeUtxoKey(ref.TxId[:], ref.OutputIdx)
			c.hotUtxo.Put(key, utxoData)
			result[ref] = utxoData
			continue
		}

		// Decode the offset reference
		offset, err := DecodeUtxoOffset(utxoData)
		if err != nil {
			return result, fmt.Errorf(
				"decode utxo offset %x#%d: %w",
				ref.TxId[:8],
				ref.OutputIdx,
				err,
			)
		}

		// Group by block
		bk := blockKey{slot: offset.BlockSlot, hash: offset.BlockHash}
		if blockGroups[bk] == nil {
			blockGroups[bk] = &blockGroup{
				slot: offset.BlockSlot,
				hash: offset.BlockHash,
			}
		}
		blockGroups[bk].refs = append(blockGroups[bk].refs, struct {
			ref    UtxoRef
			offset CborOffset
		}{ref: ref, offset: *offset})
	}

	// Phase 3: Fetch each unique block once and extract all items
	for bk, group := range blockGroups {
		// Check block LRU cache first
		cachedBlock, ok := c.blockLRU.Get(group.slot, group.hash)
		if ok {
			c.metrics.IncBlockLRUHit()
		} else {
			c.metrics.IncBlockLRUMiss()

			// Fetch block from blob store
			blockCbor, _, err := blob.GetBlock(txn, group.slot, group.hash[:])
			if err != nil {
				return nil, fmt.Errorf(
					"get block for utxo batch extraction (slot=%d, hash=%x): %w",
					group.slot,
					group.hash,
					err,
				)
			}

			c.metrics.IncColdExtraction()

			// Create cached block and add to LRU
			cachedBlock = newCachedBlock(blockCbor)
			c.blockLRU.Put(bk.slot, bk.hash, cachedBlock)
		}

		// Extract all UTxOs from this block
		for _, item := range group.refs {
			cbor := cachedBlock.Extract(item.offset.ByteOffset, item.offset.ByteLength)
			if cbor == nil {
				return nil, fmt.Errorf(
					"utxo batch extraction failed: offset out of bounds "+
						"(slot=%d, hash=%x, offset=%d, length=%d)",
					group.slot,
					group.hash,
					item.offset.ByteOffset,
					item.offset.ByteLength,
				)
			}

			// Populate hot cache
			key := makeUtxoKey(item.ref.TxId[:], item.ref.OutputIdx)
			c.hotUtxo.Put(key, cbor)
			result[item.ref] = cbor
		}
	}

	return result, nil
}

// ResolveTxCborBatch resolves multiple transaction CBOR entries in a single batch.
// It groups requests by block to minimize blob store fetches.
func (c *TieredCborCache) ResolveTxCborBatch(
	txHashes [][32]byte,
) (map[[32]byte][]byte, error) {
	result := make(map[[32]byte][]byte, len(txHashes))

	if len(txHashes) == 0 {
		return result, nil
	}

	// Phase 1: Check hot cache for each hash, collect misses
	var misses [][32]byte
	for _, txHash := range txHashes {
		if cbor, ok := c.hotTx.Get(txHash[:]); ok {
			c.metrics.IncTxHotHit()
			result[txHash] = cbor
		} else {
			c.metrics.IncTxHotMiss()
			misses = append(misses, txHash)
		}
	}

	if len(misses) == 0 {
		return result, nil
	}

	// Check database availability
	if c.db == nil {
		return result, types.ErrBlobStoreUnavailable
	}

	blob := c.db.Blob()
	if blob == nil {
		return result, types.ErrBlobStoreUnavailable
	}

	txn := blob.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	// Phase 2: Look up offset structs for all misses, group by block
	type txItem struct {
		hash   [32]byte
		offset CborOffset
	}
	type blockGroup struct {
		slot uint64
		hash [32]byte
		txs  []txItem
	}
	blockGroups := make(map[blockKey]*blockGroup)

	for _, txHash := range misses {
		txData, err := blob.GetTx(txn, txHash[:])
		if err != nil {
			// Skip TXs that can't be found, propagate other errors
			if errors.Is(err, types.ErrBlobKeyNotFound) {
				continue
			}
			return result, fmt.Errorf("get tx %x: %w", txHash[:8], err)
		}

		// Check if this is offset-based storage
		if !IsTxOffsetStorage(txData) {
			// Legacy format: raw CBOR data
			c.hotTx.Put(txHash[:], txData)
			result[txHash] = txData
			continue
		}

		// Decode the offset reference
		offset, err := DecodeTxOffset(txData)
		if err != nil {
			return result, fmt.Errorf("decode tx offset %x: %w", txHash[:8], err)
		}

		// Group by block
		bk := blockKey{slot: offset.BlockSlot, hash: offset.BlockHash}
		if blockGroups[bk] == nil {
			blockGroups[bk] = &blockGroup{
				slot: offset.BlockSlot,
				hash: offset.BlockHash,
			}
		}
		blockGroups[bk].txs = append(blockGroups[bk].txs, txItem{
			hash:   txHash,
			offset: *offset,
		})
	}

	// Phase 3: Fetch each unique block once and extract all items
	for bk, group := range blockGroups {
		// Check block LRU cache first
		cachedBlock, ok := c.blockLRU.Get(group.slot, group.hash)
		if ok {
			c.metrics.IncBlockLRUHit()
		} else {
			c.metrics.IncBlockLRUMiss()

			// Fetch block from blob store
			blockCbor, _, err := blob.GetBlock(txn, group.slot, group.hash[:])
			if err != nil {
				return nil, fmt.Errorf(
					"get block for tx batch extraction (slot=%d, hash=%x): %w",
					group.slot,
					group.hash,
					err,
				)
			}

			c.metrics.IncColdExtraction()

			// Create cached block and add to LRU
			cachedBlock = newCachedBlock(blockCbor)
			c.blockLRU.Put(bk.slot, bk.hash, cachedBlock)
		}

		// Extract all TXs from this block
		for _, item := range group.txs {
			cbor := cachedBlock.Extract(item.offset.ByteOffset, item.offset.ByteLength)
			if cbor == nil {
				return nil, fmt.Errorf(
					"tx batch extraction failed: offset out of bounds "+
						"(slot=%d, hash=%x, offset=%d, length=%d)",
					group.slot,
					group.hash,
					item.offset.ByteOffset,
					item.offset.ByteLength,
				)
			}

			// Populate hot cache
			c.hotTx.Put(item.hash[:], cbor)
			result[item.hash] = cbor
		}
	}

	return result, nil
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

// newCachedBlock creates a new CachedBlock with the given raw CBOR data.
// The TxIndex and OutputIndex maps are initialized empty for later population.
func newCachedBlock(rawBytes []byte) *CachedBlock {
	return &CachedBlock{
		RawBytes:    rawBytes,
		TxIndex:     make(map[[32]byte]Location),
		OutputIndex: make(map[OutputKey]Location),
	}
}
