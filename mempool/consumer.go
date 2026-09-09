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

package mempool

import (
	"sync"
)

type MempoolConsumer struct {
	mempool  *Mempool
	cache    map[string]*MempoolTransaction
	done     chan struct{}
	doneOnce sync.Once
	// cacheSlot signals that a cache slot was freed, waking a blocking NextTx
	// that parked on a full cache. Buffered by one and signaled without
	// blocking: a pending wake-up is all a waiter needs, since it re-checks the
	// cache after waking. A channel rather than a sync.Cond so the wait can also
	// select on mempool shutdown.
	cacheSlot       chan struct{}
	cacheLimit      int
	cacheBytes      int64
	cacheLimitBytes int64
	nextTxIdx       int
	// offered is the FIFO of tx hashes advertised to the peer via ReplyTxIds
	// and not yet acknowledged. TxSubmission acknowledges advertised ids in
	// the order they were offered, so AcknowledgeOffered must forget exactly
	// this prefix rather than the whole cache. Guarded by cacheMutex.
	offered []string
	// offeredSet mirrors offered for O(1) membership checks. A hash can be
	// evicted from cache (served) while still outstanding in offered; if the
	// underlying pool then resurfaces the same hash at a later cursor
	// position (e.g. a revalidation swap or a remove-then-readmit), offered
	// must not gain a second entry for it, or an ack would consume the
	// duplicate's slot and evict a different, still-unacknowledged body.
	offeredSet  map[string]struct{}
	cacheMutex  sync.Mutex
	nextTxIdxMu sync.Mutex
	// onWaitForTx is a test-only hook invoked after a blocking NextTx has
	// subscribed for additions and is ready to be cancelled.
	onWaitForTx func()
	// onCacheCleared is a test-only hook invoked after ClearCache releases
	// the cache lifecycle lock.
	onCacheCleared func()
}

func newConsumer(
	mempool *Mempool,
	cacheLimit int,
	cacheLimitBytes int64,
) *MempoolConsumer {
	if cacheLimit <= 0 {
		cacheLimit = DefaultConsumerCacheSize
	}
	if cacheLimitBytes <= 0 {
		cacheLimitBytes = mempool.config.MempoolCapacity /
			defaultConsumerCacheShare
		// A budget this small would permanently exclude ordinary transaction
		// bodies from relay with no other visible symptom, so the derived
		// default (unlike an operator's explicit ConsumerCacheBytes) is
		// floored to a size that comfortably holds one.
		if cacheLimitBytes < minConsumerCacheBytes {
			mempool.logger.Warn(
				"derived default consumer relay cache byte budget is below "+
					"the floor; raising it to avoid silently excluding "+
					"ordinary transaction bodies from relay",
				"component", "mempool",
				"mempool_capacity", mempool.config.MempoolCapacity,
				"derived_bytes", cacheLimitBytes,
				"floor_bytes", int64(minConsumerCacheBytes),
			)
			cacheLimitBytes = minConsumerCacheBytes
		}
	}
	return &MempoolConsumer{
		mempool:         mempool,
		cache:           make(map[string]*MempoolTransaction),
		offeredSet:      make(map[string]struct{}),
		done:            make(chan struct{}),
		cacheSlot:       make(chan struct{}, 1),
		cacheLimit:      cacheLimit,
		cacheLimitBytes: cacheLimitBytes,
	}
}

// cancel releases a blocking NextTx when its connection no longer owns the
// consumer. It is safe to call more than once because connection cleanup can
// race with other lifecycle paths.
func (m *MempoolConsumer) cancel() {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		m.doneOnce.Do(func() { close(m.done) })
	}
}

func (m *MempoolConsumer) NextTx(blocking bool) *MempoolTransaction {
	if m == nil {
		return nil
	}

	for {
		select {
		case <-m.done:
			return nil
		default:
		}

		m.mempool.RLock()
		m.nextTxIdxMu.Lock()

		// Check if we have a transaction available
		if m.nextTxIdx < len(m.mempool.transactions) {
			poolTx := m.mempool.transactions[m.nextTxIdx]
			if poolTx != nil {
				// A body larger than this consumer's total byte budget can never
				// become cacheable. Skip it so it cannot permanently pin the
				// cursor and prevent later transactions from being relayed.
				if int64(len(poolTx.Cbor)) > m.cacheLimitBytes {
					hash := poolTx.Hash
					size := len(poolTx.Cbor)
					limit := m.cacheLimitBytes
					m.nextTxIdx++
					m.nextTxIdxMu.Unlock()
					m.mempool.RUnlock()
					m.mempool.metrics.consumerCacheBytesSkipped.Inc()
					m.mempool.logger.Warn(
						"skipping transaction that exceeds this consumer's "+
							"relay cache byte budget; it will never be "+
							"relayed to this consumer",
						"component", "mempool",
						"tx_hash", hash,
						"tx_size_bytes", size,
						"cache_limit_bytes", limit,
					)
					continue
				}
				// The pool can resurface the same hash at a later cursor
				// position -- a revalidation swap or a remove-then-readmit
				// -- while an earlier offer of it is still outstanding
				// (served but not yet acknowledged, or still resident).
				// Re-offering it would create a second, ambiguous entry in
				// the peer's FIFO ack window, so advance past it instead of
				// resending.
				if m.isOffered(poolTx.Hash) {
					m.nextTxIdx++
					m.nextTxIdxMu.Unlock()
					m.mempool.RUnlock()
					continue
				}
				cached, aggregateChanged := m.cacheTransaction(poolTx)
				if !cached {
					m.nextTxIdxMu.Unlock()
					m.mempool.RUnlock()
					if !blocking {
						return nil
					}
					select {
					case <-m.cacheSlot:
						continue
					case <-aggregateChanged:
						continue
					case <-m.done:
						return nil
					case <-m.mempool.done:
						return nil
					}
				}
				// Clone while holding the pool read lock so neither the caller
				// nor the consumer cache shares mutable CBOR with pool storage.
				nextTx := cloneMempoolTransaction(poolTx)
				// Increment next TX index atomically with reading it
				m.nextTxIdx++
				m.nextTxIdxMu.Unlock()
				m.mempool.RUnlock()

				return nextTx
			}
			m.nextTxIdx++
			m.nextTxIdxMu.Unlock()
			m.mempool.RUnlock()
			continue
		}

		// No transaction available
		if !blocking {
			m.nextTxIdxMu.Unlock()
			m.mempool.RUnlock()
			return nil
		}

		// If eventBus is nil, fall back to non-blocking behavior
		if m.mempool.eventBus == nil {
			m.nextTxIdxMu.Unlock()
			m.mempool.RUnlock()
			return nil
		}

		// Wait for a transaction to be added
		addTxSubId, addTxChan := m.mempool.eventBus.Subscribe(
			AddTransactionEventType,
		)
		if m.onWaitForTx != nil {
			m.onWaitForTx()
		}
		m.nextTxIdxMu.Unlock()
		m.mempool.RUnlock()

		// Block until an event arrives or shutdown is signaled
		select {
		case <-addTxChan:
			m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
			// Loop back to check if transaction is available
			// This naturally handles the case of multiple rapid additions
		case <-m.done:
			m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
			return nil
		case <-m.mempool.done:
			// Mempool is shutting down, unsubscribe and exit
			m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
			return nil
		}
	}
}

// isOffered reports whether hash is already advertised to the peer and
// outstanding: cached, or served but not yet acknowledged.
func (m *MempoolConsumer) isOffered(hash string) bool {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	_, offered := m.offeredSet[hash]
	return offered
}

// cacheTransaction reserves both the per-consumer and aggregate retained-byte
// budgets before storing an advertised body. Permanently oversized bodies are
// filtered by NextTx; temporary reservation failures keep the cursor on the
// transaction, preserving later retransmission.
func (m *MempoolConsumer) cacheTransaction(
	tx *MempoolTransaction,
) (bool, <-chan struct{}) {
	size := int64(len(tx.Cbor))
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	select {
	case <-m.done:
		return false, nil
	default:
	}
	if _, exists := m.cache[tx.Hash]; exists {
		return true, nil
	}
	// Gate on the offered count, not the resident cache count: a served body
	// is evicted from cache immediately (freeing its bytes) but its hash
	// stays in offered until the peer acknowledges it. Gating on the cache
	// count alone would let a peer that keeps fetching bodies without ever
	// acknowledging them grow offered without bound.
	if len(m.offered) >= m.cacheLimit ||
		size > m.cacheLimitBytes-m.cacheBytes {
		return false, nil
	}
	reserved, aggregateChanged := m.mempool.reserveRelayCacheBytes(size)
	if !reserved {
		return false, aggregateChanged
	}
	m.cache[tx.Hash] = cloneMempoolTransaction(tx)
	m.cacheBytes += size
	// The tx is now advertised to the peer (returned from the RequestTxIds
	// callback that drove this NextTx call); track it for AcknowledgeOffered,
	// unless it is already outstanding (served but not yet acknowledged) --
	// the pool resurfacing the same hash at a later cursor position must not
	// add a second offered slot for it.
	if _, alreadyOffered := m.offeredSet[tx.Hash]; !alreadyOffered {
		m.offered = append(m.offered, tx.Hash)
		m.offeredSet[tx.Hash] = struct{}{}
	}
	return true, nil
}

func (m *MempoolConsumer) GetTxFromCache(hash string) *MempoolTransaction {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		return cloneMempoolTransaction(m.cache[hash])
	}
	var ret *MempoolTransaction
	return ret
}

func (m *MempoolConsumer) ClearCache() {
	if m != nil {
		m.cacheMutex.Lock()
		released := m.cacheBytes
		m.cache = make(map[string]*MempoolTransaction)
		m.cacheBytes = 0
		m.offered = nil
		m.offeredSet = make(map[string]struct{})
		m.mempool.releaseRelayCacheBytes(released)
		m.signalCacheSlotLocked()
		m.cacheMutex.Unlock()
		if m.onCacheCleared != nil {
			m.onCacheCleared()
		}
	}
}

func (m *MempoolConsumer) RemoveTxFromCache(hash string) {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		m.removeFromCacheLocked(hash)
	}
}

// AcknowledgeOffered forgets exactly the oldest count previously offered
// transaction bodies, in the order they were advertised. TxSubmission acks
// only the consumed prefix of the offered-id window; bodies for ids offered
// after that prefix are still eligible for the peer to request and must be
// preserved, so this must never clear the whole cache.
func (m *MempoolConsumer) AcknowledgeOffered(count int) {
	if m == nil || count <= 0 {
		return
	}
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	if count > len(m.offered) {
		count = len(m.offered)
	}
	for _, hash := range m.offered[:count] {
		// removeFromCacheLocked no-ops (and so does not signal) for an
		// already-served hash; freeing an offered slot can unblock a waiter
		// on its own, so signal below regardless of cache membership.
		m.removeFromCacheLocked(hash)
		delete(m.offeredSet, hash)
	}
	// Drop the acknowledged prefix; copy the remainder so the backing array
	// of the retained slice isn't shared with the one about to be discarded.
	remaining := make([]string, len(m.offered)-count)
	copy(remaining, m.offered[count:])
	m.offered = remaining
	m.signalCacheSlotLocked()
}

// removeFromCacheLocked evicts a single cached tx body, if present, and
// releases its reserved bytes. Caller must hold cacheMutex.
func (m *MempoolConsumer) removeFromCacheLocked(hash string) {
	if tx, existed := m.cache[hash]; existed {
		delete(m.cache, hash)
		size := int64(len(tx.Cbor))
		m.cacheBytes -= size
		m.mempool.releaseRelayCacheBytes(size)
		m.signalCacheSlotLocked()
	}
}

// signalCacheSlotLocked wakes one blocking NextTx parked on a full cache. The
// send is non-blocking: the buffered slot already holds a pending wake-up, and a
// waiter re-checks the cache after waking, so coalescing is correct. Caller must
// hold cacheMutex.
func (m *MempoolConsumer) signalCacheSlotLocked() {
	if m.cacheSlot == nil {
		return
	}
	select {
	case m.cacheSlot <- struct{}{}:
	default:
	}
}
