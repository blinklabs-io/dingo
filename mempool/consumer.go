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
	cacheMutex      sync.Mutex
	nextTxIdxMu     sync.Mutex
	// onWaitForTx is a test-only hook invoked after a blocking NextTx has
	// subscribed for additions and is ready to be cancelled.
	onWaitForTx func()
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
	}
	return &MempoolConsumer{
		mempool:         mempool,
		cache:           make(map[string]*MempoolTransaction),
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

// cacheTransaction reserves both the per-consumer and aggregate retained-byte
// budgets before storing an advertised body. The caller keeps the cursor on
// this transaction when reservation fails, preserving later retransmission.
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
	if len(m.cache) >= m.cacheLimit ||
		size > m.cacheLimitBytes-m.cacheBytes {
		return false, nil
	}
	reserved, aggregateChanged := m.mempool.reserveRelayCacheBytes(size)
	if !reserved {
		return false, aggregateChanged
	}
	m.cache[tx.Hash] = cloneMempoolTransaction(tx)
	m.cacheBytes += size
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
		defer m.cacheMutex.Unlock()
		released := m.cacheBytes
		m.cache = make(map[string]*MempoolTransaction)
		m.cacheBytes = 0
		m.mempool.releaseRelayCacheBytes(released)
		m.signalCacheSlotLocked()
	}
}

func (m *MempoolConsumer) RemoveTxFromCache(hash string) {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		if tx, existed := m.cache[hash]; existed {
			delete(m.cache, hash)
			size := int64(len(tx.Cbor))
			m.cacheBytes -= size
			m.mempool.releaseRelayCacheBytes(size)
			m.signalCacheSlotLocked()
		}
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
