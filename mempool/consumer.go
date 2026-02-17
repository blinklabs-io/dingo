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
	mempool     *Mempool
	cache       map[string]*MempoolTransaction
	nextTxIdx   int
	cacheMutex  sync.Mutex
	nextTxIdxMu sync.Mutex
	done        chan struct{}
	doneOnce    sync.Once
}

func newConsumer(mempool *Mempool) *MempoolConsumer {
	return &MempoolConsumer{
		mempool: mempool,
		cache:   make(map[string]*MempoolTransaction),
		done:    make(chan struct{}),
	}
}

// Close signals any goroutines blocked on NextTx to exit.
// Safe to call multiple times.
func (m *MempoolConsumer) Close() {
	if m != nil {
		m.doneOnce.Do(func() { close(m.done) })
	}
}

func (m *MempoolConsumer) NextTx(blocking bool) *MempoolTransaction {
	if m == nil {
		return nil
	}

	for {
		m.mempool.RLock()
		m.nextTxIdxMu.Lock()

		// Check if we have a transaction available
		if m.nextTxIdx < len(m.mempool.transactions) {
			nextTx := m.mempool.transactions[m.nextTxIdx]
			if nextTx != nil {
				// Increment next TX index atomically with reading it
				m.nextTxIdx++
				m.nextTxIdxMu.Unlock()
				m.mempool.RUnlock()

				// Add transaction to cache (outside of locks)
				m.cacheMutex.Lock()
				m.cache[nextTx.Hash] = nextTx
				m.cacheMutex.Unlock()

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
		m.nextTxIdxMu.Unlock()
		m.mempool.RUnlock()

		// Block until an event arrives or shutdown is signaled
		select {
		case <-addTxChan:
			m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
			// Loop back to check if transaction is available
			// This naturally handles the case of multiple rapid additions
		case <-m.mempool.done:
			// Mempool is shutting down, unsubscribe and exit
			m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
			return nil
		case <-m.done:
			// Consumer removed (connection closed), unsubscribe and exit
			m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
			return nil
		}
	}
}

func (m *MempoolConsumer) GetTxFromCache(hash string) *MempoolTransaction {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		return m.cache[hash]
	}
	var ret *MempoolTransaction
	return ret
}

func (m *MempoolConsumer) ClearCache() {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		m.cache = make(map[string]*MempoolTransaction)
	}
}

func (m *MempoolConsumer) RemoveTxFromCache(hash string) {
	if m != nil {
		m.cacheMutex.Lock()
		defer m.cacheMutex.Unlock()
		delete(m.cache, hash)
	}
}
