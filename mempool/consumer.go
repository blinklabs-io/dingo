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
	mempool    *Mempool
	cache      map[string]*MempoolTransaction
	nextTxIdx  int
	cacheMutex sync.Mutex
}

func newConsumer(mempool *Mempool) *MempoolConsumer {
	return &MempoolConsumer{
		mempool: mempool,
		cache:   make(map[string]*MempoolTransaction),
	}
}

func (m *MempoolConsumer) NextTx(blocking bool) *MempoolTransaction {
	if m == nil {
		return nil
	}
	m.mempool.RLock()
	defer m.mempool.RUnlock()
	if m.nextTxIdx >= len(m.mempool.transactions) {
		if !blocking {
			return nil
		}
		// Wait for TX to be added to mempool
		addTxSubId, addTxChan := m.mempool.eventBus.Subscribe(
			AddTransactionEventType,
		)
		m.mempool.RUnlock()
		<-addTxChan
		m.mempool.eventBus.Unsubscribe(AddTransactionEventType, addTxSubId)
		m.mempool.RLock()
		// Make sure our next TX index isn't beyond the bounds of the mempool
		// This shouldn't be necessary, but we probably have a bug elsewhere around
		// managing the consumer nextTxId values on add/remove TX in mempool.
		// This is also potentially lossy in the case of multiple TXs being added to
		// the mempool in short succession
		if m.nextTxIdx >= len(m.mempool.transactions) {
			m.nextTxIdx = len(m.mempool.transactions) - 1
		}
	}
	if m.nextTxIdx < 0 || m.nextTxIdx >= len(m.mempool.transactions) {
		panic("mempool consumer nextTxIdx out of bounds")
	}
	nextTx := m.mempool.transactions[m.nextTxIdx]
	if nextTx != nil {
		// Increment next TX index
		m.nextTxIdx++
		// Add transaction to cache
		m.cacheMutex.Lock()
		m.cache[nextTx.Hash] = nextTx
		m.cacheMutex.Unlock()
	}
	return nextTx
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
