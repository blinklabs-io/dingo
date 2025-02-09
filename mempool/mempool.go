// Copyright 2024 Blink Labs Software
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
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/event"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	txsubmissionMempoolExpiration       = 1 * time.Hour
	txSubmissionMempoolExpirationPeriod = 1 * time.Minute
)

const (
	AddTransactionEventType    event.EventType = "mempool.add_tx"
	RemoveTransactionEventType event.EventType = "mempool.remove_tx"
)

type AddTransactionEvent struct {
	Hash string
	Body []byte
	Type uint
}

type RemoveTransactionEvent struct {
	Hash string
}

type MempoolTransaction struct {
	Hash     string
	Type     uint
	Cbor     []byte
	LastSeen time.Time
}

type Mempool struct {
	sync.Mutex
	logger             *slog.Logger
	eventBus           *event.EventBus
	consumers          map[ouroboros.ConnectionId]*MempoolConsumer
	consumersMutex     sync.Mutex
	consumerIndex      map[ouroboros.ConnectionId]int
	consumerIndexMutex sync.Mutex
	transactions       []*MempoolTransaction
	metrics            struct {
		txsProcessedNum prometheus.Counter
		txsInMempool    prometheus.Gauge
		mempoolBytes    prometheus.Gauge
	}
}

func NewMempool(
	logger *slog.Logger,
	eventBus *event.EventBus,
	promRegistry prometheus.Registerer,
) *Mempool {
	m := &Mempool{
		eventBus:  eventBus,
		consumers: make(map[ouroboros.ConnectionId]*MempoolConsumer),
	}
	if logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		m.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	} else {
		m.logger = logger
	}
	// TODO: replace this with purging based on on-chain TXs (#388)
	// Schedule initial mempool expired cleanup
	m.scheduleRemoveExpired()
	// Init metrics
	promautoFactory := promauto.With(promRegistry)
	m.metrics.txsProcessedNum = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_txsProcessedNum_int",
			Help: "total transactions processed",
		},
	)
	m.metrics.txsInMempool = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_txsInMempool_int",
		Help: "current count of mempool transactions",
	})
	m.metrics.mempoolBytes = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_mempoolBytes_int",
		Help: "current size of mempool transactions in bytes",
	})
	return m
}

func (m *Mempool) AddConsumer(connId ouroboros.ConnectionId) *MempoolConsumer {
	// Create consumer
	m.consumersMutex.Lock()
	defer m.consumersMutex.Unlock()
	consumer := newConsumer()
	m.consumers[connId] = consumer
	// Start goroutine to send existing TXs to consumer
	go func(consumer *MempoolConsumer) {
		for {
			m.Lock()
			m.consumerIndexMutex.Lock()
			nextTxIdx, ok := m.consumerIndex[connId]
			if !ok {
				// Our consumer has disappeared
				m.consumerIndexMutex.Unlock()
				m.Unlock()
				return
			}
			if nextTxIdx >= len(m.transactions) {
				// We've reached the current end of the mempool
				m.consumerIndexMutex.Unlock()
				m.Unlock()
				return
			}
			nextTx := m.transactions[nextTxIdx]
			if consumer.pushTx(nextTx, true) {
				nextTxIdx++
				m.consumerIndex[connId] = nextTxIdx
			}
			m.consumerIndexMutex.Unlock()
			m.Unlock()
		}
	}(consumer)
	return consumer
}

func (m *Mempool) RemoveConsumer(connId ouroboros.ConnectionId) {
	m.consumersMutex.Lock()
	m.consumerIndexMutex.Lock()
	defer func() {
		m.consumerIndexMutex.Unlock()
		m.consumersMutex.Unlock()
	}()
	if consumer, ok := m.consumers[connId]; ok {
		consumer.stop()
		delete(m.consumers, connId)
		delete(m.consumerIndex, connId)
	}
}

func (m *Mempool) Consumer(connId ouroboros.ConnectionId) *MempoolConsumer {
	m.consumersMutex.Lock()
	defer m.consumersMutex.Unlock()
	return m.consumers[connId]
}

// TODO: replace this with purging based on on-chain TXs (#388)
func (m *Mempool) removeExpired() {
	m.Lock()
	defer m.Unlock()
	expiredBefore := time.Now().Add(-txsubmissionMempoolExpiration)
	// We iterate backward to avoid issues with shifting indexes when deleting
	for i := len(m.transactions) - 1; i >= 0; i-- {
		tx := m.transactions[i]
		if tx.LastSeen.Before(expiredBefore) {
			m.removeTransaction(tx.Hash)
			m.logger.Debug(
				"removed expired transaction",
				"component", "mempool",
				"tx_hash", tx.Hash,
			)
		}
	}
	m.scheduleRemoveExpired()
}

func (m *Mempool) scheduleRemoveExpired() {
	_ = time.AfterFunc(txSubmissionMempoolExpirationPeriod, m.removeExpired)
}

func (m *Mempool) AddTransaction(tx MempoolTransaction) error {
	m.Lock()
	m.consumersMutex.Lock()
	m.consumerIndexMutex.Lock()
	defer func() {
		m.consumerIndexMutex.Unlock()
		m.consumersMutex.Unlock()
		m.Unlock()
	}()
	// Update last seen for existing TX
	existingTx := m.getTransaction(tx.Hash)
	if existingTx != nil {
		tx.LastSeen = time.Now()
		m.logger.Debug(
			"updated last seen for transaction",
			"component", "mempool",
			"tx_hash", tx.Hash,
		)
		return nil
	}
	// Add transaction record
	m.transactions = append(m.transactions, &tx)
	m.logger.Debug(
		"added transaction",
		"component", "mempool",
		"tx_hash", tx.Hash,
	)
	m.metrics.txsProcessedNum.Inc()
	m.metrics.txsInMempool.Inc()
	m.metrics.mempoolBytes.Add(float64(len(tx.Cbor)))
	// Send new TX to consumers that are ready for it
	newTxIdx := len(m.transactions) - 1
	for connId, consumerIdx := range m.consumerIndex {
		if consumerIdx == newTxIdx {
			consumer := m.consumers[connId]
			if consumer.pushTx(&tx, false) {
				consumerIdx++
				m.consumerIndex[connId] = consumerIdx
			}
		}
	}
	// Generate event
	m.eventBus.Publish(
		AddTransactionEventType,
		event.NewEvent(
			AddTransactionEventType,
			AddTransactionEvent{
				Hash: tx.Hash,
				Type: tx.Type,
				Body: tx.Cbor[:],
			},
		),
	)
	return nil
}

func (m *Mempool) GetTransaction(txHash string) (MempoolTransaction, bool) {
	m.Lock()
	defer m.Unlock()
	ret := m.getTransaction(txHash)
	if ret == nil {
		return MempoolTransaction{}, false
	}
	return *ret, true
}

func (m *Mempool) Transactions() []MempoolTransaction {
	m.Lock()
	defer m.Unlock()
	ret := make([]MempoolTransaction, len(m.transactions))
	for i := 0; i < len(m.transactions); i++ {
		ret[i] = *m.transactions[i]
	}
	return ret
}

func (m *Mempool) getTransaction(txHash string) *MempoolTransaction {
	for _, tx := range m.transactions {
		if tx.Hash == txHash {
			return tx
		}
	}
	return nil
}

func (m *Mempool) RemoveTransaction(txHash string) {
	m.Lock()
	defer m.Unlock()
	if m.removeTransaction(txHash) {
		m.logger.Debug(
			"removed transaction",
			"component", "mempool",
			"tx_hash", txHash,
		)
	}
}

func (m *Mempool) removeTransaction(txHash string) bool {
	for txIdx, tx := range m.transactions {
		if tx.Hash == txHash {
			m.consumerIndexMutex.Lock()
			m.transactions = slices.Delete(
				m.transactions,
				txIdx,
				txIdx+1,
			)
			m.metrics.txsInMempool.Dec()
			m.metrics.mempoolBytes.Sub(float64(len(tx.Cbor)))
			// Update consumer indexes to reflect removed TX
			for connId, consumerIdx := range m.consumerIndex {
				// Decrement consumer index if the consumer has reached the removed TX
				if consumerIdx >= txIdx {
					consumerIdx--
				}
				m.consumerIndex[connId] = consumerIdx
			}
			m.consumerIndexMutex.Unlock()
			// Generate event
			m.eventBus.Publish(
				RemoveTransactionEventType,
				event.NewEvent(
					RemoveTransactionEventType,
					RemoveTransactionEvent{
						Hash: tx.Hash,
					},
				),
			)
			return true
		}
	}
	return false
}
