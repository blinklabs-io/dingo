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
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/state"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	sync.RWMutex
	logger         *slog.Logger
	eventBus       *event.EventBus
	ledgerState    *state.LedgerState
	consumers      map[ouroboros.ConnectionId]*MempoolConsumer
	consumersMutex sync.Mutex
	transactions   []*MempoolTransaction
	metrics        struct {
		txsProcessedNum prometheus.Counter
		txsInMempool    prometheus.Gauge
		mempoolBytes    prometheus.Gauge
	}
}

func NewMempool(
	logger *slog.Logger,
	eventBus *event.EventBus,
	promRegistry prometheus.Registerer,
	ledgerState *state.LedgerState,
) *Mempool {
	m := &Mempool{
		eventBus:    eventBus,
		consumers:   make(map[ouroboros.ConnectionId]*MempoolConsumer),
		ledgerState: ledgerState,
	}
	if logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		m.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	} else {
		m.logger = logger
	}
	// Subscribe to chain update events
	go m.processChainEvents()
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
	consumer := newConsumer(m)
	m.consumers[connId] = consumer
	return consumer
}

func (m *Mempool) RemoveConsumer(connId ouroboros.ConnectionId) {
	m.consumersMutex.Lock()
	delete(m.consumers, connId)
	m.consumersMutex.Unlock()
}

func (m *Mempool) Consumer(connId ouroboros.ConnectionId) *MempoolConsumer {
	m.consumersMutex.Lock()
	defer m.consumersMutex.Unlock()
	return m.consumers[connId]
}

func (m *Mempool) processChainEvents() {
	chainUpdateSubId, chainUpdateChan := m.eventBus.Subscribe(
		chain.ChainUpdateEventType,
	)
	defer func() {
		m.eventBus.Unsubscribe(chain.ChainUpdateEventType, chainUpdateSubId)
	}()
	lastValidationTime := time.Now()
	var ok bool
	for {
		// Wait for chain event
		_, ok = <-chainUpdateChan
		if !ok {
			return
		}
		// Only purge once every 30 seconds when there are more blocks available
		if time.Since(lastValidationTime) < 30*time.Second &&
			len(chainUpdateChan) > 0 {
			continue
		}
		m.Lock()
		// Re-validate each TX in mempool
		// We iterate backward to avoid issues with shifting indexes when deleting
		for i := len(m.transactions) - 1; i >= 0; i-- {
			tx := m.transactions[i]
			// Decode transaction
			tmpTx, err := ledger.NewTransactionFromCbor(tx.Type, tx.Cbor)
			if err != nil {
				m.removeTransactionByIndex(i)
				m.logger.Error(
					"removed transaction after decode failure",
					"component", "mempool",
					"tx_hash", tx.Hash,
					"error", err,
				)
				continue
			}
			// Validate transaction
			if err := m.ledgerState.ValidateTx(tmpTx); err != nil {
				m.removeTransactionByIndex(i)
				m.logger.Debug(
					"removed transaction after re-validation failure",
					"component", "mempool",
					"tx_hash", tx.Hash,
					"error", err,
				)
			}
		}
		m.Unlock()
	}
}

func (m *Mempool) AddTransaction(txType uint, txBytes []byte) error {
	// Decode transaction
	tmpTx, err := ledger.NewTransactionFromCbor(txType, txBytes)
	if err != nil {
		return err
	}
	// Validate transaction
	if err := m.ledgerState.ValidateTx(tmpTx); err != nil {
		return err
	}
	// Build mempool entry
	txHash := tmpTx.Hash().String()
	tx := MempoolTransaction{
		Hash:     txHash,
		Type:     txType,
		Cbor:     txBytes,
		LastSeen: time.Now(),
	}
	m.Lock()
	m.consumersMutex.Lock()
	defer func() {
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
			return m.removeTransactionByIndex(txIdx)
		}
	}
	return false
}

func (m *Mempool) removeTransactionByIndex(txIdx int) bool {
	if txIdx >= len(m.transactions) {
		return false
	}
	tx := m.transactions[txIdx]
	m.transactions = slices.Delete(
		m.transactions,
		txIdx,
		txIdx+1,
	)
	m.metrics.txsInMempool.Dec()
	m.metrics.mempoolBytes.Sub(float64(len(tx.Cbor)))
	// Update consumer indexes to reflect removed TX
	for _, consumer := range m.consumers {
		// Decrement consumer index if the consumer has reached the removed TX
		if consumer.nextTxIdx >= txIdx {
			consumer.nextTxIdx--
		}
	}
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
