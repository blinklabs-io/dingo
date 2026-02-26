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
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	AddTransactionEventType    event.EventType = "mempool.add_tx"
	RemoveTransactionEventType event.EventType = "mempool.remove_tx"

	DefaultEvictionWatermark  = 0.90
	DefaultRejectionWatermark = 0.95
	DefaultTransactionTTL     = 5 * time.Minute
	DefaultCleanupInterval    = 1 * time.Minute
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
	LastSeen time.Time
	Hash     string
	Cbor     []byte
	Type     uint
}

// TxValidator defines the interface for transaction validation needed by mempool.
type TxValidator interface {
	ValidateTx(tx gledger.Transaction) error
}
type MempoolConfig struct {
	PromRegistry       prometheus.Registerer
	Validator          TxValidator
	Logger             *slog.Logger
	EventBus           *event.EventBus
	MempoolCapacity    int64
	TransactionTTL     time.Duration
	CleanupInterval    time.Duration
	EvictionWatermark  float64
	RejectionWatermark float64
}

type Mempool struct {
	metrics struct {
		txsProcessedNum prometheus.Counter
		txsInMempool    prometheus.Gauge
		mempoolBytes    prometheus.Gauge
		txsEvicted      prometheus.Counter
		txsExpired      prometheus.Counter
	}
	validator          TxValidator
	logger             *slog.Logger
	eventBus           *event.EventBus
	consumers          map[ouroboros.ConnectionId]*MempoolConsumer
	done               chan struct{}
	config             MempoolConfig
	transactions       []*MempoolTransaction
	txByHash           map[string]*MempoolTransaction // O(1) lookup by hash
	currentSizeBytes   int64                          // Cached total size of all transactions in bytes
	transactionTTL     time.Duration
	cleanupInterval    time.Duration
	evictionWatermark  float64
	rejectionWatermark float64
	sync.RWMutex
	doneOnce       sync.Once
	consumersMutex sync.Mutex
}

type MempoolFullError struct {
	CurrentSize int
	TxSize      int
	Capacity    int64
}

func (e *MempoolFullError) Error() string {
	return fmt.Sprintf(
		"mempool full: current size=%d bytes, tx size=%d bytes, capacity=%d bytes",
		e.CurrentSize,
		e.TxSize,
		e.Capacity,
	)
}

func NewMempool(config MempoolConfig) *Mempool {
	evictionWatermark := config.EvictionWatermark
	if evictionWatermark == 0 {
		evictionWatermark = DefaultEvictionWatermark
	}
	rejectionWatermark := config.RejectionWatermark
	if rejectionWatermark == 0 {
		rejectionWatermark = DefaultRejectionWatermark
	}
	transactionTTL := config.TransactionTTL
	if transactionTTL == 0 {
		transactionTTL = DefaultTransactionTTL
	}
	cleanupInterval := config.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultCleanupInterval
	}
	m := &Mempool{
		eventBus:           config.EventBus,
		consumers:          make(map[ouroboros.ConnectionId]*MempoolConsumer),
		txByHash:           make(map[string]*MempoolTransaction),
		validator:          config.Validator,
		config:             config,
		done:               make(chan struct{}),
		transactionTTL:     transactionTTL,
		cleanupInterval:    cleanupInterval,
		evictionWatermark:  evictionWatermark,
		rejectionWatermark: rejectionWatermark,
	}
	if config.Logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		m.logger = slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		)
	} else {
		m.logger = config.Logger
	}
	if config.MempoolCapacity <= 0 {
		m.logger.Warn(
			"mempool capacity is zero or negative; "+
				"all transactions will be rejected",
			"component", "mempool",
			"capacity", config.MempoolCapacity,
		)
	}
	// Init metrics before launching goroutines that reference them
	promautoFactory := promauto.With(config.PromRegistry)
	m.metrics.txsProcessedNum = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_txsProcessedNum_int",
			Help: "total transactions processed",
		},
	)
	m.metrics.txsInMempool = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_txsInMempool_int",
			Help: "current count of mempool transactions",
		},
	)
	m.metrics.mempoolBytes = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_mempoolBytes_int",
			Help: "current size of mempool transactions in bytes",
		},
	)
	m.metrics.txsEvicted = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_txsEvictedNum_int",
			Help: "total transactions evicted from mempool",
		},
	)
	m.metrics.txsExpired = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_txsExpiredNum_int",
			Help: "total transactions expired from mempool by TTL",
		},
	)
	// Subscribe to chain update events
	go m.processChainEvents()
	// Start TTL cleanup goroutine
	go m.expireTransactions()
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

func (m *Mempool) Stop(ctx context.Context) error {
	// Context is accepted for API consistency but not used since cleanup is synchronous and fast
	m.logger.Debug("stopping mempool")

	// Signal the processChainEvents goroutine to stop (safe to call multiple times)
	m.doneOnce.Do(func() { close(m.done) })

	// Stop all consumers
	m.consumersMutex.Lock()
	for _, consumer := range m.consumers {
		if consumer != nil {
			consumer.ClearCache()
		}
	}
	m.consumers = make(map[ouroboros.ConnectionId]*MempoolConsumer)
	m.consumersMutex.Unlock()

	// Clear transactions
	m.Lock()
	m.transactions = []*MempoolTransaction{}
	m.txByHash = make(map[string]*MempoolTransaction)
	m.currentSizeBytes = 0
	// Reset metrics
	m.metrics.txsInMempool.Set(0)
	m.metrics.mempoolBytes.Set(0)
	m.Unlock()

	m.logger.Debug("mempool stopped")
	return nil
}

func (m *Mempool) Consumer(connId ouroboros.ConnectionId) *MempoolConsumer {
	m.consumersMutex.Lock()
	defer m.consumersMutex.Unlock()
	return m.consumers[connId]
}

func (m *Mempool) processChainEvents() {
	if m.eventBus == nil {
		// No event bus configured; nothing to process
		return
	}
	chainUpdateSubId, chainUpdateChan := m.eventBus.Subscribe(
		chain.ChainUpdateEventType,
	)
	defer func() {
		m.eventBus.Unsubscribe(chain.ChainUpdateEventType, chainUpdateSubId)
	}()
	lastValidationTime := time.Now()
	var ok bool
	for {
		select {
		case _, ok = <-chainUpdateChan:
			if !ok {
				return
			}
		case <-m.done:
			return
		}
		// Only purge once every 30 seconds when there are more blocks available
		if time.Since(lastValidationTime) < 30*time.Second &&
			len(chainUpdateChan) > 0 {
			continue
		}
		// MEM-04 fix: snapshot transactions under lock, validate outside lock,
		// then re-acquire lock to remove invalids. This avoids holding the write
		// lock during potentially expensive Plutus script re-validation.
		type txSnapshot struct {
			Hash string
			Type uint
			Cbor []byte
		}
		m.RLock()
		snapshot := make([]txSnapshot, len(m.transactions))
		for i, tx := range m.transactions {
			snapshot[i] = txSnapshot{
				Hash: tx.Hash,
				Type: tx.Type,
				Cbor: tx.Cbor,
			}
		}
		m.RUnlock()
		// Validate outside any lock
		var invalidHashes []string
		for _, snap := range snapshot {
			tmpTx, err := gledger.NewTransactionFromCbor(
				snap.Type,
				snap.Cbor,
			)
			if err != nil {
				invalidHashes = append(invalidHashes, snap.Hash)
				m.logger.Error(
					"transaction failed decode during re-validation",
					"component", "mempool",
					"tx_hash", snap.Hash,
					"error", err,
				)
				continue
			}
			if err := m.validator.ValidateTx(tmpTx); err != nil {
				invalidHashes = append(invalidHashes, snap.Hash)
				m.logger.Debug(
					"transaction failed re-validation",
					"component", "mempool",
					"tx_hash", snap.Hash,
					"error", err,
				)
			}
		}
		// Remove invalid transactions under lock
		if len(invalidHashes) > 0 {
			m.removeTransactions(invalidHashes)
		}
		lastValidationTime = time.Now()
	}
}

// expireTransactions periodically removes transactions that have
// exceeded the configured TTL. It runs every cleanupInterval and
// stops when the done channel is closed.
func (m *Mempool) expireTransactions() {
	if m.cleanupInterval <= 0 {
		return
	}
	ticker := time.NewTicker(m.cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.removeExpiredTransactions()
		case <-m.done:
			return
		}
	}
}

// removeExpiredTransactions removes all transactions whose LastSeen
// is older than the configured TTL. The TTL check and removal happen
// atomically under the write lock to prevent TOCTOU races with
// AddTransaction refreshing LastSeen. Events are published outside
// the lock (MEM-03).
func (m *Mempool) removeExpiredTransactions() {
	now := time.Now()
	var events []event.Event
	var expiredCount int
	m.Lock()
	m.consumersMutex.Lock()
	// Iterate backward to safely remove by index
	for i := len(m.transactions) - 1; i >= 0; i-- {
		tx := m.transactions[i]
		if now.Sub(tx.LastSeen) > m.transactionTTL {
			m.logger.Debug(
				"removing expired transaction",
				"component", "mempool",
				"tx_hash", tx.Hash,
				"age", now.Sub(tx.LastSeen).String(),
			)
			_, evt := m.removeTransactionByIndexLocked(i)
			expiredCount++
			if evt != nil {
				events = append(events, *evt)
			}
		}
	}
	m.consumersMutex.Unlock()
	m.Unlock()
	// MEM-03: Publish events outside locks
	if m.eventBus != nil {
		for _, evt := range events {
			m.eventBus.Publish(RemoveTransactionEventType, evt)
		}
	}
	if expiredCount > 0 {
		m.metrics.txsExpired.Add(float64(expiredCount))
		m.logger.Debug(
			"expired transactions removed from mempool",
			"component", "mempool",
			"expired_count", expiredCount,
		)
	}
}

func (m *Mempool) AddTransaction(txType uint, txBytes []byte) error {
	// Decode transaction
	tmpTx, err := gledger.NewTransactionFromCbor(txType, txBytes)
	if err != nil {
		return fmt.Errorf("decode transaction: %w", err)
	}
	// Validate transaction
	if err := m.validator.ValidateTx(tmpTx); err != nil {
		return fmt.Errorf("validate transaction: %w", err)
	}
	// Build mempool entry
	txHash := tmpTx.Hash().String()
	tx := MempoolTransaction{
		Hash:     txHash,
		Type:     txType,
		Cbor:     txBytes,
		LastSeen: time.Now(),
	}
	// Collect events to publish and eviction events inside the lock,
	// then publish them all after releasing locks (MEM-03 fix).
	var addEvent *event.Event
	var evictedEvents []event.Event
	func() {
		m.Lock()
		m.consumersMutex.Lock()
		defer func() {
			m.consumersMutex.Unlock()
			m.Unlock()
		}()
		// Update last seen for existing TX
		existingTx := m.getTransaction(tx.Hash)
		if existingTx != nil {
			existingTx.LastSeen = time.Now()
			m.logger.Debug(
				"updated last seen for transaction",
				"component", "mempool",
				"tx_hash", tx.Hash,
			)
			return
		}
		// Enforce mempool capacity using watermarks
		txSize := int64(len(tx.Cbor))
		newSize := m.currentSizeBytes + txSize
		rejectionThreshold := int64(
			float64(m.config.MempoolCapacity) * m.rejectionWatermark,
		)
		if newSize > rejectionThreshold {
			err = &MempoolFullError{
				CurrentSize: int(m.currentSizeBytes),
				TxSize:      int(txSize),
				Capacity:    m.config.MempoolCapacity,
			}
			return
		}
		evictionThreshold := int64(
			float64(m.config.MempoolCapacity) * m.evictionWatermark,
		)
		if newSize > evictionThreshold {
			targetBytes := evictionThreshold - txSize
			if targetBytes < 0 {
				targetBytes = 0
			}
			evictedEvents = m.evictOldestLocked(targetBytes)
		}
		// Add transaction record
		m.transactions = append(m.transactions, &tx)
		m.txByHash[tx.Hash] = &tx
		m.currentSizeBytes += txSize
		m.logger.Debug(
			"added transaction",
			"component", "mempool",
			"tx_hash", tx.Hash,
		)
		m.metrics.txsProcessedNum.Inc()
		m.metrics.txsInMempool.Inc()
		m.metrics.mempoolBytes.Add(float64(len(tx.Cbor)))
		// Prepare event for publishing outside the lock
		if m.eventBus != nil {
			evt := event.NewEvent(
				AddTransactionEventType,
				AddTransactionEvent{
					Hash: tx.Hash,
					Type: tx.Type,
					Body: tx.Cbor,
				},
			)
			addEvent = &evt
		}
	}()
	if err != nil {
		return err
	}
	// MEM-03: Publish events outside all locks
	if m.eventBus != nil {
		for _, evt := range evictedEvents {
			m.eventBus.Publish(RemoveTransactionEventType, evt)
		}
		if addEvent != nil {
			m.eventBus.Publish(AddTransactionEventType, *addEvent)
		}
	}
	return nil
}

func (m *Mempool) GetTransaction(txHash string) (MempoolTransaction, bool) {
	m.RLock()
	defer m.RUnlock()
	ret := m.getTransaction(txHash)
	if ret == nil {
		return MempoolTransaction{}, false
	}
	return *ret, true
}

func (m *Mempool) Transactions() []MempoolTransaction {
	m.RLock()
	defer m.RUnlock()
	ret := make([]MempoolTransaction, len(m.transactions))
	for i := range m.transactions {
		ret[i] = *m.transactions[i]
	}
	return ret
}

func (m *Mempool) getTransaction(txHash string) *MempoolTransaction {
	return m.txByHash[txHash]
}

func (m *Mempool) RemoveTransaction(txHash string) {
	var evt *event.Event
	m.Lock()
	if m.removeTransaction(txHash, &evt) {
		m.logger.Debug(
			"removed transaction",
			"component", "mempool",
			"tx_hash", txHash,
		)
	}
	m.Unlock()
	// MEM-03: Publish event outside the lock
	if evt != nil && m.eventBus != nil {
		m.eventBus.Publish(RemoveTransactionEventType, *evt)
	}
}

func (m *Mempool) removeTransaction(
	txHash string,
	evtOut **event.Event,
) bool {
	for txIdx, tx := range m.transactions {
		if tx.Hash == txHash {
			ok, evt := m.removeTransactionByIndex(txIdx)
			if evtOut != nil {
				*evtOut = evt
			}
			return ok
		}
	}
	return false
}

// removeTransactions removes multiple transactions by hash. Events
// are published outside the lock. Returns the number of transactions
// actually removed.
func (m *Mempool) removeTransactions(hashes []string) int {
	hashSet := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		hashSet[h] = struct{}{}
	}
	var events []event.Event
	var removedCount int
	m.Lock()
	m.consumersMutex.Lock()
	// Iterate backward to safely remove by index
	for i := len(m.transactions) - 1; i >= 0; i-- {
		tx := m.transactions[i]
		if _, found := hashSet[tx.Hash]; found {
			_, evt := m.removeTransactionByIndexLocked(i)
			removedCount++
			if evt != nil {
				events = append(events, *evt)
			}
			delete(hashSet, tx.Hash)
			if len(hashSet) == 0 {
				break
			}
		}
	}
	m.consumersMutex.Unlock()
	m.Unlock()
	// MEM-03: Publish events outside the lock
	if m.eventBus != nil {
		for _, evt := range events {
			m.eventBus.Publish(RemoveTransactionEventType, evt)
		}
	}
	return removedCount
}

func (m *Mempool) removeTransactionByIndex(txIdx int) (bool, *event.Event) {
	m.consumersMutex.Lock()
	result, evt := m.removeTransactionByIndexLocked(txIdx)
	m.consumersMutex.Unlock()
	return result, evt
}

// removeTransactionByIndexLocked removes a transaction by its
// slice index. The caller must hold both the mempool write lock
// and consumersMutex. Returns the event to publish (if any) --
// the caller must publish it after releasing locks (MEM-03).
func (m *Mempool) removeTransactionByIndexLocked(
	txIdx int,
) (bool, *event.Event) {
	if txIdx >= len(m.transactions) {
		return false, nil
	}
	tx := m.transactions[txIdx]
	txSize := int64(len(tx.Cbor))
	m.transactions = slices.Delete(
		m.transactions,
		txIdx,
		txIdx+1,
	)
	delete(m.txByHash, tx.Hash)
	m.currentSizeBytes -= txSize
	m.metrics.txsInMempool.Dec()
	m.metrics.mempoolBytes.Sub(float64(txSize))
	// Update consumer indexes to reflect removed TX
	for _, consumer := range m.consumers {
		consumer.nextTxIdxMu.Lock()
		if consumer.nextTxIdx > txIdx {
			consumer.nextTxIdx--
		}
		consumer.nextTxIdxMu.Unlock()
	}
	// Collect event for deferred publishing outside lock
	var evt *event.Event
	if m.eventBus != nil {
		e := event.NewEvent(
			RemoveTransactionEventType,
			RemoveTransactionEvent{
				Hash: tx.Hash,
			},
		)
		evt = &e
	}
	return true, evt
}

// evictOldestLocked removes transactions from the front of the
// slice (oldest first) until currentSizeBytes is at or below
// targetBytes. The caller must hold both the mempool write
// lock and consumersMutex. Returns events to publish after
// releasing locks (MEM-03).
func (m *Mempool) evictOldestLocked(targetBytes int64) []event.Event {
	// Calculate how many transactions to evict from the front
	var evicted int
	var evictedBytes int64
	for evicted < len(m.transactions) &&
		m.currentSizeBytes-evictedBytes > targetBytes {
		evictedBytes += int64(len(m.transactions[evicted].Cbor))
		evicted++
	}
	if evicted == 0 {
		return nil
	}

	// Clean up hash map, update metrics, and collect events
	// for each evicted transaction
	var events []event.Event
	for i := range evicted {
		tx := m.transactions[i]
		txSize := int64(len(tx.Cbor))
		delete(m.txByHash, tx.Hash)
		m.metrics.txsInMempool.Dec()
		m.metrics.mempoolBytes.Sub(float64(txSize))
		if m.eventBus != nil {
			events = append(events, event.NewEvent(
				RemoveTransactionEventType,
				RemoveTransactionEvent{
					Hash: tx.Hash,
				},
			))
		}
	}

	// Single batch removal from the front of the slice
	m.transactions = slices.Delete(
		m.transactions,
		0,
		evicted,
	)
	m.currentSizeBytes -= evictedBytes

	// Adjust all consumer indexes in one pass
	for _, consumer := range m.consumers {
		consumer.nextTxIdxMu.Lock()
		if consumer.nextTxIdx > evicted {
			consumer.nextTxIdx -= evicted
		} else {
			consumer.nextTxIdx = 0
		}
		consumer.nextTxIdxMu.Unlock()
	}

	m.metrics.txsEvicted.Add(float64(evicted))
	m.logger.Debug(
		"evicted transactions from mempool",
		"component", "mempool",
		"evicted_count", evicted,
		"current_size_bytes", m.currentSizeBytes,
	)
	return events
}
