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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
	ValidateTxWithOverlay(
		tx gledger.Transaction,
		consumedUtxos map[string]struct{},
		createdUtxos map[string]lcommon.Utxo,
	) error
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
	CurrentSlotFunc    func() uint64 // returns current slot for early TX rejection
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
	overlay        *utxoOverlay
}

// appliedTx records a pending transaction and its UTxO effects for overlay rebuild.
type appliedTx struct {
	hash     string
	txType   uint
	cbor     []byte
	consumed []string                // UTxO keys consumed by this TX
	created  map[string]lcommon.Utxo // UTxO keys created by this TX
}

// utxoOverlay tracks cumulative UTxO state changes from all pending mempool TXs.
type utxoOverlay struct {
	consumed map[string]struct{}     // all inputs consumed by pending TXs
	created  map[string]lcommon.Utxo // all outputs created by pending TXs
	applied  []appliedTx             // ordered list for rebuild
}

func newUtxoOverlay() *utxoOverlay {
	return &utxoOverlay{
		consumed: make(map[string]struct{}),
		created:  make(map[string]lcommon.Utxo),
	}
}

// applyTx adds a validated transaction's UTxO effects to the overlay.
func (o *utxoOverlay) applyTx(
	hash string,
	txType uint,
	cbor []byte,
	tx lcommon.Transaction,
) {
	at := appliedTx{
		hash:    hash,
		txType:  txType,
		cbor:    cbor,
		created: make(map[string]lcommon.Utxo),
	}
	for _, input := range tx.Inputs() {
		key := fmt.Sprintf("%s:%d", input.Id().String(), input.Index())
		o.consumed[key] = struct{}{}
		at.consumed = append(at.consumed, key)
	}
	for _, utxo := range tx.Produced() {
		key := fmt.Sprintf(
			"%s:%d",
			utxo.Id.Id().String(),
			utxo.Id.Index(),
		)
		o.created[key] = utxo
		at.created[key] = utxo
	}
	o.applied = append(o.applied, at)
}

// reset clears the overlay to empty state.
func (o *utxoOverlay) reset() {
	o.consumed = make(map[string]struct{})
	o.created = make(map[string]lcommon.Utxo)
	o.applied = nil
}

// rebuildAggregates rebuilds consumed/created maps from the applied list.
func (o *utxoOverlay) rebuildAggregates() {
	o.consumed = make(map[string]struct{})
	o.created = make(map[string]lcommon.Utxo)
	for _, at := range o.applied {
		for _, key := range at.consumed {
			o.consumed[key] = struct{}{}
		}
		maps.Copy(o.created, at.created)
	}
}

// removeBatchWithDescendants removes the specified TXs from the applied list,
// then iteratively prunes any descendant TXs that consume UTxOs created by
// removed TXs. Calls rebuildAggregates before returning.
// Returns the hashes of pruned descendants (not including the primary hashes).
func (o *utxoOverlay) removeBatchWithDescendants(
	hashes map[string]struct{},
) []string {
	// Remove specified TXs and collect their created UTxOs
	orphanedUtxos := make(map[string]struct{})
	var remaining []appliedTx
	for _, at := range o.applied {
		if _, remove := hashes[at.hash]; remove {
			for key := range at.created {
				orphanedUtxos[key] = struct{}{}
			}
		} else {
			remaining = append(remaining, at)
		}
	}
	o.applied = remaining

	// Iteratively prune TXs that consume orphaned UTxOs (transitive)
	var pruned []string
	if len(orphanedUtxos) > 0 {
		changed := true
		for changed {
			changed = false
			var newRemaining []appliedTx
			for _, at := range o.applied {
				isOrphan := false
				for _, key := range at.consumed {
					if _, ok := orphanedUtxos[key]; ok {
						isOrphan = true
						break
					}
				}
				if isOrphan {
					for key := range at.created {
						orphanedUtxos[key] = struct{}{}
					}
					pruned = append(pruned, at.hash)
					changed = true
				} else {
					newRemaining = append(newRemaining, at)
				}
			}
			o.applied = newRemaining
		}
	}

	o.rebuildAggregates()
	return pruned
}

// simulateRemoveBatch computes what consumed/created maps would look like
// after removing the specified TXs and their descendants, without mutating
// the overlay. Used to validate incoming TXs before committing eviction.
func (o *utxoOverlay) simulateRemoveBatch(
	hashes map[string]struct{},
) (map[string]struct{}, map[string]lcommon.Utxo) {
	// Remove specified TXs and collect their created UTxOs
	orphanedUtxos := make(map[string]struct{})
	remaining := make([]appliedTx, 0, len(o.applied))
	for _, at := range o.applied {
		if _, remove := hashes[at.hash]; remove {
			for key := range at.created {
				orphanedUtxos[key] = struct{}{}
			}
		} else {
			remaining = append(remaining, at)
		}
	}
	// Iteratively prune TXs that consume orphaned UTxOs (transitive)
	if len(orphanedUtxos) > 0 {
		changed := true
		for changed {
			changed = false
			var newRemaining []appliedTx
			for _, at := range remaining {
				isOrphan := false
				for _, key := range at.consumed {
					if _, ok := orphanedUtxos[key]; ok {
						isOrphan = true
						break
					}
				}
				if isOrphan {
					for key := range at.created {
						orphanedUtxos[key] = struct{}{}
					}
					changed = true
				} else {
					newRemaining = append(newRemaining, at)
				}
			}
			remaining = newRemaining
		}
	}
	// Rebuild maps from surviving TXs
	consumed := make(map[string]struct{})
	created := make(map[string]lcommon.Utxo)
	for _, at := range remaining {
		for _, key := range at.consumed {
			consumed[key] = struct{}{}
		}
		maps.Copy(created, at.created)
	}
	return consumed, created
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
	if config.Validator == nil {
		panic("mempool: validator must not be nil")
	}
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
		overlay:            newUtxoOverlay(),
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
	m.overlay.reset()
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
		// Rebuild overlay: re-validate each pending TX in order against
		// a fresh overlay, removing TXs that no longer validate.
		m.rebuildOverlay()
		lastValidationTime = time.Now()
	}
}

// rebuildOverlay re-validates all pending TXs sequentially, rebuilding the
// UTxO overlay from scratch. TXs that fail re-validation are removed.
// Runs under the write lock for the entire duration to prevent races with
// AddTransaction (which also holds the write lock when updating the overlay).
// This is safe because mempool TX submission rate is low at tip and
// cardano-node also serializes mempool additions.
func (m *Mempool) rebuildOverlay() {
	if m.validator == nil {
		panic("mempool: validator is nil in rebuildOverlay")
	}
	m.Lock()
	prevApplied := make([]appliedTx, len(m.overlay.applied))
	copy(prevApplied, m.overlay.applied)

	if len(prevApplied) == 0 {
		m.Unlock()
		return
	}

	// Re-validate each TX in order against a fresh overlay.
	// Build new overlay incrementally — each valid TX extends it.
	newOverlay := newUtxoOverlay()
	var invalidHashes []string

	for _, at := range prevApplied {
		tmpTx, err := gledger.NewTransactionFromCbor(at.txType, at.cbor)
		if err != nil {
			invalidHashes = append(invalidHashes, at.hash)
			m.logger.Error(
				"transaction failed decode during re-validation",
				"component", "mempool",
				"tx_hash", at.hash,
				"error", err,
			)
			continue
		}
		if err := m.validator.ValidateTxWithOverlay(
			tmpTx,
			newOverlay.consumed,
			newOverlay.created,
		); err != nil {
			invalidHashes = append(invalidHashes, at.hash)
			m.logger.Debug(
				"transaction failed re-validation",
				"component", "mempool",
				"tx_hash", at.hash,
				"error", err,
			)
			continue
		}
		// TX still valid — add its effects to the new overlay
		newOverlay.applyTx(at.hash, at.txType, at.cbor, tmpTx)
	}

	// Swap overlay AND remove invalid TXs atomically so readers
	// never see TXs in m.transactions that aren't in the overlay.
	m.overlay = newOverlay
	var events []event.Event
	if len(invalidHashes) > 0 {
		hashSet := make(map[string]struct{}, len(invalidHashes))
		for _, h := range invalidHashes {
			hashSet[h] = struct{}{}
		}
		m.consumersMutex.Lock()
		for i := len(m.transactions) - 1; i >= 0; i-- {
			h := m.transactions[i].Hash
			if _, found := hashSet[h]; found {
				evt := m.removeTransactionByIndexLocked(i)
				if evt != nil {
					events = append(events, *evt)
				}
				delete(hashSet, h)
				if len(hashSet) == 0 {
					break
				}
			}
		}
		m.consumersMutex.Unlock()
	}
	m.Unlock()

	// MEM-03: Publish events outside locks
	if m.eventBus != nil {
		for _, evt := range events {
			m.eventBus.Publish(RemoveTransactionEventType, evt)
		}
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
	var removedCount int
	m.Lock()
	m.consumersMutex.Lock()
	// Collect expired transaction hashes
	expiredHashes := make(map[string]struct{})
	for _, tx := range m.transactions {
		if now.Sub(tx.LastSeen) > m.transactionTTL {
			m.logger.Debug(
				"removing expired transaction",
				"component", "mempool",
				"tx_hash", tx.Hash,
				"age", now.Sub(tx.LastSeen).String(),
			)
			expiredHashes[tx.Hash] = struct{}{}
		}
	}
	directExpiredCount := len(expiredHashes)
	if directExpiredCount > 0 {
		// Remove from overlay with descendant pruning
		pruned := m.overlay.removeBatchWithDescendants(expiredHashes)
		// Add pruned descendants to the removal set
		for _, h := range pruned {
			expiredHashes[h] = struct{}{}
		}
		// Remove all from transactions list (backward for safe index handling)
		for i := len(m.transactions) - 1; i >= 0; i-- {
			if _, ok := expiredHashes[m.transactions[i].Hash]; ok {
				evt := m.removeTransactionByIndexLocked(i)
				removedCount++
				if evt != nil {
					events = append(events, *evt)
				}
			}
		}
		if len(pruned) > 0 {
			m.logger.Debug(
				"pruned orphaned descendant transactions during expiry",
				"component", "mempool",
				"pruned_count", len(pruned),
			)
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
	if removedCount > 0 {
		m.metrics.txsExpired.Add(float64(removedCount))
		m.logger.Debug(
			"expired transactions removed from mempool",
			"component", "mempool",
			"expired_count", directExpiredCount,
			"total_removed", removedCount,
		)
	}
}

func (m *Mempool) AddTransaction(txType uint, txBytes []byte) error {
	if m.validator == nil {
		return errors.New("mempool: validator is nil in AddTransaction")
	}
	// Decode transaction outside the lock (CPU-bound, no shared state)
	tmpTx, err := gledger.NewTransactionFromCbor(txType, txBytes)
	if err != nil {
		return fmt.Errorf("decode transaction: %w", err)
	}
	// Early reject TXs whose validity interval hasn't started yet.
	// Compare against the current wall-clock slot rather than the last
	// block slot. Quiet networks often have a tip that lags the current
	// slot, and using the tip would incorrectly reject transactions that
	// are already valid now.
	if m.config.CurrentSlotFunc != nil {
		if start := tmpTx.ValidityIntervalStart(); start > 0 {
			currentSlot := m.config.CurrentSlotFunc()
			if start > currentSlot {
				return fmt.Errorf(
					"transaction validity interval start %d is beyond current slot %d",
					start,
					currentSlot,
				)
			}
		}
	}
	txHash := tmpTx.Hash().String()
	// Collect events to publish outside locks (MEM-03 fix)
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
		existingTx := m.getTransaction(txHash)
		if existingTx != nil {
			existingTx.LastSeen = time.Now()
			m.logger.Debug(
				"updated last seen for transaction",
				"component", "mempool",
				"tx_hash", txHash,
			)
			return
		}
		// Enforce mempool capacity using watermarks before validation
		// so we don't waste time validating TXs that will be rejected.
		txSize := int64(len(txBytes))
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
		// Determine if eviction is needed and simulate the
		// post-eviction overlay to validate against. This avoids
		// mutating state before we know the TX is valid.
		validConsumed := m.overlay.consumed
		validCreated := m.overlay.created
		var needsEviction bool
		var targetBytes int64
		evictionThreshold := int64(
			float64(m.config.MempoolCapacity) * m.evictionWatermark,
		)
		if newSize > evictionThreshold {
			needsEviction = true
			targetBytes = max(int64(0), evictionThreshold-txSize)
			// Compute which TXs would be evicted from the front
			evictedHashes := make(map[string]struct{})
			var evictedBytes int64
			for i := 0; i < len(m.transactions) &&
				m.currentSizeBytes-evictedBytes > targetBytes; i++ {
				evictedBytes += int64(len(m.transactions[i].Cbor))
				evictedHashes[m.transactions[i].Hash] = struct{}{}
			}
			validConsumed, validCreated = m.overlay.simulateRemoveBatch(evictedHashes)
		}
		// Validate against the (possibly simulated) post-eviction
		// overlay so we don't accept a TX whose inputs were created
		// by an evicted parent, and don't evict live TXs if
		// validation fails.
		if err = m.validator.ValidateTxWithOverlay(
			tmpTx,
			validConsumed,
			validCreated,
		); err != nil {
			err = fmt.Errorf("validate transaction: %w", err)
			return
		}
		// Validation passed: commit the eviction
		if needsEviction {
			evictedEvents = m.evictOldestLocked(targetBytes)
		}
		overlayCbor := slices.Clone(txBytes)
		txCbor := slices.Clone(txBytes)
		// Update UTxO overlay with this TX's effects
		m.overlay.applyTx(txHash, txType, overlayCbor, tmpTx)
		// Add transaction record
		tx := &MempoolTransaction{
			Hash:     txHash,
			Type:     txType,
			Cbor:     txCbor,
			LastSeen: time.Now(),
		}
		m.transactions = append(m.transactions, tx)
		m.txByHash[txHash] = tx
		m.currentSizeBytes += txSize
		m.logger.Debug(
			"added transaction",
			"component", "mempool",
			"tx_hash", txHash,
		)
		m.metrics.txsProcessedNum.Inc()
		m.metrics.txsInMempool.Inc()
		m.metrics.mempoolBytes.Add(float64(txSize))
		// Prepare event for publishing outside the lock
		if m.eventBus != nil {
			evt := event.NewEvent(
				AddTransactionEventType,
				AddTransactionEvent{
					Hash: txHash,
					Type: txType,
					Body: slices.Clone(txBytes),
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
	var events []event.Event
	m.Lock()
	m.consumersMutex.Lock()
	// Remove from overlay with descendant pruning
	toRemove := map[string]struct{}{txHash: {}}
	pruned := m.overlay.removeBatchWithDescendants(toRemove)
	for _, h := range pruned {
		toRemove[h] = struct{}{}
	}
	// Remove all from transactions list (backward for safe index handling)
	var removed bool
	for i := len(m.transactions) - 1; i >= 0; i-- {
		if _, ok := toRemove[m.transactions[i].Hash]; ok {
			evt := m.removeTransactionByIndexLocked(i)
			removed = true
			if evt != nil {
				events = append(events, *evt)
			}
		}
	}
	if removed {
		if len(pruned) > 0 {
			m.logger.Debug(
				"pruned orphaned descendant transactions",
				"component", "mempool",
				"primary_tx_hash", txHash,
				"pruned_count", len(pruned),
			)
		}
		m.logger.Debug(
			"removed transaction",
			"component", "mempool",
			"tx_hash", txHash,
		)
	}
	m.consumersMutex.Unlock()
	m.Unlock()
	// MEM-03: Publish events outside the lock
	if m.eventBus != nil {
		for _, evt := range events {
			m.eventBus.Publish(RemoveTransactionEventType, evt)
		}
	}
}

// removeTransactionByIndexLocked removes a transaction by its
// slice index. The caller must hold both the mempool write lock
// and consumersMutex. Returns the event to publish (if any) --
// the caller must publish it after releasing locks (MEM-03).
func (m *Mempool) removeTransactionByIndexLocked(
	txIdx int,
) *event.Event {
	if txIdx >= len(m.transactions) {
		return nil
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
	return evt
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

	// Collect hashes of evicted TXs for overlay removal
	evictedHashes := make(map[string]struct{}, evicted)
	for i := range evicted {
		evictedHashes[m.transactions[i].Hash] = struct{}{}
	}
	// Remove from overlay with descendant pruning
	pruned := m.overlay.removeBatchWithDescendants(evictedHashes)

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

	// Adjust all consumer indexes in one pass for front removal
	for _, consumer := range m.consumers {
		consumer.nextTxIdxMu.Lock()
		if consumer.nextTxIdx > evicted {
			consumer.nextTxIdx -= evicted
		} else {
			consumer.nextTxIdx = 0
		}
		consumer.nextTxIdxMu.Unlock()
	}

	// Remove pruned orphaned descendants from transactions list
	if len(pruned) > 0 {
		prunedSet := make(map[string]struct{}, len(pruned))
		for _, h := range pruned {
			prunedSet[h] = struct{}{}
		}
		for i := len(m.transactions) - 1; i >= 0; i-- {
			if _, ok := prunedSet[m.transactions[i].Hash]; ok {
				evt := m.removeTransactionByIndexLocked(i)
				if evt != nil {
					events = append(events, *evt)
				}
			}
		}
		m.logger.Debug(
			"pruned orphaned descendant transactions during eviction",
			"component", "mempool",
			"pruned_count", len(pruned),
		)
	}

	totalEvicted := evicted + len(pruned)
	m.metrics.txsEvicted.Add(float64(totalEvicted))
	m.logger.Debug(
		"evicted transactions from mempool",
		"component", "mempool",
		"evicted_count", totalEvicted,
		"current_size_bytes", m.currentSizeBytes,
	)
	return events
}
