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

package mempool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/plugin"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	AddTransactionEventType    event.EventType = "mempool.add_tx"
	RemoveTransactionEventType event.EventType = "mempool.remove_tx"

	DefaultEvictionWatermark    = 0.0
	DefaultRejectionWatermark   = 1.0
	DefaultTransactionTTL       = 5 * time.Minute
	DefaultCleanupInterval      = 1 * time.Minute
	DefaultRevalidationDeltaCap = 64
	DefaultConsumerCacheSize    = 1024
	// defaultRevalidationJournalCap bounds the mutation journal a revalidation
	// pass may accumulate. A mempoolMutation is 40 bytes, so the backing slice
	// tops out near 42 MiB, and each entry additionally pins the transaction it
	// describes for the duration of the pass. A full drain of a cap-sized
	// journal measured 5.3 ms.
	defaultRevalidationJournalCap = 1 << 20
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

// Consumer is the neutral per-connection transaction cursor used by
// TxSubmission.
type Consumer interface {
	NextTx(bool) *MempoolTransaction
	GetTxFromCache(string) *MempoolTransaction
	ClearCache()
	RemoveTxFromCache(string)
}

// Service is the domain-owned mempool capability consumed by node wiring,
// networking, forging, ledger, and APIs.
type Service interface {
	AddTransaction(uint, []byte) error
	GetTransaction(string) (MempoolTransaction, bool)
	Transactions() []MempoolTransaction
	RemoveTransaction(string)
	RemoveTxsByHash([]string)
	NewConsumer(ouroboros.ConnectionId) Consumer
	RemoveConsumer(ouroboros.ConnectionId)
	FindConsumer(ouroboros.ConnectionId) Consumer
	CapacityBytes() int64
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

// TxValidationSessionProvider optionally pins a batch of validations to one
// coherent ledger snapshot. LedgerState implements this interface; lightweight
// validators used by tests and alternate embeddings may continue to implement
// only TxValidator.
type TxValidationSessionProvider interface {
	WithTxValidationSession(func(
		validate func(
			tx gledger.Transaction,
			consumedUtxos map[string]struct{},
			createdUtxos map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error) error
}

type MempoolConfig struct {
	PromRegistry         prometheus.Registerer
	Validator            TxValidator
	Logger               *slog.Logger
	EventBus             *event.EventBus
	MempoolCapacity      int64
	TransactionTTL       time.Duration
	CleanupInterval      time.Duration
	EvictionWatermark    float64
	RejectionWatermark   float64
	RevalidationDeltaCap int
	// ConsumerCacheSize bounds the number of transaction bodies retained per
	// transaction-submission consumer. Zero uses DefaultConsumerCacheSize.
	ConsumerCacheSize int
	CurrentSlotFunc   func() uint64 // returns current slot for early TX rejection
}

type Mempool struct {
	metrics struct {
		txsProcessedNum prometheus.Counter
		txsInMempool    prometheus.Gauge
		mempoolBytes    prometheus.Gauge
		txsEvicted      prometheus.Counter
		txsExpired      prometheus.Counter
		implementation  prometheus.Gauge
	}
	validator              TxValidator
	implementation         Implementation
	logger                 *slog.Logger
	eventBus               *event.EventBus
	consumers              map[ouroboros.ConnectionId]*MempoolConsumer
	done                   chan struct{}
	config                 MempoolConfig
	transactions           []*MempoolTransaction
	txByHash               map[string]*MempoolTransaction // O(1) lookup by hash
	currentSizeBytes       int64                          // Cached total size of all transactions in bytes
	transactionTTL         time.Duration
	cleanupInterval        time.Duration
	evictionWatermark      float64
	rejectionWatermark     float64
	revalidationDeltaCap   int
	revalidationJournalCap int
	stopped                bool
	sync.RWMutex
	doneOnce      sync.Once
	mutationMutex sync.Mutex
	startOnce     sync.Once
	stopOnce      sync.Once
	// stopTimeoutErr records the ctx.Err() from the first Stop call's context
	// if it fired before workerWG drained. stopOnce.Do only executes its
	// closure on the first Stop call, so a local variable set inside that
	// closure would be invisible to any later, concurrent, or repeated Stop
	// call -- this needs to be a field so every caller of Stop (regardless of
	// which one actually ran the closure) can observe the real outcome
	// instead of always getting nil, and so it reflects the context that
	// actually timed out rather than whichever caller happens to check it.
	stopTimeoutErr atomic.Pointer[error]

	workerWG        sync.WaitGroup
	consumersMutex  sync.Mutex
	overlay         *utxoOverlay
	dag             *transactionDAG
	headroomChanged chan struct{}

	// rebuildMutex permits one double-buffer rebuild at a time. mutationSeq and
	// mutationJournal are protected by mutationMutex and let that rebuild catch
	// up with admissions/removals performed while ledger validation runs.
	rebuildMutex    sync.Mutex
	mutationSeq     uint64
	mutationJournal []mempoolMutation
	journalActive   bool
	journalOverflow bool
}

type mempoolMutation struct {
	seq     uint64
	added   *appliedTx
	addedTx *MempoolTransaction
	removed map[string]struct{}
	stopped bool
}

type revalidationCandidate struct {
	overlay      *utxoOverlay
	transactions []*MempoolTransaction
	txByHash     map[string]*MempoolTransaction
	sizeBytes    int64
	invalid      map[string]*MempoolTransaction
	invalidUtxos map[string]struct{}
}

func newRevalidationCandidate() *revalidationCandidate {
	return &revalidationCandidate{
		overlay:      newUtxoOverlay(),
		txByHash:     make(map[string]*MempoolTransaction),
		invalid:      make(map[string]*MempoolTransaction),
		invalidUtxos: make(map[string]struct{}),
	}
}

func (c *revalidationCandidate) reject(
	at appliedTx,
	tx *MempoolTransaction,
) {
	if tx != nil {
		c.invalid[at.hash] = tx
	}
	for utxo := range at.created {
		c.invalidUtxos[utxo] = struct{}{}
	}
}

func (c *revalidationCandidate) dependsOnInvalid(at appliedTx) bool {
	for _, utxo := range at.consumed {
		if _, invalid := c.invalidUtxos[utxo]; invalid {
			return true
		}
	}
	return false
}

func (c *revalidationCandidate) remove(hashes map[string]struct{}) {
	if len(hashes) == 0 {
		return
	}
	c.overlay.removeByHashes(hashes)
	remaining := make([]*MempoolTransaction, 0, len(c.transactions))
	for _, tx := range c.transactions {
		if _, remove := hashes[tx.Hash]; remove {
			delete(c.txByHash, tx.Hash)
			delete(c.invalid, tx.Hash)
			c.sizeBytes -= int64(len(tx.Cbor))
			continue
		}
		remaining = append(remaining, tx)
	}
	c.transactions = remaining
	for hash := range hashes {
		delete(c.invalid, hash)
	}
}

func (c *revalidationCandidate) add(
	at appliedTx,
	tx *MempoolTransaction,
	decoded gledger.Transaction,
) {
	c.overlay.applyTx(at.hash, at.txType, at.cbor, decoded)
	c.transactions = append(c.transactions, tx)
	c.txByHash[at.hash] = tx
	c.sizeBytes += int64(len(tx.Cbor))
	delete(c.invalid, at.hash)
	for utxo := range at.created {
		delete(c.invalidUtxos, utxo)
	}
}

// appliedTx records a pending transaction and its UTxO effects for overlay rebuild.
type appliedTx struct {
	hash     string
	txType   uint
	cbor     []byte
	consumed []string                // UTxO keys consumed by this TX
	created  map[string]lcommon.Utxo // UTxO keys created by this TX
}

func cloneAppliedTx(at appliedTx) appliedTx {
	ret := at
	ret.cbor = slices.Clone(at.cbor)
	ret.consumed = slices.Clone(at.consumed)
	ret.created = maps.Clone(at.created)
	return ret
}

// recordMutationLocked appends an ordered mutation for an active rebuild.
// The caller must hold mutationMutex and must invoke this only after the live
// pool and overlay mutation has committed.
func (m *Mempool) recordMutationLocked(mutation mempoolMutation) {
	m.mutationSeq++
	mutation.seq = m.mutationSeq
	if m.journalActive {
		if len(m.mutationJournal) >= m.revalidationJournalCap {
			m.journalOverflow = true
			return
		}
		m.mutationJournal = append(m.mutationJournal, mutation)
	}
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

// removeByHashes removes the specified TXs from the overlay without cascading
// to their descendants. Use for confirmed TXs whose outputs now exist in the
// confirmed ledger state, making any chained descendants still valid.
func (o *utxoOverlay) removeByHashes(hashes map[string]struct{}) {
	remaining := make([]appliedTx, 0, len(o.applied))
	for _, at := range o.applied {
		if _, remove := hashes[at.hash]; !remove {
			remaining = append(remaining, at)
		}
	}
	o.applied = remaining
	o.rebuildAggregates()
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

// ErrNilValidator is returned by runtime mempool operations that require
// a non-nil validator. The constructor refuses to build a Mempool without
// one, so seeing this in a running node is a programmer error — but
// returning it lets the chain-update loop log and continue rather than
// crash the node.
var ErrNilValidator = errors.New("mempool: validator is nil")

// ErrMempoolStopped is returned when admission is attempted after shutdown.
var ErrMempoolStopped = errors.New("mempool: stopped")

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

func NewMempool(config MempoolConfig) (*Mempool, error) {
	return newMempool(config, ImplementationFIFO)
}

func newMempool(
	config MempoolConfig,
	implementation Implementation,
) (*Mempool, error) {
	if config.Validator == nil {
		return nil, ErrNilValidator
	}
	if !implementation.Valid() {
		return nil, fmt.Errorf(
			"unknown mempool implementation %q",
			implementation,
		)
	}
	evictionWatermark := config.EvictionWatermark
	if evictionWatermark < 0 {
		return nil, fmt.Errorf(
			"invalid eviction watermark: %f (must be in range [0, 1))",
			evictionWatermark,
		)
	}
	if evictionWatermark == 0 {
		evictionWatermark = DefaultEvictionWatermark
	}
	rejectionWatermark := config.RejectionWatermark
	if rejectionWatermark == 0 {
		rejectionWatermark = DefaultRejectionWatermark
	}
	if rejectionWatermark <= 0 || rejectionWatermark > 1 {
		return nil, fmt.Errorf(
			"invalid rejection watermark: %f (must be in range (0, 1])",
			rejectionWatermark,
		)
	}
	if evictionWatermark > 0 && evictionWatermark >= rejectionWatermark {
		return nil, fmt.Errorf(
			"eviction watermark (%f) must be less than rejection watermark (%f)",
			evictionWatermark,
			rejectionWatermark,
		)
	}
	transactionTTL := config.TransactionTTL
	if transactionTTL == 0 {
		transactionTTL = DefaultTransactionTTL
	}
	cleanupInterval := config.CleanupInterval
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultCleanupInterval
	}
	revalidationDeltaCap := config.RevalidationDeltaCap
	if revalidationDeltaCap <= 0 {
		revalidationDeltaCap = DefaultRevalidationDeltaCap
	}
	m := &Mempool{
		eventBus: config.EventBus,
		consumers: make(
			map[ouroboros.ConnectionId]*MempoolConsumer,
		),
		txByHash:               make(map[string]*MempoolTransaction),
		overlay:                newUtxoOverlay(),
		validator:              config.Validator,
		implementation:         implementation,
		config:                 config,
		done:                   make(chan struct{}),
		transactionTTL:         transactionTTL,
		cleanupInterval:        cleanupInterval,
		evictionWatermark:      evictionWatermark,
		rejectionWatermark:     rejectionWatermark,
		revalidationDeltaCap:   revalidationDeltaCap,
		revalidationJournalCap: defaultRevalidationJournalCap,
	}
	if implementation == ImplementationDAG {
		m.dag = newTransactionDAG()
	}
	m.headroomChanged = make(chan struct{})
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
			Name: "dingo_metrics_txsEvictedNum_int",
			Help: "total transactions evicted from mempool",
		},
	)
	m.metrics.txsExpired = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_metrics_txsExpiredNum_int",
			Help: "total transactions expired from mempool by TTL",
		},
	)
	m.metrics.implementation = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_metrics_mempool_info",
			Help: "mempool implementation identity",
			ConstLabels: prometheus.Labels{
				"implementation": string(implementation),
			},
		},
	)
	m.metrics.implementation.Set(1)
	return m, nil
}

// Start begins the mempool background lifecycle. Construction is deliberately
// side-effect free so the plugin host owns startup and rollback.
func (m *Mempool) Start(ctx context.Context) error {
	// Honor a caller that has already abandoned startup: do not launch the
	// background workers if the context is cancelled.
	if err := ctx.Err(); err != nil {
		return err
	}
	m.Lock()
	defer m.Unlock()
	if m.stopped {
		return ErrMempoolStopped
	}
	m.startOnce.Do(func() {
		m.workerWG.Add(2)
		go func() {
			defer m.workerWG.Done()
			m.processChainEvents()
		}()
		go func() {
			defer m.workerWG.Done()
			m.expireTransactions()
		}()
	})
	return nil
}

func (m *Mempool) AddConsumer(connId ouroboros.ConnectionId) *MempoolConsumer {
	m.RLock()
	defer m.RUnlock()
	if m.stopped {
		return nil
	}
	m.consumersMutex.Lock()
	defer m.consumersMutex.Unlock()
	if consumer := m.consumers[connId]; consumer != nil {
		return consumer
	}
	consumer := newConsumer(m, m.config.ConsumerCacheSize)
	m.consumers[connId] = consumer
	return consumer
}

// NewConsumer exposes AddConsumer through the neutral Service contract.
func (m *Mempool) NewConsumer(connId ouroboros.ConnectionId) Consumer {
	consumer := m.AddConsumer(connId)
	if consumer == nil {
		// AddConsumer returns a nil *MempoolConsumer when the mempool is
		// stopped. Return an untyped nil interface so callers' == nil checks
		// detect it, rather than a non-nil interface wrapping a nil pointer.
		return nil
	}
	return consumer
}

func (m *Mempool) RemoveConsumer(connId ouroboros.ConnectionId) {
	m.consumersMutex.Lock()
	delete(m.consumers, connId)
	m.consumersMutex.Unlock()
}

func (m *Mempool) Stop(ctx context.Context) error {
	m.logger.Debug("stopping mempool")
	m.stopOnce.Do(func() {
		// Establish a terminal state before waiting for background workers.
		// Releasing the mutation and pool locks lets in-flight workers finish.
		m.mutationMutex.Lock()
		m.Lock()
		m.stopped = true
		m.recordMutationLocked(mempoolMutation{stopped: true})
		m.doneOnce.Do(func() { close(m.done) })
		m.notifyHeadroomChangedLocked()
		m.Unlock()
		m.mutationMutex.Unlock()

		// Wait for the background workers to drain, but honor the caller's
		// deadline so a wedged worker cannot block shutdown forever. The
		// terminal state above already rejects new work and signals the
		// workers to exit, so if the context fires first the mempool is still
		// safely stopped; we only skip the best-effort memory teardown below
		// (the workers will still exit on their own once they observe the
		// closed done channel). Stop stays best-effort and returns nil.
		workersDone := make(chan struct{})
		go func() {
			m.workerWG.Wait()
			close(workersDone)
		}()
		select {
		case <-workersDone:
		case <-ctx.Done():
			m.logger.Debug(
				"mempool stop cancelled before workers drained; "+
					"skipping teardown",
				"error", ctx.Err(),
			)
			cancelErr := ctx.Err()
			m.stopTimeoutErr.Store(&cancelErr)
			return
		}

		m.mutationMutex.Lock()
		defer m.mutationMutex.Unlock()
		m.Lock()
		defer m.Unlock()
		m.consumersMutex.Lock()
		for _, consumer := range m.consumers {
			if consumer != nil {
				consumer.ClearCache()
			}
		}
		m.consumers = make(map[ouroboros.ConnectionId]*MempoolConsumer)
		m.consumersMutex.Unlock()

		m.transactions = []*MempoolTransaction{}
		m.txByHash = make(map[string]*MempoolTransaction)
		m.currentSizeBytes = 0
		m.overlay.reset()
		if m.dag != nil {
			m.dag.rebuild(nil)
		}
		m.metrics.txsInMempool.Set(0)
		m.metrics.mempoolBytes.Set(0)
	})

	if timeoutErr := m.stopTimeoutErr.Load(); timeoutErr != nil {
		return fmt.Errorf(
			"mempool stop: %w before background workers drained",
			*timeoutErr,
		)
	}

	m.logger.Debug("mempool stopped")
	return nil
}

func (m *Mempool) Consumer(connId ouroboros.ConnectionId) *MempoolConsumer {
	m.consumersMutex.Lock()
	defer m.consumersMutex.Unlock()
	return m.consumers[connId]
}

// FindConsumer exposes Consumer through the neutral Service contract.
func (m *Mempool) FindConsumer(connId ouroboros.ConnectionId) Consumer {
	consumer := m.Consumer(connId)
	if consumer == nil {
		return nil
	}
	return consumer
}

// ProviderConfig is the canonical configuration for built-in mempool
// providers.
type ProviderConfig struct {
	Capacity             int64   `yaml:"capacity"`
	EvictionWatermark    float64 `yaml:"evictionWatermark"`
	RejectionWatermark   float64 `yaml:"rejectionWatermark"`
	RevalidationDeltaCap int     `yaml:"revalidationDeltaCap"`
}

// ProviderDependencies are runtime dependencies assembled after ledger and
// database startup.
type ProviderDependencies struct {
	PromRegistry    prometheus.Registerer
	Validator       TxValidator
	Logger          *slog.Logger
	EventBus        *event.EventBus
	CurrentSlotFunc func() uint64
}

// RegisterProvider registers the FIFO compatibility alias as mempool/default.
func RegisterProvider(host *plugin.Host) error {
	return registerProvider(host, "default", ImplementationFIFO)
}

// RegisterFIFOProvider registers the explicit mempool/fifo provider.
func RegisterFIFOProvider(host *plugin.Host) error {
	return registerProvider(host, "fifo", ImplementationFIFO)
}

// RegisterDAGProvider registers the dependency-indexed mempool/dag provider.
func RegisterDAGProvider(host *plugin.Host) error {
	return registerProvider(host, "dag", ImplementationDAG)
}

func registerProvider(
	host *plugin.Host,
	name string,
	implementation Implementation,
) error {
	return plugin.Register(
		host,
		plugin.Descriptor{
			Capability: plugin.CapabilityMempool,
			Name:       name,
			Description: fmt.Sprintf(
				"Dingo %s transaction mempool",
				implementation,
			),
		},
		func() ProviderConfig {
			return ProviderConfig{
				EvictionWatermark:    DefaultEvictionWatermark,
				RejectionWatermark:   DefaultRejectionWatermark,
				RevalidationDeltaCap: DefaultRevalidationDeltaCap,
			}
		},
		func(_ context.Context, cfg ProviderConfig, deps ProviderDependencies) (Service, plugin.Instance, error) {
			mempoolConfig := MempoolConfig{
				PromRegistry: deps.PromRegistry, Validator: deps.Validator,
				Logger: deps.Logger, EventBus: deps.EventBus,
				MempoolCapacity:      cfg.Capacity,
				EvictionWatermark:    cfg.EvictionWatermark,
				RejectionWatermark:   cfg.RejectionWatermark,
				RevalidationDeltaCap: cfg.RevalidationDeltaCap,
				CurrentSlotFunc:      deps.CurrentSlotFunc,
			}
			var (
				service  Service
				instance plugin.Instance
				err      error
			)
			switch implementation {
			case ImplementationFIFO:
				fifo, fifoErr := NewFIFO(mempoolConfig)
				service, instance, err = fifo, fifo, fifoErr
			case ImplementationDAG:
				dag, dagErr := NewDAG(mempoolConfig)
				service, instance, err = dag, dag, dagErr
			default:
				return nil, nil, fmt.Errorf(
					"unknown mempool implementation %q",
					implementation,
				)
			}
			if err != nil {
				return nil, nil, err
			}
			return service, instance, nil
		},
	)
}

func (m *Mempool) processChainEvents() {
	if m.eventBus == nil {
		return
	}
	// Sized for catch-up bursts (one event per block). See #2106.
	chainUpdateSubId, chainUpdateChan := m.eventBus.SubscribeWithBuffer(
		chain.ChainUpdateEventType,
		event.EventQueueSize,
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
		// a fresh overlay, removing TXs that no longer validate. Log
		// and continue on error — the next chain update will try
		// again rather than crashing the node.
		if err := m.rebuildOverlay(); err != nil {
			m.logger.Error(
				"mempool overlay rebuild failed",
				"component", "mempool",
				"error", err,
			)
		}
		lastValidationTime = time.Now()
	}
}

const maxRevalidationCatchupRounds = 16

var errValidationSnapshotChanged = errors.New(
	"mempool: ledger snapshot changed during revalidation",
)

var errRevalidationJournalOverflow = errors.New(
	"mempool: revalidation mutation journal overflow",
)

// errRevalidationCatchup means the catch-up loop ran out of rounds. It is a
// defensive guard that no current path reaches, kept so that exhausting the
// budget degrades into a retryable no-op with the live pool intact rather than
// falling through to undefined behaviour.
//
// It is unreachable because recordMutationLocked bumps mutationSeq and appends
// to the journal together whenever the journal is active and under cap. So the
// one round-consuming path that does not itself enlarge the budget, the
// finalise race at mutationSeq != liveSeq, implies a journal entry that makes
// the next round observe pending > 0, which extends the budget past the current
// round. The remaining case, a mutation that bumps the sequence without
// appending because the journal is full, sets journalOverflow, and the loop
// tests that before anything else and returns
// errRevalidationJournalOverflow.
var errRevalidationCatchup = errors.New(
	"mempool: revalidation exhausted its catch-up budget",
)

// rebuildOverlay re-validates all pending TXs against a stable ledger snapshot
// in a private overlay. Admissions and removals continue against the live
// overlay and are replayed from an ordered journal before the candidate is
// swapped in during a short mutation-lock hold.
func (m *Mempool) rebuildOverlay() error {
	if m.validator == nil {
		return ErrNilValidator
	}
	m.rebuildMutex.Lock()
	defer m.rebuildMutex.Unlock()

	// A ledger publication racing the batch invalidates its pinned view. Retry
	// once from the new live pool; a later chain event provides further retries
	// without allowing a busy chain to spin here indefinitely.
	for attempt := range 2 {
		events, err := m.rebuildOverlayAttempt()
		if errors.Is(err, errValidationSnapshotChanged) && attempt == 0 {
			continue
		}
		if errors.Is(err, errRevalidationCatchup) {
			// Defensive: no current path produces this. The live pool is
			// unchanged either way, and a later chain update retries.
			return nil
		}
		if err != nil {
			return err
		}
		if m.eventBus != nil {
			for _, evt := range events {
				m.eventBus.Publish(RemoveTransactionEventType, evt)
			}
		}
		return nil
	}
	return errValidationSnapshotChanged
}

func (m *Mempool) rebuildOverlayAttempt() ([]event.Event, error) {
	m.mutationMutex.Lock()
	m.RLock()
	if m.stopped {
		m.RUnlock()
		m.mutationMutex.Unlock()
		return nil, ErrMempoolStopped
	}
	base := slices.Clone(m.overlay.applied)
	baseTxs := make(map[string]*MempoolTransaction, len(m.txByHash))
	maps.Copy(baseTxs, m.txByHash)
	startSeq := m.mutationSeq
	m.mutationJournal = nil
	m.journalActive = true
	m.journalOverflow = false
	m.RUnlock()
	m.mutationMutex.Unlock()

	finishJournal := func() {
		m.mutationMutex.Lock()
		m.journalActive = false
		m.mutationJournal = nil
		m.journalOverflow = false
		m.mutationMutex.Unlock()
	}

	var events []event.Event
	err := m.withTxValidationSession(func(
		validate func(
			gledger.Transaction,
			map[string]struct{},
			map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error {
		candidate := newRevalidationCandidate()
		for _, at := range base {
			m.revalidateAppliedTx(candidate, at, baseTxs[at.hash], validate)
		}

		appliedSeq := startSeq
		catchupRounds := maxRevalidationCatchupRounds
		for round := 0; round < catchupRounds; round++ {
			m.mutationMutex.Lock()
			if m.journalOverflow {
				m.mutationMutex.Unlock()
				return errRevalidationJournalOverflow
			}
			delta, pending := mutationWindow(
				m.mutationJournal, appliedSeq, m.revalidationDeltaCap,
			)
			if pending == 0 {
				m.RLock()
				liveOrder := slices.Clone(m.transactions)
				liveSeq := m.mutationSeq
				m.RUnlock()
				m.mutationMutex.Unlock()

				// Precompute cursor translations outside both mempool locks.
				// prefixValid[n] is the number of candidate transactions in
				// the first n entries of the current live FIFO.
				prefixValid := make([]int, len(liveOrder)+1)
				for i, tx := range liveOrder {
					prefixValid[i+1] = prefixValid[i]
					if _, ok := candidate.txByHash[tx.Hash]; ok {
						prefixValid[i+1]++
					}
				}
				invalidHashes := make([]string, 0, len(candidate.invalid))
				for _, tx := range slices.Backward(liveOrder) {
					if _, invalid := candidate.invalid[tx.Hash]; invalid {
						invalidHashes = append(invalidHashes, tx.Hash)
					}
				}

				m.mutationMutex.Lock()
				if m.mutationSeq != liveSeq {
					m.mutationMutex.Unlock()
					continue
				}
				if !stillCurrent() {
					m.journalActive = false
					m.mutationJournal = nil
					m.journalOverflow = false
					m.mutationMutex.Unlock()
					return errValidationSnapshotChanged
				}

				m.Lock()
				if m.stopped {
					m.Unlock()
					m.journalActive = false
					m.mutationJournal = nil
					m.journalOverflow = false
					m.mutationMutex.Unlock()
					return ErrMempoolStopped
				}
				m.consumersMutex.Lock()
				for _, consumer := range m.consumers {
					consumer.nextTxIdxMu.Lock()
					oldIdx := min(consumer.nextTxIdx, len(liveOrder))
					consumer.nextTxIdx = prefixValid[oldIdx]
					consumer.nextTxIdxMu.Unlock()
				}
				if m.eventBus != nil {
					for _, hash := range invalidHashes {
						events = append(events, event.NewEvent(
							RemoveTransactionEventType,
							RemoveTransactionEvent{Hash: hash},
						))
					}
				}
				m.overlay = candidate.overlay
				m.transactions = candidate.transactions
				m.txByHash = candidate.txByHash
				m.currentSizeBytes = candidate.sizeBytes
				if m.dag != nil {
					m.dag.rebuild(candidate.overlay.applied)
				}
				m.notifyHeadroomChangedLocked()
				m.metrics.txsInMempool.Set(float64(len(candidate.transactions)))
				m.metrics.mempoolBytes.Set(float64(candidate.sizeBytes))
				m.consumersMutex.Unlock()
				m.Unlock()
				m.journalActive = false
				m.mutationJournal = nil
				m.journalOverflow = false
				if len(invalidHashes) > 0 {
					removed := make(
						map[string]struct{},
						len(invalidHashes),
					)
					for _, hash := range invalidHashes {
						removed[hash] = struct{}{}
					}
					m.recordMutationLocked(mempoolMutation{removed: removed})
				}
				m.mutationMutex.Unlock()
				return nil
			}
			m.mutationMutex.Unlock()

			// Scale the total budget to the observed backlog while keeping
			// each replay round bounded. New mutations can extend this budget
			// again until the journal overflows.
			if requiredRounds := catchupBudget(
				round, pending, m.revalidationDeltaCap,
			); requiredRounds > catchupRounds {
				catchupRounds = requiredRounds
			}
			// delta is already bounded to revalidationDeltaCap by
			// mutationWindow, so the loop can observe and reconcile mutations
			// that arrive while it replays.
			for _, mutation := range delta {
				if mutation.stopped {
					return ErrMempoolStopped
				}
				if len(mutation.removed) > 0 {
					candidate.remove(mutation.removed)
				}
				if mutation.added != nil {
					m.revalidateAppliedTx(
						candidate,
						*mutation.added,
						mutation.addedTx,
						validate,
					)
				}
				appliedSeq = mutation.seq
			}
		}
		return errRevalidationCatchup
	})
	if err != nil {
		finishJournal()
		return nil, err
	}
	return events, nil
}

// mutationWindow returns up to limit mutations recorded after seq, together
// with the total number pending after seq. The returned window is never nil.
//
// Only the window is cloned. A caller that applies at most limit entries per
// round must not pay a scan of the whole journal plus a clone of its entire
// remaining suffix on every round: the journal holds up to
// defaultRevalidationJournalCap entries and this runs under mutationMutex,
// where that copy blocks admissions and removals. Journal seqs increase
// monotonically (recordMutationLocked appends with an incrementing seq), so the
// window start is a binary search.
func mutationWindow(
	journal []mempoolMutation,
	seq uint64,
	limit int,
) (window []mempoolMutation, pending int) {
	if limit < 1 {
		limit = 1
	}
	idx := sort.Search(len(journal), func(i int) bool {
		return journal[i].seq > seq
	})
	pending = len(journal) - idx
	end := min(idx+limit, len(journal))
	window = slices.Clone(journal[idx:end])
	if window == nil {
		// slices.Clone yields nil for an empty result. Return an empty slice
		// instead so the window is never nil, which keeps callers (and
		// nilaway) from having to distinguish the two.
		window = []mempoolMutation{}
	}
	return window, pending
}

// catchupBudget returns the total round budget needed to drain pending
// mutations at deltaCap per round, given the loop is already at round.
//
// Rounds already spent must be included. Without them, a backlog arriving late
// in an already-enlarged budget computes a total no larger than the current one,
// so the budget does not grow and the loop bails with errRevalidationCatchup
// even though the work would have fit.
//
// Note this means the budget never ends the loop while work remains: with
// pending > 0 the result always exceeds round, by at least
// maxRevalidationCatchupRounds. Termination therefore comes from sampling an
// empty journal, from the journal cap, or from a replay error, not from this
// number. The journal cap is the real bound on a sustained arrival rate, and
// errRevalidationCatchup is consequently unreachable; see its declaration.
func catchupBudget(round, pending, deltaCap int) int {
	if deltaCap < 1 {
		deltaCap = 1
	}
	return round + (pending+deltaCap-1)/deltaCap + maxRevalidationCatchupRounds
}

func (m *Mempool) revalidateAppliedTx(
	candidate *revalidationCandidate,
	at appliedTx,
	tx *MempoolTransaction,
	validate func(
		gledger.Transaction,
		map[string]struct{},
		map[string]lcommon.Utxo,
	) error,
) {
	if tx == nil {
		// There is no transaction body to record in candidate.invalid, but its
		// created outputs must still invalidate every dependent transaction.
		candidate.reject(at, nil)
		m.logger.Warn(
			"overlay applied transaction is missing from transaction index during re-validation",
			"component",
			"mempool",
			"tx_hash",
			at.hash,
			"tx_type",
			at.txType,
		)
		return
	}
	if candidate.dependsOnInvalid(at) {
		candidate.reject(at, tx)
		return
	}
	tmpTx, err := gledger.NewTransactionFromCbor(at.txType, at.cbor)
	if err != nil {
		candidate.reject(at, tx)
		m.logger.Error(
			"transaction failed decode during re-validation",
			"component", "mempool",
			"tx_hash", at.hash,
			"error", err,
		)
		return
	}
	if err := validate(
		tmpTx,
		candidate.overlay.consumed,
		candidate.overlay.created,
	); err != nil {
		candidate.reject(at, tx)
		m.logger.Warn(
			"transaction failed re-validation and was dropped from the mempool",
			"component", "mempool",
			"tx_hash", at.hash,
			"error", err,
		)
		return
	}
	candidate.add(at, tx, tmpTx)
}

func (m *Mempool) withTxValidationSession(
	fn func(
		validate func(
			gledger.Transaction,
			map[string]struct{},
			map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error,
) error {
	if provider, ok := m.validator.(TxValidationSessionProvider); ok {
		return provider.WithTxValidationSession(fn)
	}
	return fn(m.validator.ValidateTxWithOverlay, func() bool { return true })
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
	m.mutationMutex.Lock()
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
		var pruned []string
		if m.dag != nil {
			allExpired := m.dag.descendants(expiredHashes)
			for hash := range allExpired {
				if _, direct := expiredHashes[hash]; !direct {
					pruned = append(pruned, hash)
				}
			}
			expiredHashes = allExpired
			m.overlay.removeByHashes(expiredHashes)
			m.dag.remove(expiredHashes)
		} else {
			// Remove from overlay with descendant pruning.
			pruned = m.overlay.removeBatchWithDescendants(expiredHashes)
			// Add pruned descendants to the removal set.
			for _, h := range pruned {
				expiredHashes[h] = struct{}{}
			}
		}
		// Remove all from transactions list (backward for safe index handling)
		for i, v := range slices.Backward(m.transactions) {
			if _, ok := expiredHashes[v.Hash]; ok {
				evt := m.removeTransactionByIndexLocked(i)
				removedCount++
				if evt != nil {
					events = append(events, *evt)
				}
			}
		}
		m.recordMutationLocked(
			mempoolMutation{removed: maps.Clone(expiredHashes)},
		)
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
	m.mutationMutex.Unlock()
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
	var addEvent *event.Event
	var evictedEvents []event.Event
	err = func() error {
		// Serialize mutations without blocking snapshot readers during ledger
		// validation. This gate also guarantees the overlay used for validation
		// remains current until the transaction is committed.
		m.mutationMutex.Lock()
		defer m.mutationMutex.Unlock()

		m.Lock()
		if m.stopped {
			m.Unlock()
			return ErrMempoolStopped
		}
		existingTx := m.getTransaction(txHash)
		if existingTx != nil {
			existingTx.LastSeen = time.Now()
			m.Unlock()
			m.logger.Debug(
				"updated last seen for transaction",
				"component", "mempool",
				"tx_hash", txHash,
			)
			return nil
		}
		txSize := int64(len(txBytes))
		newSize := m.currentSizeBytes + txSize
		rejectionThreshold := m.admissionLimitBytes()
		if newSize > rejectionThreshold {
			retErr := &MempoolFullError{
				CurrentSize: int(m.currentSizeBytes),
				TxSize:      int(txSize),
				Capacity:    m.config.MempoolCapacity,
			}
			m.Unlock()
			return retErr
		}
		validConsumed := m.overlay.consumed
		validCreated := m.overlay.created
		var needsEviction bool
		var targetBytes int64
		evictionThreshold := int64(
			float64(m.config.MempoolCapacity) * m.evictionWatermark,
		)
		if m.dag == nil && m.evictionWatermark > 0 &&
			newSize > evictionThreshold {
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
			validConsumed, validCreated = m.overlay.simulateRemoveBatch(
				evictedHashes,
			)
		}
		m.Unlock()

		// The mutation gate keeps this overlay snapshot stable while the
		// potentially expensive ledger validation runs without the pool locks.
		if validateErr := m.validator.ValidateTxWithOverlay(
			tmpTx,
			validConsumed,
			validCreated,
		); validateErr != nil {
			return fmt.Errorf("validate transaction: %w", validateErr)
		}

		m.Lock()
		m.consumersMutex.Lock()
		defer func() {
			m.consumersMutex.Unlock()
			m.Unlock()
		}()
		if needsEviction {
			evictedEvents = m.evictOldestLocked(targetBytes)
		}
		overlayCbor := slices.Clone(txBytes)
		txCbor := slices.Clone(txBytes)
		m.overlay.applyTx(txHash, txType, overlayCbor, tmpTx)
		added := cloneAppliedTx(m.overlay.applied[len(m.overlay.applied)-1])
		if m.dag != nil {
			applied := m.overlay.applied[len(m.overlay.applied)-1]
			m.dag.add(applied)
		}
		tx := &MempoolTransaction{
			Hash:     txHash,
			Type:     txType,
			Cbor:     txCbor,
			LastSeen: time.Now(),
		}
		m.transactions = append(m.transactions, tx)
		m.txByHash[txHash] = tx
		m.currentSizeBytes += txSize
		m.notifyHeadroomChangedLocked()
		m.logger.Debug(
			"added transaction",
			"component", "mempool",
			"tx_hash", txHash,
		)
		m.metrics.txsProcessedNum.Inc()
		m.metrics.txsInMempool.Inc()
		m.metrics.mempoolBytes.Add(float64(txSize))
		m.recordMutationLocked(mempoolMutation{added: &added, addedTx: tx})
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
		return nil
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
	return *cloneMempoolTransaction(ret), true
}

func (m *Mempool) Transactions() []MempoolTransaction {
	m.RLock()
	ret := make([]MempoolTransaction, 0)
	var dagErr error
	if m.dag != nil {
		var order []string
		order, dagErr = m.dag.topologicalOrder()
		if dagErr == nil {
			ret = make([]MempoolTransaction, 0, len(order))
			for _, hash := range order {
				if tx := m.txByHash[hash]; tx != nil {
					ret = append(ret, *tx)
				}
			}
			if len(ret) != len(m.transactions) {
				dagErr = fmt.Errorf(
					"DAG transaction index inconsistent: %d of %d transactions resolved",
					len(ret),
					len(m.transactions),
				)
			}
		}
	}
	if m.dag == nil || dagErr != nil {
		ret = make([]MempoolTransaction, len(m.transactions))
		for i := range m.transactions {
			ret[i] = *m.transactions[i]
		}
	}
	m.RUnlock()
	if dagErr != nil {
		m.logger.Error(
			"falling back to admission order for mempool snapshot",
			"component", "mempool",
			"error", dagErr,
		)
	}

	// Transaction CBOR is immutable after admission. Copy the slice headers
	// under the state lock, then clone the bytes after releasing it so forging
	// and relay snapshots do not hold up the final revalidation swap in
	// proportion to total transaction bytes.
	for i := range ret {
		ret[i].Cbor = slices.Clone(ret[i].Cbor)
	}
	return ret
}

func (m *Mempool) AdmissionHeadroomBytes() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.admissionHeadroomBytesLocked()
}

func (m *Mempool) MaxAdmissionHeadroomBytes() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.maxAdmissionHeadroomBytesLocked()
}

func (m *Mempool) admissionHeadroomBytesLocked() int64 {
	headroom := m.maxAdmissionHeadroomBytesLocked() - m.currentSizeBytes
	if headroom < 0 {
		return 0
	}
	return headroom
}

func (m *Mempool) maxAdmissionHeadroomBytesLocked() int64 {
	return m.admissionLimitBytes()
}

// waitForAdmissionHeadroom blocks until the requested admission budget is
// available or the pool/connection stops. The state check and channel capture
// happen under the same read lock, so a concurrent removal cannot be missed.
func (m *Mempool) waitForAdmissionHeadroom(
	minBytes int64,
	done <-chan error,
) bool {
	if minBytes < 0 || minBytes > m.MaxAdmissionHeadroomBytes() {
		return false
	}
	if minBytes == 0 {
		return true
	}
	for {
		m.RLock()
		if m.stopped {
			m.RUnlock()
			return false
		}
		if m.admissionHeadroomBytesLocked() >= minBytes {
			m.RUnlock()
			return true
		}
		changed := m.headroomChanged
		m.RUnlock()
		if changed == nil {
			return false
		}
		select {
		case <-changed:
		case <-m.done:
			return false
		case <-done:
			return false
		}
	}
}

// cloneMempoolTransaction returns a deep copy that does not share mutable CBOR
// storage with the source transaction.
func cloneMempoolTransaction(tx *MempoolTransaction) *MempoolTransaction {
	if tx == nil {
		return nil
	}
	ret := *tx
	ret.Cbor = slices.Clone(tx.Cbor)
	return &ret
}

// CapacityBytes returns the configured maximum mempool size in bytes.
func (m *Mempool) CapacityBytes() int64 {
	return m.config.MempoolCapacity
}

func (m *Mempool) admissionLimitBytes() int64 {
	return int64(
		float64(m.config.MempoolCapacity) * m.rejectionWatermark,
	)
}

// notifyHeadroomChangedLocked wakes admission waiters after a size or lifecycle
// transition. The caller must hold the mempool write lock.
func (m *Mempool) notifyHeadroomChangedLocked() {
	if m.headroomChanged == nil {
		return
	}
	close(m.headroomChanged)
	m.headroomChanged = make(chan struct{})
}

func (m *Mempool) getTransaction(txHash string) *MempoolTransaction {
	return m.txByHash[txHash]
}

func (m *Mempool) RemoveTransaction(txHash string) {
	var events []event.Event
	m.mutationMutex.Lock()
	m.Lock()
	m.consumersMutex.Lock()
	toRemove := map[string]struct{}{txHash: {}}
	var pruned []string
	if m.dag != nil {
		toRemove = m.dag.descendants(toRemove)
		for hash := range toRemove {
			if hash != txHash {
				pruned = append(pruned, hash)
			}
		}
		m.overlay.removeByHashes(toRemove)
		m.dag.remove(toRemove)
	} else {
		// Remove from overlay with descendant pruning.
		pruned = m.overlay.removeBatchWithDescendants(toRemove)
		for _, h := range pruned {
			toRemove[h] = struct{}{}
		}
	}
	// Remove all from transactions list (backward for safe index handling)
	var removed bool
	for i, v := range slices.Backward(m.transactions) {
		if _, ok := toRemove[v.Hash]; ok {
			evt := m.removeTransactionByIndexLocked(i)
			removed = true
			if evt != nil {
				events = append(events, *evt)
			}
		}
	}
	if removed {
		m.recordMutationLocked(mempoolMutation{removed: maps.Clone(toRemove)})
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
	m.mutationMutex.Unlock()
	// MEM-03: Publish events outside the lock
	if m.eventBus != nil {
		for _, evt := range events {
			m.eventBus.Publish(RemoveTransactionEventType, evt)
		}
	}
}

// RemoveTxsByHash removes a batch of transactions by hash without cascading to
// descendants. Use after a block is confirmed: the block's outputs are now in
// the ledger, so chained pending transactions remain valid and must not be
// evicted.
func (m *Mempool) RemoveTxsByHash(hashes []string) {
	if len(hashes) == 0 {
		return
	}
	hashSet := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		hashSet[h] = struct{}{}
	}
	var events []event.Event
	m.mutationMutex.Lock()
	m.Lock()
	m.consumersMutex.Lock()
	m.overlay.removeByHashes(hashSet)
	removedHashes := make(map[string]struct{}, len(hashSet))
	if m.dag != nil {
		m.dag.remove(hashSet)
	}
	for i, v := range slices.Backward(m.transactions) {
		if _, ok := hashSet[v.Hash]; ok {
			removedHashes[v.Hash] = struct{}{}
			evt := m.removeTransactionByIndexLocked(i)
			if evt != nil {
				events = append(events, *evt)
			}
		}
	}
	if len(removedHashes) > 0 {
		m.recordMutationLocked(mempoolMutation{removed: removedHashes})
	}
	m.consumersMutex.Unlock()
	m.Unlock()
	m.mutationMutex.Unlock()
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
	m.notifyHeadroomChangedLocked()
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
	if m.dag != nil {
		return nil
	}
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
	// Remove from overlay with descendant pruning.
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
		for i, v := range slices.Backward(m.transactions) {
			if _, ok := prunedSet[v.Hash]; ok {
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
	removedHashes := make(map[string]struct{}, len(evictedHashes)+len(pruned))
	maps.Copy(removedHashes, evictedHashes)
	for _, hash := range pruned {
		removedHashes[hash] = struct{}{}
	}
	m.recordMutationLocked(mempoolMutation{removed: removedHashes})
	m.metrics.txsEvicted.Add(float64(totalEvicted))
	m.logger.Debug(
		"evicted transactions from mempool",
		"component", "mempool",
		"evicted_count", totalEvicted,
		"current_size_bytes", m.currentSizeBytes,
	)
	return events
}
