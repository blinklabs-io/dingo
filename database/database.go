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
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
)

var DefaultConfig = &Config{
	DataDir: ".dingo",
}

// Config represents the configuration for a database instance
type Config struct {
	PromRegistry prometheus.Registerer
	Logger       *slog.Logger
	DataDir      string
	StorageMode  string // "core" or "api"
	Network      string // Cardano network name (e.g. "preview", "mainnet")
	CacheConfig  CborCacheConfig
	// StrictUtxoValidation, when true, turns an unrecoverable consumed UTxO
	// (not present in the metadata store and not reconstructable from the
	// blob store) into a hard error for blocks past the recorded Mithril
	// trust boundary (the "mithril_ledger_slot" sync state key), instead of
	// silently skipping it. Past that boundary the node should have complete
	// producer history, so a miss indicates real corruption or a bug rather
	// than an expected gap. Leave disabled (the default) when bootstrapping
	// from a non-genesis chainsync intersect point without a Mithril
	// snapshot import, where pre-intersect UTxOs are legitimately absent.
	StrictUtxoValidation bool
	// NetworkMagic is the protocol magic. It is the real network
	// discriminator: a custom or devnet database may have an empty Network
	// while still needing identity enforcement.
	NetworkMagic uint32
	// StartEra is the experimental start era ("dijkstra" or empty).
	StartEra string
	// BlobPlugin and MetadataPlugin name the storage providers that
	// produced this database.
	BlobPlugin     string
	MetadataPlugin string
}

// Stores contains the provider-owned storage services injected into a
// Database. Their lifecycle remains owned by the plugin host.
type Stores struct {
	Blob     blob.BlobStore
	Metadata metadata.MetadataStore
}

// isNilStore reports whether an injected store is a nil interface or an
// interface wrapping a typed nil pointer, which would pass a plain == nil check
// but panic when the store is used.
func isNilStore(store any) bool {
	if store == nil {
		return true
	}
	v := reflect.ValueOf(store)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

// Database represents our data storage services
type Database struct {
	config          *Config
	logger          *slog.Logger
	blob            blob.BlobStore
	metadata        metadata.MetadataStore
	cborCache       *TieredCborCache
	sizeMetricsStop chan struct{}
	sizeMetricsDone chan struct{}
	closeOnce       sync.Once
	closeErr        error

	// commitBarrier lets PauseCommits/PauseCommitsContext hold off every
	// in-flight and new read-write Txn that opens a metadata write
	// transaction, without a full quiesce, so database/lifecycle.Snapshot
	// can back up the blob and metadata stores and NewReadSnapshotContext
	// can open both read views as of the same logical commit boundary. Only
	// Txns holding the metadata store's single write
	// connection participate (see acquireCommitBarrier) — a blob-only Txn
	// never touches that connection and never writes the commit timestamp
	// this guards, so it does not take part. Txn construction holds the
	// read side (many concurrent read-write Txns proceed normally);
	// PauseCommits/PauseCommitsContext hold the write side.
	//
	// A cancellableBarrier, not a plain sync.RWMutex: PauseCommitsContext
	// needs to be able to fully abandon a queued exclusive-acquire attempt
	// when its ctx is cancelled, which sync.RWMutex cannot do (see that
	// type's doc comment for why a bare "stop waiting on the result"
	// workaround still leaves new read-write Txns blocked behind a
	// phantom queued writer). Its zero value is directly usable, same as
	// the sync.RWMutex it replaces, so no constructor change is needed.
	commitBarrier cancellableBarrier

	// destructiveTransitionBarrier prevents a coordinated read or lifecycle
	// snapshot from opening while one logical rollback spans multiple physical
	// transactions. Primary-chain rollback deletes block blobs first, then the
	// ledger truncates metadata in a combined transaction; neither physical
	// transaction can safely stand alone as a snapshot boundary. Snapshot
	// construction holds the shared side before taking commitBarrier, while
	// BeginDestructiveTransition holds the exclusive side across the whole
	// logical rollback. Ordinary writes and blob-only cleanup do not take this
	// barrier, preserving their existing concurrency and avoiding nested-write
	// deadlocks.
	destructiveTransitionBarrier cancellableBarrier
}

// Blob returns the underling blob store instance
func (d *Database) Blob() blob.BlobStore {
	return d.blob
}

// PauseCommits first waits for any multi-transaction destructive transition,
// then blocks until every currently open read-write Txn that participates in
// the commit barrier (see acquireCommitBarrier) has reached Commit, Rollback,
// or Release — not merely until one already inside its Commit call finishes,
// but until every such Txn opened before this call, however far along it
// currently is, concludes one way or another. It then blocks any new one from
// being constructed until the returned resume func is called. Already-open
// reads remain usable, and this is not a quiesce — nothing is torn down and no
// peers are disconnected. New read-write Txn construction and other callers
// capturing an exclusive commit boundary block briefly.
//
// database/lifecycle.Snapshot uses this to bracket its blob and metadata
// backup calls: each backup is independently consistent as of whenever it
// runs, but a commit landing between the two would write its timestamp to
// one store's backup and not the other's, so the restored copy fails
// checkCommitTimestamp's cross-check. NewReadSnapshotContext similarly
// brackets opening its two read views. The destructive-transition shared hold
// also prevents either snapshot type from opening in the gap between a
// blob-only primary-chain truncation and its later ledger metadata rollback.
// Together the two barriers keep both stores at one logical boundary.
func (d *Database) PauseCommits() (resume func()) {
	d.destructiveTransitionBarrier.RLock()
	token := d.commitBarrier.Lock()
	return func() {
		d.commitBarrier.Unlock(token)
		d.destructiveTransitionBarrier.RUnlock()
	}
}

// PauseCommitsContext is PauseCommits, but the wait for the barrier can
// be abandoned via ctx: if a long-running write transaction is currently
// open, acquiring the exclusive side can block for as long as that
// transaction takes to commit, and plain PauseCommits gives callers such as
// lifecycle.Snapshot and NewReadSnapshotContext no way to give up on that wait
// if their own operation is cancelled.
//
// If ctx is cancelled before the barrier is acquired, this returns
// ctx.Err() and a nil resume, having fully withdrawn its claim on the
// barrier: unlike a plain sync.RWMutex (which has no cancellable Lock and
// so would leave an abandoned Lock() call queued, blocking every new
// read-write Txn behind it via writer preference until whatever it was
// waiting on eventually releases — see cancellableBarrier's doc comment),
// a cancelled wait here does not stall anything else.
func (d *Database) PauseCommitsContext(
	ctx context.Context,
) (resume func(), err error) {
	if err := d.destructiveTransitionBarrier.RLockContext(ctx); err != nil {
		return nil, err
	}
	token, err := d.commitBarrier.LockContext(ctx)
	if err != nil {
		d.destructiveTransitionBarrier.RUnlock()
		return nil, err
	}
	return func() {
		d.commitBarrier.Unlock(token)
		d.destructiveTransitionBarrier.RUnlock()
	}, nil
}

// BeginDestructiveTransition prevents coordinated read and lifecycle snapshots
// from opening while a logical destructive update spans multiple physical
// transactions. The returned finish function must be called exactly once after
// both the blob deletion and corresponding metadata rollback have completed.
//
// The transition barrier is deliberately separate from commitBarrier. A
// rollback must open a normal combined write transaction after its blob-only
// chain deletion, so taking commitBarrier exclusively for the whole operation
// would deadlock that nested write. Ordinary blob-only transactions do not take
// either barrier and remain safe for helpers such as deleteUtxoBlobs that open
// one beneath an existing combined write.
func (d *Database) BeginDestructiveTransition() (finish func()) {
	token := d.destructiveTransitionBarrier.Lock()
	return func() { d.destructiveTransitionBarrier.Unlock(token) }
}

// Config returns the config object used for the database instance
func (d *Database) Config() *Config {
	return d.config
}

// DataDir returns the path to the data directory used for storage
func (d *Database) DataDir() string {
	return d.config.DataDir
}

// Logger returns the logger instance
func (d *Database) Logger() *slog.Logger {
	return d.config.Logger
}

// Metadata returns the underlying metadata store instance
func (d *Database) Metadata() metadata.MetadataStore {
	return d.metadata
}

// The accessors below hand out the metadata store narrowed to one storage
// domain. Facade methods go through the accessor for the domain they touch
// rather than through d.metadata, so the compiler -- not review -- is what
// keeps a UTxO method from reaching into governance state. A facade method
// that genuinely spans two domains calls both accessors, which also makes
// that span visible at the call site instead of hiding it behind the full
// surface. d.metadata itself remains for the domains not yet extracted.

// certificateStore narrows to on-chain certificates.
func (d *Database) certificateStore() metadata.CertificateStore {
	return d.metadata
}

// epochStore narrows to the epoch table.
func (d *Database) epochStore() metadata.EpochStore {
	return d.metadata
}

// governanceStore narrows to the Conway governance surface.
func (d *Database) governanceStore() metadata.GovernanceStore {
	return d.metadata
}

// stakeSnapshotStore narrows to epoch-boundary stake snapshots.
func (d *Database) stakeSnapshotStore() metadata.StakeSnapshotStore {
	return d.metadata
}

// transactionStore narrows to chain transactions. Note this is not
// TxnStore: see the interface doc comments.
func (d *Database) transactionStore() metadata.TransactionStore {
	return d.metadata
}

// utxoStore narrows to the UTxO set.
func (d *Database) utxoStore() metadata.UtxoStore {
	return d.metadata
}

// Transaction starts a new database transaction and returns a handle to it
func (d *Database) Transaction(readWrite bool) *Txn {
	return NewTxn(d, readWrite)
}

// TransactionContext starts a transaction whose metadata queries observe
// context cancellation.
func (d *Database) TransactionContext(
	ctx context.Context,
	readWrite bool,
) *Txn {
	return NewTxnContext(ctx, d, readWrite)
}

// BlobTxn starts a new blob-only database transaction and returns a handle to it
func (d *Database) BlobTxn(readWrite bool) *Txn {
	return NewBlobOnlyTxn(d, readWrite)
}

// MetadataTxn starts a new metadata-only database transaction and returns a handle to it
func (d *Database) MetadataTxn(readWrite bool) *Txn {
	return NewMetadataOnlyTxn(d, readWrite)
}

// Close cleans up the database connections
func (d *Database) Close() error {
	d.closeOnce.Do(func() {
		// Stop the metrics goroutine if running
		if d.sizeMetricsStop != nil {
			close(d.sizeMetricsStop)
			<-d.sizeMetricsDone
		}
	})
	return d.closeErr
}

func (d *Database) init() error {
	if d.logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		d.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Check commit timestamp
	if err := d.checkCommitTimestamp(); err != nil {
		return err
	}
	// Check immutable settings have not changed since initial sync
	if err := d.CheckNodeSettings(); err != nil {
		return err
	}
	return nil
}

// New creates a database over injected stores. The caller owns the store
// lifecycle and must keep both stores alive until Database.Close returns.
func New(config *Config, stores Stores) (*Database, error) {
	if config == nil {
		config = DefaultConfig
	}
	// Create a copy of the config to avoid mutating the original
	cfgVal := *config
	configCopy := &cfgVal
	if configCopy.StorageMode == "" {
		configCopy.StorageMode = types.StorageModeCore
	}
	if configCopy.Logger == nil {
		configCopy.Logger = slog.New(slog.DiscardHandler)
	}
	// Stores is an exported injection boundary, so reject a typed nil (an
	// interface wrapping a nil pointer) as well as an untyped nil; otherwise
	// the plain == nil check passes and the nil underlying store panics later
	// in init.
	if stores.Blob == nil || isNilStore(stores.Blob) {
		return nil, errors.New("blob store is required")
	}
	if stores.Metadata == nil || isNilStore(stores.Metadata) {
		return nil, errors.New("metadata store is required")
	}
	db := &Database{
		blob:     stores.Blob,
		metadata: stores.Metadata,
		logger:   configCopy.Logger,
		config:   configCopy,
	}
	// Initialize the tiered CBOR cache
	db.cborCache = NewTieredCborCache(configCopy.CacheConfig, db)
	db.cborCache.SetLogger(configCopy.Logger)
	// Register cache metrics if prometheus registry is available
	if configCopy.PromRegistry != nil {
		db.cborCache.Metrics().Register(configCopy.PromRegistry)
		if err := RegisterBlockByHashMetrics(configCopy.PromRegistry); err != nil {
			configCopy.Logger.Warn(
				"failed to register block-hash index metrics",
				"error", err,
			)
		}
		if err := db.cborCache.RegisterCASMetrics(configCopy.PromRegistry); err != nil {
			configCopy.Logger.Warn(
				"failed to register hot cache CAS metrics",
				"error", err,
			)
		}
		if err := RegisterBlobOrphanMetrics(configCopy.PromRegistry); err != nil {
			configCopy.Logger.Warn(
				"failed to register blob orphan metrics",
				"error", err,
			)
		}
	}
	// Register database size metrics
	if configCopy.PromRegistry != nil {
		blobSizeGauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "dingo_database_size_bytes",
			Help:        "on-disk size of the database in bytes",
			ConstLabels: prometheus.Labels{"store": "blob"},
		})
		metadataSizeGauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        "dingo_database_size_bytes",
			Help:        "on-disk size of the database in bytes",
			ConstLabels: prometheus.Labels{"store": "metadata"},
		})
		if err := configCopy.PromRegistry.Register(blobSizeGauge); err != nil {
			if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
				blobSizeGauge = are.ExistingCollector.(prometheus.Gauge)
			}
		}
		if err := configCopy.PromRegistry.Register(metadataSizeGauge); err != nil {
			if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
				metadataSizeGauge = are.ExistingCollector.(prometheus.Gauge)
			}
		}

		db.sizeMetricsStop = make(chan struct{})
		db.sizeMetricsDone = make(chan struct{})
		go func() {
			defer close(db.sizeMetricsDone)
			ticker := time.NewTicker(60 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-db.sizeMetricsStop:
					return
				case <-ticker.C:
					if db.blob != nil {
						if size, err := db.blob.DiskSize(); err == nil {
							blobSizeGauge.Set(float64(size))
						}
					}
					if db.metadata != nil {
						if size, err := db.metadata.DiskSize(); err == nil {
							metadataSizeGauge.Set(float64(size))
						}
					}
				}
			}
		}()
	}
	if err := db.init(); err != nil {
		// Database is available for recovery, so return it with error
		return db, err
	}
	return db, nil
}

// StorageMode returns the configured storage mode ("core" or "api").
func (d *Database) StorageMode() string {
	return d.config.StorageMode
}

// CborCache returns the tiered CBOR cache for accessing cached CBOR data.
// This can be used for metrics registration or direct cache access.
func (d *Database) CborCache() *TieredCborCache {
	return d.cborCache
}

func (d *Database) SetBlobStore(b blob.BlobStore) {
	d.blob = b
}
