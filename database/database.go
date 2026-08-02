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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/recovery"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
)

var DefaultConfig = &Config{
	DataDir: ".dingo",
}

// defaultRecoverySubdir is where crash-recovery artifacts live under the data
// directory when the caller does not name a directory of its own.
const defaultRecoverySubdir = "recovery"

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
	//
	// UtxoValidationMode supersedes this when set; this remains the
	// fallback for callers that have not moved over: true maps to
	// UtxoValidationFail and false to UtxoValidationIgnore.
	StrictUtxoValidation bool
	// UtxoValidationMode selects what happens when a consumed UTxO past the
	// Mithril trust boundary cannot be found or recovered:
	// UtxoValidationFail, UtxoValidationWarn or UtxoValidationIgnore. An
	// empty value falls back to StrictUtxoValidation.
	UtxoValidationMode types.UtxoValidationMode
	// Recovery configures the crash-recovery subsystem: the cross-store
	// intent journal, periodic checkpoints, and the startup consistency
	// checks. Leave it nil to run without any of that, which is how the
	// database behaved before crash recovery existed.
	Recovery *recovery.Config
}

// utxoValidationMode resolves the configured mode, falling back to the
// deprecated StrictUtxoValidation boolean when no mode is set.
func (c *Config) utxoValidationMode() types.UtxoValidationMode {
	if c.UtxoValidationMode != "" {
		return c.UtxoValidationMode
	}
	if c.StrictUtxoValidation {
		return types.UtxoValidationFail
	}
	return types.UtxoValidationIgnore
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
	recovery        *recovery.Manager
	sizeMetricsStop chan struct{}
	sizeMetricsDone chan struct{}
	closeOnce       sync.Once
	closeErr        error
}

// Recovery returns the crash-recovery manager, or nil when the subsystem is
// not configured. Callers must tolerate a nil manager; its methods are all
// nil-safe no-ops.
func (d *Database) Recovery() *recovery.Manager {
	return d.recovery
}

// Blob returns the underling blob store instance
func (d *Database) Blob() blob.BlobStore {
	return d.blob
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

// Transaction starts a new database transaction and returns a handle to it
func (d *Database) Transaction(readWrite bool) *Txn {
	return NewTxn(d, readWrite)
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
		// Closing the journal flushes and syncs it, so intents recorded
		// for commits still in flight survive a shutdown that races
		// them.
		if err := d.recovery.Close(); err != nil {
			d.closeErr = errors.Join(d.closeErr, fmt.Errorf("close crash recovery journal: %w", err))
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
	if err := d.checkNodeSettings(); err != nil {
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
	if db.logger == nil {
		// Throw logs away rather than guarding every log call. init would
		// do this too, but subsystems below are brought up before it runs
		// and log on their failure paths.
		db.logger = slog.New(slog.DiscardHandler)
	}
	// Initialize the tiered CBOR cache
	db.cborCache = NewTieredCborCache(configCopy.CacheConfig, db)
	db.cborCache.SetLogger(configCopy.Logger)
	// Bring up crash recovery before init, so a database that fails its
	// consistency gate is still returned to the caller with a working
	// recovery manager attached.
	if configCopy.Recovery != nil {
		recoveryCfg := *configCopy.Recovery
		if recoveryCfg.Dir == "" {
			recoveryCfg.Dir = filepath.Join(
				configCopy.DataDir,
				defaultRecoverySubdir,
			)
		}
		if recoveryCfg.Logger == nil {
			recoveryCfg.Logger = db.logger
		}
		mgr, err := recovery.New(recoveryCfg)
		if err != nil {
			// Crash recovery makes an unclean shutdown diagnosable; it is
			// not what makes the database correct. Refusing to open a
			// database because its recovery directory is unwritable would
			// take a node down over a feature that only matters after it
			// has already crashed.
			db.logger.Error(
				"failed to open crash recovery; continuing without the intent journal, checkpoints, and startup consistency checks",
				"dir", recoveryCfg.Dir,
				"error", err,
			)
		} else {
			db.recovery = mgr
		}
	}
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
