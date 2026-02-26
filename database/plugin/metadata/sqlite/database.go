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

package sqlite

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/glebarez/sqlite"
	sloggorm "github.com/orandin/slog-gorm"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/opentelemetry/tracing"
)

// sqliteBindVarLimit is a conservative chunk size for IN clauses to avoid
// exceeding SQLite's max bind variable limit (default 999).
const sqliteBindVarLimit = 400

// sqliteTxn wraps a gorm transaction and implements types.Txn
type sqliteTxn struct {
	beginErr error
	db       *gorm.DB
	finished bool
}

func newSqliteTxn(db *gorm.DB) *sqliteTxn {
	return &sqliteTxn{db: db}
}

func newFailedSqliteTxn(err error) *sqliteTxn {
	return &sqliteTxn{beginErr: err}
}

func (t *sqliteTxn) Commit() error {
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished {
		return nil
	}
	if t.db == nil {
		t.finished = true
		return nil
	}
	if result := t.db.Commit(); result.Error != nil {
		return result.Error
	}
	t.finished = true
	return nil
}

func (t *sqliteTxn) Rollback() error {
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished {
		return nil
	}
	if t.db != nil {
		if result := t.db.Rollback(); result.Error != nil {
			return result.Error
		}
	}
	t.finished = true
	return nil
}

// MetadataStoreSqlite stores all data in sqlite. Data may not be persisted
type MetadataStoreSqlite struct {
	promRegistry   prometheus.Registerer
	db             *gorm.DB
	readDB         *gorm.DB
	logger         *slog.Logger
	timerVacuum    *time.Timer
	dataDir        string
	maxConnections int
	storageMode    string
	timerMutex     sync.Mutex
	closed         bool
}

// New creates a new database
func New(
	dataDir string,
	logger *slog.Logger,
	promRegistry prometheus.Registerer,
) (*MetadataStoreSqlite, error) {
	return NewWithOptions(
		WithDataDir(dataDir),
		WithLogger(logger),
		WithPromRegistry(promRegistry),
	)
}

// NewWithOptions creates a new database with options
func NewWithOptions(opts ...SqliteOptionFunc) (*MetadataStoreSqlite, error) {
	db := &MetadataStoreSqlite{}

	// Apply options
	for _, opt := range opts {
		opt(db)
	}

	// Set defaults after options are applied (no side effects)
	if db.logger == nil {
		db.logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}

	// Default and validate storageMode
	if db.storageMode == "" {
		db.storageMode = types.StorageModeCore
	}
	switch db.storageMode {
	case types.StorageModeCore, types.StorageModeAPI:
		// valid
	default:
		return nil, fmt.Errorf(
			"invalid storage mode %q: must be %q or %q",
			db.storageMode,
			types.StorageModeCore,
			types.StorageModeAPI,
		)
	}

	// Note: Database initialization moved to Start()
	return db, nil
}

func (d *MetadataStoreSqlite) init() error {
	if d.logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		d.logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	// Configure tracing for GORM
	if err := d.db.Use(tracing.NewPlugin(tracing.WithoutMetrics())); err != nil {
		return fmt.Errorf("failed to configure tracing on write pool: %w", err)
	}
	// Add tracing to the read pool when it is a separate instance
	if d.readDB != nil && d.readDB != d.db {
		if err := d.readDB.Use(
			tracing.NewPlugin(tracing.WithoutMetrics()),
		); err != nil {
			return fmt.Errorf("failed to configure tracing on read pool: %w", err)
		}
	}
	// Schedule daily database vacuum to free unused space
	d.scheduleDailyVacuum()
	return nil
}

func (d *MetadataStoreSqlite) gormLogger() gormlogger.Interface {
	return sloggorm.New(
		sloggorm.WithHandler(d.logger.With("component", "gorm").Handler()),
	)
}

func (d *MetadataStoreSqlite) runVacuum() error {
	d.timerMutex.Lock()
	closed := d.closed
	d.timerMutex.Unlock()
	if d.dataDir == "" || closed {
		return nil
	}
	if result := d.DB().Raw("VACUUM"); result.Error != nil {
		return result.Error
	}
	return nil
}

// scheduleDailyVacuum schedules a daily vacuum operation
func (d *MetadataStoreSqlite) scheduleDailyVacuum() {
	d.timerMutex.Lock()
	defer d.timerMutex.Unlock()
	if d.closed {
		return
	}

	if d.timerVacuum != nil {
		d.timerVacuum.Stop()
	}
	daily := time.Duration(24) * time.Hour
	f := func() {
		d.logger.Debug(
			"running vacuum on sqlite metadata database",
		)
		// schedule next run
		defer d.scheduleDailyVacuum()
		if err := d.runVacuum(); err != nil {
			d.logger.Error(
				"failed to free unused space in metadata store",
				"component", "database",
				"error", err,
			)
		}
	}
	d.timerVacuum = time.AfterFunc(daily, f)
}

// AutoMigrate wraps the gorm AutoMigrate
func (d *MetadataStoreSqlite) AutoMigrate(dst ...any) error {
	return d.DB().AutoMigrate(dst...)
}

// Start implements the plugin.Plugin interface
func (d *MetadataStoreSqlite) Start() error {
	var metadataDb *gorm.DB
	var err error
	if d.dataDir == "" {
		// No dataDir, use in-memory config
		// In-memory databases require cache=shared to be accessible
		// from multiple connections. This is needed for tests that
		// verify concurrent transaction behavior.
		//
		// Note: cache=shared can cause SQLITE_LOCKED with concurrent
		// writes, but in-memory databases are only used for testing
		// where write concurrency is controlled. The production
		// file-based database uses WAL mode with separate read/write
		// pools which handles concurrency properly.
		metadataDb, err = gorm.Open(
			sqlite.Open(
				"file::memory:?cache=shared"+
					"&_pragma=busy_timeout(30000)"+
					"&_pragma=foreign_keys(1)",
			),
			&gorm.Config{
				Logger:                 d.gormLogger(),
				SkipDefaultTransaction: true,
				PrepareStmt:            true,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to open in-memory database: %w", err)
		}
		// For in-memory mode, read and write share the same handle
		d.db = metadataDb
		d.readDB = metadataDb
	} else {
		// Make sure that we can read data dir, and create if it
		// doesn't exist
		if _, err := os.Stat(d.dataDir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf(
					"failed to read data dir: %w",
					err,
				)
			}
			// Create data directory
			if err := os.MkdirAll(d.dataDir, 0o755); err != nil {
				return fmt.Errorf(
					"failed to create data dir: %w",
					err,
				)
			}
		}
		// Open sqlite DB with separate read and write connections.
		//
		// SQLite allows exactly one writer at a time. Using separate
		// pools prevents SQLITE_BUSY by:
		//   1. Write pool (1 connection): serializes all writes
		//      naturally. No contention because only one write
		//      transaction can be active.
		//   2. Read pool (N connections): WAL mode allows concurrent
		//      readers that don't block the writer.
		//
		// Without this separation, the Go connection pool hands out
		// any available connection for any operation. Multiple
		// goroutines that each begin a write transaction compete for
		// SQLite's single write lock, causing SQLITE_BUSY even with
		// busy_timeout because the lock holder may be on a different
		// pooled connection that is blocked waiting for a connection
		// back from the pool (deadlock).
		metadataDbPath := filepath.Join(
			d.dataDir,
			"metadata.sqlite",
		)
		// Common PRAGMA parameters for both read and write pools:
		// - journal_mode(WAL): Write-Ahead Logging for concurrent
		//   read/write access
		// - synchronous(NORMAL): Safe with WAL; only syncs on
		//   checkpoint, not every commit
		// - cache_size(-50000): ~50MB page cache per connection
		// - busy_timeout(30000): Wait up to 30s for locks instead
		//   of returning SQLITE_BUSY immediately
		// - foreign_keys(1): Enforce FK constraints
		// - mmap_size(268435456): Memory-map up to 256MB for
		//   faster reads
		commonPragmas := "&_pragma=journal_mode(WAL)" +
			"&_pragma=synchronous(NORMAL)" +
			"&_pragma=cache_size(-50000)" +
			"&_pragma=busy_timeout(30000)" +
			"&_pragma=foreign_keys(1)" +
			"&_pragma=mmap_size(268435456)"

		// Write connection: uses _txlock=immediate so that BEGIN
		// acquires the RESERVED lock immediately. With a single
		// write connection, this never contends.
		writeConnStr := fmt.Sprintf(
			"file:%s?_txlock=immediate%s",
			metadataDbPath,
			commonPragmas,
		)
		writeDB, err := gorm.Open(
			sqlite.Open(writeConnStr),
			&gorm.Config{
				Logger:                 d.gormLogger(),
				SkipDefaultTransaction: true,
				PrepareStmt:            true,
			},
		)
		if err != nil {
			return fmt.Errorf("failed to open write pool: %w", err)
		}

		// Read connections: use default DEFERRED transaction mode.
		// Readers in WAL mode never block writers and vice versa.
		readConnStr := fmt.Sprintf(
			"file:%s?mode=ro%s",
			metadataDbPath,
			commonPragmas,
		)
		readDB, err := gorm.Open(
			sqlite.Open(readConnStr),
			&gorm.Config{
				Logger:                 gormlogger.Discard,
				SkipDefaultTransaction: true,
				PrepareStmt:            true,
			},
		)
		if err != nil {
			// Close the already-opened write pool to avoid
			// leaking a SQLite connection.
			if sqlDB, closeErr := writeDB.DB(); closeErr == nil {
				_ = sqlDB.Close()
			}
			return fmt.Errorf("failed to open read pool: %w", err)
		}

		d.db = writeDB
		d.readDB = readDB
	}

	// Ensure both pools are closed if anything below fails.
	var success bool
	defer func() {
		if !success {
			_ = d.Close()
		}
	}()

	// Configure connection pools
	if d.dataDir != "" {
		// Write pool: exactly 1 connection. SQLite only supports a
		// single writer, so having more than 1 write connection just
		// creates lock contention. A single connection serializes
		// writes naturally without any SQLITE_BUSY errors.
		writeSqlDB, err := d.db.DB()
		if err != nil {
			return fmt.Errorf("failed to get write sql.DB: %w", err)
		}
		writeSqlDB.SetMaxOpenConns(1)
		writeSqlDB.SetMaxIdleConns(1)
		writeSqlDB.SetConnMaxLifetime(0)

		// Read pool: multiple connections for concurrent reads.
		// WAL mode allows unlimited concurrent readers.
		maxReadConns := d.maxConnections
		if maxReadConns <= 0 {
			maxReadConns = DefaultMaxConnections
		}
		readSqlDB, err := d.readDB.DB()
		if err != nil {
			return fmt.Errorf("failed to get read sql.DB: %w", err)
		}
		readSqlDB.SetMaxOpenConns(maxReadConns)
		readSqlDB.SetMaxIdleConns(maxReadConns)
		readSqlDB.SetConnMaxLifetime(0)
	} else {
		// In-memory: single shared pool
		sqlDB, err := d.db.DB()
		if err != nil {
			return fmt.Errorf("failed to get sql.DB for in-memory pool: %w", err)
		}
		maxConns := d.maxConnections
		if maxConns <= 0 {
			maxConns = DefaultMaxConnections
		}
		sqlDB.SetMaxIdleConns(maxConns)
		sqlDB.SetMaxOpenConns(maxConns)
		sqlDB.SetConnMaxLifetime(0)
	}

	if err := d.init(); err != nil {
		return err
	}
	// Deduplicate pool_stake_snapshot rows before AutoMigrate
	// creates the unique index idx_pool_stake_epoch_pool.
	if err := models.DedupePoolStakeSnapshots(
		d.db, d.logger,
	); err != nil {
		return fmt.Errorf(
			"pool_stake_snapshot dedup failed: %w", err,
		)
	}
	// Create table schemas (uses write connection)
	d.logger.Debug(
		"creating table",
		"model", fmt.Sprintf("%T", &CommitTimestamp{}),
	)
	if err := d.db.AutoMigrate(&CommitTimestamp{}); err != nil {
		return err
	}
	for _, model := range models.MigrateModels {
		d.logger.Debug(
			"creating table",
			"model", fmt.Sprintf("%T", model),
		)
		if err := d.db.AutoMigrate(model); err != nil {
			return err
		}
	}
	success = true
	return nil
}

// Stop implements the plugin.Plugin interface
func (d *MetadataStoreSqlite) Stop() error {
	return d.Close()
}

// Close gets the database handle from our MetadataStore and closes it
func (d *MetadataStoreSqlite) Close() error {
	d.timerMutex.Lock()
	d.closed = true
	if d.timerVacuum != nil {
		d.timerVacuum.Stop()
		d.timerVacuum = nil
	}
	d.timerMutex.Unlock()

	var errs []error
	// Close read pool first (if separate from write pool)
	if d.readDB != nil && d.readDB != d.db {
		if readSqlDB, err := d.readDB.DB(); err != nil {
			errs = append(errs, err)
		} else {
			if err := readSqlDB.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	// Close write pool
	if d.db != nil {
		if db, err := d.DB().DB(); err != nil {
			errs = append(errs, err)
		} else {
			if err := db.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// Create creates a record
func (d *MetadataStoreSqlite) Create(value any) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the write database handle
func (d *MetadataStoreSqlite) DB() *gorm.DB {
	return d.db
}

// ReadDB returns the read-only database handle. For file-based
// databases this is a separate connection pool optimized for
// concurrent reads via WAL mode. For in-memory databases this
// returns the same handle as DB().
func (d *MetadataStoreSqlite) ReadDB() *gorm.DB {
	if d.readDB != nil {
		return d.readDB
	}
	return d.db
}

// First returns the first DB entry
func (d *MetadataStoreSqlite) First(args any) *gorm.DB {
	return d.DB().First(args)
}

// Order orders a DB query
func (d *MetadataStoreSqlite) Order(args any) *gorm.DB {
	return d.DB().Order(args)
}

// Transaction creates a gorm transaction
func (d *MetadataStoreSqlite) Transaction() types.Txn {
	db := d.DB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin transaction",
			"error", db.Error,
		)
		return newFailedSqliteTxn(db.Error)
	}
	return newSqliteTxn(db)
}

// ReadTransaction creates a read-only transaction using the read
// connection pool. For file-based databases this avoids contending
// with the write connection; for in-memory databases it falls back
// to the write connection.
func (d *MetadataStoreSqlite) ReadTransaction() types.Txn {
	db := d.ReadDB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin read transaction",
			"error", db.Error,
		)
		return newFailedSqliteTxn(db.Error)
	}
	return newSqliteTxn(db)
}

// BeginTxn starts a transaction and returns the handle with an error.
// Callers that prefer explicit error handling can use this instead of Transaction().
func (d *MetadataStoreSqlite) BeginTxn() (types.Txn, error) {
	db := d.DB().Begin()
	if db.Error != nil {
		d.logger.Error(
			"failed to begin transaction",
			"error", db.Error,
		)
		return newFailedSqliteTxn(db.Error), db.Error
	}
	return newSqliteTxn(db), nil
}

// Where constrains a DB query
func (d *MetadataStoreSqlite) Where(
	query any,
	args ...any,
) *gorm.DB {
	return d.DB().Where(query, args...)
}

// SetBulkLoadPragmas configures SQLite PRAGMAs optimized for bulk loading.
// These settings trade crash safety for speed, which is acceptable because
// immutable chunk data can always be reloaded.
func (d *MetadataStoreSqlite) SetBulkLoadPragmas() error {
	pragmas := []string{
		"PRAGMA synchronous = OFF",
		"PRAGMA cache_size = -200000", // 200MB cache (4x default)
		"PRAGMA temp_store = MEMORY",
		"PRAGMA wal_autocheckpoint = 10000", // Reduce checkpoint frequency
	}
	for _, pragma := range pragmas {
		if result := d.DB().Exec(pragma); result.Error != nil {
			// Rollback any already-applied pragmas before returning
			if restoreErr := d.RestoreNormalPragmas(); restoreErr != nil {
				d.logger.Error(
					"failed to restore pragmas after partial bulk-load failure",
					"error", restoreErr,
				)
			}
			return fmt.Errorf("set pragma %q: %w", pragma, result.Error)
		}
	}
	d.logger.Info(
		"set bulk-load PRAGMAs (synchronous=OFF, cache=200MB, temp=MEMORY)",
	)
	return nil
}

// RestoreNormalPragmas restores SQLite PRAGMAs to production settings
// after bulk loading is complete.
func (d *MetadataStoreSqlite) RestoreNormalPragmas() error {
	pragmas := []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -50000", // Restore default 50MB
		"PRAGMA temp_store = DEFAULT",
		"PRAGMA wal_autocheckpoint = 1000", // Restore default
	}
	var errs []error
	for _, pragma := range pragmas {
		if result := d.DB().Exec(pragma); result.Error != nil {
			errs = append(
				errs,
				fmt.Errorf(
					"restore pragma %q: %w",
					pragma,
					result.Error,
				),
			)
		}
	}
	if len(errs) > 0 {
		d.logger.Warn(
			"partially restored normal PRAGMAs",
			"error",
			errors.Join(errs...),
		)
		return errors.Join(errs...)
	}
	d.logger.Info("restored normal PRAGMAs")
	return nil
}
