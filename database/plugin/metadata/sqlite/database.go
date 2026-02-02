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
	logger         *slog.Logger
	timerVacuum    *time.Timer
	dataDir        string
	maxConnections int
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
		return err
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
		// In-memory databases require cache=shared to be accessible from
		// multiple connections. This is needed for tests that verify
		// concurrent transaction behavior.
		//
		// Note: cache=shared can cause SQLITE_LOCKED with concurrent writes,
		// but in-memory databases are only used for testing where write
		// concurrency is controlled. The production file-based database
		// uses WAL mode which handles concurrency properly.
		metadataDb, err = gorm.Open(
			sqlite.Open(
				"file::memory:?cache=shared&_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
			),
			&gorm.Config{
				Logger:                 d.gormLogger(),
				SkipDefaultTransaction: true,
				PrepareStmt:            true,
			},
		)
		if err != nil {
			return err
		}
	} else {
		// Make sure that we can read data dir, and create if it doesn't exist
		if _, err := os.Stat(d.dataDir); err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("failed to read data dir: %w", err)
			}
			// Create data directory
			if err := os.MkdirAll(d.dataDir, 0o755); err != nil {
				return fmt.Errorf("failed to create data dir: %w", err)
			}
		}
		// Open sqlite DB
		metadataDbPath := filepath.Join(
			d.dataDir,
			"metadata.sqlite",
		)
		// Use default settings. Include PRAGMA/query parameters on the DSN
		// to enable WAL mode, reduce synchronous writes for performance, and
		// set a larger negative cache size which historically improved
		// metadata sync and throughput.
		//
		// Note: We intentionally do NOT use cache=shared because it causes
		// SQLITE_LOCKED (error 6) conflicts with connection pooling that
		// cannot be resolved by busy_timeout. WAL mode provides sufficient
		// concurrency for our read-heavy workload.
		//
		// busy_timeout(30000) tells SQLite to wait up to 30 seconds for locks
		// instead of immediately returning SQLITE_BUSY. This provides enough
		// headroom for high-contention scenarios during bulk loading.
		//
		// _txlock=immediate starts transactions in IMMEDIATE mode, acquiring
		// a RESERVED lock immediately instead of waiting until the first write.
		// This prevents SQLITE_LOCKED (error 6) deadlocks that occur when
		// multiple DEFERRED transactions try to upgrade from SHARED to RESERVED
		// locks simultaneously. With IMMEDIATE mode, transactions either
		// succeed in acquiring the write lock or wait (respecting busy_timeout).
		//
		// Keep these parameters in sync with project performance testing.
		connStr := fmt.Sprintf(
			"file:%s?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=cache_size(-50000)&_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
			metadataDbPath,
		)
		metadataDb, err = gorm.Open(
			sqlite.Open(connStr),
			&gorm.Config{
				Logger:                 d.gormLogger(),
				SkipDefaultTransaction: true,
				PrepareStmt:            true,
			},
		)
		if err != nil {
			return err
		}
	}
	d.db = metadataDb
	// Configure connection pool
	// SQLite is fundamentally single-writer, so we limit connections to avoid
	// SQLITE_LOCKED (error 6) deadlocks. The connection pool size should match
	// the DatabaseWorkers configuration. Using more connections than workers
	// creates contention on SQLite's lock infrastructure.
	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	maxConns := d.maxConnections
	if maxConns <= 0 {
		maxConns = DefaultMaxConnections // Use default if not configured
	}
	sqlDB.SetMaxIdleConns(maxConns)
	sqlDB.SetMaxOpenConns(maxConns)
	sqlDB.SetConnMaxLifetime(0) // Reuse connections indefinitely

	if err := d.init(); err != nil {
		// MetadataStoreSqlite is available for recovery, so return error but keep instance
		return err
	}
	// Create table schemas
	d.logger.Debug(fmt.Sprintf("creating table: %#v", &CommitTimestamp{}))
	if err := d.db.AutoMigrate(&CommitTimestamp{}); err != nil {
		return err
	}
	for _, model := range models.MigrateModels {
		d.logger.Debug(fmt.Sprintf("creating table: %#v", model))
		if err := d.db.AutoMigrate(model); err != nil {
			return err
		}
	}
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

	// get DB handle from gorm.DB
	db, err := d.DB().DB()
	if err != nil {
		return err
	}
	return db.Close()
}

// Create creates a record
func (d *MetadataStoreSqlite) Create(value any) *gorm.DB {
	return d.DB().Create(value)
}

// DB returns the database handle
func (d *MetadataStoreSqlite) DB() *gorm.DB {
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
