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

package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
)

var savepointNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Config contains backend-neutral dependencies for a Store.
type Config struct {
	WriteDB *sql.DB
	ReadDB  *sql.DB
	Dialect Dialect
	Logger  *slog.Logger
	// StorageMode controls retention of API-only transaction detail. Empty
	// selects the consensus-focused core mode.
	StorageMode string

	Migrations      []migrations.Migration
	MigrationLocker migrations.Locker
	DiskSize        func() (int64, error)
	// Maintenance is optional backend maintenance started only after the
	// migration readiness gate succeeds. SQLite uses it for periodic VACUUM.
	Maintenance         func(context.Context) error
	MaintenanceInterval time.Duration
}

// Store owns the shared database/sql pools. Provider packages own DSN and
// driver selection; metadata behavior belongs here.
type Store struct {
	writeDB     *sql.DB
	readDB      *sql.DB
	dialect     Dialect
	logger      *slog.Logger
	storageMode string

	migrations        []migrations.Migration
	migrationLocker   migrations.Locker
	diskSize          func() (int64, error)
	maintenance       func(context.Context) error
	maintenanceEvery  time.Duration
	maintenanceCancel context.CancelFunc
	maintenanceDone   chan struct{}
	ready             atomic.Bool
	closed            atomic.Bool
	startMu           sync.Mutex
	bulkMu            sync.RWMutex
	bulkConn          *sql.Conn

	closeOnce sync.Once
	closeErr  error
}

// New constructs a shared store around already-opened connection pools.
func New(config Config) (*Store, error) {
	if config.WriteDB == nil {
		return nil, errors.New("sqlstore: write database is required")
	}
	if config.Dialect == nil {
		return nil, errors.New("sqlstore: dialect is required")
	}
	if config.ReadDB == nil {
		config.ReadDB = config.WriteDB
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.StorageMode == "" {
		config.StorageMode = types.StorageModeCore
	}
	switch config.StorageMode {
	case types.StorageModeCore, types.StorageModeAPI:
	default:
		return nil, fmt.Errorf(
			"sqlstore: invalid storage mode %q",
			config.StorageMode,
		)
	}
	if len(config.Migrations) > 0 && config.MigrationLocker == nil {
		return nil, errors.New(
			"sqlstore: migration locker is required when migrations are configured",
		)
	}
	return &Store{
		writeDB:          config.WriteDB,
		readDB:           config.ReadDB,
		dialect:          config.Dialect,
		logger:           config.Logger,
		storageMode:      config.StorageMode,
		migrations:       config.Migrations,
		migrationLocker:  config.MigrationLocker,
		diskSize:         config.DiskSize,
		maintenance:      config.Maintenance,
		maintenanceEvery: config.MaintenanceInterval,
	}, nil
}

// DiskSize returns backend storage usage when the provider supplies it.
func (s *Store) DiskSize() (int64, error) {
	if s.diskSize == nil {
		return 0, nil
	}
	return s.diskSize()
}

// Start verifies connectivity and completes every offline migration before
// making the store available to normal readers or writers.
func (s *Store) Start(ctx context.Context) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.closed.Load() {
		return errors.New("sqlstore: store is closed")
	}
	if s.ready.Load() {
		return nil
	}
	if err := s.writeDB.PingContext(ctx); err != nil {
		return fmt.Errorf("sqlstore: ping write database: %w", err)
	}
	if len(s.migrations) > 0 {
		runner := migrations.Runner{
			DB:       s.writeDB,
			Dialect:  s.dialect.Name(),
			Registry: s.migrations,
			Locker:   s.migrationLocker,
			Logger:   s.logger,
		}
		if err := runner.Run(ctx); err != nil {
			return fmt.Errorf("sqlstore: metadata upgrade: %w", err)
		}
	}
	if s.readDB != s.writeDB {
		if err := s.readDB.PingContext(ctx); err != nil {
			return fmt.Errorf("sqlstore: ping read database: %w", err)
		}
	}
	s.ready.Store(true)
	// Maintenance owns its own lifetime and must not inherit the startup
	// context, which callers commonly cancel as soon as Start returns.
	s.startMaintenance() //nolint:contextcheck
	return nil
}

// Ready reports whether startup migrations completed successfully.
func (s *Store) Ready() bool {
	return s.ready.Load()
}

// Dialect returns the backend capability implementation.
func (s *Store) Dialect() Dialect {
	return s.dialect
}

// WritePoolStats exposes database/sql pool telemetry without exposing the
// underlying database handle.
func (s *Store) WritePoolStats() sql.DBStats {
	return s.writeDB.Stats()
}

// ReadPoolStats exposes read-pool telemetry. SQLite file stores use this to
// report their independently-sized WAL reader pool.
func (s *Store) ReadPoolStats() sql.DBStats {
	return s.readDB.Stats()
}

// Transaction begins a write transaction. Begin failures are retained on the
// returned transaction because the historical MetadataStore contract cannot
// return an error from this method.
func (s *Store) Transaction() types.Txn {
	if !s.ready.Load() {
		return &sqlTxn{
			owner:    s,
			beginErr: errors.New("sqlstore: store is not ready"),
		}
	}
	tx, err := s.beginWriteTx(context.Background())
	return &sqlTxn{owner: s, tx: tx, beginErr: err}
}

// ReadTransaction begins a repeatable, read-only transaction on the read pool.
func (s *Store) ReadTransaction() types.Txn {
	if !s.ready.Load() {
		return &sqlTxn{
			owner:    s,
			beginErr: errors.New("sqlstore: store is not ready"),
		}
	}
	tx, err := s.readDB.BeginTx(
		context.Background(),
		s.dialect.BeginOptions(true),
	)
	return &sqlTxn{owner: s, tx: tx, beginErr: err}
}

// BeginTxn is the error-returning form used by new internal callers.
func (s *Store) BeginTxn(
	ctx context.Context,
	readOnly bool,
) (types.Txn, error) {
	if !s.ready.Load() {
		return nil, errors.New("sqlstore: store is not ready")
	}
	db := s.writeDB
	if readOnly {
		db = s.readDB
	}
	var (
		tx  *sql.Tx
		err error
	)
	if !readOnly {
		tx, err = s.beginWriteTx(ctx)
	} else {
		tx, err = db.BeginTx(ctx, s.dialect.BeginOptions(readOnly))
	}
	if err != nil {
		return nil, err
	}
	return &sqlTxn{owner: s, tx: tx}, nil
}

// Close closes each owned pool exactly once.
func (s *Store) Close() error {
	return s.CloseContext(context.Background())
}

// CloseContext cancels maintenance and closes each owned pool. The lifecycle
// context is also passed to the maintenance wait so cancellation can interrupt
// a long-running VACUUM before the provider shutdown deadline expires.
func (s *Store) CloseContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("sqlstore: close context is nil")
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		s.ready.Store(false)
		if s.bulkConn != nil {
			// Restore session variables before releasing the dedicated
			// connection; this is especially important for pooled PostgreSQL
			// sessions where session_replication_role is connection-scoped.
			_ = s.restoreNormalPragmas(ctx)
		}
		if s.maintenanceCancel != nil {
			s.maintenanceCancel()
			select {
			case <-s.maintenanceDone:
			case <-ctx.Done():
				closeErrors := []error{ctx.Err()}
				if s.readDB != s.writeDB {
					closeErrors = append(closeErrors, s.readDB.Close())
				}
				closeErrors = append(closeErrors, s.writeDB.Close())
				s.closeErr = errors.Join(closeErrors...)
				return
			}
		}
		var closeErrors []error
		if s.readDB != s.writeDB {
			closeErrors = append(closeErrors, s.readDB.Close())
		}
		closeErrors = append(closeErrors, s.writeDB.Close())
		s.closeErr = errors.Join(closeErrors...)
	})
	return s.closeErr
}

func (s *Store) startMaintenance() {
	if s.maintenance == nil || s.maintenanceEvery <= 0 {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.maintenanceCancel = cancel
	s.maintenanceDone = make(chan struct{})
	go func() {
		defer close(s.maintenanceDone)
		ticker := time.NewTicker(s.maintenanceEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				started := time.Now()
				if err := s.maintenance(ctx); err != nil {
					if ctx.Err() == nil {
						s.logger.Error(
							"metadata database maintenance failed",
							"dialect", s.dialect.Name(),
							"duration", time.Since(started),
							"error", err,
						)
					}
					continue
				}
				s.logger.Debug(
					"metadata database maintenance complete",
					"dialect", s.dialect.Name(),
					"duration", time.Since(started),
				)
			}
		}
	}()
}

func (s *Store) dbFromTxn(txn types.Txn) (queryer, error) {
	if txn == nil {
		if err := s.ensureReady(); err != nil {
			return nil, err
		}
		s.bulkMu.RLock()
		bulkConn := s.bulkConn
		s.bulkMu.RUnlock()
		if bulkConn != nil {
			return newDialectQueryer(bulkConn, s.dialect.Name()), nil
		}
		return newDialectQueryer(s.writeDB, s.dialect.Name()), nil
	}
	sqlTransaction, ok := txn.(*sqlTxn)
	if !ok || sqlTransaction.owner != s {
		return nil, errors.New("sqlstore: transaction belongs to another store")
	}
	sqlTransaction.mu.Lock()
	defer sqlTransaction.mu.Unlock()
	if sqlTransaction.beginErr != nil {
		return nil, sqlTransaction.beginErr
	}
	if sqlTransaction.finished || sqlTransaction.tx == nil {
		return nil, types.ErrNilTxn
	}
	return newDialectQueryer(sqlTransaction.tx, s.dialect.Name()), nil
}

func (s *Store) readDBFromTxn(txn types.Txn) (queryer, error) {
	if txn == nil {
		if err := s.ensureReady(); err != nil {
			return nil, err
		}
		return newDialectQueryer(s.readDB, s.dialect.Name()), nil
	}
	return s.dbFromTxn(txn)
}

func (s *Store) withWriteTransaction(
	ctx context.Context,
	txn types.Txn,
	fn func(queryer) error,
) error {
	if txn != nil {
		db, err := s.dbFromTxn(txn)
		if err != nil {
			return err
		}
		return fn(db)
	}
	sqlTransaction, err := s.beginWriteTx(ctx)
	if err != nil {
		return err
	}
	if err := fn(newDialectQueryer(sqlTransaction, s.dialect.Name())); err != nil {
		return errors.Join(err, sqlTransaction.Rollback())
	}
	return sqlTransaction.Commit()
}

func (s *Store) beginWriteTx(ctx context.Context) (*sql.Tx, error) {
	s.bulkMu.RLock()
	defer s.bulkMu.RUnlock()
	conn := s.bulkConn
	if conn != nil {
		return conn.BeginTx(ctx, s.dialect.BeginOptions(false))
	}
	return s.writeDB.BeginTx(ctx, s.dialect.BeginOptions(false))
}

type queryer interface {
	Execer
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqlTxn struct {
	owner    *Store
	tx       *sql.Tx
	beginErr error

	mu       sync.Mutex
	finished bool
}

func (t *sqlTxn) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished {
		return nil
	}
	t.finished = true
	if t.tx == nil {
		return nil
	}
	return t.tx.Commit()
}

func (t *sqlTxn) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished {
		return nil
	}
	t.finished = true
	if t.tx == nil {
		return nil
	}
	return t.tx.Rollback()
}

func (t *sqlTxn) SavePoint(name string) error {
	return t.execSavepoint("SAVEPOINT", name)
}

func (t *sqlTxn) RollbackTo(name string) error {
	return t.execSavepoint("ROLLBACK TO SAVEPOINT", name)
}

func (t *sqlTxn) execSavepoint(operation, name string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.beginErr != nil {
		return t.beginErr
	}
	if t.finished || t.tx == nil {
		return types.ErrNilTxn
	}
	if !savepointNamePattern.MatchString(name) {
		return fmt.Errorf("sqlstore: invalid savepoint name %q", name)
	}
	// operation is selected by private callers and name is both allow-listed
	// above and quoted by the dialect.
	statement := operation + " " + //nolint:gosec
		t.owner.dialect.QuoteIdentifier(name)
	if _, err := t.tx.ExecContext(context.Background(), statement); err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

// SetBulkLoadPragmas enables backend-specific session tuning.
func (s *Store) SetBulkLoadPragmas() error {
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()
	if s.bulkConn != nil {
		return nil
	}
	conn, err := s.writeDB.Conn(context.Background())
	if err != nil {
		return err
	}
	if err := s.dialect.SetBulkMode(context.Background(), conn); err != nil {
		_ = conn.Close()
		return err
	}
	s.bulkConn = conn
	return nil
}

// RestoreNormalPragmas restores safe backend defaults.
func (s *Store) RestoreNormalPragmas() error {
	return s.restoreNormalPragmas(context.Background())
}

func (s *Store) restoreNormalPragmas(ctx context.Context) error {
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()
	if s.bulkConn == nil {
		return s.dialect.RestoreNormalMode(ctx, s.writeDB)
	}
	conn := s.bulkConn
	restoreErr := s.dialect.RestoreNormalMode(ctx, conn)
	closeErr := conn.Close()
	s.bulkConn = nil
	return errors.Join(restoreErr, closeErr)
}

// UpdatePlannerStats refreshes backend planner statistics.
func (s *Store) UpdatePlannerStats() error {
	return s.dialect.UpdatePlannerStats(context.Background(), s.writeDB)
}
