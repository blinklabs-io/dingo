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
	// CommitteeAuthRetentionSlots overrides how far back superseded
	// auth_committee_hot rows are retained for rollback, in slots. Zero
	// selects DefaultCommitteeAuthRetentionSlots; see committee_prune.go for
	// the retention rule and why the window has to cover the rollback bound.
	CommitteeAuthRetentionSlots uint64

	Migrations      []migrations.Migration
	MigrationLocker migrations.Locker
	DiskSize        func() (int64, error)
	// Maintenance is optional backend maintenance started only after the
	// migration readiness gate succeeds. SQLite uses it for periodic VACUUM.
	Maintenance         func(context.Context) error
	MaintenanceInterval time.Duration
	// BackupTo and RestoreFrom are optional provider-owned lifecycle hooks.
	// SQLite supplies them for its file-backed store; other dialects may leave
	// them unset until a native snapshot mechanism is available.
	BackupTo    func(context.Context, string) error
	RestoreFrom func(context.Context, string) error
	// Prepare is an optional provider-owned hook run once at the start of
	// Start, before anything touches the pools. It is where a provider does
	// setup that has to happen on a connection of its own and must not
	// happen at construction time: SQLite uses it to put a new database into
	// WAL mode, which materialises the file, and constructing a store is not
	// allowed to do that -- RestoreFrom runs against a constructed but
	// unstarted store and requires the destination not to exist.
	Prepare func(context.Context) error
	// Reset is an optional provider-owned hook clearing all data this store
	// owns, using the still-open pool (it must run before the store is
	// closed). See metadata.Resettable's doc comment for why this exists:
	// a live client/server backend's restore orchestration needs a way to
	// undo a brief resolve-and-start's real migrations against the actual
	// remote database, which a directory wipe (sqlite/badger's mechanism)
	// cannot touch. Left unset, Reset is a harmless no-op.
	Reset func(context.Context) error
	// ValidateBackup is an optional provider-owned hook checking a backup
	// file's structural integrity without touching any database -- see
	// metadata.BackupValidator's doc comment for why this exists
	// specifically for Resettable providers: their restore orchestration
	// resets a live remote target before RestoreFrom ever parses the
	// backup, so an invalid backup needs to be caught before that reset,
	// not after it. Left unset, ValidateBackup is a harmless no-op.
	ValidateBackup func(context.Context, string) error
}

// Store owns the shared database/sql pools. Provider packages own DSN and
// driver selection; metadata behavior belongs here.
type Store struct {
	writeDB     *sql.DB
	readDB      *sql.DB
	dialect     Dialect
	logger      *slog.Logger
	storageMode string

	// committeeAuthRetentionSlots is the configured rollback window for
	// auth_committee_hot pruning. Read it through committeeAuthRetention(),
	// which applies the default, rather than directly.
	committeeAuthRetentionSlots uint64

	migrations        []migrations.Migration
	migrationLocker   migrations.Locker
	diskSize          func() (int64, error)
	maintenance       func(context.Context) error
	maintenanceEvery  time.Duration
	backupTo          func(context.Context, string) error
	restoreFrom       func(context.Context, string) error
	prepare           func(context.Context) error
	reset             func(context.Context) error
	validateBackup    func(context.Context, string) error
	maintenanceCancel context.CancelFunc
	maintenanceDone   chan struct{}
	maintenanceState  atomic.Uint32
	ready             atomic.Bool
	closed            atomic.Bool
	startMu           sync.Mutex
	bulkMu            sync.RWMutex
	bulkConnMu        sync.Mutex
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
	if config.CommitteeAuthRetentionSlots != 0 &&
		config.CommitteeAuthRetentionSlots < DefaultCommitteeAuthRetentionSlots {
		return nil, fmt.Errorf(
			"sqlstore: committee auth retention slots %d is below the safe rollback window %d",
			config.CommitteeAuthRetentionSlots,
			DefaultCommitteeAuthRetentionSlots,
		)
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
		writeDB:                     config.WriteDB,
		readDB:                      config.ReadDB,
		dialect:                     config.Dialect,
		logger:                      config.Logger,
		storageMode:                 config.StorageMode,
		committeeAuthRetentionSlots: config.CommitteeAuthRetentionSlots,
		migrations:                  config.Migrations,
		migrationLocker:             config.MigrationLocker,
		diskSize:                    config.DiskSize,
		maintenance:                 config.Maintenance,
		maintenanceEvery:            config.MaintenanceInterval,
		backupTo:                    config.BackupTo,
		restoreFrom:                 config.RestoreFrom,
		prepare:                     config.Prepare,
		reset:                       config.Reset,
		validateBackup:              config.ValidateBackup,
	}, nil
}

func (s *Store) BackupTo(ctx context.Context, dstPath string) error {
	if s.backupTo == nil {
		return errors.New("metadata backup is not supported by this provider")
	}
	if s.closed.Load() {
		return errors.New("metadata backup: store is closed")
	}
	if !s.ready.Load() {
		return errors.New("metadata backup: store is not ready")
	}
	return s.backupTo(ctx, dstPath)
}

func (s *Store) RestoreFrom(ctx context.Context, srcPath string) error {
	if s.restoreFrom == nil {
		return errors.New("metadata restore is not supported by this provider")
	}
	return s.restoreFrom(ctx, srcPath)
}

// Reset clears all data this store owns, for providers that supply the
// hook (see metadata.Resettable). A no-op for providers that don't --
// unlike BackupTo/RestoreFrom, silently doing nothing here is correct,
// not a lost user request: sqlite (the only file-based provider built on
// this shared Store) has nothing for this to do, since restoreMetadataStore's
// directory wipe already fully undoes its brief resolve-and-start. Every
// backend built on this shared Store -- sqlite included -- therefore
// satisfies metadata.Resettable's interface, but only postgres/mysql wire
// a non-nil Config.Reset into it; sqlite's Reset is a documented no-op, not
// evidence that it "needs more than a directory wipe" the way
// metadata.Resettable's own doc comment describes for the backends that do.
func (s *Store) Reset(ctx context.Context) error {
	if s.reset == nil {
		return nil
	}
	// Serialized with Start/CloseContext via the same startMu they already
	// hold: without it, a concurrent CloseContext could close the pool
	// while s.reset(ctx) is mid-flight (or land in the TOCTOU window right
	// after the closed check below), leaving a live database partially
	// reset with its connection pool pulled out from under it.
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.closed.Load() {
		return errors.New("metadata reset: store is closed")
	}
	return s.reset(ctx)
}

// HasDestructiveReset reports whether Reset actually mutates a live target
// (postgres/mysql, which wire a real Config.Reset callback) rather than
// being a harmless no-op (sqlite, which never sets one). Every backend's
// concrete *Store satisfies metadata.Resettable's Reset(ctx) error method
// regardless, so a plain type assertion against that interface alone
// cannot distinguish "genuinely destructive" from "no-op" -- callers that
// need to know whether Reset already happening (or having failed partway)
// means there is no safe pre-restore state left to resume on (see
// node_lifecycle.go's Restore) must check this instead.
func (s *Store) HasDestructiveReset() bool {
	return s.reset != nil
}

// ValidateBackup checks a backup file's structural integrity, for
// providers that supply the hook (see metadata.BackupValidator). A no-op
// for providers that don't -- every backend built on this shared Store
// therefore satisfies metadata.BackupValidator's interface, but only
// providers whose restore orchestration needs it wire a non-nil
// Config.ValidateBackup in.
func (s *Store) ValidateBackup(ctx context.Context, srcPath string) error {
	if s.validateBackup == nil {
		return nil
	}
	return s.validateBackup(ctx, srcPath)
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
	if s.prepare != nil {
		if err := s.prepare(ctx); err != nil {
			return fmt.Errorf("sqlstore: prepare database: %w", err)
		}
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
			Rebind:   s.dialect.Rebind,
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

// Transaction begins a write transaction bound to ctx: every statement a
// caller issues against the returned Txn (via a domain method's txn
// parameter) runs with this ctx, and per database/sql's own BeginTx
// contract, canceling it rolls the transaction back rather than leaving it
// to time out on its own. Begin failures are retained on the returned
// transaction because the historical MetadataStore contract cannot return
// an error from this method. A nil ctx is treated as context.Background(),
// matching prior behavior for any caller that does not supply one.
func (s *Store) Transaction(ctx context.Context) types.Txn {
	return s.transaction(ctx, false)
}

// ReadTransaction begins a repeatable, read-only transaction on the read
// pool, bound to ctx the same way Transaction is.
func (s *Store) ReadTransaction(ctx context.Context) types.Txn {
	return s.transaction(ctx, true)
}

// transaction begins a transaction bound to ctx. The context.Background()
// fallback below is for a caller passing a literal nil, not a dropped
// caller ctx -- there is nothing above to derive from in that case.
func (s *Store) transaction(ctx context.Context, readOnly bool) types.Txn {
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.ready.Load() {
		return &sqlTxn{
			owner:    s,
			ctx:      ctx,
			beginErr: errors.New("sqlstore: store is not ready"),
		}
	}
	var (
		tx      *sql.Tx
		release func()
		err     error
	)
	if readOnly {
		tx, err = s.readDB.BeginTx(ctx, s.dialect.BeginOptions(true))
	} else {
		tx, release, err = s.beginWriteTx(ctx)
	}
	return &sqlTxn{owner: s, tx: tx, ctx: ctx, release: release, beginErr: err}
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
		s.closeMaintenanceAdmission()
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
				s.closeErr = errors.Join(ctx.Err(), s.closePools())
				return
			}
		}
		s.closeErr = s.closePools()
	})
	return s.closeErr
}

func (s *Store) closePools() error {
	if s.readDB == s.writeDB {
		return s.writeDB.Close()
	}
	return errors.Join(s.readDB.Close(), s.writeDB.Close())
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
				// Admission is an atomic state transition. Close moves the
				// state to closed before cancelling the callback context, so a
				// racing tick cannot start maintenance against closing pools.
				if !s.maintenanceState.CompareAndSwap(0, 1) {
					return
				}
				if ctx.Err() != nil || s.closed.Load() {
					s.maintenanceState.CompareAndSwap(1, 0)
					return
				}
				started := time.Now()
				err := s.maintenance(ctx)
				s.maintenanceState.CompareAndSwap(1, 0)
				if err != nil {
					if ctx.Err() == nil {
						s.logger.Error(
							"metadata database maintenance failed",
							"dialect", s.dialect.Name(),
							"duration", time.Since(started),
							"error", err,
						)
					} else {
						return
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

func (s *Store) closeMaintenanceAdmission() {
	for {
		state := s.maintenanceState.Load()
		if state == 2 || s.maintenanceState.CompareAndSwap(state, 2) {
			return
		}
	}
}

// dbFromTxn resolves txn to a queryer plus the context.Context statements
// against it should use. txn == nil is the autocommit convenience: no
// caller-managed ctx exists for that path, so it returns
// context.Background() -- accepted for a one-off statement against the
// shared pool, bounded server-side by the statement/lock timeout config
// instead of by caller cancellation. A real *sqlTxn instead carries the
// ctx its owning Transaction/ReadTransaction call was given, so every
// statement issued against it -- through whichever domain method's txn
// parameter it arrived through -- is bound to that same caller-supplied
// ctx without that method needing a ctx parameter of its own.
func (s *Store) dbFromTxn(
	txn types.Txn,
) (queryer, context.Context, error) {
	if txn == nil {
		if err := s.ensureReady(); err != nil {
			return nil, nil, err
		}
		return newDialectQueryer(
			s.writeDB,
			s.dialect.Name(),
		), context.Background(), nil
	}
	sqlTransaction, ok := txn.(*sqlTxn)
	if !ok || sqlTransaction.owner != s {
		return nil, nil, errors.New(
			"sqlstore: transaction belongs to another store",
		)
	}
	sqlTransaction.mu.Lock()
	defer sqlTransaction.mu.Unlock()
	if sqlTransaction.beginErr != nil {
		return nil, nil, sqlTransaction.beginErr
	}
	if sqlTransaction.finished || sqlTransaction.tx == nil {
		return nil, nil, types.ErrNilTxn
	}
	return newDialectQueryer(
		sqlTransaction.tx,
		s.dialect.Name(),
	), sqlTransaction.ctx, nil
}

func (s *Store) readDBFromTxn(
	txn types.Txn,
) (queryer, context.Context, error) {
	if txn == nil {
		if err := s.ensureReady(); err != nil {
			return nil, nil, err
		}
		return newDialectQueryer(
			s.readDB,
			s.dialect.Name(),
		), context.Background(), nil
	}
	return s.dbFromTxn(txn)
}

// withWriteTransaction runs fn against either the caller-supplied txn (its
// own ctx passed through, per dbFromTxn) or, when txn is nil, a fresh
// implicit write transaction this call begins and commits/rolls back
// itself -- that implicit case has no caller-managed ctx to inherit
// either, so it uses context.Background(), the same accepted autocommit-
// path gap dbFromTxn documents.
func (s *Store) withWriteTransaction(
	txn types.Txn,
	fn func(queryer, context.Context) error,
) error {
	if err := s.ensureReady(); err != nil {
		return err
	}
	if txn != nil {
		db, ctx, err := s.dbFromTxn(txn)
		if err != nil {
			return err
		}
		return fn(db, ctx)
	}
	ctx := context.Background()
	sqlTransaction, release, err := s.beginWriteTx(ctx)
	if err != nil {
		return err
	}
	sqlTxnState := &sqlTxn{
		owner:   s,
		tx:      sqlTransaction,
		ctx:     ctx,
		release: release,
	}
	fnErr := fn(newDialectQueryer(sqlTransaction, s.dialect.Name()), ctx)
	if fnErr != nil {
		return errors.Join(fnErr, sqlTxnState.Rollback())
	}
	return sqlTxnState.Commit()
}

func (s *Store) beginWriteTx(ctx context.Context) (*sql.Tx, func(), error) {
	s.bulkMu.RLock()
	conn := s.bulkConn
	if conn != nil {
		s.bulkConnMu.Lock()
		tx, err := conn.BeginTx(ctx, s.dialect.BeginOptions(false))
		if err != nil {
			s.bulkConnMu.Unlock()
			s.bulkMu.RUnlock()
			return nil, nil, err
		}
		return tx, func() {
			s.bulkConnMu.Unlock()
			s.bulkMu.RUnlock()
		}, nil
	}
	tx, err := s.writeDB.BeginTx(ctx, s.dialect.BeginOptions(false))
	s.bulkMu.RUnlock()
	return tx, nil, err
}

type queryer interface {
	Execer
	PrepareContext(context.Context, string) (*sql.Stmt, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type sqlTxn struct {
	owner *Store
	tx    *sql.Tx
	// ctx is the context.Context this transaction was begun with (via
	// Transaction/ReadTransaction). dbFromTxn hands it back alongside the
	// queryer so every statement issued against this txn -- through
	// whichever domain method's txn parameter -- is bound to it.
	ctx      context.Context
	release  func()
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
	defer t.releaseConnection()
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
	defer t.releaseConnection()
	if t.tx == nil {
		return nil
	}
	return t.tx.Rollback()
}

func (t *sqlTxn) releaseConnection() {
	if t.release != nil {
		t.release()
		t.release = nil
	}
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
	s.startMu.Lock()
	defer s.startMu.Unlock()
	if s.closed.Load() {
		return errors.New("sqlstore: store is closed")
	}
	s.bulkMu.Lock()
	defer s.bulkMu.Unlock()
	if s.bulkConn != nil {
		return nil
	}
	if s.dialect.Name() == "sqlite" {
		return s.dialect.SetBulkMode(context.Background(), s.writeDB)
	}
	conn, err := s.writeDB.Conn(context.Background())
	if err != nil {
		return err
	}
	if err := s.dialect.SetBulkMode(context.Background(), conn); err != nil {
		// A backend may apply some session settings before a later setup
		// statement fails. Restore before releasing the connection so a
		// pooled session cannot leak partial bulk-load state.
		restoreErr := s.dialect.RestoreNormalMode(context.Background(), conn)
		closeErr := conn.Close()
		return errors.Join(err, restoreErr, closeErr)
	}
	s.bulkConn = conn
	return nil
}

// RestoreNormalPragmas restores safe backend defaults.
func (s *Store) RestoreNormalPragmas() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
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
	s.bulkMu.RLock()
	defer s.bulkMu.RUnlock()
	if s.bulkConn == nil {
		return s.dialect.UpdatePlannerStats(context.Background(), s.writeDB)
	}
	s.bulkConnMu.Lock()
	defer s.bulkConnMu.Unlock()
	return s.dialect.UpdatePlannerStats(context.Background(), s.bulkConn)
}
