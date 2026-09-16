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

package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	driversqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var sharedMemoryDBSequence atomic.Uint64

// sqliteCommonPragmas is the DSN fragment applied to both the write and read
// pools. busy_timeout leads defensively -- modernc.org/sqlite always hoists
// it ahead of the rest of the _pragma list regardless of DSN order, but
// listing it first here keeps the DSN self-documenting and matches the
// order the driver actually applies it in. Anything the driver ran before
// busy_timeout took effect would have no busy handler installed and would
// fail immediately on contention.
//
// journal_mode is deliberately absent: see ensureWALJournalMode. Everything
// else here is per-connection state that has to be set on each one.
//
// wal_autocheckpoint(10000) raises the automatic-checkpoint threshold from
// SQLite's compiled-in default of 1000 pages (~4MB at the default 4096-byte
// page size) to 10000 pages (~40MB). Every top-level write transaction
// (batches of up to 50 blocks; see ledger.batchSize) that pushes the WAL past
// the threshold triggers a passive checkpoint that copies each dirty page
// back into metadata.sqlite. Chain-state writes revisit a small set of hot
// B-tree pages (UTXO/index pages near the tip, sequence counters) far more
// often than they touch new ones, so a checkpoint firing on nearly every
// commit rewrites those same pages to the main file over and over -- one
// physical write per commit instead of one write per accumulated batch of
// commits. Measured live against a from-genesis preview sync at the old
// default: ~112MB/s sustained process write throughput (wchar) against a
// combined ~2.9MB/s of net new data (SQLite file growth + Badger blob
// growth), with the SQLite side alone accounting for the bulk of it while
// the WAL file itself held a constant size -- i.e. checkpoint-driven
// rewrites, not new data. Raising the threshold 10x lets up to 10x as many
// commits' worth of hot-page churn coalesce into a single checkpoint pass,
// since a checkpoint only ever needs to write the latest version of a given
// page once. This value already runs in production via the bulk-load path
// (see SQLiteDialect's setBulk, and its restore counterpart below, both of
// which now agree on 10000) for genesis import and snapshot restore, so it
// is not a new, unvalidated setting -- only its use outside of bulk mode is
// new.
//
// Durability: per DATABASE.md's Cross-Store Durability Contract, synchronous
// NORMAL under WAL mode means SQLite fsyncs the WAL at checkpoint boundaries,
// not after every commit -- a committed transaction survives an application
// crash (the bytes are already in the OS page cache) but an OS crash/power
// loss can still roll back whatever was written since the last checkpoint's
// fsync. That was already true at the old 1000-page threshold; this change
// only widens the rolled-back window from ~4MB to ~40MB of recent commits.
// It does not introduce a new failure class: SQLite's own WAL replay
// guarantees the database is never corrupted, only reverted to an earlier
// consistent point, and Dingo already resumes chain-sync from whatever tip
// the metadata store reports after any restart (ledger.LedgerState.loadTip),
// re-fetching and re-applying any blocks peers show as missing via the
// ordinary FindIntersect/chain-sync path -- the same mechanism that already
// recovers from an explicit rollback or from the pre-existing Badger-behind-
// metadata race the Cross-Store Durability Contract section above documents.
const sqliteCommonPragmas = "&_pragma=busy_timeout(30000)" +
	"&_pragma=synchronous(NORMAL)" +
	"&_pragma=wal_autocheckpoint(10000)" +
	"&_pragma=cache_size(-50000)" +
	"&_pragma=foreign_keys(1)" +
	"&_pragma=mmap_size(268435456)"

// walConversionTimeout bounds how long a node waits for another opener to
// finish converting a freshly created database to WAL. It matches the
// busy_timeout the pragmas set, so contention is given the same budget
// however it is waited on.
const walConversionTimeout = 30 * time.Second

// ensureWALJournalMode puts the database into WAL mode once, before either
// pool is opened.
//
// It is a separate step rather than a _pragma on every connection because
// SQLite takes the rollback-to-WAL transition's exclusive lock without
// consulting the busy handler: "PRAGMA journal_mode=WAL" against a database
// another connection is opening fails immediately with SQLITE_BUSY no matter
// how large busy_timeout is. Running it on every connection therefore turned
// a routine startup race into a failed open, and the caller saw it as
// "ping write database: database is locked".
//
// Doing it once is sufficient because journal mode is persistent -- it lives
// in the database header, not the connection -- so every later connection
// inherits WAL without asking for it. Because the busy handler does not
// apply, waiting has to be explicit, hence the retry.
func ensureWALJournalMode(ctx context.Context, databaseURI string) error {
	db, err := sqlstore.OpenDB(
		"sqlite",
		databaseURI+"?_pragma=busy_timeout(30000)",
		"sqlite",
		false, // one-shot startup helper; not worth tracing
	)
	if err != nil {
		return fmt.Errorf("open SQLite database for WAL conversion: %w", err)
	}
	defer func() {
		_ = db.Close()
	}()
	db.SetMaxOpenConns(1)

	ctx, cancel := context.WithTimeout(ctx, walConversionTimeout)
	defer cancel()

	backoff := 2 * time.Millisecond
	for {
		var mode string
		err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode)
		if err == nil {
			if !strings.EqualFold(mode, "wal") {
				return fmt.Errorf(
					"SQLite journal mode is %q after requesting WAL",
					mode,
				)
			}
			return nil
		}
		if !isSQLiteBusy(err) {
			return fmt.Errorf("set SQLite journal mode to WAL: %w", err)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"set SQLite journal mode to WAL: %w (last error: %w)",
				ctx.Err(),
				err,
			)
		case <-time.After(backoff):
		}
		if backoff < 250*time.Millisecond {
			backoff *= 2
		}
	}
}

// isSQLiteBusy reports whether err is the driver's lock-contention error.
// The extended result code carries the reason in its high bits, so compare
// only the primary code.
func isSQLiteBusy(err error) bool {
	var sqliteErr *driversqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	switch sqliteErr.Code() & 0xff {
	case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		return true
	default:
		return false
	}
}

// NewSQLStore opens the shared database/sql SQLite implementation.
func NewSQLStore(
	config Config,
	dependencies metadata.ProviderDependencies,
) (*sqlstore.Store, error) {
	store, _, _, err := openSQLStore(config, dependencies)
	return store, err
}

func openSQLStore(
	config Config,
	dependencies metadata.ProviderDependencies,
) (*sqlstore.Store, *sql.DB, *sql.DB, error) {
	if config.MaxConnections < 0 {
		return nil, nil, nil, errors.New(
			"SQLite maxConnections must not be negative",
		)
	}
	dataDir := dependencies.DataDir
	if config.DataDir != "" {
		dataDir = config.DataDir
	}
	maxConnections := dependencies.MaxConnections
	if config.MaxConnections > 0 {
		maxConnections = config.MaxConnections
	}
	if maxConnections <= 0 {
		maxConnections = DefaultMaxConnections
	}
	registry, err := migrations.SQLiteRegistry()
	if err != nil {
		return nil, nil, nil, err
	}

	var (
		writeDB      *sql.DB
		readDB       *sql.DB
		locker       migrations.Locker
		prepare      func(context.Context) error
		diskSizeFunc func() (int64, error)
		maintenance  func(context.Context) error
		backupTo     func(context.Context, string) error
		restoreFrom  func(context.Context, string) error
		databasePath string
	)
	if dataDir == "" {
		dsn := fmt.Sprintf(
			"file:dingo_sqlstore_%d?mode=memory&cache=shared"+
				"&_pragma=busy_timeout(30000)&_pragma=foreign_keys(1)",
			sharedMemoryDBSequence.Add(1),
		)
		writeDB, err = sqlstore.OpenDB(
			"sqlite", dsn, "sqlite", dependencies.TracingEnabled,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		readDB = writeDB
		writeDB.SetMaxOpenConns(maxConnections)
		writeDB.SetMaxIdleConns(maxConnections)
		locker = migrations.NewProcessLocker()
	} else {
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, nil, nil, fmt.Errorf(
				"create SQLite metadata directory: %w",
				err,
			)
		}
		databasePath = filepath.Join(dataDir, "metadata.sqlite")
		// Build a proper file URI so ?, #, %, and other URI-reserved
		// characters in the configured data directory remain part of the
		// filename instead of being interpreted as DSN options/fragments.
		databaseURI := sqliteFileURI(databasePath)
		writeDB, err = sqlstore.OpenDB(
			"sqlite",
			fmt.Sprintf(
				"%s?_txlock=immediate%s",
				databaseURI,
				sqliteCommonPragmas,
			),
			"sqlite",
			dependencies.TracingEnabled,
		)
		if err != nil {
			return nil, nil, nil, err
		}
		readDB, err = sqlstore.OpenDB(
			"sqlite",
			fmt.Sprintf(
				"%s?mode=ro%s",
				databaseURI,
				sqliteCommonPragmas,
			),
			"sqlite",
			dependencies.TracingEnabled,
		)
		if err != nil {
			_ = writeDB.Close()
			return nil, nil, nil, err
		}
		writeDB.SetMaxOpenConns(1)
		writeDB.SetMaxIdleConns(1)
		readDB.SetMaxOpenConns(maxConnections)
		readDB.SetMaxIdleConns(maxConnections)
		prepare = func(ctx context.Context) error {
			return ensureWALJournalMode(ctx, databaseURI)
		}
		locker = migrations.NewFileLocker(databasePath + ".migrate.lock")
		diskSizeFunc = sqliteDiskSize(readDB, databasePath)
		maintenance = func(ctx context.Context) error {
			_, err := writeDB.ExecContext(ctx, "VACUUM")
			return err
		}
		backupTo = func(ctx context.Context, dstPath string) error {
			return backupSQLite(ctx, databasePath, dataDir, dstPath)
		}
		restoreFrom = func(ctx context.Context, srcPath string) error {
			return restoreSQLite(ctx, dataDir, srcPath)
		}
	}

	store, err := sqlstore.New(sqlstore.Config{
		WriteDB:             writeDB,
		ReadDB:              readDB,
		Dialect:             sqlstore.SQLiteDialect(),
		Logger:              dependencies.Logger,
		StorageMode:         dependencies.StorageMode,
		Migrations:          registry,
		MigrationLocker:     locker,
		Prepare:             prepare,
		DiskSize:            diskSizeFunc,
		Maintenance:         maintenance,
		MaintenanceInterval: 24 * time.Hour,
		BackupTo:            backupTo,
		RestoreFrom:         restoreFrom,
		PromRegistry:        dependencies.PromRegistry,
	})
	if err != nil {
		if readDB != writeDB {
			_ = readDB.Close()
		}
		_ = writeDB.Close()
		return nil, nil, nil, err
	}
	if dataDir != "" && dependencies.PromRegistry != nil {
		registerSQLiteFileMetrics(
			dependencies.PromRegistry,
			databasePath,
			store,
		)
	}
	return store, writeDB, readDB, nil
}

// sqliteDiskSizeQueryTimeout bounds the page_count/page_size PRAGMA reads
// below, so a Prometheus scrape of dingo_database_sql_disk_bytes cannot
// stall indefinitely behind a slow or wedged connection.
const sqliteDiskSizeQueryTimeout = 5 * time.Second

func sqliteDiskSize(
	db *sql.DB,
	databasePath string,
) func() (int64, error) {
	return func() (int64, error) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			sqliteDiskSizeQueryTimeout,
		)
		defer cancel()
		var pageCount, pageSize int64
		if err := db.QueryRowContext(ctx, "PRAGMA page_count").
			Scan(&pageCount); err != nil {
			return 0, fmt.Errorf("SQLite page count: %w", err)
		}
		if err := db.QueryRowContext(ctx, "PRAGMA page_size").
			Scan(&pageSize); err != nil {
			return 0, fmt.Errorf("SQLite page size: %w", err)
		}
		total := pageCount * pageSize
		for _, path := range []string{
			databasePath,
			databasePath + "-wal",
			databasePath + "-shm",
		} {
			info, err := os.Stat(path)
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return 0, fmt.Errorf("stat SQLite file %s: %w", path, err)
			}
			if path == databasePath {
				if info.Size() > total {
					total = info.Size()
				}
			} else {
				total += info.Size()
			}
		}
		return total, nil
	}
}

// sqliteFileURI converts an OS path to the file URI form expected by SQLite.
// Windows volume paths need an extra leading slash (file:///C:/...), while
// URL.String handles escaping reserved characters in either form.
func sqliteFileURI(databasePath string) string {
	if !filepath.IsAbs(databasePath) {
		if absolutePath, err := filepath.Abs(databasePath); err == nil {
			databasePath = absolutePath
		}
	}
	path := filepath.ToSlash(databasePath)
	if filepath.VolumeName(databasePath) != "" &&
		!strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return (&url.URL{Scheme: "file", Path: path}).String()
}
