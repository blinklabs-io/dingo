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
	"log/slog"
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

const maxVacuumIntervalSeconds uint64 = uint64(
	(1<<63 - 1) / int64(time.Second),
)

func sqliteVacuum(
	writeDB *sql.DB,
	intervalSeconds uint64,
) (func(context.Context) error, time.Duration) {
	if intervalSeconds == 0 {
		return nil, 0
	}
	return func(ctx context.Context) error {
		_, err := writeDB.ExecContext(ctx, "VACUUM")
		return err
	}, time.Duration(intervalSeconds) * time.Second
}

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

// checkpointInterval is how often checkpointWAL forces a WAL checkpoint,
// independent of the commit-triggered wal_autocheckpoint(10000) pragma above.
//
// wal_autocheckpoint only ever runs a PASSIVE checkpoint (that is what
// SQLite's auto-checkpoint mechanism invokes internally), and PASSIVE -- like
// FULL and RESTART -- backfills WAL frames into metadata.sqlite and lets
// future writes reuse the reclaimed space, but never calls ftruncate on the
// -wal file itself: only SQLITE_CHECKPOINT_TRUNCATE does that. Verified
// directly against a copy of a live, actively-growing metadata.sqlite: with
// zero readers blocking it (every attempt reported busy=0 with
// checkpointed==log, i.e. a fully successful checkpoint), PASSIVE, FULL, and
// RESTART each left a 68062432-byte -wal file at exactly 68062432 bytes,
// while TRUNCATE alone dropped it to 0. So dingo_database_sql_wal_bytes (a
// plain os.Stat of that file -- see metrics.go) can never show a single
// decrease under wal_autocheckpoint alone, no matter how well passive
// checkpointing is working underneath -- the file's on-disk footprint is a
// high-water mark that only grows or holds steady until something truncates
// it. checkpointWAL is that something: a periodic, independent TRUNCATE
// checkpoint attempt so the WAL's on-disk size can be brought back down on a
// schedule instead of only ever growing to whatever the largest
// inter-checkpoint write burst has been so far. This is best-effort, not an
// unconditional ceiling: a reader holding an old snapshot can make a given
// attempt busy (see checkpointBusyTimeout below), in which case the file
// stays at its current size until a later tick succeeds.
const checkpointInterval = 2 * time.Minute

// checkpointBusyTimeout bounds how long a single checkpoint attempt waits for
// a reader's old snapshot to close before giving up for this tick.
//
// The attempt is deliberately never issued against writeDB. PRAGMA
// wal_checkpoint(TRUNCATE) invokes the driver's busy handler synchronously
// inside the call, and neither Go context cancellation nor a select loop
// around it can interrupt that wait once the call has entered the driver:
// verified directly, cancelling the context at 300ms still let a blocking
// wal_checkpoint(TRUNCATE) run for the full busy_timeout(30000) before
// returning. Issuing the checkpoint from writeDB -- which is capped at
// SetMaxOpenConns(1) -- would therefore hold the only connection every real
// write needs for up to 30 seconds whenever a readDB snapshot is open:
// measured, a checkpoint attempt against writeDB with one open readDB
// snapshot took 30.04s, blocked a concurrent writeDB insert for 29.99s of
// that, and still finished with busy=1 (no truncation). A dedicated
// connection with a short busy_timeout hits the same busy=1 outcome, but
// fast: measured 271ms to return. checkpointWAL below uses that dedicated
// connection instead, so a blocked checkpoint tick costs at most this bound,
// not up to 30 seconds, and never contends with writeDB at all.
const checkpointBusyTimeout = 250 * time.Millisecond

// checkpointWAL returns a Store.Checkpoint callback that attempts
// PRAGMA wal_checkpoint(TRUNCATE) on checkpointInterval's ticker (see
// openSQLStore), against a dedicated connection opened fresh for each
// attempt and closed immediately after -- never against writeDB or readDB.
// See checkpointBusyTimeout's doc comment for why: writeDB's sole connection
// has to stay free for real writes, and the whole point of this design is to
// give up quickly on a blocked checkpoint rather than occupy it. SQLite
// tracks WAL locks at the shared-memory/file level rather than per
// database/sql connection, so a separate connection to the same file still
// correctly observes (or reports busy against) a snapshot readDB holds open.
//
// TRUNCATE, not the safer-sounding RESTART, is deliberate: checkpointInterval's
// doc comment above shows RESTART does not shrink the file at all, so it
// cannot make dingo_database_sql_wal_bytes move. If the bounded wait above is
// exceeded, PRAGMA wal_checkpoint reports busy=1 with a partial checkpointed
// count rather than an error -- logged at Warn so a persistently blocked
// checkpoint (rather than a single slow tick) is visible to an operator, and
// left for the next tick to retry rather than retried in a loop here.
func checkpointWAL(
	databaseURI string,
	logger *slog.Logger,
) func(context.Context) error {
	return func(ctx context.Context) error {
		db, err := sqlstore.OpenDB(
			"sqlite",
			fmt.Sprintf(
				"%s?_pragma=busy_timeout(%d)",
				databaseURI,
				checkpointBusyTimeout.Milliseconds(),
			),
			"sqlite",
			false, // short-lived per-tick connection; not worth tracing
		)
		if err != nil {
			return fmt.Errorf("open WAL checkpoint connection: %w", err)
		}
		defer func() {
			_ = db.Close()
		}()
		db.SetMaxOpenConns(1)

		var busy, walLog, checkpointed int
		row := db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
		if err := row.Scan(&busy, &walLog, &checkpointed); err != nil {
			return fmt.Errorf("WAL checkpoint: %w", err)
		}
		if busy != 0 {
			logger.Warn(
				"WAL checkpoint(TRUNCATE) could not fully complete "+
					"(a reader is still holding an old snapshot); "+
					"will retry next tick",
				"wal_frames", walLog,
				"checkpointed_frames", checkpointed,
			)
		}
		return nil
	}
}

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
	if config.VacuumIntervalSeconds > maxVacuumIntervalSeconds {
		return nil, nil, nil, fmt.Errorf(
			"SQLite vacuumIntervalSeconds exceeds maximum %d",
			maxVacuumIntervalSeconds,
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
		writeDB        *sql.DB
		readDB         *sql.DB
		locker         migrations.Locker
		prepare        func(context.Context) error
		diskSizeFunc   func() (int64, error)
		maintenance    func(context.Context) error
		vacuum         func(context.Context) error
		vacuumInterval time.Duration
		checkpoint     func(context.Context) error
		backupTo       func(context.Context, string) error
		restoreFrom    func(context.Context, string) error
		databasePath   string
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
		diskSizeFunc = sqliteDiskSize(databaseURI, databasePath)
		vacuum, vacuumInterval = sqliteVacuum(
			writeDB,
			config.VacuumIntervalSeconds,
		)
		checkpointLogger := dependencies.Logger
		if checkpointLogger == nil {
			checkpointLogger = slog.Default()
		}
		checkpoint = checkpointWAL(databaseURI, checkpointLogger)
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
		Vacuum:              vacuum,
		VacuumInterval:      vacuumInterval,
		Checkpoint:          checkpoint,
		CheckpointInterval:  checkpointInterval,
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

// sqliteDiskSize returns a Store.DiskSize callback that opens a dedicated
// connection fresh for each call and closes it immediately after -- never
// against writeDB or readDB. This mirrors checkpointWAL's own dedicated
// connection above. An earlier version queried readDB directly: one gauge
// read left a connection idled back into that shared pool indefinitely
// (neither pool sets SetConnMaxIdleTime/SetConnMaxLifetime), confirmed by
// inspecting readDB's own sql.DB.Stats().OpenConnections after a single
// DiskSize() call (see TestDiskSizeDoesNotLeaveReadDBConnectionOpen). Against
// the live perf-branch containers this change was written to fix,
// checkpointWAL's PRAGMA wal_checkpoint(TRUNCATE) logged busy=1 on
// essentially every tick from shortly after startup onward, and an external,
// independently-opened `sqlite3 metadata.sqlite "PRAGMA
// wal_checkpoint(TRUNCATE)"` reproduced the identical busy=1 result --
// confirming a real, OS-level WAL condition rather than an artifact of this
// process's own bookkeeping. Moving this gauge onto its own dedicated,
// immediately-closed connection made the warnings stop and the observed
// on-disk -wal file shrink (~130MB down to ~55MB on the affected instance).
//
// That correlation is real; the exact mechanism it goes through is not
// pinned down. SQLite's own documentation for wal_checkpoint says
// RESTART/TRUNCATE block only on an active writer or a reader still on an
// old snapshot, not on a connection that is merely idle in a pool, and a
// direct reproduction against this exact DSN and driver bears that out:
// TestWALCheckpointTruncateIdleConnectionDoesNotBlock runs this function's
// old two-PRAGMA-read pattern against readDB, leaves the connection open in
// the pool afterward, and still observes a clean, non-busy TRUNCATE against
// real, substantial WAL content -- while an actual held, uncommitted read
// transaction (the same test's positive control) does reproduce busy=1. So
// "a merely-attached idle connection blocks TRUNCATE" is not the isolated
// cause; production readDB traffic is far higher-concurrency than that
// lab reproduction, and the true trigger there was not identified further.
// The dedicated-connection design stands on the production before/after
// observation and on matching checkpointWAL's already-established pattern,
// independent of a fully isolated microscopic explanation.
//
// Unlike checkpointWAL (which only ever runs PRAGMA wal_checkpoint, an
// operation with no meaningful "read-only" mode), this gauge's two PRAGMA
// reads are opened with mode=ro, matching readDB's own read-only DSN: it
// never has a reason to write, and read-only fails closed instead of
// silently taking a write-capable connection if that ever changes.
func sqliteDiskSize(
	databaseURI string,
	databasePath string,
) func() (int64, error) {
	return func() (int64, error) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			sqliteDiskSizeQueryTimeout,
		)
		defer cancel()
		db, err := sqlstore.OpenDB(
			"sqlite",
			fmt.Sprintf(
				"%s?mode=ro&_pragma=busy_timeout(%d)",
				databaseURI,
				sqliteDiskSizeQueryTimeout.Milliseconds(),
			),
			"sqlite",
			false, // short-lived per-call connection; not worth tracing
		)
		if err != nil {
			return 0, fmt.Errorf("open SQLite disk size connection: %w", err)
		}
		defer func() {
			_ = db.Close()
		}()
		db.SetMaxOpenConns(1)

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
