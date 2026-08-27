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

//go:build dingo_extra_plugins

package conformance

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// mysqlConformanceDatabase is the dedicated database this suite migrates
// into. MySQL has no schema/database distinction the way Postgres does --
// a MySQL "schema" is a database -- so isolation from
// database/plugin/metadata/mysql's own tests (which use the shared
// dingo_test database) means using an entirely separate database, not
// just a different namespace within one (see README.md's "Database
// isolation" section).
const mysqlConformanceDatabase = "dingo_conformance_test"

// mysqlConformanceBlobDir is a stable (not per-call-random) local directory
// for the Badger blob store paired with mysqlConformanceDatabase. It must
// stay stable across separate NewDingoMysqlStateManager calls -- even
// across separate test binary runs -- rather than a fresh os.MkdirTemp
// each time: database.New's commit-timestamp consistency check
// (database/commit_timestamp.go) requires the blob and metadata stores in
// one Database to have last committed the same timestamp, and every write
// transaction stamps both together, so a *persistent* remote database
// must be paired with an equally persistent local blob directory, not a
// new empty one each time -- otherwise every construction after the first
// real commit fails that check.
//
// Unlike the database (truncated in place between vectors -- see
// wipeMetadata's doc comment), this directory is never cleared: stale
// blob entries from an earlier, now-truncated vector are keyed by that
// vector's own transaction hashes, which the metadata rows a later read
// would need to resolve into no longer exist, so they're simply never
// looked up again -- harmless, not read, and not worth the extra I/O of
// clearing on every Reset.
var mysqlConformanceBlobDir = filepath.Join(
	os.TempDir(), "dingo-conformance-mysql-blob",
)

// NewDingoMysqlStateManager creates a DingoStateManager backed by a real
// MySQL metadata store (plus a local Badger blob store), composed through
// the same plugin.Resolve path the production node uses at startup.
// rootDSN must authenticate as an account with CREATE DATABASE privileges
// (see mysqlConformanceRootDSN in conformance_mysql_test.go): the mysql
// metadata plugin's own openStore provisions mysqlConformanceDatabase
// automatically (CREATE DATABASE IF NOT EXISTS, via its
// ensureDatabaseExists step) whenever the DSN it's given names a database,
// which the DSN built here does.
//
// An unreachable host or invalid credentials is a real construction error
// here, not a swallowed no-op: this is what makes the "invalid DSN must
// fail" acceptance tests in conformance_mysql_test.go meaningful.
func NewDingoMysqlStateManager(rootDSN string) (*DingoStateManager, error) {
	if err := os.MkdirAll(mysqlConformanceBlobDir, 0o700); err != nil {
		return nil, fmt.Errorf(
			"create mysql conformance blob data dir: %w",
			err,
		)
	}
	return newDingoMysqlStateManagerAt(rootDSN, mysqlConformanceBlobDir)
}

// newDingoMysqlStateManagerAt creates a MySQL-backed DingoStateManager
// using an explicit, caller-owned local blob data directory. Used directly
// by the restart test (TestNewDingoMysqlStateManagerRestartSurvivesReopen
// in conformance_mysql_test.go), which must reuse the same blob directory
// across two manager instances pointed at the same database: the local
// Badger blob store and the remote MySQL metadata store are paired at
// construction (database.New's commit-timestamp check rejects a mismatched
// pairing), so reopening against a *fresh* blob directory while reusing the
// same already-populated metadata store would fail that check -- exactly
// the scenario this constructor exists to avoid in the test.
// NewDingoMysqlStateManager uses it with a manager-owned temp directory.
func newDingoMysqlStateManagerAt(
	rootDSN, blobDataDir string,
) (*DingoStateManager, error) {
	return newDingoMysqlStateManagerAtDatabase(
		rootDSN,
		blobDataDir,
		mysqlConformanceDatabase,
	)
}

// newDingoMysqlStateManagerAtDatabase is newDingoMysqlStateManagerAt with an
// explicit database name, so a caller that needs full isolation from every
// other test sharing mysqlConformanceDatabase -- notably the restart test,
// which pairs a fresh, test-owned blobDataDir with the metadata side and
// must not perturb the commit-timestamp state the *shared* database/stable
// blob dir pairing (mysqlConformanceBlobDir, used by every other test via
// newDingoMysqlStateManagerAt/NewDingoMysqlStateManager) depends on -- can
// use a database nothing else touches instead. The mysql metadata plugin's
// own openStore provisions the named database automatically (CREATE
// DATABASE IF NOT EXISTS), so no separate ensure step is needed here the
// way postgres needs ensurePostgresConformanceSchema.
func newDingoMysqlStateManagerAtDatabase(
	rootDSN, blobDataDir, database string,
) (*DingoStateManager, error) {
	scopedDSN, err := mysqlDSNWithDatabase(rootDSN, database)
	if err != nil {
		return nil, fmt.Errorf(
			"build mysql conformance database DSN: %w",
			err,
		)
	}

	m, err := newDingoStateManager(realBackendOptions{
		dataDir:      blobDataDir,
		metadataName: "mysql",
		metadataConfig: map[string]any{
			"dsn": scopedDSN,
		},
		registerMetadata: mysql.RegisterProvider,
	})
	if err != nil {
		return nil, err
	}
	m.wipeMetadata = func() error {
		return truncateMysqlConformanceDatabase(rootDSN, database)
	}
	return m, nil
}

// mysqlDSNWithDatabase returns dsn with its DBName set to database,
// preserving every other connection parameter (host, port, credentials,
// TLS, timeouts) already encoded in dsn.
func mysqlDSNWithDatabase(dsn, database string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("parse mysql DSN: %w", err)
	}
	cfg.DBName = database
	return cfg.FormatDSN(), nil
}

// truncateMysqlConformanceDatabase empties every base table in database, in
// place, over an admin connection built from rootDSN (with DBName cleared,
// since MySQL rejects Ping/most statements against a DSN naming a database
// that doesn't exist yet -- though it always does here once the store has
// been constructed once). Used as DingoStateManager's wipeMetadata hook
// (see Reset in state_manager.go).
//
// This truncates rather than drops the database: unlike
// metadata.Resettable.Reset (database/plugin/metadata/mysql's own Reset
// callback, which drops tables individually, requiring a fresh migration
// run -- and, for the database-per-suite provisioning this constructor
// relies on, a fresh CREATE DATABASE -- before the store is usable again),
// TRUNCATE keeps every table in place, so the already-open store's
// connection pool keeps working immediately afterward -- no close, no
// reopen, no re-migration. At one Reset per vector across the ~300-vector
// suite, avoiding a real close/reopen/re-migrate round trip per vector is
// what keeps the MySQL backend's wall-clock cost in the same ballpark as
// SQLite's rather than a full order of magnitude slower.
//
// The table list is discovered from information_schema rather than
// hardcoded, so it stays correct as migrations add tables.
//
// schema_migrations itself is deliberately excluded: it is the migration
// runner's own bookkeeping table (database/plugin/metadata/sqlstore/migrations/runner.go),
// not conformance data. Truncating it would desync tracked migration state
// from the physical schema without reverting any DDL -- a later
// construction against this same, already-migrated database would see an
// empty schema_migrations table, decide every migration (including ones
// whose columns/tables already physically exist from the first
// construction) still needs to run, and fail with a duplicate
// column/table error partway through re-applying already-applied DDL.
func truncateMysqlConformanceDatabase(rootDSN, database string) error {
	cfg, err := mysqldriver.ParseDSN(rootDSN)
	if err != nil {
		return fmt.Errorf("parse mysql root DSN: %w", err)
	}
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return fmt.Errorf("open mysql admin connection: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(
		"SELECT table_name FROM information_schema.tables "+
			"WHERE table_schema = ? AND table_type = 'BASE TABLE' "+
			"AND table_name <> 'schema_migrations'",
		database,
	)
	if err != nil {
		return fmt.Errorf("list mysql database %q tables: %w", database, err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan mysql table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list mysql database %q tables: %w", database, err)
	}
	if len(tables) == 0 {
		// Nothing migrated yet (e.g. Reset called before any construction
		// ever ran migrations against this database) -- nothing to
		// truncate.
		return nil
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return fmt.Errorf("acquire mysql admin connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(
		context.Background(), "SET FOREIGN_KEY_CHECKS=0",
	); err != nil {
		return fmt.Errorf("disable mysql foreign key checks: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(
			context.Background(), "SET FOREIGN_KEY_CHECKS=1",
		)
	}()
	quotedDB := mysqlQuoteIdentifier(database)
	for _, table := range tables {
		quoted := quotedDB + "." + mysqlQuoteIdentifier(table)
		if _, err := conn.ExecContext(
			context.Background(), "TRUNCATE TABLE "+quoted,
		); err != nil {
			return fmt.Errorf(
				"truncate mysql table %s: %w",
				quoted,
				err,
			)
		}
	}
	return nil
}

// mysqlQuoteIdentifier backtick-quotes a MySQL identifier, doubling any
// embedded backtick.
func mysqlQuoteIdentifier(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
