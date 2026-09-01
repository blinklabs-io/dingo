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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// mysqlProcessDatabase and mysqlProcessBlobDir namespace this test binary
// process's database and local blob directory: unique across every
// process/run (a nanosecond timestamp plus the OS process ID can't collide
// between two `go test` invocations), but shared by every
// NewDingoMysqlStateManager call *within* this one process, rather than
// generating a brand-new database and blob directory per call.
//
// An earlier version of this constructor used a single fixed database name
// and stable os.TempDir() path shared across every call, every process,
// and every machine running this suite against the same server. That
// sharing was unsafe on two fronts a reviewer caught: concurrent `go test`
// invocations (a local run alongside CI, or two CI shards) truncated or
// dropped each other's in-progress backend, since Reset and teardown for
// one process's manager operated on state another process's manager was
// actively using; and recreating the disposable compose database (docker
// compose down -v / up) reset the remote database's commit timestamp to
// empty while the stable local blob directory kept whatever timestamp an
// earlier run had already committed, so the very next construction failed
// database.New's commit-timestamp consistency check
// (database/commit_timestamp.go) against a pairing neither side actually
// caused.
//
// Generating a brand-new database on every single call within one process
// (an earlier fix attempt) traded that bug for a much worse one: every
// call re-pays the real cost of migrating a fresh database from scratch
// (see the migration runner's CREATE INDEX statements, genuinely slow
// against a real server), and one test binary run calls
// NewDingoMysqlStateManager from several different test functions. Sharing
// one database for the whole process keeps that cost paid exactly once
// per run while still being unique *across* runs, which is what actually
// fixes the concurrency/staleness problem. Neither is torn down by an
// individual manager's Close -- a sibling manager elsewhere in this same
// process may still be using them -- TestMain
// (conformance_main_test.go) drops the database and removes the blob
// directory once, after every test in this process has finished.
var (
	mysqlProcessDatabase = fmt.Sprintf(
		"dingo_conformance_%d_%d", os.Getpid(), time.Now().UnixNano(),
	)
	mysqlProcessBlobDirOnce sync.Once
	mysqlProcessBlobDir     string
	mysqlProcessBlobDirErr  error
)

// ensureMysqlProcessBlobDir creates mysqlProcessBlobDir on first call and
// returns the same directory on every later call in this process.
func ensureMysqlProcessBlobDir() (string, error) {
	mysqlProcessBlobDirOnce.Do(func() {
		mysqlProcessBlobDir, mysqlProcessBlobDirErr = os.MkdirTemp(
			"", "dingo-conformance-mysql-blob-*",
		)
	})
	return mysqlProcessBlobDir, mysqlProcessBlobDirErr
}

// NewDingoMysqlStateManager creates a DingoStateManager backed by a real
// MySQL metadata store (plus a local Badger blob store), composed through
// the same plugin.Resolve path the production node uses at startup.
// rootDSN must authenticate as an account with CREATE DATABASE privileges
// (see mysqlConformanceRootDSN in conformance_mysql_test.go): the mysql
// metadata plugin's own openStore provisions the generated database name
// automatically (CREATE DATABASE IF NOT EXISTS, via its
// ensureDatabaseExists step) whenever the DSN it's given names a database,
// which the DSN built here does. See mysqlProcessDatabase's doc comment
// for why the database and blob directory are process-unique rather than
// either globally fixed or freshly generated on every call.
//
// An unreachable host or invalid credentials is a real construction error
// here, not a swallowed no-op: this is what makes the "invalid DSN must
// fail" acceptance tests in conformance_mysql_test.go meaningful.
func NewDingoMysqlStateManager(rootDSN string) (*DingoStateManager, error) {
	blobDataDir, err := ensureMysqlProcessBlobDir()
	if err != nil {
		return nil, fmt.Errorf(
			"create mysql conformance blob data dir: %w",
			err,
		)
	}
	return newDingoMysqlStateManagerAtDatabase(
		rootDSN,
		blobDataDir,
		mysqlProcessDatabase,
	)
}

// newDingoMysqlStateManagerAtDatabase creates a MySQL-backed
// DingoStateManager using an explicit database and local blob data
// directory, for a caller that must manage that database's lifecycle
// itself. The restart test (TestNewDingoMysqlStateManagerRestartSurvivesReopen
// in conformance_mysql_test.go) is the one caller: it opens a second manager
// against the same database and blob directory after closing the first, to
// prove state survives that round trip. Neither Close call drops the
// database -- DingoStateManager.Close never does, matching
// mysqlProcessDatabase's own process-wide sharing (see its doc comment) --
// the test cleans its own database up explicitly instead. The mysql
// metadata plugin's own openStore provisions the named database
// automatically (CREATE DATABASE IF NOT EXISTS), so no separate ensure step
// is needed here the way postgres needs ensurePostgresConformanceSchema.
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
	resetter, err := newMysqlResetter(rootDSN, database)
	if err != nil {
		return nil, errors.Join(err, m.Close())
	}
	m.wipeMetadata = func() error {
		return resetter.reset(context.Background())
	}
	m.closeExtra = resetter.Close
	return m, nil
}

// newMysqlResetter opens the one admin connection this manager's Reset reuses
// for its whole lifetime, replacing a per-vector sql.Open/Close.
//
// DBName is cleared because MySQL rejects Ping and most statements against a
// DSN naming a database that does not exist yet, though it always does here
// once the store has been constructed once.
//
// MySQL has no multi-table TRUNCATE, so unlike PostgreSQL this backend still
// issues one statement per table -- but only for tables a vector actually
// wrote, rather than all 84 every time. See reset_cost.go for why TRUNCATE is
// kept over a single batched DELETE.
func newMysqlResetter(rootDSN, database string) (*backendResetter, error) {
	cfg, err := mysqldriver.ParseDSN(rootDSN)
	if err != nil {
		return nil, fmt.Errorf("parse mysql root DSN: %w", err)
	}
	cfg.DBName = ""
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open mysql admin connection: %w", err)
	}
	return &backendResetter{
		db: db,
		listTables: func(
			ctx context.Context,
			db *sql.DB,
		) ([]string, error) {
			return listMysqlConformanceTables(ctx, db, database)
		},
		qualify: func(table string) string {
			return mysqlQuoteIdentifier(database) + "." +
				mysqlQuoteIdentifier(table)
		},
		truncate: func(
			ctx context.Context,
			db *sql.DB,
			qualified []string,
		) error {
			return truncateMysqlTables(ctx, db, qualified)
		},
	}, nil
}

// truncateMysqlTables empties exactly the given tables with foreign key
// checks disabled.
//
// The disable/restore pair must run on the same session as the TRUNCATEs --
// FOREIGN_KEY_CHECKS is a session variable, and a pooled *sql.DB may hand each
// statement a different connection -- so this pins one conn for the duration.
func truncateMysqlTables(
	ctx context.Context,
	db *sql.DB,
	qualified []string,
) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire mysql admin connection: %w", err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(
		ctx, "SET FOREIGN_KEY_CHECKS=0",
	); err != nil {
		return fmt.Errorf("disable mysql foreign key checks: %w", err)
	}
	defer func() {
		_, _ = conn.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1")
	}()
	for _, table := range qualified {
		if _, err := conn.ExecContext(
			ctx, "TRUNCATE TABLE "+table,
		); err != nil {
			return fmt.Errorf("truncate mysql table %s: %w", table, err)
		}
	}
	return nil
}

// listMysqlConformanceTables returns database's base tables, excluding
// schema_migrations -- see listPostgresConformanceTables for why that table is
// never truncated.
func listMysqlConformanceTables(
	ctx context.Context,
	db *sql.DB,
	database string,
) ([]string, error) {
	rows, err := db.QueryContext(
		ctx,
		"SELECT table_name FROM information_schema.tables "+
			"WHERE table_schema = ? AND table_type = 'BASE TABLE' "+
			"AND table_name <> 'schema_migrations'",
		database,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list mysql database %q tables: %w",
			database,
			err,
		)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan mysql table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"list mysql database %q tables: %w",
			database,
			err,
		)
	}
	return tables, nil
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

// mysqlQuoteIdentifier backtick-quotes a MySQL identifier, doubling any
// embedded backtick.
func mysqlQuoteIdentifier(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}
