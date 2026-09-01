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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
)

// postgresProcessSchema and postgresProcessBlobDir namespace this test
// binary process's schema and local blob directory: unique across every
// process/run (a nanosecond timestamp plus the OS process ID can't collide
// between two `go test` invocations), but shared by every
// NewDingoPostgresStateManager call *within* this one process, rather than
// generating a brand-new schema and blob directory per call.
//
// An earlier version of this constructor used a single fixed "conformance"
// schema and stable os.TempDir() path shared across every call, every
// process, and every machine running this suite against the same server.
// That sharing was unsafe on two fronts a reviewer caught: concurrent
// `go test` invocations (a local run alongside CI, or two CI shards)
// truncated or dropped each other's in-progress backend, since Reset and
// teardown for one process's manager operated on state another process's
// manager was actively using; and recreating the disposable compose
// database (docker compose down -v / up) reset the remote schema's commit
// timestamp to empty while the stable local blob directory kept whatever
// timestamp an earlier run had already committed, so the very next
// construction failed database.New's commit-timestamp consistency check
// (database/commit_timestamp.go) against a pairing neither side actually
// caused.
//
// Generating a brand-new schema on every single call within one process
// (an earlier fix attempt) traded that bug for a much worse one: every
// call re-pays the real cost of migrating a fresh schema from scratch
// (see the migration runner's CREATE INDEX statements, genuinely slow
// against a real server), and one test binary run calls
// NewDingoPostgresStateManager from several different test functions.
// Sharing one schema for the whole process keeps that cost paid exactly
// once per run while still being unique *across* runs, which is what
// actually fixes the concurrency/staleness problem. Neither is torn down
// by an individual manager's Close -- a sibling manager elsewhere in this
// same process may still be using them -- TestMain
// (conformance_main_test.go) drops the schema and removes the blob
// directory once, after every test in this process has finished.
var (
	postgresProcessSchema = fmt.Sprintf(
		"conformance_%d_%d", os.Getpid(), time.Now().UnixNano(),
	)
	postgresProcessBlobDirOnce sync.Once
	postgresProcessBlobDir     string
	postgresProcessBlobDirErr  error
)

// ensurePostgresProcessBlobDir creates postgresProcessBlobDir on first call
// and returns the same directory on every later call in this process.
func ensurePostgresProcessBlobDir() (string, error) {
	postgresProcessBlobDirOnce.Do(func() {
		postgresProcessBlobDir, postgresProcessBlobDirErr = os.MkdirTemp(
			"", "dingo-conformance-postgres-blob-*",
		)
	})
	return postgresProcessBlobDir, postgresProcessBlobDirErr
}

// NewDingoPostgresStateManager creates a DingoStateManager backed by a real
// PostgreSQL metadata store at dsn (plus a local Badger blob store),
// composed through the same plugin.Resolve path the production node uses
// at startup. See postgresProcessSchema's doc comment for why the schema
// and blob directory are process-unique rather than either globally fixed
// or freshly generated on every call.
//
// An unreachable host or invalid DSN is a real construction error here,
// not a swallowed no-op: this is what makes the "invalid DSN must fail"
// acceptance tests in conformance_postgres_test.go meaningful.
func NewDingoPostgresStateManager(dsn string) (*DingoStateManager, error) {
	blobDataDir, err := ensurePostgresProcessBlobDir()
	if err != nil {
		return nil, fmt.Errorf(
			"create postgres conformance blob data dir: %w",
			err,
		)
	}
	return newDingoPostgresStateManagerAtSchema(
		dsn,
		blobDataDir,
		postgresProcessSchema,
	)
}

// newDingoPostgresStateManagerAtSchema creates a Postgres-backed
// DingoStateManager using an explicit schema and local blob data directory,
// for a caller that must manage that schema's lifecycle itself. The restart
// test (TestNewDingoPostgresStateManagerRestartSurvivesReopen in
// conformance_postgres_test.go) is the one caller: it opens a second
// manager against the same schema and blob directory after closing the
// first, to prove state survives that round trip. Neither Close call drops
// the schema -- DingoStateManager.Close never does, matching
// postgresProcessSchema's own process-wide sharing (see its doc comment) --
// the test cleans its own schema up explicitly instead.
//
// It first ensures schema exists (a plain CREATE SCHEMA IF NOT EXISTS over
// an ordinary connection, since the postgres metadata plugin itself has no
// schema-provisioning step the way the mysql plugin does for CREATE
// DATABASE), then resolves the metadata store against dsn with its
// connection search_path pinned to that schema (see
// storagetest.PostgresDSNWithSearchPath), so every table the store's
// migrations create lands there instead of colliding with the plugin's
// own tests.
func newDingoPostgresStateManagerAtSchema(
	dsn, blobDataDir, schema string,
) (*DingoStateManager, error) {
	if err := ensurePostgresConformanceSchema(dsn, schema); err != nil {
		return nil, fmt.Errorf(
			"ensure postgres conformance schema: %w",
			err,
		)
	}

	scopedDSN := storagetest.PostgresDSNWithSearchPath(dsn, schema)
	m, err := newDingoStateManager(realBackendOptions{
		dataDir:      blobDataDir,
		metadataName: "postgres",
		metadataConfig: map[string]any{
			"dsn": scopedDSN,
		},
		registerMetadata: postgres.RegisterProvider,
	})
	if err != nil {
		return nil, err
	}
	resetter, err := newPostgresResetter(dsn, schema)
	if err != nil {
		return nil, errors.Join(err, m.Close())
	}
	m.wipeMetadata = func() error {
		return resetter.reset(context.Background())
	}
	m.closeExtra = resetter.Close
	return m, nil
}

// newPostgresResetter opens the one admin connection this manager's Reset
// reuses for its whole lifetime, replacing a per-vector sql.Open/Close. See
// reset_cost.go for why the per-vector connection, table-list query, and
// unconditional truncate were each worth removing.
func newPostgresResetter(dsn, schema string) (*backendResetter, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres admin connection: %w", err)
	}
	return &backendResetter{
		db: db,
		listTables: func(
			ctx context.Context,
			db *sql.DB,
		) ([]string, error) {
			return listPostgresConformanceTables(ctx, db, schema)
		},
		qualify: func(table string) string {
			return pgQuoteQualified(schema, table)
		},
		truncate: func(
			ctx context.Context,
			db *sql.DB,
			qualified []string,
		) error {
			// PostgreSQL takes every table in one statement, so this is a
			// single implicit-commit DDL round trip regardless of how many
			// tables the vector dirtied.
			if _, err := db.ExecContext(
				ctx,
				"TRUNCATE TABLE "+strings.Join(qualified, ", ")+" CASCADE",
			); err != nil {
				return fmt.Errorf(
					"truncate postgres schema %q: %w",
					schema,
					err,
				)
			}
			return nil
		},
	}, nil
}

// listPostgresConformanceTables returns schema's base tables, excluding
// schema_migrations.
//
// schema_migrations is the migration runner's own bookkeeping table
// (database/plugin/metadata/sqlstore/migrations/runner.go), not conformance
// data. Truncating it would desync tracked migration state from the physical
// schema without reverting any DDL: a later construction against this
// already-migrated schema would see an empty schema_migrations, decide every
// migration still needed to run, and fail with a duplicate column/table error
// partway through re-applying already-applied DDL.
func listPostgresConformanceTables(
	ctx context.Context,
	db *sql.DB,
	schema string,
) ([]string, error) {
	rows, err := db.QueryContext(
		ctx,
		`SELECT table_name FROM information_schema.tables
WHERE table_schema = $1 AND table_type = 'BASE TABLE' AND table_name <> 'schema_migrations'`,
		schema,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list postgres schema %q tables: %w",
			schema,
			err,
		)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan postgres table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"list postgres schema %q tables: %w",
			schema,
			err,
		)
	}
	return tables, nil
}

// ensurePostgresConformanceSchema creates schema (if it doesn't already
// exist) over an ordinary, unscoped connection to dsn. It must run before
// the metadata store resolves against the schema-scoped DSN: a search_path
// naming a schema that doesn't exist yet still connects, but every
// unqualified migration statement the store then issues would fail to
// find (or silently target the wrong) schema.
func ensurePostgresConformanceSchema(dsn, schema string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open postgres admin connection: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("ping postgres admin connection: %w", err)
	}
	// schema is a package-controlled constant, never operator/DSN input, so
	// building this statement by concatenation (rather than a bind
	// parameter, which CREATE SCHEMA's grammar doesn't accept for an
	// identifier) carries no injection risk.
	if _, err := db.Exec(
		"CREATE SCHEMA IF NOT EXISTS " + schema,
	); err != nil {
		return fmt.Errorf("create postgres schema %q: %w", schema, err)
	}
	return nil
}

// pgQuoteIdent double-quotes a single PostgreSQL identifier, doubling any
// embedded quote.
func pgQuoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

// pgQuoteQualified double-quotes schema and table independently, so an
// identifier requiring escaping in either part is handled correctly
// (`"schema"."table"`, not a single quoted "schema.table").
func pgQuoteQualified(schema, table string) string {
	return pgQuoteIdent(schema) + "." + pgQuoteIdent(table)
}
