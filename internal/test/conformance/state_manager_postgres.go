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
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
)

// postgresConformanceSchema is the dedicated schema this suite migrates
// into, isolating it from database/plugin/metadata/postgres's own tests,
// which connect to the same dingo_test database's default "public" schema
// (see README.md's "Schema isolation" section).
const postgresConformanceSchema = "conformance"

// postgresConformanceBlobDir is a stable (not per-call-random) local
// directory for the Badger blob store paired with postgresConformanceSchema.
// It must stay stable across separate NewDingoPostgresStateManager calls
// -- even across separate test binary runs -- rather than a fresh
// os.MkdirTemp each time: database.New's commit-timestamp consistency
// check (database/commit_timestamp.go) requires the blob and metadata
// stores in one Database to have last committed the same timestamp, and
// every write transaction stamps both together, so a *persistent* remote
// schema must be paired with an equally persistent local blob directory,
// not a new empty one each time -- otherwise every construction after the
// first real commit fails that check.
//
// Unlike the schema (truncated in place between vectors -- see
// wipeMetadata's doc comment), this directory is never cleared: stale
// blob entries from an earlier, now-truncated vector are keyed by that
// vector's own transaction hashes, which the metadata rows a later read
// would need to resolve into no longer exist, so they're simply never
// looked up again -- harmless, not read, and not worth the extra I/O of
// clearing on every Reset.
var postgresConformanceBlobDir = filepath.Join(
	os.TempDir(), "dingo-conformance-postgres-blob",
)

// NewDingoPostgresStateManager creates a DingoStateManager backed by a real
// PostgreSQL metadata store at dsn (plus a local Badger blob store),
// composed through the same plugin.Resolve path the production node uses
// at startup.
//
// An unreachable host or invalid DSN is a real construction error here,
// not a swallowed no-op: this is what makes the "invalid DSN must fail"
// acceptance tests in conformance_postgres_test.go meaningful.
func NewDingoPostgresStateManager(dsn string) (*DingoStateManager, error) {
	if err := os.MkdirAll(postgresConformanceBlobDir, 0o700); err != nil {
		return nil, fmt.Errorf(
			"create postgres conformance blob data dir: %w",
			err,
		)
	}
	return newDingoPostgresStateManagerAt(dsn, postgresConformanceBlobDir)
}

// newDingoPostgresStateManagerAt creates a Postgres-backed DingoStateManager
// using an explicit, caller-owned local blob data directory. Used directly
// by the restart test (TestNewDingoPostgresStateManagerRestartSurvivesReopen
// in conformance_postgres_test.go), which must reuse the same blob
// directory across two manager instances pointed at the same schema: the
// local Badger blob store and the remote Postgres metadata store are
// paired at construction (database.New's commit-timestamp check rejects a
// mismatched pairing), so reopening against a *fresh* blob directory while
// reusing the same already-populated metadata store would fail that check
// -- exactly the scenario this constructor exists to avoid in the test.
// NewDingoStateManager uses it with a manager-owned temp directory.
//
// It first ensures postgresConformanceSchema exists (a plain CREATE SCHEMA
// IF NOT EXISTS over an ordinary connection, since the postgres metadata
// plugin itself has no schema-provisioning step the way the mysql plugin
// does for CREATE DATABASE), then resolves the metadata store against dsn
// with its connection search_path pinned to that schema (see
// storagetest.PostgresDSNWithSearchPath), so every table the store's
// migrations create lands there instead of colliding with the plugin's
// own tests.
func newDingoPostgresStateManagerAt(
	dsn, blobDataDir string,
) (*DingoStateManager, error) {
	return newDingoPostgresStateManagerAtSchema(
		dsn,
		blobDataDir,
		postgresConformanceSchema,
	)
}

// newDingoPostgresStateManagerAtSchema is newDingoPostgresStateManagerAt with
// an explicit schema name, so a caller that needs full isolation from every
// other test sharing postgresConformanceSchema -- notably the restart test,
// which pairs a fresh, test-owned blobDataDir with the metadata side and
// must not perturb the commit-timestamp state the *shared* schema/stable
// blob dir pairing (postgresConformanceBlobDir, used by every other test
// via newDingoPostgresStateManagerAt/NewDingoPostgresStateManager) depends
// on -- can use a schema nothing else touches instead.
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
	m.wipeMetadata = func() error {
		return truncatePostgresConformanceSchema(dsn, schema)
	}
	return m, nil
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

// truncatePostgresConformanceSchema empties every base table in schema, in
// place, over an ordinary connection to dsn (not the live store's own
// connection pool -- Reset calls this without going through
// DingoStateManager.db at all, so it works whether or not the store
// considers itself mid-transaction). Used as DingoStateManager's
// wipeMetadata hook (see Reset in state_manager.go).
//
// This truncates rather than drops-and-recreates: unlike
// metadata.Resettable.Reset (database/plugin/metadata/postgres's own Reset
// callback, which drops tables outright, requiring a fresh migration run
// before the store is usable again), TRUNCATE keeps every table (and
// index/constraint) in place, so the already-open store's connection pool
// keeps working immediately afterward -- no close, no reopen, no
// re-migration. At one Reset per vector across the ~300-vector suite,
// avoiding a real close/reopen/re-migrate round trip per vector is what
// keeps the Postgres backend's wall-clock cost in the same ballpark as
// SQLite's rather than a full order of magnitude slower.
//
// The table list is discovered from information_schema rather than
// hardcoded, so it stays correct as migrations add tables. Like
// recreatePostgresConformanceSchema before it, this only ever touches
// schema (postgresConformanceSchema): unlike metadata.Resettable.Reset,
// which scans and drops tables across *every* non-system schema in the
// database -- appropriate for its actual use (preparing a target for
// RestoreFrom), not safe to call on every vector reset since it would also
// destroy database/plugin/metadata/postgres's own concurrently running
// tests' tables in the shared dingo_test database.
//
// schema_migrations itself is deliberately excluded: it is the migration
// runner's own bookkeeping table (database/plugin/metadata/sqlstore/migrations/runner.go),
// not conformance data. Truncating it would desync tracked migration state
// from the physical schema without reverting any DDL -- a later
// construction against this same, already-migrated schema would see an
// empty schema_migrations table, decide every migration (including ones
// whose columns/tables already physically exist from the first
// construction) still needs to run, and fail with a duplicate
// column/table error partway through re-applying already-applied DDL.
func truncatePostgresConformanceSchema(dsn, schema string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("open postgres admin connection: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(
		`SELECT table_name FROM information_schema.tables
WHERE table_schema = $1 AND table_type = 'BASE TABLE' AND table_name <> 'schema_migrations'`,
		schema,
	)
	if err != nil {
		return fmt.Errorf("list postgres schema %q tables: %w", schema, err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan postgres table name: %w", err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list postgres schema %q tables: %w", schema, err)
	}
	if len(tables) == 0 {
		// Nothing migrated yet (e.g. Reset called before any construction
		// ever ran migrations against this schema) -- nothing to truncate.
		return nil
	}

	quoted := make([]string, len(tables))
	for i, table := range tables {
		quoted[i] = pgQuoteQualified(schema, table)
	}
	if _, err := db.Exec(
		"TRUNCATE TABLE " + strings.Join(quoted, ", ") + " CASCADE",
	); err != nil {
		return fmt.Errorf("truncate postgres schema %q: %w", schema, err)
	}
	return nil
}

// pgQuoteQualified double-quotes schema and table independently, so an
// identifier requiring escaping in either part is handled correctly
// (`"schema"."table"`, not a single quoted "schema.table").
func pgQuoteQualified(schema, table string) string {
	quote := func(ident string) string {
		return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
	}
	return quote(schema) + "." + quote(table)
}
