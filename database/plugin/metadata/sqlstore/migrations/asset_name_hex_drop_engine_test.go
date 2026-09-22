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

package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// Migration v19 is the registry's first non-additive version: DROP INDEX and
// DROP COLUMN are not self-idempotent the way every ADD COLUMN/CREATE TABLE/
// CREATE INDEX before them was, so replaying v19's expand phase after a crash
// between its DDL and its phase advance depends entirely on runner.go's
// per-dialect "already dropped" guards. TestRunnerReplaysEveryShippedVersion-
// FromExpand covers that replay on SQLite only. These tests cover the
// PostgreSQL and MySQL guards against the real servers, whose error text and
// catalog lookups are what those guards actually key on -- go-test.yml already
// runs the dingo_extra_plugins suite with both services up, so this is real
// CI coverage rather than a locally-only check.

func requirePostgresColumnAbsent(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.columns
WHERE table_schema = $1 AND table_name = 'asset' AND column_name = 'name_hex'`,
		schema).Scan(&count))
	require.Zero(t, count, "asset.name_hex must not survive migration v19")

	require.NoError(t, db.QueryRow(`
SELECT count(*) FROM pg_indexes
WHERE schemaname = $1 AND indexname = 'idx_asset_name_hex'`,
		schema).Scan(&count))
	require.Zero(t, count, "idx_asset_name_hex must not survive migration v19")
}

// TestAssetNameHexDropReplaysOnPostgres drives v19 to completion against a
// real PostgreSQL server, then forces its expand phase to replay.
//
// A decoy schema carrying an un-migrated asset.name_hex is created first. A
// Dingo metadata schema is not necessarily alone in its database -- the
// postgres provider's schema is operator-selectable through search_path, and
// this repository's own conformance and storage-migration suites pin one
// schema per run inside a shared database -- so a replay guard that looks a
// column up by table_name/column_name alone finds the decoy's still-present
// column, concludes the drop was not already applied, and turns a resumable
// crash into a node that will not start. The guard must resolve the relation
// the migration's own unqualified DDL resolved against.
func TestAssetNameHexDropReplaysOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := postgresEngineTestDSN(t)

	schema := "dingo_v19_replay"
	decoy := "dingo_v19_replay_decoy"

	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.Ping())

	for _, s := range []string{schema, decoy} {
		_, err = admin.Exec(`DROP SCHEMA IF EXISTS "` + s + `" CASCADE`)
		require.NoError(t, err)
		_, err = admin.Exec(`CREATE SCHEMA "` + s + `"`)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, s := range []string{schema, decoy} {
			_, _ = admin.Exec(`DROP SCHEMA IF EXISTS "` + s + `" CASCADE`)
		}
	})

	// The decoy keeps a name_hex column a name-only catalog lookup would find.
	_, err = admin.Exec(`CREATE TABLE "` + decoy + `"."asset" (
"name" BYTEA, "name_hex" BYTEA, "policy_id" BYTEA, "id" BIGSERIAL PRIMARY KEY)`)
	require.NoError(t, err)
	// The same decoy for the ADD COLUMN guard, whose lookup compares the
	// column's declared type. Its type differs from the real migration's, so a
	// name-only lookup can read this row instead and refuse a legitimate
	// replay -- unreliably, since the row order across schemas is unspecified,
	// which is why the scoped lookup is the fix rather than a retry.
	_, err = admin.Exec(`CREATE TABLE "` + decoy + `"."reward_snapshot" (
"id" BIGSERIAL PRIMARY KEY, "excluded_active_stake" TEXT)`)
	require.NoError(t, err)

	db, err := sql.Open(
		"pgx",
		storagetest.PostgresDSNWithSearchPath(dsn, schema),
	)
	require.NoError(t, err)
	defer db.Close()

	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "postgres",
		Registry: registry,
		Locker:   migrations.NewProcessLocker(),
	}
	require.NoError(t, runner.Run(context.Background()))
	requirePostgresColumnAbsent(t, db, schema)

	// Reproduce a crash between v19's DDL and its phase advance.
	_, err = db.Exec(`
UPDATE schema_migrations SET phase = 'expand', dirty = true, completed_at = NULL
WHERE version = $1`, 19)
	require.NoError(t, err)

	require.NoError(
		t,
		runner.Run(context.Background()),
		"v19 must replay its expand phase against PostgreSQL",
	)
	requirePostgresColumnAbsent(t, db, schema)

	var phase string
	var dirty bool
	require.NoError(t, db.QueryRow(`
SELECT phase, dirty FROM schema_migrations WHERE version = $1`, 19).
		Scan(&phase, &dirty))
	require.Equal(t, "complete", phase)
	require.False(t, dirty)

	// The ADD COLUMN guard shares the same catalog lookup, and every version
	// through v18 depends on it. v18 adds reward_snapshot.excluded_active_stake,
	// so replaying its expand phase against the decoy proves that guard resolves
	// the migrated schema's own relation too.
	_, err = db.Exec(`
UPDATE schema_migrations SET phase = 'expand', dirty = true, completed_at = NULL
WHERE version = $1`, 18)
	require.NoError(t, err)

	require.NoError(
		t,
		runner.Run(context.Background()),
		"v18 must replay its ADD COLUMN expand phase against PostgreSQL",
	)
	require.NoError(t, db.QueryRow(`
SELECT phase, dirty FROM schema_migrations WHERE version = $1`, 18).
		Scan(&phase, &dirty))
	require.Equal(t, "complete", phase)
	require.False(t, dirty)
}

// TestAssetNameHexDropReplaysOnMySQL is the MySQL half: it additionally
// exercises the translated "DROP INDEX <name> ON <table>" form, which unlike
// SQLite's and PostgreSQL's "DROP INDEX IF EXISTS" raises error 1091 on
// replay and so reaches the guard rather than being tolerated by the engine.
func TestAssetNameHexDropReplaysOnMySQL(t *testing.T) {
	t.Parallel()
	rootDSN := mysqlEngineTestRootDSN(t)

	database := "dingo_v19_replay"
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.Ping())

	_, err = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
	require.NoError(t, err)
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
	})

	cfg, err := mysqldriver.ParseDSN(rootDSN)
	require.NoError(t, err)
	cfg.DBName = database
	cfg.MultiStatements = false

	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	defer db.Close()

	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "mysql",
		Registry: registry,
		Locker:   migrations.NewProcessLocker(),
	}
	require.NoError(t, runner.Run(context.Background()))
	requireMySQLNameHexAbsent(t, db, database)

	_, err = db.Exec(`
UPDATE schema_migrations SET phase = 'expand', dirty = 1, completed_at = NULL
WHERE version = ?`, 19)
	require.NoError(t, err)

	require.NoError(
		t,
		runner.Run(context.Background()),
		"v19 must replay its expand phase against MySQL",
	)
	requireMySQLNameHexAbsent(t, db, database)
}

func requireMySQLNameHexAbsent(t *testing.T, db *sql.DB, database string) {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.columns
WHERE table_schema = ? AND table_name = 'asset' AND column_name = 'name_hex'`,
		database).Scan(&count))
	require.Zero(t, count, "asset.name_hex must not survive migration v19")

	require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.statistics
WHERE table_schema = ? AND table_name = 'asset'
  AND index_name = 'idx_asset_name_hex'`,
		database).Scan(&count))
	require.Zero(t, count, "idx_asset_name_hex must not survive migration v19")
}

// postgresEngineTestDSN mirrors the POSTGRES_* variables the other
// PostgreSQL-backed suites read, and skips when none is configured.
func postgresEngineTestDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	if os.Getenv("POSTGRES_PASSWORD") == "" {
		t.Skip(
			"Skipping postgres v19 replay test: postgres not configured " +
				"(set POSTGRES_PASSWORD or POSTGRES_DSN)",
		)
	}
	value := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=UTC",
		storagetest.EscapeLibpqValue(value("POSTGRES_HOST", "localhost")),
		storagetest.EscapeLibpqValue(value("POSTGRES_PORT", "5432")),
		storagetest.EscapeLibpqValue(value("POSTGRES_USER", "postgres")),
		storagetest.EscapeLibpqValue(os.Getenv("POSTGRES_PASSWORD")),
		storagetest.EscapeLibpqValue(value("POSTGRES_DATABASE", "dingo_test")),
		storagetest.EscapeLibpqValue(value("POSTGRES_SSLMODE", "disable")),
	)
}

// mysqlEngineTestRootDSN needs root: the test creates and drops its own
// database, which the unprivileged test user cannot do.
func mysqlEngineTestRootDSN(t *testing.T) string {
	t.Helper()
	if dsn := os.Getenv("MYSQL_DSN"); dsn != "" {
		return dsn
	}
	if os.Getenv("MYSQL_ROOT_PASSWORD") == "" {
		t.Skip(
			"Skipping mysql v19 replay test: mysql not configured " +
				"(set MYSQL_ROOT_PASSWORD or MYSQL_DSN)",
		)
	}
	host := "localhost"
	if v := os.Getenv("MYSQL_HOST"); v != "" {
		host = v
	}
	port := "3306"
	if v := os.Getenv("MYSQL_PORT"); v != "" {
		port = v
	}
	cfg := mysqldriver.Config{
		User:                 "root",
		Passwd:               os.Getenv("MYSQL_ROOT_PASSWORD"),
		Net:                  "tcp",
		Addr:                 strings.Join([]string{host, port}, ":"),
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	return cfg.FormatDSN()
}
