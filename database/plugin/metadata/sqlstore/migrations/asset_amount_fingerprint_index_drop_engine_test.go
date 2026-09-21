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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// Migration v20 (asset-amount-fingerprint-index-drop, dingo#4598) is the
// registry's second DROP INDEX migration, after v19's asset.name_hex drop.
// Unlike v19, it carries no DROP COLUMN, so SQLite's and PostgreSQL's native
// "DROP INDEX IF EXISTS" tolerate replay without reaching any guard in
// runner.go at all. Only MySQL's translated "DROP INDEX <name> ON <table>"
// form is not self-idempotent -- it raises error 1091 on replay -- so the
// MySQL test below is what actually exercises new guard behavior;
// isMySQLDropAlreadyAppliedOnConn/parseMySQLDropIndexStatement are already
// generic over the index name (dingo#4482 built them that way), so this
// proves that generality holds for a name they were never written against.
// The PostgreSQL test is included anyway as real-server coverage that the
// translated statements (backtick-to-quote rewrite) actually apply and
// replay cleanly, not just that the guard code would tolerate a failure.

func requirePostgresAssetIndexesAbsent(t *testing.T, db *sql.DB, schema string) {
	t.Helper()
	for _, name := range []string{"idx_asset_amount", "idx_asset_fingerprint"} {
		var count int
		require.NoError(t, db.QueryRow(`
SELECT count(*) FROM pg_indexes
WHERE schemaname = $1 AND indexname = $2`,
			schema, name).Scan(&count))
		require.Zero(t, count, "%s must not survive migration v20", name)
	}
	// The columns themselves must survive: only the indexes are dropped.
	for _, column := range []string{"amount", "fingerprint"} {
		var count int
		require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.columns
WHERE table_schema = $1 AND table_name = 'asset' AND column_name = $2`,
			schema, column).Scan(&count))
		require.Equal(
			t, 1, count,
			"asset.%s must survive migration v20", column,
		)
	}
}

// TestAssetAmountFingerprintIndexDropAppliesOnPostgres drives v20 to
// completion against a real PostgreSQL server, then forces its expand phase
// to replay, confirming "DROP INDEX IF EXISTS" tolerates replay natively.
func TestAssetAmountFingerprintIndexDropAppliesOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := postgresEngineTestDSN(t)

	schema := "dingo_v20_replay"
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.Ping())

	_, err = admin.Exec(`DROP SCHEMA IF EXISTS "` + schema + `" CASCADE`)
	require.NoError(t, err)
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA IF EXISTS "` + schema + `" CASCADE`)
	})

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
	requirePostgresAssetIndexesAbsent(t, db, schema)

	// Reproduce a crash between v20's DDL and its phase advance.
	_, err = db.Exec(`
UPDATE schema_migrations SET phase = 'expand', dirty = true, completed_at = NULL
WHERE version = $1`, 20)
	require.NoError(t, err)

	require.NoError(
		t,
		runner.Run(context.Background()),
		"v20 must replay its expand phase against PostgreSQL",
	)
	requirePostgresAssetIndexesAbsent(t, db, schema)

	var phase string
	var dirty bool
	require.NoError(t, db.QueryRow(`
SELECT phase, dirty FROM schema_migrations WHERE version = $1`, 20).
		Scan(&phase, &dirty))
	require.Equal(t, "complete", phase)
	require.False(t, dirty)
}

// TestAssetAmountFingerprintIndexDropReplaysOnMySQL is the MySQL half: it
// exercises the translated "DROP INDEX <name> ON <table>" form for two index
// names the runner's replay guard has never seen, proving
// isMySQLDropAlreadyAppliedOnConn/parseMySQLDropIndexStatement generalize
// past the single name (idx_asset_name_hex) they were built against.
func TestAssetAmountFingerprintIndexDropReplaysOnMySQL(t *testing.T) {
	t.Parallel()
	rootDSN := mysqlEngineTestRootDSN(t)

	database := "dingo_v20_replay"
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
	requireMySQLAssetIndexesAbsent(t, db, database)

	_, err = db.Exec(`
UPDATE schema_migrations SET phase = 'expand', dirty = 1, completed_at = NULL
WHERE version = ?`, 20)
	require.NoError(t, err)

	require.NoError(
		t,
		runner.Run(context.Background()),
		"v20 must replay its expand phase against MySQL",
	)
	requireMySQLAssetIndexesAbsent(t, db, database)
}

func requireMySQLAssetIndexesAbsent(t *testing.T, db *sql.DB, database string) {
	t.Helper()
	for _, name := range []string{"idx_asset_amount", "idx_asset_fingerprint"} {
		var count int
		require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.statistics
WHERE table_schema = ? AND table_name = 'asset'
  AND index_name = ?`,
			database, name).Scan(&count))
		require.Zero(t, count, "%s must not survive migration v20", name)
	}
	for _, column := range []string{"amount", "fingerprint"} {
		var count int
		require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.columns
WHERE table_schema = ? AND table_name = 'asset' AND column_name = ?`,
			database, column).Scan(&count))
		require.Equal(
			t, 1, count,
			"asset.%s must survive migration v20", column,
		)
	}
}
