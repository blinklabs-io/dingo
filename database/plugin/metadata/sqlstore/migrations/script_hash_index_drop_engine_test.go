//go:build dingo_extra_plugins

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

package migrations_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

var scriptHashDroppedIndexes = []string{"idx_script_hash"}

var scriptHashKeptIndexes = []string{"idx_script_type", "uni_script_hash"}

// TestScriptHashIndexDropAppliesOnPostgres runs every migration against
// a real server, requires v65's drop to have taken effect and its kept
// indexes to remain, then replays v65's expand phase.
func TestScriptHashIndexDropAppliesOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := postgresEngineTestDSN(t)
	schema := "dingo_v65_replay"
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer admin.Close()
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
		DB: db, Dialect: "postgres", Registry: registry,
		Locker: migrations.NewProcessLocker(),
	}
	check := func() {
		for _, name := range scriptHashDroppedIndexes {
			var count int
			require.NoError(t, db.QueryRow(
				`SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`,
				schema,
				name,
			).Scan(&count))
			require.Zero(t, count, "%s must not survive v65", name)
		}
		for _, name := range scriptHashKeptIndexes {
			var count int
			require.NoError(t, db.QueryRow(
				`SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`,
				schema,
				name,
			).Scan(&count))
			require.Equal(t, 1, count, "%s must survive v65", name)
		}
	}
	require.NoError(t, runner.Run(context.Background()))
	check()
	_, err = db.Exec(`UPDATE schema_migrations
SET phase = 'expand', dirty = true, completed_at = NULL WHERE version = $1`, 65)
	require.NoError(t, err)
	require.NoError(t, runner.Run(context.Background()))
	check()
}

// TestScriptHashIndexDropAppliesOnMySQL is the MySQL half; its drops use
// the translated "DROP INDEX <name> ON <table>" form, which is not
// self-idempotent, so the replay exercises the runner's already-dropped guard
// for this name.
func TestScriptHashIndexDropAppliesOnMySQL(t *testing.T) {
	t.Parallel()
	rootDSN := mysqlEngineTestRootDSN(t)
	database := "dingo_v65_replay"
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	defer admin.Close()
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
		DB: db, Dialect: "mysql", Registry: registry,
		Locker: migrations.NewProcessLocker(),
	}
	count := func(name string) int {
		var n int
		require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.statistics
WHERE table_schema = ? AND index_name = ?`, database, name).Scan(&n))
		return n
	}
	check := func() {
		for _, name := range scriptHashDroppedIndexes {
			require.Zero(t, count(name), "%s must not survive v65", name)
		}
		for _, name := range scriptHashKeptIndexes {
			require.NotZero(t, count(name), "%s must survive v65", name)
		}
	}
	require.NoError(t, runner.Run(context.Background()))
	check()
	_, err = db.Exec(`UPDATE schema_migrations
SET phase = 'expand', dirty = 1, completed_at = NULL WHERE version = ?`, 65)
	require.NoError(t, err)
	require.NoError(t, runner.Run(context.Background()))
	check()
}
