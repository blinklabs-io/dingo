//go:build dingo_db_integration

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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// TestPostgresAccountBaselineBackfill and its MySQL twin run migration `v4`
// against a populated `account` table on the two non-default dialects.
// TestPostgresSQLStoreIntegration migrates from version 0, so its backfill
// executes over an empty table and proves only that the statement parses --
// which for MySQL error 1093 is enough, but not for the row filtering.
func TestPostgresAccountBaselineBackfill(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("baseline_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	testAccountBaselineBackfill(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
		registry,
	)
}

func TestMySQLAccountBaselineBackfill(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("baseline_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	testAccountBaselineBackfill(
		t,
		"mysql",
		mysqlDSNWithDatabase(t, dsn, database),
		"mysql",
		registry,
	)
}

func testAccountBaselineBackfill(
	t *testing.T,
	driver string,
	dsn string,
	dialectName string,
	registry []migrations.Migration,
) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDB(driver, dsn, dialectName)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	runTo := func(versions []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  dialectName,
			Registry: versions,
			Locker: migrations.NewAdvisoryLocker(
				dialectName,
				0x64696e676f6261,
				time.Second,
			),
		}
		require.NoError(t, runner.Run(ctx))
	}
	require.GreaterOrEqual(t, len(registry), 4)
	// The schema that predates the baseline table, so the rows below are the
	// legacy state the backfill reads.
	runTo(registry[:3])

	untouched := []byte{0x11, 0x22}
	delegated := []byte{0x33, 0x44}
	insertAccount := func(key []byte) {
		_, err := db.ExecContext(ctx, newDialectQueryerRebind(
			dialectName,
			`INSERT INTO account (
    staking_key, credential_tag, pool, added_slot, created_slot, active
) VALUES (?, 0, ?, 400, 0, TRUE)`,
		),
			key,
			[]byte{0xbb, 0xbb},
		)
		require.NoError(t, err)
	}
	insertAccount(untouched)
	insertAccount(delegated)
	_, err = db.ExecContext(ctx, newDialectQueryerRebind(
		dialectName,
		`INSERT INTO stake_delegation (
    staking_key, credential_tag, pool_key_hash, added_slot
) VALUES (?, 0, ?, 400)`,
	),
		delegated,
		[]byte{0xbb, 0xbb},
	)
	require.NoError(t, err)
	// A legacy row with no credential at all, which no baseline key can hold.
	_, err = db.ExecContext(ctx, `INSERT INTO account (
    staking_key, credential_tag, added_slot, created_slot, active
) VALUES (NULL, 0, 400, 0, TRUE)`)
	require.NoError(t, err)

	// Keep this regression scoped to the v4 baseline migration as later
	// migrations are appended to the registry.
	runTo(registry[:4])

	baselineKeys := func() [][]byte {
		rows, err := db.QueryContext(ctx, `
SELECT staking_key FROM account_import_baseline ORDER BY staking_key`)
		require.NoError(t, err)
		defer rows.Close()
		var keys [][]byte
		for rows.Next() {
			var key []byte
			require.NoError(t, rows.Scan(&key))
			keys = append(keys, key)
		}
		require.NoError(t, rows.Err())
		return keys
	}
	require.Equal(t, [][]byte{untouched}, baselineKeys())

	// An upgrade interrupted after the backfill committed but before its phase
	// row advanced replays the same statements.
	for _, statement := range registry[3].SQL[dialectName].Expand {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	require.Equal(t, [][]byte{untouched}, baselineKeys())
}

// newDialectQueryerRebind converts the `?` placeholders these fixtures are
// written with to the dialect's own form.
func newDialectQueryerRebind(dialectName, query string) string {
	switch dialectName {
	case "postgres":
		return PostgresDialect().Rebind(query)
	case "mysql":
		return MySQLDialect().Rebind(query)
	}
	return query
}
