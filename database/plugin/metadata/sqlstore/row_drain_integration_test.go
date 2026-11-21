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

func TestPostgresDeleteRowsChunk(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("sqlstore_drain_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	testDeleteRowsChunk(
		t, "pgx", postgresDSNWithSearchPath(t, dsn, schema), "postgres", schema,
	)
}

func TestMySQLDeleteRowsChunk(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("sqlstore_drain_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	testDeleteRowsChunk(
		t, "mysql", mysqlDSNWithDatabase(t, dsn, database), "mysql", database,
	)
}

// testDeleteRowsChunk drains a scratch table in chunks the way the ledger
// state drain does and requires every chunk to delete exactly its size until
// the last, which is short or empty.
func testDeleteRowsChunk(
	t *testing.T,
	driver, dsn, dialectName, lockNamespace string,
) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenDB(driver, dsn, dialectName, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var dialect Dialect
	var registry []migrations.Migration
	switch dialectName {
	case "postgres":
		dialect = PostgresDialect()
		registry, err = migrations.PostgresRegistry()
	case "mysql":
		dialect = MySQLDialect()
		registry, err = migrations.MySQLRegistry()
	}
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         dialect,
		Migrations:      registry,
		MigrationLocker: integrationMigrationLocker(dialectName, lockNamespace),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.Start(ctx))

	_, err = db.ExecContext(
		ctx, "CREATE TABLE drain_scratch (id BIGINT PRIMARY KEY, v BIGINT)",
	)
	require.NoError(t, err)
	count := func() int {
		var n int
		require.NoError(
			t,
			db.QueryRowContext(ctx, "SELECT count(*) FROM drain_scratch").
				Scan(&n),
		)
		return n
	}
	fill := func(rows int) {
		_, err := db.ExecContext(ctx, "DELETE FROM drain_scratch")
		require.NoError(t, err)
		for i := 1; i <= rows; i++ {
			// Keys ascend with gaps so the bound is not the row number.
			_, err := db.ExecContext(
				ctx,
				dialect.Rebind(
					"INSERT INTO drain_scratch (id, v) VALUES (?, ?)",
				),
				i*7, i,
			)
			require.NoError(t, err)
		}
		require.Equal(t, rows, count())
	}
	for _, tc := range []struct {
		rows, limit int
		want        []int64
	}{
		{23, 5, []int64{5, 5, 5, 5, 3}},
		{10, 5, []int64{5, 5, 0}},
		{3, 5, []int64{3}},
		{0, 5, []int64{0}},
		{4, 1, []int64{1, 1, 1, 1, 0}},
	} {
		fill(tc.rows)
		var got []int64
		for {
			n, err := store.DeleteRowsChunk(
				ctx,
				"drain_scratch",
				"id",
				tc.limit,
			)
			require.NoError(t, err)
			got = append(got, n)
			if n < int64(tc.limit) {
				break
			}
		}
		require.Equal(t, tc.want, got, "rows=%d limit=%d", tc.rows, tc.limit)
		require.Zero(t, count())
	}

	fill(12)
	n, err := store.DeleteRowsChunk(ctx, "drain_scratch", "id", 5)
	require.NoError(t, err)
	require.Equal(t, int64(5), n)
	var minID int
	require.NoError(
		t,
		db.QueryRowContext(ctx, "SELECT min(id) FROM drain_scratch").
			Scan(&minID),
	)
	require.Equal(t, 6*7, minID, "a chunk deletes the lowest keys first")

	_, err = store.DeleteRowsChunk(ctx, "sync_state", "key", 5)
	require.ErrorContains(t, err, "not ledger state")
}
