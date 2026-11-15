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

func TestPostgresHistoricalStakeCountsGenesisDelegation(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	t.Cleanup(func() { _ = admin.Close() })
	testHistoricalStakeCountsGenesisDelegation(
		t,
		func(t *testing.T) *Store {
			t.Helper()
			schema := fmt.Sprintf("sqlstore_genesis_stake_%d", time.Now().UnixNano())
			_, err := admin.Exec(`CREATE SCHEMA "` + schema + `"`)
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
			})
			return newIntegrationStore(
				t, "pgx", postgresDSNWithSearchPath(t, dsn, schema),
				"postgres", schema,
			)
		},
	)
}

func TestMySQLHistoricalStakeCountsGenesisDelegation(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	t.Cleanup(func() { _ = admin.Close() })
	testHistoricalStakeCountsGenesisDelegation(
		t,
		func(t *testing.T) *Store {
			t.Helper()
			database := fmt.Sprintf("sqlstore_genesis_stake_%d", time.Now().UnixNano())
			_, err := admin.Exec("CREATE DATABASE `" + database + "`")
			require.NoError(t, err)
			t.Cleanup(func() {
				_, _ = admin.Exec("DROP DATABASE `" + database + "`")
			})
			return newIntegrationStore(
				t, "mysql", mysqlDSNWithDatabase(t, dsn, database),
				"mysql", database,
			)
		},
	)
}

func newIntegrationStore(
	t *testing.T,
	driver, dsn, dialectName, lockNamespace string,
) *Store {
	t.Helper()
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
	require.NoError(t, store.Start(context.Background()))
	require.True(t, store.Ready())
	return store
}
