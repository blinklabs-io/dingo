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

func TestPostgresRewardAccountOutputIntegration(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("rao_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	exerciseRewardAccountOutput(
		t, "pgx", postgresDSNWithSearchPath(t, dsn, schema), "postgres", schema,
		`SELECT indexname FROM pg_indexes
WHERE schemaname = current_schema() AND tablename = 'reward_account_output'
ORDER BY indexname`,
		"reward_account_output_pkey",
	)
}

func TestMySQLRewardAccountOutputIntegration(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("rao_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	exerciseRewardAccountOutput(
		t, "mysql", mysqlDSNWithDatabase(t, dsn, database), "mysql", database,
		`SELECT DISTINCT index_name FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = 'reward_account_output'
ORDER BY index_name`,
		"PRIMARY",
	)
}

// exerciseRewardAccountOutput migrates an empty database to the current
// schema, checks the table's index set, and requires the readers to return
// the digest recorded on SQLite for the same workload.
func exerciseRewardAccountOutput(
	t *testing.T,
	driver, dsn, dialectName, lockNamespace, indexQuery, primary string,
) {
	t.Helper()
	db, err := OpenDB(driver, dsn, dialectName, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var dialect Dialect
	var registry []migrations.Migration
	var locker migrations.Locker
	switch dialectName {
	case "postgres":
		dialect = PostgresDialect()
		registry, err = migrations.PostgresRegistry()
		locker = integrationMigrationLocker("postgres", lockNamespace)
	case "mysql":
		dialect = MySQLDialect()
		registry, err = migrations.MySQLRegistry()
		locker = integrationMigrationLocker("mysql", lockNamespace)
	}
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         dialect,
		Migrations:      registry,
		MigrationLocker: locker,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.Start(context.Background()))

	rows, err := db.Query(indexQuery)
	require.NoError(t, err)
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.ElementsMatch(t, []string{
		primary,
		"idx_reward_account_output_captured_slot",
		"idx_reward_account_output_credential_epoch",
		"idx_reward_account_output_epoch_pool_cred_type",
		"idx_reward_account_output_pending_round",
	}, names)

	require.Equal(t, rewardAccountOutputReaderDigest,
		rewardAccountOutputReaderDigestOf(t, store))
}
