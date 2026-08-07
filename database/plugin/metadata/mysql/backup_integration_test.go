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

//go:build dingo_extra_plugins && dingo_db_integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// mysqlIntegrationDSN mirrors sqlstore/dialect_integration_test.go's
// TestMySQLSQLStoreIntegration default: same env var, same fallback.
func mysqlIntegrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	return dsn
}

// createIsolatedDatabase creates a uniquely named database on the server
// dsn points at and returns a DSN pointed at it, so concurrent test runs
// (and the src/dst databases within a single test) never see each other's
// tables -- mirrors dialect_integration_test.go's mysqlDSNWithDatabase.
func createIsolatedDatabase(t *testing.T, dsn, namePrefix string) string {
	t.Helper()
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	parsed, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	parsed.DBName = database
	return parsed.FormatDSN()
}

// TestBackupToRestoreFromIntegration validates the full round trip against
// a real MySQL server: BackupTo produces a real mysqldump archive from a
// store with known data, and RestoreFrom into a separate empty database
// reproduces that same data exactly.
func TestBackupToRestoreFromIntegration(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_src")

	srcStore, err := openStore(
		context.Background(),
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcStore.Close() })
	require.NoError(t, srcStore.Start(context.Background()))

	txn := srcStore.Transaction()
	require.NoError(t, srcStore.SetCommitTimestamp(4242, txn))
	require.NoError(t, txn.Commit())

	dumpPath := filepath.Join(t.TempDir(), "backup.sql")
	require.NoError(t, srcStore.BackupTo(context.Background(), dumpPath))
	require.FileExists(t, dumpPath)

	dstDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_dst")
	dstStore, err := openStore(
		context.Background(),
		Config{DSN: dstDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstStore.Close() })

	require.NoError(t, dstStore.RestoreFrom(context.Background(), dumpPath))
	require.NoError(t, dstStore.Start(context.Background()))

	restoredTimestamp, err := dstStore.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, int64(4242), restoredTimestamp)
}

// TestBackupRejectsExistingDestinationIntegration validates that a second
// BackupTo call against the same dstPath fails against a real server,
// rather than mysqldump silently overwriting the first backup.
func TestBackupRejectsExistingDestinationIntegration(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_exists")
	store, err := openStore(
		context.Background(),
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Start(context.Background()))

	dumpPath := filepath.Join(t.TempDir(), "backup.sql")
	require.NoError(t, store.BackupTo(context.Background(), dumpPath))
	require.Error(t, store.BackupTo(context.Background(), dumpPath))
}

// TestRestoreRejectsNonEmptyTargetIntegration validates that RestoreFrom
// refuses to run mysql against a real database that Start has already
// migrated (and therefore isn't empty), instead of merging into or
// partially overwriting it.
func TestRestoreRejectsNonEmptyTargetIntegration(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_src2")
	srcStore, err := openStore(
		context.Background(),
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcStore.Close() })
	require.NoError(t, srcStore.Start(context.Background()))
	dumpPath := filepath.Join(t.TempDir(), "backup.sql")
	require.NoError(t, srcStore.BackupTo(context.Background(), dumpPath))

	// The destination already has tables from Start's own migrations, so
	// restore must refuse rather than merge into it.
	dstDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_dst2")
	dstStore, err := openStore(
		context.Background(),
		Config{DSN: dstDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstStore.Close() })
	require.NoError(t, dstStore.Start(context.Background()))

	require.Error(t, dstStore.RestoreFrom(context.Background(), dumpPath))
}

// TestResetThenRestoreIntegration reproduces, at the package level, exactly
// what a real end-to-end "dingo database restore" run against a live
// MySQL server does through database/lifecycle/restore.go's
// restoreMetadataStore: briefly Start a store against the target (which
// runs real migrations, leaving real tables behind), then Reset it, then
// RestoreFrom a real backup. Guards the fix for the same class of bug
// found via a live Postgres restore attempt (target "already contains
// tables" because nothing undid the brief start) -- Reset must leave the
// target empty enough for RestoreFrom to succeed afterward.
func TestResetThenRestoreIntegration(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_src3")
	srcStore, err := openStore(
		context.Background(),
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcStore.Close() })
	require.NoError(t, srcStore.Start(context.Background()))
	txn := srcStore.Transaction()
	require.NoError(t, srcStore.SetCommitTimestamp(777, txn))
	require.NoError(t, txn.Commit())
	dumpPath := filepath.Join(t.TempDir(), "backup.sql")
	require.NoError(t, srcStore.BackupTo(context.Background(), dumpPath))

	dstDSN := createIsolatedDatabase(t, baseDSN, "mysqlbackup_dst3")
	dstStore, err := openStore(
		context.Background(),
		Config{DSN: dstDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstStore.Close() })
	// Simulates restoreMetadataStore's brief resolve-and-start.
	require.NoError(t, dstStore.Start(context.Background()))
	require.NoError(t, dstStore.Reset(context.Background()))

	require.NoError(t, dstStore.RestoreFrom(context.Background(), dumpPath))
	require.NoError(t, dstStore.Start(context.Background()))
	restoredTimestamp, err := dstStore.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, int64(777), restoredTimestamp)
}

// TestResetRefusesWhenTargetHasData guards a real gap: Reset used to drop
// every table unconditionally once Start had migrated it, with nothing
// checking whether the target already held real, previously accumulated
// data (e.g. a live node's own database, reused or pointed at by a
// misconfigured DSN) rather than being freshly migrated and still empty --
// silently destroying it. Reset must refuse instead, leaving the data
// exactly as it found it.
func TestResetRefusesWhenTargetHasData(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "mysqlbackup_hasdata")
	store, err := openStore(
		context.Background(),
		Config{DSN: dsn},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Start(context.Background()))

	txn := store.Transaction()
	require.NoError(t, store.SetCommitTimestamp(999, txn))
	require.NoError(t, txn.Commit())

	err = store.Reset(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "already contains data")

	restoredTimestamp, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(
		t, int64(999), restoredTimestamp,
		"a refused Reset must not have touched the existing data",
	)
}

// TestResetToleratesView guards a real gap: databaseIsEmpty/resetDatabase
// originally counted every information_schema.tables row as a base
// table, so a view sitting alongside dingo's own tables (something an
// operator's own tooling, or a future dingo migration, could add) would
// be counted as occupying the database -- and MySQL doesn't error on
// "DROP TABLE" naming a view, it just emits a note and silently leaves it
// in place, defeating the reset instead of failing loudly or actually
// clearing it. Both must restrict to table_type = 'BASE TABLE' and leave
// the view untouched.
func TestResetToleratesView(t *testing.T) {
	baseDSN := mysqlIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "mysqlbackup_view")
	store, err := openStore(
		context.Background(),
		Config{DSN: dsn},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Start(context.Background()))

	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	_, err = admin.Exec(
		"CREATE VIEW mysqlbackup_view_test AS SELECT id FROM node_settings",
	)
	require.NoError(t, err)

	parsed, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	require.NoError(t, store.Reset(context.Background()))

	empty, err := databaseIsEmpty(context.Background(), admin, parsed.DBName)
	require.NoError(t, err)
	require.True(t, empty, "reset must leave a base-table-empty database, "+
		"even with a view still present")

	var viewCount int
	require.NoError(t, admin.QueryRow(
		"SELECT count(*) FROM information_schema.views "+
			"WHERE table_schema = ? AND table_name = 'mysqlbackup_view_test'",
		parsed.DBName,
	).Scan(&viewCount))
	require.Equal(t, 1, viewCount, "reset must not have touched the view")
}
