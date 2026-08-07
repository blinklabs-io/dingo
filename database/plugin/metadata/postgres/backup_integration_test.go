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

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// postgresIntegrationDSN mirrors sqlstore/dialect_integration_test.go's
// TestPostgresSQLStoreIntegration default: same env var, same fallback.
func postgresIntegrationDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	return dsn
}

// createIsolatedDatabase creates a uniquely named database on the server
// dsn points at and returns a DSN pointed at it, so concurrent test runs
// (and the src/dst databases within a single test) never see each other's
// tables. Isolating via a separate database, not a schema within a shared
// one (contrast dialect_integration_test.go's postgresDSNWithSearchPath,
// used for schema-scoped SQL operations): pg_dump/pg_restore operate on the
// whole database a DSN points at regardless of search_path, so two "schema
// siblings" in the same database collide the moment either one is dumped
// (its CREATE SCHEMA statement conflicts with the other schema's own
// still-existing one on restore).
func createIsolatedDatabase(t *testing.T, dsn, namePrefix string) string {
	t.Helper()
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("%s_%d", namePrefix, time.Now().UnixNano())
	_, err = admin.Exec(`CREATE DATABASE "` + database + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Every caller closes its own store(s) against this database via a
		// t.Cleanup registered after this one, which Go runs first (LIFO) --
		// but terminate any backend connections that might still be open
		// anyway (a test's own store failing to close cleanly, or a still-
		// idle pooled connection) so DROP DATABASE below doesn't fail with
		// "database is being accessed by other users" and silently leave it
		// behind for good, accumulating unreclaimed databases on a shared
		// CI/Postgres server.
		_, _ = admin.Exec(
			`SELECT pg_terminate_backend(pid) FROM pg_stat_activity `+
				`WHERE datname = $1 AND pid <> pg_backend_pid()`,
			database,
		)
		_, err := admin.Exec(`DROP DATABASE "` + database + `"`)
		require.NoError(t, err, "cleanup: drop isolated test database %q", database)
		require.NoError(t, admin.Close())
	})
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	parsed.Path = "/" + database
	return parsed.String()
}

// TestBackupToRestoreFromIntegration validates the full round trip against
// a real Postgres server: BackupTo produces a real pg_dump archive from a
// store with known data, and RestoreFrom into a separate empty database
// reproduces that same data exactly. Writes to two independent tables
// (commit_timestamp via SetCommitTimestamp, and the real node_settings
// table via SetNodeSettings) rather than just one, so a pg_dump/pg_restore
// path that silently dropped or corrupted some other real user table would
// still be caught here instead of passing on the strength of a single
// table's data alone.
func TestBackupToRestoreFromIntegration(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_src")

	srcStore, err := openStore(
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcStore.Close() })
	require.NoError(t, srcStore.Start(context.Background()))

	txn := srcStore.Transaction()
	require.NoError(t, srcStore.SetCommitTimestamp(4242, txn))
	require.NoError(t, txn.Commit())
	require.NoError(t, srcStore.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeAPI,
		Network:     "preview",
	}))

	dumpPath := filepath.Join(t.TempDir(), "backup.dump")
	require.NoError(t, srcStore.BackupTo(context.Background(), dumpPath))
	require.FileExists(t, dumpPath)

	dstDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_dst")
	dstStore, err := openStore(
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

	restoredSettings, err := dstStore.GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, types.StorageModeAPI, restoredSettings.StorageMode)
	require.Equal(t, "preview", restoredSettings.Network)
}

// TestBackupRejectsExistingDestinationIntegration validates that a second
// BackupTo call against the same dstPath fails against a real server,
// rather than pg_dump silently overwriting the first backup.
func TestBackupRejectsExistingDestinationIntegration(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_exists")
	store, err := openStore(
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Start(context.Background()))

	dumpPath := filepath.Join(t.TempDir(), "backup.dump")
	require.NoError(t, store.BackupTo(context.Background(), dumpPath))
	require.Error(t, store.BackupTo(context.Background(), dumpPath))
}

// TestRestoreRejectsNonEmptyTargetIntegration validates that RestoreFrom
// refuses to run pg_restore against a real database that Start has
// already migrated (and therefore isn't empty), instead of merging into
// or partially overwriting it.
func TestRestoreRejectsNonEmptyTargetIntegration(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_src2")
	srcStore, err := openStore(
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcStore.Close() })
	require.NoError(t, srcStore.Start(context.Background()))
	dumpPath := filepath.Join(t.TempDir(), "backup.dump")
	require.NoError(t, srcStore.BackupTo(context.Background(), dumpPath))

	// The destination already has tables from Start's own migrations, so
	// restore must refuse rather than merge into it.
	dstDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_dst2")
	dstStore, err := openStore(
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
// Postgres server does through database/lifecycle/restore.go's
// restoreMetadataStore: briefly Start a store against the target (which
// runs real migrations, leaving real tables behind -- confirmed via an
// actual CLI restore attempt against a live server, which failed with
// "target database already contains tables" because nothing undid this),
// then Reset it, then RestoreFrom a real backup. Guards the fix: Reset
// must leave the target empty enough for RestoreFrom to succeed
// afterward, not merely close to empty.
func TestResetThenRestoreIntegration(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	srcDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_src3")
	srcStore, err := openStore(
		Config{DSN: srcDSN},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcStore.Close() })
	require.NoError(t, srcStore.Start(context.Background()))
	txn := srcStore.Transaction()
	require.NoError(t, srcStore.SetCommitTimestamp(777, txn))
	require.NoError(t, txn.Commit())
	dumpPath := filepath.Join(t.TempDir(), "backup.dump")
	require.NoError(t, srcStore.BackupTo(context.Background(), dumpPath))

	dstDSN := createIsolatedDatabase(t, baseDSN, "pgbackup_dst3")
	dstStore, err := openStore(
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
	baseDSN := postgresIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "pgbackup_hasdata")
	store, err := openStore(Config{DSN: dsn}, metadata.ProviderDependencies{})
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

// TestResetRefusesWhenOtherSchemaHasSchemaMigrationsTableWithData guards a
// real gap: refuseIfTargetHasData originally exempted any table literally
// named "schema_migrations" regardless of which schema it lived in, but
// resetDatabase scans every non-system schema -- an operator's own tooling
// (or an unrelated app sharing this database) could have its own,
// differently-scoped table that happens to share that common name. A
// name-only exemption let a populated one of those slip past this check
// and then get dropped anyway by resetDatabase's unconditional DROP TABLE.
// The exemption must be scoped to (schema, name), matching exactly the
// schema migrations/runner.go's own hasUserTables uses (current_schema()).
func TestResetRefusesWhenOtherSchemaHasSchemaMigrationsTableWithData(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "pgbackup_othermig")
	store, err := openStore(Config{DSN: dsn}, metadata.ProviderDependencies{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Start(context.Background()))

	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	_, err = admin.Exec("CREATE SCHEMA other_app")
	require.NoError(t, err)
	_, err = admin.Exec("CREATE TABLE other_app.schema_migrations (id int)")
	require.NoError(t, err)
	_, err = admin.Exec(
		"INSERT INTO other_app.schema_migrations (id) VALUES (1)",
	)
	require.NoError(t, err)

	err = store.Reset(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "already contains data")

	var count int
	require.NoError(t, admin.QueryRow(
		"SELECT count(*) FROM other_app.schema_migrations",
	).Scan(&count))
	require.Equal(
		t, 1, count,
		"a refused Reset must not have touched the other schema's table",
	)
}

// TestResetToleratesView guards a real gap: databaseIsEmpty/resetDatabase
// originally counted every information_schema.tables row as a base
// table, so a view sitting alongside dingo's own tables (something an
// operator's own tooling, or a future dingo migration, could add) would
// be counted as occupying the database and then fail resetDatabase
// outright -- postgres rejects "DROP TABLE" naming a view. Both must
// restrict to table_type = 'BASE TABLE' and leave the view untouched.
func TestResetToleratesView(t *testing.T) {
	baseDSN := postgresIntegrationDSN(t)
	dsn := createIsolatedDatabase(t, baseDSN, "pgbackup_view")
	store, err := openStore(Config{DSN: dsn}, metadata.ProviderDependencies{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	require.NoError(t, store.Start(context.Background()))

	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	_, err = admin.Exec(
		"CREATE VIEW pgbackup_view_test AS SELECT id FROM node_settings",
	)
	require.NoError(t, err)

	require.NoError(t, store.Reset(context.Background()))

	empty, err := databaseIsEmpty(context.Background(), admin)
	require.NoError(t, err)
	require.True(t, empty, "reset must leave a base-table-empty database, "+
		"even with a view still present")

	var viewCount int
	require.NoError(t, admin.QueryRow(
		"SELECT count(*) FROM information_schema.views "+
			"WHERE table_name = 'pgbackup_view_test'",
	).Scan(&viewCount))
	require.Equal(t, 1, viewCount, "reset must not have touched the view")
}
