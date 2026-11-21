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

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestCoreDSNSelectsOnlyTheConsensusSchema(t *testing.T) {
	t.Parallel()
	for _, dsn := range []string{
		"postgres://alice:secret@localhost:5433/archive?sslmode=disable&search_path=chain&statement_timeout=123&dbname=archive",
		`host='/tmp/socket' port=5433 user=alice password='a b' dbname=archive sslmode=disable options='-csearch_path=chain -cstatement_timeout=123'`,
	} {
		source, err := pgx.ParseConfig(dsn)
		require.NoError(t, err)
		selected, err := coreDSN(dsn, "dingo_core_test")
		require.NoError(t, err)
		core, err := pgx.ParseConfig(selected)
		require.NoError(t, err)
		require.Equal(t, "archive", core.Database,
			"the consensus schema lives in the API database")
		require.Equal(t, source.Host, core.Host)
		require.Equal(t, source.Port, core.Port)
		require.Equal(t, source.User, core.User)
		require.Equal(t, source.Password, core.Password)
		require.Equal(t, "dingo_core_test", core.RuntimeParams["search_path"])
		want := maps.Clone(source.RuntimeParams)
		want["search_path"] = "dingo_core_test"
		require.Equal(t, want, core.RuntimeParams)
	}
}

func TestCoreProviderCreatesASchemaInTheAPIDatabase(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip(
			"PostgreSQL is not configured; CI services exercise namespace provisioning",
		)
	}
	t.Parallel()
	ctx := context.Background()
	sourceName, sourceDSN := newCoreTestDatabase(t)
	schema := "chain_state"
	source, err := sql.Open("pgx", sourceDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	_, err = source.ExecContext(ctx, "CREATE SCHEMA "+pgQuoteIdentifier(schema))
	require.NoError(t, err)
	sourceDSN = storagetest.PostgresDSNWithSearchPath(sourceDSN, schema)
	coreName := metadata.CoreNamespaceName(sourceName, schema)
	for range 2 {
		store := resolveCoreStore(t, sourceDSN)
		_, err = store.GetTip(nil)
		require.NoError(t, err)
	}
	selected, err := coreDSN(sourceDSN, coreName)
	require.NoError(t, err)
	core, err := sql.Open("pgx", selected)
	require.NoError(t, err)
	defer core.Close()
	var database, gotSchema string
	require.NoError(
		t,
		core.QueryRowContext(ctx, "SELECT current_database(),current_schema()").
			Scan(&database, &gotSchema),
	)
	require.Equal(t, sourceName, database)
	require.Equal(t, coreName, gotSchema)
	var sourceTables, coreTables int
	require.NoError(t, source.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1", schema).
		Scan(&sourceTables))
	require.Zero(t, sourceTables,
		"consensus provisioning must not create tables in the API schema")
	require.NoError(t, source.QueryRowContext(ctx, "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1", coreName).
		Scan(&coreTables))
	require.NotZero(t, coreTables)
}

// TestCoreProviderNeedsOnlyCreateOnTheDatabase opens the consensus schema as
// a role without CREATEDB that holds CREATE on the API database, and reports
// the GRANT a role without it needs.
func TestCoreProviderNeedsOnlyCreateOnTheDatabase(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip(
			"PostgreSQL is not configured; CI services exercise namespace provisioning",
		)
	}
	t.Parallel()
	ctx := context.Background()
	sourceName, sourceDSN := newCoreTestDatabase(t)
	admin, err := sql.Open("pgx", sourceDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	role := fmt.Sprintf("core_role_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE ROLE "+pgQuoteIdentifier(role)+" LOGIN NOCREATEDB PASSWORD 'pw'")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP OWNED BY " + pgQuoteIdentifier(role) + " CASCADE")
	})
	for _, grant := range []string{
		"GRANT CONNECT ON DATABASE " + pgQuoteIdentifier(sourceName) + " TO " + pgQuoteIdentifier(role),
		"GRANT ALL ON SCHEMA public TO " + pgQuoteIdentifier(role),
	} {
		_, err = admin.ExecContext(ctx, grant)
		require.NoError(t, err)
	}
	cfg, err := pgx.ParseConfig(sourceDSN)
	require.NoError(t, err)
	roleDSN := fmt.Sprintf(
		"host=%s port=%d user=%s password=pw dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, role, sourceName,
	)
	_, err = resolveCoreStoreErr(t, roleDSN)
	require.ErrorContains(t, err, "lacks CREATE on the database")
	require.ErrorContains(t, err, "GRANT CREATE ON DATABASE "+pgQuoteIdentifier(sourceName)+" TO "+pgQuoteIdentifier(role))
	require.ErrorContains(t, err, metadata.CoreNamespaceName(sourceName, "public"))

	_, err = admin.ExecContext(ctx, "GRANT CREATE ON DATABASE "+pgQuoteIdentifier(sourceName)+" TO "+pgQuoteIdentifier(role))
	require.NoError(t, err)
	store, err := resolveCoreStoreErr(t, roleDSN)
	require.NoError(t, err)
	_, err = store.GetTip(nil)
	require.NoError(t, err)

	_, err = admin.ExecContext(ctx, "REVOKE CONNECT ON DATABASE "+pgQuoteIdentifier(sourceName)+" FROM PUBLIC, "+pgQuoteIdentifier(role))
	require.NoError(t, err)
	_, err = resolveCoreStoreErr(t, roleDSN)
	require.ErrorContains(t, err, "GRANT CONNECT ON DATABASE")
}

func newCoreTestDatabase(t *testing.T) (string, string) {
	t.Helper()
	ctx := context.Background()
	admin, err := sql.Open("pgx", postgresAdminDSN())
	require.NoError(t, err)
	name := fmt.Sprintf("core_adoption_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(ctx, "CREATE DATABASE "+pgQuoteIdentifier(name))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE " + pgQuoteIdentifier(name) + " WITH (FORCE)")
		require.NoError(t, admin.Close())
	})
	cfg, err := pgx.ParseConfig(postgresAdminDSN())
	require.NoError(t, err)
	return name, fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User,
		storagetest.EscapeLibpqValue(cfg.Password), name,
	)
}

func resolveCoreStore(t *testing.T, dsn string) metadata.MetadataStore {
	t.Helper()
	store, err := resolveCoreStoreErr(t, dsn)
	require.NoError(t, err)
	return store
}

func resolveCoreStoreErr(t *testing.T, dsn string) (metadata.MetadataStore, error) {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	return plugin.Resolve[metadata.MetadataStore](
		context.Background(),
		host,
		plugin.CapabilityStorageMetadata,
		"postgres",
		map[string]any{"dsn": dsn},
		metadata.ProviderDependencies{
			StorageMode:         "core",
			CoreNamespace:       true,
			GenesisStakingEmpty: true,
		},
	)
}

// TestCoreSchemaScopesWholeDatabaseOperations checks that the IndexerDB and
// its consensus schema, which share a database, never see each other's tables
// in a restore-target check or reset, and that a populated consensus schema
// is refused by name.
func TestCoreSchemaScopesWholeDatabaseOperations(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip(
			"PostgreSQL is not configured; CI services exercise namespace provisioning",
		)
	}
	t.Parallel()
	ctx := context.Background()
	sourceName, sourceDSN := newCoreTestDatabase(t)
	api, err := openStore(Config{DSN: sourceDSN}, metadata.ProviderDependencies{StorageMode: "api", GenesisStakingEmpty: true})
	require.NoError(t, err)
	require.NoError(t, api.Start(ctx))
	t.Cleanup(func() { _ = api.Close() })
	require.NoError(t, api.SetSyncState("api-row", "1", nil))
	core := resolveCoreStore(t, sourceDSN)
	require.NoError(t, core.SetSyncState("core-row", "1", nil))
	coreName := metadata.CoreNamespaceName(sourceName, "public")

	db, err := sql.Open("pgx", sourceDSN)
	require.NoError(t, err)
	defer db.Close()
	apiTables, err := tablesForReset(metadata.AllowResetOfPopulatedTarget(ctx), db, pgScope{})
	require.NoError(t, err)
	coreTables, err := tablesForReset(metadata.AllowResetOfPopulatedTarget(ctx), db, pgScope{schema: coreName})
	require.NoError(t, err)
	require.NotEmpty(t, apiTables)
	require.Len(t, coreTables, len(apiTables))
	for _, table := range apiTables {
		require.Equal(t, "public", table.schema)
	}
	for _, table := range coreTables {
		require.Equal(t, coreName, table.schema)
	}

	check, ok := core.(interface{ CheckRestoreTarget(context.Context) error })
	require.True(t, ok)
	err = check.CheckRestoreTarget(ctx)
	require.ErrorContains(t, err, `DROP SCHEMA "`+coreName+`" CASCADE`)

	require.NoError(t, resetDatabase(metadata.AllowResetOfPopulatedTarget(ctx), db, pgScope{}))
	value, err := core.GetSyncState("core-row", nil)
	require.NoError(t, err)
	require.Equal(t, "1", value, "resetting the IndexerDB leaves its consensus schema")
}

// TestCoreSchemaBackupRoundTrip backs the IndexerDB and its consensus schema
// up separately and restores the consensus schema into its emptied self: each
// archive holds only its own schemas.
func TestCoreSchemaBackupRoundTrip(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip("PostgreSQL is not configured")
	}
	if _, err := exec.LookPath("pg_dump"); err != nil {
		t.Skip("pg_dump is not installed")
	}
	t.Parallel()
	ctx := context.Background()
	sourceName, sourceDSN := newCoreTestDatabase(t)
	api, err := openStore(Config{DSN: sourceDSN}, metadata.ProviderDependencies{StorageMode: "api", GenesisStakingEmpty: true})
	require.NoError(t, err)
	require.NoError(t, api.Start(ctx))
	t.Cleanup(func() { _ = api.Close() })
	core := resolveCoreStore(t, sourceDSN)
	require.NoError(t, core.SetSyncState("core-row", "kept", nil))
	coreName := metadata.CoreNamespaceName(sourceName, "public")
	dir := t.TempDir()
	coreBackup := filepath.Join(dir, "core.dump")
	apiBackup := filepath.Join(dir, "api.dump")
	require.NoError(t, core.(metadata.Backuper).BackupTo(ctx, coreBackup))
	require.NoError(t, api.BackupTo(ctx, apiBackup))
	for path, want := range map[string]string{coreBackup: coreName, apiBackup: "public"} {
		list, err := exec.Command("pg_restore", "--list", path).Output()
		require.NoError(t, err)
		for _, line := range strings.Split(string(list), "\n") {
			if strings.Contains(line, " TABLE ") {
				require.Contains(t, line, " "+want+" ", "an archive holds only its own schemas")
			}
		}
	}
	allowed := metadata.AllowResetOfPopulatedTarget(ctx)
	require.NoError(t, core.(metadata.Resettable).Reset(allowed))
	require.NoError(t, core.(metadata.Restorer).RestoreFrom(allowed, coreBackup))
	value, err := core.GetSyncState("core-row", nil)
	require.NoError(t, err)
	require.Equal(t, "kept", value)
}

// TestCoreSchemaRestoresIntoAnotherDatabase restores a consensus-schema
// archive into the consensus schema of a database of another name, whose
// derived schema name differs from the archive's.
func TestCoreSchemaRestoresIntoAnotherDatabase(t *testing.T) {
	if !isPostgresConfigured() {
		t.Skip("PostgreSQL is not configured")
	}
	if _, err := exec.LookPath("pg_dump"); err != nil {
		t.Skip("pg_dump is not installed")
	}
	t.Parallel()
	ctx := context.Background()
	_, sourceDSN := newCoreTestDatabase(t)
	core := resolveCoreStore(t, sourceDSN)
	require.NoError(t, core.SetSyncState("core-row", "moved", nil))
	backup := filepath.Join(t.TempDir(), "core.dump")
	require.NoError(t, core.(metadata.Backuper).BackupTo(ctx, backup))

	targetName, targetDSN := newCoreTestDatabase(t)
	target := resolveCoreStore(t, targetDSN)
	allowed := metadata.AllowResetOfPopulatedTarget(ctx)
	require.NoError(t, target.(metadata.Resettable).Reset(allowed))
	require.NoError(t, target.(metadata.Restorer).RestoreFrom(allowed, backup))
	value, err := target.GetSyncState("core-row", nil)
	require.NoError(t, err)
	require.Equal(t, "moved", value)
	db, err := sql.Open("pgx", targetDSN)
	require.NoError(t, err)
	defer db.Close()
	names, err := coreSchemas(ctx, db)
	require.NoError(t, err)
	require.Equal(t, []string{metadata.CoreNamespaceName(targetName, "public")}, names)
}
