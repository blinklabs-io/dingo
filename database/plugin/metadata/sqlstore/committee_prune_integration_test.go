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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The auth_committee_hot prune statement uses a derived table inside an IN
// subquery specifically because MySQL rejects both a direct self-reference in
// a DELETE subquery and a bare LIMIT inside IN. That workaround cannot be
// verified on SQLite, so exercise the statement on each real backend.
func TestPostgresCommitteeAuthPruneIntegration(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("prune_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	exerciseCommitteeAuthPrune(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
		schema,
	)
}

func TestMySQLCommitteeAuthPruneIntegration(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("prune_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	exerciseCommitteeAuthPrune(
		t,
		"mysql",
		mysqlDSNWithDatabase(t, dsn, database),
		"mysql",
		database,
	)
}

func exerciseCommitteeAuthPrune(
	t *testing.T,
	driver, dsn, dialectName, lockNamespace string,
) {
	t.Helper()
	db, err := OpenDB(driver, dsn, dialectName)
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
	const coldTag = uint8(lcommon.CredentialTypeAddrKeyHash)
	cold := credentialHash(0xc4)

	for i := 1; i <= 20; i++ {
		_, err = store.writeDB.Exec(
			store.dialect.Rebind(`
INSERT INTO auth_committee_hot (
    cold_credential_tag, cold_credential, hot_credential_tag,
    host_credential, certificate_id, added_slot
) VALUES (?, ?, ?, ?, ?, ?)`),
			coldTag, cold, uint8(lcommon.CredentialTypeAddrKeyHash),
			hotHash(0x40, i), uint64(i), uint64(1_000*i),
		)
		require.NoError(t, err)
	}

	queryer := newDialectQueryer(store.writeDB, store.dialect.Name())
	pruned, err := store.pruneCommitteeHotAuthorizations(
		context.Background(), queryer, coldTag, cold, preprodTipSlot,
	)
	require.NoError(t, err)
	require.Equal(t, int64(19), pruned)

	var remaining, retainedSlot uint64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*), MAX(added_slot) FROM auth_committee_hot",
	).Scan(&remaining, &retainedSlot))
	require.Equal(t, uint64(1), remaining)
	require.Equal(t, uint64(20_000), retainedSlot)
}
