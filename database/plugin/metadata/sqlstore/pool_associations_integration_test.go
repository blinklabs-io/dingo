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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestPostgresGetPoolDoesNotCorruptConnection and its MySQL twin prove the
// fix for a real-backend connection-hygiene bug found while adding a real
// PostgreSQL/MySQL backend to the ledger-rules conformance suite
// (internal/test/conformance): loadPoolAssociations queried
// pool_registration through a *sql.Rows cursor and, for every row, issued
// the owner/relay child queries (loadPoolRegistrationChildren) *before*
// closing that outer cursor. On SQLite a connection may have multiple
// concurrently active statements, so this was harmless there, but MySQL
// and PostgreSQL connections are strictly request/response: opening a new
// query while the outer result set is still unread corrupts the
// connection once it is returned to the pool and reused, surfacing as
// go-sql-driver/mysql's "busy buffer" / "unexpected sequence nr" warnings
// and a "driver: bad connection" error on the next unrelated read -- which
// is exactly what made GetPool spuriously report a freshly registered
// pool as unregistered a few statements later in the conformance harness.
// A pool with at least one registered owner (or relay) reproduces it: with
// zero children, loadPoolRegistrationChildren never opens a nested cursor
// and the bug is invisible.
func TestPostgresGetPoolDoesNotCorruptConnection(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("sqlstore_pool_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	testGetPoolDoesNotCorruptConnection(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
	)
}

func TestMySQLGetPoolDoesNotCorruptConnection(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("sqlstore_pool_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	testGetPoolDoesNotCorruptConnection(
		t,
		"mysql",
		mysqlDSNWithDatabase(t, dsn, database),
		"mysql",
	)
}

func testGetPoolDoesNotCorruptConnection(
	t *testing.T,
	driver, dsn, dialectName string,
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
		locker = migrations.NewAdvisoryLocker(
			"postgres",
			0x64696e676f6d6574,
			time.Second,
		)
	case "mysql":
		dialect = MySQLDialect()
		registry, err = migrations.MySQLRegistry()
		locker = migrations.NewAdvisoryLocker(
			"mysql",
			0x64696e676f6d6574,
			time.Second,
		)
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
	require.True(t, store.Ready())

	poolKeyHash := make([]byte, 28)
	for i := range poolKeyHash {
		poolKeyHash[i] = byte(i + 1)
	}
	vrfKeyHash := make([]byte, 32)
	for i := range vrfKeyHash {
		vrfKeyHash[i] = byte(i + 2)
	}
	rewardAccount := make([]byte, 28)
	for i := range rewardAccount {
		rewardAccount[i] = byte(i + 3)
	}
	ownerKeyHash := make([]byte, 28)
	for i := range ownerKeyHash {
		ownerKeyHash[i] = byte(i + 4)
	}

	pool := &models.Pool{
		PoolKeyHash:                poolKeyHash,
		VrfKeyHash:                 vrfKeyHash,
		RewardAccount:              rewardAccount,
		RewardAccountCredentialTag: 0,
		Pledge:                     types.Uint64(1000),
		Cost:                       types.Uint64(500),
	}
	registration := &models.PoolRegistration{
		VrfKeyHash:                 vrfKeyHash,
		PoolKeyHash:                poolKeyHash,
		RewardAccount:              rewardAccount,
		RewardAccountCredentialTag: 0,
		Pledge:                     types.Uint64(1000),
		Cost:                       types.Uint64(500),
		AddedSlot:                  100,
		// At least one owner is required to reproduce the bug: with zero
		// registration children, loadPoolRegistrationChildren never opens a
		// nested cursor against the still-open outer one.
		Owners: []models.PoolRegistrationOwner{
			{KeyHash: ownerKeyHash},
		},
	}
	require.NoError(t, store.ImportPool(pool, registration, nil))

	var keyHash lcommon.PoolKeyHash
	copy(keyHash[:], poolKeyHash)

	// database.Database.GetPool never passes a nil txn down to the
	// sqlstore Store: when its own caller passes nil, it opens a real
	// read-only *sql.Tx (Database.Transaction(false) -> Store.
	// ReadTransaction) and passes that transaction's Metadata() handle
	// through instead (see database/pool.go's GetPool). That single
	// *sql.Tx is exactly what turns the unclosed outer cursor in
	// loadPoolAssociations into a connection-corrupting bug: nested
	// queries against *sql.DB safely borrow a different pooled connection,
	// but nested queries against one *sql.Tx share the same physical
	// connection, and MySQL/PostgreSQL cannot accept a new query while a
	// previous result set on that connection is still open. Reproduce that
	// exact call shape here instead of passing nil directly to Store.
	// GetPool, which -- unlike Database.GetPool -- resolves nil to the
	// *sql.DB pool and does not reproduce the bug.
	readTxn := store.ReadTransaction(context.Background())
	t.Cleanup(func() { _ = readTxn.Rollback() })

	// The first GetPool call is where loadPoolAssociations opens the
	// pool_registration cursor and (pre-fix) queries the owner table while
	// that cursor is still open, on the transaction's single connection.
	got, err := store.GetPool(keyHash, true, readTxn)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Registration, 1)
	require.Len(t, got.Registration[0].Owners, 1)

	// The regression: pre-fix, the transaction's connection is left
	// corrupted, and the very next read against the same transaction fails
	// with a driver-level protocol error (go-sql-driver/mysql's "busy
	// buffer" / "unexpected sequence nr", or an equivalent PostgreSQL
	// desync) instead of a clean result.
	for i := 0; i < 3; i++ {
		got, err := store.GetPool(keyHash, true, readTxn)
		require.NoErrorf(
			t,
			err,
			"GetPool call %d on the same read transaction",
			i,
		)
		require.NotNil(t, got)
		require.Len(t, got.Registration, 1)
		require.Len(t, got.Registration[0].Owners, 1)
	}
}
