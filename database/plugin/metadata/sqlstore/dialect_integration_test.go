//go:build dingo_db_integration

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func TestPostgresSQLStoreIntegration(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("sqlstore_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	dsn += "&options=-csearch_path%3D" + schema
	testSQLStoreIntegration(t, "pgx", dsn, "postgres")
}

func TestMySQLSQLStoreIntegration(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("sqlstore_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	dsn = strings.Replace(dsn, "/dingo_test?", "/"+database+"?", 1)
	testSQLStoreIntegration(t, "mysql", dsn, "mysql")
}

func testSQLStoreIntegration(t *testing.T, driver, dsn, dialectName string) {
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
		locker = migrations.NewAdvisoryLocker("postgres", 0x64696e676f6d6574, time.Second)
	case "mysql":
		dialect = MySQLDialect()
		registry, err = migrations.MySQLRegistry()
		locker = migrations.NewAdvisoryLocker("mysql", 0x64696e676f6d6574, time.Second)
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

	txn := store.Transaction()
	require.NoError(t, store.SetCommitTimestamp(42, txn))
	require.NoError(t, store.SetNetworkState(11, 22, 33, txn))
	require.NoError(t, txn.Commit())
	timestamp, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, int64(42), timestamp)
	state, err := store.GetNetworkState(nil)
	require.NoError(t, err)
	require.Equal(t, types.Uint64(11), state.Treasury)
	require.Equal(t, types.Uint64(22), state.Reserves)
	require.Equal(t, uint64(33), state.Slot)

	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     "integration",
	}))
	settings, err := store.GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "integration", settings.Network)

	account := &models.Account{
		StakingKey:    []byte{1, 2, 3},
		CredentialTag: 0,
		AddedSlot:     1,
		CreatedSlot:   1,
		Active:        true,
	}
	require.NoError(t, store.ImportAccount(account, nil))
	require.NotZero(t, account.ID)
	loaded, err := store.GetAccountByCredential(0, account.StakingKey, false, nil)
	require.NoError(t, err)
	require.Equal(t, account.ID, loaded.ID)
}
