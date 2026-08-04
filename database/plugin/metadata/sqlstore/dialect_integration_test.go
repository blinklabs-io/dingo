//go:build dingo_db_integration

package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	mysqldriver "github.com/go-sql-driver/mysql"
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
	testSQLStoreIntegration(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
	)
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
	testSQLStoreIntegration(
		t,
		"mysql",
		mysqlDSNWithDatabase(t, dsn, database),
		"mysql",
	)
}

func TestPostgresSQLStoreAdoptsUnversionedSchema(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("sqlstore_adopt_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	testSQLStoreAdoptionIntegration(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
	)
}

func TestMySQLSQLStoreAdoptsUnversionedSchema(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("sqlstore_adopt_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	testSQLStoreAdoptionIntegration(
		t,
		"mysql",
		mysqlDSNWithDatabase(t, dsn, database),
		"mysql",
	)
}

func postgresDSNWithSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("options", "-csearch_path="+schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func mysqlDSNWithDatabase(t *testing.T, dsn, database string) string {
	t.Helper()
	parsed, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	parsed.DBName = database
	return parsed.FormatDSN()
}

func testSQLStoreAdoptionIntegration(t *testing.T, driver, dsn, dialectName string) {
	t.Helper()
	db, err := OpenDB(driver, dsn, dialectName)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	var registry []migrations.Migration
	switch dialectName {
	case "postgres":
		registry, err = migrations.PostgresRegistry()
	case "mysql":
		registry, err = migrations.MySQLRegistry()
	default:
		t.Fatalf("unsupported adoption dialect %q", dialectName)
	}
	require.NoError(t, err)
	for _, statement := range registry[0].SQL[dialectName].Expand {
		_, err = db.Exec(statement)
		require.NoError(t, err)
	}
	// The association table was introduced by the database/sql store. Verify
	// adoption recreates it when opening a legacy schema that predates it.
	_, err = db.Exec("DROP TABLE utxo_reference_input")
	require.NoError(t, err)
	var dialect Dialect
	var locker migrations.Locker
	if dialectName == "postgres" {
		dialect = PostgresDialect()
		locker = migrations.NewAdvisoryLocker("postgres", 0x64696e676f6d6574, time.Second)
	} else {
		dialect = MySQLDialect()
		locker = migrations.NewAdvisoryLocker("mysql", 0x64696e676f6d6574, time.Second)
	}
	store, err := New(Config{
		WriteDB:         db,
		ReadDB:          db,
		Dialect:         dialect,
		Migrations:      registry,
		MigrationLocker: locker,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.Start(context.Background()))
	require.True(t, store.Ready())
	var version int
	require.NoError(t, db.QueryRow("SELECT version FROM schema_migrations WHERE version = 1").Scan(&version))
	require.Equal(t, 1, version)
	var associationTable string
	require.NoError(t, db.QueryRow(
		"SELECT table_name FROM information_schema.tables WHERE table_name = 'utxo_reference_input' LIMIT 1",
	).Scan(&associationTable))
	require.Equal(t, "utxo_reference_input", associationTable)
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
	// Simulate a legacy singleton row whose network was left empty.  The
	// MySQL duplicate no-op upsert must still run the conditional backfill even
	// when CLIENT_FOUND_ROWS makes the insert report one affected row.
	_, err = db.Exec(dialect.Rebind("UPDATE node_settings SET network = '' WHERE id = 1"))
	require.NoError(t, err)
	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     "legacy-fixed",
	}))
	settings, err = store.GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, "legacy-fixed", settings.Network)
	checkpoint := &models.BackfillCheckpoint{
		Phase:      "integration",
		LastSlot:   7,
		TotalSlots: 11,
		StartedAt:  time.UnixMilli(100),
		UpdatedAt:  time.UnixMilli(200),
	}
	require.NoError(t, store.SetBackfillCheckpoint(checkpoint, nil))
	require.NotZero(t, checkpoint.ID)
	loadedCheckpoint, err := store.GetBackfillCheckpoint("integration", nil)
	require.NoError(t, err)
	require.Equal(t, checkpoint.ID, loadedCheckpoint.ID)
	require.Equal(t, checkpoint.LastSlot, loadedCheckpoint.LastSlot)

	account := &models.Account{
		StakingKey:    []byte{1, 2, 3},
		CredentialTag: 0,
		Pool:          []byte{4, 5, 6},
		AddedSlot:     1,
		CreatedSlot:   1,
		Reward:        types.Uint64(9),
		Active:        true,
	}
	require.NoError(t, store.ImportAccount(account, nil))
	require.NotZero(t, account.ID)
	loaded, err := store.GetAccountByCredential(0, account.StakingKey, false, nil)
	require.NoError(t, err)
	require.Equal(t, account.ID, loaded.ID)
	_, err = db.Exec(dialect.Rebind(`
INSERT INTO reward_live_stake (
 pool_key_hash, staking_key, credential_tag, utxo_stake, reward_stake,
 total_stake, registered, pool_delegation_slot, pool_delegation_block_index,
 pool_delegation_cert_index, updated_slot, calculation_version
) VALUES (?, ?, 0, ?, ?, ?, TRUE, 1, 0, 0, 1, ?)`),
		account.Pool, account.StakingKey, "9", "0", "9",
		models.RewardStakeCalculationVersion,
	)
	require.NoError(t, err)
	stakes, delegators, err := store.GetStakeByPools([][]byte{account.Pool}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(9), stakes[string(account.Pool)])
	require.Equal(t, uint64(1), delegators[string(account.Pool)])
	require.NoError(t, store.RebuildRewardLiveStake(2, nil))
	inputs, err := store.GetLiveStakeInputsForPools([][]byte{account.Pool}, 0, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, types.Uint64(9), inputs[0].Stake)

	outputs := make([]*models.RewardAccountOutput, 120)
	for index := range outputs {
		outputs[index] = &models.RewardAccountOutput{
			Epoch:       uint64(index),
			StakingKey:  []byte{0x40, byte(index)},
			PoolKeyHash: []byte{0x50, byte(index)},
			RewardType:  "member",
			Amount:      types.Uint64(index + 1),
			Spendable:   true,
		}
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	for _, output := range outputs {
		require.NotZero(t, output.ID)
	}

	// Exercise the shared RETURNING adapter against a reserved table name and
	// a reserved column name.  MySQL must quote both identifiers even when
	// ANSI_QUOTES is disabled, and the duplicate path must return the same ID
	// without a second pooled connection carrying LAST_INSERT_ID state.
	queryer := newDialectQueryer(db, dialectName)
	transactionHash := []byte{0xa0, 0xb0, 0xc0}
	transactionQuery := `INSERT INTO "transaction" (
 hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
 block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hash) DO UPDATE SET block_hash = excluded.block_hash
RETURNING id`
	var transactionID int64
	require.NoError(t, queryer.QueryRowContext(context.Background(), transactionQuery,
		transactionHash, []byte{0xd0}, nil, int64(100), 0, "0", "0", "0", 0, true,
	).Scan(&transactionID))
	require.NotZero(t, transactionID)
	firstTransactionID := transactionID
	require.NoError(t, queryer.QueryRowContext(context.Background(), transactionQuery,
		transactionHash, []byte{0xe0}, nil, int64(100), 0, "0", "0", "0", 0, true,
	).Scan(&transactionID))
	require.Equal(t, firstTransactionID, transactionID)
	var redeemerCount int
	_, err = queryer.ExecContext(context.Background(), `
INSERT INTO redeemer (
		data, transaction_id, ex_units_memory, ex_units_cpu, "index", tag
) VALUES (?, ?, ?, ?, ?, ?)`,
		[]byte{0xf0}, transactionID, int64(1), int64(2), int64(0), int64(0),
	)
	require.NoError(t, err)
	require.NoError(t, queryer.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM redeemer WHERE transaction_id = ?`, transactionID,
	).Scan(&redeemerCount))
	require.Equal(t, 1, redeemerCount)

	// The deferred-index lifecycle is also shared across dialects.  In
	// particular, MySQL requires `DROP INDEX ... ON table` and a non-IF-NOT-
	// EXISTS CREATE form, while sync_state has no synthetic id column.
	require.NoError(t, store.DropDeferredIndexes())
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)
	require.NoError(t, store.BuildDeferredIndexes())
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
}
