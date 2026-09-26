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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresBackfillAnchorState and its MySQL twin run the statements the
// Mithril backfill issues at and below its anchor on the two non-default
// dialects: the import-baseline restore and the activity-only DRep write.
func TestPostgresBackfillAnchorState(t *testing.T) {
	dsn := os.Getenv("DINGO_POSTGRES_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
	}
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	schema := fmt.Sprintf("backfill_anchor_%d", time.Now().UnixNano())
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA "` + schema + `" CASCADE`)
		_ = admin.Close()
	})
	testBackfillAnchorState(
		t,
		"pgx",
		postgresDSNWithSearchPath(t, dsn, schema),
		"postgres",
		schema,
	)
}

func TestMySQLBackfillAnchorState(t *testing.T) {
	dsn := os.Getenv("DINGO_MYSQL_DSN")
	if dsn == "" {
		dsn = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
	}
	admin, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, admin.PingContext(context.Background()))
	database := fmt.Sprintf("backfill_anchor_%d", time.Now().UnixNano())
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE `" + database + "`")
		_ = admin.Close()
	})
	testBackfillAnchorState(
		t,
		"mysql",
		mysqlDSNWithDatabase(t, dsn, database),
		"mysql",
		database,
	)
}

func testBackfillAnchorState(
	t *testing.T,
	driver, dsn, dialectName, lockNamespace string,
) {
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
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.Exec(dialect.Rebind(query), args...)
		require.NoError(t, err)
	}

	const anchor = uint64(500)
	key := func(b byte) []byte { return bytes.Repeat([]byte{b}, 28) }
	stale, abstain, matching, genesis, replayOnly :=
		key(0x01), key(0x02), key(0x03), key(0x04), key(0x05)
	for _, account := range []*models.Account{
		{StakingKey: stale, AddedSlot: anchor, Reward: 700, Active: true},
		{
			StakingKey: abstain,
			AddedSlot:  anchor,
			DrepType:   models.DrepTypeAlwaysAbstain,
			Active:     true,
		},
		{StakingKey: matching, AddedSlot: anchor, Pool: key(0x10), Active: true},
		{StakingKey: genesis, Pool: key(0x11), Active: true},
	} {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: replayOnly,
		Pool:       key(0x12),
		AddedSlot:  100,
	}))
	exec(
		"UPDATE account SET pool = ?, drep = ?, drep_type = 0, added_slot = 300 "+
			"WHERE staking_key = ?",
		key(0x20), key(0x21), stale,
	)
	exec(
		"UPDATE account SET drep = ?, drep_type = 0, added_slot = 310 "+
			"WHERE staking_key = ?",
		key(0x22), abstain,
	)
	exec(
		"UPDATE account SET pool = ?, added_slot = 320 WHERE staking_key = ?",
		key(0x23), genesis,
	)

	restored, err := store.RestoreImportedAccountStates(anchor, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, restored)
	get := func(stakeKey []byte) *models.Account {
		t.Helper()
		account, err := store.GetAccountByCredential(0, stakeKey, true, nil)
		require.NoError(t, err)
		require.NotNil(t, account)
		return account
	}
	account := get(stale)
	assert.Empty(t, account.Pool)
	assert.Empty(t, account.Drep)
	assert.True(t, account.Active)
	assert.Equal(t, anchor, account.AddedSlot)
	assert.Equal(t, uint64(700), uint64(account.Reward))
	assert.Equal(t, models.DrepTypeAlwaysAbstain, get(abstain).DrepType)
	assert.Empty(t, get(abstain).Drep)
	assert.Equal(t, key(0x10), get(matching).Pool)
	assert.Equal(t, key(0x23), get(genesis).Pool)
	assert.Equal(t, key(0x12), get(replayOnly).Pool)
	restored, err = store.RestoreImportedAccountStates(anchor, nil)
	require.NoError(t, err)
	assert.Zero(t, restored)

	drepCred := key(0x30)
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			Credential:  drepCred,
			AddedSlot:   anchor,
			ExpiryEpoch: 533,
			Active:      true,
		},
		&models.RegistrationDrep{
			DrepCredential: drepCred,
			AddedSlot:      anchor,
			DepositAmount:  500_000_000,
		},
		nil,
	))
	require.NoError(t, store.RecordDRepActivityEpoch(0, drepCred, 510, nil))
	drep, err := store.GetDrepByCredential(0, drepCred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.Equal(t, uint64(533), drep.ExpiryEpoch)
	assert.Equal(t, uint64(510), drep.LastActivityEpoch)
	require.ErrorIs(
		t,
		store.RecordDRepActivityEpoch(0, key(0x31), 510, nil),
		models.ErrDrepActivityNotUpdated,
	)
}
