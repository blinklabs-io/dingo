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

package migrations_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// spentStakingEngineRows are inserted before v58: live and spent outputs, with
// and without a stake credential.
var spentStakingEngineRows = []struct {
	deletedSlot int64
	staking     []byte
}{
	{0, nil}, {0, []byte{1}}, {0, []byte{2}}, {7, []byte{1}}, {9, []byte{2}},
}

// runSpentStakingUpgrade migrates a fresh engine database to v54, loads the
// rows, optionally marks a deferred rebuild pending and drops the index as a
// bulk load would, then runs v58 and replays it. It returns the row digest
// taken before and after.
func runSpentStakingUpgrade(
	t *testing.T,
	db *sql.DB,
	dialect string,
	registry []migrations.Migration,
	pending bool,
	digest func() string,
) (string, string) {
	t.Helper()
	runner := func(registry []migrations.Migration) {
		require.NoError(t, (&migrations.Runner{
			DB:       db,
			Dialect:  dialect,
			Registry: registry,
			Locker:   migrations.NewProcessLocker(),
		}).Run(context.Background()))
	}
	runner(registry[:utxoSpentStakingIndexVersion-1])
	for i, row := range spentStakingEngineRows {
		var staking any
		if row.staking != nil {
			staking = row.staking
		}
		query := "INSERT INTO utxo (tx_id, output_idx, credential_tag, " +
			"staking_key, added_slot, deleted_slot, amount, payment_script) " +
			"VALUES (?, 0, 0, ?, 1, ?, '100', FALSE)"
		if dialect == "postgres" {
			query = "INSERT INTO utxo (tx_id, output_idx, credential_tag, " +
				"staking_key, added_slot, deleted_slot, amount, " +
				"payment_script) VALUES ($1, 0, 0, $2, 1, $3, '100', FALSE)"
		}
		_, err := db.Exec(query, []byte{byte(i)}, staking, row.deletedSlot)
		require.NoError(t, err)
	}
	if pending {
		query := "INSERT INTO sync_state (sync_key, value) VALUES (?, ?)"
		drop := "DROP INDEX idx_utxo_deleted_staking_amount ON utxo"
		if dialect == "postgres" {
			query = "INSERT INTO sync_state (sync_key, value) VALUES ($1, $2)"
			drop = "DROP INDEX idx_utxo_deleted_staking_amount"
		}
		_, err := db.Exec(query, deferred.SyncStateKey, deferred.SyncStateValue)
		require.NoError(t, err)
		_, err = db.Exec(drop)
		require.NoError(t, err)
	}
	before := digest()
	runner(registry)
	runner(registry)
	return before, digest()
}

func TestUtxoSpentStakingIndexUpgradeOnPostgres(t *testing.T) {
	t.Parallel()
	dsn := postgresEngineTestDSN(t)
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.Ping())
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)

	for _, pending := range []bool{false, true} {
		schema := fmt.Sprintf("dingo_v55_pg_%t_%d", pending, time.Now().UnixNano())
		_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = admin.Exec(`DROP SCHEMA IF EXISTS "` + schema + `" CASCADE`)
		})
		db, err := sql.Open("pgx", storagetest.PostgresDSNWithSearchPath(dsn, schema))
		require.NoError(t, err)
		defer db.Close()
		digest := func() string {
			var out string
			require.NoError(t, db.QueryRow(`SELECT string_agg(encode(tx_id, 'hex') || ':' ||
deleted_slot || ':' || amount || ':' || coalesce(encode(staking_key, 'hex'), ''),
',' ORDER BY id) FROM utxo`).Scan(&out))
			return out
		}
		before, after := runSpentStakingUpgrade(
			t, db, "postgres", registry, pending, digest,
		)
		require.Equal(t, before, after)

		var definition sql.NullString
		err = db.QueryRow(`SELECT indexdef FROM pg_indexes
WHERE schemaname = $1 AND indexname = 'idx_utxo_deleted_staking_amount'`,
			schema).Scan(&definition)
		if pending {
			require.ErrorIs(t, err, sql.ErrNoRows,
				"left to the deferred rebuild")
			continue
		}
		require.NoError(t, err)
		require.Contains(t, definition.String,
			"(deleted_slot, credential_tag, staking_key, amount)")
		require.Contains(t, definition.String, "WHERE (deleted_slot > 0)")
		var supply string
		require.NoError(t, db.QueryRow(`SELECT indexdef FROM pg_indexes
WHERE schemaname = $1 AND indexname = 'idx_utxo_deleted_payment_script'`,
			schema).Scan(&supply))
		require.NotContains(t, supply, "WHERE")
	}
}

func TestUtxoSpentStakingIndexUpgradeOnMySQL(t *testing.T) {
	t.Parallel()
	rootDSN := mysqlEngineTestRootDSN(t)
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	defer admin.Close()
	require.NoError(t, admin.Ping())
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)

	for _, pending := range []bool{false, true} {
		database := fmt.Sprintf("dingo_v55_my_%t_%d", pending, time.Now().UnixNano())
		_, err = admin.Exec("CREATE DATABASE `" + database + "`")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
		})
		cfg, err := mysqldriver.ParseDSN(rootDSN)
		require.NoError(t, err)
		cfg.DBName = database
		db, err := sql.Open("mysql", cfg.FormatDSN())
		require.NoError(t, err)
		defer db.Close()
		digest := func() string {
			var out string
			require.NoError(t, db.QueryRow(`SELECT GROUP_CONCAT(CONCAT(HEX(tx_id), ':',
deleted_slot, ':', amount, ':', COALESCE(HEX(staking_key), '')) ORDER BY id SEPARATOR ',')
FROM utxo`).Scan(&out))
			return out
		}
		before, after := runSpentStakingUpgrade(
			t, db, "mysql", registry, pending, digest,
		)
		require.Equal(t, before, after)

		rows, err := db.Query(`SELECT column_name FROM information_schema.statistics
WHERE table_schema = ? AND table_name = 'utxo'
  AND index_name = 'idx_utxo_deleted_staking_amount' ORDER BY seq_in_index`,
			database)
		require.NoError(t, err)
		var columns []string
		for rows.Next() {
			var column string
			require.NoError(t, rows.Scan(&column))
			columns = append(columns, column)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		// A pending rebuild still gets the MySQL index from the migration:
		// MySQL cannot skip a plain CREATE INDEX.
		require.Equal(t, []string{"deleted_slot"}, columns,
			strings.Join(columns, ","))
	}
}
