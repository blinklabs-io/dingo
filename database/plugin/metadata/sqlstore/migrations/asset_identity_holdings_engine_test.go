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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/storagetest"
	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// assetSplitEngine is one real server the asset split runs against.
type assetSplitEngine struct {
	dialect  string
	db       *sql.DB
	registry []migrations.Migration
	// placeholder renders the nth bind parameter.
	placeholder func(n int) string
	// tableExists reports whether the named table is in the test schema.
	tableExists func(t *testing.T, name string) bool
	indexExists func(t *testing.T, table, index string) bool
}

func (e *assetSplitEngine) query(q string) string {
	var out strings.Builder
	n := 0
	for _, r := range q {
		if r != '?' {
			out.WriteRune(r)
			continue
		}
		n++
		out.WriteString(e.placeholder(n))
	}
	return out.String()
}

func newPostgresAssetSplitEngine(t *testing.T) *assetSplitEngine {
	t.Helper()
	dsn := postgresEngineTestDSN(t)
	schema := "dingo_v60_split"
	admin, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping())
	_, err = admin.Exec(`DROP SCHEMA IF EXISTS "` + schema + `" CASCADE`)
	require.NoError(t, err)
	_, err = admin.Exec(`CREATE SCHEMA "` + schema + `"`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA IF EXISTS "` + schema + `" CASCADE`)
	})
	db, err := sql.Open(
		"pgx",
		storagetest.PostgresDSNWithSearchPath(dsn, schema),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	return &assetSplitEngine{
		dialect:     "postgres",
		db:          db,
		registry:    registry,
		placeholder: func(n int) string { return fmt.Sprintf("$%d", n) },
		tableExists: func(t *testing.T, name string) bool {
			var count int
			require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.tables
WHERE table_schema = $1 AND table_name = $2`, schema, name).Scan(&count))
			return count > 0
		},
		indexExists: func(t *testing.T, table, index string) bool {
			var count int
			require.NoError(t, db.QueryRow(`
SELECT count(*) FROM pg_indexes
WHERE schemaname = $1 AND tablename = $2 AND indexname = $3`,
				schema, table, index).Scan(&count))
			return count > 0
		},
	}
}

func newMySQLAssetSplitEngine(t *testing.T) *assetSplitEngine {
	t.Helper()
	rootDSN := mysqlEngineTestRootDSN(t)
	database := "dingo_v60_split"
	admin, err := sql.Open("mysql", rootDSN)
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close() })
	require.NoError(t, admin.Ping())
	_, err = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
	require.NoError(t, err)
	_, err = admin.Exec("CREATE DATABASE `" + database + "`")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + database + "`")
	})
	cfg, err := mysqldriver.ParseDSN(rootDSN)
	require.NoError(t, err)
	cfg.DBName = database
	cfg.MultiStatements = false
	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	return &assetSplitEngine{
		dialect:     "mysql",
		db:          db,
		registry:    registry,
		placeholder: func(int) string { return "?" },
		tableExists: func(t *testing.T, name string) bool {
			var count int
			require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.tables
WHERE table_schema = ? AND table_name = ?`, database, name).Scan(&count))
			return count > 0
		},
		indexExists: func(t *testing.T, table, index string) bool {
			var count int
			require.NoError(t, db.QueryRow(`
SELECT count(*) FROM information_schema.statistics
WHERE table_schema = ? AND table_name = ? AND index_name = ?`,
				database, table, index).Scan(&count))
			return count > 0
		},
	}
}

func (e *assetSplitEngine) run(t *testing.T, registry []migrations.Migration) {
	t.Helper()
	runner := migrations.Runner{
		DB:       e.db,
		Dialect:  e.dialect,
		Registry: registry,
		Locker:   migrations.NewProcessLocker(),
	}
	require.NoError(t, runner.Run(context.Background()))
}

// exerciseAssetSplit upgrades a database holding legacy asset rows, then
// replays the expand phase of the finished migration, and checks the data, the
// dropped table and the new index on a real server.
func exerciseAssetSplit(t *testing.T, e *assetSplitEngine) {
	t.Helper()
	e.run(t, e.registry[:assetSplitVersion-1])
	for id := 1; id <= 3; id++ {
		txID := make([]byte, 32)
		txID[0] = byte(id)
		_, err := e.db.Exec(e.query(`
INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 1, 0, '1')`), id, txID)
		require.NoError(t, err)
	}
	shared, other := assetSplitPolicy(0x01), assetSplitPolicy(0x02)
	insertLegacy := func(row legacyAssetRow) error {
		_, err := e.db.Exec(e.query(`
INSERT INTO asset (name, policy_id, fingerprint, utxo_id, amount)
VALUES (?, ?, ?, ?, ?)`), row.name, row.policy, []byte("fp"), row.utxo,
			row.amount)
		return err
	}
	for _, row := range []legacyAssetRow{
		{1, shared, []byte("tok"), "5"},
		{2, shared, []byte("tok"), "6"},
		{1, shared, []byte{}, "1"},
		// A NULL name is a distinct key to the legacy unique index but
		// the same asset as the empty name, so the split merges it into
		// the row above.
		{1, shared, nil, "1"},
		{2, other, nil, "2"},
		{3, nil, []byte("tok"), "007"},
		{3, shared, []byte("dup"), "9"},
	} {
		require.NoError(t, insertLegacy(row))
	}
	// The legacy unique key rejects an exact duplicate on both engines.
	require.Error(t, insertLegacy(legacyAssetRow{3, shared, []byte("dup"), "9"}))

	e.run(t, e.registry)
	require.False(t, e.tableExists(t, "asset"), "the legacy table is dropped")
	require.True(t, e.tableExists(t, "asset_identity"))
	require.True(t, e.tableExists(t, "asset_holding"))
	require.True(
		t,
		e.indexExists(t, "asset_holding", "idx_asset_holding_asset_id"),
	)

	holdings := func() []string {
		rows, err := e.db.Query(`
SELECT i.policy_id, i.name, h.utxo_id, h.amount
FROM asset_holding h JOIN asset_identity i ON i.id = h.asset_id`)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		var out []string
		for rows.Next() {
			var policy, name []byte
			var utxo int64
			var amount sql.NullString
			require.NoError(t, rows.Scan(&policy, &name, &utxo, &amount))
			out = append(
				out,
				fmt.Sprintf("%x/%x/%d/%s", policy, name, utxo, amount.String),
			)
		}
		require.NoError(t, rows.Err())
		return out
	}
	got := holdings()
	require.ElementsMatch(t, []string{
		fmt.Sprintf("%x/%x/1/5", shared, "tok"),
		fmt.Sprintf("%x/%x/2/6", shared, "tok"),
		fmt.Sprintf("%x/%x/1/1", shared, ""),
		fmt.Sprintf("%x/%x/2/2", other, ""),
		fmt.Sprintf("%x/%x/3/007", "", "tok"),
		fmt.Sprintf("%x/%x/3/9", shared, "dup"),
	}, got)

	// A crash between the contract phase and its completion replays every
	// earlier phase against a database whose legacy table is already gone.
	_, err := e.db.Exec(e.query(`
UPDATE schema_migrations SET phase = 'expand', dirty = ?, completed_at = NULL
WHERE version = ?`), true, assetSplitVersion)
	require.NoError(t, err)
	e.run(t, e.registry)
	require.ElementsMatch(t, got, holdings())
}

func TestAssetIdentityHoldingsAppliesOnPostgres(t *testing.T) {
	t.Parallel()
	exerciseAssetSplit(t, newPostgresAssetSplitEngine(t))
}

func TestAssetIdentityHoldingsAppliesOnMySQL(t *testing.T) {
	t.Parallel()
	exerciseAssetSplit(t, newMySQLAssetSplitEngine(t))
}
