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

// newAssetIntegrationStore opens a Store on a fresh schema of the named
// engine, migrated through the whole registry.
func newAssetIntegrationStore(t *testing.T, engine string) *Store {
	t.Helper()
	name := fmt.Sprintf("assets_%d", time.Now().UnixNano())
	var (
		driver, dsn string
		dialect     Dialect
		registry    []migrations.Migration
		err         error
	)
	switch engine {
	case "postgres":
		base := os.Getenv("DINGO_POSTGRES_DSN")
		if base == "" {
			base = "postgres://postgres:dingo@127.0.0.1:55432/dingo_test?sslmode=disable"
		}
		admin, openErr := sql.Open("pgx", base)
		require.NoError(t, openErr)
		require.NoError(t, admin.PingContext(context.Background()))
		_, err = admin.Exec(`CREATE SCHEMA "` + name + `"`)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = admin.Exec(`DROP SCHEMA "` + name + `" CASCADE`)
			_ = admin.Close()
		})
		driver, dsn = "pgx", postgresDSNWithSearchPath(t, base, name)
		dialect = PostgresDialect()
		registry, err = migrations.PostgresRegistry()
	case "mysql":
		base := os.Getenv("DINGO_MYSQL_DSN")
		if base == "" {
			base = "root:dingo@tcp(127.0.0.1:53306)/dingo_test?parseTime=true"
		}
		admin, openErr := sql.Open("mysql", base)
		require.NoError(t, openErr)
		require.NoError(t, admin.PingContext(context.Background()))
		_, err = admin.Exec("CREATE DATABASE `" + name + "`")
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = admin.Exec("DROP DATABASE `" + name + "`")
			_ = admin.Close()
		})
		driver, dsn = "mysql", mysqlDSNWithDatabase(t, base, name)
		dialect = MySQLDialect()
		registry, err = migrations.MySQLRegistry()
	}
	require.NoError(t, err)
	db, err := OpenDB(driver, dsn, engine, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         dialect,
		Migrations:      registry,
		MigrationLocker: integrationMigrationLocker(engine, name),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	require.NoError(t, store.Start(context.Background()))
	return store
}

// TestAssetHoldingsRoundTripOnEngines writes the parity scenario through the
// per-row, batched and import paths on PostgreSQL and MySQL and requires every
// read site to answer as SQLite does, so the identity get-or-create, the
// holding insert's casts and conflict rewrites, and the joined reads are
// exercised on each real backend.
func TestAssetHoldingsRoundTripOnEngines(t *testing.T) {
	t.Parallel()
	reference := newMigratedSQLiteStore(t)
	for _, path := range parityPaths() {
		if path.name == "per-row" {
			path.apply(t, reference, false)
		}
	}
	want := paritySnapshot(t, reference, false, true)

	for _, engine := range []string{"postgres", "mysql"} {
		for _, path := range parityPaths() {
			if path.coreOnly {
				continue
			}
			t.Run(engine+"/"+path.name, func(t *testing.T) {
				t.Parallel()
				store := newAssetIntegrationStore(t, engine)
				path.apply(t, store, false)
				got := paritySnapshot(t, store, false, path.ledgerBacked)
				if path.ledgerBacked {
					require.Equal(t, want, got)
				} else {
					require.Equal(t, stripTransactions(want), got)
				}
				held, err := store.AssetHeld(
					lcommon.NewBlake2b224(parityPolicy(1)), []byte("tok"), nil,
				)
				require.NoError(t, err)
				require.True(t, held)
				held, err = store.AssetHeld(
					lcommon.NewBlake2b224(parityPolicy(9)), []byte("none"), nil,
				)
				require.NoError(t, err)
				require.False(t, held)
				var identities int
				require.NoError(t, store.writeDB.QueryRow(
					`SELECT COUNT(*) FROM asset_identity`,
				).Scan(&identities))
				require.Equal(t, 5, identities)
			})
		}
	}
}

// TestConcurrentAssetIdentityCreationOnPostgres flushes the same new assets
// from several transactions at once. Each must end with a holding for every
// asset and the identities must stay unique: the losing writers resolve the
// identity the winner committed.
//
// MySQL is exercised by the sequential case below instead: InnoDB aborts one
// of several transactions that insert overlapping unique keys at once with a
// deadlock, whatever the statement form, and only a retry of the whole
// transaction recovers. The node runs one metadata writer.
func TestConcurrentAssetIdentityCreationOnPostgres(t *testing.T) {
	t.Parallel()
	exerciseAssetIdentityWriters(t, "postgres", true)
}

func TestSequentialAssetIdentityCreationOnMySQL(t *testing.T) {
	t.Parallel()
	exerciseAssetIdentityWriters(t, "mysql", false)
}

func exerciseAssetIdentityWriters(
	t *testing.T,
	engine string,
	concurrent bool,
) {
	t.Helper()
	const writers, assets = 6, 40
	store := newAssetIntegrationStore(t, engine)
	for id := 1; id <= writers; id++ {
		_, err := store.writeDB.Exec(store.dialect.Rebind(`
INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 1, 0, '1')`), id, []byte{0xaa, byte(id)})
		require.NoError(t, err)
	}
	write := func(writer int) error {
		tx, err := store.writeDB.BeginTx(t.Context(), nil)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		rows := make([][]any, 0, assets)
		for asset := range assets {
			rows = append(rows, []any{
				int64(writer), parityPolicy(byte(asset)),
				[]byte("n"), validString("1"),
			})
		}
		db := newDialectQueryer(tx, store.dialect.Name())
		if err := flushAssetHoldings(
			t.Context(), db, rows, store.dialect.ParameterLimit(),
		); err != nil {
			return err
		}
		return tx.Commit()
	}
	if concurrent {
		start := make(chan struct{})
		errs := make(chan error, writers)
		for writer := 1; writer <= writers; writer++ {
			go func() {
				<-start
				errs <- write(writer)
			}()
		}
		close(start)
		for range writers {
			require.NoError(t, <-errs)
		}
	} else {
		for writer := 1; writer <= writers; writer++ {
			require.NoError(t, write(writer))
		}
	}
	require.Equal(t, assets, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_identity`))
	require.Equal(t, writers*assets, countRows(t, store.writeDB,
		`SELECT COUNT(*) FROM asset_holding`))
}
