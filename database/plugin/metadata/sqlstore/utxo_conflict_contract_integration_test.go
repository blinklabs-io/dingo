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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func TestPostgresCreateUtxoConflictContract(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	requireCreateUtxoConflictContract(
		t, newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLCreateUtxoConflictContract(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	requireCreateUtxoConflictContract(
		t, newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

// requireEdgeBackfillBoundsBatches upgrades to the input-edge migration with a
// two-row batch over one output named by five transactions, on the engine's
// own comparison of the composite edge cursor.
func requireEdgeBackfillBoundsBatches(
	t *testing.T,
	driver, dsn, dialectName string,
	dialect Dialect,
	registry []migrations.Migration,
	locker migrations.Locker,
) {
	t.Helper()
	const edgeMigration = 58
	db, err := OpenDB(driver, dsn, dialectName, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	run := func(reg []migrations.Migration) {
		require.NoError(t, (&migrations.Runner{
			DB: db, Dialect: dialectName, Registry: reg, Locker: locker,
		}).Run(ctx))
	}
	run(registry[:edgeMigration-1])

	table := dialect.QuoteIdentifier("transaction")
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.ExecContext(ctx, dialect.Rebind(query), args...)
		require.NoError(t, err)
	}
	exec(
		`INSERT INTO utxo (tx_id, output_idx, credential_tag, added_slot,
    deleted_slot, amount) VALUES (?, 0, 0, 1, 0, '1')`,
		bytes32(0xe0),
	)
	var utxoID int64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT MIN(id) FROM utxo",
	).Scan(&utxoID))
	const edges = 5
	for i := range edges {
		hash := bytes32(byte(0xe1 + i))
		exec("INSERT INTO "+table+" (hash, slot) VALUES (?, ?)", hash, i+1)
		exec(
			"INSERT INTO utxo_reference_input (utxo_id, transaction_hash) "+
				"VALUES (?, ?)",
			utxoID, hash,
		)
	}

	reg := append([]migrations.Migration(nil), registry[:edgeMigration]...)
	var largest int64
	backfill := reg[edgeMigration-1].Backfill
	reg[edgeMigration-1].BatchSize = 2
	reg[edgeMigration-1].Backfill = func(
		ctx context.Context,
		batch migrations.Batch,
	) (migrations.BatchResult, error) {
		result, err := backfill(ctx, batch)
		largest = max(largest, result.Rows)
		return result, err
	}
	run(reg)

	var copied int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM utxo_reference_edge",
	).Scan(&copied))
	require.Equal(t, edges, copied)
	require.LessOrEqual(t, largest, int64(2))
}

func TestPostgresUtxoInputEdgeBackfillBoundsBatches(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	requireEdgeBackfillBoundsBatches(
		t, "pgx", dsn, "postgres", PostgresDialect(), registry,
		integrationMigrationLocker("postgres", schema),
	)
}

func TestMySQLUtxoInputEdgeBackfillBoundsBatches(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	requireEdgeBackfillBoundsBatches(
		t, "mysql", dsn, "mysql", MySQLDialect(), registry,
		integrationMigrationLocker("mysql", database),
	)
}
