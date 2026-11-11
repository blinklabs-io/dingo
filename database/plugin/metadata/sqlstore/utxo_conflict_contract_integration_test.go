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

// requireIdentityBackfillBoundsBatches upgrades a database holding hash-keyed
// outputs, edges and applied transactions to the identity schema with a
// two-row batch, on the engine's own comparison of the phase cursors, and
// checks every batch stays within the bound and every row resolves.
func requireIdentityBackfillBoundsBatches(
	t *testing.T,
	driver, dsn, dialectName string,
	dialect Dialect,
	registry []migrations.Migration,
	locker migrations.Locker,
) {
	t.Helper()
	const identityMigration = 60
	db, err := OpenDB(driver, dsn, dialectName, false)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	run := func(reg []migrations.Migration) {
		require.NoError(t, (&migrations.Runner{
			DB: db, Dialect: dialectName, Registry: reg, Locker: locker,
		}).Run(ctx))
	}
	run(registry[:identityMigration-1])

	table := dialect.QuoteIdentifier("transaction")
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.ExecContext(ctx, dialect.Rebind(query), args...)
		require.NoError(t, err, query)
	}
	count := func(query string, args ...any) int {
		t.Helper()
		var n int
		require.NoError(t, db.QueryRowContext(
			ctx, dialect.Rebind(query), args...,
		).Scan(&n), query)
		return n
	}
	const applied = 5
	for i := range applied {
		hash := bytes32(byte(0xe1 + i))
		exec(
			"INSERT INTO "+table+" (hash, slot, block_index, fee, valid) "+
				"VALUES (?, ?, ?, '1', TRUE)",
			hash, i+1, i,
		)
		exec(
			`INSERT INTO utxo (transaction_id, tx_id, output_idx,
    credential_tag, added_slot, deleted_slot, amount)
SELECT id, hash, 0, 0, slot, 0, '1' FROM `+table+` WHERE hash = ?`,
			hash,
		)
	}
	// Imported outputs of one producer without a transaction row, and one
	// output referenced by every applied transaction.
	for i := range 3 {
		exec(
			`INSERT INTO utxo (tx_id, output_idx, credential_tag, added_slot,
    deleted_slot, amount) VALUES (?, ?, 0, 1, 0, '1')`,
			bytes32(0xa1), i,
		)
	}
	var utxoID int64
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT MIN(id) FROM utxo",
	).Scan(&utxoID))
	for i := range applied {
		exec(
			"INSERT INTO utxo_reference_input (utxo_id, transaction_hash) "+
				"VALUES (?, ?)",
			utxoID, bytes32(byte(0xe1+i)),
		)
	}

	reg := append([]migrations.Migration(nil), registry[:identityMigration]...)
	var largest int64
	backfill := reg[identityMigration-1].Backfill
	reg[identityMigration-1].BatchSize = 2
	reg[identityMigration-1].Backfill = func(
		ctx context.Context,
		batch migrations.Batch,
	) (migrations.BatchResult, error) {
		result, err := backfill(ctx, batch)
		largest = max(largest, result.Rows)
		return result, err
	}
	run(reg)

	require.LessOrEqual(t, largest, int64(2))
	require.Equal(t, applied+1, count("SELECT COUNT(*) FROM "+table))
	require.Equal(t, applied, count("SELECT COUNT(*) FROM applied_transaction"))
	require.Equal(t, applied+3, count("SELECT COUNT(*) FROM utxo"))
	require.Equal(t, applied+3, count(
		"SELECT COUNT(*) FROM utxo u JOIN "+table+" t ON t.id = u.tx_id"))
	require.Equal(t, applied, count("SELECT COUNT(*) FROM utxo_reference_edge"))
	_, err = db.ExecContext(ctx, "SELECT slot FROM "+table)
	require.Error(t, err, "transaction keeps only the identity columns")
}

func TestPostgresUtxoIdentityBackfillBoundsBatches(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	registry, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	requireIdentityBackfillBoundsBatches(
		t, "pgx", dsn, "postgres", PostgresDialect(), registry,
		integrationMigrationLocker("postgres", schema),
	)
}

func TestMySQLUtxoIdentityBackfillBoundsBatches(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	registry, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	requireIdentityBackfillBoundsBatches(
		t, "mysql", dsn, "mysql", MySQLDialect(), registry,
		integrationMigrationLocker("mysql", database),
	)
}
