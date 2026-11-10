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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const identityMigrationVersion = 58

func identityHash(b byte) []byte {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = b
	}
	return hash
}

func identityUpgradeDB(t *testing.T) (*sql.DB, func(batchSize, upTo int)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	return db, func(batchSize, upTo int) {
		t.Helper()
		reg := append([]migrations.Migration(nil), registry[:upTo]...)
		if batchSize > 0 {
			reg[len(reg)-1].BatchSize = batchSize
		}
		runner := migrations.Runner{
			DB: db, Dialect: "sqlite", Registry: reg,
			Locker: migrations.NewFileLocker(path + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
}

// seedIdentityUpgradeRows writes the shapes a pre-identity database holds:
// applied transactions, outputs they produced, a collateral return, imported
// outputs with no producer row (one spent by a transaction that has a row,
// one by a hash with no row), and hash-keyed reference and collateral edges.
func seedIdentityUpgradeRows(t *testing.T, db *sql.DB) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.Exec(query, args...)
		require.NoError(t, err, query)
	}
	for i := 1; i <= 3; i++ {
		exec(
			"INSERT INTO `transaction` (hash, slot, block_index, fee, valid, block_hash) "+
				"VALUES (?, ?, ?, '5', 1, ?)",
			identityHash(byte(i)), int64(100+i), i, identityHash(0xB0),
		)
	}
	// Produced outputs of transactions 1 and 2, the second spent by 3.
	exec(
		"INSERT INTO utxo (transaction_id, tx_id, output_idx, added_slot, "+
			"deleted_slot, amount) VALUES (1, ?, 0, 101, 0, '10')",
		identityHash(1),
	)
	exec(
		"INSERT INTO utxo (transaction_id, tx_id, output_idx, added_slot, "+
			"deleted_slot, spent_at_tx_id, amount) "+
			"VALUES (2, ?, 0, 102, 200, ?, '20')",
		identityHash(2), identityHash(3),
	)
	// Collateral return of transaction 3.
	exec(
		"INSERT INTO utxo (collateral_return_for_tx_id, tx_id, output_idx, "+
			"added_slot, deleted_slot, amount) VALUES (3, ?, 4, 103, 0, '30')",
		identityHash(3),
	)
	// Imported outputs: producer 0xA1 has no transaction row.
	exec(
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, "+
			"amount) VALUES (?, 0, 1, 0, '40')", identityHash(0xA1),
	)
	exec(
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, "+
			"spent_at_tx_id, amount) VALUES (?, 1, 1, 300, ?, '50')",
		identityHash(0xA1), identityHash(1),
	)
	// Spender hash 0xEE has no transaction row.
	exec(
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, "+
			"spent_at_tx_id, amount) VALUES (?, 0, 1, 400, ?, '60')",
		identityHash(0xA2), identityHash(0xEE),
	)
	exec(
		"INSERT INTO utxo_reference_input (utxo_id, transaction_hash) "+
			"SELECT id, ? FROM utxo WHERE amount = '10'", identityHash(2),
	)
	exec(
		"INSERT INTO utxo_collateral_input (utxo_id, transaction_hash) "+
			"SELECT id, ? FROM utxo WHERE amount = '40'", identityHash(3),
	)
	// An edge naming a transaction with no row is dropped.
	exec(
		"INSERT INTO utxo_collateral_input (utxo_id, transaction_hash) "+
			"SELECT id, ? FROM utxo WHERE amount = '10'", identityHash(0xDD),
	)
}

func queryInt(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(query, args...).Scan(&n), query)
	return n
}

// TestUtxoTransactionIdentityUpgrade upgrades a populated previous-schema
// database in several small backfill batches and checks the new shape: the
// identity table holds every known hash, applied_transaction exactly the
// applied ones, and every output and edge is keyed by integer.
func TestUtxoTransactionIdentityUpgrade(t *testing.T) {
	t.Parallel()
	db, upgrade := identityUpgradeDB(t)
	upgrade(0, identityMigrationVersion-1)
	seedIdentityUpgradeRows(t, db)

	upgrade(2, identityMigrationVersion)

	// transaction is identity only; applied columns moved.
	require.Equal(t, 0, queryInt(t, db,
		"SELECT COUNT(*) FROM pragma_table_info('transaction') "+
			"WHERE name NOT IN ('id', 'hash')"))
	// 3 applied + imported producers 0xA1, 0xA2 + spender 0xEE.
	require.Equal(t, 6, queryInt(t, db, `SELECT COUNT(*) FROM "transaction"`))
	require.Equal(t, 3, queryInt(t, db, "SELECT COUNT(*) FROM applied_transaction"))
	require.Equal(t, 101, queryInt(t, db,
		`SELECT a.slot FROM applied_transaction a
JOIN "transaction" t ON t.id = a.tx_id WHERE t.hash = ?`, identityHash(1)))
	require.Equal(t, 0, queryInt(t, db,
		`SELECT COUNT(*) FROM applied_transaction a
JOIN "transaction" t ON t.id = a.tx_id WHERE t.hash IN (?, ?, ?)`,
		identityHash(0xA1), identityHash(0xA2), identityHash(0xEE)))

	// utxo has the integer shape and every row resolves its producer.
	for _, legacy := range []string{
		"transaction_id", "referenced_by_tx_id", "collateral_by_tx_id",
		"spent_by_tx_id", "producer_tx_id",
	} {
		require.Equal(t, 0, queryInt(t, db,
			"SELECT COUNT(*) FROM pragma_table_info('utxo') WHERE name = ?",
			legacy), legacy)
	}
	require.Equal(t, 6, queryInt(t, db, "SELECT COUNT(*) FROM utxo"))
	require.Equal(t, 6, queryInt(t, db,
		`SELECT COUNT(*) FROM utxo u JOIN "transaction" t ON t.id = u.tx_id`))
	require.Equal(t, 1, queryInt(t, db,
		`SELECT COUNT(*) FROM utxo WHERE collateral_return_for_tx_id = tx_id`))
	require.Equal(t, 3, queryInt(t, db,
		`SELECT COUNT(*) FROM utxo u JOIN "transaction" t
ON t.id = u.spent_at_tx_id WHERE t.hash IN (?, ?, ?)`,
		identityHash(1), identityHash(3), identityHash(0xEE)))
	require.Equal(t, 3, queryInt(t, db,
		"SELECT COUNT(*) FROM utxo WHERE spent_at_tx_id IS NOT NULL"))
	require.Equal(t, 2, queryInt(t, db,
		`SELECT COUNT(*) FROM utxo u JOIN "transaction" t ON t.id = u.tx_id
WHERE t.hash = ?`, identityHash(0xA1)))

	// Edges are integer keyed; the unresolvable one is gone.
	require.Equal(t, 1, queryInt(t, db,
		`SELECT COUNT(*) FROM utxo_reference_edge e JOIN "transaction" t
ON t.id = e.tx_id WHERE t.hash = ?`, identityHash(2)))
	require.Equal(t, 1, queryInt(t, db, "SELECT COUNT(*) FROM utxo_collateral_edge"))
	for _, legacy := range []string{"utxo_reference_input", "utxo_collateral_input"} {
		require.Equal(t, 0, queryInt(t, db,
			"SELECT COUNT(*) FROM sqlite_master WHERE name = ?", legacy))
	}

	// Every index exists and the foreign keys resolve.
	for _, name := range []string{
		"idx_utxo_tx_id_output_idx", "idx_utxo_collateral_return_for_tx_id",
		"idx_utxo_spent_at_tx_id", "idx_utxo_added_slot",
		"idx_utxo_payment_key", "idx_utxo_staking_key",
		"idx_utxo_staking_deleted_amount", "idx_utxo_deleted_staking_amount",
		"idx_utxo_deleted_payment_script", "idx_utxo_reference_edge_tx_id",
		"idx_utxo_collateral_edge_tx_id", "idx_transaction_hash",
		"idx_applied_transaction_slot", "idx_applied_transaction_block_hash",
	} {
		_, ok := indexSQL(t, db, name)
		require.True(t, ok, name)
	}
	_, err := db.Exec("PRAGMA foreign_keys = ON")
	require.NoError(t, err)
	require.Equal(t, 0, queryInt(t, db,
		"SELECT COUNT(*) FROM pragma_foreign_key_check"))
	// Autoincrement continues above every copied id.
	_, err = db.Exec(`INSERT INTO "transaction" (hash) VALUES (?)`, identityHash(0x77))
	require.NoError(t, err)
	require.Equal(t, 1, queryInt(t, db,
		`SELECT COUNT(*) FROM "transaction" WHERE id > 6 AND hash = ?`,
		identityHash(0x77)))
}

// TestUtxoTransactionIdentityUpgradeKeepsAutoincrementHighWater requires an
// id freed before the upgrade to stay unused after it.
func TestUtxoTransactionIdentityUpgradeKeepsAutoincrementHighWater(t *testing.T) {
	t.Parallel()
	db, upgrade := identityUpgradeDB(t)
	upgrade(0, identityMigrationVersion-1)
	seedIdentityUpgradeRows(t, db)
	// A rolled-back transaction leaves the autoincrement counter above the
	// highest remaining id.
	_, err := db.Exec("UPDATE sqlite_sequence SET seq = 50 WHERE name = 'transaction'")
	require.NoError(t, err)

	upgrade(0, identityMigrationVersion)

	_, err = db.Exec(`INSERT INTO "transaction" (hash) VALUES (?)`, identityHash(0x77))
	require.NoError(t, err)
	require.Equal(t, 1, queryInt(t, db,
		`SELECT COUNT(*) FROM "transaction" WHERE id > 50 AND hash = ?`,
		identityHash(0x77)))
}
