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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const utxoCredentialDictionaryVersion = 79

// legacyCredentialUtxo is one by-value utxo row of the schema before the
// credential dictionary.
type legacyCredentialUtxo struct {
	ID        int64
	Payment   []byte
	Staking   []byte
	Datum     []byte
	Tag       int64
	DeletedAt int64
}

// legacyCredentialAddressTx is one by-value address_transaction row.
type legacyCredentialAddressTx struct {
	ID      int64
	Payment []byte
	Staking []byte
}

func credentialKey(b byte) []byte {
	key := make([]byte, 28)
	for i := range key {
		key[i] = b
	}
	return key
}

func credentialDictionaryDB(
	t *testing.T,
	utxos []legacyCredentialUtxo,
	addressTxs []legacyCredentialAddressTx,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, utxoCredentialDictionaryVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, row := range utxos {
		txID := make([]byte, 32)
		txID[0] = byte(row.ID)
		_, err := db.Exec(`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`,
			row.ID, txID)
		require.NoError(t, err)
		_, err = db.Exec(`
INSERT INTO utxo (id, tx_id, output_idx, payment_key, staking_key,
                  credential_tag, datum_hash, added_slot, deleted_slot,
                  amount, payment_script)
VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, '5', ?)`,
			row.ID, row.ID, row.Payment, row.Staking, row.Tag, row.Datum,
			row.ID*10, row.DeletedAt, row.Tag == 1)
		require.NoError(t, err)
		// Rows that reference utxo(id) must survive the rebuild.
		address := []byte(fmt.Sprintf("addr_%d", row.ID))
		_, err = db.Exec(`INSERT INTO address (address) VALUES (?)`, address)
		require.NoError(t, err)
		_, err = db.Exec(`
INSERT INTO utxo_address (utxo_id, address_id)
SELECT ?, id FROM address WHERE address = ?`, row.ID, address)
		require.NoError(t, err)
		_, err = db.Exec(`
INSERT INTO utxo_reference_edge (tx_id, utxo_id) VALUES (?, ?)`,
			row.ID, row.ID)
		require.NoError(t, err)
	}
	for _, row := range addressTxs {
		_, err := db.Exec(`
INSERT INTO address_transaction (id, payment_key, staking_key, credential_tag,
                                 transaction_id, slot, tx_index, payment_script)
VALUES (?, ?, ?, 0, ?, ?, 0, FALSE)`,
			row.ID, row.Payment, row.Staking, row.ID, row.ID*10)
		require.NoError(t, err)
	}
	return db, path
}

func runUtxoCredentialDictionary(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[utxoCredentialDictionaryVersion-1]
	require.Equal(t, "utxo-credential-dictionary", migration.Name)
	require.NotNil(t, migration.Backfill)
	if batchSize > 0 {
		migration.BatchSize = batchSize
	}
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))
}

func tableColumns(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(
		`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table)
	require.NoError(t, err)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	return columns
}

func tableIndexes(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT name FROM sqlite_master
WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name`,
		table)
	require.NoError(t, err)
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

var credentialDictionaryUtxos = []legacyCredentialUtxo{
	{
		ID:      1,
		Payment: credentialKey(0xa1),
		Staking: credentialKey(0xb1),
		Datum:   credentialKey(0xd1),
	},
	{
		ID:        2,
		Payment:   credentialKey(0xa1),
		Staking:   credentialKey(0xb1),
		Tag:       1,
		DeletedAt: 40,
	},
	{
		ID:      4,
		Payment: credentialKey(0xa2),
		Staking: nil,
		Datum:   credentialKey(0xd1),
	},
	{ID: 7, Payment: nil, Staking: credentialKey(0xa1)},
	{
		ID:      8,
		Payment: credentialKey(0xa3),
		Staking: credentialKey(0xb2),
		Datum:   credentialKey(0xd2),
	},
	{ID: 9, Payment: []byte{}, Staking: []byte{}},
}

var credentialDictionaryAddressTxs = []legacyCredentialAddressTx{
	{ID: 1, Payment: credentialKey(0xa1), Staking: credentialKey(0xb1)},
	{ID: 3, Payment: credentialKey(0xa4), Staking: credentialKey(0xb1)},
	{ID: 5, Payment: credentialKey(0xa4), Staking: nil},
}

// TestUtxoCredentialDictionaryKeepsRowsAndHoldsEachHashOnce upgrades a
// database with rows spread over several batches and checks every row keeps
// its id, credentials, datum and kind, every distinct hash is held once
// whichever column or kind used it, and the by-value columns are gone.
func TestUtxoCredentialDictionaryKeepsRowsAndHoldsEachHashOnce(t *testing.T) {
	t.Parallel()
	for _, batchSize := range []int{0, 1, 2, 4, 5, 7} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, path := credentialDictionaryDB(
				t, credentialDictionaryUtxos, credentialDictionaryAddressTxs,
			)
			runUtxoCredentialDictionary(t, db, path, batchSize)

			// 0xa1 serves as a payment key, a stake key and an
			// address_transaction credential; it is one row.
			var distinct int
			require.NoError(t, db.QueryRow(
				`SELECT COUNT(*) FROM credential`).Scan(&distinct))
			require.Equal(t, 6, distinct)
			var duplicates int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM (SELECT hash FROM credential GROUP BY hash HAVING COUNT(*) > 1)`,
			).Scan(&duplicates))
			require.Zero(t, duplicates)

			require.Equal(t, []string{
				"id", "tx_id", "output_idx", "collateral_return_for_tx_id",
				"spent_at_tx_id", "payment_key_id", "staking_key_id",
				"credential_tag", "datum_id", "amount", "payment_script",
				"added_slot", "deleted_slot",
			}, tableColumns(t, db, "utxo"))
			require.Equal(t, []string{
				"id", "payment_key_id", "payment_script", "staking_key_id",
				"credential_tag", "transaction_id", "slot", "tx_index",
			}, tableColumns(t, db, "address_transaction"))

			for _, want := range credentialDictionaryUtxos {
				var (
					payment, staking, datum []byte
					tag, deleted, addedSlot int64
					script                  bool
				)
				require.NoError(t, db.QueryRow(`
SELECT pc.hash, sc.hash, d.hash, u.credential_tag, u.deleted_slot,
       u.added_slot, u.payment_script
FROM utxo u
LEFT JOIN credential pc ON pc.id = u.payment_key_id
LEFT JOIN credential sc ON sc.id = u.staking_key_id
LEFT JOIN datum d ON d.id = u.datum_id
WHERE u.id = ?`, want.ID).Scan(
					&payment, &staking, &datum, &tag, &deleted, &addedSlot,
					&script,
				))
				// An empty value is no credential.
				require.Equal(t, string(want.Payment), string(payment))
				require.Equal(t, string(want.Staking), string(staking))
				require.Equal(t, string(want.Datum), string(datum))
				require.Equal(t, want.Tag, tag)
				require.Equal(t, want.DeletedAt, deleted)
				require.Equal(t, want.ID*10, addedSlot)
				require.Equal(t, want.Tag == 1, script)
			}
			for _, want := range credentialDictionaryAddressTxs {
				var payment, staking []byte
				require.NoError(t, db.QueryRow(`
SELECT pc.hash, sc.hash FROM address_transaction a
LEFT JOIN credential pc ON pc.id = a.payment_key_id
LEFT JOIN credential sc ON sc.id = a.staking_key_id
WHERE a.id = ?`, want.ID).Scan(&payment, &staking))
				require.Equal(t, string(want.Payment), string(payment))
				require.Equal(t, string(want.Staking), string(staking))
			}

			// A datum hash that no transaction revealed is an unrevealed
			// row: an empty raw_datum.
			var datums, unrevealed int
			require.NoError(t, db.QueryRow(
				`SELECT COUNT(*), COALESCE(SUM(LENGTH(raw_datum) = 0), 0) FROM datum`,
			).Scan(&datums, &unrevealed))
			require.Equal(t, 2, datums)
			require.Equal(t, 2, unrevealed)

			require.Equal(t, []string{
				"idx_utxo_added_slot",
				"idx_utxo_collateral_return_for_tx_id",
				"idx_utxo_deleted_payment_script",
				"idx_utxo_deleted_staking_amount",
				"idx_utxo_payment_key",
				"idx_utxo_spent_at_tx_id",
				"idx_utxo_staking_deleted_amount",
				"idx_utxo_staking_key",
				"idx_utxo_tx_id_output_idx",
			}, tableIndexes(t, db, "utxo"))
			require.Equal(t, []string{
				"idx_addr_tx_payment",
				"idx_addr_tx_stake_position",
				"idx_address_transaction_slot",
				"idx_address_transaction_transaction_id",
			}, tableIndexes(t, db, "address_transaction"))

			var copies int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE name IN ('utxo_dictionary', 'address_transaction_dictionary')`,
			).Scan(&copies))
			require.Zero(t, copies, "the copy tables are renamed away")
		})
	}
}

// TestUtxoCredentialDictionaryKeepsChildRowsAndCascade pins that the rows that
// reference utxo(id) survive the rebuild, that the cascade from utxo still
// deletes them with foreign keys on, and that dictionary rows are never
// deleted.
func TestUtxoCredentialDictionaryKeepsChildRowsAndCascade(t *testing.T) {
	t.Parallel()
	db, path := credentialDictionaryDB(
		t, credentialDictionaryUtxos, credentialDictionaryAddressTxs,
	)
	runUtxoCredentialDictionary(t, db, path, 2)
	var addresses, edges int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM utxo_address`).Scan(&addresses))
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM utxo_reference_edge`).Scan(&edges))
	require.Equal(t, len(credentialDictionaryUtxos), addresses)
	require.Equal(t, len(credentialDictionaryUtxos), edges)

	_, err := db.Exec("PRAGMA foreign_keys = ON")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM utxo WHERE id = 1")
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM utxo_address`).Scan(&addresses))
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM utxo_reference_edge`).Scan(&edges))
	require.Equal(t, len(credentialDictionaryUtxos)-1, addresses)
	require.Equal(t, len(credentialDictionaryUtxos)-1, edges)
	var credentials int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM credential`).Scan(&credentials))
	require.Equal(t, 6, credentials, "dictionary rows are never deleted")
	var violations int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM pragma_foreign_key_check`).Scan(&violations))
	require.Zero(t, violations)
}

// TestUtxoCredentialDictionaryKeepsIssuedIds pins that the rebuilt tables do
// not issue an id the legacy tables already issued, even when the newest row
// was deleted.
func TestUtxoCredentialDictionaryKeepsIssuedIds(t *testing.T) {
	t.Parallel()
	db, path := credentialDictionaryDB(
		t, credentialDictionaryUtxos, credentialDictionaryAddressTxs,
	)
	_, err := db.Exec(`
UPDATE sqlite_sequence SET seq = 500 WHERE name IN ('utxo', 'address_transaction')`)
	require.NoError(t, err)
	runUtxoCredentialDictionary(t, db, path, 0)
	for _, table := range []string{"utxo", "address_transaction"} {
		var seq int64
		require.NoError(t, db.QueryRow(
			`SELECT seq FROM sqlite_sequence WHERE name = ?`, table,
		).Scan(&seq), table)
		require.Equal(t, int64(500), seq, table)
	}
}

func TestUtxoCredentialDictionaryOnEmptyTables(t *testing.T) {
	t.Parallel()
	db, path := credentialDictionaryDB(t, nil, nil)
	runUtxoCredentialDictionary(t, db, path, 0)
	require.Contains(t, tableColumns(t, db, "utxo"), "payment_key_id")
	require.Contains(
		t,
		tableColumns(t, db, "address_transaction"),
		"staking_key_id",
	)
	var credentials int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM credential`).Scan(&credentials))
	require.Zero(t, credentials)
}

// TestUtxoCredentialDictionaryRefusesAnInconsistentCopy pins the row-count
// reconciliation: a copy table holding a row the backfill did not copy must
// not replace the legacy table.
func TestUtxoCredentialDictionaryRefusesAnInconsistentCopy(t *testing.T) {
	t.Parallel()
	db, path := credentialDictionaryDB(
		t, credentialDictionaryUtxos, credentialDictionaryAddressTxs,
	)
	// The expand phase keeps a copy table that already exists.
	_, err := db.Exec(`
CREATE TABLE utxo_dictionary (id integer PRIMARY KEY AUTOINCREMENT,
 tx_id integer NOT NULL, output_idx integer NOT NULL,
 collateral_return_for_tx_id integer, spent_at_tx_id integer,
 payment_key_id integer, staking_key_id integer,
 credential_tag integer NOT NULL DEFAULT 0, datum_id integer, amount text,
 payment_script numeric, added_slot integer, deleted_slot integer)`)
	require.NoError(t, err)
	_, err = db.Exec(`
INSERT INTO utxo_dictionary (id, tx_id, output_idx) VALUES (999, 999, 0)`)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	err = runner.Run(context.Background())
	require.ErrorContains(t, err, "rebuild is incomplete")
	require.Contains(t, tableColumns(t, db, "utxo"), "payment_key",
		"the legacy table stays")
}
