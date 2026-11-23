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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// seedCredentialDictionary migrates to the version before the credential
// dictionary and inserts by-value utxo and address_transaction rows.
func seedCredentialDictionary(t *testing.T, e *assetSplitEngine) {
	t.Helper()
	e.run(t, e.registry[:utxoCredentialDictionaryVersion-1])
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := e.db.Exec(e.query(query), args...)
		require.NoError(t, err)
	}
	for _, row := range credentialDictionaryUtxos {
		txID := make([]byte, 32)
		txID[0] = byte(row.ID)
		exec(`INSERT INTO transaction (id, hash) VALUES (?, ?)`, row.ID, txID)
		exec(`
INSERT INTO utxo (id, tx_id, output_idx, payment_key, staking_key,
                  credential_tag, datum_hash, added_slot, deleted_slot, amount,
                  payment_script)
VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, '5', ?)`,
			row.ID, row.ID, row.Payment, row.Staking, row.Tag, row.Datum,
			row.ID*10, row.DeletedAt, row.Tag == 1)
		address := []byte(fmt.Sprintf("addr_%d", row.ID))
		exec(`INSERT INTO address (address) VALUES (?)`, address)
		exec(`
INSERT INTO utxo_address (utxo_id, address_id)
SELECT ?, id FROM address WHERE address = ?`, row.ID, address)
		exec(`INSERT INTO utxo_reference_edge (tx_id, utxo_id) VALUES (?, ?)`,
			row.ID, row.ID)
	}
	for _, row := range credentialDictionaryAddressTxs {
		exec(`
INSERT INTO address_transaction (id, payment_key, staking_key, credential_tag,
                                 transaction_id, slot, tx_index, payment_script)
VALUES (?, ?, ?, 0, ?, ?, 0, ?)`,
			row.ID, row.Payment, row.Staking, row.ID, row.ID*10, false)
	}

}

// exerciseCredentialDictionary upgrades a database holding by-value utxo and
// address_transaction rows on a real server, then replays the finished
// migration, and checks the rows, the id counters, the foreign keys of the
// tables that reference utxo and the rebuilt indexes.
func exerciseCredentialDictionary(t *testing.T, e *assetSplitEngine) {
	t.Helper()
	seedCredentialDictionary(t, e)
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := e.db.Exec(e.query(query), args...)
		require.NoError(t, err)
	}

	e.run(t, e.registry)

	hashes := func() []string {
		rows, err := e.db.Query(`
SELECT u.id, pc.hash, sc.hash, d.hash
FROM utxo u
LEFT JOIN credential pc ON pc.id = u.payment_key_id
LEFT JOIN credential sc ON sc.id = u.staking_key_id
LEFT JOIN datum d ON d.id = u.datum_id
ORDER BY u.id`)
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		var out []string
		for rows.Next() {
			var id int64
			var payment, staking, datum []byte
			require.NoError(t, rows.Scan(&id, &payment, &staking, &datum))
			out = append(out,
				fmt.Sprintf("%d/%x/%x/%x", id, payment, staking, datum))
		}
		require.NoError(t, rows.Err())
		return out
	}
	var want []string
	for _, row := range credentialDictionaryUtxos {
		want = append(want, fmt.Sprintf("%d/%x/%x/%x",
			row.ID, row.Payment, row.Staking, row.Datum))
	}
	got := hashes()
	require.Equal(t, want, got)

	var credentials int
	require.NoError(t, e.db.QueryRow(
		`SELECT COUNT(*) FROM credential`).Scan(&credentials))
	require.Equal(t, 6, credentials)
	for _, index := range []string{
		"idx_utxo_tx_id_output_idx", "idx_utxo_payment_key",
		"idx_utxo_staking_key", "idx_utxo_staking_deleted_amount",
		"idx_utxo_deleted_staking_amount", "idx_utxo_spent_at_tx_id",
	} {
		require.True(t, e.indexExists(t, "utxo", index), index)
	}
	for _, index := range []string{
		"idx_addr_tx_payment", "idx_addr_tx_stake_position",
	} {
		require.True(t, e.indexExists(t, "address_transaction", index), index)
	}
	require.False(t, e.tableExists(t, "utxo_dictionary"))
	require.False(t, e.tableExists(t, "address_transaction_dictionary"))

	// A row written after the swap takes the next id, not one already issued.
	exec(
		`INSERT INTO transaction (id, hash) VALUES (?, ?)`,
		50,
		make([]byte, 32),
	)
	exec(`
INSERT INTO utxo (tx_id, output_idx, credential_tag, added_slot, deleted_slot)
VALUES (?, 0, 0, 1, 0)`, 50)
	var newest int64
	require.NoError(t, e.db.QueryRow(
		`SELECT MAX(id) FROM utxo`).Scan(&newest))
	require.Greater(t, newest, int64(9))
	exec(`
INSERT INTO address_transaction (credential_tag, transaction_id, slot, tx_index)
VALUES (0, 50, 1, 0)`)
	require.NoError(t, e.db.QueryRow(
		`SELECT MAX(id) FROM address_transaction`).Scan(&newest))
	require.Greater(t, newest, int64(5))

	// The foreign keys to utxo were restored against the rebuilt table: the
	// cascade removes the referencing rows, and a row naming no output is
	// refused.
	exec(`DELETE FROM utxo WHERE id = ?`, 1)
	var left int
	require.NoError(t, e.db.QueryRow(
		`SELECT COUNT(*) FROM utxo_address WHERE utxo_id = 1`).Scan(&left))
	require.Zero(t, left)
	require.NoError(t, e.db.QueryRow(
		`SELECT COUNT(*) FROM utxo_reference_edge WHERE utxo_id = 1`,
	).Scan(&left))
	require.Zero(t, left)
	_, err := e.db.Exec(e.query(
		`INSERT INTO utxo_reference_edge (tx_id, utxo_id) VALUES (?, ?)`),
		2, 9999)
	require.Error(t, err, "the edge table keeps its foreign key to utxo")

	// A crash between the contract phase and its completion replays every
	// earlier phase against rebuilt tables.
	_, err = e.db.Exec(e.query(`
UPDATE schema_migrations SET phase = 'expand', dirty = ?, completed_at = NULL
WHERE version = ?`), true, utxoCredentialDictionaryVersion)
	require.NoError(t, err)
	e.run(t, e.registry)
	var rows int
	require.NoError(t, e.db.QueryRow(
		`SELECT COUNT(*) FROM utxo`).Scan(&rows))
	require.Equal(t, len(credentialDictionaryUtxos), rows)
}

func TestUtxoCredentialDictionaryAppliesOnPostgres(t *testing.T) {
	t.Parallel()
	exerciseCredentialDictionary(
		t, newPostgresAssetSplitEngine(t, "dingo_v78_credentials"),
	)
}

func TestUtxoCredentialDictionaryAppliesOnMySQL(t *testing.T) {
	t.Parallel()
	exerciseCredentialDictionary(
		t, newMySQLAssetSplitEngine(t, "dingo_v78_credentials"),
	)
}
