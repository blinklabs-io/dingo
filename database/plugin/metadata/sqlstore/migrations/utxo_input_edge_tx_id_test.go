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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

const inputEdgeMigrationVersion = 56

// edgeDigest hashes every (output, naming transaction) edge as the
// output's reference and the transaction's hash, read through whichever
// table shape the schema has.
func edgeDigest(t *testing.T, db *sql.DB, query string) (string, int) {
	t.Helper()
	rows, err := db.Query(query)
	require.NoError(t, err)
	defer rows.Close()
	digest := sha256.New()
	count := 0
	for rows.Next() {
		var txID, owner []byte
		var idx int64
		require.NoError(t, rows.Scan(&txID, &idx, &owner))
		fmt.Fprintf(digest, "%x|%d|%x\n", txID, idx, owner)
		count++
	}
	require.NoError(t, rows.Err())
	return hex.EncodeToString(digest.Sum(nil)), count
}

// TestUtxoInputEdgeTxIDUpgradePreservesEdges upgrades a database holding
// reference and collateral edges, including one output named by several
// transactions, and requires the same edges keyed by transaction id, the
// legacy single-owner columns cleared and the hash-keyed tables dropped.
func TestUtxoInputEdgeTxIDUpgradePreservesEdges(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderUpgradeDB(t)
	upgrade(0, inputEdgeMigrationVersion-1)
	seedSpenderUpgradeRows(t, db, 5)
	for _, query := range []string{
		"INSERT INTO utxo_reference_input (utxo_id, transaction_hash) SELECT id, ? FROM utxo WHERE tx_id = ?",
		"INSERT INTO utxo_collateral_input (utxo_id, transaction_hash) SELECT id, ? FROM utxo WHERE tx_id = ?",
	} {
		for _, owner := range []byte{2, 3, 4} {
			for _, producer := range []byte{0xA2, 0xA3, 0xA1} {
				_, err := db.Exec(query, spenderHash(owner), spenderHash(producer))
				require.NoError(t, err)
			}
		}
	}
	// An edge naming a transaction that has no row cannot be migrated.
	_, err := db.Exec(
		"INSERT INTO utxo_reference_input (utxo_id, transaction_hash) SELECT id, ? FROM utxo WHERE tx_id = ?",
		spenderHash(0xEE), spenderHash(0xA2),
	)
	require.NoError(t, err)

	const old = `SELECT u.tx_id, u.output_idx, e.transaction_hash FROM %s e
JOIN utxo u ON u.id = e.utxo_id
JOIN "transaction" t ON t.hash = e.transaction_hash
ORDER BY u.tx_id, u.output_idx, e.transaction_hash`
	const edge = `SELECT u.tx_id, u.output_idx, t.hash FROM %s e
JOIN utxo u ON u.id = e.utxo_id
JOIN "transaction" t ON t.id = e.transaction_id
ORDER BY u.tx_id, u.output_idx, t.hash`
	refBefore, refRows := edgeDigest(t, db, fmt.Sprintf(old, "utxo_reference_input"))
	colBefore, colRows := edgeDigest(t, db, fmt.Sprintf(old, "utxo_collateral_input"))
	require.Equal(t, 10, refRows)
	require.Equal(t, 10, colRows)

	upgrade(1, inputEdgeMigrationVersion)

	refAfter, n := edgeDigest(t, db, fmt.Sprintf(edge, "utxo_reference_edge"))
	require.Equal(t, refRows, n)
	require.Equal(t, refBefore, refAfter, "reference edges changed")
	colAfter, n := edgeDigest(t, db, fmt.Sprintf(edge, "utxo_collateral_edge"))
	require.Equal(t, colRows, n)
	require.Equal(t, colBefore, colAfter, "collateral edges changed")

	var legacy int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE referenced_by_tx_id IS NOT NULL OR collateral_by_tx_id IS NOT NULL",
	).Scan(&legacy))
	require.Zero(t, legacy)
	for _, table := range []string{"utxo_reference_input", "utxo_collateral_input"} {
		var name string
		err := db.QueryRow(
			"SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?", table,
		).Scan(&name)
		require.ErrorIs(t, err, sql.ErrNoRows, table)
	}

	upgrade(1, inputEdgeMigrationVersion)
	again, _ := edgeDigest(t, db, fmt.Sprintf(edge, "utxo_reference_edge"))
	require.Equal(t, refBefore, again)
}
