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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

const producerIndexMigrationVersion = 57

// TestUtxoProducerIndexUpgradePreservesOutputs upgrades a database holding
// produced, spent, reference and collateral input, collateral-return and
// imported outputs. Every row must keep its content, every row must be found
// again through the new indexes, the hash index over all outputs must be
// gone, and the new indexes must enforce uniqueness per identity.
func TestUtxoProducerIndexUpgradePreservesOutputs(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderUpgradeDB(t)
	upgrade(0, spenderMigrationVersion)
	seedSpenderUpgradeRows(t, db, 5)
	upgrade(0, inputEdgeMigrationVersion)
	const spender = `(SELECT t.hash FROM "transaction" t WHERE t.id = utxo.spent_by_tx_id)`
	before, rows := utxoLogicalDigest(t, db, spender)
	require.Positive(t, rows)

	upgrade(0, producerIndexMigrationVersion)

	after, afterRows := utxoLogicalDigest(t, db, spender)
	require.Equal(t, rows, afterRows)
	require.Equal(t, before, after, "utxo logical content changed")

	_, ok := indexSQL(t, db, "tx_id_output_idx")
	require.False(t, ok, "the all-output hash index must be dropped")
	for _, name := range []string{"idx_utxo_producer_output", "idx_utxo_imported_ref"} {
		definition, ok := indexSQL(t, db, name)
		require.True(t, ok, name)
		require.Contains(t, definition, "UNIQUE", name)
	}

	// Every row is found again by its identity: produced and collateral
	// return rows by producer, imported rows by hash.
	scan, err := db.Query(`SELECT id, tx_id, output_idx, transaction_id,
    collateral_return_for_tx_id, producer_tx_id FROM utxo`)
	require.NoError(t, err)
	defer scan.Close()
	var produced, collateralReturn, imported int
	for scan.Next() {
		var (
			id, idx                   int64
			txID                      []byte
			producer, collateral, gen sql.NullInt64
		)
		require.NoError(t, scan.Scan(&id, &txID, &idx, &producer, &collateral, &gen))
		var found int64
		switch {
		case producer.Valid:
			produced++
			require.Equal(t, producer, gen)
			require.NoError(t, db.QueryRow(
				"SELECT id FROM utxo WHERE producer_tx_id = ? AND output_idx = ?",
				producer.Int64, idx,
			).Scan(&found))
		case collateral.Valid:
			collateralReturn++
			require.Equal(t, collateral, gen)
			require.NoError(t, db.QueryRow(
				"SELECT id FROM utxo WHERE producer_tx_id = ? AND output_idx = ?",
				collateral.Int64, idx,
			).Scan(&found))
		default:
			imported++
			require.False(t, gen.Valid)
			require.NoError(t, db.QueryRow(
				"SELECT id FROM utxo WHERE transaction_id IS NULL AND "+
					"collateral_return_for_tx_id IS NULL AND tx_id = ? AND output_idx = ?",
				txID, idx,
			).Scan(&found))
		}
		require.Equal(t, id, found)
	}
	require.NoError(t, scan.Err())
	require.Positive(t, produced)
	require.Positive(t, collateralReturn)
	require.Positive(t, imported)

	// Uniqueness holds per identity.
	_, err = db.Exec(`INSERT INTO utxo (transaction_id, tx_id, output_idx)
SELECT transaction_id, tx_id, output_idx FROM utxo WHERE transaction_id IS NOT NULL LIMIT 1`)
	require.Error(t, err, "a produced output must be unique per producer")
	_, err = db.Exec(`INSERT INTO utxo (tx_id, output_idx)
SELECT tx_id, output_idx FROM utxo WHERE transaction_id IS NULL
  AND collateral_return_for_tx_id IS NULL LIMIT 1`)
	require.Error(t, err, "an imported output must be unique per hash")

	upgrade(0, producerIndexMigrationVersion)
	again, _ := utxoLogicalDigest(t, db, spender)
	require.Equal(t, before, again)
}
