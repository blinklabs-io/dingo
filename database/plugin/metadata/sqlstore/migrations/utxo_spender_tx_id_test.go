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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const spenderMigrationVersion = 56

func spenderHash(b byte) []byte {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = b
	}
	return hash
}

// seedSpenderUpgradeRows writes the utxo shapes a ledger holds before the
// spender column moves: unspent, spent, reference and collateral inputs
// (association rows plus the legacy single-owner columns), a collateral
// return, and an imported output with no producing transaction row.
func seedSpenderUpgradeRows(t *testing.T, db *sql.DB, spenders int) {
	t.Helper()
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := db.Exec(query, args...)
		require.NoError(t, err, query)
	}
	for i := 1; i <= spenders; i++ {
		exec(
			"INSERT INTO `transaction` (hash, slot, block_index) VALUES (?, ?, 0)",
			spenderHash(byte(i)), int64(100+i),
		)
	}
	// Outputs 1..n are spent by transaction (i%spenders)+1.
	for i := 1; i <= spenders*3; i++ {
		spender := spenderHash(byte(i%spenders + 1))
		exec(
			"INSERT INTO utxo (transaction_id, tx_id, output_idx, "+
				"credential_tag, staking_key, added_slot, deleted_slot, "+
				"spent_at_tx_id, amount) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)",
			i%spenders+1, spenderHash(byte(0x80+i%100)), i, spenderHash(byte(i)),
			int64(i), int64(1000+i), spender, fmt.Sprint(i*1000),
		)
	}
	// Unspent, reference-input, collateral-input and collateral-return rows.
	exec(
		"INSERT INTO utxo (transaction_id, tx_id, output_idx, added_slot, "+
			"deleted_slot, amount) VALUES (1, ?, 100, 5, 0, '7')",
		spenderHash(0xA1),
	)
	exec(
		"INSERT INTO utxo (transaction_id, tx_id, output_idx, added_slot, "+
			"deleted_slot, referenced_by_tx_id, amount) "+
			"VALUES (1, ?, 101, 5, 0, ?, '8')",
		spenderHash(0xA2), spenderHash(1),
	)
	exec(
		"INSERT INTO utxo_reference_input (utxo_id, transaction_hash) "+
			"SELECT id, ? FROM utxo WHERE tx_id = ?",
		spenderHash(1), spenderHash(0xA2),
	)
	exec(
		"INSERT INTO utxo (transaction_id, tx_id, output_idx, added_slot, "+
			"deleted_slot, collateral_by_tx_id, amount) "+
			"VALUES (1, ?, 102, 5, 0, ?, '9')",
		spenderHash(0xA3), spenderHash(1),
	)
	exec(
		"INSERT INTO utxo_collateral_input (utxo_id, transaction_hash) "+
			"SELECT id, ? FROM utxo WHERE tx_id = ?",
		spenderHash(1), spenderHash(0xA3),
	)
	exec(
		"INSERT INTO utxo (collateral_return_for_tx_id, tx_id, output_idx, "+
			"added_slot, deleted_slot, spent_at_tx_id, amount) "+
			"VALUES (1, ?, 3, 6, 2000, ?, '10')",
		spenderHash(0xA4), spenderHash(2),
	)
	exec(
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, "+
			"spent_at_tx_id, amount) VALUES (?, 0, 1, 3000, ?, '11')",
		spenderHash(0xA5), spenderHash(2),
	)
	exec(
		"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, "+
			"amount) VALUES (?, 1, 1, 0, '12')",
		spenderHash(0xA5),
	)
}

// utxoLogicalDigest hashes every utxo row's logical content in a form both
// schemas can produce: the spender is the spender's transaction hash, read
// through whichever column the schema stores it in.
func utxoLogicalDigest(t *testing.T, db *sql.DB, spenderExpr string) (string, int) {
	t.Helper()
	rows, err := db.Query(`SELECT tx_id, output_idx, transaction_id,
    collateral_return_for_tx_id, credential_tag, staking_key, added_slot,
    deleted_slot, amount, referenced_by_tx_id, collateral_by_tx_id, ` +
		spenderExpr + ` FROM utxo ORDER BY tx_id, output_idx`)
	require.NoError(t, err)
	defer rows.Close()
	digest := sha256.New()
	count := 0
	for rows.Next() {
		var (
			txID, staking, referenced, collateral, spender []byte
			idx, credTag                                   int64
			producer, collReturn, added, deleted           sql.NullInt64
			amount                                         sql.NullString
		)
		require.NoError(t, rows.Scan(
			&txID, &idx, &producer, &collReturn, &credTag, &staking, &added,
			&deleted, &amount, &referenced, &collateral, &spender,
		))
		fmt.Fprintf(
			digest, "%x|%d|%v|%v|%d|%x|%v|%v|%v|%x|%x|%x\n",
			txID, idx, producer, collReturn, credTag, staking, added,
			deleted, amount, referenced, collateral, spender,
		)
		count++
	}
	require.NoError(t, rows.Err())
	return hex.EncodeToString(digest.Sum(nil)), count
}

func spenderUpgradeDB(t *testing.T) (*sql.DB, func(batchSize int, upTo int)) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	return db, func(batchSize int, upTo int) {
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

// TestUtxoSpenderTxIDUpgradePreservesLedgerState upgrades a populated
// previous-schema database in several small backfill batches and requires
// every row to keep its logical content, with the spender now stored as the
// spender's transaction id.
func TestUtxoSpenderTxIDUpgradePreservesLedgerState(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderUpgradeDB(t)
	upgrade(0, spenderMigrationVersion-1)
	seedSpenderUpgradeRows(t, db, 5)
	before, beforeRows := utxoLogicalDigest(t, db, "spent_at_tx_id")
	var spentBefore int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE spent_at_tx_id IS NOT NULL",
	).Scan(&spentBefore))
	require.Equal(t, 15+2, spentBefore)

	upgrade(4, spenderMigrationVersion)

	after, afterRows := utxoLogicalDigest(t, db, `(SELECT t.hash FROM "transaction" t
WHERE t.id = utxo.spent_by_tx_id)`)
	require.Equal(t, beforeRows, afterRows)
	require.Equal(t, before, after, "utxo logical content changed")

	var legacy, byID int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE spent_at_tx_id IS NOT NULL",
	).Scan(&legacy))
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE spent_by_tx_id IS NOT NULL",
	).Scan(&byID))
	require.Zero(t, legacy, "legacy spender column must be cleared")
	require.Equal(t, spentBefore, byID)

	for _, name := range []string{
		"idx_utxo_spent_by_tx_id", "idx_utxo_spent_at_tx_id",
	} {
		definition, ok := indexSQL(t, db, name)
		require.True(t, ok, name)
		require.Contains(t, definition, "WHERE", name)
	}

	// The upgrade is idempotent once complete.
	upgrade(4, spenderMigrationVersion)
	again, _ := utxoLogicalDigest(t, db, `(SELECT t.hash FROM "transaction" t
WHERE t.id = utxo.spent_by_tx_id)`)
	require.Equal(t, before, again)
}

// TestUtxoSpenderTxIDUpgradeKeepsUnresolvedSpender keeps a spender hash with
// no transaction row in the legacy column instead of dropping the spend.
func TestUtxoSpenderTxIDUpgradeKeepsUnresolvedSpender(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderUpgradeDB(t)
	upgrade(0, spenderMigrationVersion-1)
	_, err := db.Exec(
		"INSERT INTO utxo (tx_id, output_idx, deleted_slot, spent_at_tx_id) "+
			"VALUES (?, 0, 9, ?)", spenderHash(1), spenderHash(0xEE),
	)
	require.NoError(t, err)

	upgrade(0, spenderMigrationVersion)

	var legacy []byte
	var byID sql.NullInt64
	require.NoError(t, db.QueryRow(
		"SELECT spent_at_tx_id, spent_by_tx_id FROM utxo",
	).Scan(&legacy, &byID))
	require.Equal(t, spenderHash(0xEE), legacy)
	require.False(t, byID.Valid)
}

// TestUtxoSpenderTxIDUpgradeLeavesIndexesToPendingRebuild checks that a
// pending deferred-index rebuild is left to create both spender indexes.
func TestUtxoSpenderTxIDUpgradeLeavesIndexesToPendingRebuild(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderUpgradeDB(t)
	upgrade(0, spenderMigrationVersion-1)
	seedSpenderUpgradeRows(t, db, 2)
	_, err := db.Exec(
		"INSERT INTO sync_state(sync_key,value) VALUES (?,?)",
		deferred.SyncStateKey, deferred.SyncStateValue,
	)
	require.NoError(t, err)

	upgrade(0, spenderMigrationVersion)

	for _, name := range []string{
		"idx_utxo_spent_by_tx_id", "idx_utxo_spent_at_tx_id",
	} {
		_, ok := indexSQL(t, db, name)
		require.False(t, ok, name)
	}
	var byID int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE spent_by_tx_id IS NOT NULL",
	).Scan(&byID))
	require.Positive(t, byID)
}
