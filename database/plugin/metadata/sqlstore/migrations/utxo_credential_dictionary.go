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

package migrations

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
)

const utxoCredentialDictionaryBatchSize = 50_000

// The phases of the credential dictionary rebuild, in order.
const (
	credentialPhaseUtxo = "utxo"
	credentialPhaseAtx  = "atx"
	credentialPhaseSwap = "swap"
	credentialPhaseIdx  = "idx"
)

// utxoCredentialCursor is the durable progress of the rebuild. Last is the
// highest legacy id copied in a copy phase, or the number of indexes built in
// the index phase. The counters let the swap prove every legacy row was
// copied.
type utxoCredentialCursor struct {
	Phase       string `json:"phase"`
	Last        int64  `json:"last"`
	UtxoScanned int64  `json:"utxo_scanned"`
	UtxoCopied  int64  `json:"utxo_copied"`
	AtxScanned  int64  `json:"atx_scanned"`
	AtxCopied   int64  `json:"atx_copied"`
}

// utxoCredentialIndexes lists the indexes built after the swap, in build
// order. The unique output index leads, since the apply path's conflict
// target needs it before any row is written.
var utxoCredentialIndexes = []utxoIdentityIndex{
	{
		name: "idx_utxo_tx_id_output_idx", table: "utxo",
		columns: []string{"tx_id", "output_idx"}, unique: true,
	},
	{
		name: "idx_utxo_collateral_return_for_tx_id", table: "utxo",
		columns: []string{"collateral_return_for_tx_id"}, unique: true,
		where: "collateral_return_for_tx_id IS NOT NULL",
	},
	{
		name: "idx_utxo_spent_at_tx_id", table: "utxo",
		columns: []string{"spent_at_tx_id"},
		where:   "spent_at_tx_id IS NOT NULL",
	},
	{
		name: "idx_utxo_added_slot", table: "utxo",
		columns: []string{"added_slot"},
	},
	{
		name: "idx_utxo_payment_key", table: "utxo",
		columns: []string{"payment_key_id"},
	},
	{
		name: "idx_utxo_staking_key", table: "utxo",
		columns: []string{"staking_key_id"},
		where:   "staking_key_id IS NOT NULL",
	},
	{
		name: "idx_utxo_staking_deleted_amount", table: "utxo",
		columns: []string{
			"credential_tag", "staking_key_id", "deleted_slot", "amount",
		},
		where: "staking_key_id IS NOT NULL",
	},
	{
		name: "idx_utxo_deleted_staking_amount", table: "utxo",
		columns: []string{
			"deleted_slot", "credential_tag", "staking_key_id", "amount",
		},
		where:        "deleted_slot > 0",
		mysqlColumns: []string{"deleted_slot"},
	},
	{
		name: "idx_utxo_deleted_payment_script", table: "utxo",
		columns: []string{"deleted_slot", "payment_script", "amount"},
	},
	{
		name: "idx_address_transaction_slot", table: "address_transaction",
		columns: []string{"slot"},
	},
	{
		name: "idx_address_transaction_transaction_id", table: "address_transaction",
		columns: []string{"transaction_id"},
	},
	{
		name: "idx_addr_tx_payment", table: "address_transaction",
		columns: []string{"payment_key_id"},
	},
	{
		name: "idx_addr_tx_stake_position", table: "address_transaction",
		columns: []string{
			"credential_tag", "staking_key_id", "slot", "tx_index",
			"payment_key_id",
		},
	},
}

// emptyBlobLiteral is the dialect's spelling of a zero-length binary value,
// the raw_datum of a datum that no transaction has revealed yet.
func emptyBlobLiteral(dialect string) string {
	if dialect == "postgres" {
		return `'\x'::bytea`
	}
	return "X''"
}

// utxoCredentialDictionaryBackfill copies utxo and address_transaction into
// their dictionary-keyed copies, swaps the copies in under the original names
// and builds their indexes.
//
// Phases, each resumable from its cursor:
//
//	utxo  copy utxo rows by id range. Each range first adds its credentials to
//	      the dictionary and its datum hashes to datum as unrevealed rows
//	      (empty raw_datum), then copies its rows with their ids
//	atx   copy address_transaction rows the same way
//	swap  compare row counts, then replace the legacy tables
//	idx   build one index per batch
//
// Dictionary rows are never removed. The row counts are compared in the swap's
// transaction, so a legacy table is replaced only when every row is copied.
func utxoCredentialDictionaryBackfill(dialect string) Backfill {
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		cursor := utxoCredentialCursor{Phase: credentialPhaseUtxo}
		if batch.Cursor != "" {
			if err := json.Unmarshal(
				[]byte(batch.Cursor), &cursor,
			); err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse credential dictionary cursor: %w", err,
				)
			}
		}
		b := credentialBackfill{dialect: dialect}
		switch cursor.Phase {
		case credentialPhaseUtxo:
			return b.copyUtxo(ctx, batch, cursor)
		case credentialPhaseAtx:
			return b.copyAddressTransaction(ctx, batch, cursor)
		case credentialPhaseSwap:
			return b.swap(ctx, batch, cursor)
		case credentialPhaseIdx:
			return b.buildIndex(ctx, batch, cursor)
		default:
			return BatchResult{}, fmt.Errorf(
				"unknown credential dictionary phase %q", cursor.Phase,
			)
		}
	}
}

type credentialBackfill struct {
	dialect string
}

func encodeCredentialCursor(
	cursor utxoCredentialCursor,
	rows int64,
) (BatchResult, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return BatchResult{}, err
	}
	return BatchResult{Cursor: string(encoded), Rows: rows}, nil
}

// insertIgnoreSelect renders an insert-select that skips rows whose unique
// key exists. The select carries a WHERE clause, which SQLite needs to read
// the conflict clause as the insert's.
func (b credentialBackfill) insertIgnoreSelect(
	table, columns, key, selectSQL string,
) string {
	if b.dialect == "mysql" {
		return "INSERT IGNORE INTO " + table + " (" + columns + ")\n" + selectSQL
	}
	return "INSERT INTO " + table + " (" + columns + ")\n" + selectSQL +
		"\nON CONFLICT (" + key + ") DO NOTHING"
}

// idRange returns the number of rows and the highest id of the next Limit
// rows of table after last.
func idRange(
	ctx context.Context,
	batch Batch,
	table string,
	last int64,
) (count int64, high int64, err error) {
	var upper sql.NullInt64
	if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*), MAX(id)
FROM (SELECT id FROM `+table+` WHERE id > ? ORDER BY id LIMIT ?) AS b`),
		last, batch.Limit,
	).Scan(&count, &upper); err != nil {
		return 0, 0, fmt.Errorf("read %s range: %w", table, err)
	}
	return count, upper.Int64, nil
}

func (b credentialBackfill) addCredentials(
	ctx context.Context,
	batch Batch,
	table, column string,
	lo, hi int64,
) error {
	statement := b.insertIgnoreSelect("credential", "hash", "hash", `
SELECT DISTINCT `+column+` FROM `+table+`
WHERE id > ? AND id <= ? AND `+column+` IS NOT NULL
  AND LENGTH(`+column+`) > 0`)
	if _, err := batch.Tx.ExecContext(
		ctx, batch.Rebind(statement), lo, hi,
	); err != nil {
		return fmt.Errorf("add %s.%s credentials: %w", table, column, err)
	}
	return nil
}

func (b credentialBackfill) copyUtxo(
	ctx context.Context,
	batch Batch,
	cursor utxoCredentialCursor,
) (BatchResult, error) {
	hasLegacy, err := columnExists(
		ctx, batch, b.dialect, "utxo", "payment_key",
	)
	if err != nil {
		return BatchResult{}, err
	}
	if !hasLegacy {
		// The swap already ran.
		cursor.Phase, cursor.Last = credentialPhaseIdx, 0
		return encodeCredentialCursor(cursor, 0)
	}
	count, high, err := idRange(ctx, batch, "utxo", cursor.Last)
	if err != nil {
		return BatchResult{}, err
	}
	var copied int64
	if count > 0 {
		for _, column := range []string{"payment_key", "staking_key"} {
			if err := b.addCredentials(
				ctx, batch, "utxo", column, cursor.Last, high,
			); err != nil {
				return BatchResult{}, err
			}
		}
		stubs := b.insertIgnoreSelect(
			"datum", "hash, raw_datum, added_slot", "hash", `
SELECT DISTINCT datum_hash, `+emptyBlobLiteral(b.dialect)+`, 0 FROM utxo
WHERE id > ? AND id <= ? AND datum_hash IS NOT NULL
  AND LENGTH(datum_hash) > 0`)
		if _, err := batch.Tx.ExecContext(
			ctx, batch.Rebind(stubs), cursor.Last, high,
		); err != nil {
			return BatchResult{}, fmt.Errorf("add datum rows: %w", err)
		}
		result, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO utxo_dictionary (
    id, tx_id, output_idx, collateral_return_for_tx_id, spent_at_tx_id,
    payment_key_id, staking_key_id, credential_tag, datum_id, amount,
    payment_script, added_slot, deleted_slot)
SELECT u.id, u.tx_id, u.output_idx, u.collateral_return_for_tx_id,
       u.spent_at_tx_id, pc.id, sc.id, u.credential_tag, d.id, u.amount,
       u.payment_script, u.added_slot, u.deleted_slot
FROM utxo AS u
LEFT JOIN credential AS pc ON pc.hash = u.payment_key
LEFT JOIN credential AS sc ON sc.hash = u.staking_key
LEFT JOIN datum AS d ON d.hash = u.datum_hash
WHERE u.id > ? AND u.id <= ?
ORDER BY u.id`), cursor.Last, high)
		if err != nil {
			return BatchResult{}, fmt.Errorf("copy UTxO rows: %w", err)
		}
		if copied, err = result.RowsAffected(); err != nil {
			return BatchResult{}, err
		}
		cursor.Last = high
		cursor.UtxoScanned += count
		cursor.UtxoCopied += copied
	}
	if count == int64(batch.Limit) {
		return encodeCredentialCursor(cursor, count)
	}
	cursor.Phase, cursor.Last = credentialPhaseAtx, 0
	return encodeCredentialCursor(cursor, count)
}

func (b credentialBackfill) copyAddressTransaction(
	ctx context.Context,
	batch Batch,
	cursor utxoCredentialCursor,
) (BatchResult, error) {
	count, high, err := idRange(ctx, batch, "address_transaction", cursor.Last)
	if err != nil {
		return BatchResult{}, err
	}
	if count > 0 {
		for _, column := range []string{"payment_key", "staking_key"} {
			if err := b.addCredentials(
				ctx, batch, "address_transaction", column, cursor.Last, high,
			); err != nil {
				return BatchResult{}, err
			}
		}
		result, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO address_transaction_dictionary (
    id, payment_key_id, payment_script, staking_key_id, credential_tag,
    transaction_id, slot, tx_index)
SELECT a.id, pc.id, a.payment_script, sc.id, a.credential_tag,
       a.transaction_id, a.slot, a.tx_index
FROM address_transaction AS a
LEFT JOIN credential AS pc ON pc.hash = a.payment_key
LEFT JOIN credential AS sc ON sc.hash = a.staking_key
WHERE a.id > ? AND a.id <= ?
ORDER BY a.id`), cursor.Last, high)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"copy address transaction rows: %w", err,
			)
		}
		copied, err := result.RowsAffected()
		if err != nil {
			return BatchResult{}, err
		}
		cursor.Last = high
		cursor.AtxScanned += count
		cursor.AtxCopied += copied
	}
	if count == int64(batch.Limit) {
		return encodeCredentialCursor(cursor, count)
	}
	cursor.Phase, cursor.Last = credentialPhaseSwap, 0
	return encodeCredentialCursor(cursor, count)
}

func (b credentialBackfill) count(
	ctx context.Context,
	batch Batch,
	table string,
) (int64, error) {
	var n int64
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM "+table,
	).Scan(&n); err != nil {
		return 0, fmt.Errorf("count %s: %w", table, err)
	}
	return n, nil
}

// reconcile refuses to replace legacy unless the copy holds exactly its rows.
func (b credentialBackfill) reconcile(
	ctx context.Context,
	batch Batch,
	legacy, copy string,
	scanned, copied int64,
) error {
	legacyRows, err := b.count(ctx, batch, legacy)
	if err != nil {
		return err
	}
	copyRows, err := b.count(ctx, batch, copy)
	if err != nil {
		return err
	}
	if scanned != legacyRows || copied != copyRows || scanned != copied {
		return fmt.Errorf(
			"%s rebuild is incomplete: legacy rows %d, scanned %d, copied %d, "+
				"rows in copy %d",
			legacy, legacyRows, scanned, copied, copyRows,
		)
	}
	return nil
}

func (b credentialBackfill) exec(
	ctx context.Context,
	batch Batch,
	statements ...string,
) error {
	for _, statement := range statements {
		if _, err := batch.Tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("%s: %w", statement, err)
		}
	}
	return nil
}

func (b credentialBackfill) swap(
	ctx context.Context,
	batch Batch,
	cursor utxoCredentialCursor,
) (BatchResult, error) {
	verify := func() error {
		if err := b.reconcile(
			ctx, batch, "utxo", "utxo_dictionary",
			cursor.UtxoScanned, cursor.UtxoCopied,
		); err != nil {
			return err
		}
		return b.reconcile(
			ctx, batch, "address_transaction",
			"address_transaction_dictionary",
			cursor.AtxScanned, cursor.AtxCopied,
		)
	}
	var err error
	switch b.dialect {
	case "mysql":
		err = b.swapMySQL(ctx, batch, verify)
	default:
		var hasCopy bool
		hasCopy, err = tableExists(ctx, batch, b.dialect, "utxo_dictionary")
		if err != nil || !hasCopy {
			break
		}
		if err = verify(); err != nil {
			break
		}
		if b.dialect == "postgres" {
			err = b.swapPostgres(ctx, batch)
		} else {
			err = b.swapSQLite(ctx, batch)
		}
	}
	if err != nil {
		return BatchResult{}, fmt.Errorf("swap credential tables: %w", err)
	}
	cursor.Phase, cursor.Last = credentialPhaseIdx, 0
	return encodeCredentialCursor(cursor, 0)
}

// swapSQLite replaces the legacy tables. The runner turns foreign keys off
// for a SQLite migration, so dropping utxo does not cascade into the tables
// that reference it; they resolve against the renamed copy. The copies' ids
// were inserted explicitly, so their AUTOINCREMENT counters are raised to the
// legacy counters to keep ids that were once issued from being issued again.
func (b credentialBackfill) swapSQLite(
	ctx context.Context,
	batch Batch,
) error {
	sequences := map[string]int64{}
	for _, table := range []string{"utxo", "address_transaction"} {
		var seq sql.NullInt64
		err := batch.Tx.QueryRowContext(
			ctx, "SELECT seq FROM sqlite_sequence WHERE name = ?", table,
		).Scan(&seq)
		if err != nil && err != sql.ErrNoRows {
			return fmt.Errorf("read %s sequence: %w", table, err)
		}
		sequences[table] = seq.Int64
	}
	if err := b.exec(ctx, batch,
		"DROP TABLE utxo",
		"ALTER TABLE utxo_dictionary RENAME TO utxo",
		"DROP TABLE address_transaction",
		"ALTER TABLE address_transaction_dictionary RENAME TO address_transaction",
	); err != nil {
		return err
	}
	for table, seq := range sequences {
		if _, err := batch.Tx.ExecContext(ctx, `
INSERT INTO sqlite_sequence (name, seq)
SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name = ?)`,
			table, seq, table,
		); err != nil {
			return fmt.Errorf("restore %s sequence: %w", table, err)
		}
		if _, err := batch.Tx.ExecContext(ctx, `
UPDATE sqlite_sequence SET seq = ? WHERE name = ? AND seq < ?`,
			seq, table, seq,
		); err != nil {
			return fmt.Errorf("raise %s sequence: %w", table, err)
		}
	}
	return nil
}

// childForeignKey is a foreign key to utxo(id) that a server engine drops
// before the legacy table and re-adds against the copy.
type childForeignKey struct {
	table, name, column, onDelete string
}

// childForeignKeys reads the foreign keys to utxo(id) that query selects, as
// (table, constraint, column, delete rule) rows.
func childForeignKeys(
	ctx context.Context,
	batch Batch,
	query string,
) ([]childForeignKey, error) {
	rows, err := batch.Tx.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("read utxo foreign keys: %w", err)
	}
	defer rows.Close()
	var keys []childForeignKey
	for rows.Next() {
		var fk childForeignKey
		if err := rows.Scan(
			&fk.table, &fk.name, &fk.column, &fk.onDelete,
		); err != nil {
			return nil, err
		}
		keys = append(keys, fk)
	}
	return keys, rows.Err()
}

func (b credentialBackfill) swapPostgres(
	ctx context.Context,
	batch Batch,
) error {
	keys, err := childForeignKeys(ctx, batch, `
SELECT c.conrelid::regclass::text, c.conname, a.attname,
       CASE c.confdeltype WHEN 'c' THEN 'CASCADE' ELSE 'NO ACTION' END
FROM pg_constraint c
JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
WHERE c.contype = 'f' AND c.confrelid = 'utxo'::regclass`)
	if err != nil {
		return err
	}
	sequences := map[string]int64{}
	for _, table := range []string{"utxo", "address_transaction"} {
		var start sql.NullInt64
		if err := batch.Tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT GREATEST(
    (SELECT COALESCE(MAX(id), 0) FROM %[1]s),
    (SELECT last_value FROM %[1]s_id_seq))`, table)).Scan(&start); err != nil {
			return fmt.Errorf("read %s id sequence: %w", table, err)
		}
		sequences[table] = max(start.Int64, 1)
	}
	var statements []string
	for _, fk := range keys {
		statements = append(statements,
			"ALTER TABLE "+fk.table+` DROP CONSTRAINT "`+fk.name+`"`)
	}
	statements = append(
		statements,
		`DROP TABLE "utxo"`,
		`ALTER TABLE "utxo_dictionary" RENAME TO "utxo"`,
		`ALTER INDEX "utxo_dictionary_pkey" RENAME TO "utxo_pkey"`,
		`ALTER SEQUENCE "utxo_dictionary_id_seq" RENAME TO "utxo_id_seq"`,
		`DROP TABLE "address_transaction"`,
		`ALTER TABLE "address_transaction_dictionary" RENAME TO "address_transaction"`,
		`ALTER INDEX "address_transaction_dictionary_pkey" RENAME TO "address_transaction_pkey"`,
		`ALTER SEQUENCE "address_transaction_dictionary_id_seq" RENAME TO "address_transaction_id_seq"`,
	)
	for _, fk := range keys {
		statements = append(statements,
			"ALTER TABLE "+fk.table+` ADD CONSTRAINT "`+fk.name+
				`" FOREIGN KEY ("`+fk.column+`") REFERENCES "utxo"("id") `+
				"ON DELETE "+fk.onDelete)
	}
	for table, value := range sequences {
		statements = append(statements, fmt.Sprintf(
			`SELECT setval('%s_id_seq', %d, true)`, table, value,
		))
	}
	return b.exec(ctx, batch, statements...)
}

// credentialChildForeignKeys are the foreign keys to utxo(id) on MySQL when
// this migration runs, as earlier migrations left them. The names of the edge
// and address keys are those of the tables their rebuilds renamed into place.
// The list is fixed because a rerun after the keys are dropped cannot read them
// from the catalog.
var credentialChildForeignKeys = []struct{ table, name, column string }{
	{"asset_holding", "fk_asset_holding_utxo", "utxo_id"},
	{"utxo_address", "fk_utxo_address_dictionary_utxo", "utxo_id"},
	{"utxo_collateral_edge", "fk_utxo_collateral_edge_clustered_utxo", "utxo_id"},
	{"utxo_pointer", "fk_utxo_pointer_utxo", "utxo_id"},
	{"utxo_reference_edge", "fk_utxo_reference_edge_clustered_utxo", "utxo_id"},
	{"utxo_reference_script", "fk_utxo_reference_script_utxo", "utxo_id"},
}

// swapMySQL replaces the legacy tables on MySQL, where every DDL statement
// commits implicitly, so each step checks the catalog and the whole swap can be
// rerun after a crash at any statement. The foreign keys come from the fixed
// credentialChildForeignKeys list: a rerun after they are dropped cannot read them
// back from the catalog. The copies' auto-increment counters are raised to the
// legacy counters before the rename, while the legacy tables still exist.
func (b credentialBackfill) swapMySQL(
	ctx context.Context,
	batch Batch,
	verify func() error,
) error {
	hasCopy, err := tableExists(ctx, batch, b.dialect, "utxo_dictionary")
	if err != nil {
		return err
	}
	hasLegacy, err := tableExists(ctx, batch, b.dialect, "utxo")
	if err != nil {
		return err
	}
	if hasCopy && hasLegacy {
		if err := verify(); err != nil {
			return err
		}
	}
	pairs := [][2]string{
		{"utxo", "utxo_dictionary"},
		{"address_transaction", "address_transaction_dictionary"},
	}
	if hasCopy {
		for _, pair := range pairs {
			if err := b.carryAutoIncrement(
				ctx, batch, pair[0], pair[1],
			); err != nil {
				return err
			}
		}
		for _, fk := range credentialChildForeignKeys {
			exists, err := mysqlConstraintExists(ctx, batch, fk.table, fk.name)
			if err != nil {
				return err
			}
			if exists {
				if err := b.exec(ctx, batch, "ALTER TABLE `"+fk.table+
					"` DROP FOREIGN KEY `"+fk.name+"`"); err != nil {
					return err
				}
			}
		}
	}
	for _, pair := range pairs {
		if err := swapRebuiltTable(
			ctx, batch, b.dialect, pair[0], pair[1], nil,
		); err != nil {
			return err
		}
	}
	for _, fk := range credentialChildForeignKeys {
		exists, err := mysqlConstraintExists(ctx, batch, fk.table, fk.name)
		if err != nil {
			return err
		}
		if !exists {
			if err := b.exec(ctx, batch, "ALTER TABLE `"+fk.table+
				"` ADD CONSTRAINT `"+fk.name+"` FOREIGN KEY (`"+fk.column+
				"`) REFERENCES `utxo`(`id`) ON DELETE CASCADE"); err != nil {
				return err
			}
		}
	}
	return nil
}

// carryAutoIncrement raises the copy's next id to the legacy table's, when
// the legacy table still exists, so an id once issued is not issued again.
func (b credentialBackfill) carryAutoIncrement(
	ctx context.Context,
	batch Batch,
	legacy, rebuilt string,
) error {
	hasLegacy, err := tableExists(ctx, batch, b.dialect, legacy)
	if err != nil || !hasLegacy {
		return err
	}
	var next sql.NullInt64
	if err := batch.Tx.QueryRowContext(ctx, `
SELECT auto_increment FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = ?`, legacy).Scan(&next); err != nil {
		return fmt.Errorf("read %s auto_increment: %w", legacy, err)
	}
	if !next.Valid || next.Int64 <= 0 {
		return nil
	}
	return b.exec(ctx, batch, fmt.Sprintf(
		"ALTER TABLE `%s` AUTO_INCREMENT = %d", rebuilt, next.Int64,
	))
}

// buildIndex creates the position'th index. Each index is its own transaction
// so the write-ahead log is checkpointed between builds.
func (b credentialBackfill) buildIndex(
	ctx context.Context,
	batch Batch,
	cursor utxoCredentialCursor,
) (BatchResult, error) {
	if cursor.Last >= int64(len(utxoCredentialIndexes)) {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	index := utxoCredentialIndexes[cursor.Last]
	if b.dialect == "mysql" {
		var exists int
		if err := batch.Tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
			index.table, index.name).Scan(&exists); err != nil {
			return BatchResult{}, fmt.Errorf("check %s: %w", index.name, err)
		}
		if exists > 0 {
			cursor.Last++
			return encodeCredentialCursor(cursor, 0)
		}
	}
	if _, err := batch.Tx.ExecContext(
		ctx, index.createSQL(b.dialect),
	); err != nil {
		return BatchResult{}, fmt.Errorf("create %s: %w", index.name, err)
	}
	cursor.Last++
	return encodeCredentialCursor(cursor, 1)
}
