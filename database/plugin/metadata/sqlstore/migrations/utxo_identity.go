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
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// utxoIdentityBatchSize bounds the rows one backfill transaction copies.
const utxoIdentityBatchSize = 5000

// The phases of the utxo-transaction-identity backfill, in order. The cursor
// is "<phase>:<position>"; the position is the last id a copy phase has
// finished, or the number of indexes a build phase has created. An edge
// phase copies a fixed number of edges per batch, so one output named by many
// transactions cannot make a batch unbounded: its cursor is
// "<phase>:<utxo id>:<transaction hash hex>", the last edge copied.
const (
	identityPhaseTx     = "tx"
	identityPhaseImp    = "imp"
	identityPhaseUtxo   = "utxo"
	identityPhaseRef    = "ref"
	identityPhaseCol    = "col"
	identityPhaseVerify = "verify"
	identityPhaseSwap   = "swap"
	identityPhaseIdx    = "idx"
)

// utxoIdentityIndex describes a secondary index of the rebuilt tables. The
// unique (tx_id, output_idx) index is created with the table, because the
// copy depends on it to reject a second output under one producer.
type utxoIdentityIndex struct {
	name    string
	table   string
	columns []string
	unique  bool
	// where is the predicate of a partial index. MySQL has no partial
	// indexes and builds the full index instead.
	where string
}

// utxoIdentityIndexes lists the indexes built after the swap, in build order.
// idx_utxo_spent_at_tx_id and idx_utxo_collateral_return_for_tx_id are the
// child-column indexes of the foreign keys to transaction(id); the edge
// indexes serve rollback, which deletes edges by transaction id.
var utxoIdentityIndexes = []utxoIdentityIndex{
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
		columns: []string{"payment_key"},
	},
	{
		name: "idx_utxo_staking_key", table: "utxo",
		columns: []string{"staking_key"},
		where:   "staking_key IS NOT NULL",
	},
	{
		name: "idx_utxo_staking_deleted_amount", table: "utxo",
		columns: []string{
			"credential_tag", "staking_key", "deleted_slot", "amount",
		},
		where: "staking_key IS NOT NULL",
	},
	{
		name: "idx_utxo_deleted_staking_amount", table: "utxo",
		columns: []string{
			"deleted_slot", "credential_tag", "staking_key", "amount",
		},
	},
	{
		name: "idx_utxo_deleted_payment_script", table: "utxo",
		columns: []string{"deleted_slot", "payment_script", "amount"},
	},
	{
		name: "idx_utxo_reference_edge_tx_id", table: "utxo_reference_edge",
		columns: []string{"tx_id"},
	},
	{
		name: "idx_utxo_collateral_edge_tx_id", table: "utxo_collateral_edge",
		columns: []string{"tx_id"},
	},
	{
		name: "idx_applied_transaction_slot", table: "applied_transaction",
		columns: []string{"slot"},
	},
	{
		name: "idx_applied_transaction_block_hash", table: "applied_transaction",
		columns: []string{"block_hash"},
	},
}

// mysqlIdentityPrefixColumns are the blob and text columns MySQL indexes by
// prefix.
var mysqlIdentityPrefixColumns = map[string]bool{
	"payment_key": true, "staking_key": true, "amount": true,
	"block_hash": true,
}

func (i utxoIdentityIndex) createSQL(dialect string) string {
	var b strings.Builder
	b.WriteString("CREATE ")
	if i.unique {
		b.WriteString("UNIQUE ")
	}
	b.WriteString("INDEX ")
	if dialect != "mysql" {
		b.WriteString("IF NOT EXISTS ")
	}
	b.WriteString(identityQuote(dialect, i.name) + " ON " +
		identityQuote(dialect, i.table) + " (")
	for n, column := range i.columns {
		if n > 0 {
			b.WriteString(", ")
		}
		b.WriteString(identityQuote(dialect, column))
		if dialect == "mysql" && mysqlIdentityPrefixColumns[column] {
			b.WriteString("(255)")
		}
	}
	b.WriteString(")")
	if i.where != "" && dialect != "mysql" {
		b.WriteString(" WHERE " + i.where)
	}
	return b.String()
}

func identityQuote(dialect, name string) string {
	if dialect == "mysql" {
		return "`" + name + "`"
	}
	return `"` + name + `"`
}

// utxoTransactionIdentityBackfill rebuilds the transaction and utxo tables
// around integer transaction identity.
//
// Phases, each resumable from its cursor:
//
//	tx      copy every transaction row into applied_transaction, and on
//	        SQLite into transaction_identity, with its id unchanged
//	imp     create an identity row for each hash an output or spender names
//	        that has none: the producer of an imported output and the
//	        spender of a spent one
//	utxo    copy utxo rows, resolving the producer and spender hashes to ids
//	ref,col copy the hash-keyed input edges, resolving the hash to an id
//	verify  compare row counts with the source tables
//	swap    replace the old tables with the new ones
//	idx     build the secondary indexes, one per transaction
//
// The producer of a row that already names one through transaction_id or
// collateral_return_for_tx_id is that transaction; the tx_id hash is read
// only for an imported row. An input edge whose transaction has no identity
// row is dropped, since no transaction can own it.
//
// SQLite builds transaction_identity and swaps it for transaction, because
// dropping the columns would rewrite the table once per column. PostgreSQL
// and MySQL keep transaction in place and drop its columns after the copy.
// Both rebuild utxo and swap it in; their foreign keys to utxo are dropped
// before and re-added after the swap.
func utxoTransactionIdentityBackfill(dialect string) Backfill {
	b := identityBackfill{dialect: dialect}
	return b.run
}

type identityBackfill struct {
	dialect string
}

// identityTable is the table that holds the identity rows while the
// backfill runs.
func (b identityBackfill) identityTable() string {
	if b.dialect == "sqlite" {
		return "transaction_identity"
	}
	return "transaction"
}

func (b identityBackfill) q(name string) string {
	return identityQuote(b.dialect, name)
}

func (b identityBackfill) run(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	phase, last, lastHash, err := parseIdentityCursor(batch.Cursor)
	if err != nil {
		return BatchResult{}, err
	}
	switch phase {
	case "":
		return b.init(ctx, batch)
	case identityPhaseTx:
		return b.copyTransactions(ctx, batch, last)
	case identityPhaseImp:
		return b.importIdentities(ctx, batch, last)
	case identityPhaseUtxo:
		return b.copyUtxos(ctx, batch, last)
	case identityPhaseRef:
		return b.copyEdges(ctx, batch, last, lastHash, identityPhaseRef)
	case identityPhaseCol:
		return b.copyEdges(ctx, batch, last, lastHash, identityPhaseCol)
	case identityPhaseVerify:
		return b.verify(ctx, batch)
	case identityPhaseSwap:
		return b.swap(ctx, batch)
	case identityPhaseIdx:
		return b.buildIndex(ctx, batch, int(last))
	default:
		return BatchResult{}, fmt.Errorf(
			"utxo identity backfill: unknown cursor %q", batch.Cursor,
		)
	}
}

func parseIdentityCursor(cursor string) (string, int64, []byte, error) {
	if cursor == "" {
		return "", 0, nil, nil
	}
	malformed := func() (string, int64, []byte, error) {
		return "", 0, nil, fmt.Errorf(
			"utxo identity backfill: malformed cursor %q", cursor,
		)
	}
	parts := strings.Split(cursor, ":")
	if len(parts) < 2 || len(parts) > 3 {
		return malformed()
	}
	last, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || last < 0 {
		return malformed()
	}
	var hash []byte
	if len(parts) == 3 {
		hash, err = hex.DecodeString(parts[2])
		if err != nil {
			return malformed()
		}
	}
	return parts[0], last, hash, nil
}

func identityCursor(phase string, position int64) string {
	return phase + ":" + strconv.FormatInt(position, 10)
}

func edgeCursor(phase string, utxoID int64, hash []byte) string {
	return identityCursor(phase, utxoID) + ":" + hex.EncodeToString(hash)
}

// init prepares the identity table on SQLite. Its unique hash index takes the
// name of the old transaction table's, so the old one is dropped first; the
// old table is read by id from here on, never by hash. Both new tables
// inherit their old table's autoincrement high-water mark, so an id freed by
// a rollback is never handed out again.
func (b identityBackfill) init(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	if b.dialect == "sqlite" {
		for _, statement := range []string{
			"DROP INDEX IF EXISTS idx_transaction_hash",
			"CREATE TABLE IF NOT EXISTS transaction_identity (" +
				"id integer PRIMARY KEY AUTOINCREMENT, hash blob NOT NULL)",
			"CREATE UNIQUE INDEX IF NOT EXISTS idx_transaction_hash " +
				"ON transaction_identity (hash)",
			"DELETE FROM sqlite_sequence WHERE name IN " +
				"('transaction_identity', 'utxo_identity')",
			"INSERT INTO sqlite_sequence (name, seq) " +
				"SELECT 'transaction_identity', seq FROM sqlite_sequence " +
				"WHERE name = 'transaction'",
			"INSERT INTO sqlite_sequence (name, seq) " +
				"SELECT 'utxo_identity', seq FROM sqlite_sequence " +
				"WHERE name = 'utxo'",
		} {
			if _, err := batch.Tx.ExecContext(ctx, statement); err != nil {
				return BatchResult{}, fmt.Errorf(
					"prepare transaction identity (%s): %w", statement, err,
				)
			}
		}
	}
	return BatchResult{
		Cursor: identityCursor(identityPhaseTx, 0),
	}, nil
}

// nextWindow returns the highest id of the next limit rows of table above
// last, or zero when none remain.
func (b identityBackfill) nextWindow(
	ctx context.Context,
	batch Batch,
	table, column string,
	last int64,
	extraWhere string,
) (int64, error) {
	var upper sql.NullInt64
	query := "SELECT MAX(id) FROM (SELECT " + b.q(column) + " AS id FROM " +
		b.q(table) + " WHERE " + b.q(column) + " > ?" + extraWhere +
		" ORDER BY " + b.q(column) + " LIMIT ?) AS window_ids"
	if err := batch.Tx.QueryRowContext(
		ctx, batch.Rebind(query), last, batch.Limit,
	).Scan(&upper); err != nil {
		return 0, fmt.Errorf("select %s window: %w", table, err)
	}
	return upper.Int64, nil
}

func (b identityBackfill) copyTransactions(
	ctx context.Context,
	batch Batch,
	last int64,
) (BatchResult, error) {
	upper, err := b.nextWindow(ctx, batch, "transaction", "id", last, "")
	if err != nil {
		return BatchResult{}, err
	}
	if upper == 0 {
		return BatchResult{
			Cursor: identityCursor(identityPhaseImp, 0),
		}, nil
	}
	if b.dialect == "sqlite" {
		if _, err := batch.Tx.ExecContext(ctx, `
INSERT INTO transaction_identity (id, hash)
SELECT id, hash FROM "transaction"
WHERE id > ? AND id <= ? AND hash IS NOT NULL`, last, upper); err != nil {
			return BatchResult{}, fmt.Errorf(
				"copy transaction identities: %w", err,
			)
		}
	}
	result, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO applied_transaction (
    tx_id, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
)
SELECT id, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
       block_index, valid
FROM `+b.q("transaction")+`
WHERE id > ? AND id <= ? AND hash IS NOT NULL`), last, upper)
	if err != nil {
		return BatchResult{}, fmt.Errorf("copy applied transactions: %w", err)
	}
	rows, _ := result.RowsAffected()
	return BatchResult{
		Cursor: identityCursor(identityPhaseTx, upper),
		Rows:   rows,
	}, nil
}

func (b identityBackfill) importIdentities(
	ctx context.Context,
	batch Batch,
	last int64,
) (BatchResult, error) {
	upper, err := b.nextWindow(ctx, batch, "utxo", "id", last, "")
	if err != nil {
		return BatchResult{}, err
	}
	if upper == 0 {
		return BatchResult{
			Cursor: identityCursor(identityPhaseUtxo, 0),
		}, nil
	}
	identity := b.q(b.identityTable())
	var rows int64
	for _, statement := range []string{
		// The producer of an imported output: no row links it, so its
		// hash is its only identity.
		`
INSERT INTO ` + identity + ` (hash)
SELECT DISTINCT u.tx_id FROM utxo AS u
WHERE u.id > ? AND u.id <= ?
  AND u.transaction_id IS NULL AND u.collateral_return_for_tx_id IS NULL
  AND u.tx_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM ` + identity + ` AS t WHERE t.hash = u.tx_id)`,
		// The spender of a spent output that no transaction row names.
		`
INSERT INTO ` + identity + ` (hash)
SELECT DISTINCT u.spent_at_tx_id FROM utxo AS u
WHERE u.id > ? AND u.id <= ? AND u.spent_at_tx_id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM ` + identity + ` AS t WHERE t.hash = u.spent_at_tx_id)`,
	} {
		result, err := batch.Tx.ExecContext(
			ctx, batch.Rebind(statement), last, upper,
		)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"create imported transaction identities: %w", err,
			)
		}
		n, _ := result.RowsAffected()
		rows += n
	}
	return BatchResult{
		Cursor: identityCursor(identityPhaseImp, upper),
		Rows:   rows,
	}, nil
}

func (b identityBackfill) copyUtxos(
	ctx context.Context,
	batch Batch,
	last int64,
) (BatchResult, error) {
	upper, err := b.nextWindow(ctx, batch, "utxo", "id", last, "")
	if err != nil {
		return BatchResult{}, err
	}
	if upper == 0 {
		return BatchResult{
			Cursor: identityCursor(identityPhaseRef, 0),
		}, nil
	}
	identity := b.q(b.identityTable())
	var mismatched int64
	if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*) FROM utxo AS u
WHERE u.id > ? AND u.id <= ?
  AND u.transaction_id IS NOT NULL AND u.tx_id IS NOT NULL
  AND u.transaction_id <> COALESCE(
      (SELECT t.id FROM `+identity+` AS t WHERE t.hash = u.tx_id),
      u.transaction_id)`), last, upper).Scan(&mismatched); err != nil {
		return BatchResult{}, fmt.Errorf(
			"count utxo producer mismatches: %w", err,
		)
	}
	if mismatched > 0 {
		batch.logger().Warn(
			"utxo rows name a producer whose id and hash disagree; the "+
				"linked producer id wins",
			"count", mismatched,
			"utxo_ids", fmt.Sprintf("%d-%d", last+1, upper),
		)
	}
	result, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO utxo_identity (
    id, tx_id, output_idx, collateral_return_for_tx_id, spent_at_tx_id,
    payment_key, staking_key, credential_tag, datum_hash, amount,
    payment_script, added_slot, deleted_slot
)
SELECT u.id,
       COALESCE(u.transaction_id, u.collateral_return_for_tx_id,
                (SELECT t.id FROM `+identity+` AS t WHERE t.hash = u.tx_id)),
       u.output_idx, u.collateral_return_for_tx_id,
       (SELECT t.id FROM `+identity+` AS t WHERE t.hash = u.spent_at_tx_id),
       u.payment_key, u.staking_key, u.credential_tag, u.datum_hash,
       u.amount, u.payment_script, u.added_slot, u.deleted_slot
FROM utxo AS u
WHERE u.id > ? AND u.id <= ?`), last, upper)
	if err != nil {
		return BatchResult{}, fmt.Errorf(
			"copy utxo rows %d-%d: %w", last+1, upper, err,
		)
	}
	rows, _ := result.RowsAffected()
	return BatchResult{
		Cursor: identityCursor(identityPhaseUtxo, upper),
		Rows:   rows,
	}, nil
}

func (b identityBackfill) copyEdges(
	ctx context.Context,
	batch Batch,
	last int64,
	lastHash []byte,
	phase string,
) (BatchResult, error) {
	source, target, next := "utxo_reference_input", "utxo_reference_edge",
		identityPhaseCol
	if phase == identityPhaseCol {
		source, target, next = "utxo_collateral_input",
			"utxo_collateral_edge", identityPhaseVerify
	}
	if lastHash == nil {
		lastHash = []byte{}
	}
	// The window is the next Limit edges after the cursor in primary key
	// order; its last edge bounds the copy. A short remainder has no
	// bound and is copied to the end.
	after := `(e.utxo_id > ? OR (e.utxo_id = ? AND e.transaction_hash > ?))`
	var upperID sql.NullInt64
	var upperHash []byte
	err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT e.utxo_id, e.transaction_hash FROM `+source+` AS e
WHERE `+after+`
ORDER BY e.utxo_id, e.transaction_hash
LIMIT 1 OFFSET ?`), last, last, lastHash, batch.Limit-1).Scan(
		&upperID, &upperHash,
	)
	bounded := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return BatchResult{}, fmt.Errorf("select %s window: %w", source, err)
	}
	window := after
	args := []any{last, last, lastHash}
	if bounded {
		window += ` AND (e.utxo_id < ? OR (e.utxo_id = ? ` +
			`AND e.transaction_hash <= ?))`
		args = append(args, upperID.Int64, upperID.Int64, upperHash)
	}
	identity := b.q(b.identityTable())
	var dropped int64
	if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*) FROM `+source+` AS e
WHERE `+window+`
  AND NOT EXISTS (SELECT 1 FROM `+identity+` AS t
                  WHERE t.hash = e.transaction_hash)`),
		args...).Scan(&dropped); err != nil {
		return BatchResult{}, fmt.Errorf("count unresolved %s: %w", source, err)
	}
	if dropped > 0 {
		batch.logger().Warn(
			"input edge names a transaction with no row and is dropped",
			"table", source, "count", dropped,
		)
	}
	result, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO `+target+` (utxo_id, tx_id)
SELECT e.utxo_id, t.id FROM `+source+` AS e
JOIN `+identity+` AS t ON t.hash = e.transaction_hash
WHERE `+window), args...)
	if err != nil {
		return BatchResult{}, fmt.Errorf("copy %s: %w", source, err)
	}
	rows, _ := result.RowsAffected()
	if !bounded {
		return BatchResult{Cursor: identityCursor(next, 0), Rows: rows}, nil
	}
	return BatchResult{
		Cursor: edgeCursor(phase, upperID.Int64, upperHash),
		Rows:   rows,
	}, nil
}

// verify refuses to swap when the copy is not complete: every old row must
// have a counterpart, and no transaction row may lack the hash the identity
// table requires.
func (b identityBackfill) verify(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	count := func(query string) (int64, error) {
		var n int64
		err := batch.Tx.QueryRowContext(ctx, query).Scan(&n)
		return n, err
	}
	transactions := b.q("transaction")
	noHash, err := count(
		"SELECT COUNT(*) FROM " + transactions + " WHERE hash IS NULL",
	)
	if err != nil {
		return BatchResult{}, fmt.Errorf("count transactions without hash: %w", err)
	}
	if noHash > 0 {
		return BatchResult{}, fmt.Errorf(
			"%d transaction rows have no hash; they cannot become identity rows",
			noHash,
		)
	}
	for _, pair := range [][2]string{
		{"utxo", "utxo_identity"},
		{"transaction", "applied_transaction"},
	} {
		oldQuery := "SELECT COUNT(*) FROM " + b.q(pair[0])
		if pair[0] == "transaction" && b.dialect != "sqlite" {
			// The identity rows live in this table on PostgreSQL and
			// MySQL, so the rows created for imported producers and
			// spenders sit beside the original ones. They take ids above
			// every original row, which the copy ordered by id.
			oldQuery += " WHERE id <= (SELECT COALESCE(MAX(tx_id), 0) FROM " +
				b.q("applied_transaction") + ")"
		}
		oldCount, err := count(oldQuery)
		if err != nil {
			return BatchResult{}, fmt.Errorf("count %s: %w", pair[0], err)
		}
		newCount, err := count("SELECT COUNT(*) FROM " + b.q(pair[1]))
		if err != nil {
			return BatchResult{}, fmt.Errorf("count %s: %w", pair[1], err)
		}
		if oldCount != newCount {
			return BatchResult{}, fmt.Errorf(
				"%s has %d rows but %s has %d",
				pair[0], oldCount, pair[1], newCount,
			)
		}
	}
	return BatchResult{Cursor: identityCursor(identityPhaseSwap, 0)}, nil
}

func (b identityBackfill) exec(
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

func (b identityBackfill) swap(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	var err error
	switch b.dialect {
	case "sqlite":
		err = b.exec(
			ctx, batch,
			"DROP TABLE IF EXISTS utxo_reference_input",
			"DROP TABLE IF EXISTS utxo_collateral_input",
			"DROP TABLE utxo",
			"ALTER TABLE utxo_identity RENAME TO utxo",
			`DROP TABLE "transaction"`,
			`ALTER TABLE transaction_identity RENAME TO "transaction"`,
		)
	case "postgres":
		err = b.swapPostgres(ctx, batch)
	case "mysql":
		err = b.swapMySQL(ctx, batch)
	default:
		err = fmt.Errorf("unsupported dialect %q", b.dialect)
	}
	if err != nil {
		return BatchResult{}, fmt.Errorf("swap utxo identity tables: %w", err)
	}
	return BatchResult{Cursor: identityCursor(identityPhaseIdx, 0)}, nil
}

// utxoChildForeignKeys are the foreign keys to utxo(id) that PostgreSQL and
// MySQL drop before the old utxo table and re-add against the new one.
var utxoChildForeignKeys = []struct{ table, name, column string }{
	{"asset", "fk_utxo_assets", "utxo_id"},
	{"utxo_address", "fk_utxo_address_utxo", "utxo_id"},
	{"utxo_pointer", "fk_utxo_pointer_utxo", "utxo_id"},
	{"utxo_reference_script", "fk_utxo_reference_script_utxo", "utxo_id"},
	{"utxo_reference_edge", "fk_utxo_reference_edge_utxo", "utxo_id"},
	{"utxo_collateral_edge", "fk_utxo_collateral_edge_utxo", "utxo_id"},
}

// transactionAppliedColumns are the transaction columns that move to
// applied_transaction.
var transactionAppliedColumns = []string{
	"block_hash", "metadata", "slot", "type", "fee", "collateral_fee", "ttl",
	"block_index", "valid",
}

func (b identityBackfill) swapPostgres(
	ctx context.Context,
	batch Batch,
) error {
	var sequenceStart sql.NullInt64
	if err := batch.Tx.QueryRowContext(ctx, `
SELECT GREATEST(
    (SELECT COALESCE(MAX(id), 0) FROM utxo),
    (SELECT last_value FROM utxo_id_seq))`).Scan(&sequenceStart); err != nil {
		return fmt.Errorf("read utxo id sequence: %w", err)
	}
	statements := []string{}
	for _, fk := range utxoChildForeignKeys {
		statements = append(statements,
			`ALTER TABLE "`+fk.table+`" DROP CONSTRAINT IF EXISTS "`+fk.name+`"`)
	}
	statements = append(statements,
		`DROP TABLE IF EXISTS "utxo_reference_input"`,
		`DROP TABLE IF EXISTS "utxo_collateral_input"`,
		`DROP TABLE "utxo"`,
		`ALTER TABLE "utxo_identity" RENAME TO "utxo"`,
		`ALTER INDEX "utxo_identity_pkey" RENAME TO "utxo_pkey"`,
		`ALTER SEQUENCE "utxo_identity_id_seq" RENAME TO "utxo_id_seq"`,
	)
	for _, fk := range utxoChildForeignKeys {
		statements = append(statements,
			`ALTER TABLE "`+fk.table+`" ADD CONSTRAINT "`+fk.name+
				`" FOREIGN KEY ("`+fk.column+`") REFERENCES "utxo"("id") `+
				`ON DELETE CASCADE`)
	}
	statements = append(statements,
		fmt.Sprintf(
			`SELECT setval('utxo_id_seq', %d, true)`, max(sequenceStart.Int64, 1),
		),
		`ALTER TABLE "transaction" ALTER COLUMN "hash" SET NOT NULL`,
	)
	drops := make([]string, len(transactionAppliedColumns))
	for i, column := range transactionAppliedColumns {
		drops[i] = `DROP COLUMN IF EXISTS "` + column + `"`
	}
	statements = append(statements,
		`ALTER TABLE "transaction" `+strings.Join(drops, ", "))
	return b.exec(ctx, batch, statements...)
}

func (b identityBackfill) swapMySQL(
	ctx context.Context,
	batch Batch,
) error {
	var nextAutoIncrement sql.NullInt64
	if err := batch.Tx.QueryRowContext(ctx, `
SELECT auto_increment FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'utxo'`,
	).Scan(&nextAutoIncrement); err != nil {
		return fmt.Errorf("read utxo auto_increment: %w", err)
	}
	for _, fk := range utxoChildForeignKeys {
		exists, err := b.mysqlConstraintExists(ctx, batch, fk.table, fk.name)
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
	if err := b.exec(ctx, batch,
		"DROP TABLE IF EXISTS `utxo_reference_input`",
		"DROP TABLE IF EXISTS `utxo_collateral_input`",
		"DROP TABLE `utxo`",
		"RENAME TABLE `utxo_identity` TO `utxo`",
	); err != nil {
		return err
	}
	if nextAutoIncrement.Valid {
		if err := b.exec(ctx, batch, fmt.Sprintf(
			"ALTER TABLE `utxo` AUTO_INCREMENT = %d",
			nextAutoIncrement.Int64,
		)); err != nil {
			return err
		}
	}
	for _, fk := range utxoChildForeignKeys {
		if err := b.exec(ctx, batch, "ALTER TABLE `"+fk.table+
			"` ADD CONSTRAINT `"+fk.name+"` FOREIGN KEY (`"+fk.column+
			"`) REFERENCES `utxo`(`id`) ON DELETE CASCADE"); err != nil {
			return err
		}
	}
	// One statement drops the moved columns: each separate ALTER would
	// rebuild the whole table.
	alter := "ALTER TABLE `transaction` MODIFY `hash` VARBINARY(255) NOT NULL"
	for _, column := range transactionAppliedColumns {
		var present int
		if err := batch.Tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'transaction'
  AND column_name = ?`, column).Scan(&present); err != nil {
			return fmt.Errorf("check transaction.%s: %w", column, err)
		}
		if present > 0 {
			alter += ", DROP COLUMN `" + column + "`"
		}
	}
	return b.exec(ctx, batch, alter)
}

func (b identityBackfill) mysqlConstraintExists(
	ctx context.Context,
	batch Batch,
	table, name string,
) (bool, error) {
	var n int
	if err := batch.Tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.table_constraints
WHERE table_schema = DATABASE() AND table_name = ?
  AND constraint_name = ?`, table, name).Scan(&n); err != nil {
		return false, fmt.Errorf("check constraint %s.%s: %w", table, name, err)
	}
	return n > 0, nil
}

// buildIndex creates the position'th secondary index. Each index is its own
// transaction so the write-ahead log is checkpointed between builds.
func (b identityBackfill) buildIndex(
	ctx context.Context,
	batch Batch,
	position int,
) (BatchResult, error) {
	if position >= len(utxoIdentityIndexes) {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	index := utxoIdentityIndexes[position]
	if b.dialect == "mysql" {
		var exists int
		if err := batch.Tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?`,
			index.table, index.name).Scan(&exists); err != nil {
			return BatchResult{}, fmt.Errorf("check %s: %w", index.name, err)
		}
		if exists > 0 {
			return BatchResult{
				Cursor: identityCursor(identityPhaseIdx, int64(position+1)),
			}, nil
		}
	}
	if _, err := batch.Tx.ExecContext(
		ctx, index.createSQL(b.dialect),
	); err != nil {
		return BatchResult{}, fmt.Errorf("create %s: %w", index.name, err)
	}
	return BatchResult{
		Cursor: identityCursor(identityPhaseIdx, int64(position+1)),
		Rows:   1,
	}, nil
}
