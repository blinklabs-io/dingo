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

package sqlite

import (
	"context"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"gorm.io/gorm"
)

// Fixed-shape SQL bulk writers for the hottest API-backfill flush paths.
//
// These replace GORM's reflection-based CreateInBatches on the dominant child
// tables. Each writer emits a multi-row INSERT whose statement string is stable
// for a given row count, so GORM's PrepareStmt cache (enabled on the write pool)
// compiles it once and reuses it, and no per-row model reflection happens.
// Conflict/idempotency semantics are kept identical to the GORM path.

// Column lists exclude the autoincrement primary key. They are asserted to
// match the GORM-migrated schema by TestBulkInsertColumnsMatchSchema, so a
// model change that drifts from these lists fails a test rather than silently
// writing the wrong columns.
var (
	keyWitnessCols = []string{
		"vkey", "signature", "public_key", "chain_code",
		"attributes", "transaction_id", "type",
	}
	witnessScriptCols = []string{"script_hash", "transaction_id", "type"}
	scriptCols        = []string{"hash", "content", "created_slot", "type"}
	plutusDataCols    = []string{"data", "transaction_id"}
	redeemerCols      = []string{
		"data", "transaction_id", "ex_units_memory",
		"ex_units_cpu", "index", "tag",
	}
	addressTxCols = []string{
		"payment_key", "staking_key", "credential_tag",
		"transaction_id", "slot", "tx_index",
	}
	utxoCols = []string{
		"transaction_id", "collateral_return_for_tx_id", "tx_id",
		"payment_key", "staking_key", "credential_tag", "datum_hash",
		"spent_at_tx_id", "referenced_by_tx_id", "collateral_by_tx_id",
		"added_slot", "deleted_slot", "amount", "output_idx", "payment_script",
	}
	assetCols = []string{
		"name", "name_hex", "policy_id", "fingerprint", "utxo_id", "amount",
	}
	transactionCols = []string{
		"hash", "block_hash", "metadata", "slot", "type",
		"fee", "ttl", "block_index", "valid",
	}
)

// Conflict clauses preserved verbatim from the prior GORM clause.OnConflict
// targets; their unique indexes are the idempotency contract for resumed
// backfill.
const (
	utxoConflictClause  = `ON CONFLICT ("tx_id","output_idx") DO NOTHING`
	assetConflictClause = `ON CONFLICT ("utxo_id","policy_id","name") DO NOTHING`
)

func appendUtxoRow(dst []any, u *models.Utxo) []any {
	// SpentAtTxId/ReferencedByTxId/CollateralByTxId are types.NullableHash,
	// whose driver.Valuer binds an empty value as SQL NULL (not an empty
	// blob), so the FK to transaction(hash) is correctly skipped for unspent/
	// unreferenced UTxOs. database/sql invokes the Valuer on these positional
	// args, so no normalization is needed here.
	return append(dst,
		u.TransactionID, u.CollateralReturnForTxID, u.TxId,
		u.PaymentKey, u.StakingKey, u.CredentialTag, u.DatumHash,
		u.SpentAtTxId, u.ReferencedByTxId, u.CollateralByTxId, u.AddedSlot,
		u.DeletedSlot, u.Amount, u.OutputIdx, u.PaymentScript,
	)
}

func appendAssetRow(dst []any, a *models.Asset) []any {
	return append(dst,
		a.Name, a.NameHex, a.PolicyId, a.Fingerprint, a.UtxoID, a.Amount,
	)
}

func appendTransactionRow(dst []any, t *models.Transaction) []any {
	return append(dst,
		t.Hash, t.BlockHash, t.Metadata, t.Slot, t.Type,
		t.Fee, t.TTL, t.BlockIndex, t.Valid,
	)
}

// insertReturningID runs an INSERT ... RETURNING id on the transaction's conn
// pool. It returns (id, true) for a row that was actually inserted, or
// (0, false) when an ON CONFLICT DO NOTHING skipped the insert (RETURNING
// yields no row). []byte args bind as blobs, like execRawOnConn.
func insertReturningID(db *gorm.DB, query string, args []any) (uint, bool, error) {
	ctx := db.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	rows, err := db.Statement.ConnPool.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, false, err
	}
	defer rows.Close() //nolint:errcheck
	if !rows.Next() {
		return 0, false, rows.Err()
	}
	var id uint
	if err := rows.Scan(&id); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// buildBulkInsertSQL returns a multi-row INSERT for nRows rows. For a given
// (table, cols, conflict, nRows) the result is byte-identical on every call,
// so GORM's PrepareStmt cache can reuse the compiled statement. Identifiers are
// double-quoted so reserved words (e.g. "index") are safe.
func buildBulkInsertSQL(
	table string,
	cols []string,
	conflict string,
	nRows int,
) string {
	var b strings.Builder
	b.WriteString(`INSERT INTO "`)
	b.WriteString(table)
	b.WriteString(`" (`)
	for i, c := range cols {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('"')
		b.WriteString(c)
		b.WriteByte('"')
	}
	b.WriteString(") VALUES ")
	// Single placeholder tuple, e.g. "(?,?,?)".
	var tuple strings.Builder
	tuple.WriteByte('(')
	for i := range cols {
		if i > 0 {
			tuple.WriteByte(',')
		}
		tuple.WriteByte('?')
	}
	tuple.WriteByte(')')
	t := tuple.String()
	for i := range nRows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(t)
	}
	if conflict != "" {
		b.WriteByte(' ')
		b.WriteString(conflict)
	}
	return b.String()
}

// execRawOnConn runs query on the transaction's underlying connection pool,
// bypassing GORM's clause builder, and returns the affected-row count. GORM's
// positional-var path expands a []byte argument into one bound value per byte;
// database/sql binds it as a single blob. The conn pool is the active
// transaction's, and (with PrepareStmt on) caches the compiled statement keyed
// by the stable query string. For INSERT ... ON CONFLICT DO NOTHING the count
// is the number of rows actually inserted (skipped conflicts are not counted).
func execRawOnConn(db *gorm.DB, query string, args []any) (int64, error) {
	ctx := db.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	res, err := db.Statement.ConnPool.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// execBulkInsert writes nRows rows to table in fixed-size chunks. Full chunks
// share one stable SQL string; a final partial chunk uses a statement built for
// its exact size. appendRow appends row i's column values, in cols order, to
// dst. conflict is an optional fixed clause (e.g. ON CONFLICT (...) DO NOTHING).
func execBulkInsert(
	db *gorm.DB,
	table string,
	cols []string,
	conflict string,
	nRows int,
	appendRow func(dst []any, i int) []any,
) error {
	if nRows == 0 {
		return nil
	}
	chunkRows := batchChunkRows
	args := make([]any, 0, chunkRows*len(cols))
	var fullSQL string
	i := 0
	for ; i+chunkRows <= nRows; i += chunkRows {
		if fullSQL == "" {
			fullSQL = buildBulkInsertSQL(table, cols, conflict, chunkRows)
		}
		args = args[:0]
		for j := i; j < i+chunkRows; j++ {
			args = appendRow(args, j)
		}
		if _, err := execRawOnConn(db, fullSQL, args); err != nil {
			return err
		}
	}
	if rem := nRows - i; rem > 0 {
		remSQL := buildBulkInsertSQL(table, cols, conflict, rem)
		args = args[:0]
		for j := i; j < nRows; j++ {
			args = appendRow(args, j)
		}
		if _, err := execRawOnConn(db, remSQL, args); err != nil {
			return err
		}
	}
	return nil
}

func insertKeyWitnesses(db *gorm.DB, items []models.KeyWitness) error {
	return execBulkInsert(
		db, "key_witness", keyWitnessCols, "", len(items),
		func(dst []any, i int) []any {
			w := &items[i]
			return append(dst,
				w.Vkey, w.Signature, w.PublicKey, w.ChainCode,
				w.Attributes, w.TransactionID, w.Type,
			)
		},
	)
}

func insertWitnessScripts(db *gorm.DB, items []models.WitnessScripts) error {
	return execBulkInsert(
		db, "witness_scripts", witnessScriptCols, "", len(items),
		func(dst []any, i int) []any {
			w := &items[i]
			return append(dst, w.ScriptHash, w.TransactionID, w.Type)
		},
	)
}

func insertScripts(db *gorm.DB, items []models.Script) error {
	return execBulkInsert(
		db, "script", scriptCols, `ON CONFLICT ("hash") DO NOTHING`,
		len(items),
		func(dst []any, i int) []any {
			s := &items[i]
			return append(dst, s.Hash, s.Content, s.CreatedSlot, s.Type)
		},
	)
}

func insertPlutusData(db *gorm.DB, items []models.PlutusData) error {
	return execBulkInsert(
		db, "plutus_data", plutusDataCols, "", len(items),
		func(dst []any, i int) []any {
			p := &items[i]
			return append(dst, p.Data, p.TransactionID)
		},
	)
}

func insertRedeemers(db *gorm.DB, items []models.Redeemer) error {
	return execBulkInsert(
		db, "redeemer", redeemerCols, "", len(items),
		func(dst []any, i int) []any {
			r := &items[i]
			return append(dst,
				r.Data, r.TransactionID, r.ExUnitsMemory,
				r.ExUnitsCPU, r.Index, r.Tag,
			)
		},
	)
}

func insertAddressTxs(db *gorm.DB, items []models.AddressTransaction) error {
	return execBulkInsert(
		db, "address_transaction", addressTxCols, "", len(items),
		func(dst []any, i int) []any {
			a := &items[i]
			return append(dst,
				a.PaymentKey, a.StakingKey, a.CredentialTag,
				a.TransactionID, a.Slot, a.TxIndex,
			)
		},
	)
}
