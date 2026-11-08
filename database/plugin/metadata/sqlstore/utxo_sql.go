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
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
)

// Statements that name an output by its transaction hash and output index
// exist in two forms.
//
// PostgreSQL and MySQL keep the unique (tx_id, output_idx) index, so their
// statements say "tx_id = ? AND output_idx = ?" and bind two arguments per
// reference.
//
// On SQLite an output produced by a transaction is identified by the
// producer's transaction row id and its output index, through the unique
// partial index idx_utxo_producer_output. The producer is the generated
// column producer_tx_id, COALESCE(transaction_id, collateral_return_for_tx_id):
// a collateral return shares its transaction's output-index space with the
// regular outputs, and a transaction that fails phase 2 has no regular
// outputs, so the pair is unique. An output with neither id (a ledger-state
// or snapshot import, or one recovered for a missing producer) is identified
// by its tx_id hash through the partial unique index idx_utxo_imported_ref.
// The hash index over every output is gone: it received an insert at a random
// leaf per output. A SQLite statement therefore resolves a reference to a row
// id through sqliteRefID, which binds the hash and index twice, once per arm,
// and every statement is written out here rather than derived at run time.
//
// Statements over a variable number of references come in a fixed set of
// sizes. A caller pads its arguments up to the next size with NULL, which
// matches no row, so each size is one statement text and one prepared
// statement. The texts are composed once, at package initialisation, by
// sqliteRefStatements; nothing builds SQL text per call.

const (
	// sharedRefEq is the reference predicate of the PostgreSQL and MySQL
	// statements.
	sharedRefEq      = "tx_id = ? AND output_idx = ?"
	sharedRefEqAlias = "u.tx_id = ? AND u.output_idx = ?"
	// sharedTxIDIn opens the hash list of the shared statements.
	sharedTxIDIn = "tx_id IN ("

	// sqliteRefID is the id of the output named by (hash, index): the
	// produced output if there is one, else the imported one. Each arm
	// matches at most one row, so the whole is a scalar, and "id = <scalar>"
	// plans as a primary-key lookup whatever other predicates the statement
	// carries. An "id IN (subquery)" form does not: with a liveness predicate
	// beside it SQLite prefers a deleted_slot index and visits every live row.
	// Its four arguments are hash, index, hash, index.
	sqliteRefID = `COALESCE(` +
		`(SELECT ref.id FROM utxo AS ref WHERE ref.producer_tx_id = ` +
		`(SELECT ref_tx.id FROM "transaction" AS ref_tx WHERE ref_tx.hash = ?) ` +
		`AND ref.output_idx = ?), ` +
		`(SELECT ref.id FROM utxo AS ref ` +
		`WHERE ref.transaction_id IS NULL AND ref.collateral_return_for_tx_id IS NULL ` +
		`AND ref.tx_id = ? AND ref.output_idx = ?))`

	// sqliteRefEq and sqliteRefEqAlias replace sharedRefEq.
	sqliteRefEq      = "id = " + sqliteRefID
	sqliteRefEqAlias = "u.id = " + sqliteRefID

	// sqliteRefIDsInHead, Mid and Tail surround two hash lists, which are
	// the same hashes, to select the ids of every output of those
	// transactions: the produced outputs, then the imported ones.
	sqliteRefIDsInHead = `SELECT ref.id FROM utxo AS ref ` +
		`WHERE ref.producer_tx_id IN ` +
		`(SELECT ref_tx.id FROM "transaction" AS ref_tx WHERE ref_tx.hash IN (`
	sqliteRefIDsInMid = `)) UNION ALL SELECT ref.id FROM utxo AS ref ` +
		`WHERE ref.transaction_id IS NULL AND ref.collateral_return_for_tx_id IS NULL ` +
		`AND ref.tx_id IN (`
	sqliteRefIDsInTail = `)`

	// sqliteRefIDsIn1 is sqliteRefIDsIn for one hash.
	sqliteRefIDsIn1 = sqliteRefIDsInHead + `?` +
		sqliteRefIDsInMid + `?` + sqliteRefIDsInTail

	// sqliteUtxoConflictTargets names both unique indexes of SQLite. A produced
	// output can only collide in the first and an imported one only in the
	// second, so one statement serves both kinds. A produced output
	// colliding with an imported row, or the reverse, is not a unique
	// violation; see existingUtxoRefs.
	sqliteUtxoConflictTargets = `ON CONFLICT (producer_tx_id, output_idx) WHERE ` +
		`producer_tx_id IS NOT NULL ` +
		`DO NOTHING ON CONFLICT (tx_id, output_idx) WHERE ` +
		`transaction_id IS NULL AND collateral_return_for_tx_id IS NULL ` +
		`DO NOTHING`
	sharedUtxoConflictTargets = `ON CONFLICT (tx_id, output_idx) DO NOTHING`
)

// utxoSQL holds the statement text of one dialect family for every statement
// that names an output by reference. sharedUtxoSQL is PostgreSQL and MySQL;
// sqliteUtxoSQL is SQLite. Callers take the set from Store.utxoSQL and bind
// arguments through its methods, so the difference in argument count stays in
// this file.
type utxoSQL struct {
	sqlite bool

	getSpendState      string
	getSpendStateHash  string
	repairConflicted   string
	getLiveByRef       string
	getIncludingSpent  string
	idByRef            string
	idByOutput         string
	amountByOutput     string
	spendScriptHash    string
	markDeletedAtSlot  string
	countByRef         string
	hydrateTransaction string
	hydrateCollateral  string
	clearStaleSpend    string
	insertIgnore       string
	insertResult       string
	insertResultIgnore string
	historyTxFilter    string
}

var sharedUtxoSQL = &utxoSQL{
	getSpendState:     getUtxoSpendStateHead + sharedRefEq,
	getSpendStateHash: getUtxoSpendStateHashHead + sharedRefEq,
	repairConflicted:  repairConflictedUtxoHead + sharedRefEq + repairConflictedUtxoTail,
	getLiveByRef:      getLiveUtxoByRefHead + sharedRefEq + " AND deleted_slot = 0",
	getIncludingSpent: getUtxoIncludingSpentByRefHead + sharedRefEq,
	idByRef:           utxoIDByRefHead + sharedRefEq,
	idByOutput:        utxoIDByOutputHead + sharedRefEq,
	amountByOutput:    utxoAmountByOutputHead + sharedRefEq,
	spendScriptHash:   spendScriptHashHead + sharedRefEq,
	markDeletedAtSlot: markUtxoDeletedAtSlotHead + sharedRefEq +
		markUtxoDeletedAtSlotTail,
	countByRef: countUtxoByRefHead + sharedRefEq,
	hydrateTransaction: hydrateUtxoTransactionHead + sharedRefEq +
		hydrateUtxoTransactionTail,
	hydrateCollateral: hydrateUtxoCollateralHead + sharedRefEq +
		hydrateUtxoCollateralTail,
	clearStaleSpend: clearStaleUtxoSpendHead + sharedRefEq +
		clearStaleUtxoSpendTail,
	insertIgnore: insertUtxoIgnoreConflictHead + sharedUtxoConflictTargets +
		insertUtxoIgnoreConflictTail,
	historyTxFilter: " AND utxo." + sharedTxIDIn + "?)",
}

var sqliteUtxoSQL = &utxoSQL{
	sqlite:            true,
	getSpendState:     getUtxoSpendStateHead + sqliteRefEq,
	getSpendStateHash: getUtxoSpendStateHashHead + sqliteRefEq,
	repairConflicted:  repairConflictedUtxoHead + sqliteRefEq + repairConflictedUtxoTail,
	getLiveByRef:      getLiveUtxoByRefHead + sqliteRefEq + " AND deleted_slot = 0",
	getIncludingSpent: getUtxoIncludingSpentByRefHead + sqliteRefEq,
	idByRef:           utxoIDByRefHead + sqliteRefEq,
	idByOutput:        utxoIDByOutputHead + sqliteRefEq,
	amountByOutput:    utxoAmountByOutputHead + sqliteRefEq,
	spendScriptHash:   spendScriptHashHead + sqliteRefEq,
	markDeletedAtSlot: markUtxoDeletedAtSlotHead + sqliteRefEq +
		markUtxoDeletedAtSlotTail,
	countByRef: countUtxoByRefHead + sqliteRefEq,
	hydrateTransaction: hydrateUtxoTransactionHead + sqliteRefEq +
		hydrateUtxoTransactionTail,
	hydrateCollateral: hydrateUtxoCollateralHead + sqliteRefEq +
		hydrateUtxoCollateralTail,
	clearStaleSpend: clearStaleUtxoSpendHead + sqliteRefEq +
		clearStaleUtxoSpendTail,
	insertIgnore: insertUtxoIgnoreConflictHead + sqliteUtxoConflictTargets +
		insertUtxoIgnoreConflictTail,
	insertResult: insertUtxoResultStatement,
	insertResultIgnore: insertUtxoResultIgnoreHead +
		sqliteUtxoConflictTargets,
	historyTxFilter: " AND utxo.id IN (" + sqliteRefIDsIn1 + ")",
}

// utxoSQL returns the statement set of the store's dialect.
func (s *Store) utxoSQL() *utxoSQL {
	if s.dialect.Name() == "sqlite" {
		return sqliteUtxoSQL
	}
	return sharedUtxoSQL
}

// bindRef appends the arguments of one reference predicate.
func (u *utxoSQL) bindRef(args []any, hash, idx any) []any {
	if u.sqlite {
		return append(args, hash, idx, hash, idx)
	}
	return append(args, hash, idx)
}

// refArgs is the argument list of a statement whose only placeholders are
// one reference predicate.
func (u *utxoSQL) refArgs(hash, idx any) []any {
	return u.bindRef(make([]any, 0, 4), hash, idx)
}

// refParams is the number of bound arguments one reference takes.
func (u *utxoSQL) refParams() int {
	if u.sqlite {
		return 4
	}
	return 2
}

// bindHashes appends the arguments of a list of hashes: once, or twice on
// SQLite, where the list feeds both arms.
func (u *utxoSQL) bindHashes(args []any, hashes [][]byte, size int) []any {
	repeats := 1
	if u.sqlite {
		repeats = 2
	}
	for range repeats {
		for _, hash := range hashes {
			args = append(args, hash)
		}
		for i := len(hashes); i < size; i++ {
			args = append(args, nil)
		}
	}
	return args
}

// Heads and tails of the statements that exist in both forms. The reference
// predicate sits between them.
const (
	getUtxoSpendStateHead = `-- name: GetUtxoSpendState :one
SELECT deleted_slot, spent_by_tx_id
FROM utxo WHERE `

	// getUtxoSpendStateHashHead projects the spender's hash for readers
	// that report it; the apply path compares transaction ids and uses
	// getUtxoSpendStateHead.
	getUtxoSpendStateHashHead = `-- name: GetUtxoSpendStateWithHash :one
SELECT deleted_slot, (SELECT spender.hash FROM "transaction" AS spender
    WHERE spender.id = utxo.spent_by_tx_id)
FROM utxo WHERE `

	repairConflictedUtxoHead = `-- name: RepairConflictedUtxo :one
UPDATE utxo
SET transaction_id = COALESCE(transaction_id, ?),
    collateral_return_for_tx_id = COALESCE(collateral_return_for_tx_id, ?)
WHERE `
	repairConflictedUtxoTail = `
RETURNING id`

	getLiveUtxoByRefHead = "-- name: GetLiveUtxoByRef :one\nSELECT " +
		utxoRowColumns + " FROM utxo WHERE "
	getUtxoIncludingSpentByRefHead = "-- name: GetUtxoIncludingSpentByRef :one\nSELECT " +
		utxoRowColumns + " FROM utxo WHERE "

	utxoIDByRefHead        = "-- name: ResolveUtxoIDByRef :one\nSELECT id FROM utxo WHERE "
	utxoIDByOutputHead     = "-- name: GetUtxoIDByOutput :one\nSELECT id FROM utxo WHERE "
	utxoAmountByOutputHead = "-- name: GetUtxoAmountByOutput :one\nSELECT amount FROM utxo WHERE "

	spendScriptHashHead = `-- name: GetSpendScriptHashByOutput :one
SELECT payment_key, payment_script FROM utxo
WHERE `

	markUtxoDeletedAtSlotHead = `
UPDATE utxo
SET deleted_slot = ?, spent_by_tx_id = ?
WHERE `
	markUtxoDeletedAtSlotTail = `
  AND spent_by_tx_id IS NULL
  AND (deleted_slot = 0 OR deleted_slot = ?)`

	countUtxoByRefHead = `
SELECT COUNT(*) FROM utxo WHERE `

	hydrateUtxoTransactionHead = `-- name: HydrateUtxoTransaction :execrows
UPDATE utxo
SET transaction_id = ?, added_slot = ?
WHERE `
	hydrateUtxoTransactionTail = ` AND transaction_id IS NULL`

	hydrateUtxoCollateralHead = `-- name: HydrateUtxoCollateralReturn :execrows
UPDATE utxo
SET collateral_return_for_tx_id = ?, added_slot = ?
WHERE `
	hydrateUtxoCollateralTail = `
  AND collateral_return_for_tx_id IS NULL`

	clearStaleUtxoSpendHead = `-- name: ClearStaleUtxoSpend :execrows
UPDATE utxo
SET deleted_slot = 0, spent_by_tx_id = NULL
WHERE `
	clearStaleUtxoSpendTail = ` AND deleted_slot > 0`

	insertUtxoIgnoreConflictHead = `-- name: InsertUtxoIfAbsent :one
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_by_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ` + spentByPlaceholder + `, ?, ?, ?, ?, ?, ?, ?)
`
	insertUtxoIgnoreConflictTail = `
RETURNING id`
)

// insertUtxoResultStatement and insertUtxoResultIgnoreHead are the SQLite
// Exec-result forms of the single-row inserts: they capture the inserted id
// in the Exec result before releasing the connection, avoiding a RETURNING
// cursor. A skipped conflict leaves LastInsertId unchanged, so callers check
// RowsAffected first.
const (
	insertUtxoResultStatement = `-- name: InsertUtxoResult :execresult
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_by_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ` + spentByPlaceholder + `, ?, ?, ?, ?, ?, ?, ?)`

	insertUtxoResultIgnoreHead = `-- name: InsertUtxoIfAbsentResult :execresult
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_by_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ` + spentByPlaceholder + `, ?, ?, ?, ?, ?, ?, ?)
`
)

// Statement sizes of the SQLite statements over lists. A list shorter than
// its size is padded with NULL arguments.
var (
	// sqliteInListSizes are the hash-list sizes, up to the 400 hashes one
	// statement may name: with two lists of 400 bound arguments each the
	// statement stays under SQLite's conservative 999-parameter limit.
	sqliteInListSizes = [...]int{1, 2, 4, 8, 16, 32, 64, 128, 256, 400}
	// sqliteExactRefSizes are the reference counts of the exact-reference
	// lookup, up to utxoRefsMaxPerQuery.
	sqliteExactRefSizes = [...]int{1, 2, 4, 8, 16, 32, 64, 128, 200}
	// sqliteOrRefSizes are the reference counts of the OR-of-references
	// delete: 128 references bind 512 arguments.
	sqliteOrRefSizes = [...]int{1, 2, 4, 8, 16, 32, 64, 128}
)

// sizeFor returns the smallest of sizes that holds n, or the largest.
func sizeFor(sizes []int, n int) int {
	for _, size := range sizes {
		if n <= size {
			return size
		}
	}
	return sizes[len(sizes)-1]
}

// sqliteRefStatements are the SQLite statement texts over lists of
// references, keyed by size. They are composed once and read-only.
type sqliteRefStatements struct {
	stakeConsumedDelta map[int]string
	stakeRefsByTxIDs   map[int]string
	deletionRows       map[int]string
	exactRefs          map[int]string
	deleteRefs         map[int]string
	addressInputs      map[int]string
	edgeInsert         map[string]map[int]string
	consumeBatch       [2]map[int]string
	insertBatch        map[int]string
	importedProbe      map[int]string
	producedProbe      map[int]string
}

// sqliteRefs is the statement set of SQLite. This is the one place that
// composes SQL text over a variable count: each statement is a fixed head,
// a list fragment repeated size times, and a fixed tail, and the sizes are
// the bounded sets above (the apply path's batch sizes, powers of two, and
// the 200 and 400 caps), so the set of distinct texts is finite.
var sqliteRefs = newSQLiteRefStatements()

func newSQLiteRefStatements() *sqliteRefStatements {
	st := &sqliteRefStatements{
		stakeConsumedDelta: make(map[int]string),
		stakeRefsByTxIDs:   make(map[int]string),
		deletionRows:       make(map[int]string),
		exactRefs:          make(map[int]string),
		deleteRefs:         make(map[int]string),
		addressInputs:      make(map[int]string),
		edgeInsert: map[string]map[int]string{
			utxoCollateralEdgeTable: {},
			utxoReferenceEdgeTable:  {},
		},
		consumeBatch:  [2]map[int]string{{}, {}},
		insertBatch:   make(map[int]string),
		importedProbe: make(map[int]string),
		producedProbe: make(map[int]string),
	}
	for _, n := range sqliteInListSizes {
		list := bindPlaceholders(n)
		ids := "id IN (" + sqliteRefIDsInHead + list +
			sqliteRefIDsInMid + list + sqliteRefIDsInTail + ")"
		st.stakeConsumedDelta[n] = "-- name: GetUtxoStakeConsumedDeltaBatch :many\n" +
			"SELECT tx_id, output_idx, credential_tag, staking_key, amount " +
			"FROM utxo WHERE " + ids
		st.stakeRefsByTxIDs[n] = "-- name: GetUtxoStakeRefsByTxIDs :many\n" +
			"SELECT tx_id, output_idx, deleted_slot, credential_tag, " +
			"staking_key FROM utxo WHERE " + ids
		st.deletionRows[n] = "-- name: GetUtxoRowsByTxIDs :many\n" +
			"SELECT id, tx_id, output_idx, deleted_slot, credential_tag, " +
			"staking_key FROM utxo WHERE " + ids
	}
	for _, n := range sqliteExactRefSizes {
		st.exactRefs[n] = "id IN (" + joinN(
			n, " UNION ALL ", "SELECT id FROM utxo WHERE "+sqliteRefEq,
		) + ")"
	}
	for _, n := range sqliteOrRefSizes {
		st.deleteRefs[n] = "DELETE FROM utxo WHERE " + joinN(
			n, " OR ", "("+sqliteRefEq+")",
		)
	}
	for _, n := range [...]int{1, 2, 4, 8, 16, 32} {
		st.addressInputs[n] = "-- name: GetAddressTransactionInputs :many" +
			joinN(n, " UNION ALL ", "\nSELECT payment_key, payment_script, "+
				"credential_tag, staking_key\nFROM utxo WHERE "+sqliteRefEq)
		for table, bySize := range st.edgeInsert {
			bySize[n] = "-- name: InsertUtxoReferenceBatch :execrows\n" +
				"INSERT INTO " + table + " (utxo_id, transaction_id)\n" +
				"SELECT u.id, ? FROM utxo AS u\n" +
				"WHERE (" + joinN(n, " OR ", "("+sqliteRefEqAlias+")") + ")\n" +
				"  AND NOT EXISTS (\n" +
				"      SELECT 1 FROM " + table + " AS r\n" +
				"      WHERE r.utxo_id = u.id AND r.transaction_id = ?\n" +
				"  )"
		}
	}
	const arm = "SELECT id FROM utxo WHERE " + sqliteRefEq +
		" AND deleted_slot = 0 AND spent_by_tx_id IS NULL"
	for n := 2; n <= utxoSpendBatchSize; n++ {
		arms := joinN(n, " UNION ALL ", arm)
		st.consumeBatch[0][n] = "-- name: ConsumeUtxosBatch :many\n" +
			"UPDATE utxo\nSET deleted_slot = ?, spent_by_tx_id = ?\n" +
			"WHERE id IN (" + arms + ")\nRETURNING tx_id, output_idx"
		st.consumeBatch[1][n] = "-- name: ConsumeUtxosBatchReturningStake :many\n" +
			"UPDATE utxo\nSET deleted_slot = ?, spent_by_tx_id = ?\n" +
			"WHERE id IN (" + arms + ")\n" +
			"RETURNING tx_id, output_idx, credential_tag, staking_key, amount"
	}
	const row = "(?,?,?,?,?,?,?," + spentByPlaceholder + ",?,?,?,?,?,?,?)"
	for n := 1; n <= utxoInsertBatchSize; n++ {
		st.insertBatch[n] = "-- name: InsertUtxoBatch :many\n" +
			"INSERT INTO utxo (\n" +
			"    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,\n" +
			"    staking_key, credential_tag, datum_hash, spent_by_tx_id,\n" +
			"    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,\n" +
			"    amount, output_idx, payment_script\n" +
			") VALUES " + joinN(n, ",", row) + "\n" +
			sqliteUtxoConflictTargets + "\nRETURNING id, tx_id, output_idx"
	}
	for n := 1; n <= maxImportedProbeHashes; n *= 2 {
		st.importedProbe[n] = "-- name: GetImportedUtxoKeys :many\n" +
			"SELECT tx_id, output_idx FROM utxo INDEXED BY idx_utxo_imported_ref " +
			"WHERE transaction_id IS NULL AND collateral_return_for_tx_id IS NULL " +
			"AND tx_id IN (" + bindPlaceholders(n) + ")"
	}
	for n := 1; n <= maxImportedProbeHashes; n *= 2 {
		st.producedProbe[n] = "-- name: GetProducedUtxoKeys :many\n" +
			`SELECT t.hash, u.output_idx FROM "transaction" AS t ` +
			"JOIN utxo AS u ON u.producer_tx_id = t.id " +
			"WHERE t.hash IN (" + bindPlaceholders(n) + ")"
	}
	return st
}

func joinN(n int, sep, item string) string {
	return strings.TrimSuffix(strings.Repeat(item+sep, n), sep)
}

// stakeConsumedDelta is the lookup of the consumed outputs' stake
// credentials by transaction hash, for up to the 400 hashes of one batch.
func (u *utxoSQL) stakeConsumedDelta(hashes [][]byte) (string, []any) {
	size := sizeFor(sqliteInListSizes[:], len(hashes))
	if !u.sqlite {
		return utxoStakeConsumedDeltaSharedQuery(size),
			u.bindHashes(make([]any, 0, size), hashes, size)
	}
	return sqliteRefs.stakeConsumedDelta[size],
		u.bindHashes(make([]any, 0, 2*size), hashes, size)
}

// stakeRefsByTxIDs is the lookup of the stake credentials of every output of
// the given transactions, for up to 400 hashes.
func (u *utxoSQL) stakeRefsByTxIDs(hashes [][]byte) (string, []any) {
	if !u.sqlite {
		return utxoStakeRefsByTxIDSharedQuery(len(hashes)),
			u.bindHashes(make([]any, 0, len(hashes)), hashes, len(hashes))
	}
	size := sizeFor(sqliteInListSizes[:], len(hashes))
	return sqliteRefs.stakeRefsByTxIDs[size],
		u.bindHashes(make([]any, 0, 2*size), hashes, size)
}

// deletionRows is the lookup of the live rows of every output of the given
// transactions, for up to 400 hashes. lockRows is a PostgreSQL and MySQL
// row lock, which SQLite has no use for.
func (u *utxoSQL) deletionRows(
	hashes [][]byte,
	lockRows bool,
) (string, []any) {
	if !u.sqlite {
		return utxoDeletionRowsSharedQuery(len(hashes), lockRows),
			u.bindHashes(make([]any, 0, len(hashes)), hashes, len(hashes))
	}
	size := sizeFor(sqliteInListSizes[:], len(hashes))
	return sqliteRefs.deletionRows[size],
		u.bindHashes(make([]any, 0, 2*size), hashes, size)
}

// deleteRefsChunk is the number of references one DELETE names.
func (u *utxoSQL) deleteRefsChunk(parameterLimit int) int {
	if u.sqlite {
		return sqliteOrRefSizes[len(sqliteOrRefSizes)-1]
	}
	return parameterLimit / 2
}

// deleteRefs is the DELETE of the outputs named by ids, at most
// deleteRefsChunk of them. On SQLite the list is padded to its size by
// repeating the last reference, which a DELETE of a set of rows does not
// notice.
func (u *utxoSQL) deleteRefs(ids []models.UtxoId) (string, []any) {
	if !u.sqlite {
		parts := make([]string, len(ids))
		args := make([]any, 0, len(ids)*2)
		for i := range ids {
			parts[i] = "(" + sharedRefEq + ")"
			args = append(args, ids[i].Hash, ids[i].Idx)
		}
		return "DELETE FROM utxo WHERE " + strings.Join(parts, " OR "), args
	}
	size := sizeFor(sqliteOrRefSizes[:], len(ids))
	args := make([]any, 0, size*4)
	for _, id := range ids {
		args = u.bindRef(args, id.Hash, id.Idx)
	}
	last := ids[len(ids)-1]
	for i := len(ids); i < size; i++ {
		args = u.bindRef(args, last.Hash, last.Idx)
	}
	return sqliteRefs.deleteRefs[size], args
}

// exactRefs is the predicate selecting exactly the outputs named by refs, at
// most utxoRefsMaxPerQuery of them, and its arguments. On SQLite the list is
// padded to its size with NULL references, which match no row.
func (u *utxoSQL) exactRefs(refs []models.UtxoId) (string, []any) {
	if !u.sqlite {
		args := make([]any, 0, len(refs)*2)
		for _, ref := range refs {
			args = append(args, ref.Hash, ref.Idx)
		}
		return utxoRefsExactSharedQuery(len(refs)), args
	}
	size := sizeFor(sqliteExactRefSizes[:], len(refs))
	args := make([]any, 0, size*4)
	for _, ref := range refs {
		args = u.bindRef(args, ref.Hash, ref.Idx)
	}
	for i := len(refs); i < size; i++ {
		args = append(args, nil, nil, nil, nil)
	}
	return sqliteRefs.exactRefs[size], args
}

// stakeConsumedDeltaQuery is the statement text of stakeConsumedDelta for a
// list of size hashes.
func (u *utxoSQL) stakeConsumedDeltaQuery(size int) string {
	if !u.sqlite {
		return utxoStakeConsumedDeltaSharedQuery(size)
	}
	return sqliteRefs.stakeConsumedDelta[size]
}

// addressInputsQuery is the input-address lookup of size references.
func (u *utxoSQL) addressInputsQuery(size int) string {
	if !u.sqlite {
		return addressTransactionInputSharedQuery(size)
	}
	return sqliteRefs.addressInputs[size]
}

// edgeInsertQuery is the insert of the edges of size references into
// edgeTable.
func (u *utxoSQL) edgeInsertQuery(edgeTable string, size int) string {
	if !u.sqlite {
		return utxoReferenceInsertBatchSharedSQL(edgeTable, size)
	}
	return sqliteRefs.edgeInsert[edgeTable][size]
}
