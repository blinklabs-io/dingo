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
// resolve the hash to the producer's transaction row id in a scalar subquery
// and look the row up by the unique (tx_id, output_idx) index. One predicate,
// refEq, serves every dialect and every kind of output: an output produced by
// an applied transaction, an imported one and a collateral return all carry
// the producer's row id in tx_id. A hash with no transaction row resolves to
// NULL and matches nothing.
//
// Statements over a variable number of references come in a fixed set of
// sizes on SQLite. A caller pads its arguments up to the next size with NULL,
// which matches no row, so each size is one statement text and one prepared
// statement. The texts are composed once, at package initialisation, by
// sqliteRefStatements; nothing builds SQL text per call. PostgreSQL and MySQL
// build the text of the exact length.

const (
	// refEq is the reference predicate of every statement over one output.
	refEq      = `tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = ?`
	refEqAlias = `u.tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND u.output_idx = ?`

	utxoRowByHashFrom = ` FROM "transaction" AS t `
	utxoRowByHashOn   = ` utxo AS u ON u.tx_id = t.id WHERE t.hash IN (`
)

// utxoRowByHashJoin selects the rows of the outputs of the transactions whose
// hashes follow it. On SQLite the join starts from the transaction hash
// index; a plain JOIN lets SQLite choose to scan utxo instead, and CROSS JOIN
// fixes the loop order. PostgreSQL does not accept ON after CROSS JOIN and
// plans the lookup from the hash index on its own.
func utxoRowByHashJoin(sqlite bool) string {
	if sqlite {
		return utxoRowByHashFrom + "CROSS JOIN" + utxoRowByHashOn
	}
	return utxoRowByHashFrom + "JOIN" + utxoRowByHashOn
}

// utxoSQL holds the statement text of one dialect family for every statement
// that names an output by reference. The SQLite and shared sets differ only
// in the single-row insert forms.
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
	hydrate            string
	clearStaleSpend    string
	insertIgnore       string
	insertResult       string
	insertResultIgnore string
	historyTxFilter    string
}

var sharedUtxoSQL = newUtxoSQL(false)

var sqliteUtxoSQL = newUtxoSQL(true)

func newUtxoSQL(sqlite bool) *utxoSQL {
	u := &utxoSQL{
		sqlite:            sqlite,
		getSpendState:     getUtxoSpendStateHead + refEq,
		getSpendStateHash: getUtxoSpendStateHashHead + refEq,
		repairConflicted:  repairConflictedUtxoHead + refEq + repairConflictedUtxoTail,
		getLiveByRef:      getLiveUtxoByRefHead + refEq + " AND deleted_slot = 0",
		getIncludingSpent: getUtxoIncludingSpentByRefHead + refEq,
		idByRef:           utxoIDByRefHead + refEq,
		idByOutput:        utxoIDByOutputHead + refEq,
		amountByOutput:    utxoAmountByOutputHead + refEq,
		spendScriptHash:   spendScriptHashHead + refEq,
		markDeletedAtSlot: markUtxoDeletedAtSlotHead + refEq +
			markUtxoDeletedAtSlotTail,
		countByRef: countUtxoByRefHead + refEq,
		hydrate:    hydrateUtxoHead + refEq,
		clearStaleSpend: clearStaleUtxoSpendHead + refEq +
			clearStaleUtxoSpendTail,
		insertIgnore:    insertUtxoIgnoreConflictStatement,
		historyTxFilter: " AND utxo.tx_id = (SELECT id FROM \"transaction\" WHERE hash = ?)",
	}
	if sqlite {
		u.insertResult = insertUtxoResultStatement
		u.insertResultIgnore = insertUtxoResultIgnoreStatement
	}
	return u
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
	return append(args, hash, idx)
}

// refArgs is the argument list of a statement whose only placeholders are
// one reference predicate.
func (u *utxoSQL) refArgs(hash, idx any) []any {
	return u.bindRef(make([]any, 0, 2), hash, idx)
}

// refParams is the number of bound arguments one reference takes.
func (u *utxoSQL) refParams() int {
	return 2
}

// bindHashes appends the arguments of a list of hashes, padded with NULL up
// to size.
func (u *utxoSQL) bindHashes(args []any, hashes [][]byte, size int) []any {
	for _, hash := range hashes {
		args = append(args, hash)
	}
	for i := len(hashes); i < size; i++ {
		args = append(args, nil)
	}
	return args
}

// Heads and tails of the statements that name one output. The reference
// predicate sits between them.
const (
	getUtxoSpendStateHead = `-- name: GetUtxoSpendState :one
SELECT deleted_slot, spent_at_tx_id
FROM utxo WHERE `

	// getUtxoSpendStateHashHead projects the spender's hash for readers
	// that report it; the apply path compares transaction ids and uses
	// getUtxoSpendStateHead.
	getUtxoSpendStateHashHead = `-- name: GetUtxoSpendStateWithHash :one
SELECT deleted_slot, spent_at_tx_id, (SELECT spender.hash FROM "transaction" AS spender
    WHERE spender.id = utxo.spent_at_tx_id)
FROM utxo WHERE `

	// repairConflictedUtxoHead marks the collateral return of a producer
	// whose output an import stored before the producer was applied. The
	// row already carries the producer's id; only the marker is missing.
	repairConflictedUtxoHead = `-- name: RepairConflictedUtxo :one
UPDATE utxo
SET collateral_return_for_tx_id = COALESCE(collateral_return_for_tx_id, ?)
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
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE `
	markUtxoDeletedAtSlotTail = `
  AND spent_at_tx_id IS NULL
  AND (deleted_slot = 0 OR deleted_slot = ?)`

	countUtxoByRefHead = `
SELECT COUNT(*) FROM utxo WHERE `

	hydrateUtxoHead = `-- name: HydrateUtxo :execrows
UPDATE utxo
SET added_slot = ?,
    collateral_return_for_tx_id = COALESCE(collateral_return_for_tx_id, ?)
WHERE `

	clearStaleUtxoSpendHead = `-- name: ClearStaleUtxoSpend :execrows
UPDATE utxo
SET deleted_slot = 0, spent_at_tx_id = NULL
WHERE `
	clearStaleUtxoSpendTail = ` AND deleted_slot > 0`

	utxoInsertColumns = `
    tx_id, output_idx, collateral_return_for_tx_id, spent_at_tx_id,
    payment_key, staking_key, credential_tag, datum_hash, amount,
    payment_script, added_slot, deleted_slot`

	insertUtxoIgnoreConflictStatement = `-- name: InsertUtxoIfAbsent :one
INSERT INTO utxo (` + utxoInsertColumns + `
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_id, output_idx) DO NOTHING
RETURNING id`
)

// insertUtxoResultStatement and insertUtxoResultIgnoreStatement are the
// SQLite Exec-result forms of the single-row inserts: they capture the
// inserted id in the Exec result before releasing the connection, avoiding a
// RETURNING cursor. A skipped conflict leaves LastInsertId unchanged, so
// callers check RowsAffected first.
const (
	insertUtxoResultStatement = `-- name: InsertUtxoResult :execresult
INSERT INTO utxo (` + utxoInsertColumns + `
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	insertUtxoResultIgnoreStatement = `-- name: InsertUtxoIfAbsentResult :execresult
INSERT INTO utxo (` + utxoInsertColumns + `
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_id, output_idx) DO NOTHING`
)

// Statement sizes of the SQLite statements over lists. A list shorter than
// its size is padded with NULL arguments.
var (
	// sqliteInListSizes are the hash-list sizes, up to the 400 hashes one
	// statement may name, under SQLite's conservative 999-parameter limit.
	sqliteInListSizes = [...]int{1, 2, 4, 8, 16, 32, 64, 128, 256, 400}
	// sqliteExactRefSizes are the reference counts of the exact-reference
	// lookup, up to utxoRefsMaxPerQuery.
	sqliteExactRefSizes = [...]int{1, 2, 4, 8, 16, 32, 64, 128, 200}
	// sqliteOrRefSizes are the reference counts of the OR-of-references
	// delete: 128 references bind 256 arguments.
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
		consumeBatch: [2]map[int]string{{}, {}},
		insertBatch:  make(map[int]string),
	}
	for _, n := range sqliteInListSizes {
		st.stakeConsumedDelta[n] = utxoStakeConsumedDeltaQuery(n, true)
		st.stakeRefsByTxIDs[n] = utxoStakeRefsByTxIDQuery(n, true)
		st.deletionRows[n] = utxoDeletionRowsQuery(n, false, true)
	}
	for _, n := range sqliteExactRefSizes {
		st.exactRefs[n] = utxoRefsExactQuery(n)
	}
	for _, n := range sqliteOrRefSizes {
		st.deleteRefs[n] = "DELETE FROM utxo WHERE " + joinN(
			n, " OR ", "("+refEq+")",
		)
	}
	for _, n := range [...]int{1, 2, 4, 8, 16, 32} {
		st.addressInputs[n] = addressTransactionInputQuery(n)
		for table, bySize := range st.edgeInsert {
			bySize[n] = utxoReferenceInsertBatchQuery(table, n)
		}
	}
	const arm = "SELECT id FROM utxo WHERE " + refEq +
		" AND deleted_slot = 0 AND spent_at_tx_id IS NULL"
	for n := 2; n <= utxoSpendBatchSize; n++ {
		arms := joinN(n, " UNION ALL ", arm)
		st.consumeBatch[0][n] = "-- name: ConsumeUtxosBatch :many\n" +
			"UPDATE utxo\nSET deleted_slot = ?, spent_at_tx_id = ?\n" +
			"WHERE id IN (" + arms + ")\nRETURNING " + utxoConsumedHash +
			", output_idx"
		st.consumeBatch[1][n] = "-- name: ConsumeUtxosBatchReturningStake :many\n" +
			"UPDATE utxo\nSET deleted_slot = ?, spent_at_tx_id = ?\n" +
			"WHERE id IN (" + arms + ")\n" +
			"RETURNING " + utxoConsumedHash +
			", output_idx, credential_tag, staking_key, amount"
	}
	const row = "(?,?,?,?,?,?,?,?,?,?,?,?)"
	for n := 1; n <= utxoInsertBatchSize; n++ {
		st.insertBatch[n] = "-- name: InsertUtxoBatch :many\n" +
			"INSERT INTO utxo (" + utxoInsertColumns + "\n) VALUES " +
			joinN(n, ",", row) + "\n" +
			"ON CONFLICT (tx_id, output_idx) DO NOTHING\n" +
			"RETURNING id, tx_id, output_idx"
	}
	return st
}

// utxoConsumedHash projects the producer's hash of a row a statement
// returns, so the caller can match returned rows to the references it bound.
const utxoConsumedHash = `(SELECT p.hash FROM "transaction" AS p WHERE p.id = utxo.tx_id)`

func joinN(n int, sep, item string) string {
	return strings.TrimSuffix(strings.Repeat(item+sep, n), sep)
}

// utxoStakeConsumedDeltaQuery is the lookup of the consumed outputs' stake
// credentials by producer hash. It reads every output of each named
// transaction and the caller matches (hash, output_idx) in Go, so the lookup
// stays on the leading column of the unique output index however many
// references the batch names.
func utxoStakeConsumedDeltaQuery(n int, sqlite bool) string {
	return "-- name: GetUtxoStakeConsumedDeltaBatch :many\n" +
		"SELECT t.hash, u.output_idx, u.credential_tag, u.staking_key, u.amount" +
		utxoRowByHashJoin(sqlite) + bindPlaceholders(n) + ")"
}

// utxoStakeRefsByTxIDQuery is the lookup of the stake credentials of every
// output of the transactions named by n hashes. It carries no
// "deleted_slot = 0" predicate, which would let the planner drive the lookup
// from a deleted_slot index; the caller filters in Go.
func utxoStakeRefsByTxIDQuery(n int, sqlite bool) string {
	return "-- name: GetUtxoStakeRefsByTxIDs :many\n" +
		"SELECT t.hash, u.output_idx, u.deleted_slot, u.credential_tag, " +
		"u.staking_key" + utxoRowByHashJoin(sqlite) + bindPlaceholders(n) + ")"
}

// utxoDeletionRowsQuery is the lookup of every output row of the
// transactions named by n hashes. lockRows adds the row lock PostgreSQL and
// MySQL take, which SQLite has no use for.
func utxoDeletionRowsQuery(n int, lockRows, sqlite bool) string {
	name := "GetUtxoRowsByTxIDs"
	if lockRows {
		name = "LockUtxoRowsByTxIDs"
	}
	query := "-- name: " + name + " :many\n" +
		"SELECT u.id, t.hash, u.output_idx, u.deleted_slot, " +
		"u.credential_tag, u.staking_key" +
		utxoRowByHashJoin(sqlite) + bindPlaceholders(n) + ")"
	if lockRows {
		query += " ORDER BY u.id FOR UPDATE OF u"
	}
	return query
}

// utxoRefsExactQuery builds the predicate selecting exactly the n outputs
// bound to it. Each term is its own equality lookup, so the planner stays on
// the unique output index for every reference, which a wide OR of pairs does
// not guarantee. Only the requested outputs, and so only their assets, are
// read.
func utxoRefsExactQuery(n int) string {
	return "id IN (" + joinN(
		n, " UNION ALL ", "SELECT id FROM utxo WHERE "+refEq,
	) + ")"
}

// stakeConsumedDelta is the lookup of the consumed outputs' stake
// credentials by transaction hash, for up to the 400 hashes of one batch.
func (u *utxoSQL) stakeConsumedDelta(hashes [][]byte) (string, []any) {
	if !u.sqlite {
		return utxoStakeConsumedDeltaQuery(len(hashes), false),
			u.bindHashes(make([]any, 0, len(hashes)), hashes, len(hashes))
	}
	size := sizeFor(sqliteInListSizes[:], len(hashes))
	return sqliteRefs.stakeConsumedDelta[size],
		u.bindHashes(make([]any, 0, size), hashes, size)
}

// stakeRefsByTxIDs is the lookup of the stake credentials of every output of
// the given transactions, for up to 400 hashes.
func (u *utxoSQL) stakeRefsByTxIDs(hashes [][]byte) (string, []any) {
	if !u.sqlite {
		return utxoStakeRefsByTxIDQuery(len(hashes), false),
			u.bindHashes(make([]any, 0, len(hashes)), hashes, len(hashes))
	}
	size := sizeFor(sqliteInListSizes[:], len(hashes))
	return sqliteRefs.stakeRefsByTxIDs[size],
		u.bindHashes(make([]any, 0, size), hashes, size)
}

// deletionRows is the lookup of the live rows of every output of the given
// transactions, for up to 400 hashes.
func (u *utxoSQL) deletionRows(
	hashes [][]byte,
	lockRows bool,
) (string, []any) {
	if !u.sqlite {
		return utxoDeletionRowsQuery(len(hashes), lockRows, false),
			u.bindHashes(make([]any, 0, len(hashes)), hashes, len(hashes))
	}
	size := sizeFor(sqliteInListSizes[:], len(hashes))
	return sqliteRefs.deletionRows[size],
		u.bindHashes(make([]any, 0, size), hashes, size)
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
			parts[i] = "(" + refEq + ")"
			args = u.bindRef(args, ids[i].Hash, ids[i].Idx)
		}
		return "DELETE FROM utxo WHERE " + strings.Join(parts, " OR "), args
	}
	size := sizeFor(sqliteOrRefSizes[:], len(ids))
	args := make([]any, 0, size*2)
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
			args = u.bindRef(args, ref.Hash, ref.Idx)
		}
		return utxoRefsExactQuery(len(refs)), args
	}
	size := sizeFor(sqliteExactRefSizes[:], len(refs))
	args := make([]any, 0, size*2)
	for _, ref := range refs {
		args = u.bindRef(args, ref.Hash, ref.Idx)
	}
	for i := len(refs); i < size; i++ {
		args = append(args, nil, nil)
	}
	return sqliteRefs.exactRefs[size], args
}

// stakeConsumedDeltaQuery is the statement text of stakeConsumedDelta for a
// list of size hashes.
func (u *utxoSQL) stakeConsumedDeltaQuery(size int) string {
	if !u.sqlite {
		return utxoStakeConsumedDeltaQuery(size, false)
	}
	return sqliteRefs.stakeConsumedDelta[size]
}

// addressInputsQuery is the input-address lookup of size references.
func (u *utxoSQL) addressInputsQuery(size int) string {
	if !u.sqlite {
		return addressTransactionInputQuery(size)
	}
	return sqliteRefs.addressInputs[size]
}

// edgeInsertQuery is the insert of the edges of size references into
// edgeTable.
func (u *utxoSQL) edgeInsertQuery(edgeTable string, size int) string {
	if !u.sqlite {
		return utxoReferenceInsertBatchQuery(edgeTable, size)
	}
	return sqliteRefs.edgeInsert[edgeTable][size]
}
