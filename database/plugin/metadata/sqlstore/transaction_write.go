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

//nolint:gosec,rowserrcheck,sqlclosecheck // SQL INTEGER mappings preserve the unsigned domain API; cursors are explicitly closed before dependent queries.
package sqlstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/labelcodec"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// transactionFee returns a transaction's fee, treating a nil fee as zero.
// TransactionBodyBase.Fee returns nil, so any body that does not override it --
// such as the synthetic transactions used to carry imported certificates --
// would otherwise panic here. ledger/eras applies the same guard before
// comparing a fee against the computed minimum.
func transactionFee(transaction lcommon.Transaction) types.Uint64 {
	fee := transaction.Fee()
	if fee == nil {
		return 0
	}
	return types.Uint64(fee.Uint64())
}

// transactionBatchAccumulator owns statements that are safe to reuse for one
// metadata transaction.  API backfill keeps one SQL transaction open across a
// block window; preparing the transaction upsert for every row defeats much
// of that batching.  The statement is deliberately scoped to the accumulator
// (and therefore to one caller transaction), because database/sql statements
// prepared on a transaction must not escape it.
type transactionBatchAccumulator struct {
	leiosAPIRowsOnly     bool
	validatedAPIRowsOnly bool
	transactionInsert    *sql.Stmt
	mysql                bool
	sqlite               bool
	backfillBatchLimits  func() (int64, int)
	backfillBatchFlushed func(int64)
	// sqlOperations is the same counter instrumentedQueryer increments for
	// every other query path (see metrics.go); nil when Config.PromRegistry
	// was nil. insertTransaction executes transactionInsert directly against
	// a cached *sql.Stmt rather than through a queryer, so it is never
	// wrapped in countingQueryer and must count itself here to keep
	// dingo_database_sql_operations_total covering this path too.
	sqlOperations *prometheus.CounterVec
	// rows holds API-mode detail rows queued by SetTransactionBatched until
	// FlushBatch writes them as multi-row inserts.
	rows rowBatch
	// stakeDeltas coalesces credential changes until the batch is flushed.
	stakeDeltas     map[string]pendingStakeCredentialDelta
	stakeDeltaOrder []string
	opCertSequences map[poolOpCertSequenceKey]queuedPoolOpCertSequence
	opCertOrder     []poolOpCertSequenceKey
	// overlay stages UTxO writes in memory when EnableBatchUtxoOverlay
	// attached one; nil on the per-row path.
	overlay *utxoOverlay
}

type poolOpCertSequenceKey struct {
	poolKeyHash lcommon.PoolKeyHash
	slot        uint64
}

type queuedPoolOpCertSequence struct {
	key           poolOpCertSequenceKey
	sequence      uint64
	slotValue     int64
	sequenceValue int64
}

type pendingStakeCredentialDelta struct {
	ref   models.StakeCredentialRef
	delta int64
	slot  uint64
}

type transactionBatchCheckpoint struct {
	rows            rowBatch
	stakeDeltas     map[string]pendingStakeCredentialDelta
	stakeDeltaOrder []string
	opCertSequences map[poolOpCertSequenceKey]queuedPoolOpCertSequence
	opCertOrder     []poolOpCertSequenceKey
	overlay         *utxoOverlay
	overlayVersion  uint64
}

func (a *transactionBatchAccumulator) addStakeDeltas(
	deltas []stakeCredentialDelta,
	slot uint64,
) error {
	for _, delta := range deltas {
		if len(delta.ref.Key) == 0 {
			continue
		}
		key := delta.ref.MapKey()
		pending, exists := a.stakeDeltas[key]
		if !exists {
			if a.stakeDeltas == nil {
				a.stakeDeltas = make(map[string]pendingStakeCredentialDelta)
			}
			pending = pendingStakeCredentialDelta{
				ref: models.NewStakeCredentialRef(
					delta.ref.Tag, append([]byte(nil), delta.ref.Key...),
				),
				slot: slot,
			}
			a.stakeDeltaOrder = append(a.stakeDeltaOrder, key)
		} else if (delta.delta > 0 && pending.delta > math.MaxInt64-delta.delta) ||
			(delta.delta < 0 && pending.delta < math.MinInt64-delta.delta) {
			return errors.New("reward live stake delta overflow")
		}
		pending.delta += delta.delta
		if slot > pending.slot {
			pending.slot = slot
		}
		a.stakeDeltas[key] = pending
	}
	return nil
}

func (a *transactionBatchAccumulator) checkpoint() transactionBatchCheckpoint {
	checkpoint := transactionBatchCheckpoint{
		rows:            a.rows.clone(),
		stakeDeltaOrder: append([]string(nil), a.stakeDeltaOrder...),
		opCertOrder:     append([]poolOpCertSequenceKey(nil), a.opCertOrder...),
		overlay:         a.overlay,
	}
	if a.overlay != nil {
		checkpoint.overlayVersion = a.overlay.mutations
	}
	if len(a.stakeDeltas) > 0 {
		checkpoint.stakeDeltas = make(
			map[string]pendingStakeCredentialDelta,
			len(a.stakeDeltas),
		)
		maps.Copy(checkpoint.stakeDeltas, a.stakeDeltas)
	}
	if len(a.opCertSequences) > 0 {
		checkpoint.opCertSequences = maps.Clone(a.opCertSequences)
	}
	return checkpoint
}

func (a *transactionBatchAccumulator) restore(
	checkpoint transactionBatchCheckpoint,
) {
	a.Reset()
	a.rows = checkpoint.rows.clone()
	a.stakeDeltaOrder = append([]string(nil), checkpoint.stakeDeltaOrder...)
	a.opCertOrder = append(
		[]poolOpCertSequenceKey(nil),
		checkpoint.opCertOrder...)
	if len(checkpoint.stakeDeltas) > 0 {
		a.stakeDeltas = make(
			map[string]pendingStakeCredentialDelta,
			len(checkpoint.stakeDeltas),
		)
		maps.Copy(a.stakeDeltas, checkpoint.stakeDeltas)
	}
	if len(checkpoint.opCertSequences) > 0 {
		a.opCertSequences = maps.Clone(checkpoint.opCertSequences)
	}
	a.overlay = checkpoint.overlay
	if a.overlay != nil && a.overlay.mutations != checkpoint.overlayVersion &&
		a.overlay.err == nil {
		a.overlay.err = errOverlaySavepoint
	}
}

// Applying a transaction writes two rows. The identity row, which holds only
// the hash, may already exist: an imported or recovered output names its
// producer before the producer is applied. The application row exists exactly
// when the transaction has been applied, so its insert is what tells a first
// application from a replay.
const transactionIdentityInsertSQL = `-- name: InsertTransactionIdentity :execresult
INSERT INTO "transaction" (hash) VALUES (?)
ON CONFLICT (hash) DO NOTHING`

const transactionIdentityInsertReturningSQL = `-- name: InsertTransactionIdentityReturning :one
INSERT INTO "transaction" (hash) VALUES (?)
ON CONFLICT (hash) DO NOTHING
RETURNING id`

const transactionIdentityIDSQL = `-- name: GetTransactionIdentityID :one
SELECT id FROM "transaction" WHERE hash = ?`

// A fresh application row means the transaction has no child rows, so both
// write paths can skip replay cleanup after an insert that created it.
const appliedTransactionInsertSQL = `-- name: InsertAppliedTransaction :execresult
INSERT INTO applied_transaction (
    tx_id, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_id) DO NOTHING`

const appliedTransactionUpdateSQL = `-- name: UpdateAppliedTransactionOnConflict :exec
UPDATE applied_transaction
SET block_hash = ?, block_index = ?, slot = ?, collateral_fee = ?
WHERE tx_id = ?`

// consumeUtxoSQL is the PostgreSQL and MySQL spend of one input. SQLite
// spends through consumeUtxoSQLiteReturningSQL and consumeUtxosBatchQuery.
const consumeUtxoSQL = `-- name: ConsumeUtxo :execrows
UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE ` + refEq + `
  AND deleted_slot = 0 AND spent_at_tx_id IS NULL`

const consumeUtxoSQLiteReturningSQL = `-- name: ConsumeUtxoReturningStake :one
UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE ` + refEq + `
  AND deleted_slot = 0 AND spent_at_tx_id IS NULL
RETURNING ` + utxoConsumedHash + `, output_idx, credential_tag, staking_key, amount`

// insertUtxoBatchQuery is the SQLite batched insert of rowCount outputs, one
// of the sizes in sqliteRefs.
func insertUtxoBatchQuery(rowCount int) string {
	return sqliteRefs.insertBatch[rowCount]
}

// consumeUtxosBatchQuery is the SQLite batched spend for rowCount inputs, one
// of the statements of sqliteRefs. Each input is its own "id = <reference>"
// arm of an id subquery, the shape of consumeUtxoSQLiteReturningSQL, so SQLite
// resolves it through the unique producer or imported index whatever its
// statistics say.
//
// A row-value "(tx_id, output_idx) IN (...)" list or a join against VALUES
// does not hold that plan. "deleted_slot = 0" and "spent_at_tx_id IS NULL"
// match every live row, but spent rows carry many distinct values with few
// rows each, so sqlite_stat1 reports both columns as selective and SQLite
// drives the statement from a deleted_slot-leading index, visiting every live
// UTxO per transaction. The liveness predicates stay inside each arm, where
// the fully bound key already pins the plan; the outer statement carries
// none, because any predicate on deleted_slot there lets the planner choose
// that index again (see markUtxosDeletedQuery).
//
// Arguments are the two SET values followed by hash, index, hash, index per
// input.
func consumeUtxosBatchQuery(rowCount int, returnStake bool) string {
	if returnStake {
		return sqliteRefs.consumeBatch[1][rowCount]
	}
	return sqliteRefs.consumeBatch[0][rowCount]
}

func scanConsumedUtxoRows(
	rows *sql.Rows,
	returnStake bool,
) (map[string]struct{}, []stakeCredentialDelta, error) {
	updated := make(map[string]struct{})
	var deltas []stakeCredentialDelta
	for rows.Next() {
		var (
			txID      []byte
			outputIdx uint32
			tag       int64
			key       []byte
			amount    sql.NullString
		)
		if returnStake {
			if err := rows.Scan(
				&txID, &outputIdx, &tag, &key, &amount,
			); err != nil {
				_ = rows.Close()
				return nil, nil, err
			}
			delta, ok, err := consumedUtxoStakeDelta(tag, key, amount)
			if err != nil {
				_ = rows.Close()
				return nil, nil, err
			}
			if ok {
				deltas = append(deltas, delta)
			}
		} else if err := rows.Scan(&txID, &outputIdx); err != nil {
			_ = rows.Close()
			return nil, nil, err
		}
		updated[utxoIdentityKey(txID, outputIdx)] = struct{}{}
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if rowsErr != nil {
		return nil, nil, rowsErr
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	return updated, deltas, nil
}

func utxoIdentityKey(txID []byte, outputIdx uint32) string {
	return string(txID) + ":" + strconv.FormatUint(uint64(outputIdx), 10)
}

const (
	// 32 UTxOs use 480 bind parameters, below SQLite's conservative limit.
	utxoInsertBatchSize = 32
	utxoSpendBatchSize  = 8
)

func (s *Store) insertUtxoModelsChecked(
	ctx context.Context,
	db queryer,
	utxos []*models.Utxo,
	ignoreConflict bool,
	deferredRows *rowBatch,
) ([]bool, error) {
	inserted := make([]bool, len(utxos))
	if len(utxos) < 2 || s.dialect.Name() != "sqlite" || !ignoreConflict {
		for i, utxo := range utxos {
			created, err := s.insertUtxoModelCheckedWithRows(
				ctx, db, utxo, ignoreConflict, deferredRows,
			)
			if err != nil {
				return nil, err
			}
			inserted[i] = created
		}
		return inserted, nil
	}
	if err := s.resolveUtxoIdentities(ctx, db, utxos); err != nil {
		return nil, err
	}
	for start := 0; start < len(utxos); start += utxoInsertBatchSize {
		end := min(start+utxoInsertBatchSize, len(utxos))
		chunk := utxos[start:end]
		args := make([]any, 0, len(chunk)*12)
		for _, utxo := range chunk {
			params, err := createUtxoParams(utxo)
			if err != nil {
				return nil, err
			}
			args = append(args, params.args()...)
		}
		rows, err := s.queryRowsCached(
			ctx, db, insertUtxoBatchQuery(len(chunk)), args...,
		)
		if err != nil {
			return nil, err
		}
		createdIDs := make(map[string]uint, len(chunk))
		for rows.Next() {
			var (
				id        uint
				txID      int64
				outputIdx uint32
			)
			if err := rows.Scan(&id, &txID, &outputIdx); err != nil {
				_ = rows.Close()
				return nil, err
			}
			createdIDs[utxoRowKey(txID, outputIdx)] = id
		}
		rowsErr := rows.Err()
		closeErr := rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		processed := make(map[string]struct{}, len(chunk))
		for i, utxo := range chunk {
			key := utxoRowKey(int64(utxoProducerID(utxo)), utxo.OutputIdx)
			_, repeat := processed[key]
			processed[key] = struct{}{}
			id, created := createdIDs[key]
			if repeat || !created {
				// The output is stored already, by an import before its
				// producer was applied or by an earlier occurrence in this
				// batch: link it to this replay.
				if err := s.repairConflictedUtxo(
					ctx, db, utxo, deferredRows,
				); err != nil {
					return nil, err
				}
				continue
			}
			utxo.ID = id
			inserted[start+i] = true
			var err error
			if deferredRows != nil {
				err = s.persistUtxoRelationsWithDeferredAssets(
					ctx, db, utxo, id, deferredRows,
				)
			} else {
				err = s.persistUtxoRelations(ctx, db, utxo, id)
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return inserted, nil
}

// utxoProducerID is the row id of the transaction that produced utxo, which
// resolveUtxoIdentities has set.
func utxoProducerID(utxo *models.Utxo) uint {
	if utxo.TransactionID != nil {
		return *utxo.TransactionID
	}
	if utxo.CollateralReturnForTxID != nil {
		return *utxo.CollateralReturnForTxID
	}
	return 0
}

// utxoRowKey identifies an output by its producer's row id and index.
func utxoRowKey(txID int64, outputIdx uint32) string {
	return strconv.FormatInt(txID, 10) + ":" +
		strconv.FormatUint(uint64(outputIdx), 10)
}

// repairConflictedUtxo links an output that is already stored to the
// transaction replaying it and re-applies its relation rows. The stored row
// already names the producer; an output an import stored before the producer
// was applied lacks only the collateral-return marker.
func (s *Store) repairConflictedUtxo(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	addressRows *rowBatch,
) error {
	params, err := createUtxoParams(utxo)
	if err != nil {
		return err
	}
	var id int64
	u := s.utxoSQL()
	args := u.bindRef(
		[]any{params.CollateralReturnForTxID}, utxo.TxId, params.OutputIdx,
	)
	if err := s.queryRowCached(ctx, db, u.repairConflicted, args...).
		Scan(&id); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// The insert was suppressed by the unique index, yet no stored
			// output has this reference.
			return fmt.Errorf(
				"%w: output %x:%d collides with a stored output of its "+
					"producer",
				types.ErrUtxoConflict, utxo.TxId, utxo.OutputIdx,
			)
		}
		return err
	}
	utxo.ID = uint(id)
	return s.persistUtxoRelationsWithAddressRows(ctx, db, utxo, uint(id), addressRows)
}

// A no-op conflict retains the preceding last-insert ID. Check the affected
// count before using that ID to tell an inserted row from a skipped one.
func insertedRowID(result sql.Result) (int64, error) {
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		return 0, sql.ErrNoRows
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, sql.ErrNoRows
	}
	return id, nil
}

// transactionRowOps are the statement runners insertTransactionRows writes
// through: the Store's cached statements for a single write, the
// accumulator's prepared identity insert for a batch.
type transactionRowOps struct {
	insertIdentity          func(hash any) (sql.Result, error)
	insertIdentityReturning func(hash any) *sql.Row
	exec                    func(query string, args ...any) (sql.Result, error)
	queryRow                func(query string, args ...any) *sql.Row
}

// insertIdentityRow returns the id of the identity row of hash, inserting the
// row if absent.
func insertIdentityRow(
	dialect string,
	ops transactionRowOps,
	hash any,
) (int64, error) {
	var (
		id  int64
		err error
	)
	if dialect == "postgres" {
		err = ops.insertIdentityReturning(hash).Scan(&id)
	} else {
		var result sql.Result
		result, err = ops.insertIdentity(hash)
		if err == nil {
			id, err = insertedRowID(result)
		}
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = ops.queryRow(transactionIdentityIDSQL, hash).Scan(&id)
	}
	if err != nil {
		return 0, fmt.Errorf("insert transaction identity: %w", err)
	}
	return id, nil
}

// insertTransactionRows resolves the identity row of args[0], inserting it
// if absent, then writes the application row. args are the ten values of
// applied_transaction in column order after the hash: hash, block_hash,
// metadata, slot, type, fee, collateral_fee, ttl, block_index, valid. It
// returns the identity row id and whether the application row was created;
// on a replay the application row's position fields are updated.
func insertTransactionRows(
	dialect string,
	ops transactionRowOps,
	args []any,
) (int64, bool, error) {
	if len(args) != 10 {
		return 0, false, fmt.Errorf(
			"insert transaction: got %d arguments, want 10",
			len(args),
		)
	}
	id, err := insertIdentityRow(dialect, ops, args[0])
	if err != nil {
		return 0, false, err
	}
	applied := append([]any{id}, args[1:]...)
	result, err := ops.exec(appliedTransactionInsertSQL, applied...)
	if err != nil {
		return 0, false, fmt.Errorf("insert applied transaction: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	if affected > 0 {
		return id, true, nil
	}
	if _, err := ops.exec(
		appliedTransactionUpdateSQL,
		args[1], args[8], args[3], args[6], id,
	); err != nil {
		return 0, false, fmt.Errorf("update applied transaction: %w", err)
	}
	return id, false, nil
}

// cachedTransactionRowOps runs the transaction row statements through the
// Store's cached statements.
func (s *Store) cachedTransactionRowOps(
	ctx context.Context,
	db queryer,
) transactionRowOps {
	return transactionRowOps{
		insertIdentity: func(hash any) (sql.Result, error) {
			return s.execCached(ctx, db, transactionIdentityInsertSQL, hash)
		},
		insertIdentityReturning: func(hash any) *sql.Row {
			return s.queryRowCached(
				ctx, db, transactionIdentityInsertReturningSQL, hash,
			)
		},
		exec: func(query string, args ...any) (sql.Result, error) {
			return s.execCached(ctx, db, query, args...)
		},
		queryRow: func(query string, args ...any) *sql.Row {
			return s.queryRowCached(ctx, db, query, args...)
		},
	}
}

func (s *Store) insertTransactionUnbatched(
	ctx context.Context,
	db queryer,
	args ...any,
) (int64, bool, error) {
	return insertTransactionRows(
		s.dialect.Name(), s.cachedTransactionRowOps(ctx, db), args,
	)
}

func (a *transactionBatchAccumulator) insertTransaction(
	ctx context.Context,
	db queryer,
	args ...any,
) (uint, bool, error) {
	if a.transactionInsert == nil {
		// unwrapDialectQueryer, not a bare type assertion: whenever
		// Config.PromRegistry is set, Store.instrumentedQueryer wraps every
		// handle it hands out in countingQueryer, making countingQueryer
		// (not dialectQueryer) db's outermost concrete type. A plain
		// db.(dialectQueryer) would silently miss that case and leave
		// a.mysql false on a metrics-enabled MySQL store.
		if dialect, ok := unwrapDialectQueryer(db); ok {
			a.mysql = dialect.dialect == "mysql"
			a.sqlite = dialect.dialect == "sqlite"
		}
		query := transactionIdentityInsertSQL
		if !a.mysql && !a.sqlite {
			query = transactionIdentityInsertReturningSQL
		}
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			return 0, false, err
		}
		a.transactionInsert = stmt
	}
	dialect := "postgres"
	switch {
	case a.sqlite:
		dialect = "sqlite"
	case a.mysql:
		dialect = "mysql"
	}
	id, isNew, err := insertTransactionRows(dialect, transactionRowOps{
		insertIdentity: func(hash any) (sql.Result, error) {
			a.countInsert()
			return a.transactionInsert.ExecContext(ctx, hash)
		},
		insertIdentityReturning: func(hash any) *sql.Row {
			a.countInsert()
			return a.transactionInsert.QueryRowContext(ctx, hash)
		},
		exec: func(query string, args ...any) (sql.Result, error) {
			return db.ExecContext(ctx, query, args...)
		},
		queryRow: func(query string, args ...any) *sql.Row {
			return db.QueryRowContext(ctx, query, args...)
		},
	}, args)
	return uint(id), isNew, err
}

// countInsert counts the identity insert, which runs on a prepared
// statement outside any countingQueryer.
func (a *transactionBatchAccumulator) countInsert() {
	if a.sqlOperations != nil {
		op, _ := classifySQLStatement(transactionIdentityInsertSQL)
		a.sqlOperations.WithLabelValues(op).Inc()
	}
}

func (a *transactionBatchAccumulator) resetStatement() {
	if a.transactionInsert != nil {
		_ = a.transactionInsert.Close()
		a.transactionInsert = nil
	}
}

func (a *transactionBatchAccumulator) Reset() {
	a.resetStatement()
	a.rows.reset()
	a.stakeDeltas = nil
	a.stakeDeltaOrder = nil
	a.opCertSequences = nil
	a.opCertOrder = nil
	a.overlay = nil
}

// Len reports pending metadata rows for the API backfill batch bound.
func (a *transactionBatchAccumulator) Len() int {
	length := len(a.stakeDeltaOrder) + len(a.opCertOrder)
	for i := range a.rows.entries {
		length += len(a.rows.entries[i].rows)
	}
	return length
}

// EstimatedSizeBytes reports queued SQL argument payload and per-value
// overhead for the API backfill memory bound. It is an estimate, not RSS.
func (a *transactionBatchAccumulator) EstimatedSizeBytes() int64 {
	if !a.rows.trackSize {
		return 0
	}
	return a.rows.EstimatedSizeBytes()
}

func (a *transactionBatchAccumulator) EnableSizeTracking() {
	a.rows.enableSizeTracking()
}

// SetBackfillBatchLimitCallbacks lets API backfill bound temporary detail
// rows and account for rows flushed into its open SQL transaction.
func (a *transactionBatchAccumulator) SetBackfillBatchLimitCallbacks(
	remaining func() (int64, int),
	flushed func(int64),
) {
	a.backfillBatchLimits = remaining
	a.backfillBatchFlushed = flushed
}

func (s *Store) NewBatchAccumulator() types.MetadataBatchAccumulator {
	return &transactionBatchAccumulator{
		sqlite:        s.dialect.Name() == "sqlite",
		sqlOperations: s.sqlOperations,
	}
}

func (s *Store) FlushBatch(
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	batched, ok := accumulator.(*transactionBatchAccumulator)
	if !ok {
		return fmt.Errorf(
			"sqlstore FlushBatch: wrong accumulator type %T",
			accumulator,
		)
	}
	transaction, _ := txn.(*sqlTxn)
	if transaction != nil {
		if err := transaction.bindBatch(batched); err != nil {
			return err
		}
	}
	if !batched.rows.empty() || len(batched.stakeDeltaOrder) > 0 ||
		len(batched.opCertOrder) > 0 || batched.overlay != nil {
		if err := s.withWriteTransaction(
			txn,
			func(db queryer, ctx context.Context) error {
				if err := s.flushPoolOpCertSequences(ctx, db, batched); err != nil {
					return err
				}
				if batched.overlay != nil {
					return s.flushOverlay(ctx, db, batched)
				}
				if err := batched.rows.flush(
					ctx, db, s.dialect.ParameterLimit(),
				); err != nil {
					return err
				}
				return s.flushQueuedStakeDeltas(ctx, db, batched)
			},
		); err != nil {
			return err
		}
	}
	if transaction != nil && batched.overlay != nil {
		transaction.setOverlayAccumulator(nil)
	}
	accumulator.Reset()
	return nil
}

func (s *Store) FlushBatchStakeDeltas(
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
) error {
	batched, ok := accumulator.(*transactionBatchAccumulator)
	if !ok {
		return fmt.Errorf(
			"sqlstore FlushBatchStakeDeltas: wrong accumulator type %T",
			accumulator,
		)
	}
	if len(batched.stakeDeltaOrder) == 0 {
		return nil
	}
	if transaction, ok := txn.(*sqlTxn); ok {
		if err := transaction.bindBatch(batched); err != nil {
			return err
		}
	}
	if batched.overlay != nil {
		// A block ends: the overlay keeps its deltas as one segment and
		// writes them at FlushBatch or a read barrier.
		batched.overlay.closeStakeSegment(batched)
		return nil
	}
	if err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			return s.flushQueuedStakeDeltas(ctx, db, batched)
		},
	); err != nil {
		return err
	}
	batched.clearQueuedStakeDeltas()
	return nil
}

func (s *Store) SetTransactionBatched(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
	protocolMajor uint64,
) error {
	if _, ok := accumulator.(*transactionBatchAccumulator); !ok {
		return fmt.Errorf(
			"SetTransactionBatched: wrong accumulator type %T",
			accumulator,
		)
	}
	return s.setTransactionBatched(
		transaction,
		point,
		index,
		certDeposits,
		skipWithdrawalWitness,
		false,
		false,
		accumulator,
		txn,
		protocolMajor,
	)
}

// SetTransactionBatchedHistorical is the historical-replay variant. It keeps
// the public MetadataStore contract stable while allowing API backfill to
// preserve snapshot-boundary reward balances instead of applying live-slot
// withdrawal sufficiency checks.
func (s *Store) SetTransactionBatchedHistorical(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	historicalBackfill bool,
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
	protocolMajor uint64,
) error {
	if _, ok := accumulator.(*transactionBatchAccumulator); !ok {
		return fmt.Errorf(
			"SetTransactionBatchedHistorical: wrong accumulator type %T",
			accumulator,
		)
	}
	return s.setTransactionBatched(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, historicalBackfill, false,
		accumulator, txn, protocolMajor,
	)
}

func (s *Store) SetTransaction(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
	protocolMajor uint64,
) error {
	return s.setTransaction(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, false, false, txn, protocolMajor,
	)
}

func (s *Store) setTransactionBatched(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	historicalBackfill bool,
	tolerateConsumedInputConflict bool,
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
	protocolMajor uint64,
) error {
	return s.setTransactionWithAccumulator(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, historicalBackfill,
		tolerateConsumedInputConflict, accumulator, nil, txn, protocolMajor,
	)
}

// NewLeiosClosureBatchAccumulator returns an API-only row accumulator. Its
// immutable mode survives Reset so a flush cannot change closure semantics.
func (s *Store) NewLeiosClosureBatchAccumulator() types.MetadataBatchAccumulator {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	return &transactionBatchAccumulator{
		sqlite:           s.dialect.Name() == "sqlite",
		leiosAPIRowsOnly: true,
		sqlOperations:    s.sqlOperations,
	}
}

// NewValidatedBlockBatchAccumulator defers API indexes while keeping every
// stateful transaction effect visible to subsequent validation.
func (s *Store) NewValidatedBlockBatchAccumulator() types.MetadataBatchAccumulator {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	return &transactionBatchAccumulator{
		sqlite:               s.dialect.Name() == "sqlite",
		validatedAPIRowsOnly: true,
		sqlOperations:        s.sqlOperations,
	}
}

func (a *transactionBatchAccumulator) apiRowsOnly() bool {
	return a.leiosAPIRowsOnly || a.validatedAPIRowsOnly
}

// LeiosClosureRowsOnly identifies the ordered API-detail accumulator.
// Retained UTxO staging is a separate optional capability.
func (a *transactionBatchAccumulator) LeiosClosureRowsOnly() bool { return a.leiosAPIRowsOnly }

// SetTransactionLeiosClosureBatched preserves ordered closure effects and
// conflict tolerance, with optional retained UTxO staging until FlushBatch.
func (s *Store) SetTransactionLeiosClosureBatched(
	transaction lcommon.Transaction, point ocommon.Point, index uint32,
	certDeposits map[int]uint64, skipWithdrawalWitness bool,
	contextSlot *uint64, accumulator types.MetadataBatchAccumulator,
	txn types.Txn, protocolMajor uint64,
) error {
	acc, ok := accumulator.(*transactionBatchAccumulator)
	if !ok || !acc.leiosAPIRowsOnly ||
		(acc.overlay != nil && !acc.overlay.retainHistory) {
		return errors.New("closure batching requires an API-only accumulator")
	}
	if txn == nil {
		return types.ErrNilTxn
	}
	if contextSlot != nil && *contextSlot >= point.Slot {
		return errors.New("closure context must precede its certifying block")
	}
	return s.setTransactionWithAccumulator(
		transaction,
		point,
		index,
		certDeposits,
		skipWithdrawalWitness,
		false,
		true,
		accumulator,
		contextSlot,
		txn,
		protocolMajor,
	)
}

// SetTransactionLeiosClosure applies a certified closure, tolerating inputs
// consumed by an earlier closure while preserving all other effects.
func (s *Store) SetTransactionLeiosClosure(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
	protocolMajor uint64,
) error {
	return s.setTransaction(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, false, true, txn, protocolMajor,
	)
}

// SetTransactionLeiosClosureInContext records the execution context before
// certificates are applied, so epoch-dependent effects use the unticked state.
func (s *Store) SetTransactionLeiosClosureInContext(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	slot uint64,
	txn types.Txn,
	protocolMajor uint64,
) error {
	if slot >= point.Slot {
		return errors.New("closure context must precede its certifying block")
	}
	return s.setTransactionWithAccumulator(
		transaction,
		point,
		index,
		certDeposits,
		skipWithdrawalWitness,
		false,
		true,
		nil,
		&slot,
		txn,
		protocolMajor,
	)
}

func (s *Store) setTransaction(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	historicalBackfill bool,
	// tolerateConsumedInputConflict makes a consumed input that is already
	// spent by a *different* transaction a no-op instead of ErrUtxoConflict.
	// Set only on the validated Leios endorser-block closure path, where
	// the reference ledger's applyLeiosClosure folds the certified closure onto
	// the UTxO set: re-consuming an input an earlier
	// certified endorser-block transaction already spent is Map.delete on a
	// missing key (a no-op), not a fault. Two certified endorser blocks can name
	// the same input across blocks (a legitimate cross-EB double-consume the
	// canonical chain tolerates); Dingo previously wedged the ledger pipeline on
	// it. Normal ranking-block application leaves this false so a real
	// double-spend still fails.
	tolerateConsumedInputConflict bool,
	txn types.Txn,
	protocolMajor uint64,
) error {
	return s.setTransactionWithAccumulator(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, historicalBackfill,
		tolerateConsumedInputConflict, nil, nil, txn, protocolMajor,
	)
}

func (s *Store) setTransactionWithAccumulator(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	historicalBackfill bool,
	tolerateConsumedInputConflict bool,
	accumulator types.MetadataBatchAccumulator,
	ledgerContextSlot *uint64,
	txn types.Txn,
	protocolMajor uint64,
) error {
	if transaction == nil {
		return errors.New("set transaction: nil transaction")
	}
	hash := transaction.Hash().Bytes()
	var (
		metadataValue  []byte
		metadataLabels []labelcodec.Entry
	)
	if transaction.Metadata() != nil &&
		s.storageMode == types.StorageModeAPI {
		var err error
		metadataValue, metadataLabels, err = labelcodec.EncodeAndExtract(
			transaction.Metadata(),
		)
		if err != nil {
			return fmt.Errorf("extract transaction metadata: %w", err)
		}
	}
	batchedAccumulator, _ := accumulator.(*transactionBatchAccumulator)
	if batchedAccumulator != nil && txn == nil {
		defer batchedAccumulator.resetStatement()
	}
	if transaction, ok := txn.(*sqlTxn); ok && batchedAccumulator != nil {
		if err := transaction.bindBatch(batchedAccumulator); err != nil {
			return err
		}
	}
	var overlay *utxoOverlay
	if batchedAccumulator != nil {
		overlay = batchedAccumulator.overlay
	}
	if overlay != nil && overlay.retainHistory {
		if !batchedAccumulator.leiosAPIRowsOnly ||
			!tolerateConsumedInputConflict ||
			historicalBackfill ||
			!plainLeiosOverlayTransaction(transaction) {
			return errors.New(
				"unsupported transaction in API closure UTxO overlay",
			)
		}
		if _, ok := overlay.apiTransactions[string(hash)]; !ok {
			return errors.New(
				"transaction was not preflighted for API closure UTxO overlay",
			)
		}
		delete(overlay.apiTransactions, string(hash))
		overlay.mutations++
	}
	// Detail rows are staged here and reach the accumulator only once the
	// write succeeds, so a failed write cannot leave rows queued for a
	// transaction that was rolled back, nor drop the rows an earlier
	// successful application queued.
	var (
		staged        rowBatch
		transactionID int64
		// A fresh auto-generated transaction ID cannot have child detail rows.
		// The API write path uses this to avoid empty replay-cleanup deletes.
		transactionIsNew   bool
		batchedStakeDeltas []stakeCredentialDelta
		repairedUtxoIDs    []int64
	)
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			if historicalBackfill && txn != nil &&
				batchedAccumulator != nil &&
				batchedAccumulator.backfillBatchLimits != nil {
				staged.enableSizeTracking()
				maxBytes, maxRows := batchedAccumulator.backfillBatchLimits()
				staged.setFlushLimit(
					maxBytes,
					maxRows,
					func() error {
						return staged.flush(
							ctx, db, s.dialect.ParameterLimit(),
						)
					},
					batchedAccumulator.backfillBatchFlushed,
					batchedAccumulator.backfillBatchLimits,
				)
			}
			var (
				collateralFee uint64
				err           error
			)
			if overlay != nil {
				collateralFee, err = overlay.collateralFee(
					ctx, db, transaction,
				)
			} else {
				collateralFee, err = s.collateralFeeForTransaction(
					ctx, db, transaction,
				)
			}
			if err != nil {
				return err
			}
			if overlay != nil && !overlay.retainHistory {
				transactionID, err = overlay.stageTransaction(
					hash,
					point.Hash,
					metadataValue,
					point.Slot,
					transaction.Type(),
					decimalUint64(transactionFee(transaction)),
					decimalUint64(types.Uint64(collateralFee)),
					decimalUint64(types.Uint64(transaction.TTL())),
					index,
					transaction.IsValid(),
				)
				transactionIsNew = true
			} else if batched := batchedAccumulator; batched != nil {
				var id uint
				id, transactionIsNew, err = batched.insertTransaction(ctx, db,
					hash,
					point.Hash,
					metadataValue,
					point.Slot,
					transaction.Type(),
					decimalUint64(transactionFee(transaction)),
					decimalUint64(types.Uint64(collateralFee)),
					decimalUint64(types.Uint64(transaction.TTL())),
					index,
					transaction.IsValid(),
				)
				transactionID = int64(id)
			} else {
				transactionID, transactionIsNew, err = s.insertTransactionUnbatched(ctx, db,
					hash,
					point.Hash,
					metadataValue,
					point.Slot,
					transaction.Type(),
					decimalUint64(transactionFee(transaction)),
					decimalUint64(types.Uint64(collateralFee)),
					decimalUint64(types.Uint64(transaction.TTL())),
					index,
					transaction.IsValid(),
				)
			}
			if err != nil {
				return fmt.Errorf("create transaction %x: %w", hash, err)
			}
			if ledgerContextSlot != nil {
				if err := recordTransactionLedgerContext(ctx, db, transactionID, *ledgerContextSlot); err != nil {
					return err
				}
			}
			s.applyTransactionMetadataLabels(
				&staged,
				transactionID,
				point.Slot,
				metadataLabels,
			)
			s.applyTransactionAssetMintBurn(
				transaction,
				hash,
				point.Slot,
				index,
				&staged,
			)
			// Collected here and merged with this transaction's UTxO-driven
			// refresh below rather than refreshed immediately: see the
			// mergeStakeCredentialRefs call beside stakeRefs for why.
			var certificateRefs []models.StakeCredentialRef
			if transaction.IsValid() {
				if err := s.applyTransactionWithdrawals(
					ctx,
					db,
					transaction,
					point.Slot,
					hash,
					skipWithdrawalWitness,
					historicalBackfill,
					txn,
					batchedAccumulator,
				); err != nil {
					return err
				}
				var err error
				certificateRefs, err = s.applyTransactionCertificates(
					ctx,
					db,
					transactionID,
					transaction.Certificates(),
					point,
					index,
					certDeposits,
					requireKnownDeposits,
					transactionIsNew,
					protocolMajor,
				)
				if err != nil {
					return err
				}
			}
			collateralReturn := transaction.CollateralReturn()
			producedModels := make(
				[]models.Utxo,
				0,
				len(transaction.Produced()),
			)
			for _, produced := range transaction.Produced() {
				model, err := models.UtxoLedgerToModel(produced, point.Slot)
				if err != nil {
					return fmt.Errorf(
						"convert output %d: %w",
						produced.Id.Index(),
						err,
					)
				}
				if collateralReturn != nil &&
					produced.Output == collateralReturn {
					id := uint(transactionID)
					model.CollateralReturnForTxID = &id
				} else {
					id := uint(transactionID)
					model.TransactionID = &id
				}
				producedModels = append(producedModels, model)
			}
			if overlay != nil {
				var produced, consumed, skipped []stakeCredentialDelta
				produced, consumed, skipped, err = s.setTransactionOverlayUtxos(
					ctx, db, overlay, transaction, point, transactionID,
					producedModels, uniqueConsumedInputs(transaction),
				)
				if err != nil {
					return err
				}
				if historicalBackfill {
					batchedStakeDeltas = mergeStakeCredentialDeltas(
						consumed, produced,
					)
					return nil
				}
				batchedStakeDeltas = mergeStakeCredentialDeltas(
					refsToStakeCredentialDeltas(certificateRefs),
					consumed,
					skipped,
					produced,
				)
				if overlay.retainHistory {
					for i := range producedModels {
						output := overlay.created[newOverlayKey(producedModels[i].TxId, producedModels[i].OutputIdx)]
						producedModels[i].ID = uint(output.id)
					}
					if err := s.applyTransactionAPIDetails(ctx, db, transactionID, transaction, point.Slot, index, producedModels, &staged, transactionIsNew); err != nil {
						return err
					}
					if err := staged.flushExceptClosureAPIRows(ctx, db, s.dialect.ParameterLimit()); err != nil {
						return err
					}
					return nil
				}
				return nil
			}
			producedModelPtrs := make([]*models.Utxo, len(producedModels))
			for i := range producedModels {
				producedModelPtrs[i] = &producedModels[i]
			}
			var deferredRows *rowBatch
			if batchedAccumulator != nil {
				deferredRows = &staged
			}
			producedInserted, err := s.insertUtxoModelsChecked(
				ctx, db, producedModelPtrs, true, deferredRows,
			)
			if err != nil {
				return fmt.Errorf("create transaction outputs: %w", err)
			}
			if batchedAccumulator != nil && s.storageMode == types.StorageModeAPI {
				for i, created := range producedInserted {
					if !created {
						repairedUtxoIDs = append(repairedUtxoIDs, int64(producedModels[i].ID))
					}
				}
			}
			producedStakeDeltas := make([]stakeCredentialDelta, 0)
			for i := range producedModels {
				model := &producedModels[i]
				if len(model.StakingKey) > 0 {
					gain, err := producedStakeCredentialDelta(
						model.CredentialTag,
						model.StakingKey,
						model.Amount,
						producedInserted[i],
					)
					if err != nil {
						return err
					}
					producedStakeDeltas = append(producedStakeDeltas, gain)
				}
			}
			if err := s.applyTransactionAPIDetails(
				ctx,
				db,
				transactionID,
				transaction,
				point.Slot,
				index,
				producedModels,
				&staged,
				transactionIsNew,
			); err != nil {
				return err
			}
			if batchedAccumulator != nil &&
				batchedAccumulator.apiRowsOnly() {
				if err := staged.flushExceptClosureAPIRows(ctx, db, s.dialect.ParameterLimit()); err != nil {
					return err
				}
			}
			if batchedAccumulator == nil {
				if err := staged.flush(
					ctx, db, s.dialect.ParameterLimit(),
				); err != nil {
					return err
				}
			}
			// spentRefs holds only the inputs this write actually moved from
			// live to deleted, and is what the live-stake delta is derived
			// from. skippedRefs holds the inputs whose UPDATE matched nothing
			// because the row was already spent -- by an earlier certified
			// endorser block on the Leios closure path, or by an earlier
			// application of this same transaction. Those changed nothing in
			// the utxo table, so they contribute no delta; they
			// are still refreshed at zero delta so the set of credentials this
			// write touches is unchanged from the full-scan path.
			var spentRefs []models.UtxoId
			if s.dialect.Name() != "sqlite" {
				spentRefs = make(
					[]models.UtxoId,
					0,
					len(transaction.Consumed()),
				)
			}
			var skippedRefs []models.UtxoId
			consumedStakeDeltas := make(
				[]stakeCredentialDelta, 0, len(transaction.Consumed()),
			)
			u := s.utxoSQL()
			uniqueConsumed := uniqueConsumedInputs(transaction)
			for start := 0; start < len(uniqueConsumed); start += utxoSpendBatchSize {
				end := min(start+utxoSpendBatchSize, len(uniqueConsumed))
				batch := uniqueConsumed[start:end]
				updated := make(map[string]struct{}, len(batch))
				if s.dialect.Name() == "sqlite" && len(batch) > 1 {
					query := consumeUtxosBatchQuery(len(batch), true)
					args := make([]any, 0, 2+len(batch)*4)
					args = append(args, point.Slot, transactionID)
					for _, ref := range batch {
						args = u.bindRef(args, ref.Hash, ref.Idx)
					}
					rows, err := s.queryRowsCached(ctx, db, query, args...)
					if err != nil {
						return err
					}
					updatedRows, deltas, scanErr := scanConsumedUtxoRows(
						rows,
						true,
					)
					if scanErr != nil {
						return scanErr
					}
					updated = updatedRows
					consumedStakeDeltas = append(consumedStakeDeltas, deltas...)
				} else if s.dialect.Name() == "sqlite" {
					rows, err := s.queryRowsCached(
						ctx, db, consumeUtxoSQLiteReturningSQL,
						u.bindRef(
							[]any{point.Slot, transactionID},
							batch[0].Hash, batch[0].Idx,
						)...,
					)
					if err != nil {
						return err
					}
					updatedRows, deltas, scanErr := scanConsumedUtxoRows(
						rows, true,
					)
					if scanErr != nil {
						return scanErr
					}
					updated = updatedRows
					consumedStakeDeltas = append(consumedStakeDeltas, deltas...)
				} else {
					for _, ref := range batch {
						result, err := s.execCached(
							ctx, db, consumeUtxoSQL,
							point.Slot, transactionID, ref.Hash, ref.Idx,
						)
						if err != nil {
							return err
						}
						affected, err := result.RowsAffected()
						if err != nil {
							return err
						}
						if affected > 0 {
							updated[utxoIdentityKey(ref.Hash, ref.Idx)] = struct{}{}
						}
					}
				}

				for _, utxoID := range batch {
					if _, ok := updated[utxoIdentityKey(utxoID.Hash, utxoID.Idx)]; ok {
						if s.dialect.Name() != "sqlite" {
							spentRefs = append(spentRefs, utxoID)
						}
						continue
					}
					if !historicalBackfill {
						skippedRefs = append(skippedRefs, utxoID)
					}
					var (
						deletedSlot uint64
						spentBy     sql.NullInt64
					)
					err = s.queryRowCached(ctx, db, u.getSpendState,
						u.refArgs(utxoID.Hash, utxoID.Idx)...,
					).Scan(&deletedSlot, &spentBy)
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					if err != nil {
						return err
					}
					if spentBy.Valid && spentBy.Int64 == transactionID {
						continue
					}
					if deletedSlot == 0 && !spentBy.Valid {
						return fmt.Errorf(
							"consume UTxO %x#%d: row was not updated",
							utxoID.Hash,
							utxoID.Idx,
						)
					}
					if tolerateConsumedInputConflict {
						continue
					}
					return fmt.Errorf(
						"%w: %x:%d (already spent_at_tx_id=%v deleted_slot=%d, this_tx_id=%d)",
						types.ErrUtxoConflict,
						utxoID.Hash,
						utxoID.Idx,
						spentBy.Int64,
						deletedSlot,
						transactionID,
					)
				}
			}
			if historicalBackfill {
				if s.dialect.Name() != "sqlite" {
					consumedStakeDeltas, err = s.queryUtxoStakeConsumedDeltas(
						ctx,
						db,
						spentRefs,
					)
					if err != nil {
						return err
					}
				}
				// Historical replay defers account-derived fields to the final
				// rebuild, but it still changes the live UTxO set. Keep its
				// running stake totals aligned with the outputs it inserts and
				// inputs it spends.
				batchedStakeDeltas = mergeStakeCredentialDeltas(
					consumedStakeDeltas,
					producedStakeDeltas,
				)
				if batchedAccumulator != nil {
					return nil
				}
				return s.refreshRewardLiveStakeDeltas(
					ctx, db, batchedStakeDeltas, point.Slot,
				)
			}
			if s.dialect.Name() != "sqlite" {
				consumedStakeDeltas, err = s.queryUtxoStakeConsumedDeltas(
					ctx,
					db,
					spentRefs,
				)
				if err != nil {
					return err
				}
			}
			// An input this write did not actually spend still names a
			// credential the full-scan path would have refreshed, so keep it
			// in the touch set at zero delta rather than dropping it.
			var skippedStakeDeltas []stakeCredentialDelta
			if len(skippedRefs) > 0 {
				skippedStakeRefs, err := queryUtxoStakeRefs(
					ctx, db, u, skippedRefs, false,
				)
				if err != nil {
					return err
				}
				skippedStakeDeltas = refsToStakeCredentialDeltas(
					skippedStakeRefs,
				)
			}
			// Merge every credential this transaction touched -- via its
			// certificates, consumed inputs, and produced outputs -- into
			// one incremental refresh pass instead of the full
			// sumCredentialUtxoStake rescan refreshRewardLiveStakeRefs would
			// run once per occurrence: a transaction with
			// several outputs to the same staking credential (an ordinary
			// change pattern), or one that both spends from and pays back to
			// the credential a certificate in the same transaction just
			// registered or delegated, has its per-source deltas summed into
			// one net delta before refreshRewardLiveStakeAggregateDelta
			// applies it, so the result matches what a full-scan recompute
			// after all of this transaction's mutations would find (see
			// TestSetTransactionRefreshesSharedCredentialOnce and
			// TestSetTransactionIncrementalDeltaMatchesFullScan).
			batchedStakeDeltas = mergeStakeCredentialDeltas(
				refsToStakeCredentialDeltas(certificateRefs),
				consumedStakeDeltas,
				skippedStakeDeltas,
				producedStakeDeltas,
			)
			if batchedAccumulator != nil &&
				!batchedAccumulator.apiRowsOnly() {
				return nil
			}
			return s.refreshRewardLiveStakeDeltas(
				ctx, db, batchedStakeDeltas, point.Slot,
			)
		},
	)
	if err == nil && staged.flushErr != nil {
		err = fmt.Errorf(
			"flushing bounded metadata rows: %w", staged.flushErr,
		)
	}
	if err != nil || batchedAccumulator == nil {
		return err
	}
	retained := batchedAccumulator.overlay != nil && batchedAccumulator.overlay.retainHistory
	if !batchedAccumulator.apiRowsOnly() || retained {
		if err := batchedAccumulator.addStakeDeltas(batchedStakeDeltas, point.Slot); err != nil {
			return err
		}
		if retained {
			batchedAccumulator.overlay.closeStakeSegment(batchedAccumulator)
		}
	}
	// The per-transaction cleanup deletes only reach flushed rows, so rows
	// an earlier application of this transaction queued in the same window
	// are replaced here.
	// A replay repairs the same output IDs that an earlier successful write
	// may already have queued; PostgreSQL cannot upsert one ID twice in a statement.
	for _, id := range repairedUtxoIDs {
		batchedAccumulator.rows.dropUtxoAddress(id)
	}
	batchedAccumulator.rows.dropTransaction(transactionID)
	batchedAccumulator.rows.merge(&staged)
	return nil
}

func (s *Store) SetGapBlockTransaction(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	txn types.Txn,
	protocolMajor uint64,
) error {
	// Gap ingestion intentionally has no available input state, so this is
	// equivalent to SetTransaction with the consumed-input update suppressed.
	// The transaction, its certificates, and its produced outputs are all
	// persisted.
	//
	// certDeposits may be partial or nil: the caller calculates it from the
	// era's CertDepositFunc, which has no entry for a certificate whose
	// deposit it could not derive (and none at all before Shelley). A
	// certificate with no entry records NULL rather than a fabricated zero,
	// so the two stay distinguishable in the row even though today's readers
	// map both to 0.
	if transaction == nil {
		return errors.New("set gap transaction: nil transaction")
	}
	hash := transaction.Hash().Bytes()
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			collateralFee, err := s.collateralFeeForTransaction(
				ctx,
				db,
				transaction,
			)
			if err != nil {
				return err
			}
			rowID, _, err := s.insertTransactionUnbatched(
				ctx,
				db,
				hash,
				point.Hash,
				nil,
				point.Slot,
				transaction.Type(),
				decimalUint64(transactionFee(transaction)),
				decimalUint64(types.Uint64(collateralFee)),
				decimalUint64(types.Uint64(transaction.TTL())),
				index,
				transaction.IsValid(),
			)
			transactionID := rowID
			if err != nil {
				return err
			}
			var certificateRefs []models.StakeCredentialRef
			if transaction.IsValid() {
				var err error
				certificateRefs, err = s.applyTransactionCertificates(
					ctx,
					db,
					transactionID,
					transaction.Certificates(),
					point,
					index,
					certDeposits,
					allowUnknownDeposits,
					false,
					protocolMajor,
				)
				if err != nil {
					return err
				}
			}
			collateralReturn := transaction.CollateralReturn()
			producedStakeDeltas := make([]stakeCredentialDelta, 0)
			for _, produced := range transaction.Produced() {
				model, err := models.UtxoLedgerToModel(produced, point.Slot)
				if err != nil {
					return fmt.Errorf(
						"convert output %d: %w",
						produced.Id.Index(),
						err,
					)
				}
				id := uint(transactionID)
				if collateralReturn != nil &&
					produced.Output == collateralReturn {
					model.CollateralReturnForTxID = &id
				} else {
					model.TransactionID = &id
				}
				inserted, err := s.insertUtxoModelChecked(
					ctx, db, &model, true,
				)
				if err != nil {
					return err
				}
				if len(model.StakingKey) > 0 {
					gain, err := producedStakeCredentialDelta(
						model.CredentialTag,
						model.StakingKey,
						model.Amount,
						inserted,
					)
					if err != nil {
						return err
					}
					producedStakeDeltas = append(producedStakeDeltas, gain)
				}
			}
			// Merge as setTransactionWithAccumulator does: certificateRefs
			// and producedStakeDeltas are each already deduped/summed
			// against themselves, but not against each other, and a
			// certificate touching the same credential as one of this gap
			// block's produced outputs would otherwise apply that
			// credential's delta twice instead of once net.
			return s.refreshRewardLiveStakeDeltas(
				ctx,
				db,
				mergeStakeCredentialDeltas(
					refsToStakeCredentialDeltas(certificateRefs),
					producedStakeDeltas,
				),
				point.Slot,
			)
		},
	)
}

func (s *Store) RecomputeGapCollateralFee(
	transaction lcommon.Transaction,
	_ ocommon.Point,
	txn types.Txn,
) error {
	if transaction.IsValid() {
		return nil
	}
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			fee, err := s.collateralFeeForTransaction(ctx, db, transaction)
			if err != nil {
				return err
			}
			_, err = db.ExecContext(
				ctx,
				`-- name: UpdateTransactionCollateralFee :exec
UPDATE "transaction" SET collateral_fee = ? WHERE hash = ?`,
				decimalUint64(types.Uint64(fee)),
				transaction.Hash().Bytes(),
			)
			return err
		},
	)
}

func (s *Store) SetGenesisTransaction(
	hash []byte,
	blockHash []byte,
	outputs []models.Utxo,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			id, _, err := s.insertTransactionUnbatched(
				ctx, db,
				hash, blockHash, nil, 0, 0, "0", "0", "0", 0, true,
			)
			if err != nil {
				return fmt.Errorf(
					"create genesis transaction %x: %w",
					hash,
					err,
				)
			}
			transactionID := uint(id)
			refs := make([]models.StakeCredentialRef, 0, len(outputs))
			for i := range outputs {
				outputs[i].ID = 0
				outputs[i].TransactionID = &transactionID
				if err := s.insertUtxoModel(ctx, db, &outputs[i], true); err != nil {
					return err
				}
				refs = append(refs, models.NewStakeCredentialRef(
					outputs[i].CredentialTag,
					outputs[i].StakingKey,
				))
			}
			// Genesis outputs commonly repeat a staking credential across
			// several UTxOs; dedupe as setTransaction does so each credential
			// gets one sumCredentialUtxoStake scan instead of one per output.
			return s.refreshRewardLiveStakeRefs(
				ctx,
				db,
				mergeStakeCredentialRefs(refs),
				0,
			)
		},
	)
}

// rollbackTransactionDeletes delete what a transaction's id names, ahead of
// its application row. The statements end where the id list begins. The
// foreign keys to "transaction" do not cascade from applied_transaction, and
// the identity row can outlive the rollback, so none of this is left to a
// cascade.
var rollbackTransactionDeletes = []string{
	"DELETE FROM utxo_reference_edge WHERE tx_id IN (",
	"DELETE FROM utxo_collateral_edge WHERE tx_id IN (",
	"DELETE FROM utxo WHERE tx_id IN (",
	"DELETE FROM certs WHERE transaction_id IN (",
	"DELETE FROM key_witness WHERE transaction_id IN (",
	"DELETE FROM plutus_data WHERE transaction_id IN (",
	"DELETE FROM redeemer WHERE transaction_id IN (",
	"DELETE FROM redeemer_script WHERE transaction_id IN (",
	"DELETE FROM witness_scripts WHERE transaction_id IN (",
	"DELETE FROM leios_transaction_context WHERE transaction_id IN (",
}

const (
	rollbackIdentityDelete = `DELETE FROM "transaction" WHERE id IN (`
	rollbackIdentityGuard  = `)
  AND NOT EXISTS (SELECT 1 FROM utxo u WHERE u.tx_id = "transaction".id)
  AND NOT EXISTS (SELECT 1 FROM utxo u WHERE u.spent_at_tx_id = "transaction".id)
  AND NOT EXISTS (SELECT 1 FROM utxo_reference_edge e WHERE e.tx_id = "transaction".id)
  AND NOT EXISTS (SELECT 1 FROM utxo_collateral_edge e WHERE e.tx_id = "transaction".id)`
)

// DeleteTransactionsAfterSlot removes the transactions applied after slot:
// their application rows, outputs, edges and detail rows, and the spends they
// recorded. A transaction's identity row is deleted when nothing references
// it any more and otherwise kept; the row id of a deleted transaction is not
// handed out again.
func (s *Store) DeleteTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			rows, err := db.QueryContext(ctx, `
SELECT tx_id FROM applied_transaction WHERE slot > ?`,
				slot,
			)
			if err != nil {
				return err
			}
			txIDs := []int64{}
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				txIDs = append(txIDs, id)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("scan transactions for rollback: %w", err)
			}
			refs := []models.StakeCredentialRef{}
			for start := 0; start < len(txIDs); start += 400 {
				end := min(start+400, len(txIDs))
				idArgs := make([]any, end-start)
				for i, id := range txIDs[start:end] {
					idArgs[i] = id
				}
				stakeQuery, err := s.rollbackSpentStakeRefsQuery(
					ctx, db, len(idArgs),
				)
				if err != nil {
					return err
				}
				stakeRows, err := db.QueryContext(ctx, stakeQuery, idArgs...)
				if err != nil {
					return err
				}
				for stakeRows.Next() {
					var ref models.StakeCredentialRef
					if err := stakeRows.Scan(&ref.Tag, &ref.Key); err != nil {
						stakeRows.Close()
						return err
					}
					refs = append(refs, ref)
				}
				if err := stakeRows.Close(); err != nil {
					return err
				}
				if err := stakeRows.Err(); err != nil {
					return fmt.Errorf(
						"scan affected stake credentials for rollback: %w",
						err,
					)
				}
				if _, err := db.ExecContext(ctx, `
UPDATE utxo SET spent_at_tx_id = NULL, deleted_slot = 0
WHERE spent_at_tx_id IN (`+bindPlaceholders(len(idArgs))+`)`,
					idArgs...,
				); err != nil {
					return err
				}
				for _, statement := range rollbackTransactionDeletes {
					if _, err := db.ExecContext(
						ctx,
						statement+bindPlaceholders(len(idArgs))+")",
						idArgs...,
					); err != nil {
						return err
					}
				}
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM transaction_metadata_label WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(ctx, `
DELETE FROM asset_mint_burn WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			if _, err := db.ExecContext(
				ctx,
				`DELETE FROM applied_transaction WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			// A rolled-back transaction's identity row is deleted unless
			// something still names it. Nothing that outlives the rollback
			// should, since its outputs, spends and edges were deleted above;
			// the guard keeps the row, and the foreign keys that point at it
			// intact, if one does.
			for start := 0; start < len(txIDs); start += 400 {
				end := min(start+400, len(txIDs))
				idArgs := make([]any, end-start)
				for i, id := range txIDs[start:end] {
					idArgs[i] = id
				}
				if _, err := db.ExecContext(
					ctx,
					rollbackIdentityDelete+bindPlaceholders(len(idArgs))+
						rollbackIdentityGuard,
					idArgs...,
				); err != nil {
					return err
				}
			}
			// The stake lookup returns one row per spent UTxO; refresh each
			// credential once.
			return s.refreshRewardLiveStakeRefs(
				ctx, db, mergeStakeCredentialRefs(refs), slot,
			)
		},
	)
}

// insertUtxoQuery inserts one new output. PostgreSQL reads RETURNING
// natively; MySQL's dialect layer emulates it from the insert result.
const insertUtxoQuery = `-- name: InsertUtxo :one
INSERT INTO utxo (` + utxoInsertColumns + `
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id`

// utxoSQL.insertIgnore leaves an existing output untouched: its conflict
// target is the unique index of the store's dialect. Its empty RETURNING result
// signals that provenance must be repaired without crediting the output's
// stake a second time. On SQLite, insertResult and insertResultIgnore capture
// the inserted ID in the Exec result before releasing the connection,
// avoiding a RETURNING cursor; check RowsAffected first, since a skipped
// conflict leaves LastInsertId unchanged.

func (s *Store) insertUtxoModel(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	ignoreConflict bool,
) error {
	_, err := s.insertUtxoModelChecked(ctx, db, utxo, ignoreConflict)
	return err
}

// insertUtxoModelChecked is insertUtxoModel plus the one fact a caller
// maintaining a running live-stake total needs: whether this call actually
// created the row, or left a pre-existing (tx_id, output_idx) row untouched
// through insertUtxoQueryIgnoreConflict's ON CONFLICT DO NOTHING. Only the
// former changed the live UTxO set, and only the former may contribute a
// delta -- see producedStakeCredentialDelta.
//
// The signal is the same one the provenance repair below already relies on:
// a conflicting DO NOTHING insert yields no RETURNING row, which every
// dialect surfaces as sql.ErrNoRows (dialect_queryer.go maps MySQL's
// zero-rows-affected case onto an empty row set explicitly).
func (s *Store) insertUtxoModelChecked(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	ignoreConflict bool,
) (bool, error) {
	return s.insertUtxoModelCheckedWithRows(
		ctx, db, utxo, ignoreConflict, nil,
	)
}

func (s *Store) insertUtxoModelCheckedWithRows(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	ignoreConflict bool,
	deferredRows *rowBatch,
) (bool, error) {
	inserted := true
	// Resolve on a copy: a row id written onto the caller's model would
	// outlive a rollback of the identity row it names.
	resolved := *utxo
	if err := s.resolveUtxoIdentities(
		ctx, db, []*models.Utxo{&resolved},
	); err != nil {
		return false, err
	}
	params, err := createUtxoParams(&resolved)
	if err != nil {
		return false, err
	}
	u := s.utxoSQL()
	query := insertUtxoQuery
	if ignoreConflict {
		query = u.insertIgnore
	}
	args := params.args()
	var id int64
	if s.dialect.Name() == "sqlite" {
		query = u.insertResult
		if ignoreConflict {
			query = u.insertResultIgnore
		}
		var result sql.Result
		result, err = s.execCached(ctx, db, query, args...)
		if err == nil {
			var affected int64
			affected, err = result.RowsAffected()
			if err == nil && affected == 0 {
				err = sql.ErrNoRows
			}
			if err == nil {
				id, err = result.LastInsertId()
			}
		}
	} else {
		err = s.queryRowCached(ctx, db, query, args...).Scan(&id)
	}
	if errors.Is(err, sql.ErrNoRows) && ignoreConflict {
		inserted = false
		err = s.queryRowCached(
			ctx, db, u.idByOutput, u.refArgs(utxo.TxId, utxo.OutputIdx)...,
		).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf(
				"%w: output %x:%d collides with a stored output of its "+
					"producer",
				types.ErrUtxoConflict, utxo.TxId, utxo.OutputIdx,
			)
		}
		if err == nil {
			// An import can create an output before its producer is
			// applied. The row already carries the producer's id; only the
			// collateral-return marker is missing.
			//
			// The stake credential deliberately is not repaired here. A
			// pointer address's credential is not stored on the row at all
			// (see pointer_stake.go); every other address form carries its
			// credential in the address, so an imported row already has it.
			_, err = db.ExecContext(
				ctx,
				`-- name: RepairConflictedUtxoByID :exec
UPDATE utxo
SET collateral_return_for_tx_id = COALESCE(collateral_return_for_tx_id, ?)
WHERE id = ?`,
				params.CollateralReturnForTxID,
				id,
			)
		}
	}
	if err != nil {
		return false, err
	}
	utxo.ID = uint(id)
	// A pointer address names a certificate position rather than carrying a
	// credential, so the position is recorded alongside the output and
	// resolved when stake is computed. This runs on the
	// conflict path too: an output a snapshot import created before its
	// producing transaction was replayed has no pointer row yet.
	var relationErr error
	if deferredRows != nil && inserted {
		relationErr = s.persistUtxoRelationsWithDeferredAssets(
			ctx, db, utxo, uint(id), deferredRows,
		)
	} else {
		relationErr = s.persistUtxoRelationsWithAddressRows(ctx, db, utxo, uint(id), deferredRows)
	}
	if relationErr != nil {
		return false, relationErr
	}
	return inserted, nil
}

func (s *Store) persistUtxoRelations(
	ctx context.Context, db queryer, utxo *models.Utxo, id uint,
) error {
	return s.persistUtxoRelationsWithAddressRows(ctx, db, utxo, id, nil)
}

func (s *Store) persistUtxoRelationsWithAddressRows(
	ctx context.Context, db queryer, utxo *models.Utxo, id uint, addressRows *rowBatch,
) error {
	if err := s.persistUtxoRelationsWithoutAssets(
		ctx, db, utxo, id, addressRows,
	); err != nil {
		return err
	}
	for i := range utxo.Assets {
		if err := s.insertAssetHolding(
			ctx, db, int64(id), &utxo.Assets[i],
		); err != nil {
			return err
		}
	}
	return nil
}

// persistUtxoRelationsWithoutAssets writes the address, pointer, and
// reference-script rows of an existing output.
func (s *Store) persistUtxoRelationsWithoutAssets(
	ctx context.Context, db queryer, utxo *models.Utxo, id uint, addressRows *rowBatch,
) error {
	utxo.ID = id
	if addressRows != nil && s.storageMode == types.StorageModeAPI {
		address, err := utxoAddress(utxo)
		if err != nil {
			return err
		}
		addressRows.dropUtxoAddress(int64(id))
		addressRows.add(utxoAddressShape, int64(id), address)
	} else if err := s.persistUtxoAddress(ctx, db, int64(id), utxo); err != nil {
		return err
	}
	if err := persistUtxoPointer(ctx, db, int64(id), utxo.Pointer); err != nil {
		return err
	}
	return s.persistUtxoReferenceScript(
		ctx, db, id, utxo.ReferenceScriptHash,
	)
}

func (s *Store) persistUtxoRelationsWithDeferredAssets(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	id uint,
	rows *rowBatch,
) error {
	utxo.ID = id
	if s.storageMode == types.StorageModeAPI {
		address, err := utxoAddress(utxo)
		if err != nil {
			return err
		}
		rows.add(utxoAddressShape, int64(id), address)
	}
	if err := persistUtxoPointer(ctx, db, int64(id), utxo.Pointer); err != nil {
		return err
	}
	if err := s.persistUtxoReferenceScript(
		ctx, db, id, utxo.ReferenceScriptHash,
	); err != nil {
		return err
	}
	for i := range utxo.Assets {
		queueAssetHolding(rows, int64(id), &utxo.Assets[i])
	}
	return nil
}

// uniqueConsumedInputs returns transaction's consumed inputs, first
// occurrence of each, in order.
func uniqueConsumedInputs(transaction lcommon.Transaction) []models.UtxoId {
	consumed := transaction.Consumed()
	seen := make(map[string]struct{}, len(consumed))
	ret := make([]models.UtxoId, 0, len(consumed))
	for _, input := range consumed {
		refKey := utxoIdentityKey(input.Id().Bytes(), input.Index())
		if _, ok := seen[refKey]; ok {
			continue
		}
		seen[refKey] = struct{}{}
		ret = append(ret, models.UtxoId{
			Hash: input.Id().Bytes(),
			Idx:  input.Index(),
		})
	}
	return ret
}

func (s *Store) collateralFeeForTransaction(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
) (uint64, error) {
	return collateralFeeWithLookup(transaction, func(
		input lcommon.TransactionInput,
	) (string, bool, error) {
		var amount string
		u := s.utxoSQL()
		err := s.queryRowCached(
			ctx, db, u.amountByOutput,
			u.refArgs(input.Id().Bytes(), input.Index())...,
		).Scan(&amount)
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		return amount, true, nil
	})
}

// collateralFeeWithLookup computes a phase-2-invalid transaction's collateral
// fee, reading each collateral input's amount through lookup, which reports
// false for an input it cannot find.
func collateralFeeWithLookup(
	transaction lcommon.Transaction,
	lookup func(lcommon.TransactionInput) (string, bool, error),
) (uint64, error) {
	if transaction.IsValid() {
		return 0, nil
	}
	if total := transaction.TotalCollateral(); total != nil &&
		total.Sign() > 0 {
		if !total.IsUint64() {
			return 0, errors.New("total collateral exceeds uint64")
		}
		return total.Uint64(), nil
	}
	var total uint64
	seen := make(map[string]struct{})
	for _, input := range transaction.Collateral() {
		key := fmt.Sprintf("%x:%d", input.Id().Bytes(), input.Index())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		amount, found, err := lookup(input)
		if err != nil {
			return 0, err
		}
		if !found {
			continue
		}
		value, err := parseUint64("collateral input", amount)
		if err != nil {
			return 0, err
		}
		if value > math.MaxUint64-total {
			return 0, errors.New("collateral input sum overflow")
		}
		total += value
	}
	if output := transaction.CollateralReturn(); output != nil {
		amount := output.Amount()
		if amount != nil && amount.Sign() > 0 {
			if !amount.IsUint64() || amount.Uint64() > total {
				return 0, nil
			}
			total -= amount.Uint64()
		}
	}
	return total, nil
}

func nullBytes(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func (s *Store) applyTransactionWithdrawals(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
	slot uint64,
	txHash []byte,
	skipWithdrawalWitness bool,
	historicalBackfill bool,
	txn types.Txn,
	batchedAccumulator *transactionBatchAccumulator,
) error {
	for address, amount := range transaction.Withdrawals() {
		if address == nil || amount == nil {
			continue
		}
		if amount.Sign() < 0 || !amount.IsUint64() {
			return fmt.Errorf(
				"invalid reward withdrawal amount %s",
				amount.String(),
			)
		}
		stakeKey := address.StakeKeyHash()
		if stakeKey == (lcommon.Blake2b224{}) {
			return errors.New("reward withdrawal missing stake credential")
		}
		tag, ok := models.StakeCredentialTagFromAddress(*address)
		if !ok {
			return errors.New("derive reward withdrawal credential tag")
		}
		if !skipWithdrawalWitness {
			// CIP-0163: only the delegator-inactivity gate's rollback/renewal
			// paths read this table (see BatchedTxIngestOpts.
			// SkipWithdrawalWitnessWrite), so gate-off callers elide the
			// insert rather than growing an unbounded, never-pruned table
			// nothing reads.
			if _, err := s.execCached(ctx, db, insertAccountWithdrawalWitnessSQL,
				stakeKey.Bytes(),
				tag,
				txHash,
				slot,
			); err != nil {
				return err
			}
		}
		var accountID uint
		var reward sql.NullString
		err := s.queryRowCached(ctx, db, getActiveAccountRewardSQL,
			tag,
			stakeKey.Bytes(),
		).Scan(&accountID, &reward)
		accountFound := true
		if errors.Is(err, sql.ErrNoRows) {
			if !historicalBackfill {
				return models.ErrAccountNotFound
			}
			accountFound = false
			// A historical withdrawal may be applied after deregistration,
			// so an inactive account is still a valid historical account. It
			// must be present; silently journaling a withdrawal with no account
			// would hide a broken certificate replay or skipped block.
			var fallbackActive sql.NullBool
			err = s.queryRowCached(ctx, db, getAccountRewardStateSQL,
				tag,
				stakeKey.Bytes(),
			).Scan(&accountID, &reward, &fallbackActive)
			if errors.Is(err, sql.ErrNoRows) {
				return models.ErrAccountNotFound
			} else if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		var exists bool
		if err := s.queryRowCached(ctx, db, withdrawalRewardDeltaExistsSQL,
			txHash,
			tag,
			stakeKey.Bytes(),
		).Scan(&exists); err != nil {
			return err
		}
		if exists {
			continue
		}
		previous, err := parseNullUint64("account reward", reward)
		if err != nil {
			return err
		}
		if !historicalBackfill && amount.Uint64() > previous {
			return fmt.Errorf(
				"reward withdrawal amount %s exceeds account balance %d: %w",
				amount.String(),
				previous,
				models.ErrRewardWithdrawalExceedsBalance,
			)
		}
		if amount.Sign() == 0 {
			continue
		}
		// Historical API backfill replays withdrawals before the imported
		// snapshot balance's intervening credits are available. Record the
		// withdrawal history, but leave that trusted boundary balance untouched.
		// accountFound is guaranteed true here whenever historicalBackfill is
		// false (the account lookup above returns early otherwise); the extra
		// check keeps this update from ever reactivating or fabricating a
		// current stake-registration account.
		if !historicalBackfill && accountFound {
			rewardAfter := previous - amount.Uint64()
			if _, err := s.execCached(ctx, db, updateAccountRewardSQL,
				strconv.FormatUint(rewardAfter, 10),
				accountID,
			); err != nil {
				return err
			}
		}
		if _, err := s.execCached(ctx, db, insertWithdrawalRewardDeltaSQL,
			stakeKey.Bytes(),
			tag,
			txHash,
			strconv.FormatUint(amount.Uint64(), 10),
			strconv.FormatUint(previous, 10),
			slot,
		); err != nil {
			return err
		}
		if !historicalBackfill {
			ref := models.NewStakeCredentialRef(tag, stakeKey.Bytes())
			pending, err := pendingUtxoStakeDelta(
				ctx, db, txn, batchedAccumulator, ref,
			)
			if err != nil {
				return err
			}
			if err := s.refreshRewardLiveStakeAggregateNetOfPending(
				ctx, db, ref, slot, pending,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func recordTransactionLedgerContext(
	ctx context.Context,
	db queryer,
	transactionID int64,
	slot uint64,
) error {
	value, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `-- name: UpsertLeiosTransactionContext :exec
INSERT INTO leios_transaction_context (transaction_id, slot) VALUES (?, ?) ON CONFLICT (transaction_id) DO UPDATE SET slot = excluded.slot`, transactionID, value)
	return err
}

// rollbackSpentStakeRefsIndex is the spender index the rollback stake lookup
// reads. It is a deferred index, so it can be absent while a bulk-load
// rebuild is pending.
var rollbackSpentStakeRefsIndex = deferred.Index{
	Name:  "idx_utxo_spent_at_tx_id",
	Table: "utxo",
}

// rollbackSpentStakeRefsQuery returns the statement that collects the stake
// credentials of the UTxOs spent by n rolled-back transactions.
//
// On SQLite the statement names idx_utxo_spent_at_tx_id whenever that index
// exists. Its statistics are unreliable: utxo statistics taken over an
// imported, never-spent UTxO set record spent_at_tx_id as one value covering
// every row, and the table never grows enough afterwards for the incremental
// refresh to re-analyze it. With those statistics a single-hash lookup was
// planned as a pass over every utxo row, holding the ledger write lock for
// tens of seconds on a mainnet-sized table. Without the index any plan scans,
// so the hint is dropped rather than failing the rollback.
//
// There is no DISTINCT: it lets SQLite satisfy the query by walking
// idx_utxo_staking_deleted_amount in credential order, and the caller
// deduplicates in Go across all chunks anyway.
func (s *Store) rollbackSpentStakeRefsQuery(
	ctx context.Context,
	db queryer,
	n int,
) (string, error) {
	from := "FROM utxo"
	if s.dialect.Name() == "sqlite" {
		exists, err := s.deferredIndexExists(
			ctx, db, rollbackSpentStakeRefsIndex,
		)
		if err != nil {
			return "", fmt.Errorf(
				"check %s: %w", rollbackSpentStakeRefsIndex.Name, err,
			)
		}
		if exists {
			from += " INDEXED BY " + rollbackSpentStakeRefsIndex.Name
		}
	}
	return "-- name: GetRollbackSpentStakeRefs\n" +
		"SELECT credential_tag, staking_key " + from + "\n" +
		"WHERE spent_at_tx_id IN (" + bindPlaceholders(n) + ")", nil
}

const insertAccountWithdrawalWitnessSQL = `-- name: InsertAccountWithdrawalWitness :exec
INSERT INTO account_withdrawal_witness (
    staking_key, credential_tag, tx_hash, added_slot
) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_hash, credential_tag, staking_key) DO NOTHING`

const getActiveAccountRewardSQL = `-- name: GetActiveAccountReward :one
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`

const getAccountRewardStateSQL = `-- name: GetAccountRewardState :one
SELECT id, reward, active FROM account
WHERE credential_tag = ? AND staking_key = ?`

const withdrawalRewardDeltaExistsSQL = `-- name: WithdrawalRewardDeltaExists :one
SELECT EXISTS (
    SELECT 1 FROM account_reward_delta
    WHERE withdrawal = TRUE AND tx_hash = ?
      AND credential_tag = ? AND staking_key = ?
)`

const updateAccountRewardSQL = `-- name: UpdateAccountReward :exec
UPDATE account SET reward = ? WHERE id = ?`

const insertWithdrawalRewardDeltaSQL = `-- name: InsertWithdrawalRewardDelta :exec
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal
) VALUES (?, ?, ?, ?, ?, ?, TRUE)
ON CONFLICT (
    withdrawal, tx_hash, credential_tag, staking_key, added_slot
) DO NOTHING`
