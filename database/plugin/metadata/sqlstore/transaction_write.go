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
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"math"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
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
	transactionInsert *sql.Stmt
	mysql             bool
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
	}
	if len(a.stakeDeltas) > 0 {
		checkpoint.stakeDeltas = make(
			map[string]pendingStakeCredentialDelta,
			len(a.stakeDeltas),
		)
		maps.Copy(checkpoint.stakeDeltas, a.stakeDeltas)
	}
	return checkpoint
}

func (a *transactionBatchAccumulator) restore(
	checkpoint transactionBatchCheckpoint,
) {
	a.Reset()
	a.rows = checkpoint.rows.clone()
	a.stakeDeltaOrder = append([]string(nil), checkpoint.stakeDeltaOrder...)
	if len(checkpoint.stakeDeltas) > 0 {
		a.stakeDeltas = make(
			map[string]pendingStakeCredentialDelta,
			len(checkpoint.stakeDeltas),
		)
		maps.Copy(a.stakeDeltas, checkpoint.stakeDeltas)
	}
}

// getUtxoSpendStateQuery reads back a consumed input that the spend UPDATE
// did not mark, to tell an already-spent input from a missing one.
const getUtxoSpendStateQuery = `
SELECT deleted_slot, spent_at_tx_id
FROM utxo WHERE tx_id = ? AND output_idx = ?`

const transactionInsertSQL = `
INSERT INTO "transaction" (
    hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hash) DO UPDATE SET
    block_hash = excluded.block_hash,
    block_index = excluded.block_index,
    slot = excluded.slot,
    collateral_fee = excluded.collateral_fee
RETURNING id`

// The batched path needs to distinguish a fresh ID from a replay so it can
// skip child-table cleanup only when those rows cannot already exist.
const transactionBatchInsertSQL = `
INSERT INTO "transaction" (
    hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hash) DO NOTHING
RETURNING id`

const transactionBatchConflictUpdateSQL = `
UPDATE "transaction"
SET block_hash = ?, block_index = ?, slot = ?, collateral_fee = ?
WHERE hash = ?`

const transactionBatchConflictIDSQL = `
SELECT id FROM "transaction" WHERE hash = ?`

const consumeUtxoSQL = `
UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE tx_id = ? AND output_idx = ?
  AND deleted_slot = 0 AND spent_at_tx_id IS NULL`

const consumeUtxoSQLiteReturningSQL = `
UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE tx_id = ? AND output_idx = ?
  AND deleted_slot = 0 AND spent_at_tx_id IS NULL
RETURNING tx_id, output_idx, credential_tag, staking_key, amount`

func insertUtxoBatchQuery(rowCount int) string {
	row := "(" + strings.TrimSuffix(strings.Repeat("?,", 15), ",") + ")"
	values := strings.TrimSuffix(strings.Repeat(row+",", rowCount), ",")
	return `INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES ` + values + `
ON CONFLICT (tx_id, output_idx) DO NOTHING
RETURNING id, tx_id, output_idx`
}

func consumeUtxosBatchQuery(rowCount int, returnStake bool) string {
	row := "(?,?)"
	values := strings.TrimSuffix(strings.Repeat(row+",", rowCount), ",")
	returning := "tx_id, output_idx"
	if returnStake {
		returning += ", credential_tag, staking_key, amount"
	}
	return `UPDATE utxo
SET deleted_slot = ?, spent_at_tx_id = ?
WHERE deleted_slot = 0 AND spent_at_tx_id IS NULL
  AND (tx_id, output_idx) IN (` + values + `)
RETURNING ` + returning
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

const utxoBatchSize = 8

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

	for start := 0; start < len(utxos); start += utxoBatchSize {
		end := min(start+utxoBatchSize, len(utxos))
		batch := utxos[start:end]
		query := insertUtxoBatchQuery(len(batch))
		args := make([]any, 0, len(batch)*15)
		for _, utxo := range batch {
			params, err := createUtxoParams(utxo)
			if err != nil {
				return nil, err
			}
			args = append(args,
				params.TransactionID,
				params.CollateralReturnForTxID,
				params.TxID,
				params.PaymentKey,
				params.StakingKey,
				params.CredentialTag,
				params.DatumHash,
				nullBytes(params.SpentAtTxID),
				nullBytes(params.ReferencedByTxID),
				nullBytes(params.CollateralByTxID),
				params.AddedSlot,
				params.DeletedSlot,
				params.Amount,
				params.OutputIdx,
				params.PaymentScript,
			)
		}

		rows, err := s.queryRowsCached(ctx, db, query, args...)
		if err != nil {
			return nil, err
		}
		createdIDs := make(map[string]uint, len(batch))
		for rows.Next() {
			var (
				id        uint
				txID      []byte
				outputIdx uint32
			)
			if err := rows.Scan(&id, &txID, &outputIdx); err != nil {
				_ = rows.Close()
				return nil, err
			}
			createdIDs[utxoIdentityKey(txID, outputIdx)] = id
		}
		rowsErr := rows.Err()
		closeErr := rows.Close()
		if rowsErr != nil {
			return nil, rowsErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		processed := make(map[string]struct{}, len(batch))
		for i, utxo := range batch {
			key := utxoIdentityKey(utxo.TxId, utxo.OutputIdx)
			if _, duplicate := processed[key]; duplicate {
				wasCreated, err := s.insertUtxoModelCheckedWithRows(
					ctx, db, utxo, true, deferredRows,
				)
				if err != nil {
					return nil, err
				}
				inserted[start+i] = wasCreated
				continue
			}
			processed[key] = struct{}{}
			id, created := createdIDs[key]
			if !created {
				wasCreated, err := s.insertUtxoModelCheckedWithRows(
					ctx, db, utxo, true, deferredRows,
				)
				if err != nil {
					return nil, err
				}
				inserted[start+i] = wasCreated
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
		// a.mysql false on a metrics-enabled MySQL store, routing this
		// insert down the RETURNING-id path MySQL cannot serve.
		if dialect, ok := unwrapDialectQueryer(db); ok {
			a.mysql = dialect.dialect == "mysql"
		}
		query := transactionBatchInsertSQL
		if a.mysql {
			query = transactionInsertSQL
		}
		stmt, err := db.PrepareContext(ctx, query)
		if err != nil {
			return 0, false, err
		}
		a.transactionInsert = stmt
	}
	if a.sqlOperations != nil {
		op, _ := classifySQLStatement(transactionBatchInsertSQL)
		a.sqlOperations.WithLabelValues(op).Inc()
	}
	if a.mysql {
		result, err := a.transactionInsert.ExecContext(ctx, args...)
		if err != nil {
			return 0, false, err
		}
		id, err := result.LastInsertId()
		if err != nil {
			return 0, false, err
		}
		return uint(id), false, nil
	}
	var id int64
	if err := a.transactionInsert.QueryRowContext(ctx, args...).Scan(&id); err == nil {
		return uint(id), true, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}
	if len(args) != 10 {
		return 0, false, fmt.Errorf(
			"update existing transaction: got %d insert arguments, want 10",
			len(args),
		)
	}
	if _, err := db.ExecContext(
		ctx,
		transactionBatchConflictUpdateSQL,
		args[1], args[8], args[3], args[6], args[0],
	); err != nil {
		return 0, false, fmt.Errorf("update existing transaction: %w", err)
	}
	if err := db.QueryRowContext(
		ctx,
		transactionBatchConflictIDSQL,
		args[0],
	).Scan(&id); err != nil {
		return 0, false, fmt.Errorf("find existing transaction: %w", err)
	}
	return uint(id), false, nil
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
}

func (s *Store) NewBatchAccumulator() types.MetadataBatchAccumulator {
	return &transactionBatchAccumulator{sqlOperations: s.sqlOperations}
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
	if transaction, ok := txn.(*sqlTxn); ok {
		if err := transaction.bindBatch(batched); err != nil {
			return err
		}
	}
	if !batched.rows.empty() || len(batched.stakeDeltaOrder) > 0 {
		if err := s.withWriteTransaction(
			txn,
			func(db queryer, ctx context.Context) error {
				if err := batched.rows.flush(
					ctx, db, s.dialect.ParameterLimit(),
				); err != nil {
					return err
				}
				for _, key := range batched.stakeDeltaOrder {
					pending := batched.stakeDeltas[key]
					if err := s.refreshRewardLiveStakeAggregateDelta(
						ctx, db, pending.ref, pending.slot, pending.delta,
					); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			return err
		}
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
	if err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			for _, key := range batched.stakeDeltaOrder {
				pending := batched.stakeDeltas[key]
				if err := s.refreshRewardLiveStakeAggregateDelta(
					ctx, db, pending.ref, pending.slot, pending.delta,
				); err != nil {
					return err
				}
			}
			return nil
		},
	); err != nil {
		return err
	}
	batched.stakeDeltas = nil
	batched.stakeDeltaOrder = nil
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
		accumulator, txn,
	)
}

func (s *Store) SetTransaction(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
) error {
	return s.setTransaction(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, false, false, txn,
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
) error {
	return s.setTransactionWithAccumulator(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, historicalBackfill,
		tolerateConsumedInputConflict, accumulator, nil, txn,
	)
}

// SetTransactionLeiosClosure records a transaction on the Leios endorser-block
// closure path (the Musashi/Haskell-conformant ValidateNone apply). It behaves
// like SetTransaction except that a consumed input already spent by a
// *different* transaction is treated as a no-op instead of ErrUtxoConflict,
// matching the reference ledger's applyLeiosClosure: two certified endorser
// blocks may legitimately name the same input across blocks, and the canonical
// chain folds the closure without re-validation rather than rejecting it. Do
// not use this for ranking-block application, where a real double-spend must
// still fail.
func (s *Store) SetTransactionLeiosClosure(
	transaction lcommon.Transaction,
	point ocommon.Point,
	index uint32,
	certDeposits map[int]uint64,
	skipWithdrawalWitness bool,
	txn types.Txn,
) error {
	return s.setTransaction(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, false, true, txn,
	)
}

// SetTransactionLeiosClosureInContext records the execution context before
// certificates are applied, so epoch-dependent effects use the unticked state.
func (s *Store) SetTransactionLeiosClosureInContext(transaction lcommon.Transaction, point ocommon.Point, index uint32, certDeposits map[int]uint64, skipWithdrawalWitness bool, slot uint64, txn types.Txn) error {
	if slot >= point.Slot {
		return errors.New("closure context must precede its certifying block")
	}
	return s.setTransactionWithAccumulator(transaction, point, index, certDeposits, skipWithdrawalWitness, false, true, nil, &slot, txn)
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
	// Set only on the Leios endorser-block closure path (ValidateNone), where
	// the reference ledger's applyLeiosClosure folds the certified closure onto
	// the UTxO set without re-validation: re-consuming an input an earlier
	// certified endorser-block transaction already spent is Map.delete on a
	// missing key (a no-op), not a fault. Two certified endorser blocks can name
	// the same input across blocks (a legitimate cross-EB double-consume the
	// canonical chain tolerates); Dingo previously wedged the ledger pipeline on
	// it. Normal ranking-block application leaves this false so a real
	// double-spend still fails.
	tolerateConsumedInputConflict bool,
	txn types.Txn,
) error {
	return s.setTransactionWithAccumulator(
		transaction, point, index, certDeposits,
		skipWithdrawalWitness, historicalBackfill,
		tolerateConsumedInputConflict, nil, nil, txn,
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
	)
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			collateralFee, err := collateralFeeForTransaction(
				ctx,
				db,
				transaction,
			)
			if err != nil {
				return err
			}
			if batched, ok := accumulator.(*transactionBatchAccumulator); ok {
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
				err = s.queryRowCached(ctx, db, transactionInsertSQL,
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
				).Scan(&transactionID)
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
			if s.dialect.Name() != "sqlite" && !historicalBackfill {
				spentRefs = make([]models.UtxoId, 0, len(transaction.Consumed()))
			}
			var skippedRefs []models.UtxoId
			var consumedStakeDeltas []stakeCredentialDelta
			if !historicalBackfill {
				consumedStakeDeltas = make(
					[]stakeCredentialDelta, 0, len(transaction.Consumed()),
				)
			}
			seenConsumed := make(
				map[string]struct{},
				len(transaction.Consumed()),
			)
			uniqueConsumed := make(
				[]models.UtxoId,
				0,
				len(transaction.Consumed()),
			)
			for _, input := range transaction.Consumed() {
				refKey := utxoIdentityKey(input.Id().Bytes(), input.Index())
				if _, ok := seenConsumed[refKey]; ok {
					continue
				}
				seenConsumed[refKey] = struct{}{}
				uniqueConsumed = append(uniqueConsumed, models.UtxoId{
					Hash: input.Id().Bytes(),
					Idx:  input.Index(),
				})
			}
			for start := 0; start < len(uniqueConsumed); start += utxoBatchSize {
				end := min(start+utxoBatchSize, len(uniqueConsumed))
				batch := uniqueConsumed[start:end]
				updated := make(map[string]struct{}, len(batch))
				if s.dialect.Name() == "sqlite" && len(batch) > 1 {
					query := consumeUtxosBatchQuery(
						len(batch), !historicalBackfill,
					)
					args := make([]any, 0, 2+len(batch)*2)
					args = append(args, point.Slot, hash)
					for _, ref := range batch {
						args = append(args, ref.Hash, ref.Idx)
					}
					rows, err := s.queryRowsCached(ctx, db, query, args...)
					if err != nil {
						return err
					}
					updatedRows, deltas, scanErr := scanConsumedUtxoRows(
						rows, !historicalBackfill,
					)
					if scanErr != nil {
						return scanErr
					}
					updated = updatedRows
					consumedStakeDeltas = append(consumedStakeDeltas, deltas...)
				} else if s.dialect.Name() == "sqlite" && !historicalBackfill {
					rows, err := s.queryRowsCached(
						ctx, db, consumeUtxoSQLiteReturningSQL,
						point.Slot, hash, batch[0].Hash, batch[0].Idx,
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
							point.Slot, hash, ref.Hash, ref.Idx,
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
						spentBy     []byte
					)
					err = s.queryRowCached(ctx, db, getUtxoSpendStateQuery,
						utxoID.Hash,
						utxoID.Idx,
					).Scan(&deletedSlot, &spentBy)
					if errors.Is(err, sql.ErrNoRows) {
						continue
					}
					if err != nil {
						return err
					}
					if bytes.Equal(spentBy, hash) {
						continue
					}
					if deletedSlot == 0 && len(spentBy) == 0 {
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
						"%w: %x:%d (already spent_by=%x deleted_slot=%d, this_tx=%x)",
						types.ErrUtxoConflict,
						utxoID.Hash,
						utxoID.Idx,
						spentBy,
						deletedSlot,
						hash,
					)
				}
			}
			if historicalBackfill {
				return nil
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
					ctx, db, skippedRefs, false,
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
			if batchedAccumulator != nil {
				return nil
			}
			return s.refreshRewardLiveStakeDeltas(
				ctx, db, batchedStakeDeltas, point.Slot,
			)
		},
	)
	if err != nil || batchedAccumulator == nil {
		return err
	}
	if err := batchedAccumulator.addStakeDeltas(
		batchedStakeDeltas, point.Slot,
	); err != nil {
		return err
	}
	// The per-transaction cleanup deletes only reach flushed rows, so rows
	// an earlier application of this transaction queued in the same window
	// are replaced here.
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
			collateralFee, err := collateralFeeForTransaction(
				ctx,
				db,
				transaction,
			)
			if err != nil {
				return err
			}
			transactionID, err := queryReturnedID(ctx, db, `
INSERT INTO "transaction" (
    hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (hash) DO UPDATE SET
    block_hash = excluded.block_hash, block_index = excluded.block_index,
    slot = excluded.slot, collateral_fee = excluded.collateral_fee
RETURNING id`,
				hash,
				point.Hash,
				point.Slot,
				transaction.Type(),
				decimalUint64(transactionFee(transaction)),
				decimalUint64(types.Uint64(collateralFee)),
				decimalUint64(types.Uint64(transaction.TTL())),
				index,
				transaction.IsValid(),
			)
			if err != nil {
				return err
			}
			var certificateRefs []models.StakeCredentialRef
			if transaction.IsValid() {
				var err error
				certificateRefs, err = s.applyTransactionCertificates(
					ctx, db, transactionID, transaction.Certificates(),
					point, index, certDeposits, allowUnknownDeposits, false,
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
			fee, err := collateralFeeForTransaction(ctx, db, transaction)
			if err != nil {
				return err
			}
			_, err = db.ExecContext(ctx, `
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
			id, err := queryReturnedID(ctx, db, `
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES (?, ?, 0, 0, '0', '0', '0', 0, TRUE)
ON CONFLICT (hash) DO UPDATE SET hash = excluded.hash
RETURNING id`,
				hash,
				blockHash,
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

func (s *Store) DeleteTransactionsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	return s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			rows, err := db.QueryContext(ctx, `
SELECT hash FROM "transaction" WHERE slot > ?`,
				slot,
			)
			if err != nil {
				return err
			}
			hashes := [][]byte{}
			for rows.Next() {
				var hash []byte
				if err := rows.Scan(&hash); err != nil {
					rows.Close()
					return err
				}
				hashes = append(hashes, hash)
			}
			if err := rows.Close(); err != nil {
				return err
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("scan transactions for rollback: %w", err)
			}
			refs := []models.StakeCredentialRef{}
			for start := 0; start < len(hashes); start += 400 {
				end := min(start+400, len(hashes))
				args := make([]any, end-start)
				for i, hash := range hashes[start:end] {
					args[i] = hash
				}
				stakeRows, err := db.QueryContext(ctx, `
SELECT DISTINCT credential_tag, staking_key FROM utxo
WHERE spent_at_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				)
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
WHERE spent_at_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
UPDATE utxo SET collateral_by_tx_id = NULL
WHERE collateral_by_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
UPDATE utxo SET referenced_by_tx_id = NULL
WHERE referenced_by_tx_id IN (`+bindPlaceholders(len(args))+`)`,
					args...,
				); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
DELETE FROM utxo_reference_input
WHERE transaction_hash IN (`+bindPlaceholders(len(args))+`)`, args...); err != nil {
					return err
				}
				if _, err := db.ExecContext(ctx, `
DELETE FROM utxo_collateral_input
WHERE transaction_hash IN (`+bindPlaceholders(len(args))+`)`, args...); err != nil {
					return err
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
				`DELETE FROM "transaction" WHERE slot > ?`,
				slot,
			); err != nil {
				return err
			}
			return s.refreshRewardLiveStakeRefs(ctx, db, refs, slot)
		},
	)
}

// insertUtxoQuery is insertUtxoModel's ordinary-path INSERT (ignoreConflict
// == false), used where a caller expects the (tx_id, output_idx) pair to be
// new. It carries a trailing "RETURNING id" (see hasReturningID in
// dialect_queryer.go), so prepareHotStatements deliberately does not cache
// it on MySQL: dialectQueryer.QueryRowContext's MySQL RETURNING-id emulation
// does its own ExecContext+LastInsertId dance instead of ever calling
// QueryRowContext with the translated query text, so a cached *sql.Stmt here
// would never be consulted by that path and MySQL is left on the existing
// uncached fallback instead. SQLite and PostgreSQL both support RETURNING
// natively, so this participates in the hot-statement cache normally on
// those dialects.
const insertUtxoQuery = `
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING id`

// insertUtxoQueryIgnoreConflict is insertUtxoModel's actual production path
// (every real call site passes ignoreConflict == true): identical to
// insertUtxoQuery but with ON CONFLICT (tx_id, output_idx) DO NOTHING, for
// the snapshot-import case where this output may already exist. The
// hot-statement cache is keyed by exact query text, so this needs its own
// constant distinct from insertUtxoQuery -- see that constant's doc comment
// for the MySQL RETURNING caveat, which applies here identically. This was
// dingo's single largest uncached raw-SQL call site: a 30s CPU profile of a
// live from-genesis Preview sync attributed 3.07s (8.8% of total samples) to
// this one QueryRowContext call, almost entirely modernc.org/sqlite
// re-parsing and re-planning the identical statement text on every UTxO
// output insert.
const insertUtxoQueryIgnoreConflict = `
-- name: CreateUtxoIfAbsent :one
INSERT INTO utxo (
    transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tx_id, output_idx) DO NOTHING
RETURNING id`

// importAssetQuery is the conflict-tolerant asset INSERT used by both the
// snapshot importer and insertUtxoModel. It is a fixed query shape on every
// imported asset, so keep one prepared statement for the Store lifetime just
// like the surrounding UTxO importer statements.
const importAssetQuery = `
INSERT INTO asset (
    name, policy_id, fingerprint, utxo_id, amount
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (name, policy_id, utxo_id) DO NOTHING
`

// getAssetIDQuery resolves the caller-visible asset row ID after an
// insert that may have been skipped by its conflict clause.
const getAssetIDQuery = `
SELECT id FROM asset
WHERE utxo_id = ? AND policy_id = ? AND name = ?
ORDER BY id DESC LIMIT 1`

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
	params, err := createUtxoParams(utxo)
	if err != nil {
		return false, err
	}
	query := insertUtxoQuery
	if ignoreConflict {
		query = insertUtxoQueryIgnoreConflict
	}
	var id int64
	err = s.queryRowCached(ctx, db, query,
		params.TransactionID,
		params.CollateralReturnForTxID,
		params.TxID,
		params.PaymentKey,
		params.StakingKey,
		params.CredentialTag,
		params.DatumHash,
		nullBytes(params.SpentAtTxID),
		nullBytes(params.ReferencedByTxID),
		nullBytes(params.CollateralByTxID),
		params.AddedSlot,
		params.DeletedSlot,
		params.Amount,
		params.OutputIdx,
		params.PaymentScript,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) && ignoreConflict {
		inserted = false
		err = db.QueryRowContext(ctx, `
SELECT id FROM utxo WHERE tx_id = ? AND output_idx = ?`,
			params.TxID,
			params.OutputIdx,
		).Scan(&id)
		if err == nil {
			// Snapshot imports can create an output before its producer
			// transaction is replayed. Once that transaction is known, fill in
			// the provenance without overwriting an already-linked output.
			//
			// The stake credential deliberately is not repaired here. A
			// pointer address's credential is not stored on the row at all
			// (see pointer_stake.go); every other address form carries its
			// credential in the address, so an imported row already has it.
			_, err = db.ExecContext(ctx, `
UPDATE utxo
SET transaction_id = COALESCE(transaction_id, ?),
    collateral_return_for_tx_id = COALESCE(collateral_return_for_tx_id, ?)
WHERE id = ?`,
				params.TransactionID,
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
		relationErr = s.persistUtxoRelations(ctx, db, utxo, uint(id))
	}
	if relationErr != nil {
		return false, relationErr
	}
	return inserted, nil
}

func (s *Store) persistUtxoRelations(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	id uint,
) error {
	utxo.ID = id
	if err := persistUtxoPointer(ctx, db, int64(id), utxo.Pointer); err != nil {
		return err
	}
	for i := range utxo.Assets {
		asset := &utxo.Assets[i]
		asset.UtxoID = id
		asset.ID = 0
		if _, err := s.execCached(ctx, db, importAssetQuery,
			asset.Name,
			asset.PolicyId,
			asset.Fingerprint,
			sql.NullInt64{Int64: int64(id), Valid: true},
			sql.NullString{
				String: decimalUint64(asset.Amount),
				Valid:  true,
			},
		); err != nil {
			return err
		}
		var assetID uint
		if err := s.queryRowCached(ctx, db, getAssetIDQuery,
			id,
			asset.PolicyId,
			asset.Name,
		).Scan(&assetID); err != nil {
			return err
		}
		asset.ID = assetID
	}
	return nil
}

func (s *Store) persistUtxoRelationsWithDeferredAssets(
	ctx context.Context,
	db queryer,
	utxo *models.Utxo,
	id uint,
	rows *rowBatch,
) error {
	utxo.ID = id
	if err := persistUtxoPointer(ctx, db, int64(id), utxo.Pointer); err != nil {
		return err
	}
	for i := range utxo.Assets {
		asset := &utxo.Assets[i]
		asset.UtxoID = id
		asset.ID = 0
		rows.add(
			assetShape,
			asset.Name,
			asset.PolicyId,
			asset.Fingerprint,
			sql.NullInt64{Int64: int64(id), Valid: true},
			sql.NullString{
				String: decimalUint64(asset.Amount),
				Valid:  true,
			},
		)
	}
	return nil
}

func collateralFeeForTransaction(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
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
		var amount string
		err := db.QueryRowContext(ctx, `
SELECT amount FROM utxo WHERE tx_id = ? AND output_idx = ?`,
			input.Id().Bytes(),
			input.Index(),
		).Scan(&amount)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, err
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
			if _, err := db.ExecContext(ctx, `
INSERT INTO account_withdrawal_witness (
    staking_key, credential_tag, tx_hash, added_slot
) VALUES (?, ?, ?, ?)
ON CONFLICT (tx_hash, credential_tag, staking_key) DO NOTHING`,
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
		err := db.QueryRowContext(ctx, `
SELECT id, reward FROM account
WHERE credential_tag = ? AND staking_key = ? AND active = TRUE`,
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
			err = db.QueryRowContext(ctx, `
SELECT id, reward, active FROM account
WHERE credential_tag = ? AND staking_key = ?`,
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
		if err := db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1 FROM account_reward_delta
    WHERE withdrawal = TRUE AND tx_hash = ?
      AND credential_tag = ? AND staking_key = ?
)`,
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
			if _, err := db.ExecContext(ctx, `
UPDATE account SET reward = ? WHERE id = ?`,
				strconv.FormatUint(rewardAfter, 10),
				accountID,
			); err != nil {
				return err
			}
		}
		if _, err := db.ExecContext(ctx, `
INSERT INTO account_reward_delta (
    staking_key, credential_tag, tx_hash, amount, previous_reward,
    added_slot, withdrawal
) VALUES (?, ?, ?, ?, ?, ?, TRUE)
ON CONFLICT (
    withdrawal, tx_hash, credential_tag, staking_key, added_slot
) DO NOTHING`,
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
			if err := s.refreshRewardLiveStakeAggregate(
				ctx, db,
				models.NewStakeCredentialRef(tag, stakeKey.Bytes()),
				slot,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func recordTransactionLedgerContext(ctx context.Context, db queryer, transactionID int64, slot uint64) error {
	value, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `INSERT INTO leios_transaction_context (transaction_id, slot) VALUES (?, ?) ON CONFLICT (transaction_id) DO UPDATE SET slot = excluded.slot`, transactionID, value)
	return err
}
