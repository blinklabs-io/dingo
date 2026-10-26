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

//nolint:gosec // SQL INTEGER mappings preserve the unsigned domain API.
package sqlstore

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// The batch UTxO overlay stages one trusted block-apply transaction's UTxO
// writes in memory and writes their net effect at FlushBatch. DATABASE.md
// ("Trusted batch UTxO overlay") describes the contract; the invariants the
// code below depends on are:
//
//   - Row ids are assigned at creation from sqlite_sequence, in the order the
//     per-row path would have inserted the rows, including outputs that are
//     spent again inside the batch and never written. Surviving rows, their
//     asset rows and both sqlite_sequence entries therefore end exactly as
//     the per-row path leaves them.
//   - Nothing in utxo, utxo_pointer or reward_live_stake is read while the
//     overlay holds unwritten state: the read barrier (barrierQueryer) writes
//     it first. Asset and utxo_address rows stay queued until FlushBatch, as
//     on the per-row path.
//   - Transaction rows take the ids the per-row path's inserts would, in
//     apply order, and are written before any statement that names
//     "transaction" or a table with a foreign key to it, so every foreign
//     key is checked immediately and finds its parent.
//   - Writes have no conflict clause. A UNIQUE failure is reported as
//     types.ErrBatchUtxoOverlayConflict so the caller can apply the batch
//     again on the per-row path, which repairs conflicting rows.

const (
	// overlayPrefetchChunk keys use two bind parameters each.
	overlayPrefetchChunk = 499
	// overlayUtxoInsertColumns is the explicit-id utxo insert width; 62
	// rows stay under SQLite's 999-parameter limit.
	overlayUtxoInsertColumns = 16
	overlayMarkChunk         = 333
	overlayPointerColumns    = 4
	overlayTxColumns         = 11
)

var assetWithIDShape = rowShape{
	table: "asset",
	columns: []string{
		"id", "name", "policy_id", "fingerprint", "utxo_id", "amount",
	},
}

type overlayKey struct {
	txID string
	idx  uint32
}

func newOverlayKey(txID []byte, idx uint32) overlayKey {
	return overlayKey{txID: string(txID), idx: idx}
}

// overlayOutput is an output created inside the batch.
type overlayOutput struct {
	model     models.Utxo
	id        int64
	assetBase int64
	// spentBy is the consuming transaction's hash, nil while live.
	spentBy   []byte
	spentSlot uint64
	// written reports that the row exists in utxo, because a read barrier
	// wrote it before it was spent.
	written bool
}

// overlayInput is a pre-batch row a transaction in the batch consumes, as
// the prefetch read it and as the batch has since changed it.
type overlayInput struct {
	id          int64
	tag         int64
	stakingKey  []byte
	amount      sql.NullString
	deletedSlot uint64
	spentBy     []byte
	// deleteOnSpend marks an output an earlier transaction of the same
	// trusted batch committed. The per-row path deletes such a row when the
	// batch spends it, instead of leaving it for the post-commit prune.
	deleteOnSpend bool
}

func (in *overlayInput) live() bool {
	return in.deletedSlot == 0 && len(in.spentBy) == 0
}

type overlayMark struct {
	id   int64
	slot uint64
	hash []byte
}

// overlayStats counts what one overlay did, for the per-batch debug line.
type overlayStats struct {
	Created        int
	Elided         int
	PrebatchSpent  int
	Prefetched     int
	BarrierFlushes int
	Transactions   int
	// TransactionWrites counts the writes of staged transaction rows.
	TransactionWrites int
}

// overlayTransaction is a staged "transaction" row: its id and the values
// of the per-row path's insert, in transactionBatchInsertSQL's order.
type overlayTransaction struct {
	id     int64
	values []any
}

type utxoOverlay struct {
	created map[overlayKey]*overlayOutput
	// order holds every created output in id order.
	order  []*overlayOutput
	inputs map[overlayKey]*overlayInput
	absent map[overlayKey]struct{}
	marks  []overlayMark
	// segments holds each closed block's queued live-stake deltas, in
	// block order, until replayStakeSegments writes them.
	segments []stakeSegment
	deletes  []int64
	deleteOn map[overlayKey]struct{}
	// unwritten counts created outputs that are live and not yet written.
	unwritten int
	// writtenUpTo indexes order: every output before it was handled by a
	// barrier flush.
	writtenUpTo int

	// txSeq is the last "transaction" id handed out; txPending holds the
	// staged rows not yet written, in id order.
	txSeq     int64
	txPending []overlayTransaction
	txHashes  map[string]struct{}
	// txChildren names the tables with a foreign key to "transaction".
	txChildren []string

	utxoSeq      int64
	utxoSeqRow   bool
	utxoWritten  int64
	assetSeq     int64
	assetSeqRow  bool
	assetWritten int64

	stats overlayStats
	// mutations advances on every change, so a savepoint restore can tell
	// that it cannot undo the overlay.
	mutations uint64
	flushing  bool
	err       error
}

func (o *utxoOverlay) dirty() bool {
	return o.unwritten > 0 || len(o.marks) > 0 || len(o.segments) > 0
}

// stakeSegment is one block's queued live-stake deltas.
type stakeSegment struct {
	order  []string
	deltas map[string]pendingStakeCredentialDelta
}

// closeStakeSegment ends the current block's live-stake deltas. The per-row
// path writes them here; the overlay keeps them, so a credential's row is
// written once per transaction yet goes through the same per-block
// lifecycle (see replayStakeSegments).
func (o *utxoOverlay) closeStakeSegment(
	accumulator *transactionBatchAccumulator,
) {
	if len(accumulator.stakeDeltaOrder) == 0 {
		return
	}
	o.mutations++
	o.segments = append(o.segments, stakeSegment{
		order:  accumulator.stakeDeltaOrder,
		deltas: accumulator.stakeDeltas,
	})
	accumulator.clearQueuedStakeDeltas()
}

// errOverlaySavepoint reports a savepoint rollback across overlay changes.
// The overlay keeps no undo log: no savepoint is taken inside a trusted
// block-apply transaction while it is active, so a restore that would need
// one fails the batch instead of continuing from wrong state.
var errOverlaySavepoint = errors.New(
	"batch UTxO overlay cannot roll back to a savepoint",
)

// EnableBatchUtxoOverlay attaches a UTxO overlay to accumulator for the rest
// of txn's block-apply batch and reads the pre-batch rows of inputs, the
// inputs the batch consumes that it does not itself produce. deleteOnSpend
// names inputs an earlier transaction of the same trusted batch committed.
// It reports false, and changes nothing, outside SQLite core storage mode.
func (s *Store) EnableBatchUtxoOverlay(
	accumulator types.MetadataBatchAccumulator,
	txn types.Txn,
	inputs []models.UtxoId,
	deleteOnSpend []models.UtxoId,
) (bool, error) {
	batched, ok := accumulator.(*transactionBatchAccumulator)
	if !ok || s.dialect.Name() != "sqlite" ||
		s.storageMode == types.StorageModeAPI {
		return false, nil
	}
	transaction, ok := txn.(*sqlTxn)
	if !ok || transaction.owner != s {
		return false, nil
	}
	if batched.overlay != nil {
		return false, errors.New("batch UTxO overlay already enabled")
	}
	if err := transaction.bindBatch(batched); err != nil {
		return false, err
	}
	overlay := &utxoOverlay{
		created:  make(map[overlayKey]*overlayOutput),
		inputs:   make(map[overlayKey]*overlayInput, len(inputs)),
		absent:   make(map[overlayKey]struct{}),
		deleteOn: make(map[overlayKey]struct{}, len(deleteOnSpend)),
		txHashes: make(map[string]struct{}),
	}
	for _, id := range deleteOnSpend {
		overlay.deleteOn[newOverlayKey(id.Hash, id.Idx)] = struct{}{}
	}
	err := s.withWriteTransaction(
		txn,
		func(db queryer, ctx context.Context) error {
			var err error
			overlay.utxoSeq, overlay.utxoSeqRow, err = readSQLiteSequence(
				ctx, db, "utxo",
			)
			if err != nil {
				return err
			}
			overlay.utxoWritten = overlay.utxoSeq
			overlay.assetSeq, overlay.assetSeqRow, err = readSQLiteSequence(
				ctx, db, "asset",
			)
			if err != nil {
				return err
			}
			overlay.assetWritten = overlay.assetSeq
			if err := db.QueryRowContext(
				ctx, overlayTransactionMaxIDQuery,
			).Scan(&overlay.txSeq); err != nil {
				return fmt.Errorf("read transaction id: %w", err)
			}
			overlay.txChildren, err = transactionChildTables(ctx, db)
			if err != nil {
				return err
			}
			return prefetchOverlayInputs(ctx, db, overlay, inputs)
		},
	)
	if err != nil {
		return false, fmt.Errorf("prefetch batch UTxO overlay inputs: %w", err)
	}
	batched.overlay = overlay
	transaction.setOverlayAccumulator(batched)
	return true, nil
}

func readSQLiteSequence(
	ctx context.Context,
	db queryer,
	table string,
) (int64, bool, error) {
	var seq int64
	err := db.QueryRowContext(ctx, `-- name: BatchOverlaySequence :one
SELECT seq FROM sqlite_sequence WHERE name = ?`, table).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read %s id sequence: %w", table, err)
	}
	return seq, true, nil
}

// overlayTransactionMaxIDQuery is the id an AUTOINCREMENT insert continues
// from: the larger of the highest id and the sequence, which still holds the
// ids of rows a rollback deleted.
const overlayTransactionMaxIDQuery = `
-- name: BatchOverlayTransactionMaxID :one
SELECT MAX(
    COALESCE((SELECT MAX(id) FROM "transaction"), 0),
    COALESCE(
        (SELECT seq FROM sqlite_sequence WHERE name = 'transaction'), 0
    )
)`

const overlayTransactionChildrenQuery = `
-- name: BatchOverlayTransactionChildren :many
SELECT DISTINCT m.name
FROM sqlite_master AS m, pragma_foreign_key_list(m.name) AS p
WHERE m.type = 'table' AND p."table" = 'transaction'`

// transactionChildTables names the tables whose foreign keys reference
// "transaction", read from the schema so that a new one is covered.
func transactionChildTables(
	ctx context.Context,
	db queryer,
) ([]string, error) {
	rows, err := db.QueryContext(ctx, overlayTransactionChildrenQuery)
	if err != nil {
		return nil, fmt.Errorf("read transaction foreign keys: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// stageTransaction queues a "transaction" row with the id the per-row
// path's AUTOINCREMENT insert would assign, in apply order: nothing else
// inserts into the table inside a trusted batch, and the explicit-id insert
// raises sqlite_sequence to the highest id as that insert would.
// hash and rest are transactionBatchInsertSQL's arguments.
func (o *utxoOverlay) stageTransaction(
	hash []byte,
	rest ...any,
) (int64, error) {
	if _, ok := o.txHashes[string(hash)]; ok {
		return 0, fmt.Errorf(
			"%w: transaction %x applied twice in one batch",
			types.ErrBatchUtxoOverlayConflict, hash,
		)
	}
	o.mutations++
	o.txHashes[string(hash)] = struct{}{}
	o.txSeq++
	o.txPending = append(o.txPending, overlayTransaction{
		id: o.txSeq, values: append([]any{hash}, rest...),
	})
	o.stats.Transactions++
	return o.txSeq, nil
}

// needsTransactions reports whether query names "transaction" or a table
// with a foreign key to it.
func (o *utxoOverlay) needsTransactions(query string) bool {
	if containsIdentifier(query, "transaction") {
		return true
	}
	for _, table := range o.txChildren {
		if containsIdentifier(query, table) {
			return true
		}
	}
	return false
}

// writeOverlayTransactions writes the staged transaction rows with their
// ids. No conflict clause: a hash that already has a row is an overlay
// conflict, and the per-row path reuses that row.
func writeOverlayTransactions(
	ctx context.Context,
	db queryer,
	o *utxoOverlay,
) error {
	if len(o.txPending) == 0 {
		return nil
	}
	o.mutations++
	o.stats.TransactionWrites++
	args := make([]any, 0, len(o.txPending)*overlayTxColumns)
	for _, row := range o.txPending {
		args = append(append(args, row.id), row.values...)
	}
	err := execChunked(ctx, db, args, overlayTxColumns, func(rows int) string {
		return `-- name: BatchOverlayInsertTransactions :exec
INSERT INTO "transaction" (
    id, hash, block_hash, metadata, slot, type, fee, collateral_fee, ttl,
    block_index, valid
) VALUES ` + strings.TrimSuffix(strings.Repeat(
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), ", rows), ", ")
	})
	if err != nil {
		return overlayWriteError("insert batch transaction rows", err)
	}
	o.txPending = nil
	return nil
}

// overlayTransactionBarrier writes the staged transaction rows before a
// statement that needs them. The overlay's own writes run with flushing set
// and write them first themselves.
func (s *Store) overlayTransactionBarrier(
	ctx context.Context,
	db queryer,
	accumulator *transactionBatchAccumulator,
	query string,
) error {
	o := accumulator.overlay
	if o == nil || o.flushing || len(o.txPending) == 0 {
		return nil
	}
	if o.err != nil {
		return o.err
	}
	if !o.needsTransactions(query) {
		return nil
	}
	o.flushing = true
	defer func() { o.flushing = false }()
	if err := writeOverlayTransactions(ctx, db, o); err != nil {
		o.err = err
		return err
	}
	return nil
}

// overlayPrefetchQuery reads pre-batch rows by key. CROSS JOIN fixes the
// loop order and INDEXED BY the index: without both, sqlite_stat1's view of
// the liveness columns drives SQLite to a deleted_slot index (see
// consumeUtxosBatchQuery). TestBatchUtxoOverlayStatementPlans pins the plan.
func overlayPrefetchQuery(keys int) string {
	values := strings.TrimSuffix(strings.Repeat("(?, ?), ", keys), ", ")
	return `-- name: BatchOverlayPrefetchInputs :many
SELECT u.id, u.tx_id, u.output_idx, u.credential_tag, u.staking_key,
       u.amount, u.deleted_slot, u.spent_at_tx_id
FROM (VALUES ` + values + `) AS k
CROSS JOIN utxo AS u INDEXED BY tx_id_output_idx
  ON u.tx_id = k.column1 AND u.output_idx = k.column2`
}

func prefetchOverlayInputs(
	ctx context.Context,
	db queryer,
	overlay *utxoOverlay,
	inputs []models.UtxoId,
) error {
	keys := make([]models.UtxoId, 0, len(inputs))
	for _, input := range inputs {
		key := newOverlayKey(input.Hash, input.Idx)
		if _, ok := overlay.inputs[key]; ok {
			continue
		}
		if _, ok := overlay.absent[key]; ok {
			continue
		}
		// Reserve the key so duplicates in inputs are read once.
		overlay.absent[key] = struct{}{}
		keys = append(keys, input)
	}
	for start := 0; start < len(keys); start += overlayPrefetchChunk {
		end := min(start+overlayPrefetchChunk, len(keys))
		chunk := keys[start:end]
		args := make([]any, 0, 2*len(chunk))
		for _, key := range chunk {
			args = append(args, key.Hash, key.Idx)
		}
		rows, err := db.QueryContext(
			ctx, overlayPrefetchQuery(len(chunk)), args...,
		)
		if err != nil {
			return err
		}
		if err := scanOverlayInputs(rows, overlay); err != nil {
			return err
		}
	}
	overlay.stats.Prefetched += len(keys)
	return nil
}

func scanOverlayInputs(rows *sql.Rows, overlay *utxoOverlay) error {
	defer rows.Close()
	for rows.Next() {
		var (
			in          overlayInput
			txID        []byte
			outputIdx   int64
			deletedSlot sql.NullInt64
		)
		if err := rows.Scan(
			&in.id, &txID, &outputIdx, &in.tag, &in.stakingKey,
			&in.amount, &deletedSlot, &in.spentBy,
		); err != nil {
			return err
		}
		if deletedSlot.Valid {
			in.deletedSlot = uint64(deletedSlot.Int64)
		}
		key := newOverlayKey(txID, uint32(outputIdx))
		_, in.deleteOnSpend = overlay.deleteOn[key]
		delete(overlay.absent, key)
		overlay.inputs[key] = &in
	}
	return rows.Err()
}

// lookupInput resolves a consumed key the prefetch did not cover. The ledger
// prefetches every input from the same blocks, so this is a safety net, not
// a path the batch is expected to take. It reads a pre-batch row, which no
// pending overlay state changes, so it skips the read barrier: a barrier
// write here would run in the middle of an overlay update.
func (o *utxoOverlay) lookupInput(
	ctx context.Context,
	db queryer,
	id models.UtxoId,
) (*overlayInput, error) {
	key := newOverlayKey(id.Hash, id.Idx)
	if in, ok := o.inputs[key]; ok {
		return in, nil
	}
	if _, ok := o.absent[key]; ok {
		return nil, nil
	}
	flushing := o.flushing
	o.flushing = true
	err := prefetchOverlayInputs(ctx, db, o, []models.UtxoId{id})
	o.flushing = flushing
	if err != nil {
		return nil, err
	}
	return o.inputs[key], nil
}

// collateralFee is collateralFeeForTransaction resolving each collateral
// amount from the overlay. Like the per-row lookup it ignores liveness: an
// output already spent earlier in the batch still has its amount.
func (o *utxoOverlay) collateralFee(
	ctx context.Context,
	db queryer,
	transaction lcommon.Transaction,
) (uint64, error) {
	return collateralFeeWithLookup(transaction, func(
		input lcommon.TransactionInput,
	) (string, bool, error) {
		key := newOverlayKey(input.Id().Bytes(), input.Index())
		if out, ok := o.created[key]; ok {
			return decimalUint64(out.model.Amount), true, nil
		}
		in, err := o.lookupInput(ctx, db, models.UtxoId{
			Hash: input.Id().Bytes(), Idx: input.Index(),
		})
		if err != nil || in == nil || !in.amount.Valid {
			return "", false, err
		}
		return in.amount.String, true, nil
	})
}

// create records the produced outputs of one transaction level and returns
// their stake deltas.
func (o *utxoOverlay) create(
	produced []models.Utxo,
) ([]stakeCredentialDelta, error) {
	o.mutations++
	deltas := make([]stakeCredentialDelta, 0, len(produced))
	for i := range produced {
		model := produced[i]
		key := newOverlayKey(model.TxId, model.OutputIdx)
		if _, exists := o.created[key]; exists {
			return nil, fmt.Errorf(
				"%w: output %x#%d produced twice in one batch",
				types.ErrBatchUtxoOverlayConflict, model.TxId, model.OutputIdx,
			)
		}
		o.utxoSeq++
		out := &overlayOutput{
			model:     model,
			id:        o.utxoSeq,
			assetBase: o.assetSeq + 1,
		}
		out.model.ID = uint(out.id)
		o.assetSeq += int64(len(model.Assets))
		o.created[key] = out
		o.order = append(o.order, out)
		o.unwritten++
		o.stats.Created++
		if len(model.StakingKey) > 0 {
			gain, err := producedStakeCredentialDelta(
				model.CredentialTag, model.StakingKey, model.Amount, true,
			)
			if err != nil {
				return nil, err
			}
			deltas = append(deltas, gain)
		}
	}
	return deltas, nil
}

// consume spends one transaction level's unique inputs. It returns the
// stake deltas of the inputs it spent and zero-delta touches for inputs that
// were already spent by this same transaction, as the per-row spend does.
func (o *utxoOverlay) consume(
	ctx context.Context,
	db queryer,
	inputs []models.UtxoId,
	slot uint64,
	hash []byte,
) (spent, skipped []stakeCredentialDelta, err error) {
	o.mutations++
	for _, input := range inputs {
		key := newOverlayKey(input.Hash, input.Idx)
		if out, ok := o.created[key]; ok {
			if out.spentBy != nil {
				if bytes.Equal(out.spentBy, hash) {
					skipped = appendStakeTouch(
						skipped, int64(out.model.CredentialTag),
						out.model.StakingKey,
					)
					continue
				}
				return nil, nil, overlayConflict(
					input, out.spentBy, out.spentSlot, hash,
				)
			}
			out.spentBy = append([]byte(nil), hash...)
			out.spentSlot = slot
			o.stats.Elided++
			if out.written {
				o.marks = append(o.marks, overlayMark{
					id: out.id, slot: slot, hash: out.spentBy,
				})
				o.deletes = append(o.deletes, out.id)
			} else {
				o.unwritten--
			}
			delta, ok, err := consumedUtxoStakeDelta(
				int64(out.model.CredentialTag),
				out.model.StakingKey,
				sql.NullString{
					String: decimalUint64(out.model.Amount), Valid: true,
				},
			)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				spent = append(spent, delta)
			}
			continue
		}
		in, err := o.lookupInput(ctx, db, input)
		if err != nil {
			return nil, nil, err
		}
		if in == nil {
			// The per-row spend matches nothing and the follow-up read
			// finds no row: a missing input changes nothing.
			continue
		}
		if !in.live() {
			if bytes.Equal(in.spentBy, hash) {
				skipped = appendStakeTouch(skipped, in.tag, in.stakingKey)
				continue
			}
			return nil, nil, overlayConflict(
				input, in.spentBy, in.deletedSlot, hash,
			)
		}
		in.deletedSlot = slot
		in.spentBy = append([]byte(nil), hash...)
		o.stats.PrebatchSpent++
		o.marks = append(o.marks, overlayMark{
			id: in.id, slot: slot, hash: in.spentBy,
		})
		if in.deleteOnSpend {
			o.deletes = append(o.deletes, in.id)
		}
		delta, ok, err := consumedUtxoStakeDelta(
			in.tag, in.stakingKey, in.amount,
		)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			spent = append(spent, delta)
		}
	}
	return spent, skipped, nil
}

func appendStakeTouch(
	deltas []stakeCredentialDelta,
	tag int64,
	key []byte,
) []stakeCredentialDelta {
	if len(key) == 0 {
		return deltas
	}
	return append(deltas, stakeCredentialDelta{
		ref: models.NewStakeCredentialRef(uint8(tag), key),
	})
}

func overlayConflict(
	input models.UtxoId,
	spentBy []byte,
	deletedSlot uint64,
	hash []byte,
) error {
	return fmt.Errorf(
		"%w: %x:%d (already spent_by=%x deleted_slot=%d, this_tx=%x)",
		types.ErrUtxoConflict,
		input.Hash,
		input.Idx,
		spentBy,
		deletedSlot,
		hash,
	)
}

// writeOverlay writes what the overlay holds. A barrier write inserts every
// live unwritten output, its pointer row and the queued spend marks, so the
// tables read as the per-row path would have them; final also deletes the
// rows the per-row path deletes before commit and queues the asset and
// address rows of every surviving output.
func (s *Store) writeOverlay(
	ctx context.Context,
	db queryer,
	accumulator *transactionBatchAccumulator,
	final bool,
) error {
	o := accumulator.overlay
	if o.unwritten > 0 || len(o.marks) > 0 || final {
		// A savepoint rollback undoes these rows but not the overlay's
		// record of them; restore must see that the overlay changed.
		o.mutations++
	}
	// utxo rows reference their producing and spending transactions.
	if err := writeOverlayTransactions(ctx, db, o); err != nil {
		return err
	}
	pending := make([]*overlayOutput, 0, o.unwritten)
	for _, out := range o.order[o.writtenUpTo:] {
		if out.written || out.spentBy != nil {
			continue
		}
		pending = append(pending, out)
	}
	if err := s.insertOverlayUtxos(ctx, db, o, pending); err != nil {
		return err
	}
	if err := insertOverlayPointers(ctx, db, pending); err != nil {
		return err
	}
	for _, out := range pending {
		out.written = true
	}
	o.unwritten = 0
	o.writtenUpTo = len(o.order)
	deleted := make(map[int64]struct{}, len(o.deletes))
	if final {
		for _, id := range o.deletes {
			deleted[id] = struct{}{}
		}
	}
	if err := markOverlaySpends(ctx, db, o.marks, deleted); err != nil {
		return err
	}
	o.marks = nil
	if !final {
		return nil
	}
	if err := deleteOverlayRows(ctx, db, o.deletes); err != nil {
		return err
	}
	o.deletes = nil
	for _, out := range o.order {
		if out.spentBy != nil {
			continue
		}
		queueOverlayRelations(&accumulator.rows, out, &o.assetWritten)
	}
	return nil
}

func (s *Store) insertOverlayUtxos(
	ctx context.Context,
	db queryer,
	o *utxoOverlay,
	outputs []*overlayOutput,
) error {
	perStatement := max(1, s.dialect.ParameterLimit()/overlayUtxoInsertColumns)
	row := "(" + strings.TrimSuffix(
		strings.Repeat("?, ", overlayUtxoInsertColumns), ", ",
	) + ")"
	for start := 0; start < len(outputs); start += perStatement {
		end := min(start+perStatement, len(outputs))
		args := make([]any, 0, (end-start)*overlayUtxoInsertColumns)
		for _, out := range outputs[start:end] {
			params, err := createUtxoParams(&out.model)
			if err != nil {
				return err
			}
			args = append(args,
				out.id,
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
			o.utxoWritten = max(o.utxoWritten, out.id)
		}
		query := `-- name: BatchOverlayInsertUtxos :exec
INSERT INTO utxo (
    id, transaction_id, collateral_return_for_tx_id, tx_id, payment_key,
    staking_key, credential_tag, datum_hash, spent_at_tx_id,
    referenced_by_tx_id, collateral_by_tx_id, added_slot, deleted_slot,
    amount, output_idx, payment_script
) VALUES ` + strings.TrimSuffix(strings.Repeat(row+", ", end-start), ", ")
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return overlayWriteError("insert batch UTxO rows", err)
		}
	}
	return nil
}

func insertOverlayPointers(
	ctx context.Context,
	db queryer,
	outputs []*overlayOutput,
) error {
	args := make([]any, 0)
	for _, out := range outputs {
		pointer := out.model.Pointer
		if pointer == nil {
			continue
		}
		// An unrepresentable position is not recorded, as in
		// persistUtxoPointer.
		slot, err := checkedInt64(pointer.Slot)
		if err != nil {
			continue
		}
		txIndex, err := checkedInt64(pointer.TxIndex)
		if err != nil {
			continue
		}
		certIndex, err := checkedInt64(pointer.CertIndex)
		if err != nil {
			continue
		}
		args = append(args, out.id, slot, txIndex, certIndex)
	}
	const chunk = 999 / overlayPointerColumns * overlayPointerColumns
	for start := 0; start < len(args); start += chunk {
		end := min(start+chunk, len(args))
		rows := (end - start) / overlayPointerColumns
		query := `-- name: BatchOverlayInsertUtxoPointers :exec
INSERT INTO utxo_pointer (utxo_id, ptr_slot, ptr_tx_index, ptr_cert_index)
VALUES ` + strings.TrimSuffix(strings.Repeat("(?, ?, ?, ?), ", rows), ", ")
		_, err := db.ExecContext(ctx, query, args[start:end]...)
		if err != nil {
			return overlayWriteError("record batch UTxO pointers", err)
		}
	}
	return nil
}

// overlayMarkQuery marks spends by primary key. The rows are joined from the
// VALUES list onto utxo's rowid, so no index on the liveness columns is ever
// a candidate.
func overlayMarkQuery(rows int) string {
	return `-- name: BatchOverlayMarkSpent :exec
UPDATE utxo SET deleted_slot = v.column2, spent_at_tx_id = v.column3
FROM (VALUES ` +
		strings.TrimSuffix(strings.Repeat("(?, ?, ?), ", rows), ", ") + `) AS v
WHERE utxo.id = v.column1`
}

func markOverlaySpends(
	ctx context.Context,
	db queryer,
	marks []overlayMark,
	skip map[int64]struct{},
) error {
	args := make([]any, 0, 3*min(len(marks), overlayMarkChunk))
	rows := 0
	flush := func() error {
		if rows == 0 {
			return nil
		}
		if _, err := db.ExecContext(
			ctx, overlayMarkQuery(rows), args...,
		); err != nil {
			return overlayWriteError("mark batch UTxO spends", err)
		}
		args = args[:0]
		rows = 0
		return nil
	}
	for _, mark := range marks {
		if _, ok := skip[mark.id]; ok {
			continue
		}
		slot, err := checkedInt64(mark.slot)
		if err != nil {
			return err
		}
		args = append(args, mark.id, slot, mark.hash)
		rows++
		if rows == overlayMarkChunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	return flush()
}

func deleteOverlayRows(
	ctx context.Context,
	db queryer,
	ids []int64,
) error {
	const chunk = 999
	for start := 0; start < len(ids); start += chunk {
		end := min(start+chunk, len(ids))
		args := make([]any, 0, end-start)
		for _, id := range ids[start:end] {
			args = append(args, id)
		}
		query := `-- name: BatchOverlayDeleteUtxos :exec
DELETE FROM utxo WHERE id IN (` +
			strings.TrimSuffix(strings.Repeat("?, ", end-start), ", ") + `)`
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return overlayWriteError("delete batch-spent UTxO rows", err)
		}
	}
	return nil
}

// queueOverlayRelations queues out's asset rows. Asset ids are the ones
// reserved at creation, so an elided output's assets leave the same gap in
// the id sequence that inserting and deleting them would. It queues no
// utxo_address row: the overlay runs only in core storage mode, which does
// not index UTxO addresses.
func queueOverlayRelations(
	rows *rowBatch,
	out *overlayOutput,
	assetWritten *int64,
) {
	for i := range out.model.Assets {
		asset := &out.model.Assets[i]
		id := out.assetBase + int64(i)
		rows.add(
			assetWithIDShape,
			id,
			asset.Name,
			asset.PolicyId,
			asset.Fingerprint,
			sql.NullInt64{Int64: out.id, Valid: true},
			sql.NullString{String: decimalUint64(asset.Amount), Valid: true},
		)
		*assetWritten = max(*assetWritten, id)
	}
}

// restoreOverlaySequences raises sqlite_sequence to the last id the batch
// reserved when that id belongs to a row the batch never wrote. Inserting
// with an explicit id raises it to the highest written id on its own.
func restoreOverlaySequences(
	ctx context.Context,
	db queryer,
	o *utxoOverlay,
) error {
	if err := restoreSQLiteSequence(
		ctx, db, "utxo", o.utxoSeq, o.utxoWritten, o.utxoSeqRow,
	); err != nil {
		return err
	}
	return restoreSQLiteSequence(
		ctx, db, "asset", o.assetSeq, o.assetWritten, o.assetSeqRow,
	)
}

func restoreSQLiteSequence(
	ctx context.Context,
	db queryer,
	table string,
	reserved, written int64,
	hadRow bool,
) error {
	if reserved <= written {
		return nil
	}
	result, err := db.ExecContext(ctx, `-- name: BatchOverlayRaiseSequence :execresult
UPDATE sqlite_sequence SET seq = ? WHERE name = ? AND seq < ?`,
		reserved, table, reserved)
	if err != nil {
		return fmt.Errorf("raise %s id sequence: %w", table, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	// The row exists once any row was ever inserted; the batch may have
	// reserved ids in a table that never held one.
	if affected > 0 || hadRow || written > 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, `-- name: BatchOverlayCreateSequence :exec
INSERT INTO sqlite_sequence (name, seq) VALUES (?, ?)`,
		table, reserved); err != nil {
		return fmt.Errorf("create %s id sequence: %w", table, err)
	}
	return nil
}

// overlayConflictPattern matches a UNIQUE failure on a table whose rows the
// overlay writes under its own ids. A UNIQUE failure elsewhere -- a queued
// detail row with no conflict clause -- would fail the per-row path the same
// way, so retrying there gains nothing.
//
// A FOREIGN KEY failure in an overlay write means a row was written before
// a parent the overlay still held; the per-row path writes parents first,
// so it is retried there too.
var overlayConflictPattern = regexp.MustCompile(
	`UNIQUE constraint failed: ` +
		`(utxo|asset|utxo_pointer|reward_live_stake|transaction)\.` +
		`|FOREIGN KEY constraint failed`,
)

func overlayWriteError(action string, err error) error {
	if overlayConflictPattern.MatchString(err.Error()) {
		return fmt.Errorf(
			"%s: %w: %w", action, types.ErrBatchUtxoOverlayConflict, err,
		)
	}
	return fmt.Errorf("%s: %w", action, err)
}

// overlayBarrier writes the overlay's pending rows and the ended blocks'
// stake deltas before a statement reads utxo, utxo_pointer or
// reward_live_stake. Afterwards those tables' live rows hold what the
// per-row path would hold at this point of the batch.
func (s *Store) overlayBarrier(
	ctx context.Context,
	db queryer,
	accumulator *transactionBatchAccumulator,
) error {
	o := accumulator.overlay
	if o == nil || o.flushing || o.err != nil {
		if o != nil {
			return o.err
		}
		return nil
	}
	if !o.dirty() {
		return nil
	}
	o.flushing = true
	defer func() { o.flushing = false }()
	o.stats.BarrierFlushes++
	if err := s.writeOverlay(ctx, db, accumulator, false); err != nil {
		o.err = err
		return err
	}
	// The per-row path's utxo sequence already covers every id it handed
	// out, elided rows included. Asset rows wait for FlushBatch on both
	// paths, so that sequence is repaired only there.
	if err := restoreSQLiteSequence(
		ctx, db, "utxo", o.utxoSeq, o.utxoWritten, o.utxoSeqRow,
	); err != nil {
		o.err = err
		return err
	}
	o.utxoWritten = o.utxoSeq
	// The current block's deltas stay queued, as on the per-row path,
	// which writes them only when the block ends.
	if err := s.replayStakeSegments(ctx, db, accumulator); err != nil {
		o.err = err
		return err
	}
	return nil
}

func (s *Store) flushQueuedStakeDeltas(
	ctx context.Context,
	db queryer,
	accumulator *transactionBatchAccumulator,
) error {
	for _, key := range accumulator.stakeDeltaOrder {
		pending := accumulator.stakeDeltas[key]
		if err := s.refreshRewardLiveStakeAggregateDelta(
			ctx, db, pending.ref, pending.slot, pending.delta,
		); err != nil {
			return err
		}
	}
	return nil
}

// BatchUtxoOverlayError returns the error the accumulator's batch UTxO
// overlay recorded, nil when there is none or no overlay is attached.
func (a *transactionBatchAccumulator) BatchUtxoOverlayError() error {
	if a.overlay == nil {
		return nil
	}
	return a.overlay.err
}

func (a *transactionBatchAccumulator) clearQueuedStakeDeltas() {
	a.stakeDeltas = nil
	a.stakeDeltaOrder = nil
}

// flushOverlay is FlushBatch's overlay step: the final write, the queued
// rows, the sequence repair and the live-stake deltas.
func (s *Store) flushOverlay(
	ctx context.Context,
	db queryer,
	accumulator *transactionBatchAccumulator,
) error {
	o := accumulator.overlay
	if o.err != nil {
		return o.err
	}
	o.flushing = true
	defer func() { o.flushing = false }()
	if err := s.writeOverlay(ctx, db, accumulator, true); err != nil {
		return err
	}
	if err := accumulator.rows.flush(
		ctx, db, s.dialect.ParameterLimit(),
	); err != nil {
		return overlayWriteError("flush batch rows", err)
	}
	if err := restoreOverlaySequences(ctx, db, o); err != nil {
		return err
	}
	// Live stake goes last: a first-touch re-sum reads the utxo rows written
	// above.
	o.closeStakeSegment(accumulator)
	if err := s.replayStakeSegments(ctx, db, accumulator); err != nil {
		return err
	}
	if s.logger != nil {
		s.logger.Debug(
			"trusted batch UTxO overlay flushed",
			"created", o.stats.Created,
			"elided", o.stats.Elided,
			"prebatch_spent", o.stats.PrebatchSpent,
			"prefetched", o.stats.Prefetched,
			"barrier_flushes", o.stats.BarrierFlushes,
			"transactions", o.stats.Transactions,
			"transaction_writes", o.stats.TransactionWrites,
			"component", "database",
		)
	}
	return nil
}

// setTransactionOverlayUtxos is setTransactionWithAccumulator's UTxO step
// with an overlay attached: the produced outputs are staged and the consumed
// inputs resolved in memory, and no utxo statement runs.
func (s *Store) setTransactionOverlayUtxos(
	ctx context.Context,
	db queryer,
	o *utxoOverlay,
	transaction lcommon.Transaction,
	point ocommon.Point,
	producedModels []models.Utxo,
	uniqueConsumed []models.UtxoId,
) (produced, consumed, skipped []stakeCredentialDelta, err error) {
	if o.err != nil {
		return nil, nil, nil, o.err
	}
	produced, err = o.create(producedModels)
	if err != nil {
		return nil, nil, nil, err
	}
	consumed, skipped, err = o.consume(
		ctx, db, uniqueConsumed, point.Slot, transaction.Hash().Bytes(),
	)
	return produced, consumed, skipped, err
}

// stakeRowState is one credential's reward_live_stake row as the replay
// sees it.
type stakeRowState struct {
	ref models.StakeCredentialRef
	// dbID is the row's id in the table, 0 when there is none.
	dbID int64
	// id is the row's id after the replayed blocks, 0 when they deleted it.
	id       int64
	hasTotal bool
	utxo     uint64
	slot     uint64
	account  stakeAccountState
}

// stakeAccountState is a credential's account row as the replay reads it.
type stakeAccountState struct {
	liveStakeAccount
	createdSlot int64
}

// existedAt reports whether the account row existed at the end of the block
// at slot. Account rows are only ever created inside a batch, and
// created_slot records the slot, so the row existed then when created_slot
// is at or below it.
func (a stakeAccountState) existedAt(slot uint64) bool {
	return a.exists && a.createdSlot >= 0 && uint64(a.createdSlot) <= slot
}

// replayStakeSegments writes the closed blocks' live-stake deltas. It
// replays them block by block in memory with the per-row path's row
// lifecycle -- a credential with no account whose UTxO stake reaches zero
// loses its row, and a later touch inserts a new one -- and reserves the id
// each of the per-row path's upserts would take. Each credential's row is
// then written once, under the id the per-row path would leave it with, and
// sqlite_sequence('reward_live_stake') is raised to the last reserved id.
//
// The per-row path decides at the end of each block, so the replay needs
// two things as they were then. Account existence: see existedAt. The UTxO
// total of a credential with no running total: the utxo table now holds
// every later change too, so the later deltas are subtracted.
//
// The credentials' reward_live_stake and account rows are read up front, in
// bulk: nothing the replay does before its writes changes either table, so
// they read as the per-row path's per-credential reads would.
func (s *Store) replayStakeSegments(
	ctx context.Context,
	db queryer,
	accumulator *transactionBatchAccumulator,
) error {
	o := accumulator.overlay
	segments := o.segments
	if len(segments) == 0 {
		return nil
	}
	o.segments = nil
	o.mutations++
	// remaining starts as every delta not yet replayed, including the
	// current block's, and loses each segment's delta as it is replayed.
	remaining := make(map[string]int64)
	addRemaining := func(key string, delta int64) error {
		sum, err := addStakeDelta(remaining[key], delta)
		remaining[key] = sum
		return err
	}
	for _, key := range accumulator.stakeDeltaOrder {
		if err := addRemaining(
			key, accumulator.stakeDeltas[key].delta,
		); err != nil {
			return err
		}
	}
	states := make(map[string]*stakeRowState)
	var ordered []*stakeRowState
	for _, segment := range segments {
		for _, key := range segment.order {
			pending := segment.deltas[key]
			if err := addRemaining(key, pending.delta); err != nil {
				return err
			}
			if _, ok := states[key]; !ok {
				st := &stakeRowState{ref: pending.ref}
				states[key] = st
				ordered = append(ordered, st)
			}
		}
	}
	if err := loadStakeRowStates(ctx, db, states, ordered); err != nil {
		return err
	}
	reserved, hadRow, err := readSQLiteSequence(
		ctx, db, "reward_live_stake",
	)
	if err != nil {
		return err
	}
	for _, segment := range segments {
		for _, key := range segment.order {
			pending := segment.deltas[key]
			st, ok := states[key]
			if !ok {
				return fmt.Errorf(
					"reward live stake replay lost credential %d:%x",
					pending.ref.Tag, pending.ref.Key,
				)
			}
			if err := addRemaining(key, -pending.delta); err != nil {
				return err
			}
			var utxo uint64
			if st.hasTotal {
				utxo, err = applyUtxoStakeDelta(
					st.utxo, pending.delta, pending.ref,
				)
			} else {
				utxo, err = s.stakeAtSegmentEnd(
					ctx, db, pending.ref, remaining[key],
				)
			}
			if err != nil {
				return err
			}
			if utxo == 0 && !st.account.existedAt(pending.slot) {
				st.id = 0
				st.hasTotal = false
				st.utxo = 0
				st.slot = pending.slot
				continue
			}
			// Every upsert takes an id, also one that updates.
			reserved++
			if st.id == 0 {
				st.id = reserved
			}
			st.hasTotal = true
			st.utxo = utxo
			st.slot = pending.slot
		}
	}
	written, err := writeStakeRowStates(ctx, db, ordered)
	if err != nil {
		return err
	}
	return restoreSQLiteSequence(
		ctx, db, "reward_live_stake", reserved, written, hadRow,
	)
}

func addStakeDelta(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, errors.New("reward live stake delta overflow")
	}
	return a + b, nil
}

// stakeKeyValues is a VALUES list of n (credential_tag, staking_key) rows.
func stakeKeyValues(n int) string {
	return strings.TrimSuffix(strings.Repeat("(?, ?), ", n), ", ")
}

// overlayStakeRowsQuery and overlayStakeAccountsQuery read the replay's
// credentials by their unique credential index. Neither index is deferred
// or dropped for catch-up. TestBatchUtxoOverlayStatementPlans pins both.
func overlayStakeRowsQuery(n int) string {
	return `-- name: BatchOverlayRewardLiveStakeRows :many
SELECT r.credential_tag, r.staking_key, r.id, r.utxo_stake
FROM (VALUES ` + stakeKeyValues(n) + `) AS k
CROSS JOIN reward_live_stake AS r INDEXED BY idx_reward_live_stake_cred
  ON r.credential_tag = k.column1 AND r.staking_key = k.column2`
}

func overlayStakeAccountsQuery(n int) string {
	return `-- name: BatchOverlayRewardLiveStakeAccounts :many
SELECT a.credential_tag, a.staking_key, a.reward, a.pool, a.active,
       a.added_slot, a.created_slot
FROM (VALUES ` + stakeKeyValues(n) + `) AS k
CROSS JOIN account AS a INDEXED BY idx_account_credential
  ON a.credential_tag = k.column1 AND a.staking_key = k.column2`
}

// loadStakeRowStates fills each state's reward_live_stake row and account.
func loadStakeRowStates(
	ctx context.Context,
	db queryer,
	states map[string]*stakeRowState,
	ordered []*stakeRowState,
) error {
	for start := 0; start < len(ordered); start += overlayPrefetchChunk {
		chunk := ordered[start:min(start+overlayPrefetchChunk, len(ordered))]
		args := make([]any, 0, 2*len(chunk))
		for _, st := range chunk {
			args = append(args, st.ref.Tag, st.ref.Key)
		}
		if err := scanStakeRows(
			ctx, db, overlayStakeRowsQuery(len(chunk)), args, states,
		); err != nil {
			return fmt.Errorf("read reward live stake rows: %w", err)
		}
		if err := scanStakeAccounts(
			ctx, db, overlayStakeAccountsQuery(len(chunk)), args, states,
		); err != nil {
			return fmt.Errorf("read reward live stake accounts: %w", err)
		}
	}
	return nil
}

func scanStakeRows(
	ctx context.Context,
	db queryer,
	query string,
	args []any,
	states map[string]*stakeRowState,
) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			tag int64
			key []byte
			id  int64
			raw sql.NullString
		)
		if err := rows.Scan(&tag, &key, &id, &raw); err != nil {
			return err
		}
		st, ok := states[models.NewStakeCredentialRef(uint8(tag), key).MapKey()]
		if !ok {
			continue
		}
		st.dbID = id
		st.id = id
		// An empty total is no baseline, as in currentCredentialUtxoStake:
		// the per-row path re-sums it but keeps the row.
		if raw.Valid && raw.String != "" {
			st.utxo, err = parseUint64(
				"reward live stake UTxO total", raw.String,
			)
			if err != nil {
				return err
			}
			st.hasTotal = true
		}
	}
	return rows.Err()
}

func scanStakeAccounts(
	ctx context.Context,
	db queryer,
	query string,
	args []any,
	states map[string]*stakeRowState,
) error {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			tag     int64
			key     []byte
			account stakeAccountState
		)
		if err := rows.Scan(
			&tag, &key, &account.reward, &account.pool, &account.active,
			&account.addedSlot, &account.createdSlot,
		); err != nil {
			return err
		}
		st, ok := states[models.NewStakeCredentialRef(uint8(tag), key).MapKey()]
		if !ok {
			continue
		}
		account.exists = true
		st.account = account
	}
	return rows.Err()
}

const (
	// stakeRowInsertColumns is the bind width of one explicit-id
	// reward_live_stake row; stakeRowUpdateColumns of one in-place update.
	stakeRowInsertColumns = 11
	stakeRowUpdateColumns = 9
)

// writeStakeRowStates writes each credential's replayed row: rows the
// replay removed or moved to a new id are deleted, rows under a new id are
// inserted with it, and rows that keep their id are updated in place. It
// returns the highest id it inserted, or zero when none.
func writeStakeRowStates(
	ctx context.Context,
	db queryer,
	ordered []*stakeRowState,
) (int64, error) {
	var (
		deletes []any
		inserts []any
		updates []any
		written int64
	)
	for _, st := range ordered {
		replaced := st.id == 0 || st.id != st.dbID
		if replaced && st.dbID != 0 {
			deletes = append(deletes, st.dbID)
		}
		if st.id == 0 {
			continue
		}
		row, keep, err := rewardLiveStakeRowValues(
			st.ref, st.account.liveStakeAccount, st.slot, st.utxo,
		)
		if err != nil {
			return 0, err
		}
		if !keep {
			if !replaced {
				deletes = append(deletes, st.dbID)
			}
			continue
		}
		values := []any{
			row.pool,
			decimalUint64(types.Uint64(row.utxo)),
			decimalUint64(types.Uint64(row.reward)),
			decimalUint64(types.Uint64(row.total)),
			row.registered,
			row.delegationSlot,
			row.slot,
			models.RewardStakeCalculationVersion,
		}
		if !replaced {
			updates = append(append(updates, st.id), values...)
			continue
		}
		inserts = append(
			append(inserts, st.id, st.ref.Tag, st.ref.Key), values...,
		)
		written = max(written, st.id)
	}
	// Deletes go first: a credential's new row must not meet its old one in
	// the unique credential index.
	if err := execChunked(ctx, db, deletes, 1, func(rows int) string {
		return `-- name: BatchOverlayDeleteRewardLiveStake :exec
DELETE FROM reward_live_stake WHERE id IN (` +
			strings.TrimSuffix(strings.Repeat("?, ", rows), ", ") + `)`
	}); err != nil {
		return 0, overlayWriteError("delete reward live stake rows", err)
	}
	if err := execChunked(
		ctx, db, inserts, stakeRowInsertColumns, func(rows int) string {
			return `-- name: BatchOverlayInsertRewardLiveStake :exec
INSERT INTO reward_live_stake (
    id, credential_tag, staking_key, pool_key_hash, utxo_stake, reward_stake,
    total_stake, registered, pool_delegation_slot, updated_slot,
    calculation_version
) VALUES ` + strings.TrimSuffix(strings.Repeat(
				"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?), ", rows), ", ")
		},
	); err != nil {
		return 0, overlayWriteError("insert reward live stake rows", err)
	}
	if err := execChunked(
		ctx, db, updates, stakeRowUpdateColumns, func(rows int) string {
			return `-- name: BatchOverlayUpdateRewardLiveStake :exec
UPDATE reward_live_stake SET
    pool_key_hash = v.column2, utxo_stake = v.column3,
    reward_stake = v.column4, total_stake = v.column5,
    registered = v.column6, pool_delegation_slot = v.column7,
    pool_delegation_block_index = 0, pool_delegation_cert_index = 0,
    updated_slot = v.column8, calculation_version = v.column9
FROM (VALUES ` + strings.TrimSuffix(strings.Repeat(
				"(?, ?, ?, ?, ?, ?, ?, ?, ?), ", rows), ", ") + `) AS v
WHERE reward_live_stake.id = v.column1`
		},
	); err != nil {
		return 0, overlayWriteError("update reward live stake rows", err)
	}
	return written, nil
}

// execChunked runs query over args, width bind parameters per row, in
// statements that stay under SQLite's 999-parameter limit.
func execChunked(
	ctx context.Context,
	db queryer,
	args []any,
	width int,
	query func(rows int) string,
) error {
	perStatement := 999 / width * width
	for start := 0; start < len(args); start += perStatement {
		end := min(start+perStatement, len(args))
		if _, err := db.ExecContext(
			ctx, query((end-start)/width), args[start:end]...,
		); err != nil {
			return err
		}
	}
	return nil
}

// stakeAtSegmentEnd is the full re-sum the per-row path runs for a
// credential with no running total, as of the end of the block being
// replayed: today's live total less the deltas queued after that block.
func (s *Store) stakeAtSegmentEnd(
	ctx context.Context,
	db queryer,
	ref models.StakeCredentialRef,
	later int64,
) (uint64, error) {
	now, err := s.sumCredentialUtxoStake(ctx, db, ref)
	if err != nil {
		return 0, fmt.Errorf("sum reward live stake UTxOs: %w", err)
	}
	if later == math.MinInt64 {
		return 0, errors.New("reward live stake delta overflow")
	}
	return applyUtxoStakeDelta(now, -later, ref)
}
