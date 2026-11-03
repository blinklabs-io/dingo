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
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// These conservative accounting weights include row/value descriptors and
// variable payload capacity; they bound queued row data, not process RSS.
const (
	rowBatchRowOverheadBytes   = int64(24)
	rowBatchValueOverheadBytes = int64(40)
)

// rowShape is one INSERT form. key_witness has two shapes (vkey and
// bootstrap) with different column lists, so rows are grouped by shape rather
// than by table. suffix is appended after the VALUES list, for an ON CONFLICT
// clause.
type rowShape struct {
	// name is the fixed query name flush stamps on the statement, so the
	// metrics label is bounded by the shapes declared here.
	name    string
	table   string
	columns []string
	suffix  string
}

var (
	vkeyWitnessShape = rowShape{
		name:    "InsertKeyWitnessVkeyBatch",
		table:   "key_witness",
		columns: []string{"vkey", "signature", "transaction_id", "type"},
	}
	bootstrapWitnessShape = rowShape{
		name:  "InsertKeyWitnessBootstrapBatch",
		table: "key_witness",
		columns: []string{
			"signature", "public_key", "chain_code", "attributes",
			"transaction_id", "type",
		},
	}
	witnessScriptShape = rowShape{
		name:    "InsertWitnessScriptBatch",
		table:   "witness_scripts",
		columns: []string{"script_hash", "transaction_id", "type"},
	}
	plutusDataShape = rowShape{
		name:    "InsertPlutusDataBatch",
		table:   "plutus_data",
		columns: []string{"data", "transaction_id"},
	}
	redeemerShape = rowShape{
		name:  "InsertRedeemerBatch",
		table: "redeemer",
		columns: []string{
			"data", "transaction_id", "ex_units_memory", "ex_units_cpu",
			`"index"`, "tag",
		},
	}
	redeemerScriptShape = rowShape{
		name:  "InsertRedeemerScriptBatch",
		table: "redeemer_script",
		columns: []string{
			"transaction_id", "tag", `"index"`, "script_hash", "slot",
			"tx_index",
		},
	}
	addressTransactionShape = rowShape{
		name:  "InsertAddressTransactionBatch",
		table: "address_transaction",
		columns: []string{
			"payment_key", "payment_script", "staking_key", "credential_tag",
			"transaction_id", "slot", "tx_index",
		},
	}
	metadataLabelShape = rowShape{
		name:  "UpsertTransactionMetadataLabelBatch",
		table: "transaction_metadata_label",
		columns: []string{
			"transaction_id", "label", "slot", "cbor_value", "json_value",
		},
		suffix: `ON CONFLICT (transaction_id, label) DO UPDATE SET
    slot = excluded.slot,
    cbor_value = excluded.cbor_value,
    json_value = excluded.json_value`,
	}
	// datum rows are content-addressed and shared between transactions, so
	// they carry no transaction_id and are never replaced.
	datumShape = rowShape{
		name:    "InsertDatumBatch",
		table:   "datum",
		columns: []string{"hash", "raw_datum", "added_slot"},
		suffix:  "ON CONFLICT (hash) DO NOTHING",
	}
	assetShape = rowShape{
		name:    "InsertAssetBatch",
		table:   "asset",
		columns: []string{"name", "policy_id", "fingerprint", "utxo_id", "amount"},
		suffix:  "ON CONFLICT (name, policy_id, utxo_id) DO NOTHING",
	}
	utxoAddressShape = rowShape{
		name:    "UpsertUtxoAddressBatch",
		table:   "utxo_address",
		columns: []string{"utxo_id", "address"},
		suffix:  "ON CONFLICT (utxo_id) DO UPDATE SET address = excluded.address",
	}
	assetMintBurnShape = rowShape{
		name:  "InsertAssetMintBurnBatch",
		table: "asset_mint_burn",
		columns: []string{
			"tx_hash", "policy_id", "name", "fingerprint", "slot",
			"quantity", "tx_index",
		},
		suffix: "ON CONFLICT (tx_hash, policy_id, name) DO NOTHING",
	}
)

// shapeRows holds rows queued for one shape. txIDCol indexes the
// "transaction_id" column, or is -1 when the shape has none.
type shapeRows struct {
	shape              rowShape
	txIDCol            int
	rows               [][]any
	estimatedSizeBytes int64
}

// rowBatch queues rows for multi-row insertion. Shapes are kept in
// first-queued order so a flush is deterministic.
type rowBatch struct {
	entries            []shapeRows
	estimatedSizeBytes int64
	rowCount           int
	trackSize          bool
	// Backfill sets these only on a transaction level's temporary row buffer.
	maxSizeBytes   int64
	maxRows        int
	flushAtLimit   func() error
	onLimitFlushed func(int64)
	nextLimit      func() (int64, int)
	flushErr       error
	// queued records which transactions have rows pending, so replacing a
	// transaction's rows only scans when it actually has some.
	queued map[int64]struct{}
}

func (b *rowBatch) empty() bool {
	return b == nil || len(b.entries) == 0
}

func (b *rowBatch) add(shape rowShape, row ...any) {
	if b.flushErr != nil {
		return
	}
	if len(row) != len(shape.columns) {
		panic("sqlstore: row width does not match shape")
	}
	var rowSize int64
	if b.trackSize {
		rowSize = estimateRowSizeBytes(row)
	}
	// Flush before adding a row that would cross the cap. One row larger than
	// the cap is still inserted and flushed as the minimum unit.
	if b.flushAtLimit != nil && !b.empty() &&
		((b.maxRows > 0 && b.rowCount >= b.maxRows) ||
			(b.maxSizeBytes > 0 && b.estimatedSizeBytes+rowSize > b.maxSizeBytes)) {
		b.flushForLimit()
		if b.flushErr != nil {
			return
		}
	}
	i := b.entryIndex(shape)
	b.entries[i].rows = append(b.entries[i].rows, row)
	b.rowCount++
	if b.trackSize {
		b.entries[i].estimatedSizeBytes += rowSize
		b.estimatedSizeBytes += rowSize
	}
	if col := b.entries[i].txIDCol; col >= 0 {
		if b.queued == nil {
			b.queued = make(map[int64]struct{})
		}
		id, _ := row[col].(int64)
		b.queued[id] = struct{}{}
	}
	if b.flushAtLimit != nil &&
		((b.maxRows > 0 && b.rowCount >= b.maxRows) ||
			(b.maxSizeBytes > 0 && b.estimatedSizeBytes >= b.maxSizeBytes)) {
		b.flushForLimit()
	}
}

func (b *rowBatch) setFlushLimit(
	maxBytes int64,
	maxRows int,
	flush func() error,
	onFlushed func(int64),
	nextLimit func() (int64, int),
) {
	b.maxSizeBytes = maxBytes
	b.maxRows = maxRows
	b.flushAtLimit = flush
	b.onLimitFlushed = onFlushed
	b.nextLimit = nextLimit
}

func (b *rowBatch) flushForLimit() {
	if b.flushErr != nil || b.flushAtLimit == nil || b.empty() {
		return
	}
	flushedBytes := b.estimatedSizeBytes
	if err := b.flushAtLimit(); err != nil {
		b.flushErr = err
		return
	}
	if b.onLimitFlushed != nil {
		b.onLimitFlushed(flushedBytes)
	}
	if b.nextLimit != nil {
		b.maxSizeBytes, b.maxRows = b.nextLimit()
	}
}

func (b *rowBatch) entryIndex(shape rowShape) int {
	for i := range b.entries {
		if b.entries[i].shape.table == shape.table &&
			strings.Join(b.entries[i].shape.columns, ",") ==
				strings.Join(shape.columns, ",") {
			return i
		}
	}
	entry := shapeRows{shape: shape, txIDCol: -1}
	for col, column := range shape.columns {
		if column == "transaction_id" {
			entry.txIDCol = col
		}
	}
	b.entries = append(b.entries, entry)
	return len(b.entries) - 1
}

// dropTransaction discards rows queued for transactionID.
func (b *rowBatch) dropTransaction(transactionID int64) {
	if _, ok := b.queued[transactionID]; !ok {
		return
	}
	delete(b.queued, transactionID)
	for i := range b.entries {
		entry := &b.entries[i]
		if entry.txIDCol < 0 {
			continue
		}
		kept := entry.rows[:0]
		for _, row := range entry.rows {
			if id, _ := row[entry.txIDCol].(int64); id != transactionID {
				kept = append(kept, row)
			} else if b.trackSize {
				rowSize := estimateRowSizeBytes(row)
				entry.estimatedSizeBytes -= rowSize
				b.estimatedSizeBytes -= rowSize
			}
		}
		b.rowCount -= len(entry.rows) - len(kept)
		clear(entry.rows[len(kept):])
		entry.rows = kept
	}
}

func estimateRowSizeBytes(row []any) int64 {
	size := rowBatchRowOverheadBytes +
		int64(len(row))*rowBatchValueOverheadBytes
	for _, value := range row {
		switch typed := value.(type) {
		case []byte:
			size += int64(cap(typed))
		case string:
			size += int64(len(typed))
		}
	}
	return size
}

// merge moves every queued row of other into b.
func (b *rowBatch) merge(other *rowBatch) {
	for _, entry := range other.entries {
		for _, row := range entry.rows {
			b.add(entry.shape, row...)
		}
	}
}

func (b *rowBatch) reset() {
	b.clearRows()
	b.maxSizeBytes = 0
	b.maxRows = 0
	b.flushAtLimit = nil
	b.onLimitFlushed = nil
	b.nextLimit = nil
	b.flushErr = nil
}

func (b *rowBatch) clearRows() {
	b.entries = nil
	b.estimatedSizeBytes = 0
	b.rowCount = 0
	b.queued = nil
}

func (b *rowBatch) enableSizeTracking() {
	b.trackSize = true
}

func (b *rowBatch) EstimatedSizeBytes() int64 {
	return b.estimatedSizeBytes
}

// flush writes the queued rows with multi-row INSERTs bounded by
// parameterLimit bind parameters per statement, then clears the queue.
func (b *rowBatch) flush(
	ctx context.Context,
	db queryer,
	parameterLimit int,
) error {
	if b.empty() {
		return nil
	}
	defer b.clearRows()
	for _, entry := range b.entries {
		width := len(entry.shape.columns)
		perStatement := max(1, parameterLimit/width)
		placeholder := "(" + strings.TrimSuffix(
			strings.Repeat("?, ", width), ", ",
		) + ")"
		for start := 0; start < len(entry.rows); start += perStatement {
			end := min(start+perStatement, len(entry.rows))
			args := make([]any, 0, (end-start)*width)
			for _, row := range entry.rows[start:end] {
				args = append(args, row...)
			}
			query := "-- name: " + entry.shape.name + " :exec\n" +
				"INSERT INTO " + entry.shape.table + " (" +
				strings.Join(entry.shape.columns, ", ") + ") VALUES " +
				strings.TrimSuffix(
					strings.Repeat(placeholder+", ", end-start), ", ",
				)
			if entry.shape.suffix != "" {
				query += "\n" + entry.shape.suffix
			}
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf(
					"insert %s rows: %w", entry.shape.table, err,
				)
			}
		}
	}
	return nil
}

// clone retains the queue at a transaction or savepoint boundary. Mutation
// replaces row slices but never changes individual SQL argument values.
func (b *rowBatch) clone() rowBatch {
	ret := rowBatch{
		queued:             maps.Clone(b.queued),
		entries:            slices.Clone(b.entries),
		estimatedSizeBytes: b.estimatedSizeBytes,
		trackSize:          b.trackSize,
	}
	for i := range ret.entries {
		ret.entries[i].rows = slices.Clone(ret.entries[i].rows)
	}
	return ret
}

// flushExceptClosureAPIRows keeps only API lookup/history rows queued. Assets,
// datums and script-related rows remain visible before the next tx validates.
func (b *rowBatch) flushExceptClosureAPIRows(ctx context.Context, db queryer, parameterLimit int) error {
	var immediate, deferred rowBatch
	immediate.trackSize = b.trackSize
	deferred.trackSize = b.trackSize
	for _, entry := range b.entries {
		target := &immediate
		switch entry.shape.table {
		case "key_witness", "address_transaction", "transaction_metadata_label":
			target = &deferred
		}
		for _, row := range entry.rows {
			target.add(entry.shape, row...)
		}
	}
	if err := immediate.flush(ctx, db, parameterLimit); err != nil {
		return err
	}
	*b = deferred
	return nil
}
