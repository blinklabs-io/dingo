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
	"strings"
)

// witnessInsertShape is one INSERT form for a witness table. key_witness has
// two shapes (vkey and bootstrap) with different column lists, so rows are
// grouped by shape rather than by table.
type witnessInsertShape struct {
	table   string
	columns []string
}

var (
	vkeyWitnessShape = witnessInsertShape{
		table:   "key_witness",
		columns: []string{"vkey", "signature", "transaction_id", "type"},
	}
	bootstrapWitnessShape = witnessInsertShape{
		table: "key_witness",
		columns: []string{
			"signature", "public_key", "chain_code", "attributes",
			"transaction_id", "type",
		},
	}
	witnessScriptShape = witnessInsertShape{
		table:   "witness_scripts",
		columns: []string{"script_hash", "transaction_id", "type"},
	}
	plutusDataShape = witnessInsertShape{
		table:   "plutus_data",
		columns: []string{"data", "transaction_id"},
	}
	redeemerShape = witnessInsertShape{
		table: "redeemer",
		columns: []string{
			"data", "transaction_id", "ex_units_memory", "ex_units_cpu",
			`"index"`, "tag",
		},
	}
)

// witnessRows holds rows queued for one insert shape. The transaction_id
// column position is recorded so a transaction's queued rows can be dropped
// when it is applied again within the same window.
type witnessRows struct {
	shape witnessInsertShape
	// txIDCol indexes shape.columns' "transaction_id" entry.
	txIDCol int
	rows    [][]any
}

// witnessBatch queues witness rows for multi-row insertion. Shapes are kept
// in first-queued order so a flush is deterministic.
type witnessBatch struct {
	entries []witnessRows
	// index maps a shape key to its position in entries.
	index map[string]int
	// queued records which transactions have rows pending, so replacing a
	// transaction's rows only scans when it actually has some.
	queued map[int64]struct{}
}

func (b *witnessBatch) empty() bool {
	return b == nil || len(b.entries) == 0
}

func (b *witnessBatch) add(
	shape witnessInsertShape,
	transactionID int64,
	row ...any,
) {
	if b.index == nil {
		b.index = make(map[string]int)
		b.queued = make(map[int64]struct{})
	}
	key := shape.table + "(" + strings.Join(shape.columns, ",") + ")"
	i, ok := b.index[key]
	if !ok {
		entry := witnessRows{shape: shape}
		for col, column := range shape.columns {
			if column == "transaction_id" {
				entry.txIDCol = col
			}
		}
		i = len(b.entries)
		b.entries = append(b.entries, entry)
		b.index[key] = i
	}
	b.entries[i].rows = append(b.entries[i].rows, row)
	b.queued[transactionID] = struct{}{}
}

// dropTransaction discards rows queued for transactionID.
func (b *witnessBatch) dropTransaction(transactionID int64) {
	if b == nil {
		return
	}
	if _, ok := b.queued[transactionID]; !ok {
		return
	}
	delete(b.queued, transactionID)
	for i := range b.entries {
		entry := &b.entries[i]
		kept := make([][]any, 0, len(entry.rows))
		for _, row := range entry.rows {
			if id, _ := row[entry.txIDCol].(int64); id != transactionID {
				kept = append(kept, row)
			}
		}
		entry.rows = kept
	}
}

// merge moves every queued row of other into b.
func (b *witnessBatch) merge(other *witnessBatch) {
	for _, entry := range other.entries {
		for _, row := range entry.rows {
			id, _ := row[entry.txIDCol].(int64)
			b.add(entry.shape, id, row...)
		}
	}
}

func (b *witnessBatch) reset() {
	if b == nil {
		return
	}
	b.entries = nil
	b.index = nil
	b.queued = nil
}

// flush writes the queued rows with multi-row INSERTs bounded by
// parameterLimit bind parameters per statement, then clears the queue.
func (b *witnessBatch) flush(
	ctx context.Context,
	db queryer,
	parameterLimit int,
) error {
	if b.empty() {
		return nil
	}
	defer b.reset()
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
			query := "INSERT INTO " + entry.shape.table + " (" +
				strings.Join(entry.shape.columns, ", ") + ") VALUES " +
				strings.TrimSuffix(
					strings.Repeat(placeholder+", ", end-start), ", ",
				)
			if _, err := db.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf(
					"insert %s rows: %w", entry.shape.table, err,
				)
			}
		}
	}
	return nil
}
