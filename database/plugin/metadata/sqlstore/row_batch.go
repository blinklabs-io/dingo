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

// rowShape is one INSERT form. key_witness has two shapes (vkey and
// bootstrap) with different column lists, so rows are grouped by shape rather
// than by table. suffix is appended after the VALUES list, for an ON CONFLICT
// clause.
type rowShape struct {
	table   string
	columns []string
	suffix  string
}

var (
	vkeyWitnessShape = rowShape{
		table:   "key_witness",
		columns: []string{"vkey", "signature", "transaction_id", "type"},
	}
	bootstrapWitnessShape = rowShape{
		table: "key_witness",
		columns: []string{
			"signature", "public_key", "chain_code", "attributes",
			"transaction_id", "type",
		},
	}
	witnessScriptShape = rowShape{
		table:   "witness_scripts",
		columns: []string{"script_hash", "transaction_id", "type"},
	}
	plutusDataShape = rowShape{
		table:   "plutus_data",
		columns: []string{"data", "transaction_id"},
	}
	redeemerShape = rowShape{
		table: "redeemer",
		columns: []string{
			"data", "transaction_id", "ex_units_memory", "ex_units_cpu",
			`"index"`, "tag",
		},
	}
	addressTransactionShape = rowShape{
		table: "address_transaction",
		columns: []string{
			"payment_key", "staking_key", "credential_tag",
			"transaction_id", "slot", "tx_index",
		},
	}
	metadataLabelShape = rowShape{
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
		table:   "datum",
		columns: []string{"hash", "raw_datum", "added_slot"},
		suffix:  "ON CONFLICT (hash) DO NOTHING",
	}
	assetShape = rowShape{
		table:   "asset",
		columns: []string{"name", "policy_id", "fingerprint", "utxo_id", "amount"},
		suffix:  "ON CONFLICT (name, policy_id, utxo_id) DO NOTHING",
	}
	assetMintBurnShape = rowShape{
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
	shape   rowShape
	txIDCol int
	rows    [][]any
}

// rowBatch queues rows for multi-row insertion. Shapes are kept in
// first-queued order so a flush is deterministic.
type rowBatch struct {
	entries []shapeRows
	// queued records which transactions have rows pending, so replacing a
	// transaction's rows only scans when it actually has some.
	queued map[int64]struct{}
}

func (b *rowBatch) empty() bool {
	return b == nil || len(b.entries) == 0
}

func (b *rowBatch) add(shape rowShape, row ...any) {
	i := b.entryIndex(shape)
	b.entries[i].rows = append(b.entries[i].rows, row)
	if col := b.entries[i].txIDCol; col >= 0 {
		if b.queued == nil {
			b.queued = make(map[int64]struct{})
		}
		id, _ := row[col].(int64)
		b.queued[id] = struct{}{}
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
			}
		}
		entry.rows = kept
	}
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
	b.entries = nil
	b.queued = nil
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
	ret := rowBatch{queued: maps.Clone(b.queued), entries: slices.Clone(b.entries)}
	for i := range ret.entries {
		ret.entries[i].rows = slices.Clone(ret.entries[i].rows)
	}
	return ret
}
