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
	"encoding/json"
	"errors"
	"fmt"
)

const utxoEdgeClusteredBatchSize = 50_000

// utxoEdgeTables are the edge tables rebuilt as clustered tables, in copy
// order. The clustered copy of each is swapped in under the original name.
var utxoEdgeTables = []string{
	"utxo_reference_edge",
	"utxo_collateral_edge",
}

// utxoEdgeClusteredCursor is the durable progress of the edge copy. Table is
// the index into utxoEdgeTables being copied; Tx and Utxo are the last copied
// key in (tx_id, utxo_id) order.
type utxoEdgeClusteredCursor struct {
	Table  int   `json:"table"`
	Tx     int64 `json:"tx"`
	Utxo   int64 `json:"utxo"`
	Copied int64 `json:"copied"`
}

// utxoEdgeClusteredBackfill copies each legacy edge table into its clustered
// copy in (tx_id, utxo_id) order, so the copy appends to the new primary key,
// and swaps the copy in under the original name when the last row of a table
// is copied. The swap and the row-count reconciliation share the transaction
// of the final batch, so a table is replaced only when every row is present.
func utxoEdgeClusteredBackfill(dialect string) Backfill {
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		var cursor utxoEdgeClusteredCursor
		if batch.Cursor != "" {
			if err := json.Unmarshal(
				[]byte(batch.Cursor), &cursor,
			); err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse edge copy cursor: %w", err,
				)
			}
		}
		if cursor.Table >= len(utxoEdgeTables) {
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		table := utxoEdgeTables[cursor.Table]
		clustered := table + "_clustered"
		hasClustered, err := tableExists(ctx, batch, dialect, clustered)
		if err != nil {
			return BatchResult{}, err
		}
		if !hasClustered {
			// The swap already ran for this table.
			return advanceEdgeTable(cursor, 0, len(utxoEdgeTables))
		}
		hasLegacy, err := tableExists(ctx, batch, dialect, table)
		if err != nil {
			return BatchResult{}, err
		}
		var rows int64
		if hasLegacy {
			rows, err = copyEdgeWindow(ctx, batch, &cursor, table, clustered)
			if err != nil {
				return BatchResult{}, err
			}
			if rows == int64(batch.Limit) {
				return encodeEdgeCursor(cursor, rows, false)
			}
			if err := reconcileEdgeCopy(
				ctx, batch, table, clustered,
			); err != nil {
				return BatchResult{}, err
			}
			if _, err := batch.Tx.ExecContext(
				ctx, "DROP TABLE "+table,
			); err != nil {
				return BatchResult{}, fmt.Errorf("drop legacy %s: %w", table, err)
			}
		}
		if _, err := batch.Tx.ExecContext(
			ctx, "ALTER TABLE "+clustered+" RENAME TO "+table,
		); err != nil {
			return BatchResult{}, fmt.Errorf("swap in %s: %w", table, err)
		}
		return advanceEdgeTable(cursor, rows, len(utxoEdgeTables))
	}
}

func advanceEdgeTable(
	cursor utxoEdgeClusteredCursor,
	rows int64,
	tables int,
) (BatchResult, error) {
	cursor.Table++
	cursor.Tx, cursor.Utxo = 0, 0
	return encodeEdgeCursor(cursor, rows, cursor.Table >= tables)
}

func encodeEdgeCursor(
	cursor utxoEdgeClusteredCursor,
	rows int64,
	done bool,
) (BatchResult, error) {
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return BatchResult{}, err
	}
	return BatchResult{Cursor: string(encoded), Rows: rows, Done: done}, nil
}

// copyEdgeWindow copies the next Limit edges after the cursor and advances it.
// It returns how many were copied.
func copyEdgeWindow(
	ctx context.Context,
	batch Batch,
	cursor *utxoEdgeClusteredCursor,
	table, clustered string,
) (int64, error) {
	// Row-value comparisons let the planner seek the tx_id index to the full
	// cursor instead of rescanning every edge of one transaction per batch.
	var upperTx, upperUtxo sql.NullInt64
	err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT tx_id, utxo_id FROM `+table+`
WHERE (tx_id, utxo_id) > (?, ?)
ORDER BY tx_id, utxo_id
LIMIT 1 OFFSET ?`), cursor.Tx, cursor.Utxo, batch.Limit-1,
	).Scan(&upperTx, &upperUtxo)
	bounded := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("select %s window: %w", table, err)
	}
	query := `
INSERT INTO ` + clustered + ` (tx_id, utxo_id)
SELECT tx_id, utxo_id FROM ` + table + `
WHERE (tx_id, utxo_id) > (?, ?)`
	args := []any{cursor.Tx, cursor.Utxo}
	if bounded {
		query += ` AND (tx_id, utxo_id) <= (?, ?)`
		args = append(args, upperTx.Int64, upperUtxo.Int64)
	}
	query += ` ORDER BY tx_id, utxo_id`
	result, err := batch.Tx.ExecContext(ctx, batch.Rebind(query), args...)
	if err != nil {
		return 0, fmt.Errorf("copy %s edges: %w", table, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("count copied %s edges: %w", table, err)
	}
	if bounded {
		cursor.Tx, cursor.Utxo = upperTx.Int64, upperUtxo.Int64
	}
	cursor.Copied += rows
	return rows, nil
}

// reconcileEdgeCopy refuses to drop the legacy table unless the clustered copy
// holds exactly its rows.
func reconcileEdgeCopy(
	ctx context.Context,
	batch Batch,
	table, clustered string,
) error {
	var legacy, copied int64
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM "+table,
	).Scan(&legacy); err != nil {
		return fmt.Errorf("count %s edges: %w", table, err)
	}
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM "+clustered,
	).Scan(&copied); err != nil {
		return fmt.Errorf("count %s edges: %w", clustered, err)
	}
	if legacy != copied {
		return fmt.Errorf(
			"%s copy is incomplete: %d legacy edges, %d copied",
			table, legacy, copied,
		)
	}
	return nil
}

// tableExists reports whether a table of the given name exists in the
// migration's schema.
func tableExists(
	ctx context.Context,
	batch Batch,
	dialect, name string,
) (bool, error) {
	var query string
	switch dialect {
	case "postgres":
		query = `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = ?`
	case "mysql":
		query = `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = ?`
	default:
		query = `SELECT COUNT(*) FROM sqlite_master
WHERE type = 'table' AND name = ?`
	}
	var count int64
	if err := batch.Tx.QueryRowContext(
		ctx, batch.Rebind(query), name,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("look up table %s: %w", name, err)
	}
	return count > 0, nil
}
