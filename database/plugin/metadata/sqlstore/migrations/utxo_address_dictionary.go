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
	"fmt"
)

const utxoAddressDictionaryBatchSize = 50_000

// utxoAddressDictionaryCursor is the durable progress of the rebuild. LastID
// is the highest legacy utxo_id handled; the counters let the last batch prove
// every legacy row was copied.
type utxoAddressDictionaryCursor struct {
	LastID  int64 `json:"last_id"`
	Scanned int64 `json:"scanned"`
	Copied  int64 `json:"copied"`
}

// utxoAddressDictionaryBackfill copies the by-value utxo_address table into
// its dictionary-keyed copy one utxo_id range at a time. Each range first adds
// its addresses to the dictionary, then copies its rows with the address id
// and the stored page position, and the last batch swaps the copy in under the
// original name. Dictionary rows are never removed.
//
// The swap and the row-count reconciliation share the last batch's
// transaction, so the legacy table is replaced only when every row is copied.
func utxoAddressDictionaryBackfill(dialect string) Backfill {
	insertAddresses := `
INSERT INTO address (address)
SELECT DISTINCT u.address
FROM utxo_address AS u
WHERE u.utxo_id > ? AND u.utxo_id <= ?
ON CONFLICT (address) DO NOTHING`
	if dialect == "mysql" {
		insertAddresses = `
INSERT IGNORE INTO address (address)
SELECT DISTINCT u.address
FROM utxo_address AS u
WHERE u.utxo_id > ? AND u.utxo_id <= ?`
	}
	insertRows := `
INSERT INTO utxo_address_dictionary
  (utxo_id, address_id, tx_id, output_idx, slot, block_index)
SELECT u.utxo_id, a.id, u.tx_id, u.output_idx, u.slot, u.block_index
FROM utxo_address AS u
JOIN address AS a ON a.address = u.address
WHERE u.utxo_id > ? AND u.utxo_id <= ?
ORDER BY u.utxo_id`
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		var cursor utxoAddressDictionaryCursor
		if batch.Cursor != "" {
			if err := json.Unmarshal(
				[]byte(batch.Cursor), &cursor,
			); err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse UTxO address cursor: %w", err,
				)
			}
		}
		hasLegacy, err := columnExists(
			ctx, batch, dialect, "utxo_address", "address",
		)
		if err != nil {
			return BatchResult{}, err
		}
		if !hasLegacy {
			// The swap already ran.
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		var (
			count int64
			high  sql.NullInt64
		)
		if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*), MAX(utxo_id)
FROM (SELECT utxo_id FROM utxo_address WHERE utxo_id > ?
      ORDER BY utxo_id LIMIT ?) AS b`),
			cursor.LastID, batch.Limit,
		).Scan(&count, &high); err != nil {
			return BatchResult{}, fmt.Errorf("read UTxO address range: %w", err)
		}
		if count > 0 {
			if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(insertAddresses),
				cursor.LastID, high.Int64,
			); err != nil {
				return BatchResult{}, fmt.Errorf("copy addresses: %w", err)
			}
			result, err := batch.Tx.ExecContext(ctx, batch.Rebind(insertRows),
				cursor.LastID, high.Int64,
			)
			if err != nil {
				return BatchResult{}, fmt.Errorf("copy UTxO addresses: %w", err)
			}
			copied, err := result.RowsAffected()
			if err != nil {
				return BatchResult{}, err
			}
			cursor.LastID = high.Int64
			cursor.Scanned += count
			cursor.Copied += copied
		}
		done := count < int64(batch.Limit)
		if done {
			if err := swapUtxoAddress(ctx, batch, cursor); err != nil {
				return BatchResult{}, err
			}
		}
		encoded, err := json.Marshal(cursor)
		if err != nil {
			return BatchResult{}, err
		}
		return BatchResult{Cursor: string(encoded), Rows: count, Done: done}, nil
	}
}

func swapUtxoAddress(
	ctx context.Context,
	batch Batch,
	cursor utxoAddressDictionaryCursor,
) error {
	var legacy, copied int64
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM utxo_address",
	).Scan(&legacy); err != nil {
		return fmt.Errorf("count legacy UTxO addresses: %w", err)
	}
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM utxo_address_dictionary",
	).Scan(&copied); err != nil {
		return fmt.Errorf("count copied UTxO addresses: %w", err)
	}
	if cursor.Scanned != legacy || cursor.Copied != copied ||
		cursor.Scanned != cursor.Copied {
		return fmt.Errorf(
			"UTxO address rebuild is incomplete: legacy rows %d, scanned %d, "+
				"copied %d, rows in copy %d",
			legacy, cursor.Scanned, cursor.Copied, copied,
		)
	}
	for _, statement := range []string{
		"DROP TABLE utxo_address",
		"ALTER TABLE utxo_address_dictionary RENAME TO utxo_address",
	} {
		if _, err := batch.Tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("swap in utxo_address: %w", err)
		}
	}
	return nil
}
