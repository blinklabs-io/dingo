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
	"fmt"
	"strconv"
)

// appliedTransactionBlockIdentityBatchSize is the number of applied_transaction
// rows one backfill batch converts. Each batch commits with its cursor, so an
// interrupted run resumes at the last committed range.
const appliedTransactionBlockIdentityBatchSize = 50_000

// appliedTransactionBlockIdentityBackfill fills applied_transaction.block_id
// from block_hash, one tx_id range at a time. Block hashes enter the
// dictionary in order of their first transaction, so ids follow chain order
// for a store that applied blocks in order.
//
// A range is idempotent: rerunning it inserts no dictionary row twice and sets
// each block_id to the value it already holds. The contract phase drops
// block_hash, so the last batch refuses to finish while a row that has a hash
// has no id.
func appliedTransactionBlockIdentityBackfill(dialect string) Backfill {
	insertBlocks := `
INSERT INTO block_identity (hash)
SELECT block_hash FROM applied_transaction
WHERE tx_id > ? AND tx_id <= ? AND block_hash IS NOT NULL
GROUP BY block_hash
ORDER BY MIN(tx_id)
ON CONFLICT (hash) DO NOTHING`
	if dialect == "mysql" {
		insertBlocks = `
INSERT IGNORE INTO block_identity (hash)
SELECT block_hash FROM applied_transaction
WHERE tx_id > ? AND tx_id <= ? AND block_hash IS NOT NULL
GROUP BY block_hash
ORDER BY MIN(tx_id)`
	}
	const setBlockID = `
UPDATE applied_transaction
SET block_id = (
    SELECT b.id FROM block_identity AS b
    WHERE b.hash = applied_transaction.block_hash
)
WHERE tx_id > ? AND tx_id <= ? AND block_hash IS NOT NULL`
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		present, err := appliedTransactionBlockHashExists(ctx, batch, dialect)
		if err != nil {
			return BatchResult{}, err
		}
		if !present {
			// Only the contract phase drops the column, and it runs after
			// this backfill reconciled; a replay from an earlier phase finds
			// the work finished.
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		var last int64
		if batch.Cursor != "" {
			parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
			if err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse block identity cursor: %w", err,
				)
			}
			last = parsed
		}
		var (
			count int64
			high  sql.NullInt64
		)
		if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*), MAX(tx_id)
FROM (
    SELECT tx_id FROM applied_transaction WHERE tx_id > ? ORDER BY tx_id LIMIT ?
) AS b`), last, batch.Limit,
		).Scan(&count, &high); err != nil {
			return BatchResult{}, fmt.Errorf(
				"read applied transaction range: %w", err,
			)
		}
		if count > 0 {
			if _, err := batch.Tx.ExecContext(
				ctx, batch.Rebind(insertBlocks), last, high.Int64,
			); err != nil {
				return BatchResult{}, fmt.Errorf(
					"copy block hashes: %w", err,
				)
			}
			if _, err := batch.Tx.ExecContext(
				ctx, batch.Rebind(setBlockID), last, high.Int64,
			); err != nil {
				return BatchResult{}, fmt.Errorf("set block ids: %w", err)
			}
			last = high.Int64
		}
		done := count < int64(batch.Limit)
		if done {
			var unresolved int64
			if err := batch.Tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM applied_transaction
WHERE block_hash IS NOT NULL AND block_id IS NULL`,
			).Scan(&unresolved); err != nil {
				return BatchResult{}, fmt.Errorf(
					"count unresolved block ids: %w", err,
				)
			}
			if unresolved > 0 {
				return BatchResult{}, fmt.Errorf(
					"block identity backfill left %d applied transactions "+
						"without a block id", unresolved,
				)
			}
		}
		return BatchResult{
			Cursor: strconv.FormatInt(last, 10),
			Rows:   count,
			Done:   done,
		}, nil
	}
}

func appliedTransactionBlockHashExists(
	ctx context.Context,
	batch Batch,
	dialect string,
) (bool, error) {
	var query string
	switch dialect {
	case "postgres":
		query = `SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = current_schema()
  AND table_name = 'applied_transaction' AND column_name = 'block_hash'`
	case "mysql":
		query = `SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'applied_transaction' AND column_name = 'block_hash'`
	default:
		query = `SELECT COUNT(*) FROM pragma_table_info('applied_transaction')
WHERE name = 'block_hash'`
	}
	var count int64
	if err := batch.Tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("look up applied transaction block hash: %w", err)
	}
	return count > 0, nil
}
