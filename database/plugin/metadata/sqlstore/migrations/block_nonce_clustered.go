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
	"math"
	"strconv"
)

const blockNonceClusteredBatchSize = 50_000

// blockNonceClusteredBackfill copies the legacy block_nonce table into its
// clustered copy one slot range at a time and swaps the copy in under the
// original name with the last batch. The cursor is the last slot covered.
//
// A block's ordinal is its place among the rows of its slot in legacy id
// order, which is the order they were first written. A row with no slot or no
// hash cannot be keyed and is dropped; the reconciliation in the last batch
// accounts for every legacy row as copied or dropped before the legacy table
// is replaced.
func blockNonceClusteredBackfill(dialect string) Backfill {
	const copyRows = `
INSERT INTO block_nonce_clustered (slot, hash, ordinal, nonce, is_checkpoint)
SELECT b.slot, b.hash,
       (SELECT COUNT(*) FROM block_nonce AS o
        WHERE o.slot = b.slot AND o.id < b.id),
       b.nonce, b.is_checkpoint
FROM block_nonce AS b
WHERE b.slot > ? AND b.slot <= ? AND b.hash IS NOT NULL
ORDER BY b.slot, b.hash`
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		// Slot 0 is a real slot, so the range starts below every slot.
		lastSlot := int64(math.MinInt64)
		if batch.Cursor != "" {
			parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
			if err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse block nonce cursor: %w", err,
				)
			}
			lastSlot = parsed
		}
		hasLegacy, err := columnExists(
			ctx, batch, dialect, "block_nonce", "id",
		)
		if err != nil {
			return BatchResult{}, err
		}
		if !hasLegacy {
			// The swap already ran, or stopped part way through.
			if err := completeSwap(
				ctx, batch, dialect,
				"block_nonce", "block_nonce_clustered",
			); err != nil {
				return BatchResult{}, err
			}
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		var upper sql.NullInt64
		if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT MAX(slot) FROM (
    SELECT slot FROM block_nonce
    WHERE slot IS NOT NULL AND slot > ? ORDER BY slot LIMIT ?
) AS page`), lastSlot, batch.Limit).Scan(&upper); err != nil {
			return BatchResult{}, fmt.Errorf("read block nonce range: %w", err)
		}
		if upper.Valid {
			result, err := batch.Tx.ExecContext(
				ctx, batch.Rebind(copyRows), lastSlot, upper.Int64,
			)
			if err != nil {
				return BatchResult{}, fmt.Errorf("copy block nonces: %w", err)
			}
			rows, err := result.RowsAffected()
			if err != nil {
				return BatchResult{}, err
			}
			return BatchResult{
				Cursor: strconv.FormatInt(upper.Int64, 10), Rows: rows,
			}, nil
		}
		if err := swapBlockNonce(ctx, batch, dialect); err != nil {
			return BatchResult{}, err
		}
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
}

func swapBlockNonce(ctx context.Context, batch Batch, dialect string) error {
	return swapRebuiltTable(
		ctx, batch, dialect, "block_nonce", "block_nonce_clustered",
		func() error { return verifyBlockNonce(ctx, batch) },
	)
}

func verifyBlockNonce(ctx context.Context, batch Batch) error {
	var legacy, unkeyable, copied int64
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM block_nonce",
	).Scan(&legacy); err != nil {
		return fmt.Errorf("count legacy block nonces: %w", err)
	}
	if err := batch.Tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM block_nonce WHERE slot IS NULL OR hash IS NULL`,
	).Scan(&unkeyable); err != nil {
		return fmt.Errorf("count unkeyable block nonces: %w", err)
	}
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM block_nonce_clustered",
	).Scan(&copied); err != nil {
		return fmt.Errorf("count copied block nonces: %w", err)
	}
	if legacy != copied+unkeyable {
		return fmt.Errorf(
			"block nonce rebuild is incomplete: legacy rows %d, "+
				"without slot or hash %d, rows in copy %d",
			legacy, unkeyable, copied,
		)
	}
	return nil
}
