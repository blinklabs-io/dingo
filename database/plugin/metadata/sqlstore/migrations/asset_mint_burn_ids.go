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

const assetMintBurnIDsBatchSize = 50_000

// assetMintBurnIDsCursor is the durable progress of the rebuild. LastID is the
// highest legacy id handled; the counters let the last batch prove every
// legacy row was copied or dropped for a stated reason.
type assetMintBurnIDsCursor struct {
	LastID  int64 `json:"last_id"`
	Scanned int64 `json:"scanned"`
	Copied  int64 `json:"copied"`
	Orphans int64 `json:"orphans"`
}

// assetMintBurnIDsBackfill copies the legacy asset_mint_burn table into its
// clustered copy one id range at a time, resolving the transaction and the
// asset identity of each row in SQL, and swaps the copy in under the original
// name with the last batch.
//
// A NULL policy or name takes the empty value, as in the asset split. A row
// naming no transaction row cannot be keyed and is dropped; the number of such
// rows is carried in the cursor. The swap and the reconciliation share the
// last batch's transaction, so the legacy table is replaced only when every
// row is accounted for.
func assetMintBurnIDsBackfill(dialect string) Backfill {
	insertIdentity := `
INSERT INTO asset_identity (policy_id, name)
SELECT DISTINCT COALESCE(m.policy_id, ?), COALESCE(m.name, ?)
FROM asset_mint_burn AS m
WHERE m.id > ? AND m.id <= ?
ON CONFLICT (policy_id, name) DO NOTHING`
	if dialect == "mysql" {
		insertIdentity = `
INSERT IGNORE INTO asset_identity (policy_id, name)
SELECT DISTINCT COALESCE(m.policy_id, ?), COALESCE(m.name, ?)
FROM asset_mint_burn AS m
WHERE m.id > ? AND m.id <= ?`
	}
	insertRows := `
INSERT INTO asset_mint_burn_clustered
  (transaction_id, asset_id, slot, tx_index, quantity)
SELECT t.id, i.id, COALESCE(m.slot, 0), COALESCE(m.tx_index, 0),
       COALESCE(m.quantity, '0')
FROM asset_mint_burn AS m
JOIN ` + quotedTransactionTable(dialect) + ` AS t ON t.hash = m.tx_hash
JOIN asset_identity AS i
  ON i.policy_id = COALESCE(m.policy_id, ?)
 AND i.name = COALESCE(m.name, ?)
WHERE m.id > ? AND m.id <= ?
ORDER BY t.id, i.id`
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		var cursor assetMintBurnIDsCursor
		if batch.Cursor != "" {
			if err := json.Unmarshal(
				[]byte(batch.Cursor), &cursor,
			); err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse mint burn cursor: %w", err,
				)
			}
		}
		hasLegacy, err := columnExists(
			ctx, batch, dialect, "asset_mint_burn", "tx_hash",
		)
		if err != nil {
			return BatchResult{}, err
		}
		if !hasLegacy {
			// The swap already ran, or stopped part way through.
			if err := completeSwap(
				ctx, batch, dialect,
				"asset_mint_burn", "asset_mint_burn_clustered",
			); err != nil {
				return BatchResult{}, err
			}
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		var (
			count int64
			high  sql.NullInt64
		)
		if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*), MAX(id)
FROM (SELECT id FROM asset_mint_burn WHERE id > ? ORDER BY id LIMIT ?) AS b`),
			cursor.LastID, batch.Limit,
		).Scan(&count, &high); err != nil {
			return BatchResult{}, fmt.Errorf("read mint burn range: %w", err)
		}
		if count > 0 {
			empty := []byte{}
			low := cursor.LastID
			if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(insertIdentity),
				empty, empty, low, high.Int64,
			); err != nil {
				return BatchResult{}, fmt.Errorf("copy mint burn assets: %w", err)
			}
			result, err := batch.Tx.ExecContext(ctx, batch.Rebind(insertRows),
				empty, empty, low, high.Int64,
			)
			if err != nil {
				return BatchResult{}, fmt.Errorf("copy mint burn rows: %w", err)
			}
			copied, err := result.RowsAffected()
			if err != nil {
				return BatchResult{}, err
			}
			cursor.LastID = high.Int64
			cursor.Scanned += count
			cursor.Copied += copied
			cursor.Orphans += count - copied
		}
		done := count < int64(batch.Limit)
		if done {
			if err := swapAssetMintBurn(ctx, batch, dialect, cursor); err != nil {
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

func quotedTransactionTable(dialect string) string {
	switch dialect {
	case "mysql":
		return "`transaction`"
	default:
		return `"transaction"`
	}
}

func swapAssetMintBurn(
	ctx context.Context,
	batch Batch,
	dialect string,
	cursor assetMintBurnIDsCursor,
) error {
	return swapRebuiltTable(
		ctx, batch, dialect, "asset_mint_burn", "asset_mint_burn_clustered",
		func() error { return verifyAssetMintBurn(ctx, batch, cursor) },
	)
}

func verifyAssetMintBurn(
	ctx context.Context,
	batch Batch,
	cursor assetMintBurnIDsCursor,
) error {
	var legacy, copied int64
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM asset_mint_burn",
	).Scan(&legacy); err != nil {
		return fmt.Errorf("count legacy mint burn rows: %w", err)
	}
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM asset_mint_burn_clustered",
	).Scan(&copied); err != nil {
		return fmt.Errorf("count copied mint burn rows: %w", err)
	}
	if cursor.Scanned != legacy || cursor.Copied != copied ||
		cursor.Scanned != cursor.Copied+cursor.Orphans {
		return fmt.Errorf(
			"mint burn rebuild is incomplete: legacy rows %d, scanned %d, "+
				"copied %d, orphans %d, rows in copy %d",
			legacy, cursor.Scanned, cursor.Copied, cursor.Orphans, copied,
		)
	}
	return nil
}

// columnExists reports whether a table has a column.
func columnExists(
	ctx context.Context,
	batch Batch,
	dialect, table, column string,
) (bool, error) {
	var query string
	switch dialect {
	case "postgres":
		query = `SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?`
	case "mysql":
		query = `SELECT COUNT(*) FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`
	default:
		query = `SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`
	}
	var count int64
	if err := batch.Tx.QueryRowContext(
		ctx, batch.Rebind(query), table, column,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("look up column %s.%s: %w", table, column, err)
	}
	return count > 0, nil
}
