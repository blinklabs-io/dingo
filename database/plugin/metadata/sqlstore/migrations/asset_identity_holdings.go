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

// assetIdentityHoldingsBatchSize is the number of legacy asset rows one
// backfill batch reads. Each batch commits with its cursor, so an interrupted
// run resumes at the last committed range.
const assetIdentityHoldingsBatchSize = 50_000

// assetIdentityHoldingsCursor is the durable progress of the asset split.
// last_id is the highest legacy asset id already handled. The counters let the
// final batch prove every legacy row was either copied, merged into an
// identical holding, or dropped because its UTxO no longer exists.
type assetIdentityHoldingsCursor struct {
	LastID    int64 `json:"last_id"`
	Scanned   int64 `json:"scanned"`
	Copied    int64 `json:"copied"`
	Orphans   int64 `json:"orphans"`
	Merged    int64 `json:"merged"`
	Coalesced int64 `json:"coalesced"`
}

func parseAssetIdentityHoldingsCursor(
	raw string,
) (assetIdentityHoldingsCursor, error) {
	var cursor assetIdentityHoldingsCursor
	if raw == "" {
		return cursor, nil
	}
	if err := json.Unmarshal([]byte(raw), &cursor); err != nil {
		return cursor, fmt.Errorf("parse asset split cursor: %w", err)
	}
	return cursor, nil
}

// assetIdentityHoldingsBackfill copies the legacy asset table into
// asset_identity and asset_holding one id range at a time.
//
// A legacy row with a NULL policy_id or name takes the empty value, which is
// what the node writes for an empty name; the number of such rows is carried
// in the cursor. A row whose UTxO is gone, or that has no UTxO, is dropped,
// as the foreign key would have dropped it. Rows that land on one
// (utxo, policy, name) merge only when their amounts are identical text; a
// difference fails the migration rather than choosing a value.
//
// The legacy table is dropped by the contract phase, so the last batch
// reconciles the cursor against the tables themselves before it reports done.
func assetIdentityHoldingsBackfill(dialect string) Backfill {
	insertIdentity := `
INSERT INTO asset_identity (policy_id, name)
SELECT DISTINCT COALESCE(a.policy_id, ?), COALESCE(a.name, ?)
FROM asset AS a
WHERE a.id > ? AND a.id <= ?
ON CONFLICT (policy_id, name) DO NOTHING`
	insertHolding := `
INSERT INTO asset_holding (utxo_id, asset_id, amount)
SELECT a.utxo_id, i.id, a.amount
FROM asset AS a
JOIN utxo AS u ON u.id = a.utxo_id
JOIN asset_identity AS i
  ON i.policy_id = COALESCE(a.policy_id, ?)
 AND i.name = COALESCE(a.name, ?)
WHERE a.id > ? AND a.id <= ?
ON CONFLICT (utxo_id, asset_id) DO NOTHING`
	if dialect == "mysql" {
		insertIdentity = `
INSERT IGNORE INTO asset_identity (policy_id, name)
SELECT DISTINCT COALESCE(a.policy_id, ?), COALESCE(a.name, ?)
FROM asset AS a
WHERE a.id > ? AND a.id <= ?`
		insertHolding = `
INSERT IGNORE INTO asset_holding (utxo_id, asset_id, amount)
SELECT a.utxo_id, i.id, a.amount
FROM asset AS a
JOIN utxo AS u ON u.id = a.utxo_id
JOIN asset_identity AS i
  ON i.policy_id = COALESCE(a.policy_id, ?)
 AND i.name = COALESCE(a.name, ?)
WHERE a.id > ? AND a.id <= ?`
	}
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		cursor, err := parseAssetIdentityHoldingsCursor(batch.Cursor)
		if err != nil {
			return BatchResult{}, err
		}
		exists, err := legacyAssetTableExists(ctx, batch, dialect)
		if err != nil {
			return BatchResult{}, err
		}
		if !exists {
			// Only the contract phase drops the table, and it runs after
			// this backfill reconciled; a replay of an earlier phase finds
			// the work finished.
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		empty := []byte{}
		var (
			count int64
			high  sql.NullInt64
		)
		if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*), MAX(id)
FROM (SELECT id FROM asset WHERE id > ? ORDER BY id LIMIT ?) AS b`),
			cursor.LastID, batch.Limit,
		).Scan(&count, &high); err != nil {
			return BatchResult{}, fmt.Errorf("read asset split range: %w", err)
		}
		if count > 0 {
			if err := copyAssetRange(
				ctx, batch, &cursor, high.Int64, empty,
				insertIdentity, insertHolding,
			); err != nil {
				return BatchResult{}, err
			}
		}
		done := count < int64(batch.Limit)
		if done {
			if err := reconcileAssetSplit(ctx, batch, cursor); err != nil {
				return BatchResult{}, err
			}
		}
		encoded, err := json.Marshal(cursor)
		if err != nil {
			return BatchResult{}, err
		}
		return BatchResult{
			Cursor: string(encoded),
			Rows:   count,
			Done:   done,
		}, nil
	}
}

func legacyAssetTableExists(
	ctx context.Context,
	batch Batch,
	dialect string,
) (bool, error) {
	var query string
	switch dialect {
	case "postgres":
		query = `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = current_schema() AND table_name = 'asset'`
	case "mysql":
		query = `SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = DATABASE() AND table_name = 'asset'`
	default:
		query = `SELECT COUNT(*) FROM sqlite_master
WHERE type = 'table' AND name = 'asset'`
	}
	var count int64
	if err := batch.Tx.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("look up legacy asset table: %w", err)
	}
	return count > 0, nil
}

func copyAssetRange(
	ctx context.Context,
	batch Batch,
	cursor *assetIdentityHoldingsCursor,
	high int64,
	empty []byte,
	insertIdentity, insertHolding string,
) error {
	low := cursor.LastID
	var scanned, coalesced, joined int64
	if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN a.policy_id IS NULL OR a.name IS NULL
                         THEN 1 ELSE 0 END), 0),
       COALESCE(SUM(CASE WHEN u.id IS NULL THEN 0 ELSE 1 END), 0)
FROM asset AS a
LEFT JOIN utxo AS u ON u.id = a.utxo_id
WHERE a.id > ? AND a.id <= ?`), low, high,
	).Scan(&scanned, &coalesced, &joined); err != nil {
		return fmt.Errorf("count asset split range: %w", err)
	}
	if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(insertIdentity),
		empty, empty, low, high,
	); err != nil {
		return fmt.Errorf("copy asset identities: %w", err)
	}
	result, err := batch.Tx.ExecContext(ctx, batch.Rebind(insertHolding),
		empty, empty, low, high,
	)
	if err != nil {
		return fmt.Errorf("copy asset holdings: %w", err)
	}
	copied, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count copied asset holdings: %w", err)
	}
	var differing int64
	if err := batch.Tx.QueryRowContext(ctx, batch.Rebind(`
SELECT COUNT(*)
FROM asset AS a
JOIN utxo AS u ON u.id = a.utxo_id
JOIN asset_identity AS i
  ON i.policy_id = COALESCE(a.policy_id, ?)
 AND i.name = COALESCE(a.name, ?)
JOIN asset_holding AS h ON h.utxo_id = a.utxo_id AND h.asset_id = i.id
WHERE a.id > ? AND a.id <= ?
  AND ((h.amount IS NULL AND a.amount IS NOT NULL)
    OR (h.amount IS NOT NULL AND a.amount IS NULL)
    OR h.amount <> a.amount)`), empty, empty, low, high,
	).Scan(&differing); err != nil {
		return fmt.Errorf("check merged asset amounts: %w", err)
	}
	if differing > 0 {
		return fmt.Errorf(
			"asset rows in id range (%d, %d] hold one asset twice in a UTxO "+
				"with different amounts (%d rows)",
			low, high, differing,
		)
	}
	cursor.LastID = high
	cursor.Scanned += scanned
	cursor.Copied += copied
	cursor.Orphans += scanned - joined
	cursor.Merged += joined - copied
	cursor.Coalesced += coalesced
	return nil
}

// reconcileAssetSplit refuses to finish unless the cursor accounts for every
// legacy row and the new holdings are exactly what was copied. A cursor moved
// past a range, or rows written out of band, fail here instead of being
// dropped with the legacy table.
func reconcileAssetSplit(
	ctx context.Context,
	batch Batch,
	cursor assetIdentityHoldingsCursor,
) error {
	var legacy, holdings int64
	if err := batch.Tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM asset",
	).Scan(&legacy); err != nil {
		return fmt.Errorf("count legacy asset rows: %w", err)
	}
	if err := batch.Tx.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM asset_holding",
	).Scan(&holdings); err != nil {
		return fmt.Errorf("count asset holdings: %w", err)
	}
	if cursor.Scanned != legacy ||
		cursor.Scanned != cursor.Copied+cursor.Orphans+cursor.Merged ||
		cursor.Copied != holdings {
		return fmt.Errorf(
			"asset split is incomplete: legacy rows %d, scanned %d, "+
				"copied %d, orphans %d, merged %d, holdings %d",
			legacy, cursor.Scanned, cursor.Copied, cursor.Orphans,
			cursor.Merged, holdings,
		)
	}
	return nil
}
