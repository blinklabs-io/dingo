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
	"encoding/json"
	"fmt"
)

const poolOpCertClusteredBatchSize = 50_000

// poolOpCertClusteredCursor is the durable progress of the rebuild. LastID is
// the highest legacy id handled; the counters let the last batch account for
// every legacy row as copied or dropped.
type poolOpCertClusteredCursor struct {
	LastID    int64 `json:"last_id"`
	Scanned   int64 `json:"scanned"`
	Copied    int64 `json:"copied"`
	Unkeyable int64 `json:"unkeyable"`
}

// poolOpCertClusteredBackfill copies the legacy pool_opcert_sequence table
// into its clustered copy one legacy id range at a time. Each range first adds
// its pool key hashes to the credential dictionary, then copies its rows with
// the pool's credential id, and the last batch swaps the copy in under the
// original name. Dictionary rows are never removed.
//
// A row with no pool key hash or no slot cannot be keyed and is dropped; the
// reconciliation in the last batch accounts for every legacy row as copied or
// dropped, in the same transaction as the swap.
func poolOpCertClusteredBackfill(dialect string) Backfill {
	b := credentialBackfill{dialect: dialect}
	const copyRows = `
INSERT INTO pool_opcert_sequence_clustered (pool_key_id, slot, sequence)
SELECT c.id, o.slot, o.sequence
FROM pool_opcert_sequence AS o
JOIN credential AS c ON c.hash = o.pool_key_hash
WHERE o.id > ? AND o.id <= ? AND o.slot IS NOT NULL
ORDER BY c.id, o.slot`
	return func(ctx context.Context, batch Batch) (BatchResult, error) {
		var cursor poolOpCertClusteredCursor
		if batch.Cursor != "" {
			if err := json.Unmarshal(
				[]byte(batch.Cursor), &cursor,
			); err != nil {
				return BatchResult{}, fmt.Errorf(
					"parse pool op-cert cursor: %w", err,
				)
			}
		}
		hasLegacy, err := columnExists(
			ctx, batch, dialect, "pool_opcert_sequence", "pool_key_hash",
		)
		if err != nil {
			return BatchResult{}, err
		}
		if !hasLegacy {
			// The swap already ran.
			return BatchResult{Cursor: batch.Cursor, Done: true}, nil
		}
		count, high, err := idRange(
			ctx, batch, "pool_opcert_sequence", cursor.LastID,
		)
		if err != nil {
			return BatchResult{}, err
		}
		if count > 0 {
			if err := b.addCredentials(
				ctx, batch, "pool_opcert_sequence", "pool_key_hash",
				cursor.LastID, high,
			); err != nil {
				return BatchResult{}, err
			}
			result, err := batch.Tx.ExecContext(
				ctx, batch.Rebind(copyRows), cursor.LastID, high,
			)
			if err != nil {
				return BatchResult{}, fmt.Errorf("copy pool op-certs: %w", err)
			}
			copied, err := result.RowsAffected()
			if err != nil {
				return BatchResult{}, err
			}
			cursor.LastID = high
			cursor.Scanned += count
			cursor.Copied += copied
			cursor.Unkeyable += count - copied
		}
		done := count < int64(batch.Limit)
		if done {
			if err := swapPoolOpCert(ctx, batch, cursor); err != nil {
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

func swapPoolOpCert(
	ctx context.Context,
	batch Batch,
	cursor poolOpCertClusteredCursor,
) error {
	var legacy, copied int64
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM pool_opcert_sequence",
	).Scan(&legacy); err != nil {
		return fmt.Errorf("count legacy pool op-certs: %w", err)
	}
	if err := batch.Tx.QueryRowContext(
		ctx, "SELECT COUNT(*) FROM pool_opcert_sequence_clustered",
	).Scan(&copied); err != nil {
		return fmt.Errorf("count copied pool op-certs: %w", err)
	}
	if cursor.Scanned != legacy || cursor.Copied != copied ||
		cursor.Copied+cursor.Unkeyable != cursor.Scanned {
		return fmt.Errorf(
			"pool op-cert rebuild is incomplete: legacy rows %d, scanned %d, "+
				"copied %d, without pool or slot %d, rows in copy %d",
			legacy, cursor.Scanned, cursor.Copied, cursor.Unkeyable, copied,
		)
	}
	for _, statement := range []string{
		"DROP TABLE pool_opcert_sequence",
		"ALTER TABLE pool_opcert_sequence_clustered RENAME TO pool_opcert_sequence",
	} {
		if _, err := batch.Tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("swap in pool_opcert_sequence: %w", err)
		}
	}
	return nil
}
