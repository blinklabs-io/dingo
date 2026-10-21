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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/leioskeyepoch"
)

// rebindQuerier lets the shared replay run its ? statements inside a backfill
// transaction on a dialect that numbers its placeholders.
type rebindQuerier struct {
	tx     *sql.Tx
	rebind func(string) string
}

func (q rebindQuerier) QueryContext(
	ctx context.Context,
	query string,
	args ...any,
) (*sql.Rows, error) {
	return q.tx.QueryContext(ctx, q.rebind(query), args...)
}

func (q rebindQuerier) QueryRowContext(
	ctx context.Context,
	query string,
	args ...any,
) *sql.Row {
	return q.tx.QueryRowContext(ctx, q.rebind(query), args...)
}

type leiosKeyBackfillRow struct {
	id, poolID int64
	// The slots and indexes scan straight into the unsigned domain, which
	// rejects a negative stored value.
	addedSlot, blockIndex, certIndex, executionSlot uint64
	public, proof                                   []byte
}

// leiosKeyRegistrationEpochBackfill recomputes leios_key_registration_epoch
// for the live registrations that carry a BLS key and no epoch, with the same
// replay the write path uses, so a database written by an earlier release
// agrees with one synced from scratch.
//
// Only rows with a key are read, so a network that never carried one matches
// nothing and the first batch finishes the migration. Each row's result
// depends on the pool's earlier key material and on snapshot rows, which this
// backfill never rewrites, so rows can be recomputed independently in id order.
func leiosKeyRegistrationEpochBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	lastID := int64(0)
	if batch.Cursor != "" {
		parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"parse Leios key epoch backfill cursor: %w",
				err,
			)
		}
		lastID = parsed
	}
	pending, err := leiosKeyBackfillRows(ctx, batch, lastID)
	if err != nil {
		return BatchResult{}, err
	}
	if len(pending) == 0 {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	db := rebindQuerier{tx: batch.Tx, rebind: batch.Rebind}
	for _, row := range pending {
		epoch, ageUnknown, err := leioskeyepoch.Registration(
			ctx, db, row.poolID,
			leioskeyepoch.Position{
				Slot:       row.addedSlot,
				BlockIndex: row.blockIndex,
				CertIndex:  row.certIndex,
			},
			row.executionSlot, row.public, row.proof,
		)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"pool registration %d: %w", row.id, err,
			)
		}
		if epoch == nil && !ageUnknown {
			// The epoch table cannot place the registration, which is how
			// the write path left it too.
			continue
		}
		var value sql.NullInt64
		if epoch != nil {
			if *epoch > uint64(math.MaxInt64) {
				return BatchResult{}, fmt.Errorf(
					"pool registration %d: epoch %d exceeds int64",
					row.id, *epoch,
				)
			}
			value = sql.NullInt64{Int64: int64(*epoch), Valid: true}
		}
		if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
UPDATE pool_registration
SET leios_key_registration_epoch = ?,
    leios_key_registration_age_unknown = ?
WHERE id = ?`), value, ageUnknown, row.id); err != nil {
			return BatchResult{}, fmt.Errorf(
				"pool registration %d: %w", row.id, err,
			)
		}
	}
	return BatchResult{
		Cursor: strconv.FormatInt(pending[len(pending)-1].id, 10),
		Rows:   int64(len(pending)),
	}, nil
}

func leiosKeyBackfillRows(
	ctx context.Context,
	batch Batch,
	lastID int64,
) ([]leiosKeyBackfillRow, error) {
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(`
SELECT pr.id, pr.pool_id, pr.added_slot,
       COALESCE(t.block_index, 0), COALESCE(c.cert_index, 0),
       COALESCE((
           SELECT lc.slot FROM leios_transaction_context lc
           WHERE lc.transaction_id = c.transaction_id), pr.added_slot),
       pr.leios_key_public, pr.leios_key_possession_proof
FROM pool_registration pr
LEFT JOIN certs c ON c.id = pr.certificate_id
LEFT JOIN "transaction" t ON t.id = c.transaction_id
WHERE pr.id > ?
  AND pr.leios_key_public IS NOT NULL
  AND pr.leios_key_possession_proof IS NOT NULL
  AND pr.certificate_id IS NOT NULL
  AND pr.certificate_id <> 0
  AND pr.leios_key_registration_epoch IS NULL
  AND pr.leios_key_registration_age_unknown = FALSE
ORDER BY pr.id
LIMIT ?`), lastID, batch.Limit)
	if err != nil {
		return nil, fmt.Errorf("read keyed pool registrations: %w", err)
	}
	defer rows.Close()
	var out []leiosKeyBackfillRow
	for rows.Next() {
		var row leiosKeyBackfillRow
		if err := rows.Scan(
			&row.id, &row.poolID, &row.addedSlot, &row.blockIndex,
			&row.certIndex, &row.executionSlot, &row.public, &row.proof,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
