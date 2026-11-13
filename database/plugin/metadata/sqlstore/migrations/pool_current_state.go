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
	"strings"
)

// poolCurrentStateBatchPools bounds the pools one backfill batch ranks, so the
// IN lists stay under SQLite's bind-variable limit.
const poolCurrentStateBatchPools = 200

// scanBatchRows runs a query inside the batch transaction and hands each row
// to fn, closing the rows before it returns.
func scanBatchRows(
	ctx context.Context,
	batch Batch,
	query string,
	args []any,
	fn func(*sql.Rows) error,
) error {
	rows, err := batch.Tx.QueryContext(ctx, batch.Rebind(query), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}

// poolCurrentStateHorizonEpochs is the horizon the backfill writes. It is a
// frozen copy: a migration may not follow later edits to the store's constant.
const poolCurrentStateHorizonEpochs = 8

const poolCurrentEffectiveSlot = `COALESCE((SELECT lc.slot FROM leios_transaction_context lc JOIN certs lc_cert ON lc_cert.transaction_id = lc.transaction_id WHERE lc_cert.id = %[1]s.certificate_id), %[1]s.added_slot)`

type poolCurrentEvent struct {
	kind                                                  int
	id, slot, added, synthetic, block, cert, index, epoch int64
}

// poolCurrentStateBackfill fills pool_current_state from the certificate
// history, a page of pools at a time in pool id order. The ranking and the
// horizon are a frozen copy of the ones the store applies on every
// certificate write: a migration may not follow later edits to them.
func poolCurrentStateBackfill(
	ctx context.Context,
	batch Batch,
) (BatchResult, error) {
	lastID := int64(0)
	if batch.Cursor != "" {
		parsed, err := strconv.ParseInt(batch.Cursor, 10, 64)
		if err != nil {
			return BatchResult{}, fmt.Errorf(
				"parse pool current state cursor: %w", err,
			)
		}
		lastID = parsed
	}
	limit := min(batch.Limit, poolCurrentStateBatchPools)
	if limit <= 0 {
		limit = poolCurrentStateBatchPools
	}
	var ids []int64
	var hashes [][]byte
	idByHash := map[string]int64{}
	if err := scanBatchRows(ctx, batch, `
SELECT id, pool_key_hash FROM pool
WHERE id > ? AND pool_key_hash IS NOT NULL
ORDER BY id
LIMIT ?`, []any{lastID, limit}, func(rows *sql.Rows) error {
		var id int64
		var hash []byte
		if err := rows.Scan(&id, &hash); err != nil {
			return err
		}
		ids = append(ids, id)
		hashes = append(hashes, hash)
		idByHash[string(hash)] = id
		return nil
	}); err != nil {
		return BatchResult{}, fmt.Errorf("read pools: %w", err)
	}
	if len(ids) == 0 {
		return BatchResult{Cursor: batch.Cursor, Done: true}, nil
	}
	var horizon int64
	if err := scanBatchRows(ctx, batch, `
SELECT start_slot FROM epoch
WHERE epoch_id <= (SELECT MAX(epoch_id) FROM epoch) - ?
ORDER BY epoch_id DESC LIMIT 1`,
		[]any{poolCurrentStateHorizonEpochs},
		func(rows *sql.Rows) error { return rows.Scan(&horizon) },
	); err != nil {
		return BatchResult{}, fmt.Errorf("read horizon: %w", err)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(hashes)), ",")
	args := make([]any, len(hashes))
	for i, hash := range hashes {
		args[i] = hash
	}
	events := map[int64][]poolCurrentEvent{}
	err := scanBatchRows(ctx, batch, `
SELECT pr.pool_key_hash, pr.id,
       `+fmt.Sprintf(poolCurrentEffectiveSlot, "pr")+` ev_slot,
       pr.added_slot ev_added,
       COALESCE(tx.block_index, 0) ev_block,
       COALESCE(certs.cert_index, 0) ev_cert,
       pr.registration_index ev_index
FROM pool_registration pr
LEFT JOIN certs ON certs.id = pr.certificate_id
LEFT JOIN applied_transaction tx ON tx.tx_id = certs.transaction_id
WHERE pr.pool_key_hash IN (`+placeholders+`)
ORDER BY pr.pool_key_hash, ev_slot DESC, ev_added DESC, ev_block DESC,
         ev_cert DESC, ev_index DESC, pr.id DESC`, args,
		func(rows *sql.Rows) error {
			var hash []byte
			event := poolCurrentEvent{kind: 0}
			if err := rows.Scan(
				&hash, &event.id, &event.slot, &event.added, &event.block,
				&event.cert, &event.index,
			); err != nil {
				return err
			}
			poolID := idByHash[string(hash)]
			events[poolID] = append(events[poolID], event)
			return nil
		})
	if err != nil {
		return BatchResult{}, fmt.Errorf("rank pool registrations: %w", err)
	}
	retirements := map[int64][]poolCurrentEvent{}
	err = scanBatchRows(ctx, batch, `
SELECT rt.pool_key_hash, rt.id, rt.epoch,
       `+fmt.Sprintf(poolCurrentEffectiveSlot, "rt")+` ev_slot,
       rt.added_slot ev_added,
       CASE WHEN rt.certificate_id = 0 THEN 1 ELSE 0 END ev_synthetic,
       COALESCE(tx.block_index, 0) ev_block,
       COALESCE(certs.cert_index, 0) ev_cert
FROM pool_retirement rt
LEFT JOIN certs ON certs.id = rt.certificate_id
LEFT JOIN applied_transaction tx ON tx.tx_id = certs.transaction_id
WHERE rt.pool_key_hash IN (`+placeholders+`)
ORDER BY rt.pool_key_hash, ev_slot DESC, ev_added DESC, ev_synthetic DESC,
         ev_block DESC, ev_cert DESC, rt.id DESC`, args,
		func(rows *sql.Rows) error {
			var hash []byte
			event := poolCurrentEvent{kind: 1}
			if err := rows.Scan(
				&hash, &event.id, &event.epoch, &event.slot, &event.added,
				&event.synthetic, &event.block, &event.cert,
			); err != nil {
				return err
			}
			poolID := idByHash[string(hash)]
			retirements[poolID] = append(retirements[poolID], event)
			return nil
		})
	if err != nil {
		return BatchResult{}, fmt.Errorf("rank pool retirements: %w", err)
	}
	var written int64
	for _, id := range ids {
		for _, table := range []string{
			"pool_current_event", "pool_current_state",
		} {
			if _, err := batch.Tx.ExecContext(
				ctx,
				batch.Rebind("DELETE FROM "+table+" WHERE pool_id = ?"),
				id,
			); err != nil {
				return BatchResult{}, err
			}
		}
		if len(events[id]) == 0 && len(retirements[id]) == 0 {
			continue
		}
		if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO pool_current_state (pool_id, horizon_slot) VALUES (?, ?)`),
			id, horizon,
		); err != nil {
			return BatchResult{}, fmt.Errorf(
				"write pool current state: %w", err,
			)
		}
		written++
		for _, list := range [][]poolCurrentEvent{events[id], retirements[id]} {
			for rank, event := range list {
				if _, err := batch.Tx.ExecContext(ctx, batch.Rebind(`
INSERT INTO pool_current_event (
    pool_id, kind, event_rank, event_id, effective_slot, added_slot,
    synthetic, block_index, cert_index, registration_index, epoch
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`),
					id, event.kind, rank, event.id, event.slot, event.added,
					event.synthetic, event.block, event.cert, event.index,
					event.epoch,
				); err != nil {
					return BatchResult{}, fmt.Errorf(
						"write pool current event: %w", err,
					)
				}
				written++
				if event.slot < horizon {
					break
				}
			}
		}
	}
	return BatchResult{
		Cursor: strconv.FormatInt(ids[len(ids)-1], 10),
		Rows:   written,
	}, nil
}
