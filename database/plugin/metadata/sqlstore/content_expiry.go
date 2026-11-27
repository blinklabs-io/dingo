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
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
)

var _ metadata.PayloadExpiryPreparer = (*Store)(nil)

// PrepareBlockExpiry keeps the datum and script rows that name the block at
// (slot, hash) readable past its expiry. A datum or script row names one
// block that carried its bytes, while any number of canonical transactions
// may carry them, so a row is re-pointed to the first other carrier whose
// block still holds the bytes. A row no such carrier holds is left naming the
// block as an expired-payload marker, which a later carrier replaces on
// insert. The block's other payload columns name bytes no other block
// carries, and are left to read back as expired.
func (s *Store) PrepareBlockExpiry(
	ctx context.Context,
	blocks metadata.PayloadBlockSource,
	slot uint64,
	hash []byte,
) error {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	for _, c := range slotPayloadColumns {
		if !contentColumn(c) {
			continue
		}
		err := s.withWriteTransactionContext(
			ctx, nil, func(db queryer, ctx context.Context) error {
				rows, err := readPayloadRows(ctx, db, fmt.Sprintf(
					"SELECT %[2]s, %[3]s, %[4]s FROM %[1]s WHERE %[4]s = ?",
					c.table, c.key, c.column, c.slot,
				), sqlSlot)
				if err != nil {
					return err
				}
				for _, row := range rows {
					offset, err := types.DecodeCborOffset(row.value)
					if err != nil || !bytes.Equal(offset.BlockHash[:], hash) {
						// Already a marker, or bytes held by a live output.
						continue
					}
					payload, err := s.resolvePayload(row.value)
					if err != nil {
						return fmt.Errorf(
							"%s %d at slot %d: %w", c.table, row.key, slot, err,
						)
					}
					moved, err := s.relocateExpiredContent(
						ctx, db, blocks, c,
						payloadRow{key: row.key, value: payload, slot: slot},
					)
					if err != nil {
						return fmt.Errorf(
							"%s %d at slot %d: %w", c.table, row.key, slot, err,
						)
					}
					if moved {
						continue
					}
					if _, err := db.ExecContext(ctx, fmt.Sprintf(
						"UPDATE %s SET %s = ? WHERE %s = ?",
						c.table, c.column, c.key,
					), types.EncodeExpiredPayload(slot, hash), row.key); err != nil {
						return err
					}
				}
				return nil
			},
		)
		if err != nil {
			return fmt.Errorf("prepare %s rows for block expiry: %w", c.table, err)
		}
	}
	return nil
}

// rollbackContentRows settles the datum and script rows that name a block
// above the rollback point, which the rollback abandons; their carriers above
// it were deleted with their transactions. Such a row is first carried there,
// or was moved there because every earlier carrier's block had expired
// (PrepareBlockExpiry, expiredContentConflict). In the second case a surviving
// carrier still holds the bytes in an expired block, so the row is kept as an
// expired-payload marker naming the newest one. Otherwise no canonical
// transaction carries the row and it is deleted, so the same transaction
// applied in a replacement block writes it afresh instead of leaving it
// naming a body that is released later.
func (s *Store) rollbackContentRows(
	ctx context.Context,
	db queryer,
	slot uint64,
) error {
	sqlSlot, err := checkedInt64(slot)
	if err != nil {
		return err
	}
	for _, c := range slotPayloadColumns {
		if !contentColumn(c) {
			continue
		}
		rows, err := readPayloadRows(ctx, db, fmt.Sprintf(
			"SELECT %[2]s, %[3]s, %[4]s FROM %[1]s WHERE %[4]s > ?",
			c.table, c.key, c.column, c.slot,
		), sqlSlot)
		if err != nil {
			return err
		}
		for _, row := range rows {
			marker, err := s.survivingCarrierMarker(ctx, db, c, row)
			if err != nil {
				return err
			}
			if marker == nil {
				_, err = db.ExecContext(ctx, fmt.Sprintf(
					"DELETE FROM %s WHERE %s = ?", c.table, c.key,
				), row.key)
			} else {
				_, err = db.ExecContext(ctx, fmt.Sprintf(
					"UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
					c.table, c.column, c.slot, c.key,
				), marker.encoded, marker.slot, row.key)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type carrierMarker struct {
	encoded []byte
	slot    uint64
}

// survivingCarrierMarker returns the expired-payload marker of the newest
// canonical carrier that holds a content row's bytes, or nil when none does
// or the row's bytes can no longer be read to look for one.
func (s *Store) survivingCarrierMarker(
	ctx context.Context,
	db queryer,
	c slotPayloadColumn,
	row payloadRow,
) (*carrierMarker, error) {
	payload, err := s.resolvePayload(row.value)
	if err != nil || len(payload) == 0 {
		return nil, nil //nolint:nilerr // an unreadable row has no carrier to find
	}
	_, carriers, err := contentCarriers(
		ctx, db, c, payloadRow{key: row.key, value: payload},
	)
	if err != nil {
		return nil, err
	}
	for _, carrier := range slices.Backward(carriers) {
		if !carrier.required || carrier.slot >= row.slot {
			continue
		}
		carrierSlot, err := checkedInt64(carrier.slot)
		if err != nil {
			return nil, err
		}
		var hash []byte
		err = db.QueryRowContext(ctx,
			"SELECT block_hash FROM applied_transaction WHERE slot = ? LIMIT 1",
			carrierSlot,
		).Scan(&hash)
		if err != nil {
			return nil, fmt.Errorf("find the block at slot %d: %w", carrier.slot, err)
		}
		return &carrierMarker{
			encoded: types.EncodeExpiredPayload(carrier.slot, hash),
			slot:    carrier.slot,
		}, nil
	}
	return nil, nil
}
