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
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	fxcbor "github.com/fxamacker/cbor/v2"
)

var _ metadata.PayloadColumnMigrator = (*Store)(nil)

// payloadMigrationBatch is how many rows one write transaction of the payload
// migration converts.
const payloadMigrationBatch = 500

// slotPayloadColumn is a payload column whose row names the slot of the block
// that carried the payload.
type slotPayloadColumn struct {
	table, key, column, slot string
}

// contentPayloadColumn is a payload column whose row names no block: the
// payload is a datum, found by its hash in the datum table.
type contentPayloadColumn struct {
	table, key, column string
}

// slotPayloadColumns are converted first and in this order: datum rows are
// what the content columns find their offsets in.
var slotPayloadColumns = []slotPayloadColumn{
	{"datum", "id", "raw_datum", "added_slot"},
	{"script", "id", "content", "created_slot"},
	{"applied_transaction", "tx_id", "metadata", "slot"},
	{"transaction_metadata_label", "id", "cbor_value", "slot"},
	{"pparam_update", "id", "cbor", "added_slot"},
}

var contentPayloadColumns = []contentPayloadColumn{
	{"midnight_registrations", "id", "full_datum"},
	{"midnight_deregistrations", "id", "full_datum"},
	{"midnight_governance_datums", "id", "datum"},
	{"midnight_ariadne_params", "id", "datum"},
	{"midnight_ariadne_rollbacks", "id", "previous_datum"},
	{"midnight_candidate_removals", "id", "datum"},
}

// isPayloadOffset reports whether a column value is a stored offset.
func isPayloadOffset(value []byte) bool {
	_, err := types.DecodeCborOffset(value)
	return err == nil || types.IsEndorserOffset(value) ||
		types.IsExpiredPayload(value)
}

// MigratePayloadColumns implements metadata.PayloadColumnMigrator.
func (s *Store) MigratePayloadColumns(
	ctx context.Context,
	blocks metadata.PayloadBlockSource,
) error {
	if s.storageMode != types.StorageModeAPI {
		return nil
	}
	for _, column := range slotPayloadColumns {
		if err := s.migrateSlotPayloadColumn(ctx, blocks, column); err != nil {
			return fmt.Errorf(
				"migrate %s.%s to block offsets: %w",
				column.table, column.column, err,
			)
		}
	}
	for _, column := range contentPayloadColumns {
		if err := s.migrateContentPayloadColumn(ctx, column); err != nil {
			return fmt.Errorf(
				"migrate %s.%s to block offsets: %w",
				column.table, column.column, err,
			)
		}
	}
	if err := s.migrateEpochCandidates(ctx); err != nil {
		return fmt.Errorf(
			"migrate midnight_epoch_candidates.candidates_cbor to block offsets: %w",
			err,
		)
	}
	return nil
}

type payloadRow struct {
	key   int64
	value []byte
	slot  uint64
}

func (s *Store) migrateSlotPayloadColumn(
	ctx context.Context,
	blocks metadata.PayloadBlockSource,
	c slotPayloadColumn,
) error {
	query := fmt.Sprintf(
		"SELECT %[2]s, %[3]s, %[4]s FROM %[1]s WHERE %[2]s > ? ORDER BY %[2]s LIMIT %[5]d",
		c.table, c.key, c.column, c.slot, payloadMigrationBatch,
	)
	update := fmt.Sprintf(
		"UPDATE %s SET %s = ? WHERE %s = ?", c.table, c.column, c.key,
	)
	var last int64 = -1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rows []payloadRow
		err := s.withWriteTransactionContext(
			ctx, nil, func(db queryer, ctx context.Context) error {
				var err error
				rows, err = readPayloadRows(ctx, db, query, last)
				if err != nil {
					return err
				}
				for _, row := range rows {
					if len(row.value) == 0 || isPayloadOffset(row.value) {
						continue
					}
					locator, err := blocks.LocatorAtSlot(ctx, row.slot)
					if expired, ok := errors.AsType[*types.HistoryExpiredError](err); ok {
						// The block is gone and no archive has it: the
						// payload is not kept, and a read names the block.
						if _, err := db.ExecContext(
							ctx, update,
							types.EncodeExpiredPayload(
								expired.Slot, expired.Hash,
							),
							row.key,
						); err != nil {
							return err
						}
						continue
					}
					if err != nil {
						return fmt.Errorf(
							"%s %d at slot %d: %w",
							c.table, row.key, row.slot, err,
						)
					}
					offset, err := locator.LocatePayload(row.value)
					if err != nil {
						return fmt.Errorf(
							"%s %d at slot %d: %w",
							c.table, row.key, row.slot, err,
						)
					}
					if _, err := db.ExecContext(
						ctx, update, offset, row.key,
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
		if len(rows) < payloadMigrationBatch {
			return nil
		}
		last = rows[len(rows)-1].key
	}
}

func readPayloadRows(
	ctx context.Context,
	db queryer,
	query string,
	after int64,
) ([]payloadRow, error) {
	rows, err := db.QueryContext(ctx, query, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []payloadRow
	for rows.Next() {
		var (
			row  payloadRow
			slot sql.NullInt64
		)
		if err := rows.Scan(&row.key, &row.value, &slot); err != nil {
			return nil, err
		}
		if slot.Valid && slot.Int64 > 0 {
			row.slot = uint64(slot.Int64)
		}
		ret = append(ret, row)
	}
	return ret, rows.Err()
}

// datumOffset returns the offset the datum table holds for the datum with the
// given bytes.
func datumOffset(
	ctx context.Context,
	db queryer,
	datum []byte,
) ([]byte, error) {
	hash := lcommon.Blake2b256Hash(datum)
	var offset []byte
	err := db.QueryRowContext(
		ctx, "SELECT raw_datum FROM datum WHERE hash = ?", hash.Bytes(),
	).Scan(&offset)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("datum %s is not in the datum table", hash)
	}
	if err != nil {
		return nil, err
	}
	if !isPayloadOffset(offset) {
		return nil, fmt.Errorf("datum %s has not been converted", hash)
	}
	return offset, nil
}

func (s *Store) migrateContentPayloadColumn(
	ctx context.Context,
	c contentPayloadColumn,
) error {
	query := fmt.Sprintf(
		"SELECT %[2]s, %[3]s, 0 FROM %[1]s WHERE %[2]s > ? ORDER BY %[2]s LIMIT %[4]d",
		c.table, c.key, c.column, payloadMigrationBatch,
	)
	update := fmt.Sprintf(
		"UPDATE %s SET %s = ? WHERE %s = ?", c.table, c.column, c.key,
	)
	var last int64 = -1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rows []payloadRow
		err := s.withWriteTransactionContext(
			ctx, nil, func(db queryer, ctx context.Context) error {
				var err error
				rows, err = readPayloadRows(ctx, db, query, last)
				if err != nil {
					return err
				}
				for _, row := range rows {
					if len(row.value) == 0 || isPayloadOffset(row.value) {
						continue
					}
					offset, err := datumOffset(ctx, db, row.value)
					if err != nil {
						return fmt.Errorf("%s %d: %w", c.table, row.key, err)
					}
					if _, err := db.ExecContext(
						ctx, update, offset, row.key,
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
		if len(rows) < payloadMigrationBatch {
			return nil
		}
		last = rows[len(rows)-1].key
	}
}

// migrateEpochCandidates rewrites each candidate snapshot that carries its
// datums into the stored form that carries their offsets.
func (s *Store) migrateEpochCandidates(ctx context.Context) error {
	const query = "SELECT id, candidates_cbor, 0 FROM midnight_epoch_candidates WHERE id > ? ORDER BY id LIMIT 100"
	var last int64 = -1
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rows []payloadRow
		err := s.withWriteTransactionContext(
			ctx, nil, func(db queryer, ctx context.Context) error {
				var err error
				rows, err = readPayloadRows(ctx, db, query, last)
				if err != nil {
					return err
				}
				for _, row := range rows {
					stored, err := snapshotToOffsets(ctx, db, row.value)
					if err != nil {
						return fmt.Errorf(
							"midnight_epoch_candidates %d: %w", row.key, err,
						)
					}
					if stored == nil {
						continue
					}
					if _, err := db.ExecContext(
						ctx,
						"UPDATE midnight_epoch_candidates SET candidates_cbor = ? WHERE id = ?",
						stored, row.key,
					); err != nil {
						return err
					}
				}
				return nil
			},
		)
		if err != nil {
			return err
		}
		if len(rows) < 100 {
			return nil
		}
		last = rows[len(rows)-1].key
	}
}

// snapshotToOffsets returns the stored form of a snapshot that carries
// datums, or nil when the snapshot is empty or already stored with offsets.
func snapshotToOffsets(
	ctx context.Context,
	db queryer,
	snapshot []byte,
) ([]byte, error) {
	if len(snapshot) == 0 {
		return nil, nil
	}
	var entries []candidateWriteEntry
	if err := fxcbor.Unmarshal(snapshot, &entries); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}
	stored := make([]candidateOffsetEntry, 0, len(entries))
	converted := false
	for _, entry := range entries {
		offset := entry.DatumOffset
		if len(offset) == 0 && len(entry.Datum) > 0 {
			var err error
			offset, err = datumOffset(ctx, db, entry.Datum)
			if err != nil {
				return nil, err
			}
			converted = true
		}
		stored = append(stored, candidateOffsetEntry{
			TxHash:      entry.TxHash,
			OutputIndex: entry.OutputIndex,
			DatumOffset: offset,
		})
	}
	if !converted {
		return nil, nil
	}
	return fxcbor.Marshal(stored)
}
