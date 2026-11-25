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
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
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
	// pending are the orphaned rows no indexed table names a carrier for,
	// settled once the table has been read through.
	var pending []unresolvedOrphan
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var rows []payloadRow
		var batchPending []unresolvedOrphan
		err := s.withWriteTransactionContext(
			ctx, nil, func(db queryer, ctx context.Context) error {
				var err error
				batchPending = nil
				rows, err = readPayloadRows(ctx, db, query, last)
				if err != nil {
					return err
				}
				for _, row := range rows {
					if len(row.value) == 0 || isPayloadOffset(row.value) {
						continue
					}
					offset, expired, err := locatePayloadAt(
						ctx, blocks, row.slot, row.value,
					)
					if err != nil && contentColumn(c) && isOrphaned(err) {
						orphan, err := s.resolveOrphanedContent(
							ctx, db, blocks, c, row,
						)
						if err != nil {
							return fmt.Errorf(
								"%s %d at slot %d: %w",
								c.table, row.key, row.slot, err,
							)
						}
						if orphan != nil {
							batchPending = append(batchPending, *orphan)
						}
						continue
					}
					if err != nil {
						return fmt.Errorf(
							"%s %d at slot %d: %w",
							c.table, row.key, row.slot, err,
						)
					}
					if expired != nil {
						// The block is gone and no archive has it: the
						// payload is not kept, and a read names the block.
						offset = types.EncodeExpiredPayload(
							expired.Slot, expired.Hash,
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
		pending = append(pending, batchPending...)
		if len(rows) < payloadMigrationBatch {
			return s.resolveUnindexedCarriers(ctx, blocks, c, pending)
		}
		last = rows[len(rows)-1].key
	}
}

// locatePayloadAt finds the payload in the canonical block at slot. A block
// that expired with no archive returns the error naming it, with no offset.
func locatePayloadAt(
	ctx context.Context,
	blocks metadata.PayloadBlockSource,
	slot uint64,
	payload []byte,
) ([]byte, *types.HistoryExpiredError, error) {
	locator, err := blocks.LocatorAtSlot(ctx, slot)
	if expired, ok := errors.AsType[*types.HistoryExpiredError](err); ok {
		return nil, expired, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if locator == nil {
		return nil, nil, fmt.Errorf("no block locator at slot %d", slot)
	}
	offset, err := locator.LocatePayload(payload)
	if expired, ok := errors.AsType[*types.HistoryExpiredError](err); ok {
		// A locator over an expired block's live outputs found none that
		// holds the payload.
		return nil, expired, nil
	}
	return offset, nil, err
}

// contentColumn reports whether the column is a content-addressed table whose
// row keeps the slot of the first block that carried it.
func contentColumn(c slotPayloadColumn) bool {
	return c.table == "datum" || c.table == "script"
}

// isOrphaned reports whether a row's slot names no canonical block, or a
// block that does not hold the row's bytes: the signature of a row written by
// a block that a rollback abandoned.
func isOrphaned(err error) bool {
	return errors.Is(err, models.ErrBlockNotFound) ||
		errors.Is(err, types.ErrPayloadNotInBlock)
}

// payloadCarrier is a canonical transaction's slot that carries a content row.
// required is set when the transaction's own tables say its block holds the
// bytes, as a witness does, and clear when it only names them by hash.
type payloadCarrier struct {
	slot     uint64
	required bool
}

// carrierQueries find, per content table, the slots of the applied
// transactions that carry a hash. A rolled-back transaction's rows are
// deleted with it, so every row they reach is canonical.
var carrierQueries = map[string][]struct {
	query    string
	required bool
}{
	"datum": {
		{`SELECT DISTINCT a.slot FROM plutus_data p
JOIN applied_transaction a ON a.tx_id = p.transaction_id
WHERE p.data = ?`, true},
		{`SELECT DISTINCT a.slot FROM utxo u
JOIN applied_transaction a ON a.tx_id = u.tx_id
WHERE u.datum_hash = ?`, false},
	},
	"script": {
		{`SELECT DISTINCT a.slot FROM witness_scripts w
JOIN applied_transaction a ON a.tx_id = w.transaction_id
WHERE w.script_hash = ?`, true},
		{`SELECT DISTINCT a.slot FROM utxo_reference_script r
JOIN utxo u ON u.id = r.utxo_id
JOIN applied_transaction a ON a.tx_id = u.tx_id
WHERE r.script_hash = ?`, true},
	},
}

// resolveOrphanedContent gives a datum or script row a defined result when
// the block it names is not canonical or does not hold its bytes. The row is
// re-pointed to the first canonical transaction whose block carries the
// content, its slot following. A row no canonical transaction carries is
// unreachable and is deleted, which is what a rollback now does to the same
// row. A canonical transaction that must carry the bytes in a block that does
// not hold them stops the migration instead: that row would otherwise be lost.
func (s *Store) resolveOrphanedContent(
	ctx context.Context,
	db queryer,
	blocks metadata.PayloadBlockSource,
	c slotPayloadColumn,
	row payloadRow,
) (*unresolvedOrphan, error) {
	var hash []byte
	if err := db.QueryRowContext(
		ctx,
		fmt.Sprintf("SELECT hash FROM %s WHERE %s = ?", c.table, c.key),
		row.key,
	).Scan(&hash); err != nil {
		return nil, err
	}
	var carriers []payloadCarrier
	for _, q := range carrierQueries[c.table] {
		arg := hash
		if c.table == "datum" && q.required {
			arg = row.value
		}
		slots, err := carrierSlots(ctx, db, q.query, arg)
		if err != nil {
			return nil, err
		}
		for _, slot := range slots {
			carriers = append(carriers, payloadCarrier{
				slot: slot, required: q.required,
			})
		}
	}
	slices.SortStableFunc(carriers, func(a, b payloadCarrier) int {
		return cmp.Compare(a.slot, b.slot)
	})
	update := fmt.Sprintf(
		"UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
		c.table, c.column, c.slot, c.key,
	)
	for _, carrier := range carriers {
		offset, expired, err := locatePayloadAt(
			ctx, blocks, carrier.slot, row.value,
		)
		if expired != nil {
			offset = types.EncodeExpiredPayload(expired.Slot, expired.Hash)
		} else if err != nil && !carrier.required && isOrphaned(err) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf(
				"the canonical transaction at slot %d carries it: %w",
				carrier.slot, err,
			)
		}
		_, err = db.ExecContext(ctx, update, offset, carrier.slot, row.key)
		return nil, err
	}
	return &unresolvedOrphan{row: row, hash: hash}, nil
}

// unresolvedOrphan is an orphaned content row no indexed table accounts for.
type unresolvedOrphan struct {
	row  payloadRow
	hash []byte
}

// resolveUnindexedCarriers settles the orphaned rows of one content table that
// no indexed table names a carrier for. One scan of the canonical blocks finds
// those a transaction still carries somewhere the store keeps no row for, such
// as a script in auxiliary data; each is re-pointed to the first such block, as
// for any other carrier. The rest are carried by no canonical transaction and
// are deleted. A source that cannot read transactions finds no carrier.
func (s *Store) resolveUnindexedCarriers(
	ctx context.Context,
	blocks metadata.PayloadBlockSource,
	c slotPayloadColumn,
	orphans []unresolvedOrphan,
) error {
	if len(orphans) == 0 {
		return nil
	}
	carriers := map[metadata.ContentKey]uint64{}
	var unreadable *types.HistoryExpiredError
	if scanner, ok := blocks.(metadata.PayloadCarrierScanner); ok {
		wanted := make(map[metadata.ContentKey]struct{}, len(orphans))
		for _, o := range orphans {
			wanted[metadata.ContentKey{Table: c.table, Hash: string(o.hash)}] = struct{}{}
		}
		var err error
		if carriers, unreadable, err = scanner.FindCarriers(ctx, wanted); err != nil {
			return fmt.Errorf("scan canonical transactions for %s rows: %w", c.table, err)
		}
	}
	update := fmt.Sprintf(
		"UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
		c.table, c.column, c.slot, c.key,
	)
	remove := fmt.Sprintf("DELETE FROM %s WHERE %s = ?", c.table, c.key)
	return s.withWriteTransactionContext(
		ctx, nil, func(db queryer, ctx context.Context) error {
			for _, o := range orphans {
				slot, ok := carriers[metadata.ContentKey{Table: c.table, Hash: string(o.hash)}]
				if !ok && unreadable != nil {
					// A block the scan could not read might carry the row.
					// No archive serves it, so the payload is not kept: the
					// marker names that block, which may not be the carrier,
					// and a read fails with the expired-history error.
					if _, err := db.ExecContext(
						ctx,
						fmt.Sprintf("UPDATE %s SET %s = ? WHERE %s = ?", c.table, c.column, c.key),
						types.EncodeExpiredPayload(unreadable.Slot, unreadable.Hash),
						o.row.key,
					); err != nil {
						return err
					}
					continue
				}
				if !ok {
					if _, err := db.ExecContext(ctx, remove, o.row.key); err != nil {
						return err
					}
					continue
				}
				offset, expired, err := locatePayloadAt(ctx, blocks, slot, o.row.value)
				if err != nil {
					return fmt.Errorf(
						"%s %d: the canonical transaction at slot %d carries it: %w",
						c.table, o.row.key, slot, err,
					)
				}
				if expired != nil {
					offset = types.EncodeExpiredPayload(expired.Slot, expired.Hash)
				}
				if _, err := db.ExecContext(ctx, update, offset, slot, o.row.key); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func carrierSlots(
	ctx context.Context,
	db queryer,
	query string,
	arg []byte,
) ([]uint64, error) {
	rows, err := db.QueryContext(ctx, query, arg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var slots []uint64
	for rows.Next() {
		var slot int64
		if err := rows.Scan(&slot); err != nil {
			return nil, err
		}
		if slot > 0 {
			slots = append(slots, uint64(slot))
		}
	}
	return slots, rows.Err()
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
