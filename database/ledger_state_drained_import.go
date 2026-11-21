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

package database

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DrainedImportSyncKey is the sync_state key that marks an import of ledger
// state into a drained database as begun. Its value is the drained mask
// encoding. It is set before the import writes a row and cleared only after
// the live UTxO set holds every drained family and the SQL rows are marked
// for deletion, so a start that finds it knows the set is not yet the
// complete copy of the ledger state.
const DrainedImportSyncKey = "ledger.state_import.drained"

// LedgerReimportSyncKey marks an import of ledger state the node started on
// its own to recover its live UTxO set, as opposed to one a Mithril sync
// command began. A start that finds it with the import unfinished runs the
// import again.
const LedgerReimportSyncKey = "ledger.state_reimport.active"

// LedgerReimportSwappedSyncKey records, as `<slot>:<hash>`, the point of the
// imported live UTxO set a re-import has made current. A start that finds the
// re-import marks set finishes the run without importing again only when this
// names the ledger's tip: a set at the ledger's tip says nothing by itself,
// since a crash before the import wrote anything leaves the set as it was.
const LedgerReimportSwappedSyncKey = "ledger.state_reimport.swapped"

// LedgerReimportPointMark is the value of LedgerReimportSwappedSyncKey for the
// imported point.
func LedgerReimportPointMark(point ocommon.Point) string {
	return strconv.FormatUint(point.Slot, 10) + ":" + hex.EncodeToString(point.Hash)
}

// DrainedImportPointSyncKey records the snapshot point the pending import into
// the live UTxO set is writing, as `<slot>:<hash>`. It lives and dies with
// DrainedImportSyncKey.
const DrainedImportPointSyncKey = "ledger.state_import.drained.point"

// ErrDrainedImportPending reports a database whose import into the live UTxO
// set began and did not finish.
var ErrDrainedImportPending = errors.New(
	"an import of ledger state into the live UTxO set did not finish; " +
		"only the import that began it finishes it",
)

// DrainedImportPending returns the families a begun import into the live UTxO
// set covers, with ok false when none is pending.
func (d *Database) DrainedImportPending(
	_ context.Context,
) (nodesettings.FamilyMask, bool, error) {
	v, err := d.GetSyncState(DrainedImportSyncKey, nil)
	if err != nil {
		return 0, false, fmt.Errorf("read drained import marker: %w", err)
	}
	if v == "" {
		return 0, false, nil
	}
	mask, err := nodesettings.DecodeFamilyMask(v)
	if err != nil {
		return 0, false, err
	}
	return mask, true, nil
}

// BeginDrainedImport prepares an import of ledger state into a database that
// drained families: the import writes those families' SQL rows again, as the
// loaders of the live UTxO set read them, and FinishDrainedImport moves them
// into the set and deletes them. The latch is never cleared. While the import
// runs, the database reports no family drained, so every SQL path an import
// uses writes its rows.
//
// It returns false, changing nothing, for a database with no drained family
// and no pending import. A drain that began and did not finish is refused:
// its rows are partly deleted, and the import would mix them with the new
// ones.
func (d *Database) BeginDrainedImport(
	ctx context.Context,
) (bool, error) {
	if d.drained.suspended.Load() {
		return true, nil
	}
	latch, begun, err := d.readDrainState(ctx)
	if err != nil {
		return false, err
	}
	pending, hasPending, err := d.DrainedImportPending(ctx)
	if err != nil {
		return false, err
	}
	if latch == 0 && !hasPending {
		if begun != 0 {
			return false, fmt.Errorf(
				"%w (drain begun: %v)", ErrLedgerStateDrained, begun.Families(),
			)
		}
		return false, nil
	}
	if begun != 0 && !hasPending {
		return false, fmt.Errorf(
			"a drain of %v began and did not finish: start the node once so it finishes the deletes, then import again",
			begun.Families(),
		)
	}
	mask := latch | pending
	if err := d.SetSyncState(
		DrainedImportSyncKey, nodesettings.EncodeFamilyMask(mask), nil,
	); err != nil {
		return false, fmt.Errorf("record drained import: %w", err)
	}
	d.drained.suspended.Store(true)
	d.publishDrained(0)
	return true, nil
}

// PinDrainedImportPoint records the snapshot point the begun import writes.
// A pending import over a different point left rows of its own snapshot in the
// drained families' SQL tables, which the new import would mix with its own,
// so those tables are emptied first; it reports whether it did. Call it after
// BeginDrainedImport and before the import writes a row.
func (d *Database) PinDrainedImportPoint(
	ctx context.Context,
	point ocommon.Point,
) (reset bool, err error) {
	mask, ok, err := d.DrainedImportPending(ctx)
	if err != nil || !ok {
		return false, err
	}
	want := fmt.Sprintf("%d:%x", point.Slot, point.Hash)
	have, err := d.GetSyncState(DrainedImportPointSyncKey, nil)
	if err != nil {
		return false, fmt.Errorf("read drained import point: %w", err)
	}
	if have == want {
		return false, nil
	}
	if have != "" {
		store := drainImportStore{d}
		for _, family := range mask.Families() {
			for _, table := range statedrain.Tables(family) {
				for {
					if err := ctx.Err(); err != nil {
						return false, err
					}
					n, err := store.DeleteLedgerStateRows(
						ctx, table.Name, table.Key, statedrain.DefaultChunkRows,
					)
					if err != nil {
						return false, fmt.Errorf("empty %s: %w", table.Name, err)
					}
					if n < statedrain.DefaultChunkRows {
						break
					}
				}
			}
		}
		reset = true
	}
	if err := d.SetSyncState(DrainedImportPointSyncKey, want, nil); err != nil {
		return false, fmt.Errorf("record drained import point: %w", err)
	}
	return reset, nil
}

// FinishDrainedImport ends an import BeginDrainedImport began. load moves the
// imported rows into the live UTxO set; the import is complete only once it
// returns. The drained families are then marked for deletion, the pending
// marker is cleared, and the SQL rows are deleted in chunks. A crash after
// the marks leaves the ordinary drain markers, which the next start resumes.
func (d *Database) FinishDrainedImport(
	ctx context.Context,
	load func(context.Context) error,
) (err error) {
	mask, ok, err := d.DrainedImportPending(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("no import into the live UTxO set is pending")
	}
	d.drained.suspended.Store(true)
	d.publishDrained(0)
	defer func() {
		if err != nil {
			d.EndDrainedImport(ctx)
		}
	}()
	if err := load(ctx); err != nil {
		return fmt.Errorf("load the live UTxO set: %w", err)
	}
	store := drainImportStore{d}
	if err := statedrain.MarkRewritten(ctx, store, mask); err != nil {
		return err
	}
	if err := d.DeleteSyncState(DrainedImportSyncKey, nil); err != nil {
		return fmt.Errorf("clear drained import marker: %w", err)
	}
	if err := d.DeleteSyncState(DrainedImportPointSyncKey, nil); err != nil {
		return fmt.Errorf("clear drained import point: %w", err)
	}
	d.drained.suspended.Store(false)
	if _, err := d.EffectiveDrainedFamilies(ctx); err != nil {
		return err
	}
	return statedrain.DeleteMarked(ctx, statedrain.Env{
		Store: store,
	}, mask)
}

// drainImportStore adapts the database to the deletes a drain finishes with.
type drainImportStore struct{ d *Database }

func (s drainImportStore) SQLTip(context.Context) (ocommon.Point, error) {
	tip, err := s.d.GetTip(nil)
	return tip.Point, err
}

func (s drainImportStore) DrainedFamilies(
	ctx context.Context,
) (nodesettings.FamilyMask, error) {
	return s.d.DrainedFamilies(ctx)
}

func (s drainImportStore) RecordDrainedFamily(
	ctx context.Context,
	f nodesettings.Family,
) error {
	return s.d.RecordDrainedFamily(ctx, f)
}

func (s drainImportStore) DeleteLedgerStateRows(
	ctx context.Context,
	table, key string,
	limit int,
) (int64, error) {
	return s.d.DeleteLedgerStateRows(ctx, table, key, limit)
}

func (s drainImportStore) GetSyncState(
	_ context.Context,
	key string,
) (string, error) {
	return s.d.GetSyncState(key, nil)
}

func (s drainImportStore) SetSyncState(
	_ context.Context,
	key, value string,
) error {
	return s.d.SetSyncState(key, value, nil)
}

func (s drainImportStore) DeleteSyncState(
	_ context.Context,
	key string,
) error {
	return s.d.DeleteFamilySyncState(key)
}

func (s drainImportStore) ListSyncStateKeys(
	_ context.Context,
	prefix string,
) ([]string, error) {
	return s.d.ListSyncStateKeysByPrefix(prefix, nil)
}

// DrainedImportActive reports whether BeginDrainedImport has suspended the
// drained mask and FinishDrainedImport has not yet run.
func (d *Database) DrainedImportActive() bool {
	return d.drained.suspended.Load()
}

// EndDrainedImport stops treating the database as undrained without clearing
// the pending marker, for an import that stopped part way: the next import
// resumes it, and a start refuses the database until then.
func (d *Database) EndDrainedImport(ctx context.Context) {
	d.drained.suspended.Store(false)
	_, _ = d.EffectiveDrainedFamilies(ctx)
}

// DeleteFamilySyncState deletes a sync_state key a drained family owns. The
// drain calls it for its own keys, which DeleteSyncState refuses once the
// family is latched, and which a drain resumed after a crash or an import
// into the set deletes after the latch is set.
func (d *Database) DeleteFamilySyncState(key string) error {
	if err := d.metadata.DeleteSyncState(key, nil); err != nil {
		return fmt.Errorf("Database.DeleteFamilySyncState(%q): %w", key, err)
	}
	return nil
}
