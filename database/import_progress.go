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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// Import progress says how far a snapshot import has replaced the stored
// chain and the ledger state built on it. It describes the blocks the block
// store holds, so it is chain state: a value that named an import the block
// store no longer reflects would let a start serve a half-replaced chain.

// SyncStatusKey names the state of a snapshot import that has begun and not
// finished. It is unset on a database no import is running against.
const SyncStatusKey = "sync_status"

// ImportedLedgerStateSlotKey and ImportedLedgerStateHashKey name the point a
// ledger state was loaded at without replaying the blocks below it.
const (
	ImportedLedgerStateSlotKey = "mithril_ledger_slot"
	ImportedLedgerStateHashKey = "mithril_ledger_hash"
)

const (
	mithrilProgressPrefix = "mithril_"
	ledgerReimportPrefix  = "ledger.state_reimport."
)

// importCheckpointKey names the last phase a snapshot import wrote, with the
// key of the import that wrote it.
const importCheckpointKey = "mithril_ledger_import_checkpoint"

// SetImportCheckpoint records phase as the last phase the import importKey
// has written. It replaces the checkpoint of any earlier import.
func (d *Database) SetImportCheckpoint(importKey, phase string) error {
	if strings.Contains(phase, "\n") {
		return fmt.Errorf("import checkpoint phase %q holds a newline", phase)
	}
	return d.SetChainState(importCheckpointKey, phase+"\n"+importKey, nil)
}

// ImportCheckpoint returns the last phase the import importKey has written,
// or the empty string when the recorded checkpoint is another import's or
// none is recorded.
func (d *Database) ImportCheckpoint(importKey string) (string, error) {
	raw, err := d.GetChainState(importCheckpointKey, nil)
	if err != nil {
		return "", err
	}
	phase, key, ok := strings.Cut(raw, "\n")
	if !ok || key != importKey {
		return "", nil
	}
	return phase, nil
}

// maxImportedLedgerStateHashBytes bounds the block hash a recorded boundary
// may carry.
const maxImportedLedgerStateHashBytes = 32

// IsImportProgressKey reports whether key names import progress. A released
// database can still hold such a row in sync_state after the open-time move,
// which leaves the boundary rows and a backfill status in place.
func IsImportProgressKey(key string) bool {
	return importProgressChainState(key)
}

// importProgressChainState reports whether key names import progress.
func importProgressChainState(key string) bool {
	return key == SyncStatusKey ||
		strings.HasPrefix(key, mithrilProgressPrefix) ||
		strings.HasPrefix(key, ledgerReimportPrefix)
}

// LookupChainState returns the chain state value name and whether one is
// recorded, which GetChainState's empty string cannot tell apart from a value
// recorded empty. It reads through a transaction of its own.
func (d *Database) LookupChainState(name string) (string, bool, error) {
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return "", false, types.ErrBlobStoreUnavailable
	}
	readTxn := store.NewTransaction(false)
	defer func() { _ = readTxn.Rollback() }()
	raw, err := store.Get(readTxn, chainStateBlobKey(name))
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("LookupChainState(%q): %w", name, err)
	}
	return string(raw), true, nil
}

// SyncChainState returns once the block store has flushed the chain state
// written so far, so it survives an unclean host shutdown.
func (d *Database) SyncChainState() error {
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if err := store.Sync(); err != nil {
		return fmt.Errorf("sync chain state: %w", err)
	}
	return nil
}

// ImportedLedgerStateBoundaryHash returns the block hash recorded with the
// imported ledger-state boundary as hex, or the empty string when none is.
func (d *Database) ImportedLedgerStateBoundaryHash() (string, error) {
	if d.isIndexer() {
		return d.GetSyncState(ImportedLedgerStateHashKey, nil)
	}
	return d.GetChainState(ImportedLedgerStateHashKey, nil)
}

// setImportedLedgerStateBoundary stores point as the ledger database's
// boundary. Both values go in one blob transaction, so no reader pairs a new
// slot with the hash of the point it replaced.
func (d *Database) setImportedLedgerStateBoundary(
	point ocommon.Point,
	txn *Txn,
) error {
	if len(point.Hash) > maxImportedLedgerStateHashBytes {
		return fmt.Errorf(
			"imported ledger-state hash is %d bytes, more than %d",
			len(point.Hash), maxImportedLedgerStateHashBytes,
		)
	}
	if txn == nil || txn.Blob() == nil {
		return d.BlobTxn(true).Do(func(own *Txn) error {
			return d.setImportedLedgerStateBoundary(point, own)
		})
	}
	if err := d.SetChainState(
		ImportedLedgerStateSlotKey, strconv.FormatUint(point.Slot, 10), txn,
	); err != nil {
		return fmt.Errorf("record imported ledger-state slot: %w", err)
	}
	if len(point.Hash) == 0 {
		if err := d.DeleteChainState(ImportedLedgerStateHashKey, txn); err != nil {
			return fmt.Errorf("clear imported ledger-state hash: %w", err)
		}
		return nil
	}
	if err := d.SetChainState(
		ImportedLedgerStateHashKey, hex.EncodeToString(point.Hash), txn,
	); err != nil {
		return fmt.Errorf("record imported ledger-state hash: %w", err)
	}
	return nil
}

// FollowImportedLedgerStateBoundaryRow rewrites the boundary rows a released
// database still holds in sync_state to point, and writes nothing when the
// slot row is absent. The metadata store's block counts read that row, so one
// left at the boundary of an import since replaced, or removed while the
// counter rows it guards remain, would let those counts take imported op-cert
// counters for minted blocks. The caller records the boundary in the block
// store afterwards; a stop between the two repeats the import's last step.
func (d *Database) FollowImportedLedgerStateBoundaryRow(
	point ocommon.Point,
) error {
	if d.isIndexer() || isNilStore(d.metadata) {
		return nil
	}
	if len(point.Hash) > maxImportedLedgerStateHashBytes {
		return fmt.Errorf(
			"imported ledger-state hash is %d bytes, more than %d",
			len(point.Hash), maxImportedLedgerStateHashBytes,
		)
	}
	rows, err := d.metadata.ListSyncStateKeysByPrefix(
		ImportedLedgerStateSlotKey, nil,
	)
	if err != nil {
		return fmt.Errorf("list imported ledger-state rows: %w", err)
	}
	if !slices.Contains(rows, ImportedLedgerStateSlotKey) {
		return nil
	}
	if err := d.metadata.SetSyncState(
		ImportedLedgerStateSlotKey,
		strconv.FormatUint(point.Slot, 10),
		nil,
	); err != nil {
		return fmt.Errorf("rewrite imported ledger-state slot row: %w", err)
	}
	if len(point.Hash) == 0 {
		err = d.metadata.DeleteSyncState(ImportedLedgerStateHashKey, nil)
	} else {
		err = d.metadata.SetSyncState(
			ImportedLedgerStateHashKey, hex.EncodeToString(point.Hash), nil,
		)
	}
	if err != nil {
		return fmt.Errorf("rewrite imported ledger-state hash row: %w", err)
	}
	return nil
}

// moveImportProgress copies into the block store the import progress a
// v0.94.2 database holds in sync_state.
//
// The boundary rows are copied and left in place. The metadata store's own
// block counts raise their start past the boundary by reading its row, both
// when the reward block counts are loaded into the LedgerDB from a v0.94.2
// database and once the store has become an IndexerDB; a count that found no
// row would take the op-cert counters an import recorded at the boundary for
// minted blocks. Every other row but a backfill status is removed once
// copied.
func (d *Database) moveImportProgress() error {
	if err := d.copyImportedLedgerStateBoundary(); err != nil {
		return err
	}
	if err := d.moveSyncStatus(); err != nil {
		return err
	}
	for _, prefix := range []string{
		mithrilProgressPrefix, ledgerReimportPrefix,
	} {
		rows, err := d.metadata.ListSyncStateKeysByPrefix(prefix, nil)
		if err != nil {
			return fmt.Errorf("list %s rows: %w", prefix, err)
		}
		for _, row := range rows {
			if row == ImportedLedgerStateSlotKey ||
				row == ImportedLedgerStateHashKey {
				continue
			}
			if err := d.moveSyncStateValueIfAbsent(row); err != nil {
				return err
			}
		}
	}
	return nil
}

// syncStatusBackfill is the status of a v0.94.2 API database whose import
// stopped while it was still writing API history from the imported blocks.
const syncStatusBackfill = "backfill"

// moveSyncStatus moves the sync status row. A backfill status is copied and
// left in place: adopting the API database as an IndexerDB reads the row from
// the metadata store to refuse the unfinished history, and tells the operator
// to bring the database back as v0.94.2 left it.
func (d *Database) moveSyncStatus() error {
	status, err := d.metadata.GetSyncState(SyncStatusKey, nil)
	if err != nil {
		return fmt.Errorf("read %s row: %w", SyncStatusKey, err)
	}
	if status != syncStatusBackfill {
		return d.moveSyncStateValueIfAbsent(SyncStatusKey)
	}
	held, err := d.GetChainState(SyncStatusKey, nil)
	if err != nil || held != "" {
		return err
	}
	return d.SetChainStateSynced(SyncStatusKey, status)
}

// copyImportedLedgerStateBoundary copies the boundary rows unless the block
// store already records a boundary slot: once copied the boundary is written
// only there, so a recorded one is the newer. A slot row holding nothing is
// copied as it is, so the strict accessor still refuses it.
func (d *Database) copyImportedLedgerStateBoundary() error {
	_, held, err := d.LookupChainState(ImportedLedgerStateSlotKey)
	if err != nil || held {
		return err
	}
	rows, err := d.metadata.ListSyncStateKeysByPrefix(
		ImportedLedgerStateSlotKey, nil,
	)
	if err != nil {
		return fmt.Errorf("list imported ledger-state rows: %w", err)
	}
	if !slices.Contains(rows, ImportedLedgerStateSlotKey) {
		return nil
	}
	slot, err := d.metadata.GetSyncState(ImportedLedgerStateSlotKey, nil)
	if err != nil {
		return fmt.Errorf("read imported ledger-state slot row: %w", err)
	}
	hash, err := d.metadata.GetSyncState(ImportedLedgerStateHashKey, nil)
	if err != nil {
		return fmt.Errorf("read imported ledger-state hash row: %w", err)
	}
	if len(hash) > hex.EncodedLen(maxImportedLedgerStateHashBytes) {
		return fmt.Errorf(
			"imported ledger-state hash row is %d characters, more than %d",
			len(hash), hex.EncodedLen(maxImportedLedgerStateHashBytes),
		)
	}
	store, release := d.PinBlob()
	defer release()
	if store == nil {
		return types.ErrBlobStoreUnavailable
	}
	if err := d.BlobTxn(true).Do(func(txn *Txn) error {
		if hash != "" {
			if err := d.SetChainState(
				ImportedLedgerStateHashKey, hash, txn,
			); err != nil {
				return err
			}
		}
		return d.SetChainState(ImportedLedgerStateSlotKey, slot, txn)
	}); err != nil {
		return fmt.Errorf("copy the imported ledger-state boundary: %w", err)
	}
	if err := store.Sync(); err != nil {
		return fmt.Errorf("sync the imported ledger-state boundary: %w", err)
	}
	return nil
}
