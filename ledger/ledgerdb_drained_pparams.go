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

package ledger

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
)

// SyncMarkerReader serves the protocol parameter sync markers from a live
// UTxO set that keeps them.
type SyncMarkerReader interface {
	SyncMarkersLoaded() (bool, error)
	// SyncMarker returns the value of marker name and whether it is set.
	SyncMarker(name string) (string, bool, error)
}

// drainedPParamsSource serves the database's own protocol parameter reads,
// and a rollback's, from the committed rows of the set once the
// pparams-updates family is drained.
type drainedPParamsSource struct {
	ls *LedgerState
}

func (d drainedPParamsSource) pparams() (PParamsReader, error) {
	r, ok := d.ls.readSet(nil).(PParamsReader)
	if !ok || d.ls.utxoSetFailed.Load() {
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	return r, nil
}

func (d drainedPParamsSource) updates() (PParamUpdateReader, error) {
	r, ok := d.ls.readSet(nil).(PParamUpdateReader)
	if !ok || d.ls.utxoSetFailed.Load() {
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	return r, nil
}

func (d drainedPParamsSource) markers() (SyncMarkerReader, error) {
	r, ok := d.ls.readSet(nil).(SyncMarkerReader)
	if !ok || d.ls.utxoSetFailed.Load() {
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	return r, nil
}

func (d drainedPParamsSource) PParamsLoaded() (bool, error) {
	r, ok := d.ls.readSet(nil).(PParamsReader)
	if !ok {
		return false, nil
	}
	return r.PParamsLoaded()
}

func (d drainedPParamsSource) PParamsForEra(
	era uint,
) ([]models.PParams, error) {
	r, err := d.pparams()
	if err != nil {
		return nil, err
	}
	return r.PParamsForEra(era)
}

func (d drainedPParamsSource) PParamUpdatesLoaded() (bool, error) {
	r, ok := d.ls.readSet(nil).(PParamUpdateReader)
	if !ok {
		return false, nil
	}
	return r.PParamUpdatesLoaded()
}

func (d drainedPParamsSource) PParamUpdates(
	fn func(models.PParamUpdate) error,
) error {
	r, err := d.updates()
	if err != nil {
		return err
	}
	return r.PParamUpdates(fn)
}

func (d drainedPParamsSource) SyncMarkersLoaded() (bool, error) {
	r, ok := d.ls.readSet(nil).(SyncMarkerReader)
	if !ok {
		return false, nil
	}
	return r.SyncMarkersLoaded()
}

func (d drainedPParamsSource) SyncMarker(name string) (string, bool, error) {
	r, err := d.markers()
	if err != nil {
		return "", false, err
	}
	return r.SyncMarker(name)
}

// installPParamsSource gives the database the set's protocol parameters,
// proposals and sync markers for the reads it makes once the pparams-updates
// family is drained.
func (ls *LedgerState) installPParamsSource() {
	if ls.db != nil {
		ls.db.SetPParamsSource(drainedPParamsSource{ls})
	}
}

// syncMarkerView is marker name as txn sees it: the set's value under the
// changes txn and the batches it sees have staged. No read of a marker
// follows a rollback staged in the same transaction, so a staged rollback is
// not applied here.
func (ls *LedgerState) syncMarkerView(
	txn ledgerdb.Txn,
	name string,
) (string, error) {
	r, ok := ls.readSet(txn).(SyncMarkerReader)
	if !ok || ls.utxoSetFailed.Load() {
		return "", drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	loaded, err := r.SyncMarkersLoaded()
	if err != nil {
		return "", err
	}
	if !loaded {
		return "", drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	ls.utxoSetApplyMu.RLock()
	defer ls.utxoSetApplyMu.RUnlock()
	ops := ls.stagedEpochStateOps(txn)
	value, _, err := r.SyncMarker(name)
	if err != nil {
		return "", err
	}
	for _, o := range ops {
		for _, c := range o.syncMarkers {
			if c.Name != name {
				continue
			}
			value = c.Value
			if c.Delete {
				value = ""
			}
		}
	}
	return value, nil
}

// GetSyncState reads from the set the sync_state keys that name ledger
// state: the protocol parameter sync markers, the reward precompute cursors,
// the incomplete stake input snapshots, the pending ratification record and
// the delegator inactivity activation.
func (s stagingLedgerDB) GetSyncState(
	key string,
	txn ledgerdb.Txn,
) (string, error) {
	if database.IsPParamsSyncMarker(key) {
		return s.ls.syncMarkerView(txn, key)
	}
	if isRewardCursorKey(key) {
		return s.getCursor(key, txn)
	}
	switch {
	case key == rewardStakeInputsPendingKey:
		return s.getPendingStakeInputs(txn)
	case key == pendingRatificationSyncKey:
		return s.ratifyPendingState(txn)
	case isInactivityMarker(key):
		return s.inactivityMarker(txn)
	case !isChainSyncStateKey(key):
		return "", fmt.Errorf("%w: %q", errSyncStateKeyUnowned, key)
	}
	return s.LedgerDB.GetSyncState(key, txn)
}

// SetSyncState stages for the set a write of a sync_state key that names
// ledger state.
func (s stagingLedgerDB) SetSyncState(
	key, value string,
	txn ledgerdb.Txn,
) error {
	if isRewardCursorKey(key) {
		s.cursorWrite(key, value, txn)
		return nil
	}
	switch {
	case key == rewardStakeInputsPendingKey:
		return s.setPendingStakeInputs(value, txn)
	case key == pendingRatificationSyncKey:
		s.setRatifyPending(value, txn)
		return nil
	case isInactivityMarker(key):
		return errInactivityMarkerWrite
	case isChainSyncStateKey(key):
		return s.LedgerDB.SetSyncState(key, value, txn)
	case !database.IsPParamsSyncMarker(key):
		return fmt.Errorf("%w: %q", errSyncStateKeyUnowned, key)
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		SyncMarkers: []SyncMarkerChange{{Name: key, Value: value}},
	})
	return nil
}

// DeleteSyncState stages for the set the deletion of a sync_state key that
// names ledger state.
func (s stagingLedgerDB) DeleteSyncState(
	key string,
	txn ledgerdb.Txn,
) error {
	if isRewardCursorKey(key) {
		return s.deleteCursor(key, txn)
	}
	switch {
	case key == rewardStakeInputsPendingKey:
		return s.setPendingStakeInputs("", txn)
	case key == pendingRatificationSyncKey:
		return s.deleteRatifyPending(txn)
	case isInactivityMarker(key):
		return errInactivityMarkerWrite
	case isChainSyncStateKey(key):
		return s.LedgerDB.DeleteSyncState(key, txn)
	case !database.IsPParamsSyncMarker(key):
		return fmt.Errorf("%w: %q", errSyncStateKeyUnowned, key)
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		SyncMarkers: []SyncMarkerChange{{Name: key, Delete: true}},
	})
	return nil
}

// GetSyncStateBounded reads a chain-store sync_state key, refusing a value
// over maxBytes; no ledger-state key is read this way.
func (s stagingLedgerDB) GetSyncStateBounded(
	key string,
	maxBytes int,
	txn ledgerdb.Txn,
) (string, error) {
	if !isChainSyncStateKey(key) {
		return "", fmt.Errorf("%w: %q", errSyncStateKeyUnowned, key)
	}
	return s.LedgerDB.GetSyncStateBounded(key, maxBytes, txn)
}

// ListSyncStateKeysByPrefix lists the chain-store sync_state keys under
// prefix. Only the deferred header validation markers are listed this way.
func (s stagingLedgerDB) ListSyncStateKeysByPrefix(
	prefix string,
	txn ledgerdb.Txn,
) ([]string, error) {
	if prefix != deferredHeaderValidationSyncStatePrefix {
		return nil, fmt.Errorf("%w: prefix %q", errSyncStateKeyUnowned, prefix)
	}
	return s.LedgerDB.ListSyncStateKeysByPrefix(prefix, txn)
}

// deleteCursor accepts the deletion of a reward precompute cursor that txn
// has already removed with its epoch's outputs (DeleteRewardOutputsForEpoch
// stages both); the set keeps no cursor apart from the outputs it describes.
func (s stagingLedgerDB) deleteCursor(key string, txn ledgerdb.Txn) error {
	raw, err := s.getCursor(key, txn)
	if err != nil {
		return err
	}
	if raw != "" {
		return fmt.Errorf(
			"%w: delete the reward outputs of the cursor's epoch with it",
			errCursorDeleteAlone,
		)
	}
	return nil
}

var errCursorDeleteAlone = errors.New(
	"a reward precompute cursor is deleted only with its epoch's outputs",
)

// isChainSyncStateKey reports a sync_state key the ledger reads or writes
// that names chain data rather than ledger state: the deferred header
// validation markers and their eviction floor, the Mithril trust boundary
// and the consumed-UTxO prune floor of the transaction index. These stay
// in the SQL chain store with the blocks and the index they describe.
func isChainSyncStateKey(key string) bool {
	switch key {
	case deferredHeaderValidationFloorSyncStateKey,
		mithrilLedgerSlotSyncKey,
		mithrilLedgerHashSyncKey,
		database.ConsumedUtxoPruneFloorSyncKey:
		return true
	}
	return strings.HasPrefix(key, deferredHeaderValidationSyncStatePrefix)
}

// errSyncStateKeyUnowned refuses a sync_state key that names neither ledger
// state the LedgerDB set keeps nor chain data the SQL chain store holds.
var errSyncStateKeyUnowned = errors.New(
	"sync_state key is neither LedgerDB state nor chain data",
)
