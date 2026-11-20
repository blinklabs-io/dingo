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

func (s stagingLedgerDB) pparamsDrained() bool {
	return s.ls.familyDrained(nodesettings.FamilyPParamsUpdates)
}

// drainedPParamsSource serves the database's own protocol parameter reads,
// and a rollback's, from the committed rows of the set once the
// pparams-updates family is drained.
type drainedPParamsSource struct {
	ls *LedgerState
}

func (d drainedPParamsSource) pparams() (PParamsReader, error) {
	r, ok := d.ls.readSet(nil).(PParamsReader)
	if !ok {
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	return r, nil
}

func (d drainedPParamsSource) updates() (PParamUpdateReader, error) {
	r, ok := d.ls.readSet(nil).(PParamUpdateReader)
	if !ok {
		return nil, drainedUnserved(nodesettings.FamilyPParamsUpdates)
	}
	return r, nil
}

func (d drainedPParamsSource) markers() (SyncMarkerReader, error) {
	r, ok := d.ls.readSet(nil).(SyncMarkerReader)
	if !ok {
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
	if ls.db != nil && ls.keepsEpochState() {
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
	if !ok {
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

// GetSyncState reads a protocol parameter sync marker from the set once the
// pparams-updates family is drained.
func (s stagingLedgerDB) GetSyncState(
	key string,
	txn ledgerdb.Txn,
) (string, error) {
	if database.IsPParamsSyncMarker(key) && s.pparamsDrained() {
		return s.ls.syncMarkerView(txn, key)
	}
	if isRewardCursorKey(key) {
		return s.getCursor(key, txn)
	}
	return s.LedgerDB.GetSyncState(key, txn)
}

// SetSyncState stages a write of a protocol parameter sync marker for the
// set, and skips the metadata store's write once the family is drained.
func (s stagingLedgerDB) SetSyncState(
	key, value string,
	txn ledgerdb.Txn,
) error {
	if isRewardCursorKey(key) {
		if !s.rewardOutputsDrained() {
			if err := s.LedgerDB.SetSyncState(key, value, txn); err != nil {
				return err
			}
		}
		s.cursorWrite(key, value, txn)
		return nil
	}
	if !database.IsPParamsSyncMarker(key) {
		return s.LedgerDB.SetSyncState(key, value, txn)
	}
	if !s.pparamsDrained() {
		if err := s.LedgerDB.SetSyncState(key, value, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		SyncMarkers: []SyncMarkerChange{{Name: key, Value: value}},
	})
	return nil
}

// DeleteSyncState stages the deletion of a protocol parameter sync marker
// for the set, and skips the metadata store's delete once the family is
// drained.
func (s stagingLedgerDB) DeleteSyncState(
	key string,
	txn ledgerdb.Txn,
) error {
	if !database.IsPParamsSyncMarker(key) {
		return s.LedgerDB.DeleteSyncState(key, txn)
	}
	if !s.pparamsDrained() {
		if err := s.LedgerDB.DeleteSyncState(key, txn); err != nil {
			return err
		}
	}
	s.ls.stageLedgerChanges(txn, LedgerChanges{
		SyncMarkers: []SyncMarkerChange{{Name: key, Delete: true}},
	})
	return nil
}
