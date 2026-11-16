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

package dblifecycle

import (
	"errors"
	"fmt"
	"os"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// OpenUtxoSet opens the live UTxO set under dataDir for an offline lifecycle
// operation. It returns a nil store when the directory does not exist, so a
// node that never kept the set is unaffected. The directory is locked by a
// running node, which makes the open fail instead of sharing it.
func OpenUtxoSet(dataDir string) (*badgerutxo.Store, error) {
	dir := badgerutxo.DirFor(dataDir)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("inspect live UTxO set directory: %w", err)
	}
	store, err := badgerutxo.Open(dir)
	if err != nil {
		return nil, fmt.Errorf("open live UTxO set: %w", err)
	}
	return store, nil
}

// TruncateUtxoSet adapts a live UTxO set store to lifecycle.TruncateUtxoSet.
type TruncateUtxoSet struct{ Store *badgerutxo.Store }

var (
	_ lifecycle.TruncateUtxoSet = TruncateUtxoSet{}
	_ database.NonceSource      = TruncateUtxoSet{}
	_ database.EpochSource      = TruncateUtxoSet{}
	_ database.PParamsSource    = TruncateUtxoSet{}
)

// Tip implements lifecycle.TruncateUtxoSet.
func (s TruncateUtxoSet) Tip() (ocommon.Point, bool, error) {
	return s.Store.Tip()
}

// JournalFloor implements lifecycle.TruncateUtxoSet.
func (s TruncateUtxoSet) JournalFloor() (ocommon.Point, bool, error) {
	return s.Store.JournalFloor()
}

// CanRollbackTo implements lifecycle.TruncateUtxoSet.
func (s TruncateUtxoSet) CanRollbackTo(point ocommon.Point) (bool, error) {
	return s.Store.HasJournalPoint(point)
}

// RollbackTo implements lifecycle.TruncateUtxoSet.
func (s TruncateUtxoSet) RollbackTo(point ocommon.Point) error {
	_, err := s.Store.Rollback(point)
	return err
}

// BlockNonceAt implements database.NonceSource from the set's nonces, nil
// when it holds none for point.
func (s TruncateUtxoSet) BlockNonceAt(point ocommon.Point) ([]byte, error) {
	nonce, ok, err := s.Store.Nonce(point)
	if err != nil || !ok {
		return nil, err
	}
	return nonce, nil
}

// HasCheckpointAtOrBefore implements database.NonceSource: whether the set
// holds a checkpoint nonce of full length at or before slot.
func (s TruncateUtxoSet) HasCheckpointAtOrBefore(slot uint64) (bool, error) {
	nonces, err := s.Store.NoncesInSlotRange(0, slot+1)
	if err != nil {
		return false, err
	}
	for _, n := range nonces {
		if n.Checkpoint && len(n.Nonce) == lcommon.Blake2b256Size {
			return true, nil
		}
	}
	return false, nil
}

// Epochs implements database.EpochSource from the set's epoch rows.
func (s TruncateUtxoSet) Epochs() ([]models.Epoch, error) {
	rows, err := s.Store.Epochs()
	if err != nil {
		return nil, err
	}
	return utxoset.DecodeEpochRows(rows)
}

// EraSwitch implements database.EpochSource from the set's recorded switch.
func (s TruncateUtxoSet) EraSwitch() (*database.MidEpochEraSwitch, error) {
	sw, err := s.Store.EraSwitch()
	if err != nil || sw == nil {
		return nil, err
	}
	return &database.MidEpochEraSwitch{
		Epoch:   sw.Epoch,
		Slot:    sw.Slot,
		FromEra: uint(sw.FromEra),
		ToEra:   uint(sw.ToEra),
	}, nil
}

// PParamsLoaded implements database.PParamsSource.
func (s TruncateUtxoSet) PParamsLoaded() (bool, error) {
	return s.Store.PParamsLoaded()
}

// PParamsForEra implements database.PParamsSource from the set's rows.
func (s TruncateUtxoSet) PParamsForEra(era uint) ([]models.PParams, error) {
	rows, err := s.Store.PParamsForEra(uint64(era))
	if err != nil {
		return nil, err
	}
	return utxoset.DecodePParamsRows(rows), nil
}

// PParamUpdatesLoaded implements database.PParamsSource.
func (s TruncateUtxoSet) PParamUpdatesLoaded() (bool, error) {
	return s.Store.PParamUpdatesLoaded()
}

// PParamUpdates implements database.PParamsSource from the set's proposals.
func (s TruncateUtxoSet) PParamUpdates(
	fn func(models.PParamUpdate) error,
) error {
	return utxoset.EachPParamUpdate(s.Store, fn)
}

// SyncMarkersLoaded implements database.PParamsSource.
func (s TruncateUtxoSet) SyncMarkersLoaded() (bool, error) {
	return s.Store.MarkersLoaded()
}

// SyncMarker implements database.PParamsSource from the set's markers.
func (s TruncateUtxoSet) SyncMarker(name string) (string, bool, error) {
	return s.Store.Marker(name)
}

// TruncateOptions returns the options that make a truncate of the database
// under dataDir roll its live UTxO set back too, and the function that closes
// the set. With no set directory it returns no options.
func TruncateOptions(
	dataDir string,
) ([]lifecycle.TruncateOption, func() error, error) {
	store, err := OpenUtxoSet(dataDir)
	if err != nil || store == nil {
		return nil, func() error { return nil }, err
	}
	return []lifecycle.TruncateOption{
		lifecycle.WithTruncateUtxoSet(TruncateUtxoSet{Store: store}),
	}, store.Close, nil
}

// SnapshotOptions returns the option that makes an offline snapshot of the
// database under dataDir include its live UTxO set, and the function that
// closes the set. With no set directory it returns no options.
func SnapshotOptions(
	dataDir string,
) ([]lifecycle.ManifestOption, func() error, error) {
	store, err := OpenUtxoSet(dataDir)
	if err != nil || store == nil {
		return nil, func() error { return nil }, err
	}
	return []lifecycle.ManifestOption{
		lifecycle.WithUtxoSetBackup(store),
	}, store.Close, nil
}
