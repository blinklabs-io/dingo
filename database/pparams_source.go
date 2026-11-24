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
	"errors"
	"slices"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// PParamsSource answers the protocol parameter, update proposal and sync
// marker reads of a database with no metadata store, from the live UTxO
// set. Its rows are the committed ones, in the order written,
// without row IDs.
type PParamsSource interface {
	PParamsLoaded() (bool, error)
	// PParamsForEra returns era's rows in the order they were written.
	PParamsForEra(era uint) ([]models.PParams, error)
	PParamUpdatesLoaded() (bool, error)
	// PParamUpdates calls fn with every proposal in the order stored.
	PParamUpdates(fn func(models.PParamUpdate) error) error
	SyncMarkersLoaded() (bool, error)
	// SyncMarker returns the sync marker name and whether it is set.
	SyncMarker(name string) (string, bool, error)
}

// ErrNoPParamsSource is returned by a protocol parameter read of a database
// with no metadata store when no PParamsSource was installed.
var ErrNoPParamsSource = errors.New(
	"the database keeps no protocol parameters and no source is installed",
)

// ErrPParamsSourceNotLoaded is returned when the installed PParamsSource does
// not hold every protocol parameter row.
var ErrPParamsSourceNotLoaded = errors.New(
	"the protocol parameter source does not hold them all",
)

// SetPParamsSource installs the source the protocol parameter reads of a
// database with no metadata store come from.
func (d *Database) SetPParamsSource(src PParamsSource) {
	d.pparamsSourceLoaded.Store(false)
	d.pparamsSource.Store(&src)
}

func (d *Database) pparamsFromSource() bool {
	return d.metadata == nil
}

func (d *Database) pparamsSourceOrErr() (PParamsSource, error) {
	p := d.pparamsSource.Load()
	if p == nil || *p == nil {
		return nil, ErrNoPParamsSource
	}
	src := *p
	if d.pparamsSourceLoaded.Load() {
		return src, nil
	}
	for _, loaded := range []func() (bool, error){
		src.PParamsLoaded, src.PParamUpdatesLoaded, src.SyncMarkersLoaded,
	} {
		ok, err := loaded()
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, ErrPParamsSourceNotLoaded
		}
	}
	d.pparamsSourceLoaded.Store(true)
	return src, nil
}

// IsPParamsSyncMarker reports whether key is a sync_state key the
// pparams-updates family owns.
func IsPParamsSyncMarker(key string) bool {
	return key == SyntheticV2CostModelSyncKey ||
		key == SyntheticV2CostModelClearedEpochSyncKey
}

func (d *Database) pparamsSourceSyncMarker(key string) (string, error) {
	src, err := d.pparamsSourceOrErr()
	if err != nil {
		return "", err
	}
	v, _, err := src.SyncMarker(key)
	return v, err
}

// sourcePParamRows is the metadata store's GetPParams over the source: the
// row of era with the latest epoch at or before epoch, and of that epoch's
// rows the last written; none when era has no such row.
func (d *Database) sourcePParamRows(
	epoch uint64,
	era uint,
) ([]models.PParams, error) {
	src, err := d.pparamsSourceOrErr()
	if err != nil {
		return nil, err
	}
	rows, err := src.PParamsForEra(era)
	if err != nil {
		return nil, err
	}
	var best *models.PParams
	for i := range rows {
		if rows[i].Epoch <= epoch &&
			(best == nil || rows[i].Epoch >= best.Epoch) {
			best = &rows[i]
			best.ID = uint(i) + 1
		}
	}
	if best == nil {
		return nil, nil
	}
	return []models.PParams{*best}, nil
}

func (d *Database) sourceListPParamsForEra(
	era uint,
) ([]models.PParams, error) {
	src, err := d.pparamsSourceOrErr()
	if err != nil {
		return nil, err
	}
	rows, err := src.PParamsForEra(era)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].ID = uint(i) + 1
	}
	return rows, nil
}

func (d *Database) sourceListPParamUpdates() ([]models.PParamUpdate, error) {
	src, err := d.pparamsSourceOrErr()
	if err != nil {
		return nil, err
	}
	var rows []models.PParamUpdate
	err = src.PParamUpdates(func(u models.PParamUpdate) error {
		u.GenesisHash = slices.Clone(u.GenesisHash)
		u.Cbor = slices.Clone(u.Cbor)
		u.ID = uint(len(rows)) + 1
		rows = append(rows, u)
		return nil
	})
	return rows, err
}

// sourcePParamUpdates is the metadata store's GetPParamUpdates over the
// source: the proposals targeting epoch or the epoch before it, newest first.
func (d *Database) sourcePParamUpdates(
	epoch uint64,
) ([]models.PParamUpdate, error) {
	rows, err := d.sourceListPParamUpdates()
	if err != nil {
		return nil, err
	}
	previous := epoch
	if epoch > 0 {
		previous--
	}
	rows = slices.DeleteFunc(rows, func(u models.PParamUpdate) bool {
		return u.Epoch != epoch && u.Epoch != previous
	})
	slices.Reverse(rows)
	return rows, nil
}

// getPParamUpdates reads the proposals for epoch from the metadata store, or
// from the source when there is no metadata store.
func (d *Database) getPParamUpdates(
	epoch uint64,
	txn types.Txn,
) ([]models.PParamUpdate, error) {
	if d.pparamsFromSource() {
		return d.sourcePParamUpdates(epoch)
	}
	return d.metadata.GetPParamUpdates(epoch, txn)
}

// GetPParamUpdates returns the protocol parameter update proposals that
// target epoch or the epoch before it, newest first.
func (d *Database) GetPParamUpdates(
	epoch uint64,
	txn *Txn,
) ([]models.PParamUpdate, error) {
	return d.getPParamUpdates(epoch, metadataOf(txn))
}

// ListPParamUpdates returns every stored protocol parameter update proposal,
// oldest first.
func (d *Database) ListPParamUpdates(txn *Txn) ([]models.PParamUpdate, error) {
	if d.pparamsFromSource() {
		return d.sourceListPParamUpdates()
	}
	if err := d.requireStore("ListPParamUpdates"); err != nil {
		return nil, err
	}
	return d.metadata.ListPParamUpdates(metadataOf(txn))
}

// ListPParamsForEra returns every stored protocol parameter row of era in
// the order written.
func (d *Database) ListPParamsForEra(
	era uint,
	txn *Txn,
) ([]models.PParams, error) {
	if d.pparamsFromSource() {
		return d.sourceListPParamsForEra(era)
	}
	if err := d.requireStore("ListPParamsForEra"); err != nil {
		return nil, err
	}
	return d.metadata.ListPParamsForEra(era, metadataOf(txn))
}

// GetPParamRows returns the protocol parameter row GetPParams decodes: the
// latest one of era at or before epoch.
func (d *Database) GetPParamRows(
	epoch uint64,
	era uint,
	txn *Txn,
) ([]models.PParams, error) {
	if d.pparamsFromSource() {
		return d.sourcePParamRows(epoch, era)
	}
	if err := d.requireStore("GetPParamRows"); err != nil {
		return nil, err
	}
	return d.metadata.GetPParams(epoch, era, metadataOf(txn))
}

func metadataOf(txn *Txn) types.Txn {
	if txn == nil {
		return nil
	}
	return txn.Metadata()
}
