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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// EpochSource answers the epoch and era switch reads of a database with no
// metadata store, from the live UTxO set.
type EpochSource interface {
	// Epochs returns every epoch row, in epoch order.
	Epochs() ([]models.Epoch, error)
	// EraSwitch returns the recorded mid-epoch era switch, nil when none.
	EraSwitch() (*MidEpochEraSwitch, error)
}

// ErrNoEpochSource is returned by a read or rollback of a database with no
// metadata store that needs epochs or the era switch when no EpochSource was
// installed.
var ErrNoEpochSource = errors.New(
	"the database keeps no epochs or era switch and no epoch source is installed",
)

// SetEpochSource installs the source epoch and era switch reads of a
// database with no metadata store come from.
func (d *Database) SetEpochSource(src EpochSource) {
	d.epochSource.Store(&src)
}

func (d *Database) epochSourceOrErr() (EpochSource, error) {
	p := d.epochSource.Load()
	if p == nil || *p == nil {
		return nil, ErrNoEpochSource
	}
	return *p, nil
}

func (d *Database) sourceEpochs() ([]models.Epoch, error) {
	src, err := d.epochSourceOrErr()
	if err != nil {
		return nil, err
	}
	return src.Epochs()
}

// sourceEpochBySlot picks the epoch that contains slot the way the metadata
// store does: the latest start at or before slot whose length covers it.
func sourceEpochBySlot(
	epochs []models.Epoch,
	slot uint64,
) *models.Epoch {
	var best *models.Epoch
	for i := range epochs {
		e := &epochs[i]
		if e.StartSlot <= slot &&
			slot < e.StartSlot+uint64(e.LengthInSlots) &&
			(best == nil || e.StartSlot > best.StartSlot) {
			best = e
		}
	}
	return best
}

// truncateEraSwitch returns the recorded era switch for a rollback: from the
// source when there is no metadata store, else from sync_state.
func (d *Database) truncateEraSwitch(txn *Txn) (*MidEpochEraSwitch, error) {
	if d.metadata == nil {
		src, err := d.epochSourceOrErr()
		if err != nil {
			return nil, err
		}
		return src.EraSwitch()
	}
	value, err := d.GetSyncState(MidEpochEraSwitchSyncKey, txn)
	if err != nil || value == "" {
		return nil, err
	}
	sw, err := ParseMidEpochEraSwitch(value)
	if err != nil {
		return nil, err
	}
	return &sw, nil
}

// newestEpochAtOrBefore returns the newest epoch whose start is at or before
// slot, which is the newest epoch a rollback to slot leaves.
func (d *Database) newestEpochAtOrBefore(
	slot uint64,
	txn *Txn,
) (uint64, bool, error) {
	epochs, err := d.GetEpochs(txn)
	if err != nil {
		return 0, false, fmt.Errorf("read epochs after rollback: %w", err)
	}
	var newest uint64
	found := false
	for _, e := range epochs {
		if e.StartSlot > slot {
			continue
		}
		if !found || e.EpochId > newest {
			newest, found = e.EpochId, true
		}
	}
	return newest, found, nil
}

// epochRecord reads one epoch row for the metadata-store paths that take a
// metadata transaction: from the source when there is no metadata store.
func (d *Database) epochRecord(
	epochID uint64,
	txn types.Txn,
) (*models.Epoch, error) {
	if d.metadata != nil {
		return d.metadata.GetEpoch(epochID, txn)
	}
	epochs, err := d.sourceEpochs()
	if err != nil {
		return nil, err
	}
	for i := range epochs {
		if epochs[i].EpochId == epochID {
			return &epochs[i], nil
		}
	}
	return nil, nil
}
