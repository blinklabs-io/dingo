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
	"fmt"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// GovernanceBoundaryReader serves the Conway epoch boundary snapshots from a
// live UTxO set that keeps them.
type GovernanceBoundaryReader interface {
	// GovernanceBoundarySnapshotAt returns the boundary snapshot of epoch as
	// a rollback to *at would leave it, or as it is now when at is nil.
	GovernanceBoundarySnapshotAt(
		epoch uint64,
		at *uint64,
	) (*models.GovernanceBoundarySnapshot, bool, error)
}

// governanceBoundarySeam stores boundary snapshots for a test ledger that
// has no live UTxO set keeping ledger state.
type governanceBoundarySeam interface {
	GovernanceBoundaryReader
	save(*models.GovernanceBoundarySnapshot)
}

// governanceBoundaryReader returns the set's boundary snapshots, or nil when
// the ledger's set keeps no ledger state or has stopped following it.
func (ls *LedgerState) governanceBoundaryReader() GovernanceBoundaryReader {
	if ls.boundarySnapshotSeam != nil {
		return ls.boundarySnapshotSeam
	}
	if !ls.keepsEpochState() || ls.utxoSetFailed.Load() {
		return nil
	}
	r, ok := ls.config.UtxoSet.(GovernanceBoundaryReader)
	if !ok {
		return nil
	}
	return r
}

// saveGovernanceBoundarySnapshot stages snapshot for the live UTxO set in
// txn, journaled at its boundary slot so that a rollback below the boundary
// removes it. Without txn it is applied at once. A ledger whose set keeps no
// ledger state retains no boundary snapshots.
func (ls *LedgerState) saveGovernanceBoundarySnapshot(
	snapshot *models.GovernanceBoundarySnapshot,
	txn ledgerdb.Txn,
) error {
	if snapshot == nil {
		return nil
	}
	if ls.boundarySnapshotSeam != nil {
		ls.boundarySnapshotSeam.save(snapshot)
		return nil
	}
	if !ls.keepsEpochState() {
		return nil
	}
	ls.stageLedgerChanges(txn, LedgerChanges{
		GovernanceBoundaries: []models.GovernanceBoundarySnapshot{*snapshot},
	})
	return nil
}

// GovernanceBoundarySnapshotAtSlot returns the boundary snapshot of the epoch
// holding slot as the chain stood at slot, which is what a local state query
// acquired at slot reads. It returns nil for a slot before any epoch or in a
// pre-Conway epoch. For a Conway-or-later epoch whose snapshot the live UTxO
// set does not hold, because the boundary lies below the rollback window or
// was never rebuilt, it returns an error wrapping
// models.ErrGovernanceBoundarySnapshotNotRetained; callers must not
// substitute another epoch's snapshot or live voting state.
func (ls *LedgerState) GovernanceBoundarySnapshotAtSlot(
	slot uint64,
) (*models.GovernanceBoundarySnapshot, error) {
	epochs, err := ls.ledgerDB().GetEpochs(nil)
	if err != nil {
		return nil, fmt.Errorf("list epochs for slot %d: %w", slot, err)
	}
	var epoch *models.Epoch
	for i := range epochs {
		e := &epochs[i]
		if e.StartSlot <= slot &&
			(epoch == nil || e.StartSlot > epoch.StartSlot) {
			epoch = e
		}
	}
	if epoch == nil || epoch.EraId < eras.ConwayEraDesc.Id {
		return nil, nil
	}
	notRetained := fmt.Errorf(
		"%w %d (slot %d)",
		models.ErrGovernanceBoundarySnapshotNotRetained,
		epoch.EpochId,
		slot,
	)
	if slot >= epoch.StartSlot+uint64(epoch.LengthInSlots) {
		// The epoch holding slot has not begun.
		return nil, notRetained
	}
	r := ls.governanceBoundaryReader()
	if r == nil {
		return nil, notRetained
	}
	snapshot, ok, err := r.GovernanceBoundarySnapshotAt(epoch.EpochId, &slot)
	if err != nil {
		return nil, fmt.Errorf(
			"read governance boundary snapshot of epoch %d: %w",
			epoch.EpochId, err,
		)
	}
	if !ok || snapshot.Epoch != epoch.EpochId {
		return nil, notRetained
	}
	return snapshot, nil
}
