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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// seedBoundaryEpochs stores epochs 9 (Babbage) and 10-12 (Conway), each 100
// slots long, with epoch N starting at slot (N-9)*100.
func seedBoundaryEpochs(t *testing.T, db *Database) {
	t.Helper()
	for epoch := uint64(9); epoch <= 12; epoch++ {
		era := uint(ledger.EraIdConway)
		if epoch == 9 {
			era = ledger.EraIdBabbage
		}
		require.NoError(t, db.SetEpoch(
			(epoch-9)*100, epoch, nil, nil, nil, nil, era, 1000, 100, nil,
		))
	}
}

func saveBoundary(t *testing.T, db *Database, epoch uint64) {
	t.Helper()
	require.NoError(t, db.SaveGovernanceBoundarySnapshot(
		t.Context(),
		&models.GovernanceBoundarySnapshot{
			Epoch:        epoch,
			BoundarySlot: (epoch - 9) * 100,
		},
		nil,
	))
}

func TestGovernanceBoundarySnapshotAtSlotRejectsOtherEpoch(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	seedBoundaryEpochs(t, db)
	// Epoch 11 has no row, as after an upgrade that passed its boundary.
	saveBoundary(t, db, 10)
	saveBoundary(t, db, 12)

	for _, slot := range []uint64{200, 250, 299} {
		snapshot, err := db.GetGovernanceBoundarySnapshotAtSlot(
			t.Context(), slot, nil,
		)
		require.ErrorIs(
			t, err, models.ErrGovernanceBoundarySnapshotNotRetained,
			"slot %d", slot,
		)
		require.Nil(t, snapshot)
	}
}

func TestGovernanceBoundarySnapshotAtSlotCompleteEpochs(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	seedBoundaryEpochs(t, db)
	saveBoundary(t, db, 10)
	saveBoundary(t, db, 11)
	saveBoundary(t, db, 12)

	for slot, epoch := range map[uint64]uint64{
		100: 10, 199: 10, 200: 11, 299: 11, 300: 12, 399: 12,
	} {
		snapshot, err := db.GetGovernanceBoundarySnapshotAtSlot(
			t.Context(), slot, nil,
		)
		require.NoError(t, err, "slot %d", slot)
		require.NotNil(t, snapshot, "slot %d", slot)
		require.Equal(t, epoch, snapshot.Epoch, "slot %d", slot)
	}

	// Pre-Conway epochs have no boundary snapshot and no error.
	snapshot, err := db.GetGovernanceBoundarySnapshotAtSlot(
		t.Context(), 50, nil,
	)
	require.NoError(t, err)
	require.Nil(t, snapshot)
}

func TestGovernanceBoundarySnapshotAtSlotBeyondLastEpoch(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	seedBoundaryEpochs(t, db)
	saveBoundary(t, db, 11)

	// The newest row predates the last stored epoch, so it cannot describe
	// a slot past that epoch.
	_, err := db.GetGovernanceBoundarySnapshotAtSlot(t.Context(), 5000, nil)
	require.ErrorIs(t, err, models.ErrGovernanceBoundarySnapshotNotRetained)

	saveBoundary(t, db, 12)
	snapshot, err := db.GetGovernanceBoundarySnapshotAtSlot(
		t.Context(), 5000, nil,
	)
	require.NoError(t, err)
	require.EqualValues(t, 12, snapshot.Epoch)
}
