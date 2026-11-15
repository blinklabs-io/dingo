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

package database_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

type fakeEpochSource struct {
	epochs []models.Epoch
	sw     *database.MidEpochEraSwitch
}

func (s fakeEpochSource) Epochs() ([]models.Epoch, error) {
	return s.epochs, nil
}

func (s fakeEpochSource) EraSwitch() (*database.MidEpochEraSwitch, error) {
	return s.sw, nil
}

func newDrainedEpochsDatabase(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	// The metadata store keeps a row the source does not, so an answer from
	// it would differ.
	require.NoError(t, db.SetEpoch(0, 7, nil, nil, nil, nil, 1, 1, 100, nil))
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyEpochs,
	))
	return db
}

// A drained epochs family answers every epoch read from the installed source
// and fails closed without one.
func TestDrainedEpochReadsComeFromTheSource(t *testing.T) {
	t.Parallel()
	db := newDrainedEpochsDatabase(t)

	_, err := db.GetEpochs(nil)
	require.ErrorIs(t, err, database.ErrNoEpochSource)
	_, err = db.GetEpoch(1, nil)
	require.ErrorIs(t, err, database.ErrNoEpochSource)
	_, err = db.GetEpochBySlot(5, nil)
	require.ErrorIs(t, err, database.ErrNoEpochSource)
	_, err = db.GetEpochsByEra(1, nil)
	require.ErrorIs(t, err, database.ErrNoEpochSource)

	db.SetEpochSource(fakeEpochSource{epochs: []models.Epoch{
		{EpochId: 1, StartSlot: 0, LengthInSlots: 100, EraId: 1},
		{EpochId: 2, StartSlot: 100, LengthInSlots: 100, EraId: 2},
		{EpochId: 3, StartSlot: 200, LengthInSlots: 100, EraId: 2},
	}})
	epochs, err := db.GetEpochs(nil)
	require.NoError(t, err)
	require.Len(t, epochs, 3)
	one, err := db.GetEpoch(2, nil)
	require.NoError(t, err)
	require.NotNil(t, one)
	require.Equal(t, uint64(100), one.StartSlot)
	missing, err := db.GetEpoch(7, nil)
	require.NoError(t, err)
	require.Nil(t, missing, "the SQL row of epoch 7 is not consulted")
	for slot, want := range map[uint64]uint64{0: 1, 99: 1, 100: 2, 250: 3} {
		got, err := db.GetEpochBySlot(slot, nil)
		require.NoError(t, err)
		require.NotNil(t, got, slot)
		require.Equal(t, want, got.EpochId, slot)
	}
	beyond, err := db.GetEpochBySlot(300, nil)
	require.NoError(t, err)
	require.Nil(t, beyond, "no epoch covers a slot past the last one")
	byEra, err := db.GetEpochsByEra(2, nil)
	require.NoError(t, err)
	require.Len(t, byEra, 2)
}

// A write to a drained family is refused, so no SQL row reappears beside the
// set.
func TestDrainedEpochWritesAreRefused(t *testing.T) {
	t.Parallel()
	db := newDrainedEpochsDatabase(t)
	require.ErrorIs(
		t,
		db.SetEpoch(0, 8, nil, nil, nil, nil, 1, 1, 100, nil),
		database.ErrFamilyDrained,
	)
	require.ErrorIs(
		t, db.DeleteEpochsAfterSlot(0, nil), database.ErrFamilyDrained,
	)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyEraSwitch,
	))
	require.ErrorIs(
		t,
		db.SetMidEpochEraSwitch(database.MidEpochEraSwitch{}, nil),
		database.ErrFamilyDrained,
	)
	key, err := db.GetSyncState(database.MidEpochEraSwitchSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, key)
}
