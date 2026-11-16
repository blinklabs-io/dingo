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
	"bytes"
	"slices"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/statedrain"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type stubNonceSource struct {
	nonce         []byte
	hasCheckpoint bool
}

func (s stubNonceSource) BlockNonceAt(ocommon.Point) ([]byte, error) {
	return s.nonce, nil
}

func (s stubNonceSource) HasCheckpointAtOrBefore(uint64) (bool, error) {
	return s.hasCheckpoint, nil
}

// newDrainedTruncateDB opens a database behind a statement guard, creates a
// Praos block as the truncate target, and latches and arms every family in
// families.
func newDrainedTruncateDB(
	t *testing.T,
	families ...nodesettings.Family,
) (*database.Database, *statedrain.TripGuard, ocommon.Point) {
	t.Helper()
	guard := statedrain.NewTripGuard()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	target := models.Block{
		ID:     1,
		Slot:   1500,
		Hash:   bytes.Repeat([]byte{0x15}, 32),
		Cbor:   []byte{0x80},
		Number: 1,
		Type:   babbage.BlockTypeBabbage,
	}
	require.NoError(t, db.BlockCreate(target, nil))
	for _, family := range families {
		require.NoError(t, db.RecordDrainedFamily(t.Context(), family))
		guard.Arm(family, statedrain.TableNames(family))
	}
	return db, guard, ocommon.Point{Slot: target.Slot, Hash: target.Hash}
}

func TestTruncateAfterSlotWritesNothingToDrainedTables(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		families []nodesettings.Family
	}{
		{"nonces", []nodesettings.Family{nodesettings.FamilyNonces}},
		{
			"network-donations",
			[]nodesettings.Family{nodesettings.FamilyNetworkDonations},
		},
		{"mir-genesis", []nodesettings.Family{nodesettings.FamilyMIRGenesis}},
		{
			"both",
			[]nodesettings.Family{
				nodesettings.FamilyNonces,
				nodesettings.FamilyNetworkDonations,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, guard, point := newDrainedTruncateDB(t, tc.families...)
			want := bytes.Repeat([]byte{0xc2}, 32)
			db.SetNonceSource(stubNonceSource{nonce: want, hasCheckpoint: true})
			noncesDrained := slices.Contains(
				tc.families, nodesettings.FamilyNonces,
			)
			if !noncesDrained {
				require.NoError(t, db.SetBlockNonce(
					point.Hash, point.Slot, want, true, nil,
				))
			}

			_, nonce, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

			require.NoError(t, err)
			require.Empty(t, guard.Trips(), "a drained table was touched")
			require.Equal(t, want, nonce)
		})
	}
}

// A rollback of a database whose epochs and summaries families are drained
// reads the newest remaining epoch from the source and touches neither table:
// no epoch delete, no summary delete and no read of the epoch rows.
func TestTruncateAfterSlotTouchesNeitherEpochTableOnceDrained(t *testing.T) {
	t.Parallel()
	db, guard, point := newDrainedTruncateDB(
		t, nodesettings.FamilySummaries, nodesettings.FamilyEpochs,
	)
	want := bytes.Repeat([]byte{0xc2}, 32)
	require.NoError(
		t, db.SetBlockNonce(point.Hash, point.Slot, want, true, nil),
	)
	db.SetEpochSource(fakeEpochSource{epochs: []models.Epoch{
		{EpochId: 1, StartSlot: 1000, LengthInSlots: 1000, EraId: 1},
		{EpochId: 2, StartSlot: 2000, LengthInSlots: 1000, EraId: 1},
	}})

	_, nonce, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

	require.NoError(t, err)
	require.Empty(t, guard.Trips(), "a drained table was touched")
	require.Equal(t, want, nonce)
	require.ErrorIs(
		t, db.SetEpoch(0, 3, nil, nil, nil, nil, 1, 1, 100, nil),
		database.ErrFamilyDrained,
	)
	require.Empty(t, guard.Trips(), "the refusal must not reach the table")
}

func TestTruncateAfterSlotWithoutNonceSourceFailsWhenNoncesDrained(
	t *testing.T,
) {
	t.Parallel()
	db, guard, point := newDrainedTruncateDB(t, nodesettings.FamilyNonces)

	_, _, err := db.TruncateAfterSlot(t.Context(), point, 0, nil)

	require.ErrorIs(t, err, database.ErrNoNonceSource)
	require.Empty(t, guard.Trips(), "the refusal must not read the table")
}

func TestStatementGuardTripsOnAWriteToADrainedTable(t *testing.T) {
	t.Parallel()
	db, guard, point := newDrainedTruncateDB(
		t,
		nodesettings.FamilyNetworkDonations,
	)

	err := db.Metadata().AddNetworkDonation(point.Slot, 3, 10, nil)

	require.ErrorIs(t, err, statedrain.ErrDrainedWrite)
	trips := guard.Trips()
	require.Len(t, trips, 1)
	require.True(t, trips[0].Write)
	require.Equal(t, "network_donation", trips[0].Table)
}

// The drain's chunked delete is the one writer an armed guard lets through,
// in each of the statement shapes it issues: the bound lookup with the
// bounded delete, and the unbounded delete of a short table.
func TestStatementGuardAllowsTheDrainsChunkedDelete(t *testing.T) {
	t.Parallel()
	guard := statedrain.NewTripGuard()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config:         &database.Config{DataDir: t.TempDir()},
		StatementGuard: guard,
	})
	require.NoError(t, err)
	for i := range 3 {
		require.NoError(t, db.SetBlockNonce(
			bytes.Repeat([]byte{byte(i + 1)}, 32), uint64(i+1),
			bytes.Repeat([]byte{0xaa}, 32), false, nil,
		))
	}
	guard.Arm(
		nodesettings.FamilyNonces,
		statedrain.TableNames(nodesettings.FamilyNonces),
	)

	deleted, err := db.DeleteLedgerStateRows(
		t.Context(),
		"block_nonce",
		"id",
		2,
	)
	require.NoError(t, err)
	require.EqualValues(t, 2, deleted, "bounded delete")
	deleted, err = db.DeleteLedgerStateRows(
		t.Context(),
		"block_nonce",
		"id",
		100,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted, "unbounded delete")

	require.Empty(t, guard.Trips())
}
