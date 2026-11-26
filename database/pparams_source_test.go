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
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

type fakePParamsSource struct {
	loaded  bool
	rows    map[uint][]models.PParams
	updates []models.PParamUpdate
	markers map[string]string
}

func (s fakePParamsSource) PParamsLoaded() (bool, error) { return s.loaded, nil }

func (s fakePParamsSource) PParamsForEra(era uint) ([]models.PParams, error) {
	return append([]models.PParams(nil), s.rows[era]...), nil
}

func (s fakePParamsSource) PParamUpdatesLoaded() (bool, error) {
	return s.loaded, nil
}

func (s fakePParamsSource) PParamUpdates(
	fn func(models.PParamUpdate) error,
) error {
	for _, u := range s.updates {
		if err := fn(u); err != nil {
			return err
		}
	}
	return nil
}

func (s fakePParamsSource) SyncMarkersLoaded() (bool, error) {
	return s.loaded, nil
}

func (s fakePParamsSource) SyncMarker(name string) (string, bool, error) {
	v, ok := s.markers[name]
	return v, ok, nil
}

// newLedgerPParamsDatabase opens the ledger's database, whose protocol
// parameters, proposals and markers are LedgerDB records it reads from the
// installed source.
func newLedgerPParamsDatabase(t *testing.T) *Database {
	t.Helper()
	db, err := newTestDatabase(t, &Config{DataDir: ""})
	require.NoError(t, err)
	require.Nil(t, db.Metadata())
	return db
}

// The ledger's database answers every protocol parameter, proposal and marker
// read from the installed source, and fails closed without one or while it
// does not hold the family.
func TestPParamReadsComeFromTheSource(t *testing.T) {
	t.Parallel()
	db := newLedgerPParamsDatabase(t)
	decode := func(b []byte) (lcommon.ProtocolParameters, error) {
		return nil, nil
	}

	_, err := db.GetPParamRows(1, 5, nil)
	require.ErrorIs(t, err, ErrNoPParamsSource)
	_, err = db.GetPParams(1, 5, decode, nil)
	require.ErrorIs(t, err, ErrNoPParamsSource)
	_, err = db.ListPParamUpdates(nil)
	require.ErrorIs(t, err, ErrNoPParamsSource)
	_, err = db.GetSyncState(SyntheticV2CostModelSyncKey, nil)
	require.ErrorIs(t, err, ErrNoPParamsSource)

	src := fakePParamsSource{
		rows: map[uint][]models.PParams{
			5: {
				{Epoch: 1, AddedSlot: 10, EraId: 5, Cbor: []byte{1}},
				{Epoch: 3, AddedSlot: 30, EraId: 5, Cbor: []byte{2}},
				{Epoch: 3, AddedSlot: 31, EraId: 5, Cbor: []byte{3}},
			},
		},
		updates: []models.PParamUpdate{
			{
				GenesisHash: []byte{1},
				Cbor:        []byte{0xa1},
				AddedSlot:   5,
				Epoch:       0,
			},
			{
				GenesisHash: []byte{2},
				Cbor:        []byte{0xa2},
				AddedSlot:   15,
				Epoch:       1,
			},
			{
				GenesisHash: []byte{3},
				Cbor:        []byte{0xa3},
				AddedSlot:   25,
				Epoch:       2,
			},
			{
				GenesisHash: []byte{4},
				Cbor:        []byte{0xa4},
				AddedSlot:   35,
				Epoch:       5,
			},
		},
		markers: map[string]string{SyntheticV2CostModelSyncKey: "false"},
	}
	db.SetPParamsSource(src)
	_, err = db.GetPParamRows(1, 5, nil)
	require.ErrorIs(t, err, ErrPParamsSourceNotLoaded)

	src.loaded = true
	db.SetPParamsSource(src)
	var picked []byte
	_, err = db.GetPParams(
		5,
		5,
		func(b []byte) (lcommon.ProtocolParameters, error) {
			picked = b
			return nil, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{3}, picked, "the last row of the latest epoch")
	for epoch, want := range map[uint64]byte{1: 1, 2: 1, 3: 3} {
		rows, err := db.GetPParamRows(epoch, 5, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1, epoch)
		require.Equal(t, []byte{want}, rows[0].Cbor, epoch)
	}
	rows, err := db.GetPParamRows(0, 5, nil)
	require.NoError(t, err)
	require.Empty(t, rows, "no row at or before epoch 0")
	rows, err = db.GetPParamRows(9, 4, nil)
	require.NoError(t, err)
	require.Empty(t, rows, "no row in the era")
	listed, err := db.ListPParamsForEra(5, nil)
	require.NoError(t, err)
	require.Len(t, listed, 3)
	require.Equal(t, []byte{1}, listed[0].Cbor)

	all, err := db.ListPParamUpdates(nil)
	require.NoError(t, err)
	require.Len(t, all, 4)
	require.Equal(t, []byte{0xa1}, all[0].Cbor, "oldest first")
	updates, err := db.GetPParamUpdates(2, nil)
	require.NoError(t, err)
	require.Len(t, updates, 2, "the proposals of epochs 2 and 1")
	require.Equal(t, []byte{0xa3}, updates[0].Cbor, "newest first")
	require.Equal(t, []byte{0xa2}, updates[1].Cbor)
	require.Greater(t, updates[0].ID, updates[1].ID)
	updates, err = db.GetPParamUpdates(0, nil)
	require.NoError(t, err)
	require.Len(t, updates, 1, "epoch 0 has no epoch before it")

	v, err := db.GetSyncState(SyntheticV2CostModelSyncKey, nil)
	require.NoError(t, err)
	require.Equal(t, "false", v)
	_, cleared, err := SyntheticV2CostModelClearedEpoch(db, nil)
	require.NoError(t, err)
	require.False(t, cleared, "the source holds no cleared epoch")
}

// A protocol parameter write to the ledger's database is refused, so no SQL
// row appears beside the set, and a rollback leaves the markers to the set.
func TestPParamWritesAreRefused(t *testing.T) {
	t.Parallel()
	db := newLedgerPParamsDatabase(t)
	ctx := context.Background()
	require.ErrorIs(
		t, db.SetPParams([]byte{1}, 2, 2, 5, nil), ErrNoMetadataStore,
	)
	require.ErrorIs(
		t, db.SetPParamUpdate([]byte{1}, []byte{1}, 2, 2, nil),
		ErrNoMetadataStore,
	)
	require.ErrorIs(
		t, db.DeletePParamsAfterSlot(ctx, 0, nil), ErrNoMetadataStore,
	)
	require.ErrorIs(
		t, db.DeletePParamUpdatesAfterSlot(ctx, 0, nil), ErrNoMetadataStore,
	)
	for _, key := range []string{
		SyntheticV2CostModelSyncKey, SyntheticV2CostModelClearedEpochSyncKey,
	} {
		require.ErrorIs(
			t, db.SetSyncState(key, "1", nil), ErrNoMetadataStore,
		)
		require.ErrorIs(t, db.DeleteSyncState(key, nil), ErrNoMetadataStore)
	}
	// Every other sync key is refused too: there is no sync_state.
	require.ErrorIs(t, db.SetSyncState("other", "1", nil), ErrNoMetadataStore)
	// The database has no epoch to place the slot in and no source; a
	// rollback that read the marker would fail.
	require.NoError(
		t, RecomputeSyntheticV2CostModelMarkerAfterTruncate(db, nil, 0),
	)
}

// A per-byte Alonzo unit marker on the ledger's database, whose protocol
// parameters have no rows, is refused, not read as "no Alonzo rows left".
func TestAlonzoUnitMarkerWithoutRowsIsNotReclassified(t *testing.T) {
	t.Parallel()
	db := newLedgerPParamsDatabase(t)
	ctx := context.Background()
	require.NoError(t, db.SetNodeSettingsGates(ctx,
		nodesettings.Values{
			nodesettings.AlonzoPParamsUnitGateName: nodesettings.AlonzoPParamsUnitLegacyByteV0,
		},
	))
	require.ErrorIs(
		t, db.ReconcileAlonzoPParamsUnitAfterRecovery(ctx),
		ErrAlonzoPParamsUnitNoRows,
	)
	require.ErrorIs(t, db.CheckNodeSettings(ctx), ErrAlonzoPParamsUnitNoRows)
	gates, err := db.NodeSettingsGates(ctx)
	require.NoError(t, err)
	require.Equal(t, nodesettings.AlonzoPParamsUnitLegacyByteV0,
		gates[nodesettings.AlonzoPParamsUnitGateName])
}
