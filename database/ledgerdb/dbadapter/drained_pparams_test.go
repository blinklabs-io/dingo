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

package dbadapter_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/stretchr/testify/require"
)

type pparamsSource struct{}

func (pparamsSource) PParamsLoaded() (bool, error) { return true, nil }

func (pparamsSource) PParamsForEra(uint) ([]models.PParams, error) {
	return []models.PParams{
		{Epoch: 1, AddedSlot: 10, EraId: 6, Cbor: []byte{1}},
	}, nil
}

func (pparamsSource) PParamUpdatesLoaded() (bool, error) { return true, nil }

func (pparamsSource) PParamUpdates(fn func(models.PParamUpdate) error) error {
	return fn(models.PParamUpdate{
		GenesisHash: []byte{9}, Cbor: []byte{0xa1}, AddedSlot: 5, Epoch: 1,
	})
}

func (pparamsSource) SyncMarkersLoaded() (bool, error) { return true, nil }

func (pparamsSource) SyncMarker(string) (string, bool, error) {
	return "", false, nil
}

// The protocol parameter reads of the adapter, which the set loader and the
// Koios source use, come from the installed source once the family is
// drained, not from the metadata store's empty tables.
func TestAdapterProtocolParameterReadsComeFromTheSourceOnceDrained(
	t *testing.T,
) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	a := dbadapter.New(db)
	require.NoError(t, db.RecordDrainedFamily(
		t.Context(), nodesettings.FamilyPParamsUpdates,
	))
	db.SetPParamsSource(pparamsSource{})

	rows, err := a.ListPParamsForEra(6, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	rows, err = a.GetPParamsRows(3, 6, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	all, err := a.ListPParamUpdates(nil)
	require.NoError(t, err)
	require.Len(t, all, 1)
	updates, err := a.GetPParamUpdates(1, nil)
	require.NoError(t, err)
	require.Len(t, updates, 1)
}
