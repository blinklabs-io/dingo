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

package utxoset_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/stretchr/testify/require"
)

// The store keeps the markers under the metadata store's sync keys.
func TestSyncMarkerNamesAreTheSyncKeys(t *testing.T) {
	t.Parallel()
	require.Equal(
		t, database.SyntheticV2CostModelSyncKey, badgerutxo.SyntheticV2Marker,
	)
	require.Equal(
		t, database.SyntheticV2CostModelClearedEpochSyncKey,
		badgerutxo.SyntheticV2ClearedMarker,
	)
}

// A set from before it kept the markers loads them from the metadata store,
// the set the loader finds already loaded keeps its own.
func TestLoadLedgerStateLoadsTheSyncMarkers(t *testing.T) {
	t.Parallel()
	set := openRecoverySet(t, utxoset.Config{})
	db := releasedAt(t, slotPoint(1))
	require.NoError(t, db.SetSyncState(
		database.SyntheticV2CostModelSyncKey, "false", nil,
	))
	require.NoError(t, db.SetSyncState(
		database.SyntheticV2CostModelClearedEpochSyncKey, "4", nil,
	))
	loaded, err := set.SyncMarkersLoaded()
	require.NoError(t, err)
	require.False(t, loaded)

	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	loaded, err = set.SyncMarkersLoaded()
	require.NoError(t, err)
	require.True(t, loaded)
	v, ok, err := set.SyncMarker(database.SyntheticV2CostModelSyncKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "false", v)
	v, ok, err = set.SyncMarker(
		database.SyntheticV2CostModelClearedEpochSyncKey,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "4", v)

	require.NoError(t, db.SetSyncState(
		database.SyntheticV2CostModelSyncKey, "true", nil,
	))
	require.NoError(t, set.LoadLedgerState(t.Context(), dbadapter.New(db)))
	v, _, err = set.SyncMarker(database.SyntheticV2CostModelSyncKey)
	require.NoError(t, err)
	require.Equal(t, "false", v, "a loaded set is not reloaded")
}
