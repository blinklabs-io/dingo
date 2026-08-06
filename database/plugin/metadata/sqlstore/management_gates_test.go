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

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
)

func TestNodeSettingsGatesRoundTrip(t *testing.T) {
	store := newManagementTestStore(t)
	gates, err := store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Empty(t, gates)

	require.NoError(t, store.SetNodeSettingsGates(
		nodesettings.Values{
			"network_magic": "1",
			"start_era":     "dijkstra",
		},
		42, 1000,
	))

	gates, err = store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "1", gates["network_magic"])
	require.Equal(t, "dijkstra", gates["start_era"])
}

func TestNodeSettingsGatesUpsertOverwrites(t *testing.T) {
	store := newManagementTestStore(t)
	require.NoError(t, store.SetNodeSettingsGates(
		nodesettings.Values{"storage_mode": "api"}, 1, 10,
	))
	require.NoError(t, store.SetNodeSettingsGates(
		nodesettings.Values{"storage_mode": "core"}, 2, 20,
	))
	gates, err := store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "core", gates["storage_mode"])
}

func TestNodeSettingsGatesEmptyWriteIsNoOp(t *testing.T) {
	store := newManagementTestStore(t)
	require.NoError(t, store.SetNodeSettingsGates(nil, 0, 0))
	gates, err := store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Empty(t, gates)
}
