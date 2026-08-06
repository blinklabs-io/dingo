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
	"sync"
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

// TestInsertNodeSettingsGateIfAbsentFirstCallWins pins the ordinary case:
// the first call for a name inserts and reports it, unlike
// SetNodeSettingsGates's unconditional upsert.
func TestInsertNodeSettingsGateIfAbsentFirstCallWins(t *testing.T) {
	store := newManagementTestStore(t)
	inserted, err := store.InsertNodeSettingsGateIfAbsent(
		"network_magic", "1", 0, 0,
	)
	require.NoError(t, err)
	require.True(t, inserted)

	gates, err := store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(t, "1", gates["network_magic"])
}

// TestInsertNodeSettingsGateIfAbsentLoserDoesNotOverwrite is
// InsertNodeSettingsGateIfAbsent's whole point: a second call for a name
// that already has a row must report that it did not insert and must never
// touch the existing value -- the opposite of SetNodeSettingsGates's
// upsert, which always overwrites regardless of what is already there.
func TestInsertNodeSettingsGateIfAbsentLoserDoesNotOverwrite(t *testing.T) {
	store := newManagementTestStore(t)
	inserted, err := store.InsertNodeSettingsGateIfAbsent(
		"network_magic", "1", 0, 0,
	)
	require.NoError(t, err)
	require.True(t, inserted)

	inserted, err = store.InsertNodeSettingsGateIfAbsent(
		"network_magic", "2", 10, 100,
	)
	require.NoError(t, err)
	require.False(t, inserted)

	gates, err := store.GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(
		t,
		"1",
		gates["network_magic"],
		"a losing call must never overwrite the winner's value",
	)
}

// TestInsertNodeSettingsGateIfAbsentConcurrentCallsExactlyOneWins runs many
// concurrent conditional inserts for the same name against a real
// connection pool and asserts exactly one reports having inserted -- the
// property commit_timestamp.go's evaluateAndPersistGates depends on to
// detect a concurrent first-ever opener instead of racing an unconditional
// upsert.
func TestInsertNodeSettingsGateIfAbsentConcurrentCallsExactlyOneWins(
	t *testing.T,
) {
	store := newManagementTestStore(t)
	const attempts = 8
	results := make([]bool, attempts)
	var wg sync.WaitGroup
	wg.Add(attempts)
	for i := range attempts {
		go func(i int) {
			defer wg.Done()
			inserted, err := store.InsertNodeSettingsGateIfAbsent(
				"storage_mode", "core", 0, 0,
			)
			require.NoError(t, err)
			results[i] = inserted
		}(i)
	}
	wg.Wait()

	winners := 0
	for _, inserted := range results {
		if inserted {
			winners++
		}
	}
	require.Equal(t, 1, winners, "exactly one concurrent insert must win")
}
