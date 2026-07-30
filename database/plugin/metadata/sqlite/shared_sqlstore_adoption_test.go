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

package sqlite

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestSharedSQLStoreAdoptsCurrentUnversionedSQLite(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	legacy, raw, _, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, legacy.Start(t.Context()))
	expected := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 9123,
			Hash: bytes.Repeat([]byte{0x71}, 32),
		},
		BlockNumber: 456,
	}
	require.NoError(t, legacy.SetTip(expected, nil))
	// Simulate the final pre-versioning database shape: all version-one tables
	// and data exist, but no migration ledger does.
	_, err = raw.Exec("DELETE FROM schema_migrations")
	require.NoError(t, err)
	require.NoError(t, legacy.Close())

	store, adoptedRaw, _, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	actual, err := store.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
	var phase string
	var dirty bool
	require.NoError(t, adoptedRaw.QueryRow(`
SELECT phase, dirty FROM schema_migrations WHERE version = 1`,
	).Scan(&phase, &dirty))
	require.Equal(t, "complete", phase)
	require.False(t, dirty)
}
