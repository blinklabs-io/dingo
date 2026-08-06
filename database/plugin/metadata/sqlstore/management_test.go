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
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newManagementTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store
}

func TestGetPoolByVrfKeyHashExcludesRetiredPool(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKey := make([]byte, 28)
	poolKey[0] = 1
	vrfKey := make([]byte, 32)
	vrfKey[0] = 2
	pool := &models.Pool{PoolKeyHash: poolKey, VrfKeyHash: vrfKey}
	registration := &models.PoolRegistration{
		PoolKeyHash: poolKey,
		VrfKeyHash:  vrfKey,
		AddedSlot:   10,
	}
	require.NoError(t, store.ImportPool(pool, registration, nil))

	// The pool remains in historical metadata, but a retirement effective in
	// the current epoch must no longer reserve its VRF key.
	require.NoError(t, store.RetirePools(nil, [][]byte{poolKey}, 1, 20))
	require.NoError(t, store.SetEpoch(0, 1, nil, nil, nil, nil, 0, 1, 100, nil))
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip")},
		BlockNumber: 1,
	}, nil))
	got, err := store.GetPoolByVrfKeyHash(vrfKey, nil)
	require.NoError(t, err)
	require.Nil(t, got)

}

func TestCommitTimestamp(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	timestamp, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Zero(t, timestamp)

	transaction := store.Transaction()
	require.NoError(t, store.SetCommitTimestamp(1234, transaction))
	require.NoError(t, transaction.Commit())
	timestamp, err = store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Equal(t, int64(1234), timestamp)
}

func TestNodeSettingsAreImmutableWithNetworkBackfill(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	settings, err := store.GetNodeSettings()
	require.NoError(t, err)
	require.Nil(t, settings)

	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
	}))
	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     "preview",
	}))
	require.NoError(t, store.SetNodeSettings(&types.NodeSettings{
		StorageMode: types.StorageModeAPI,
		Network:     "mainnet",
	}))
	settings, err = store.GetNodeSettings()
	require.NoError(t, err)
	require.Equal(t, &types.NodeSettings{
		StorageMode: types.StorageModeCore,
		Network:     "preview",
	}, settings)
}
