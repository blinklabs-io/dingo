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

package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// A snapshot v0.94.2 takes carries its metadata store, and the store it
// restores is still a v0.94.2 one: the first open of the restored data
// directory moves its ledger state, to the same state the source reaches.
func TestRestoredReleasedSnapshotMigratesOnItsFirstOpen(t *testing.T) {
	t.Parallel()
	src := adoptedReleasedDatabase(t)
	cfg := parityConfig(src)
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		&database.Config{
			Role:    database.RoleReleased,
			DataDir: src, StorageMode: "core", Network: "preview",
			IndexerLayout: config.IndexerLayoutIndexerDB,
		},
		internalplugins.StorageSelections{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{DataDir: src, StorageMode: "core"},
	)
	require.NoError(t, err)
	snapDir := filepath.Join(testutil.OwnerOnlyTempDir(t), "snap")
	manifest, err := lifecycle.Snapshot(
		t.Context(), rt.Database, snapDir, lifecycle.TriggerManual, "test",
		cfg.Plugins.Storage.Blob.Provider,
		cfg.Plugins.Storage.Metadata.Provider,
	)
	require.NoError(t, errors.Join(err, rt.Close(context.Background())))
	require.NotZero(t, manifest.MetadataBytes)

	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	target := filepath.Join(testutil.OwnerOnlyTempDir(t), "db")
	_, err = lifecycle.RestoreValidated(
		t.Context(), host, nil, snapDir, target, nil,
		lifecycle.RestoreStorageConfig{},
	)
	require.NoError(t, err)
	require.NoError(t, host.Stop(context.Background()))

	restored, err := releasedOpen(t, target, nil)
	require.NoError(t, err)
	state, err := database.ReadLedgerStoreState(restored.Database.Blob())
	require.NoError(t, err)
	require.Equal(t, database.LedgerStoreCurrent, state)
	require.NoError(t, restored.Close(context.Background()))

	migrated, err := releasedOpen(t, src, nil)
	require.NoError(t, err)
	require.NoError(t, migrated.Close(context.Background()))
	require.Equal(t, readMigratedSet(t, src), readMigratedSet(t, target))
}
