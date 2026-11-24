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

package storageadopt_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/node"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/storageadopt"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestMigrateToSplitRecoversInterruptedRestoreBeforeOpeningStores(t *testing.T) {
	t.Parallel()
	cfg, tip := storageadopt.SeedAPIStore(t, "api", 5)
	backupDir := cfg.DatabasePath + ".pre-restore"
	require.NoError(t, os.Rename(cfg.DatabasePath, backupDir))
	require.NoError(t, node.MigrateToSplit(t.Context(), cfg, storageadopt.MigrateTestLogger()))
	require.FileExists(t, filepath.Join(filepath.Join(cfg.DatabasePath, "indexer"), "metadata.sqlite"))
	storageadopt.RequireSplitStores(t, cfg, tip, 5)
	require.NoDirExists(t, backupDir)
}

func TestRestoreAdoptsLegacySQLiteAPIStore(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cfg, tip := storageadopt.SeedAPIStore(t, "api", 5)
	logger := storageadopt.MigrateTestLogger()
	rt, err := internalplugins.OpenDatabase(
		ctx,
		&database.Config{
			Role:    database.RoleReleased,
			DataDir: cfg.DatabasePath, Logger: logger, StorageMode: "api",
			Network: cfg.Network,
		},
		internalplugins.StorageSelections{
			Blob: cfg.Plugins.Storage.Blob, Metadata: cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{
			DataDir: cfg.DatabasePath, Logger: logger, StorageMode: "api",
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close(context.WithoutCancel(ctx)) })
	snap := filepath.Join(testutil.OwnerOnlyTempDir(t), "snapshot")
	manifest, err := lifecycle.Snapshot(
		ctx, rt.Database, snap, lifecycle.TriggerManual, "test", "badger", "sqlite",
	)
	require.NoError(t, err)
	require.Equal(t, "api", manifest.StorageMode)
	require.Empty(t, manifest.IndexerSHA256)
	require.NoError(t, rt.Close(ctx))

	cfg.DatabasePath = filepath.Join(testutil.OwnerOnlyTempDir(t), "target")
	_, err = dblifecycle.NewService(cfg, nil, logger).Restore(ctx, snap)
	require.NoError(t, err)
	storageadopt.RequireSplitStores(t, cfg, tip, 5)
}
