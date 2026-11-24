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
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

var errMigrationStopped = errors.New("stopped")

// releasedOpen opens the ledger's database of dir as every opener does,
// moving a v0.94.2 database's ledger state first; hook is the migration's
// step hook.
func releasedOpen(
	t *testing.T,
	dir string,
	hook func(step string) error,
) (*internalplugins.DatabaseRuntime, error) {
	t.Helper()
	cfg := parityConfig(dir)
	dbConfig := &database.Config{
		DataDir: dir, StorageMode: "core", Network: "preview",
		IndexerLayout:  config.IndexerLayoutSplit,
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
	}
	return internalplugins.OpenDatabase(
		t.Context(),
		dbConfig,
		internalplugins.StorageSelections{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{
			DataDir: dir, StorageMode: "core",
			MigrateReleased: func(
				ctx context.Context,
				stores database.Stores,
				openMetadata func(context.Context) (metadata.MetadataStore, func() error, error),
			) error {
				_, err := nodestorage.ReleasedMigration{
					Config:       dbConfig,
					Blob:         stores.Blob,
					Immutable:    stores.Immutable,
					OpenMetadata: openMetadata,
					UtxoSet: nodestorage.UtxoSetConfig{
						DataDir:             dir,
						CardanoNodeConfig:   previewNodeConfig(t),
						RetainRewardRecords: true,
					},
					RewindBlocks: previewSecurityParam(t),
					Hook:         hook,
				}.Run(ctx)
				return err
			},
		},
	)
}

// migratedSetState is what a finished migration leaves in the live UTxO set.
type migratedSetState struct {
	tip     ocommon.Point
	outputs uint64
}

func readMigratedSet(t *testing.T, dir string) migratedSetState {
	t.Helper()
	set, _, err := nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
		DataDir:           dir,
		CardanoNodeConfig: previewNodeConfig(t),
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, set.Close()) }()
	tip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	outputs, err := set.Count()
	require.NoError(t, err)
	return migratedSetState{tip: tip, outputs: outputs}
}

func adoptedReleasedDatabase(t *testing.T) string {
	t.Helper()
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	require.NoError(t, MigrateToSplit(
		t.Context(), parityConfig(dir),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	return dir
}

// A v0.94.2 database's ledger state moves once, the first time it is opened.
// A start that stops after any step of the move opens nothing, and the start
// after it finishes the move to the same state an uninterrupted one reaches.
// The ledger's database refuses the block store until the move has finished,
// and no later open runs a step again.
func TestReleasedMigrationResumesAfterAStopAtEachStep(t *testing.T) {
	t.Parallel()
	refDir := adoptedReleasedDatabase(t)
	// The reference run goes through the migration every opener installs.
	refCfg := parityConfig(refDir)
	refDBConfig := &database.Config{
		DataDir: refDir, StorageMode: "core", Network: "preview",
		IndexerLayout:  config.IndexerLayoutSplit,
		BlobPlugin:     refCfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: refCfg.Plugins.Storage.Metadata.Provider,
	}
	refCfg.CardanoConfig = ""
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		refDBConfig,
		internalplugins.ConfiguredStorageSelections(refCfg),
		internalplugins.StorageDependencies{
			DataDir: refDir, StorageMode: "core",
			MigrateReleased: nodestorage.MigrateReleased(
				refCfg, refDBConfig, previewNodeConfig(t), nil,
			),
		},
	)
	require.NoError(t, err)
	require.NoError(t, rt.Close(context.Background()))
	want := readMigratedSet(t, refDir)

	for _, step := range []string{
		nodestorage.ReleasedStepOpened,
		nodestorage.ReleasedStepRewound,
		nodestorage.ReleasedStepRewards,
		nodestorage.ReleasedStepOutputs,
		nodestorage.ReleasedStepLoaded,
		nodestorage.ReleasedStepVerified,
	} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			dir := adoptedReleasedDatabase(t)
			_, err := releasedOpen(t, dir, func(s string) error {
				if s == step {
					return errMigrationStopped
				}
				return nil
			})
			require.ErrorIs(t, err, errMigrationStopped)

			_, err = internalplugins.OpenDatabase(
				t.Context(),
				&database.Config{
					DataDir: dir, StorageMode: "core", Network: "preview",
					IndexerLayout: config.IndexerLayoutSplit,
				},
				internalplugins.StorageSelections{
					Blob:     parityConfig(dir).Plugins.Storage.Blob,
					Metadata: parityConfig(dir).Plugins.Storage.Metadata,
				},
				internalplugins.StorageDependencies{
					DataDir: dir, StorageMode: "core",
				},
			)
			require.ErrorIs(t, err, database.ErrLedgerNotMigrated)

			rt, err := releasedOpen(t, dir, nil)
			require.NoError(t, err)
			require.NoError(t, rt.Close(context.Background()))
			require.Equal(t, want, readMigratedSet(t, dir))

			rt, err = releasedOpen(t, dir, func(s string) error {
				return errors.New("the move ran again at " + s)
			})
			require.NoError(t, err)
			require.NoError(t, rt.Close(context.Background()))
		})
	}
}

// The migration is one-way: it upgrades the v0.94.2 metadata store's schema
// past the last version v0.94.2 knows, so that release refuses the store it
// leaves rather than opening a ledger the migration has rewound.
func TestReleasedMigrationLeavesAStoreV0942Refuses(t *testing.T) {
	t.Parallel()
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	_, err := dbtest.ReleasedMetadataSyncStatus(dir)
	require.NoError(t, err, "v0.94.2 opens the store it wrote")
	require.NoError(t, MigrateToSplit(
		t.Context(), parityConfig(dir),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))

	rt, err := releasedOpen(t, dir, nil)
	require.NoError(t, err)
	require.NoError(t, rt.Close(context.Background()))

	_, err = dbtest.ReleasedMetadataSyncStatus(dir)
	require.ErrorIs(t, err, migrations.ErrNewerSchema)
}

func TestReleasedMigrationReadsPromotedBlocks(t *testing.T) {
	t.Parallel()
	dir := adoptedReleasedDatabase(t)
	cfg := parityConfig(dir)
	dbConfig := &database.Config{
		Role:    database.RoleReleased,
		DataDir: dir, StorageMode: "core", Network: "preview",
		IndexerLayout:  config.IndexerLayoutSplit,
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		Immutable:      database.ImmutableConfig{Enabled: true, ChunkSlots: 4320, CanContainEBB: true},
	}
	rt, err := internalplugins.OpenDatabase(t.Context(), dbConfig,
		internalplugins.ConfiguredStorageSelections(cfg),
		internalplugins.StorageDependencies{DataDir: dir, StorageMode: "core"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rt.Close(context.Background())) })
	want, err := rt.Database.BlockByIndex(1, nil)
	require.NoError(t, err)
	require.NotEmpty(t, want.Cbor)
	promoted, err := rt.Database.PromoteThrough(t.Context(), 10)
	require.NoError(t, err)
	require.Positive(t, promoted.Promoted)
	require.NoError(t, rt.Close(context.Background()))
	dbConfig.Role = ""
	dbConfig.Immutable = database.ImmutableConfig{}
	for range 2 {
		reopened, err := internalplugins.OpenDatabase(t.Context(), dbConfig,
			internalplugins.ConfiguredStorageSelections(cfg),
			internalplugins.StorageDependencies{DataDir: dir, StorageMode: "core",
				MigrateReleased: nodestorage.MigrateReleased(cfg, dbConfig, previewNodeConfig(t), nil)})
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reopened.Close(context.Background())) })
		got, err := reopened.Database.BlockByIndex(1, nil)
		require.NoError(t, err)
		require.Equal(t, want.Hash, got.Hash)
		require.Equal(t, want.Cbor, got.Cbor)
		require.NoError(t, reopened.Close(context.Background()))
	}
}
