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

package dingo

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/consensus"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/blob/badger"
	"github.com/blinklabs-io/dingo/database/plugin/immutable/chunkfile"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// metadataResolutions records the data directory of every metadata store a
// plugin host resolves.
type metadataResolutions struct {
	mu   sync.Mutex
	dirs []string
}

func (r *metadataResolutions) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.dirs...)
}

// newMetadataCountingHost returns a host with the storage and mempool
// providers a live node resolves, whose sqlite metadata provider records each
// resolution before opening the store.
func newMetadataCountingHost(
	t *testing.T,
) (*plugin.Host, *metadataResolutions) {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, badger.RegisterProvider(host))
	require.NoError(t, chunkfile.RegisterProvider(host))
	require.NoError(t, consensus.RegisterProvider(host))
	require.NoError(t, mempool.RegisterProvider(host))
	require.NoError(t, mempool.RegisterFIFOProvider(host))
	require.NoError(t, mempool.RegisterDAGProvider(host))
	got := &metadataResolutions{}
	require.NoError(t, plugin.Register(
		host,
		plugin.Descriptor{
			Capability:   plugin.CapabilityStorageMetadata,
			Name:         sqlite.ProviderName,
			DurableClose: true,
		},
		func() sqlite.Config { return sqlite.Config{} },
		func(
			_ context.Context,
			cfg sqlite.Config,
			deps metadata.ProviderDependencies,
		) (*sqlstore.Store, plugin.Instance, error) {
			got.mu.Lock()
			got.dirs = append(got.dirs, deps.DataDir)
			got.mu.Unlock()
			store, err := sqlite.NewSQLStore(cfg, deps)
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: store.Start,
				StopFunc:  store.CloseContext,
			}, nil
		},
	))
	return host, got
}

// requireNoSQLiteFile fails if a SQLite metadata file exists anywhere under
// dir outside the IndexerDB's directory.
func requireNoSQLiteFile(t *testing.T, dir string) {
	t.Helper()
	indexerDir := filepath.Join(dir, internalplugins.IndexerDirName)
	require.NoError(t, filepath.WalkDir(
		dir,
		func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && path == indexerDir {
				return filepath.SkipDir
			}
			require.Falsef(
				t,
				strings.HasPrefix(d.Name(), "metadata.sqlite"),
				"a SQLite metadata file exists at %s", path,
			)
			return nil
		},
	))
}

// A core node's ledger database keeps no metadata store: opening it,
// applying and rolling back blocks, truncating, snapshotting, restoring the
// snapshot and reopening storage after each resolve none.
func TestCoreNodeResolvesNoMetadataStore(t *testing.T) {
	t.Parallel()
	const (
		numBlocks   = 21
		epochLength = 100
	)
	host, got := newMetadataCountingHost(t)
	n, points := newLiveLifecycleTestNodeWithGenesis(
		t, numBlocks, smallEpochGenesisCfgForLifecycleTest(t),
		disabledLiveLifecycleTestWorkerPoolCfg, withPluginHost(host),
	)
	tail := loadRawLiveLifecycleTestBlocks(t, numBlocks)[10:]

	target := points[9].Slot
	requireLedgerAtChainTip(t, n)
	_, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &target},
	)
	require.NoError(t, err)
	addBlocksSerially(t, n, tail, epochLength)

	require.NoError(t, n.chainManager.PrimaryChain().Rollback(
		t.Context(), points[14],
	))
	require.Eventually(t, func() bool {
		return n.ledgerState.Tip().Point.Slot == points[14].Slot
	}, 10*time.Second, 10*time.Millisecond, "the ledger rolls back")
	for _, b := range tail[5:] {
		require.NoError(t, n.chainManager.PrimaryChain().AddBlock(
			t.Context(), b, nil,
		))
	}
	require.Eventually(t, func() bool {
		return n.ledgerState.Tip().Point.Slot == points[numBlocks-1].Slot
	}, 10*time.Second, 10*time.Millisecond, "the ledger applies the blocks")

	snapshotDir := snapshotTestDir(t, "snap")
	_, err = lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)
	_, err = n.Restore(t.Context(), snapshotDir)
	require.NoError(t, err)

	require.Empty(t, got.all(), "metadata stores resolved")
	requireNoSQLiteFile(t, n.config.dataDir)
}

// An API node resolves the IndexerDB's metadata store and no other, also
// when a truncate reopens storage.
func TestAPINodeResolvesOnlyTheIndexerDBMetadataStore(t *testing.T) {
	t.Parallel()
	host, got := newMetadataCountingHost(t)
	n, points := newLiveLifecycleTestNodeWithStorageModeAtBlock(
		t, 20, nil, disabledLiveLifecycleTestWorkerPoolCfg,
		StorageModeCore, 1, withLivedInUtxoSet, withPluginHost(host),
	)
	require.NoError(t, n.db.SetNodeSettingsGates(
		t.Context(),
		nodesettings.Values{
			"indexer_layout": internalconfig.IndexerLayoutIndexerDB,
		},
	))
	writers := &lifecycleIndexerWriters{}
	n.config.hasIndexerDB = true
	n.config.indexerWriterFactory = writers.build
	require.NoError(t, n.openIndexerWriter(t.Context()))
	t.Cleanup(func() {
		_ = n.stopIndexerWriter(context.Background())
		_ = n.closeIndexerDatabase()
	})
	indexerDir := filepath.Join(
		n.config.dataDir, internalplugins.IndexerDirName,
	)
	require.Equal(t, []string{indexerDir}, got.all())

	target := points[10].Slot
	_, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &target},
	)
	require.NoError(t, err)
	for _, dir := range got.all() {
		require.Equal(t, indexerDir, dir)
	}
	requireNoSQLiteFile(t, n.config.dataDir)
}

// The seed ledger writes only the genesis epoch row however long it stays open:
// it applies none of the loaded blocks, which span several epochs, so the
// fixture's single-row check does not depend on how fast the ledger closes.
func TestLiveLifecycleSeedLedgerAppliesNoLoadedBlocks(t *testing.T) {
	t.Parallel()
	const numBlocks = 21
	// Long enough for a ledger that processed blocks to cross the boundaries
	// the loaded blocks span: it replayed them in well under this in testing.
	const holdWindow = 2 * time.Second
	newLiveLifecycleTestNodeWithGenesis(
		t, numBlocks, smallEpochGenesisCfgForLifecycleTest(t),
		disabledLiveLifecycleTestWorkerPoolCfg,
		withSeedLedgerHold(func() { <-time.After(holdWindow) }),
	)
}
