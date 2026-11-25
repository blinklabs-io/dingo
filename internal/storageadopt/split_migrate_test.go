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

package storageadopt

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/indexer"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/settingsresolve"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/plugin"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

const migrateProbeKey = "migration_parity_probe"

func migrateTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// seedAPIStore builds the data directory of a v0.94.2 node holding
// numBlocks blocks in the given storage mode, and returns the configuration
// of a split node over it.
func seedAPIStore(
	t *testing.T,
	storageMode string,
	numBlocks int,
) (*config.Config, ocommon.Point) {
	t.Helper()
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  storageMode,
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins: config.PluginsConfig{
			Storage: config.StoragePluginsConfig{
				Blob: plugin.Selection{
					Provider: "badger", Config: map[string]any{},
				},
				Metadata: plugin.Selection{
					Provider: "sqlite", Config: map[string]any{},
				},
			},
		},
		Indexer: config.IndexerConfig{MaxReadyLagBlocks: 10},
	}
	m := &splitMigration{cfg: cfg, logger: migrateTestLogger()}
	deps, err := m.dependencies(storageMode)
	require.NoError(t, err)
	rt, err := internalplugins.OpenDatabase(
		t.Context(), m.databaseConfig(storageMode, ""), m.selections(), deps,
	)
	require.NoError(t, err)
	db := rt.Database
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	cm, err := chain.NewManager(t.Context(), db, bus)
	require.NoError(t, err)
	var last ocommon.Point
	if numBlocks > 0 {
		blocks, err := testfixtures.GenerateBabbageChainAt(1, 2, numBlocks)
		require.NoError(t, err)
		for _, b := range blocks {
			require.NoError(t, cm.PrimaryChain().AddBlock(
				context.Background(), b, nil,
			))
			last = ocommon.Point{
				Slot: b.SlotNumber(), Hash: b.Hash().Bytes(),
			}
		}
		// A released store keeps the ledger tip in its metadata store.
		require.NoError(t, db.Metadata().SetTip(ochainsync.Tip{
			Point: last, BlockNumber: uint64(numBlocks),
		}, nil))
	}
	blobTxn := db.Blob().NewTransaction(true)
	require.NoError(t, db.Blob().SetUtxo(
		blobTxn, migrateOffsetTx, 3, migrateOffsetUtxoValue,
	))
	require.NoError(t, db.Blob().SetTx(
		blobTxn, migrateOffsetTx, migrateOffsetTxValue,
	))
	require.NoError(t, blobTxn.Commit())
	require.NoError(
		t, db.Metadata().SetSyncState(migrateProbeKey, "api-row", nil),
	)
	require.NoError(t, db.Metadata().SetSyncState(
		models.RewardStakeInputsPendingKey, migratePendingInputs, nil,
	))
	require.NoError(t, rt.Close(t.Context()))
	return cfg, last
}

func indexerDirOf(cfg *config.Config) string {
	return filepath.Join(cfg.DatabasePath, internalplugins.IndexerDirName)
}

// requireSplitStores checks what a split node finds after a migration: the
// consensus store opens as a core store with the layout latched, and the
// IndexerDB opens at the ledger tip with the API rows the store held.
func requireSplitStores(
	t *testing.T,
	cfg *config.Config,
	tip ocommon.Point,
	blocks uint64,
) {
	t.Helper()
	m := &splitMigration{cfg: cfg, logger: migrateTestLogger()}
	apiDeps, err := m.dependencies("api")
	require.NoError(t, err)
	if stale, err := internalplugins.OpenDatabase(
		t.Context(),
		m.databaseConfig("api", config.IndexerLayoutSplit),
		m.selections(), apiDeps,
	); err == nil {
		_ = stale.Close(context.Background())
		t.Fatal("the consensus store still opens in API mode")
	}
	deps, err := m.dependencies("core")
	require.NoError(t, err)
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		m.databaseConfig("core", config.IndexerLayoutSplit),
		m.selections(), deps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close(context.Background()) })
	gates, err := rt.Database.NodeSettingsGates(t.Context())
	require.NoError(t, err)
	require.Equal(t, "core", gates["storage_mode"])
	require.Equal(t, "split", gates["indexer_layout"])

	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	idx, err := internalplugins.OpenIndexerDatabase(
		t.Context(), host, rt.Database,
		m.databaseConfig("core", config.IndexerLayoutSplit),
		m.selections(), deps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	got, err := store.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, tip.Slot, got.Slot)
	require.Equal(t, tip.Hash, got.Hash)
	require.Equal(t, blocks, got.BlockNumber)
	_, err = database.BlockByHash(t.Context(), idx, tip.Hash)
	require.NoError(t, err, "the IndexerDB reads blocks through the blob store")
	probe, err := idx.Metadata().GetSyncState(migrateProbeKey, nil)
	require.NoError(t, err)
	require.Equal(t, "api-row", probe,
		"the IndexerDB serves the rows the API store held")
	blobTxn := idx.Blob().NewTransaction(false)
	defer blobTxn.Rollback() //nolint:errcheck
	gotUtxo, err := idx.Blob().GetUtxo(blobTxn, migrateOffsetTx, 3)
	require.NoError(t, err, "the IndexerDB serves the ledger's output entries")
	require.Equal(t, migrateOffsetUtxoValue, gotUtxo)
	gotTx, err := idx.Blob().GetTx(blobTxn, migrateOffsetTx)
	require.NoError(t, err, "the IndexerDB serves the ledger's transaction entries")
	require.Equal(t, migrateOffsetTxValue, gotTx)
	offsets, ok := idx.Metadata().(metadata.IndexerOffsetStore)
	require.True(t, ok)
	storedUtxo, found, err := offsets.GetUtxoOffset(nil, migrateOffsetTx, 3)
	require.NoError(t, err)
	require.True(t, found, "the output entry is in the IndexerDB's own store")
	require.Equal(t, migrateOffsetUtxoValue, storedUtxo)
	storedTx, found, err := offsets.GetTransactionOffset(nil, migrateOffsetTx)
	require.NoError(t, err)
	require.True(t, found,
		"the transaction entry is in the IndexerDB's own store")
	require.Equal(t, migrateOffsetTxValue, storedTx)
	deferred, err := idx.Metadata().GetSyncState(
		indexer.DeferredStakeInputsKey, nil,
	)
	require.NoError(t, err)
	require.Equal(t, migratePendingInputs, deferred,
		"snapshots the ledger was still writing stay pending for the writer")
}

// A transaction and an output that no block of the chain produced, such as a
// genesis output, so the only record of their CBOR is the ledger's offset
// entry.
var (
	migrateOffsetTx        = bytes.Repeat([]byte{0x42}, 32)
	migrateOffsetUtxoValue = []byte{0x82, 0x01, 0x02}
	migrateOffsetTxValue   = []byte{0x83, 0x03, 0x04, 0x05}
)

const migratePendingInputs = `{"entries":[{"epoch":7,"boundary_slot":700}]}`

func requireNoMigrationWork(t *testing.T, cfg *config.Config) {
	t.Helper()
	entries, err := os.ReadDir(cfg.DatabasePath)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasPrefix(e.Name(), migrateWorkPrefix),
			"the work directory is removed")
	}
}

func TestMigrateToSplitMovesAnAPIStoreInPlace(t *testing.T) {
	t.Parallel()
	cfg, tip := seedAPIStore(t, "api", 5)
	require.NoError(t, MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil))

	require.FileExists(t, filepath.Join(indexerDirOf(cfg), "metadata.sqlite"))
	requireNoMigrationWork(t, cfg)
	requireSplitStores(t, cfg, tip, 5)
	require.NoError(t, MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil),
		"a migrated directory is left alone")
}

// A migration that stops after any step is finished by the next start, and a
// stop before the rename leaves the original store as it was.
func TestMigrateToSplitResumesAfterAStopAtEachStep(t *testing.T) {
	t.Parallel()
	for _, step := range []string{
		migrateStepCopied, migrateStepRenamed, migrateStepLatched,
	} {
		t.Run(step, func(t *testing.T) {
			t.Parallel()
			cfg, tip := seedAPIStore(t, "api", 4)
			stop := errors.New("stopped after " + step)
			m := &splitMigration{
				cfg: cfg, logger: migrateTestLogger(),
				afterStep: func(s string) error {
					if s == step {
						return stop
					}
					return nil
				},
			}
			require.ErrorIs(t, m.run(t.Context()), stop)
			if step == migrateStepCopied {
				require.NoDirExists(t, indexerDirOf(cfg),
					"nothing is published before the rename")
			}
			// What an interrupted copy leaves behind.
			require.NoError(t, os.MkdirAll(
				filepath.Join(cfg.DatabasePath, migrateWorkPrefix+"dead"),
				0o700,
			))

			require.NoError(
				t, MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil),
			)
			requireNoMigrationWork(t, cfg)
			requireSplitStores(t, cfg, tip, 4)
		})
	}
}

func TestMigrateToSplitRefusesTooLittleFreeSpace(t *testing.T) {
	t.Parallel()
	cfg, _ := seedAPIStore(t, "api", 2)
	m := &splitMigration{
		cfg: cfg, logger: migrateTestLogger(),
		availableBytes: func(string) (uint64, error) { return 1024, nil },
	}
	err := m.run(t.Context())
	require.ErrorContains(t, err, "not enough free disk space")
	require.NoDirExists(t, indexerDirOf(cfg))
	requireNoMigrationWork(t, cfg)
	require.NoError(t, MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil),
		"the original store is untouched and migrates once there is space")
}

func TestMigrateToSplitLeavesWhatItCannotMigrate(t *testing.T) {
	t.Parallel()
	logger := migrateTestLogger()
	t.Run("core store", func(t *testing.T) {
		t.Parallel()
		cfg, _ := seedAPIStore(t, "core", 2)
		require.NoError(t, MigrateToSplit(t.Context(), cfg, logger, nil))
		require.NoDirExists(t, indexerDirOf(cfg))
	})
	t.Run("no chain state", func(t *testing.T) {
		t.Parallel()
		cfg, _ := seedAPIStore(t, "api", 0)
		require.NoError(t, MigrateToSplit(t.Context(), cfg, logger, nil))
		require.NoDirExists(t, indexerDirOf(cfg))
	})
	t.Run("fresh directory", func(t *testing.T) {
		t.Parallel()
		cfg, _ := seedAPIStore(t, "api", 0)
		require.NoError(t, os.RemoveAll(cfg.DatabasePath))
		require.NoError(t, MigrateToSplit(t.Context(), cfg, logger, nil))
	})
}

func TestMigrateToSplitRefusesWhatDoesNotMatch(t *testing.T) {
	t.Parallel()
	logger := migrateTestLogger()
	t.Run("network mismatch", func(t *testing.T) {
		t.Parallel()
		cfg, _ := seedAPIStore(t, "api", 2)
		cfg.Network = "preprod"
		require.ErrorContains(t, MigrateToSplit(t.Context(), cfg, logger, nil),
			"the database is for network")
		require.NoDirExists(t, indexerDirOf(cfg))
	})
	t.Run("an IndexerDB that is not a finished migration", func(t *testing.T) {
		t.Parallel()
		cfg, _ := seedAPIStore(t, "api", 2)
		require.NoError(t, os.MkdirAll(indexerDirOf(cfg), 0o750))
		require.NoError(t, os.WriteFile(
			filepath.Join(indexerDirOf(cfg), "metadata.sqlite"),
			nil, 0o600,
		))
		err := MigrateToSplit(t.Context(), cfg, logger, nil)
		require.Error(t, err)
		require.ErrorContains(t, err, "IndexerDB")
	})
}

// markInterruptedBackfill leaves the store at dir as v0.94.2 left an API
// database whose Mithril bootstrap stopped during its metadata backfill.
func markInterruptedBackfill(t *testing.T, dir string) *sql.DB {
	t.Helper()
	raw, err := sql.Open("sqlite", filepath.Join(dir, "metadata.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	_, err = raw.Exec(
		`INSERT INTO sync_state(sync_key,value) VALUES('sync_status','backfill')`,
	)
	require.NoError(t, err)
	return raw
}

// An API store whose v0.94.2 backfill did not finish holds API history only
// up to the backfill's last block. Adopting it would record the IndexerDB at
// the ledger tip, so the migration refuses it and copies nothing.
func TestMigrateToSplitRefusesAnInterruptedBackfill(t *testing.T) {
	t.Parallel()
	cfg, _ := seedAPIStore(t, "api", 3)
	raw := markInterruptedBackfill(t, cfg.DatabasePath)

	err := MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil)
	require.ErrorIs(t, err, errInterruptedBackfill)

	require.NoDirExists(t, indexerDirOf(cfg))
	requireNoMigrationWork(t, cfg)
	var status string
	require.NoError(t, raw.QueryRow(
		`SELECT value FROM sync_state WHERE sync_key='sync_status'`,
	).Scan(&status))
	require.Equal(t, "backfill", status)
	gates, ok, err := settingsresolve.ReadPersistedGateValues(cfg)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "api", gates["storage_mode"])
	require.Empty(t, gates["indexer_layout"])

	// A store without the status migrates.
	_, err = raw.Exec(`DELETE FROM sync_state WHERE sync_key='sync_status'`)
	require.NoError(t, err)
	require.NoError(t, MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil))
	require.FileExists(t, filepath.Join(indexerDirOf(cfg), "metadata.sqlite"))
}

func TestMigrateToSplitRecordsCheckpointsBelowTheTip(t *testing.T) {
	t.Parallel()
	const numBlocks = 500
	cfg, _ := seedAPIStore(t, "api", numBlocks)
	require.NoError(t, MigrateToSplit(t.Context(), cfg, migrateTestLogger(), nil))

	m := &splitMigration{cfg: cfg, logger: migrateTestLogger()}
	deps, err := m.dependencies("core")
	require.NoError(t, err)
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		m.databaseConfig("core", config.IndexerLayoutSplit),
		m.selections(), deps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Close(context.Background()) })
	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	t.Cleanup(func() { _ = host.Stop(context.Background()) })
	idx, err := internalplugins.OpenIndexerDatabase(
		t.Context(), host, rt.Database,
		m.databaseConfig("core", config.IndexerLayoutSplit),
		m.selections(), deps,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)

	checkpoints, err := store.Checkpoints(nil)
	require.NoError(t, err)
	numbers := make([]uint64, 0, len(checkpoints))
	for _, cp := range checkpoints {
		numbers = append(numbers, cp.BlockNumber)
		block, err := database.BlockByHash(t.Context(), idx, cp.Hash)
		require.NoError(t, err)
		require.Equal(t, cp.BlockNumber, block.Number)
		require.Equal(t, cp.Slot, block.Slot)
	}
	require.Contains(t, numbers, uint64(numBlocks))
	require.Contains(t, numbers, uint64(numBlocks-432),
		"a point below the tip outlives the volatile window")
}

// The startup test covers the caller's recovery ordering with the same
// persisted fixture as the migration tests.
var (
	SeedAPIStore       = seedAPIStore
	MigrateTestLogger  = migrateTestLogger
	RequireSplitStores = requireSplitStores
)
