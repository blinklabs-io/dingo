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

package dblifecycle_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/badgerutxo"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func splitConfig(dataDir string) *config.Config {
	cfg := testConfig(dataDir)
	cfg.Indexer = config.IndexerConfig{
		Layout: config.IndexerLayoutSplit, MaxReadyLagBlocks: 10,
	}
	return cfg
}

// seedSplitDir builds a split data directory: a ledger with numBlocks blocks
// and, unless indexerTip is negative, an IndexerDB whose tip is block index
// indexerTip. It returns the block points.
func seedSplitDir(
	t *testing.T,
	cfg *config.Config,
	numBlocks, indexerTip int,
) []ocommon.Point {
	t.Helper()
	svc := dblifecycle.NewService(cfg, nil, nil)
	_ = svc
	deps := internalplugins.StorageDependencies{
		DataDir: cfg.DatabasePath, StorageMode: "core",
	}
	dbConfig := &database.Config{
		DataDir: cfg.DatabasePath, StorageMode: "core",
		Network: cfg.Network, IndexerLayout: config.IndexerLayoutSplit,
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
	}
	selections := internalplugins.StorageSelections{
		Blob: cfg.Plugins.Storage.Blob, Metadata: cfg.Plugins.Storage.Metadata,
	}
	rt, err := internalplugins.OpenDatabase(
		t.Context(), dbConfig, selections, deps,
	)
	require.NoError(t, err)
	db := rt.Database
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	cm, err := chain.NewManager(t.Context(), db, bus)
	require.NoError(t, err)
	blocks, err := testfixtures.GenerateBabbageChainAt(1, 2, numBlocks)
	require.NoError(t, err)
	points := make([]ocommon.Point, 0, numBlocks)
	for i, b := range blocks {
		require.NoError(t, cm.PrimaryChain().AddBlock(
			context.Background(), b, nil,
		))
		if i == 0 {
			// The checkpoint a truncate reconstructs nonces from.
			require.NoError(t, db.SetBlockNonce(
				b.Hash().Bytes(), b.SlotNumber(),
				bytes.Repeat([]byte{0x5c}, 32), true, nil,
			))
		}
		points = append(points, ocommon.Point{
			Slot: b.SlotNumber(), Hash: b.Hash().Bytes(),
		})
	}
	last := points[len(points)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: last, BlockNumber: uint64(numBlocks),
	}, nil))
	if indexerTip >= 0 {
		host, err := internalplugins.NewHost()
		require.NoError(t, err)
		idx, err := internalplugins.OpenIndexerDatabase(
			t.Context(), host, db, dbConfig, selections, deps,
		)
		require.NoError(t, err)
		store, err := indexerdb.NewStore(idx, idx, 10)
		require.NoError(t, err)
		txn := idx.Transaction(t.Context(), true)
		require.NoError(t, store.SetTip(indexerdb.Tip{
			Slot:        points[indexerTip].Slot,
			BlockNumber: uint64(indexerTip) + 1,
			Hash:        points[indexerTip].Hash,
		}, txn))
		require.NoError(t, txn.Commit())
		require.NoError(t, idx.Close())
		require.NoError(t, host.Stop(t.Context()))
	}
	require.NoError(t, rt.Close(t.Context()))
	return points
}

// indexerTipOf opens the IndexerDB of a split data directory and returns its
// tip.
func indexerTipOf(t *testing.T, cfg *config.Config) indexerdb.Tip {
	t.Helper()
	deps := internalplugins.StorageDependencies{
		DataDir: cfg.DatabasePath, StorageMode: "core",
	}
	dbConfig := &database.Config{
		DataDir: cfg.DatabasePath, StorageMode: "core",
		Network: cfg.Network, IndexerLayout: config.IndexerLayoutSplit,
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
	}
	selections := internalplugins.StorageSelections{
		Blob: cfg.Plugins.Storage.Blob, Metadata: cfg.Plugins.Storage.Metadata,
	}
	rt, err := internalplugins.OpenDatabase(
		t.Context(), dbConfig, selections, deps,
	)
	require.NoError(t, err)
	defer rt.Close(context.Background()) //nolint:errcheck
	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	defer host.Stop(context.Background()) //nolint:errcheck
	idx, err := internalplugins.OpenIndexerDatabase(
		t.Context(), host, rt.Database, dbConfig, selections, deps,
	)
	require.NoError(t, err)
	defer idx.Close() //nolint:errcheck
	store, err := indexerdb.NewStore(idx, idx, 10)
	require.NoError(t, err)
	tip, err := store.Tip(nil)
	require.NoError(t, err)
	return tip
}

func TestServiceSnapshotAndRestoreCarryTheIndexerDBOnSplit(t *testing.T) {
	t.Parallel()
	srcCfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "src"))
	points := seedSplitDir(t, srcCfg, 8, 4)
	snapDir := filepath.Join(testutil.OwnerOnlyTempDir(t), "snap")

	manifest, err := dblifecycle.NewService(srcCfg, nil, nil).Snapshot(
		t.Context(), snapDir, "", "",
	)
	require.NoError(t, err)
	require.FileExists(
		t,
		filepath.Join(snapDir, dblifecycle.IndexerPayloadFileName),
	)
	payload, err := dblifecycle.ReadIndexerPayload(snapDir)
	require.NoError(t, err)
	require.Equal(t, points[4].Slot, payload.TipSlot)
	require.LessOrEqual(t, payload.TipSlot, manifest.TipSlot)

	dstCfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "dst"))
	_, err = dblifecycle.NewService(dstCfg, nil, nil).Restore(
		t.Context(), snapDir,
	)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(
		dstCfg.DatabasePath, internalplugins.IndexerDirName, "metadata.sqlite",
	))
	require.Equal(t, points[4].Slot, indexerTipOf(t, dstCfg).Slot,
		"the restored IndexerDB is the one the snapshot carried")
	entries, err := os.ReadDir(filepath.Dir(dstCfg.DatabasePath))
	require.NoError(t, err)
	for _, e := range entries {
		require.NotContains(t, e.Name(), ".dingo-indexer-restore-")
	}
}

func TestServiceSnapshotOnSplitRefusesWhatTheIndexerDBCannotFollow(
	t *testing.T,
) {
	t.Parallel()
	for name, mutate := range map[string]func(*config.Config){
		"trust key": func(c *config.Config) {
			c.DatabaseLifecycle.SnapshotTrustKeyFile = "/nonexistent"
		},
		"cloud destination": func(c *config.Config) {
			c.DatabaseLifecycle.SnapshotCloudDestination = "s3://bucket/x"
		},
		"non sqlite metadata": func(c *config.Config) {
			c.Plugins.Storage.Metadata.Provider = "postgres"
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := splitConfig(
				filepath.Join(testutil.OwnerOnlyTempDir(t), "src"),
			)
			seedSplitDir(t, cfg, 3, 1)
			mutate(cfg)
			snapDir := filepath.Join(testutil.OwnerOnlyTempDir(t), "snap")
			_, err := dblifecycle.NewService(cfg, nil, nil).Snapshot(
				t.Context(), snapDir, "", "",
			)
			require.Error(t, err)
			require.NoDirExists(t, snapDir)
		})
	}
	t.Run("no IndexerDB yet", func(t *testing.T) {
		t.Parallel()
		cfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "src"))
		seedSplitDir(t, cfg, 3, -1)
		snapDir := filepath.Join(testutil.OwnerOnlyTempDir(t), "snap")
		_, err := dblifecycle.NewService(cfg, nil, nil).Snapshot(
			t.Context(), snapDir, "", "",
		)
		require.ErrorContains(t, err, "no IndexerDB")
		require.NoDirExists(t, snapDir)
	})
}

func TestServiceRestoreOnSplitRefusesBeforeChangingAnything(t *testing.T) {
	t.Parallel()
	srcCfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "src"))
	seedSplitDir(t, srcCfg, 5, 2)
	snapDir := filepath.Join(testutil.OwnerOnlyTempDir(t), "snap")
	_, err := dblifecycle.NewService(srcCfg, nil, nil).Snapshot(
		t.Context(), snapDir, "", "",
	)
	require.NoError(t, err)

	t.Run("a snapshot without the payload", func(t *testing.T) {
		t.Parallel()
		sharedCfg := testConfig(
			filepath.Join(testutil.OwnerOnlyTempDir(t), "shared"),
		)
		seedSplitDir(t, sharedCfg, 3, -1)
		ledgerOnly := filepath.Join(testutil.OwnerOnlyTempDir(t), "ledger-only")
		_, err := dblifecycle.NewService(sharedCfg, nil, nil).Snapshot(
			t.Context(), ledgerOnly, "", "",
		)
		require.NoError(t, err)
		dstCfg := splitConfig(
			filepath.Join(testutil.OwnerOnlyTempDir(t), "dst"),
		)
		_, err = dblifecycle.NewService(dstCfg, nil, nil).Restore(
			t.Context(), ledgerOnly,
		)
		require.ErrorContains(t, err, "no IndexerDB payload")
		require.NoDirExists(t, dstCfg.DatabasePath)
	})
	t.Run("a payload that does not match its digest", func(t *testing.T) {
		t.Parallel()
		bad := filepath.Join(testutil.OwnerOnlyTempDir(t), "bad")
		require.NoError(t, os.CopyFS(bad, os.DirFS(snapDir)))
		require.NoError(t, os.Chmod(bad, 0o700))
		path := filepath.Join(bad, dblifecycle.IndexerPayloadFileName)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		raw[len(raw)/2] ^= 0xff
		require.NoError(t, os.WriteFile(path, raw, 0o600))
		dstCfg := splitConfig(
			filepath.Join(testutil.OwnerOnlyTempDir(t), "dst"),
		)
		_, err = dblifecycle.NewService(dstCfg, nil, nil).Restore(
			t.Context(), bad,
		)
		require.ErrorIs(t, err, lifecycle.ErrSnapshotPayloadMismatch)
		require.NoDirExists(t, dstCfg.DatabasePath)
	})
	t.Run("a trust key", func(t *testing.T) {
		t.Parallel()
		dstCfg := splitConfig(
			filepath.Join(testutil.OwnerOnlyTempDir(t), "dst"),
		)
		dstCfg.DatabaseLifecycle.SnapshotTrustKeyFile = "/nonexistent"
		_, err := dblifecycle.NewService(dstCfg, nil, nil).Restore(
			t.Context(), snapDir,
		)
		require.ErrorContains(t, err, "snapshot trust key")
		require.NoDirExists(t, dstCfg.DatabasePath)
	})
	t.Run("a remote snapshot", func(t *testing.T) {
		t.Parallel()
		dstCfg := splitConfig(
			filepath.Join(testutil.OwnerOnlyTempDir(t), "dst"),
		)
		_, err := dblifecycle.NewService(dstCfg, nil, nil).Restore(
			t.Context(), "s3://bucket/snap",
		)
		require.ErrorContains(t, err, "local snapshot directory")
		require.NoDirExists(t, dstCfg.DatabasePath)
	})
}

func TestServiceTruncateOnSplitRollsTheIndexerBackFirst(t *testing.T) {
	t.Parallel()
	const target = 3
	cfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "db"))
	points := seedSplitDir(t, cfg, 8, 6)
	var ledgerHadBlocks atomic.Bool
	var rolledTo atomic.Uint64
	svc := dblifecycle.NewService(cfg, nil, nil)
	svc.SetIndexerRollbackFactory(func(
		consensus, indexer *database.Database,
	) (dblifecycle.IndexerRoller, error) {
		return rollerFunc(func(
			ctx context.Context, to indexerdb.Tip,
		) error {
			_, err := database.BlockByHash(ctx, consensus, points[7].Hash)
			ledgerHadBlocks.Store(err == nil)
			rolledTo.Store(to.Slot)
			store, err := indexerdb.NewStore(indexer, indexer, 10)
			if err != nil {
				return err
			}
			txn := indexer.Transaction(ctx, true)
			if err := store.SetTip(to, txn); err != nil {
				return err
			}
			return txn.Commit()
		}), nil
	})
	slot := points[target].Slot
	removed, err := svc.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &slot},
	)
	require.NoError(t, err)
	require.EqualValues(t, 8-1-target, removed)
	require.True(t, ledgerHadBlocks.Load(),
		"the IndexerDB rolls back while the ledger still stores the blocks")
	require.Equal(t, slot, rolledTo.Load())
	require.Equal(t, slot, indexerTipOf(t, cfg).Slot)
}

type rollerFunc func(context.Context, indexerdb.Tip) error

func (f rollerFunc) RollbackTo(ctx context.Context, to indexerdb.Tip) error {
	return f(ctx, to)
}

func TestServiceTruncateOnSplitRefusalsLeaveTheLedgerWhole(t *testing.T) {
	t.Parallel()
	t.Run("no writer wired", func(t *testing.T) {
		t.Parallel()
		cfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "db"))
		points := seedSplitDir(t, cfg, 5, 3)
		slot := points[1].Slot
		_, err := dblifecycle.NewService(cfg, nil, nil).Truncate(
			t.Context(), dblifecycle.TruncateTarget{Slot: &slot},
		)
		require.ErrorContains(t, err, "indexer writer")
		requireLedgerTip(t, cfg, points[4].Slot)
	})
	t.Run("the rollback is refused", func(t *testing.T) {
		t.Parallel()
		cfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "db"))
		points := seedSplitDir(t, cfg, 5, 3)
		svc := dblifecycle.NewService(cfg, nil, nil)
		svc.SetIndexerRollbackFactory(func(
			_, _ *database.Database,
		) (dblifecycle.IndexerRoller, error) {
			return rollerFunc(func(context.Context, indexerdb.Tip) error {
				return errors.New("rollback refused")
			}), nil
		})
		slot := points[1].Slot
		_, err := svc.Truncate(
			t.Context(), dblifecycle.TruncateTarget{Slot: &slot},
		)
		require.ErrorContains(t, err, "rollback refused")
		requireLedgerTip(t, cfg, points[4].Slot)
	})
}

// A target below the live UTxO set's journal floor is refused before the
// IndexerDB rolls back, so the IndexerDB and the ledger stay at one tip.
func TestServiceTruncateOnSplitChecksTheUtxoSetBeforeTheIndexerRollsBack(
	t *testing.T,
) {
	t.Parallel()
	cfg := splitConfig(filepath.Join(testutil.OwnerOnlyTempDir(t), "db"))
	points := seedSplitDir(t, cfg, 8, 6)
	store, err := badgerutxo.Open(badgerutxo.DirFor(cfg.DatabasePath))
	require.NoError(t, err)
	for i, p := range points[4:] {
		_, err := store.Apply(
			ocommon.NewPoint(p.Slot, p.Hash),
			[]badgerutxo.Output{{
				Ref:  badgerutxo.NewRef(bytes.Repeat([]byte{byte(i + 1)}, 32), 0),
				Cbor: []byte{0x80, byte(i)},
			}},
			nil,
		)
		require.NoError(t, err)
	}
	require.NoError(t, store.Close())

	var rolled atomic.Bool
	svc := dblifecycle.NewService(cfg, nil, nil)
	svc.SetIndexerRollbackFactory(func(
		_, _ *database.Database,
	) (dblifecycle.IndexerRoller, error) {
		return rollerFunc(func(context.Context, indexerdb.Tip) error {
			rolled.Store(true)
			return nil
		}), nil
	})
	slot := points[2].Slot
	_, err = svc.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &slot},
	)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "journal floor")
	require.False(t, rolled.Load(), "the IndexerDB rolled back before the refusal")
	requireLedgerTip(t, cfg, points[7].Slot)
}

func requireLedgerTip(t *testing.T, cfg *config.Config, slot uint64) {
	t.Helper()
	deps := internalplugins.StorageDependencies{
		DataDir: cfg.DatabasePath, StorageMode: "core",
	}
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		&database.Config{
			DataDir: cfg.DatabasePath, StorageMode: "core",
			Network:        cfg.Network,
			BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
			MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		},
		internalplugins.StorageSelections{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		deps,
	)
	require.NoError(t, err)
	defer rt.Close(context.Background()) //nolint:errcheck
	tip, err := rt.Database.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, slot, tip.Point.Slot)
}
