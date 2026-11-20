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
	"encoding/hex"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

const (
	// legacyParityTipSlot is the tip of the database under
	// legacyParityArchive.
	legacyParityTipSlot = 90700
	// legacyRollbackChunks is the chunks of epoch 0: a database loaded from
	// them is at the last block of the epoch, which is inside the security
	// parameter of the released database's tip and on the other side of an
	// epoch boundary from it.
	legacyRollbackChunks = 20
	// previewEpochLength is the slots of a preview epoch.
	previewEpochLength = 86400
)

func previewNodeConfig(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"), "preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	return cfg
}

// migrateLegacyDatabase unpacks the released version's data directory and
// runs a node over it until its ledger is back at the released tip, as an
// upgraded node does on its first start.
func migrateLegacyDatabase(
	t *testing.T,
	nodeCfg *cardano.CardanoNodeConfig,
) string {
	t.Helper()
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	require.NoError(t, MigrateToSplit(
		t.Context(), parityConfig(dir),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	n := startParityNode(t, "migrated", dir, nodeCfg, warnLogger(t))
	require.Eventually(t, func() bool {
		return n.ledgerSlot() == legacyParityTipSlot
	}, 2*time.Minute, 20*time.Millisecond,
		"the migrated ledger applies the blocks up to the released tip again")
	n.stop()
	return dir
}

// openConsensus opens the consensus database of the split data directory
// dir. The returned function closes it.
func openConsensus(t *testing.T, dir string) (*database.Database, func()) {
	t.Helper()
	cfg := parityConfig(dir)
	rt, err := internalplugins.OpenDatabase(
		t.Context(),
		&database.Config{
			DataDir: dir, StorageMode: "core", Network: "preview",
			IndexerLayout:  config.IndexerLayoutSplit,
			BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
			MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		},
		internalplugins.StorageSelections{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{
			DataDir: dir, StorageMode: "core",
		},
	)
	require.NoError(t, err)
	closed := false
	closeDB := func() {
		if closed {
			return
		}
		closed = true
		require.NoError(t, rt.Close(context.Background()))
	}
	t.Cleanup(closeDB)
	return rt.Database, closeDB
}

func previewSecurityParam(t *testing.T) uint64 {
	t.Helper()
	k := previewNodeConfig(t).ShelleyGenesis().SecurityParam
	require.Positive(t, k)
	return uint64(k) //nolint:gosec // checked positive above
}

// lastBlockOfEpochZero is the rollback target of the tests.
func lastBlockOfEpochZero(t *testing.T, dir string) models.Block {
	t.Helper()
	db, closeDB := openConsensus(t, dir)
	defer closeDB()
	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.EqualValues(t, legacyParityTipSlot, tip.Point.Slot)
	blk, err := database.BlockBeforeSlot(t.Context(), db, previewEpochLength)
	require.NoError(t, err)
	require.Less(t, tip.BlockNumber-blk.Number, previewSecurityParam(t),
		"the target is inside the security parameter of the released tip")
	require.Greater(t, tip.BlockNumber-blk.Number, uint64(100))
	return blk
}

func liveOutputs(t *testing.T, set *utxoset.Set) map[string]struct{} {
	t.Helper()
	refs := map[string]struct{}{}
	require.NoError(t, set.EachUtxo(
		func(txID []byte, idx uint32, _ []byte) error {
			ref := hex.EncodeToString(txID) + "#" +
				strconv.FormatUint(uint64(idx), 10)
			refs[ref] = struct{}{}
			return nil
		},
	))
	return refs
}

// A node that was upgraded from the released version follows a fork that
// leaves the chain below the tip its database was migrated at: the ledger
// and the live UTxO set roll back together, and the node then serves what a
// node that only ever applied the blocks up to that point serves, from the
// UTxO set, the accounts, the pools, the epoch state and nonces, and the
// IndexerDB, which rolls back when the node starts.
func TestMigratedNodeRollsBackBelowItsMigrationPoint(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg := previewNodeConfig(t)
	dir := migrateLegacyDatabase(t, nodeCfg)
	target := lastBlockOfEpochZero(t, dir)

	consensus, closeDB := openConsensus(t, dir)
	set, _, err := nodestorage.OpenUtxoSet(nodestorage.UtxoSetConfig{
		DataDir:             dir,
		Logger:              logger,
		CardanoNodeConfig:   nodeCfg,
		RetainRewardRecords: true,
	})
	require.NoError(t, err)
	bus := event.NewEventBus(nil, logger)
	cm, err := chain.NewManager(ctx, consensus, bus)
	require.NoError(t, err)
	fatal := make(chan error, 1)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          consensus,
		UtxoSet:           set,
		ChainManager:      cm,
		EventBus:          bus,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		Network:           "preview",
		PromRegistry:      prometheus.NewRegistry(),
		FatalErrorFunc: func(err error) {
			select {
			case fatal <- err:
			default:
			}
		},
	})
	require.NoError(t, err)
	require.NoError(t, (nodestorage.Reconcile{
		DB:      consensus,
		Set:     set,
		DataDir: dir,
		Logger:  logger,
		Publish: func(mask nodesettings.FamilyMask) {
			ls.SetDrainedFamilies(mask)
			set.SetDrainedFamilies(mask)
		},
		Resume: ls.ResumeUtxoSetFrom,
	}).Run(ctx))
	require.NoError(t, cm.SetLedger(ls))
	require.NoError(t, ls.Start(ctx))
	closed := false
	closeLedger := func() {
		if closed {
			return
		}
		closed = true
		require.NoError(t, ls.Close())
		require.NoError(t, set.Close())
		closeDB()
	}
	t.Cleanup(closeLedger)
	require.EqualValues(t, legacyParityTipSlot, ls.Tip().Point.Slot)
	before := liveOutputs(t, set)

	// The rollbacks a peer on another fork sends: first to a block with no
	// transactions a few below the tip, which the ledger applied in one
	// commit with its neighbours, then across the epoch boundary.
	near, err := database.BlockByNumber(ctx, consensus, ls.Tip().BlockNumber-5)
	require.NoError(t, err)
	rollBack := func(to models.Block) {
		t.Helper()
		bus.Publish(ledger.ChainsyncEventType, event.NewEvent(
			ledger.ChainsyncEventType,
			ledger.ChainsyncEvent{
				Rollback: true,
				Point:    ocommon.NewPoint(to.Slot, to.Hash),
			},
		))
		var fatalErr error
		require.Eventually(t, func() bool {
			select {
			case fatalErr = <-fatal:
				return true
			default:
			}
			setTip, _, err := set.Tip()
			return err == nil && ls.Tip().Point.Slot == to.Slot &&
				setTip.Slot == to.Slot
		}, time.Minute, 10*time.Millisecond,
			"the ledger and the live UTxO set roll back to slot %d", to.Slot)
		require.NoError(t, fatalErr, "the rollback stopped the node")
	}
	rollBack(near)
	rollBack(target)
	setTip, _, err := set.Tip()
	require.NoError(t, err)
	require.Equal(t, target.Hash, setTip.Hash)
	require.NotEqual(t, before, liveOutputs(t, set),
		"the rolled-back blocks changed the UTxO set")
	closeLedger()

	requireNodesServeTheSame(
		t,
		loadParityChain(t, copyImmutableChunkPrefix(t, legacyRollbackChunks)),
		dir,
		nodeCfg,
	)
}

// An operator truncates an upgraded node's database to a block below the tip
// it was migrated at, and the node then serves what a node loaded only up to
// that block serves.
func TestMigratedDatabaseTruncatesBelowItsMigrationPoint(t *testing.T) {
	t.Parallel()
	nodeCfg := previewNodeConfig(t)
	dir := migrateLegacyDatabase(t, nodeCfg)
	target := lastBlockOfEpochZero(t, dir)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := dblifecycle.NewService(parityConfig(dir), nil, logger)
	svc.SetIndexerRollbackFactory(func(
		consensus, indexer *database.Database,
	) (dblifecycle.IndexerRoller, error) {
		return NewIndexerService(dingo.IndexerWriterDeps{
			Consensus: consensus, Indexer: indexer, Logger: logger,
		})
	})
	removed, err := svc.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Hash: target.Hash},
	)
	require.NoError(t, err)
	require.Greater(t, removed, uint64(100))

	requireNodesServeTheSame(
		t,
		loadParityChain(t, copyImmutableChunkPrefix(t, legacyRollbackChunks)),
		dir,
		nodeCfg,
	)
}

// A start that stops after the metadata store was reverted and before its
// ledger state moved into the live UTxO set does not revert it a second
// time: each further window would lose blocks the first already covers.
func TestMigrationRewindRunsOnce(t *testing.T) {
	t.Parallel()
	dir := unpackLegacyDatabase(t, legacyParityArchive)
	require.NoError(t, MigrateToSplit(
		t.Context(), parityConfig(dir),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	))
	db, _ := openConsensus(t, dir)
	k := previewSecurityParam(t)
	released, err := db.GetTip(nil)
	require.NoError(t, err)
	// No set is open, so the run stops where the UTxO rows would move.
	run := func() error {
		return nodestorage.Reconcile{DB: db, RewindBlocks: k}.Run(t.Context())
	}
	require.ErrorContains(t, run(), "is not open")
	rewound, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, released.BlockNumber-k, rewound.BlockNumber)
	mark, err := db.GetSyncState(nodestorage.MigrationRewindSyncKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, mark)
	_, err = database.BlockByPoint(t.Context(), db, released.Point)
	require.NoError(t, err, "the blocks after the target are kept")

	require.ErrorContains(t, run(), "is not open")
	again, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, rewound, again)
}
