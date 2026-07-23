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
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/ledger"
	ouroborosPkg "github.com/blinklabs-io/dingo/ouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// liveLifecycleTestDataDir returns the immutable testdata directory used
// elsewhere in the repo (internal/integration, database package tests) —
// real preview-testnet blocks.
func liveLifecycleTestDataDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "database", "immutable", "testdata")
}

// newLiveLifecycleTestNode hand-builds a partial but real *Node — real
// database, chain manager, ledger state (loaded with real blocks), event
// bus, and ouroboros object, wired the same way Run() wires them — without
// going through Run() itself, matching this file's existing
// newNodeTestDivergedLedger-style test convention (real subsystems, no
// networking/APIs/block production). Returns the node and the ordered
// points of every block loaded, for resolving truncate/verifying restore
// targets.
func newLiveLifecycleTestNode(
	t *testing.T,
	numBlocks int,
) (*Node, []ocommon.Point) {
	t.Helper()
	return newLiveLifecycleTestNodeWithGenesis(t, numBlocks, nil)
}

// newLiveLifecycleTestNodeWithGenesis is newLiveLifecycleTestNode with an
// overridable Cardano genesis config — nil uses the real preview genesis
// (newNodeTestCardanoNodeCfg), matching newLiveLifecycleTestNode. Tests that
// need epoch boundaries to actually fall within their loaded block range
// (the real preview genesis's epochLength is far larger than any small
// block count could reach) pass a custom config with a small epochLength
// instead.
func newLiveLifecycleTestNodeWithGenesis(
	t *testing.T,
	numBlocks int,
	cardanoNodeCfgOverride *cardano.CardanoNodeConfig,
) (*Node, []ocommon.Point) {
	t.Helper()

	tmpDir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		Logger:         logger,
	})
	require.NoError(t, err)

	eventBus := event.NewEventBus(nil, nil)

	cm, err := chain.NewManager(db, eventBus)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(nodeTestSecurityParamLedger{securityParam: 432}),
	)

	points := loadLiveLifecycleTestBlocks(t, cm.PrimaryChain(), numBlocks)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       points[len(points)-1],
		BlockNumber: uint64(len(points)),
	}, nil))

	cardanoNodeCfg := cardanoNodeCfgOverride
	if cardanoNodeCfg == nil {
		cardanoNodeCfg = newNodeTestCardanoNodeCfg(t)
	}

	ouro := ouroborosPkg.NewOuroboros(ouroborosPkg.OuroborosConfig{
		Logger:       logger,
		EventBus:     eventBus,
		NetworkMagic: 2, // preview
	})

	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:           db,
		ChainManager:       cm,
		EventBus:           eventBus,
		CardanoNodeConfig:  cardanoNodeCfg,
		Logger:             logger,
		ValidateHistorical: false,
		DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
			WorkerPoolSize: 1,
			TaskQueueSize:  1,
			Disabled:       true,
		},
	})
	require.NoError(t, err)
	ouro.LedgerState = ledgerState
	require.NoError(t, ledgerState.Start(context.Background()))

	cfg := NewConfig(
		WithDatabasePath(tmpDir),
		WithLogger(logger),
		WithNetwork("preview"),
		WithCardanoNodeConfig(cardanoNodeCfg),
		WithBlobPlugin("badger"),
		WithMetadataPlugin("sqlite"),
		WithDatabaseWorkerPoolConfig(ledger.DatabaseWorkerPoolConfig{
			WorkerPoolSize: 1,
			TaskQueueSize:  1,
			Disabled:       true,
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config:       cfg,
		eventBus:     eventBus,
		db:           db,
		chainManager: cm,
		ledgerState:  ledgerState,
		ouroboros:    ouro,
		ctx:          ctx,
		cancel:       cancel,
	}
	t.Cleanup(func() {
		cancel()
		if n.mempool != nil {
			_ = n.mempool.Stop(context.Background())
		}
		if n.connManager != nil {
			_ = n.connManager.Stop(context.Background())
		}
		if n.peerGov != nil {
			n.peerGov.Stop()
		}
		if n.snapshotMgr != nil {
			_ = n.snapshotMgr.Stop()
		}
		if n.dbLifecycleMgr != nil {
			_ = n.dbLifecycleMgr.Stop()
		}
		if n.ledgerState != nil {
			_ = n.ledgerState.Close()
		}
		if n.db != nil {
			_ = n.db.Close()
		}
		eventBus.Stop()
	})

	return n, points
}

// loadLiveLifecycleTestBlocks loads numBlocks real blocks from the
// immutable testdata into c, mirroring
// internal/integration.loadBlocksFromImmutable (a different package, so
// not directly reusable).
func loadLiveLifecycleTestBlocks(
	t *testing.T,
	c *chain.Chain,
	numBlocks int,
) []ocommon.Point {
	t.Helper()
	imm, err := immutable.New(liveLifecycleTestDataDir())
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err)
	defer iter.Close()

	var points []ocommon.Point
	for range numBlocks {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		require.NoError(t, c.AddBlock(block, nil))
		points = append(points, ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		})
	}
	require.NotEmpty(t, points, "no blocks loaded from testdata")
	if len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(points),
			numBlocks,
		)
	}
	return points
}

// loadRawLiveLifecycleTestBlocks loads the first numBlocks real blocks from
// the immutable testdata WITHOUT adding them to any chain, so a caller can
// feed them in one at a time later (e.g. to simulate new blocks arriving
// live after a truncate/restore, extending from whatever tip the operation
// left rather than from the original full chain).
func loadRawLiveLifecycleTestBlocks(
	t *testing.T,
	numBlocks int,
) []gledger.Block {
	t.Helper()
	imm, err := immutable.New(liveLifecycleTestDataDir())
	require.NoError(t, err)

	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	require.NoError(t, err)
	defer iter.Close()

	var blocks []gledger.Block
	for range numBlocks {
		immBlock, err := iter.Next()
		require.NoError(t, err)
		if immBlock == nil {
			break
		}
		block, err := gledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		blocks = append(blocks, block)
	}
	if len(blocks) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}
	return blocks
}

// lifecycleSnapshot snapshots n's current database directly via
// database/lifecycle, the same primitive internal/dblifecycle.Service
// uses for the offline CLI path.
func lifecycleSnapshot(
	t *testing.T,
	n *Node,
	destDir string,
) (lifecycle.Manifest, error) {
	t.Helper()
	return lifecycle.Snapshot(
		context.Background(),
		n.db,
		destDir,
		lifecycle.TriggerManual,
		"test",
	)
}

// TestLiveTruncateRebuildsStorageAndKeepsNodeUsable is the core Phase 2
// verification: truncating a real, multi-block chain on an already-running
// node's live objects must (a) actually remove the truncated blocks and
// fix up the tip, (b) rebuild every storage-dependent subsystem so it
// points at the new db/ledgerState rather than the closed ones, and (c)
// leave n.ctx alone, so the node's normal shutdown signalling is
// unaffected by having gone through a live truncate.
func TestLiveTruncateRebuildsStorageAndKeepsNodeUsable(t *testing.T) {
	const numBlocks = 20
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	oldDB := n.db
	oldLedgerState := n.ledgerState
	oldChainManager := n.chainManager
	oldCtx := n.ctx

	targetIndex := numBlocks / 2
	targetSlot := points[targetIndex].Slot

	blocksRemoved, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(numBlocks-1-targetIndex), blocksRemoved)

	// n.ctx must survive untouched — a live truncate must never cancel or
	// replace it, or SIGINT/SIGTERM handling would silently stop working.
	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())

	// Storage objects must be genuinely new, not the same (now-closed)
	// instances.
	require.NotSame(t, oldDB, n.db)
	require.NotSame(t, oldLedgerState, n.ledgerState)
	require.NotSame(t, oldChainManager, n.chainManager)

	// Every rebuilt subsystem must exist.
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)
	require.NotNil(t, n.snapshotMgr)
	require.NotNil(t, n.dbLifecycleMgr)

	// The kept-alive ouroboros object must be rewired to the NEW
	// dependencies, not left pointing at the closed ones.
	require.Same(t, n.ledgerState, n.ouroboros.LedgerState)
	require.Same(t, n.mempool, n.ouroboros.Mempool)
	require.Same(t, n.chainsyncState, n.ouroboros.ChainsyncState)
	require.Same(t, n.connManager, n.ouroboros.ConnManager)
	require.Same(t, n.peerGov, n.ouroboros.PeerGov)

	// The truncate itself must have taken effect: tip at the target, and
	// blocks after it gone from the new database.
	tip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, targetSlot, tip.Point.Slot)

	for i, p := range points {
		_, err := database.BlockByHash(n.db, p.Hash)
		if i <= targetIndex {
			require.NoErrorf(t, err, "block %d (slot %d) should survive", i, p.Slot)
		} else {
			require.Errorf(t, err, "block %d (slot %d) should be truncated away", i, p.Slot)
		}
	}
}

// TestLiveTruncateIsSerializedAgainstConcurrentCalls exercises
// n.liveLifecycleMu: two Truncate calls racing must not interleave their
// quiesce/rebuild sequences.
func TestLiveTruncateIsSerializedAgainstConcurrentCalls(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	slotA := points[numBlocks/2].Slot
	slotB := points[numBlocks/2].Slot

	errCh := make(chan error, 2)
	go func() {
		_, err := n.Truncate(
			context.Background(),
			dblifecycle.TruncateTarget{Slot: &slotA},
		)
		errCh <- err
	}()
	go func() {
		_, err := n.Truncate(
			context.Background(),
			dblifecycle.TruncateTarget{Slot: &slotB},
		)
		errCh <- err
	}()

	err1 := <-errCh
	err2 := <-errCh
	// Both target the same (already-truncated-to) point, so the second
	// call to actually run should be a no-op success, not a race/panic.
	require.NoError(t, err1)
	require.NoError(t, err2)
}

// TestLiveTruncateRejectsTargetAheadOfTipWithoutTearingDownNode guards
// against a severe finding from live testing (dingo#1651 follow-up): a
// live Truncate whose target is rejected during read-only validation (here,
// a block number ahead of the current tip — unlike a too-high slot, which
// ResolveTargetBySlot treats as a no-op resolving to the tip itself, an
// out-of-range block number is a real ResolveTarget error) used to
// unconditionally call n.cancel(), tearing down the entire node over a
// request that never touched any data. Since ResolveTarget/
// lifecycle.Truncate's own pre-DeleteBlocksAfter checks are provably
// read-only, lifecycle.ErrTruncateNotStarted now lets Node.Truncate resume
// normally instead.
func TestLiveTruncateRejectsTargetAheadOfTipWithoutTearingDownNode(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	oldCtx := n.ctx
	aheadNumber := uint64(len(points)) + 1_000_000

	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{BlockNumber: &aheadNumber},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a rejected truncate", p.Slot,
		)
	}
}

// TestLiveRestoreRebuildsStorageAndKeepsNodeUsable verifies the Restore
// path end to end: snapshot a node's database, then restore that same
// snapshot back onto the running node, and confirm it comes back with the
// same tip and every subsystem rebuilt and rewired, same as Truncate.
func TestLiveRestoreRebuildsStorageAndKeepsNodeUsable(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	manifest, err := lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, manifest.TipSlot)

	oldCtx := n.ctx
	restoredManifest, err := n.Restore(context.Background(), snapshotDir)
	require.NoError(t, err)
	require.Equal(t, manifest.CommitTimestamp, restoredManifest.CommitTimestamp)

	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.NotNil(t, n.mempool)
	require.NotNil(t, n.chainsyncState)
	require.NotNil(t, n.connManager)
	require.NotNil(t, n.peerGov)
	require.Same(t, n.ledgerState, n.ouroboros.LedgerState)

	tip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, err := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(t, err, "block at slot %d missing after restore", p.Slot)
	}
}

// TestLiveRestoreRejectsCorruptedSnapshotWithoutDataLoss guards against a
// severe regression found via manual live testing (dingo#1651 follow-up):
// a Restore that failed because the blob backup was corrupted used to
// delete the node's OWN current data directory before validating the
// incoming snapshot at all, then bring the whole node process down — an
// operator who tried to restore from a bad snapshot lost the running node
// AND its existing database, with nothing left to restart into. Restore
// now stages the incoming snapshot in a sibling directory and validates it
// fully before touching the node's real data directory, so a corrupted
// snapshot must be rejected with the node's original data and tip
// completely intact and the node still usable.
func TestLiveRestoreRejectsCorruptedSnapshotWithoutDataLoss(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)

	// Corrupt the blob backup's content without touching manifest.json, so
	// ReadManifest still succeeds and the corruption is only caught once
	// Badger's own restore-stream parsing runs. Garbage bytes (rather than
	// e.g. flipping a few bytes in place, which can land inside an entry's
	// payload and still parse structurally) reliably fail Badger's
	// internal length-prefix parsing — same technique already proven in
	// bark's TestVerifySnapshotFailsForCorruptedSnapshot.
	blobPath := filepath.Join(snapshotDir, lifecycle.BlobBackupFileName)
	require.NoError(t, os.WriteFile(
		blobPath, []byte("not a valid badger backup stream"), 0o644,
	))

	oldCtx := n.ctx
	oldDB := n.db
	_, err = n.Restore(context.Background(), snapshotDir)
	require.Error(t, err)

	// The node must still be alive and usable: n.ctx untouched, no
	// cancellation, same live db instance, unchanged tip and blocks.
	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.Same(t, oldDB, n.db)

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a rejected restore", p.Slot,
		)
	}

	// No leftover staging or backup directory should remain.
	_, statErr := os.Stat(n.config.dataDir + ".restore-staging")
	require.True(t, os.IsNotExist(statErr))
	_, statErr = os.Stat(n.config.dataDir + ".pre-restore")
	require.True(t, os.IsNotExist(statErr))
}

// TestLiveRestoreRejectsNetworkMismatchWithoutDataLoss confirms the other
// half of the same fix: restoring a snapshot from a genuinely different
// network onto a running node must be rejected — caught by
// validateRestoredAgainstNodeConfig before the swap — with the node's own
// data and tip left completely untouched and the node still usable,
// rather than the node being torn down (dingo#1651 follow-up).
func TestLiveRestoreRejectsNetworkMismatchWithoutDataLoss(t *testing.T) {
	const numBlocks = 10
	n, points := newLiveLifecycleTestNode(t, numBlocks)

	// Build a standalone database configured for a different network and
	// snapshot IT, rather than hand-tampering a preview snapshot's
	// manifest: this way the snapshot is entirely self-consistent (valid
	// checksum, tip matches its own data) and only mismatches what n is
	// actually configured to run — the real-world scenario this guards.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	otherDB, err := database.New(&database.Config{
		DataDir:        t.TempDir(),
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		Logger:         logger,
		Network:        "devnet",
	})
	require.NoError(t, err)
	require.NoError(t, otherDB.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 0, Hash: []byte{}},
		BlockNumber: 0,
	}, nil))

	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err = lifecycle.Snapshot(
		context.Background(), otherDB, snapshotDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)
	require.NoError(t, otherDB.Close())

	oldCtx := n.ctx
	oldDB := n.db
	_, err = n.Restore(context.Background(), snapshotDir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "node settings mismatch")

	require.Same(t, oldCtx, n.ctx)
	require.NoError(t, n.ctx.Err())
	require.Same(t, oldDB, n.db)

	tip, tipErr := n.db.GetTip(nil)
	require.NoError(t, tipErr)
	require.Equal(t, points[len(points)-1].Slot, tip.Point.Slot)
	for _, p := range points {
		_, blockErr := database.BlockByHash(n.db, p.Hash)
		require.NoErrorf(
			t, blockErr,
			"block at slot %d missing after a rejected restore", p.Slot,
		)
	}
}

// smallEpochGenesisCfgForLifecycleTest returns a CardanoNodeConfig with
// epochLength=100 — the real preview genesis newNodeTestCardanoNodeCfg
// loads has an epochLength far larger than any small block count could
// reach, which mattered for a manual-testing bug (dingo#1651 follow-up)
// specifically tied to crossing epoch boundaries after a live truncate:
// with the real genesis, a reproduction never actually exercises the
// epoch-rollover/stake-snapshot code path at all.
func smallEpochGenesisCfgForLifecycleTest(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	// Start from the real preview genesis (already has valid Byron/Shelley
	// data, genesis UTxOs, hashes, etc.) and override just the one field
	// that matters here, rather than hand-building a minimal genesis from
	// scratch — a from-scratch genesis needs realistic bootstrap UTxO/stake
	// data before LedgerState.Start will even get through createGenesisBlock.
	cfg := newNodeTestCardanoNodeCfg(t)
	require.NotNil(t, cfg.ShelleyGenesis())
	cfg.ShelleyGenesis().EpochLength = 100
	return cfg
}

// TestSecondLiveTruncateResumesTipAdvancement is a regression reproduction
// found via manual live testing (dingo#1651 follow-up): after a SECOND
// consecutive live truncate on the same long-running node, new blocks kept
// getting added to the chain but the reported tip stayed stuck at the
// second truncate's landing point forever — the rebuilt ledger-processing
// pipeline never committed them, specifically once an epoch boundary was
// involved. A single live truncate's rebuild works fine
// (TestLiveTruncateRebuildsStorageAndKeepsNodeUsable already proves that
// without crossing an epoch boundary); this test truncates twice in a row,
// each time re-adding blocks that cross at least one epoch boundary
// (epochLength=100, real testdata blocks are 20 slots apart), and confirms
// the tip actually advances again after each one, not just the first.
func TestSecondLiveTruncateResumesTipAdvancement(t *testing.T) {
	const numBlocks = 20
	n, points := newLiveLifecycleTestNodeWithGenesis(
		t, numBlocks, smallEpochGenesisCfgForLifecycleTest(t),
	)
	// Re-loaded separately (not added to any chain yet) so they can be fed
	// back in one at a time after each truncate, simulating new blocks
	// arriving live — reusing the real, already-validated blocks 10-19
	// keeps their prev-hash chain correct without fabricating new ones.
	tailBlocks := loadRawLiveLifecycleTestBlocks(t, numBlocks)[10:20]

	// First truncate: back to block 9 (slot 180, epoch 1), removing blocks
	// 10-19. Re-adding them crosses epoch 1->2 (block 10, slot 200) and
	// epoch 2->3 (block 15, slot 300).
	target1 := points[9].Slot
	_, err := n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &target1},
	)
	require.NoError(t, err)

	for _, b := range tailBlocks {
		require.NoError(t, n.chainManager.PrimaryChain().AddBlock(b, nil))
	}
	require.Eventually(t, func() bool {
		tip, err := n.db.GetTip(nil)
		return err == nil && tip.Point.Slot == points[19].Slot
	}, 5*time.Second, 10*time.Millisecond,
		"tip did not advance after the FIRST truncate")

	// Second truncate: back to block 14 (slot 280, epoch 2), removing
	// blocks 15-19. Re-adding them crosses epoch 2->3 (block 15, slot 300)
	// again — this time on storage rebuilt by a SECOND live truncate.
	target2 := points[14].Slot
	_, err = n.Truncate(
		context.Background(),
		dblifecycle.TruncateTarget{Slot: &target2},
	)
	require.NoError(t, err)

	for _, b := range tailBlocks[5:] {
		require.NoError(t, n.chainManager.PrimaryChain().AddBlock(b, nil))
	}
	require.Eventually(t, func() bool {
		tip, err := n.db.GetTip(nil)
		return err == nil && tip.Point.Slot == points[19].Slot
	}, 5*time.Second, 10*time.Millisecond,
		"tip did not advance after the SECOND truncate")
}
