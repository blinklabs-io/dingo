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

package integration

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// setupLifecycleTestChain opens a fresh database in tmpDir, loads
// numBlocks real blocks from the immutable testdata into it via a chain
// manager (as a running node's chainsync would), and persists the tip —
// Chain.AddBlock only updates in-memory chain state, so the tip must be
// set explicitly to match what full ledger processing would do, since
// database/lifecycle reads the persisted tip, not the in-memory chain.
func setupLifecycleTestChain(
	t *testing.T,
	tmpDir string,
	numBlocks int,
) (db *database.Database, points []ocommon.Point) {
	t.Helper()
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", tmpDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", tmpDir,
	))

	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(&mockLedgerState{securityParam: 50}))
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, pts := loadBlocksFromImmutable(t, c, numBlocks)
	if len(blocks) < numBlocks || len(pts) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       pts[len(pts)-1],
		BlockNumber: last.BlockNumber(),
	}, nil))

	return db, pts
}

// TestDatabaseLifecycleSnapshotRestoreRoundTrip verifies that a snapshot
// of a real multi-block chain restores byte-for-byte, tip included.
func TestDatabaseLifecycleSnapshotRestoreRoundTrip(t *testing.T) {
	const numBlocks = 60
	db, points := setupLifecycleTestChain(t, t.TempDir(), numBlocks)
	defer db.Close()

	snapDir := filepath.Join(t.TempDir(), "snap")
	manifest, err := lifecycle.Snapshot(
		context.Background(), db, snapDir, lifecycle.TriggerManual, "test",
	)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, manifest.TipSlot)

	restoredDir := filepath.Join(t.TempDir(), "restored")
	restoredManifest, err := lifecycle.Restore(
		context.Background(), snapDir, restoredDir,
	)
	require.NoError(t, err)
	require.Equal(t, manifest.CommitTimestamp, restoredManifest.CommitTimestamp)

	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob, config.DefaultBlobPlugin, "data-dir", restoredDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata, config.DefaultMetadataPlugin, "data-dir", restoredDir,
	))
	restoredDB, err := database.New(&database.Config{
		DataDir:        restoredDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	defer restoredDB.Close()

	// Every real block that was in the source database must round-trip
	// byte-identically-addressable (same hash resolves) in the restored one.
	for _, p := range points {
		_, err := database.BlockByHash(restoredDB, p.Hash)
		require.NoErrorf(t, err, "block at slot %d missing after restore", p.Slot)
	}
	restoredTip, err := restoredDB.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[len(points)-1].Slot, restoredTip.Point.Slot)
}

// TestDatabaseLifecycleTruncateRealChain is the direct CIP-0135
// conformance check: truncating a real, multi-block chain to a target
// point removes everything after it and leaves the tip consistent, so
// the database is immediately resync-ready from that point (verified
// separately end-to-end via `dingo load` -> `dingo database truncate` ->
// `dingo load` during manual testing, since re-running full ledger sync
// from this package's lighter chain-manager-only harness is out of scope
// here).
func TestDatabaseLifecycleTruncateRealChain(t *testing.T) {
	const numBlocks = 60
	db, points := setupLifecycleTestChain(t, t.TempDir(), numBlocks)
	defer db.Close()

	targetIndex := numBlocks / 2
	target, err := lifecycle.ResolveTargetBySlot(db, points[targetIndex].Slot)
	require.NoError(t, err)
	require.Equal(t, points[targetIndex].Slot, target.Slot)

	blocksRemoved, err := lifecycle.Truncate(context.Background(), db, target, 0)
	require.NoError(t, err)
	// Blocks strictly after targetIndex, up to and including the last one
	// (numBlocks-1): a difference between contiguous IDs, so it's exact
	// regardless of the ID space's starting offset.
	require.Equal(t, uint64(numBlocks-1-targetIndex), blocksRemoved)

	for i, p := range points {
		_, err := database.BlockByHash(db, p.Hash)
		if i <= targetIndex {
			require.NoErrorf(t, err, "block at index %d (slot %d) should survive truncation", i, p.Slot)
		} else {
			require.Errorf(t, err, "block at index %d (slot %d) should have been truncated away", i, p.Slot)
			require.ErrorIs(t, err, models.ErrBlockNotFound)
		}
	}

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Point.Slot)
	require.Equal(t, target.Hash, tip.Point.Hash)
}

// TestDatabaseLifecycleTruncateRejectsBeyondMithrilBoundary verifies that
// a truncate target before the recorded Mithril trust boundary is rejected.
func TestDatabaseLifecycleTruncateRejectsBeyondMithrilBoundary(t *testing.T) {
	const numBlocks = 60
	db, points := setupLifecycleTestChain(t, t.TempDir(), numBlocks)
	defer db.Close()

	boundaryIndex := numBlocks / 2
	require.NoError(t, db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(points[boundaryIndex].Slot, 10),
		nil,
	))

	beforeBoundary, err := lifecycle.ResolveTargetBySlot(
		db, points[boundaryIndex/2].Slot,
	)
	require.NoError(t, err)

	_, err = lifecycle.Truncate(context.Background(), db, beforeBoundary, 0)
	require.Error(t, err)
	// Not just any error: specifically the Mithril-boundary rejection,
	// wrapped in ErrTruncateNotStarted (nothing on disk was touched) --
	// asserting only require.Error above would also pass if Truncate
	// failed for a completely unrelated reason (e.g. a bug elsewhere that
	// broke target resolution or the delete path), silently defeating the
	// point of this test.
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "before the Mithril trust boundary")
}
