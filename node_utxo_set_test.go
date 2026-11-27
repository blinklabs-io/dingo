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
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/utxoset"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func init() {
	utxoset.DefaultMemTableSize = 8 << 20
	utxoset.DefaultBlockCacheSize = 1 << 20
}

// TestCheckUtxoSetMigratedMarksAFreshDatabase marks a v0.94.2 database at
// origin as holding its UTxO set only in the live UTxO set.
// checkUtxoSetMigrated alone refuses an unmarked synced database;
// migrateUtxoRows is what marks one.
func TestCheckUtxoSetMigratedMarksAFreshDatabase(t *testing.T) {
	t.Parallel()
	node := func(db *database.Database) *Node {
		n := &Node{db: db}
		n.config.dataDir = t.TempDir()
		return n
	}

	fresh, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(fresh) })
	require.NoError(t, node(fresh).checkUtxoSetMigrated())
	require.NoError(t, node(fresh).checkUtxoSetMigrated(), "already marked")

	synced, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(synced) })
	require.NoError(t, synced.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
	}, nil))
	require.Error(t, node(synced).checkUtxoSetMigrated())
}

// TestMigrateUtxoRowsMarksASyncedDatabase moves a synced v0.94.2 database that
// holds its UTxO set in the metadata store into the live UTxO set instead of
// refusing it, and leaves a marked database that has no move pending alone.
func TestMigrateUtxoRowsMarksASyncedDatabase(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	tip := ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)}
	require.NoError(t, db.SetTip(ochainsync.Tip{Point: tip}, nil))
	set, err := utxoset.Open(utxoset.Config{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	n := &Node{db: db, utxoSet: set}
	n.config.dataDir = t.TempDir()
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	require.Error(t, n.checkUtxoSetMigrated(), "unmarked before the move")
	require.NoError(t, n.migrateUtxoRows(t.Context()))
	require.NoError(t, n.checkUtxoSetMigrated())
	setTip, ok, err := set.Tip()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, tip, setTip)
	progress, err := db.GetSyncState(utxoset.MigrationSyncKey, nil)
	require.NoError(t, err)
	require.Empty(t, progress)
	require.NoError(t, n.migrateUtxoRows(t.Context()), "nothing pending")

	n.utxoSet = nil
	require.NoError(
		t,
		n.migrateUtxoRows(t.Context()),
		"marked: nothing to move",
	)
}

// TestMigrateUtxoRowsNeedsTheSet requires an unmarked synced v0.94.2 database
// to name the missing live UTxO set rather than tell the operator to resync.
func TestMigrateUtxoRowsNeedsTheSet(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewReleasedDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 10, Hash: bytes.Repeat([]byte{1}, 32)},
	}, nil))
	n := &Node{db: db}
	n.config.dataDir = t.TempDir()
	err = n.migrateUtxoRows(t.Context())
	require.ErrorContains(t, err, "live UTxO set")
	require.NotContains(t, err.Error(), "resync")
}

// TestReinitializeCoreStorageClearsTheUtxoSetReconciledMark requires a live
// Restore or Truncate rebuild to invalidate the set's reconcile, so the
// startup stake backfill waits for the reconcile of the rebuilt ledger.
func TestReinitializeCoreStorageClearsTheUtxoSetReconciledMark(t *testing.T) {
	t.Parallel()
	n := &Node{utxoSetReconciled: true}
	n.config.cfg = &internalconfig.Config{}

	// The rebuild cannot complete for a Node with no resolved storage
	// plugins; the mark is cleared before that failure returns.
	require.Error(t, n.reinitializeCoreStorage(t.Context()))
	require.False(t, n.utxoSetReconciled)
}

func TestCheckpointPolicyWaitsForTheWallClockTip(t *testing.T) {
	t.Parallel()
	p := checkpointPolicy{every: 2}
	// Replaying history: nothing is written and no epoch is counted.
	for epoch := range uint64(50) {
		require.False(t, p.due(epoch, false, utxoSetCheckpointNearTipSlots+1))
	}
	// First pass near the tip with no checkpoint writes one at once.
	require.True(t, p.due(50, false, 0))
	require.False(t, p.due(51, true, 0))
	require.True(t, p.due(52, true, 0))
	// Falling behind again pauses it without losing the count.
	require.False(t, p.due(60, true, utxoSetCheckpointNearTipSlots+1))
	require.True(t, p.due(60, true, utxoSetCheckpointNearTipSlots))
}

func TestCheckpointPolicyCountsAnEarlierRunsCheckpointFromNow(t *testing.T) {
	t.Parallel()
	p := checkpointPolicy{every: 1}
	require.False(t, p.due(10, true, 0))
	require.True(t, p.due(11, true, 0))
}

// TestCloseUtxoSetStopsARunningCheckpointCopy requires closing the set to
// cancel a checkpoint copy and wait for its goroutine, so the copy never runs
// against a closed set.
func TestCloseUtxoSetStopsARunningCheckpointCopy(t *testing.T) {
	t.Parallel()
	set, err := utxoset.Open(utxoset.Config{
		Dir: t.TempDir(), CheckpointDir: t.TempDir(), CheckpointKeep: 2,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = set.Close() })
	entered := make(chan struct{})
	set.SetCheckpointBarrier(func(ctx context.Context) (func(), error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	n := &Node{utxoSet: set}
	n.config.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	n.anchoredUtxoSet.Store(set)
	tip := ocommon.Point{Slot: 1, Hash: bytes.Repeat([]byte{1}, 32)}
	require.NoError(t, set.LoadUtxoSet(tip,
		func(func([]byte, uint32, []byte) error) error { return nil }))
	ended := make(chan struct{})
	require.True(
		t,
		n.launchUtxoSetCheckpoint(t.Context(), set, func() { close(ended) }),
	)
	<-entered

	require.NoError(t, n.closeUtxoSet())

	select {
	case <-ended:
	default:
		t.Fatal("the checkpoint copy was still running when the set closed")
	}
	require.False(t, n.launchUtxoSetCheckpoint(t.Context(), set, func() {}),
		"a copy started after the set closed")
}
