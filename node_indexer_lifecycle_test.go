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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// lifecycleIndexerWriter records what a live lifecycle operation does to the
// writer the node built for it.
type lifecycleIndexerWriter struct {
	deps       IndexerWriterDeps
	wakes      chan struct{}
	started    atomic.Bool
	stopped    atomic.Bool
	rollbackTo func(IndexerWriterDeps, indexerdb.Tip) error
	rollbacks  atomic.Int32
}

func (w *lifecycleIndexerWriter) Start(context.Context) error {
	w.started.Store(true)
	return nil
}

func (w *lifecycleIndexerWriter) Stop(context.Context) error {
	w.stopped.Store(true)
	return nil
}

func (w *lifecycleIndexerWriter) Wake() {
	select {
	case w.wakes <- struct{}{}:
	default:
	}
}

func (w *lifecycleIndexerWriter) RollbackTo(
	_ context.Context,
	target indexerdb.Tip,
) error {
	w.rollbacks.Add(1)
	if w.rollbackTo != nil {
		return w.rollbackTo(w.deps, target)
	}
	return nil
}

type lifecycleIndexerWriters struct {
	mu         sync.Mutex
	writers    []*lifecycleIndexerWriter
	rollbackTo func(IndexerWriterDeps, indexerdb.Tip) error
}

func (f *lifecycleIndexerWriters) build(
	deps IndexerWriterDeps,
) (IndexerWriter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w := &lifecycleIndexerWriter{
		deps:       deps,
		wakes:      make(chan struct{}, 8),
		rollbackTo: f.rollbackTo,
	}
	f.writers = append(f.writers, w)
	return w, nil
}

func (f *lifecycleIndexerWriters) all() []*lifecycleIndexerWriter {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*lifecycleIndexerWriter(nil), f.writers...)
}

// newSplitLifecycleTestNode is the live-lifecycle harness node switched to
// the split layout, with the IndexerDB open and its writer started.
func newSplitLifecycleTestNode(
	t *testing.T,
	numBlocks int,
	rollbackTo func(IndexerWriterDeps, indexerdb.Tip) error,
) (*Node, []ocommon.Point, *lifecycleIndexerWriters) {
	t.Helper()
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	// The latch a split node's first start writes; the harness database was
	// created before the layout was known.
	require.NoError(t, n.db.Metadata().SetNodeSettingsGates(
		t.Context(),
		nodesettings.Values{
			"indexer_layout": internalconfig.IndexerLayoutSplit,
		},
		0, 0,
	),
	)
	writers := &lifecycleIndexerWriters{rollbackTo: rollbackTo}
	n.config.indexerLayout = internalconfig.IndexerLayoutSplit
	n.config.indexerWriterFactory = writers.build
	require.NoError(t, n.openIndexerWriter(t.Context()))
	require.NoError(t, n.startIndexerWriter(n.ctx))
	t.Cleanup(func() {
		_ = n.stopIndexerWriter(context.Background())
		_ = n.closeIndexerDatabase()
	})
	return n, points, writers
}

func TestLiveTruncateOnSplitRollsTheIndexerBackThenTheLedger(t *testing.T) {
	t.Parallel()
	const numBlocks = 20
	var ledgerHadTheBlocks atomic.Bool
	var points []ocommon.Point
	n, pts, writers := newSplitLifecycleTestNode(
		t, numBlocks,
		func(deps IndexerWriterDeps, _ indexerdb.Tip) error {
			_, err := database.BlockByHash(
				context.Background(), deps.Consensus, points[numBlocks-1].Hash,
			)
			ledgerHadTheBlocks.Store(err == nil)
			return nil
		},
	)
	points = pts
	oldIndexerDB := n.indexerDB
	first := writers.all()[0]
	require.True(t, first.started.Load())
	// Start wakes the writer once; only a wake after the truncate counts.
	testutil.RequireReceive(
		t, first.wakes, 5*time.Second, "the wake Start sends",
	)

	targetIndex := numBlocks / 2
	targetSlot := points[targetIndex].Slot
	_, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.NoError(t, err)

	all := writers.all()
	require.Len(
		t,
		all,
		3,
		"initial, the truncate's rollback, the restarted one",
	)
	require.True(t, first.stopped.Load(), "the running writer is stopped")
	rollback := all[1]
	require.EqualValues(t, 1, rollback.rollbacks.Load())
	require.True(
		t, ledgerHadTheBlocks.Load(),
		"the IndexerDB rolls back while the ledger still stores the blocks",
	)
	require.False(t, rollback.started.Load(), "the rollback writer never runs")

	running := all[2]
	require.True(t, running.started.Load(), "the writer restarts")
	require.False(t, running.stopped.Load())
	require.Same(t, n.db, running.deps.Consensus,
		"the new writer reads the reopened ledger database")
	require.Same(t, n.indexerDB, running.deps.Indexer)
	require.NotSame(t, oldIndexerDB, n.indexerDB, "the IndexerDB is reopened")
	require.NotNil(t, n.indexerStore)
	_, closedErr := oldIndexerDB.GetSyncState("probe", nil)
	require.Error(t, closedErr, "the replaced IndexerDB is closed")

	// The reopened writer follows the new ledger's tip events; the old one
	// no longer does.
	n.eventBus.Publish(
		ledger.TipEventType,
		event.NewEvent(ledger.TipEventType, ledger.TipEvent{}),
	)
	testutil.RequireReceive(
		t, running.wakes, 5*time.Second, "a wake for the restarted writer",
	)
	testutil.RequireNoReceive(
		t, first.wakes, 200*time.Millisecond, "a wake for the stopped writer",
	)
}

func TestLiveTruncateOnSplitRefusedByTheIndexerLeavesTheLedgerWhole(
	t *testing.T,
) {
	t.Parallel()
	const numBlocks = 12
	n, points, writers := newSplitLifecycleTestNode(
		t, numBlocks,
		func(IndexerWriterDeps, indexerdb.Tip) error {
			return errors.New("indexer rollback refused")
		},
	)
	targetSlot := points[3].Slot
	_, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.ErrorContains(t, err, "indexer rollback refused")
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	tip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[numBlocks-1].Slot, tip.Point.Slot,
		"the ledger is untouched")
	all := writers.all()
	require.True(t, all[len(all)-1].started.Load(),
		"the node resumes with a running writer")
}

func TestLiveTruncateOnSplitRefusedByTheLedgerLeavesTheIndexerUntouched(
	t *testing.T,
) {
	t.Parallel()
	const numBlocks = 12
	n, points, writers := newSplitLifecycleTestNode(t, numBlocks, nil)
	// A trust boundary above the target: the ledger refuses the truncate.
	require.NoError(t, n.db.SetSyncState(
		"mithril_ledger_slot",
		strconv.FormatUint(points[numBlocks-1].Slot, 10),
		nil,
	))
	targetSlot := points[3].Slot
	_, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "Mithril trust boundary")

	for _, w := range writers.all() {
		require.Zero(t, w.rollbacks.Load(),
			"a truncate the ledger refuses must not roll the IndexerDB back")
	}
}

// Restore and Truncate both tear storage down with quiesce and close, and a
// failed step resumes through reinitializeAndResume; on the split layout that
// whole path must stop the writer, close the IndexerDB before the ledger store
// goes, and bring both back.
func TestLiveLifecycleTeardownAndResumeCoverTheIndexer(t *testing.T) {
	t.Parallel()
	n, _, writers := newSplitLifecycleTestNode(t, 6, nil)
	first := writers.all()[0]

	require.NoError(t, n.quiesceForLiveLifecycleOp(t.Context()))
	require.True(t, first.stopped.Load(), "quiesce stops the writer")
	require.Nil(t, n.indexerWriter)

	ledgerDBAtClose := n.db
	require.NoError(t, n.closeStorageForLiveLifecycleOp(t.Context()))
	require.Nil(t, n.indexerDB, "close releases the IndexerDB")
	require.Nil(t, n.indexerStore)
	require.Nil(t, n.db)
	require.NotNil(t, ledgerDBAtClose)

	require.NoError(
		t,
		n.reinitializeAndResume(context.WithoutCancel(t.Context())),
	)
	require.NotNil(t, n.indexerDB)
	require.NotNil(t, n.indexerStore)
	all := writers.all()
	require.Len(t, all, 2)
	require.True(t, all[1].started.Load())
	require.Same(t, n.db, all[1].deps.Consensus)
}

// setIndexerTip moves the IndexerDB tip to block index i of points; the
// harness chain starts at block number 1.
func setIndexerTip(
	t *testing.T,
	n *Node,
	points []ocommon.Point,
	i int,
) indexerdb.Tip {
	t.Helper()
	tip := indexerdb.Tip{
		Slot: points[i].Slot, BlockNumber: uint64(i) + 1, Hash: points[i].Hash,
	}
	txn := n.indexerDB.Transaction(t.Context(), true)
	require.NoError(t, n.indexerStore.SetTip(tip, txn))
	require.NoError(t, txn.Commit())
	return tip
}

func requireIndexerTip(t *testing.T, n *Node, want indexerdb.Tip) {
	t.Helper()
	got, err := n.indexerStore.Tip(nil)
	require.NoError(t, err)
	require.Equal(t, want.Slot, got.Slot)
	require.Equal(t, want.Hash, got.Hash)
}

func TestLiveSnapshotOnSplitCarriesTheIndexerDB(t *testing.T) {
	t.Parallel()
	n, points, _ := newSplitLifecycleTestNode(t, 12, nil)
	captured := setIndexerTip(t, n, points, 5)

	dir := snapshotTestDir(t, "snap")
	manifest, err := n.Snapshot(t.Context(), dir, "", "")
	require.NoError(t, err)

	raw, err := os.ReadFile(filepath.Join(dir, IndexerPayloadManifestName))
	require.NoError(t, err)
	var payload indexerPayload
	require.NoError(t, json.Unmarshal(raw, &payload))
	require.Equal(t, captured.Slot, payload.TipSlot)
	require.LessOrEqual(t, payload.TipSlot, manifest.TipSlot,
		"the IndexerDB is captured at or behind the ledger")
	info, err := os.Stat(filepath.Join(dir, IndexerPayloadFileName))
	require.NoError(t, err)
	require.Equal(t, info.Size(), payload.Bytes)
	require.NotEmpty(t, payload.SHA256)
	entries, err := os.ReadDir(n.config.dataDir)
	require.NoError(t, err)
	for _, e := range entries {
		require.NotContains(t, e.Name(), ".indexer-snapshot-",
			"the staging directory is removed")
	}
}

func TestLiveRestoreOnSplitReturnsTheIndexerDBToTheSnapshot(t *testing.T) {
	t.Parallel()
	n, points, writers := newSplitLifecycleTestNode(t, 12, nil)
	captured := setIndexerTip(t, n, points, 5)
	dir := snapshotTestDir(t, "snap")
	_, err := n.Snapshot(t.Context(), dir, "", "")
	require.NoError(t, err)

	// The IndexerDB moves on after the snapshot; the restore must undo that.
	setIndexerTip(t, n, points, 9)
	oldIndexerDB := n.indexerDB
	_, err = n.Restore(t.Context(), dir)
	require.NoError(t, err)

	require.NotSame(t, oldIndexerDB, n.indexerDB)
	requireIndexerTip(t, n, captured)
	all := writers.all()
	require.True(t, all[len(all)-1].started.Load(), "the writer restarts")
	tip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[11].Slot, tip.Point.Slot)
}

func TestLiveRestoreOnSplitRefusesWhatCannotRestoreTheIndexerDB(t *testing.T) {
	t.Parallel()
	t.Run("a snapshot without the payload", func(t *testing.T) {
		t.Parallel()
		n, points, _ := newSplitLifecycleTestNode(t, 6, nil)
		current := setIndexerTip(t, n, points, 3)
		dir := snapshotTestDir(t, "ledger-only")
		_, err := lifecycleSnapshot(t, n, dir)
		require.NoError(t, err)
		oldDB := n.db
		_, err = n.Restore(t.Context(), dir)
		require.ErrorContains(t, err, "no IndexerDB payload")
		require.Same(t, oldDB, n.db, "the node was not touched")
		requireIndexerTip(t, n, current)
	})
	t.Run("a payload that does not match its digest", func(t *testing.T) {
		t.Parallel()
		n, points, writers := newSplitLifecycleTestNode(t, 6, nil)
		dir := snapshotTestDir(t, "snap")
		_, err := n.Snapshot(t.Context(), dir, "", "")
		require.NoError(t, err)
		path := filepath.Join(dir, IndexerPayloadFileName)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		raw[len(raw)/2] ^= 0xff
		require.NoError(t, os.WriteFile(path, raw, 0o600))

		current := setIndexerTip(t, n, points, 4)
		_, err = n.Restore(t.Context(), dir)
		require.ErrorIs(t, err, lifecycle.ErrSnapshotPayloadMismatch)
		// The node resumed on its own data.
		requireIndexerTip(t, n, current)
		all := writers.all()
		require.True(t, all[len(all)-1].started.Load())
		tip, err := n.db.GetTip(nil)
		require.NoError(t, err)
		require.Equal(t, points[5].Slot, tip.Point.Slot)
	})
	t.Run("a snapshot trust key", func(t *testing.T) {
		t.Parallel()
		n, _, _ := newSplitLifecycleTestNode(t, 3, nil)
		keyFile := filepath.Join(testutil.OwnerOnlyTempDir(t), "trust.key")
		require.NoError(t, os.WriteFile(
			keyFile, bytes.Repeat([]byte{0x42}, 64), 0o600,
		))
		n.config.databaseLifecycle.SnapshotTrustKeyFile = keyFile
		dir := snapshotTestDir(t, "snap")
		_, err := n.Snapshot(t.Context(), dir, "", "")
		require.ErrorContains(
			t,
			err,
			"cannot be combined with a snapshot trust key",
		)
		require.NoDirExists(t, dir)
	})
	t.Run("a cloud destination", func(t *testing.T) {
		t.Parallel()
		n, _, _ := newSplitLifecycleTestNode(t, 3, nil)
		n.config.databaseLifecycle.SnapshotCloudDestination = "s3://bucket/x"
		dir := snapshotTestDir(t, "snap")
		_, err := n.Snapshot(t.Context(), dir, "", "")
		require.ErrorContains(t, err, "cloud destination")
		require.NoDirExists(t, dir)
	})
}

// A quiesce that fails without closing storage resumes through
// reinitializeAndResume, which opens the IndexerDB again. The writer still
// held at that point must be stopped and unsubscribed, not left running beside
// the new one over the same IndexerDB.
func TestOpenIndexerWriterReleasesTheWriterStillRunning(t *testing.T) {
	t.Parallel()
	n, _, writers := newSplitLifecycleTestNode(t, 6, nil)
	oldDB := n.indexerDB
	first := writers.all()[0]
	testutil.RequireReceive(
		t, first.wakes, 5*time.Second, "the wake Start sends",
	)

	require.NoError(t, n.openIndexerWriter(t.Context()))
	require.NoError(t, n.startIndexerWriter(n.ctx))

	require.True(t, first.stopped.Load(), "the writer held before is stopped")
	require.NotSame(t, oldDB, n.indexerDB)
	all := writers.all()
	require.Len(t, all, 2)
	require.Same(t, n.indexerDB, all[1].deps.Indexer)
	require.True(t, all[1].started.Load())

	testutil.RequireReceive(
		t, all[1].wakes, 5*time.Second, "the wake Start sends",
	)
	n.eventBus.Publish(
		ledger.TipEventType,
		event.NewEvent(ledger.TipEventType, ledger.TipEvent{}),
	)
	testutil.RequireReceive(
		t, all[1].wakes, 5*time.Second, "a wake for the new writer",
	)
	testutil.RequireNoReceive(
		t, first.wakes, 200*time.Millisecond, "a wake for the replaced writer",
	)
}

// A crash between the IndexerDB rollback and the ledger truncate leaves the
// IndexerDB at the target and the ledger untouched. The test stops the
// operation at that boundary with the node's hook, which resumes the node on
// the disk state a restart would find, and then runs the truncate again as an
// operator would.
func TestLiveTruncateOnSplitCrashBetweenTheStepsKeepsTheIndexerBehindTheLedger(
	t *testing.T,
) {
	t.Parallel()
	const numBlocks = 12
	const target = 5
	var points []ocommon.Point
	n, pts, writers := newSplitLifecycleTestNode(
		t, numBlocks,
		func(deps IndexerWriterDeps, to indexerdb.Tip) error {
			store, err := indexerdb.NewStore(deps.Indexer, deps.Indexer, 0)
			if err != nil {
				return err
			}
			txn := deps.Indexer.Transaction(context.Background(), true)
			if err := store.SetTip(to, txn); err != nil {
				return err
			}
			return txn.Commit()
		},
	)
	points = pts
	setIndexerTip(t, n, points, numBlocks-1)
	targetTip := indexerdb.Tip{
		Slot:        points[target].Slot,
		BlockNumber: uint64(target) + 1,
		Hash:        points[target].Hash,
	}

	crash := errors.New("crash between the truncate steps")
	n.afterIndexerRollback = func() error {
		return errors.Join(lifecycle.ErrTruncateNotStarted, crash)
	}
	targetSlot := points[target].Slot
	_, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.ErrorIs(t, err, crash)

	ledgerTip, err := n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, points[numBlocks-1].Slot, ledgerTip.Point.Slot,
		"the ledger was not truncated")
	requireIndexerTip(t, n, targetTip)
	indexerTip, err := n.indexerStore.Tip(nil)
	require.NoError(t, err)
	require.LessOrEqual(t, indexerTip.Slot, ledgerTip.Point.Slot,
		"the IndexerDB is never ahead of the ledger")
	all := writers.all()
	require.True(t, all[len(all)-1].started.Load(),
		"the node resumes with a running writer")

	n.afterIndexerRollback = nil
	removed, err := n.Truncate(
		t.Context(), dblifecycle.TruncateTarget{Slot: &targetSlot},
	)
	require.NoError(t, err)
	require.EqualValues(t, numBlocks-1-target, removed)
	ledgerTip, err = n.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, targetSlot, ledgerTip.Point.Slot)
	requireIndexerTip(t, n, targetTip)
}

// The scheduled snapshot manager of a split node carries the IndexerDB, so
// the configurations a manual snapshot refuses are refused at startup.
func TestSplitNodeSchedulesSnapshotsWithTheIndexerDB(t *testing.T) {
	t.Parallel()
	n, _, _ := newSplitLifecycleTestNode(t, 3, nil)
	n.config.databaseLifecycle.SnapshotEnabled = true
	n.config.databaseLifecycle.SnapshotDir = testutil.OwnerOnlyTempDir(t)
	n.config.databaseLifecycle.SnapshotCloudDestination = "s3://bucket/x"
	err := n.newDatabaseLifecycleManager().Start(t.Context())
	require.ErrorContains(t, err, "cloud destination")

	n.config.databaseLifecycle.SnapshotCloudDestination = ""
	keyFile := filepath.Join(testutil.OwnerOnlyTempDir(t), "trust.key")
	require.NoError(t, os.WriteFile(
		keyFile, bytes.Repeat([]byte{0x42}, 64), 0o600,
	))
	n.config.databaseLifecycle.SnapshotTrustKeyFile = keyFile
	err = n.newDatabaseLifecycleManager().Start(t.Context())
	require.ErrorContains(t, err, "snapshot trust key")
}

// refusingUtxoSet is a live UTxO set ahead of every target that holds no
// journal to roll back through.
type refusingUtxoSet struct{ tip ocommon.Point }

func (s refusingUtxoSet) Tip() (ocommon.Point, bool, error) { return s.tip, true, nil }

func (refusingUtxoSet) JournalFloor() (ocommon.Point, bool, error) {
	return ocommon.Point{}, false, nil
}

func (refusingUtxoSet) CanRollbackTo(ocommon.Point) (bool, error) {
	return false, nil
}

func (refusingUtxoSet) RollbackTo(ocommon.Point) error { return nil }

// The refusals of the live UTxO set run before the IndexerDB rolls back, so
// the set option reaches them: a truncate the set refuses leaves the IndexerDB
// where it is.
func TestIndexerRollbackForTruncateHonoursTheUtxoSetRefusal(t *testing.T) {
	t.Parallel()
	n, points, _ := newSplitLifecycleTestNode(t, 12, nil)
	current := setIndexerTip(t, n, points, 9)
	target, err := database.BlockByHash(t.Context(), n.db, points[5].Hash)
	require.NoError(t, err)

	err = n.rollbackIndexerForTruncate(
		t.Context(), n.db, target,
		lifecycle.WithTruncateUtxoSet(refusingUtxoSet{tip: points[11]}),
	)

	require.ErrorContains(t, err, "journal floor")
	requireIndexerTip(t, n, current)
}
