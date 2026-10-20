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
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// startHeldCatchUp starts a large-gap Mithril run on n that holds the ledger,
// with the catch-up droppable indexes dropped, until it is aborted. It
// returns a channel closed once the run has returned.
func startHeldCatchUp(t *testing.T, n *Node) <-chan struct{} {
	t.Helper()
	running := make(chan struct{})
	src := &fakeMithrilSource{
		target: 10 * mithrilTunedCatchUpMinChunks,
		catchUp: func(ctx context.Context, _ TrustedBatchApplier) error {
			close(running)
			<-ctx.Done()
			return ctx.Err()
		},
	}
	c := &mithrilCoordinator{
		source: src,
		cfg: internalconfig.MithrilConfig{
			Enabled:       true,
			MinGapChunks:  4,
			CheckInterval: time.Minute,
			RetryBackoff:  30 * time.Minute,
		},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:       time.Now,
		lifecycle: &n.liveLifecycleMu,
		live:      n.mithrilLive,
	}
	n.liveLifecycleMu.Lock()
	n.mithrilCoordinator = c
	n.liveLifecycleMu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.cycle(n.ctx)
	}()
	testutil.RequireReceive(t, running, 10*time.Second,
		"the catch-up did not start")
	requireCatchUpIndexesMissing(t, n.db)
	return done
}

func missingCriticalIndexes(t *testing.T, db *database.Database) []string {
	t.Helper()
	missing, err := db.Metadata().(metadata.MissingCriticalDeferredIndexLister).
		MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	return missing
}

func requireCatchUpIndexesMissing(t *testing.T, db *database.Database) {
	t.Helper()
	require.ElementsMatch(t, catchUpIndexNames(), missingCriticalIndexes(t, db),
		"the held run did not drop the catch-up indexes")
}

// A live Truncate during a held large catch-up aborts the run without
// waiting on its index rebuild, keeps the node running, and resumes chain
// ingest on a database whose critical indexes are all present.
func TestLiveTruncateDuringHeldCatchUpRepairsIndexes(t *testing.T) {
	t.Parallel()
	const numBlocks = 20
	n, points := newLiveLifecycleTestNode(t, numBlocks)
	done := startHeldCatchUp(t, n)

	targetSlot := points[numBlocks/2].Slot
	_, err := n.Truncate(context.Background(), dblifecycle.TruncateTarget{
		Slot: &targetSlot,
	})
	require.NoError(t, err)
	testutil.RequireReceive(t, done, 10*time.Second, "the run did not end")
	require.NoError(t, n.ctx.Err(), "the truncate stopped the node")
	require.Empty(t, missingCriticalIndexes(t, n.db),
		"chain ingest resumed with a critical index missing")
}

// A live Restore of a snapshot taken mid-catch-up, while another catch-up
// holds the ledger, keeps the node running and repairs the critical indexes
// the snapshot lacks before chain ingest resumes.
func TestLiveRestoreOfMidCatchUpSnapshotRepairsIndexes(t *testing.T) {
	t.Parallel()
	n, _ := newLiveLifecycleTestNode(t, 10)
	manager, ok := n.db.Metadata().(metadata.CatchUpIndexManager)
	require.True(t, ok)
	require.NoError(t, manager.DropCatchUpIndexesContext(t.Context()))
	snapshotDir := filepath.Join(t.TempDir(), "snap")
	_, err := lifecycleSnapshot(t, n, snapshotDir)
	require.NoError(t, err)
	require.NoError(t, manager.BuildCatchUpIndexesContext(t.Context()))
	done := startHeldCatchUp(t, n)

	_, err = n.Restore(context.Background(), snapshotDir)
	require.NoError(t, err)
	testutil.RequireReceive(t, done, 10*time.Second, "the run did not end")
	require.NoError(t, n.ctx.Err(), "the restore stopped the node")
	require.Empty(t, missingCriticalIndexes(t, n.db),
		"chain ingest resumed with a critical index missing")
}

// The live reinitialization repairs the critical indexes of a database that
// carries a pending catch-up cycle before it builds the chain manager and
// ledger, independently of the Truncate and Restore repairs around it.
func TestReinitializeCoreStorageRepairsCatchUpIndexes(t *testing.T) {
	t.Parallel()
	n, _ := newLiveLifecycleTestNode(t, 10)
	manager, ok := n.db.Metadata().(metadata.CatchUpIndexManager)
	require.True(t, ok)
	require.NoError(t, manager.DropCatchUpIndexesContext(t.Context()))
	requireCatchUpIndexesMissing(t, n.db)

	n.liveLifecycleMu.Lock()
	n.networkingCoreMu.Lock()
	n.snapshotMu.Lock()
	err := n.quiesceForLiveLifecycleOp(t.Context())
	if err == nil {
		err = n.closeStorageForLiveLifecycleOp(t.Context())
	}
	if err == nil {
		err = n.reinitializeAndResume(t.Context())
	}
	n.snapshotMu.Unlock()
	n.networkingCoreMu.Unlock()
	n.liveLifecycleMu.Unlock()
	require.NoError(t, err)
	require.NoError(t, n.ctx.Err())
	require.Empty(t, missingCriticalIndexes(t, n.db),
		"the reinitialized ledger was built with a critical index missing")
}
