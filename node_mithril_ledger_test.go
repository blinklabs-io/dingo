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
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mithrilLedgerBlocks is how many real preview blocks a test applies as one
// trusted batch.
const mithrilLedgerBlocks = 20

// mithrilTestLedger is a started real preview LedgerState at origin over a
// fresh core-mode store, the first preview blocks from the immutable
// testdata, and the block after them.
type mithrilTestLedger struct {
	ls     *ledger.LedgerState
	db     *database.Database
	cm     *chain.ChainManager
	blocks []gledger.Block
	next   gledger.Block
}

func newMithrilTestLedger(t *testing.T) mithrilTestLedger {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardano.EmbeddedConfigPath("preview"),
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		StorageMode: types.StorageModeCore,
	})
	require.NoError(t, err)
	bus := event.NewEventBus(nil, logger)
	cm, err := chain.NewManager(t.Context(), db, bus)
	require.NoError(t, err)
	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		EventBus:          bus,
		Logger:            logger,
		CardanoNodeConfig: nodeCfg,
		Network:           "preview",
		PromRegistry:      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	// With no upstream connection and reachedTip unlatched, the ledger loop
	// holds a lone new block for seconds waiting for more, and a read taken
	// across the catch-up is discarded and held again on resume. That leaves
	// requireIngestFree nothing under its five second wait on a slow runner.
	// These tests hand the ledger back to a live node, so latch it.
	ls.MarkReachedTipForTesting()
	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, ls.Start(ctx))
	t.Cleanup(func() {
		cancel()
		require.NoError(t, ls.Close())
		bus.Stop()
		require.NoError(t, dbtest.CloseDatabase(db))
	})

	imm, err := immutable.New(
		filepath.Join("database", "immutable", "testdata"),
	)
	require.NoError(t, err)
	iter, err := imm.BlocksInChunk(0)
	require.NoError(t, err)
	var blocks []gledger.Block
	for len(blocks) <= mithrilLedgerBlocks {
		b, err := iter.Next()
		require.NoError(t, err)
		require.NotNil(t, b, "testdata chunk 0 is too short")
		if b.IsEbb {
			continue
		}
		blk, err := gledger.NewBlockFromCbor(b.Type, b.Cbor)
		require.NoError(t, err)
		blocks = append(blocks, blk)
	}
	return mithrilTestLedger{
		ls:     ls,
		db:     db,
		cm:     cm,
		blocks: blocks[:mithrilLedgerBlocks],
		next:   blocks[mithrilLedgerBlocks],
	}
}

// realLedgerSource is a MithrilCatchUp whose catch-up the test scripts,
// reporting a certified target far above the ledger.
type realLedgerSource struct {
	catchUp func(context.Context, TrustedBatchApplier) error
}

func (realLedgerSource) SnapshotMode() bool { return false }

func (realLedgerSource) CertifiedImmutable(context.Context) (uint64, error) {
	return 1_000, nil
}

func (realLedgerSource) ImmutableForSlot(slot uint64) uint64 {
	return slot / 4_320
}

func (realLedgerSource) UseMithril(d MithrilDecision) bool {
	return d.TargetImmutable >= d.LocalImmutable+max(d.MinGapChunks, 1)
}

func (realLedgerSource) SnapshotState(
	context.Context, *database.Database,
) (MithrilSnapshotState, error) {
	return MithrilSnapshotPopulated, nil
}

func (realLedgerSource) SnapshotProgress(
	context.Context, *database.Database,
) (string, error) {
	return "", nil
}

func (realLedgerSource) RestoreSnapshot(
	context.Context, *database.Database,
) (ocommon.Point, error) {
	return ocommon.Point{}, errors.New("fake: not snapshot mode")
}

func (s realLedgerSource) CatchUp(
	ctx context.Context,
	_ *database.Database,
	applier TrustedBatchApplier,
	hold func() func(),
) error {
	release := hold()
	defer release()
	return s.catchUp(ctx, applier)
}

// newRealLedgerCoordinator wires a coordinator to a node holding a real
// LedgerState, the way startMithrilCoordinator does.
func newRealLedgerCoordinator(
	t *testing.T,
	catchUp func(context.Context, TrustedBatchApplier) error,
) (*Node, *mithrilCoordinator, mithrilTestLedger) {
	t.Helper()
	tl := newMithrilTestLedger(t)
	n := &Node{ledgerState: tl.ls, db: tl.db}
	c := &mithrilCoordinator{
		source: realLedgerSource{catchUp: catchUp},
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
	n.mithrilCoordinator = c
	return n, c, tl
}

// requireIngestFree checks the ledger is back with Ouroboros: network ingest
// is not held, no chain-ingest pause is outstanding, and the ledger's own loop
// applies the next block the primary chain receives.
func requireIngestFree(t *testing.T, tl mithrilTestLedger) {
	t.Helper()
	assert.False(t, tl.ls.NetworkIngestHeld(), "network ingest still held")
	_, err := tl.ls.ApplyTrustedBatch(t.Context(), tl.ls.Tip().Point, nil)
	require.ErrorIs(t, err, ledger.ErrChainIngestNotPaused,
		"a chain-ingest pause is still outstanding")
	require.NoError(t, tl.cm.PrimaryChain().AddBlock(
		t.Context(), tl.next, nil,
	))
	want := blockTip(tl.next)
	require.Eventually(t, func() bool {
		return tl.ls.Tip().Point.Slot == want.Slot
	}, 5*time.Second, 10*time.Millisecond,
		"the ledger loop did not apply the next block")
}

// applyHeld applies blocks through the coordinator's applier, which the real
// ledger refuses unless chain ingest is paused.
func applyHeld(
	ctx context.Context,
	ls *ledger.LedgerState,
	applier TrustedBatchApplier,
	blocks []gledger.Block,
) error {
	if !ls.NetworkIngestHeld() {
		return errors.New("catch-up ran without the network ingest hold")
	}
	_, err := applier.ApplyTrustedBatch(ctx, applier.Tip(), blocks)
	return err
}

func blockTip(b gledger.Block) ocommon.Point {
	return ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes())
}

// The coordinator composes with a real LedgerState: the catch-up applies a
// real trusted batch while network ingest is held and chain ingest paused,
// and both are handed back when it returns.
func TestCoordinatorRealLedgerHandoff(t *testing.T) {
	t.Parallel()
	var tl mithrilTestLedger
	_, c, tl := newRealLedgerCoordinator(t, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		return applyHeld(ctx, tl.ls, applier, tl.blocks)
	})
	c.cycle(t.Context())
	assert.True(t, c.suppressedUntil.IsZero(), "the catch-up failed")
	assert.Equal(t, blockTip(tl.blocks[len(tl.blocks)-1]), tl.ls.Tip().Point)
	requireIngestFree(t, tl)
}

// A catch-up that fails after committing a batch leaves the batch applied,
// hands the ledger back the same way, and backs off.
func TestCoordinatorRealLedgerError(t *testing.T) {
	t.Parallel()
	var tl mithrilTestLedger
	_, c, tl := newRealLedgerCoordinator(t, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		if err := applyHeld(ctx, tl.ls, applier, tl.blocks); err != nil {
			return err
		}
		return errors.New("immutable file 00002 digest mismatch")
	})
	c.cycle(t.Context())
	assert.False(t, c.suppressedUntil.IsZero(), "no backoff after an error")
	assert.Equal(t, blockTip(tl.blocks[len(tl.blocks)-1]), tl.ls.Tip().Point)
	assert.Zero(t, c.mithrilBPS, "a failed run must not set the rate")
	requireIngestFree(t, tl)
}

// A live Restore or Truncate aborts a run through quiesceComponentStops under
// the node's liveLifecycleMu. The real pause and hold are unwound before the
// stop returns, while the lifecycle operation still holds the lock.
func TestCoordinatorRealLedgerAbort(t *testing.T) {
	t.Parallel()
	running := make(chan struct{})
	var tl mithrilTestLedger
	n, c, tl := newRealLedgerCoordinator(t, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		if err := applyHeld(ctx, tl.ls, applier, tl.blocks); err != nil {
			return err
		}
		close(running)
		<-ctx.Done()
		return ctx.Err()
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.cycle(t.Context())
	}()
	testutil.RequireReceive(t, running, testutil.AsyncWait,
		"catch-up did not start")

	n.liveLifecycleMu.Lock()
	stops := n.quiesceComponentStops()
	require.NotEmpty(t, stops)
	require.Equal(t, "mithril catch-up", stops[0].name)
	require.NoError(t, stops[0].stop())
	requireIngestFree(t, tl)
	n.liveLifecycleMu.Unlock()

	testutil.RequireReceive(t, done, 5*time.Second, "cycle did not return")
	assert.True(t, c.suppressedUntil.IsZero(), "an abort must not back off")
	assert.Nil(t, c.sample.ledger, "the aborted run's ledger is still sampled")
	// The batch the run committed stayed, and Ouroboros' block followed it.
	assert.Equal(t, blockTip(tl.next), tl.ls.Tip().Point)
}
