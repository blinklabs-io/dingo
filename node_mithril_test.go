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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeMithrilLedger records the coordinator's calls on the ledger it drives.
// The coordinator's ledger contract is the four calls, so the fake stands in
// for a LedgerState rather than mocking any ledger rule.
type fakeMithrilLedger struct {
	mu      sync.Mutex
	tip     ochainsync.Tip
	events  []string
	holds   int
	pauses  int
	applied []ochainsync.Tip
	// next is the tip each ApplyTrustedBatch moves to, in order.
	next     []ochainsync.Tip
	applyErr error
	// onPause runs when chain ingest is paused, standing in for the wait
	// for the ledger loop.
	onPause func()
	// drain, when set, stands in for DrainTrustedPrune, which is then
	// recorded as an event.
	drain func(ctx context.Context) error
}

func (f *fakeMithrilLedger) Tip() ochainsync.Tip {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tip
}

func (f *fakeMithrilLedger) record(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
}

func (f *fakeMithrilLedger) HoldNetworkIngest() func() {
	f.mu.Lock()
	f.holds++
	f.events = append(f.events, "hold")
	f.mu.Unlock()
	return sync.OnceFunc(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.holds--
		f.events = append(f.events, "release")
	})
}

func (f *fakeMithrilLedger) PauseChainIngest() func() {
	f.mu.Lock()
	f.pauses++
	f.events = append(f.events, "pause")
	onPause := f.onPause
	f.mu.Unlock()
	if onPause != nil {
		onPause()
	}
	return sync.OnceFunc(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.pauses--
		f.events = append(f.events, "resume")
	})
}

func (f *fakeMithrilLedger) DrainTrustedPrune(ctx context.Context) error {
	f.mu.Lock()
	drain := f.drain
	if drain != nil {
		f.events = append(f.events, "drain")
	}
	f.mu.Unlock()
	if drain == nil {
		return nil
	}
	return drain(ctx)
}

func (f *fakeMithrilLedger) ApplyTrustedBatch(
	_ context.Context,
	expected ocommon.Point,
	_ []gledger.Block,
) (ocommon.Point, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "apply")
	if f.applyErr != nil {
		return f.tip.Point, f.applyErr
	}
	if f.holds == 0 || f.pauses == 0 {
		return f.tip.Point, errors.New("fake: applied without hold and pause")
	}
	if expected.Slot != f.tip.Point.Slot {
		return f.tip.Point, ledger.ErrStaleBatch
	}
	f.tip, f.next = f.next[0], f.next[1:]
	f.applied = append(f.applied, f.tip)
	return f.tip.Point, nil
}

func (f *fakeMithrilLedger) state() (holds, pauses int, events []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.holds, f.pauses, append([]string(nil), f.events...)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

const testChunkSlots = 21_600

func tipAt(file, blockNumber uint64) ochainsync.Tip {
	slot := file*testChunkSlots + 100
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte(fmt.Sprintf("%032d", slot))),
		BlockNumber: blockNumber,
	}
}

// fakeMithrilSource is the Mithril half, scripted. It decides with the
// rule mithril.Decide documents, which the mithril package tests.
type fakeMithrilSource struct {
	mu       sync.Mutex
	target   uint64
	probeErr error
	probes   int
	runs     int
	catchUp  func(context.Context, TrustedBatchApplier) error
	// prepare, when set, runs before the hold, as the stream runner's
	// verification and tip locate do.
	prepare func(context.Context) error
}

func (s *fakeMithrilSource) SnapshotMode() bool { return false }

func (s *fakeMithrilSource) CertifiedImmutable(
	context.Context,
) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probes++
	return s.target, s.probeErr
}

func (s *fakeMithrilSource) ImmutableForSlot(slot uint64) uint64 {
	return slot / testChunkSlots
}

func (s *fakeMithrilSource) UseMithril(d MithrilDecision) bool {
	if d.TargetImmutable < d.LocalImmutable ||
		d.TargetImmutable-d.LocalImmutable < max(d.MinGapChunks, 1) {
		return false
	}
	if d.OuroborosBPS <= 0 || d.MithrilBPS <= 0 {
		return true
	}
	return d.MithrilBPS > 1.25*d.OuroborosBPS
}

func (s *fakeMithrilSource) CatchUp(
	ctx context.Context,
	_ *database.Database,
	applier TrustedBatchApplier,
	hold func() func(),
) error {
	s.mu.Lock()
	s.runs++
	run := s.catchUp
	prepare := s.prepare
	s.mu.Unlock()
	if run == nil {
		return errors.New("fake: unexpected catch-up")
	}
	if prepare != nil {
		if err := prepare(ctx); err != nil {
			return err
		}
	}
	release := hold()
	defer release()
	return run(ctx, applier)
}

func (s *fakeMithrilSource) SnapshotState(
	context.Context, *database.Database,
) (MithrilSnapshotState, error) {
	return MithrilSnapshotPopulated, nil
}

func (s *fakeMithrilSource) SnapshotProgress(
	context.Context, *database.Database,
) (string, error) {
	return "", nil
}

func (s *fakeMithrilSource) RestoreSnapshot(
	context.Context, *database.Database,
) (ocommon.Point, error) {
	return ocommon.Point{}, errors.New("fake: not snapshot mode")
}

func (s *fakeMithrilSource) counts() (probes, runs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes, s.runs
}

type coordinatorHarness struct {
	c         *mithrilCoordinator
	src       *fakeMithrilSource
	clock     *fakeClock
	lifecycle sync.Mutex
	mu        sync.Mutex
	ledger    *fakeMithrilLedger
}

func (h *coordinatorHarness) setLedger(l *fakeMithrilLedger) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ledger = l
}

func newCoordinatorHarness(
	t *testing.T,
	l *fakeMithrilLedger,
	target uint64,
	run func(context.Context, TrustedBatchApplier) error,
) *coordinatorHarness {
	t.Helper()
	h := &coordinatorHarness{
		src:    &fakeMithrilSource{target: target, catchUp: run},
		clock:  &fakeClock{now: time.Unix(1_700_000_000, 0)},
		ledger: l,
	}
	h.c = &mithrilCoordinator{
		source: h.src,
		cfg: internalconfig.MithrilConfig{
			Enabled:       true,
			MinGapChunks:  4,
			CheckInterval: time.Minute,
			RetryBackoff:  30 * time.Minute,
		},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:       h.clock.Now,
		lifecycle: &h.lifecycle,
		live: func() mithrilLive {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.ledger == nil {
				return mithrilLive{}
			}
			return mithrilLive{ledger: h.ledger}
		},
	}
	return h
}

// An aggregator that cannot be reached fails the probe before anything is
// held, so Ouroboros never stops, and Mithril is not tried again until the
// retry backoff has passed.
func TestCoordinatorAggregatorDown(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{tip: tipAt(0, 10)}
	h := newCoordinatorHarness(t, l, 0, nil)
	h.src.probeErr = errors.New(
		"dial tcp 127.0.0.1:9: connect: connection refused",
	)

	h.c.cycle(t.Context())
	probes, runs := h.src.counts()
	assert.Equal(t, 1, probes)
	assert.Zero(t, runs)
	holds, pauses, events := l.state()
	assert.Zero(t, holds)
	assert.Zero(t, pauses)
	assert.Empty(t, events, "a failed probe must not hold or pause the ledger")

	h.clock.Advance(h.c.cfg.RetryBackoff - time.Second)
	h.c.cycle(t.Context())
	probes, _ = h.src.counts()
	assert.Equal(t, 1, probes, "retried inside the backoff")

	h.clock.Advance(time.Second)
	h.c.cycle(t.Context())
	probes, _ = h.src.counts()
	assert.Equal(t, 2, probes, "not retried after the backoff")
	_, _, events = l.state()
	assert.Empty(t, events)
}

// With a gap worth syncing the coordinator holds and pauses the ledger, runs
// the catch-up to the target, then releases both, and a later check with no
// gap leaves Ouroboros alone.
func TestCoordinatorCatchUpThenHandoff(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(5, 510), tipAt(10, 1010)},
	}
	var h *coordinatorHarness
	h = newCoordinatorHarness(t, l, 10, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		holds, pauses, _ := l.state()
		if holds != 1 || pauses != 1 {
			return fmt.Errorf("fake: holds %d pauses %d", holds, pauses)
		}
		for range 2 {
			if _, err := applier.ApplyTrustedBatch(
				ctx, applier.Tip(), nil,
			); err != nil {
				return err
			}
			h.clock.Advance(5 * time.Second)
		}
		return nil
	})

	h.c.cycle(t.Context())
	_, runs := h.src.counts()
	require.Equal(t, 1, runs)
	holds, pauses, events := l.state()
	assert.Zero(t, holds, "network hold not released")
	assert.Zero(t, pauses, "ingest pause not resumed")
	assert.Equal(t,
		[]string{"hold", "pause", "apply", "apply", "resume", "release"},
		events,
	)
	assert.Equal(t, tipAt(10, 1010), l.Tip())
	assert.InDelta(t, 100.0, h.c.mithrilBPS, 0.001, "1000 blocks in 10s")
	assert.True(t, h.c.suppressedUntil.IsZero(), "success must not back off")

	// The ledger now holds the target's file: no gap, so no hold.
	h.clock.Advance(h.c.cfg.CheckInterval)
	h.c.cycle(t.Context())
	probes, runs := h.src.counts()
	assert.Equal(t, 2, probes)
	assert.Equal(t, 1, runs, "ran Mithril with no gap")
	_, _, after := l.state()
	assert.Equal(t, events, after, "touched the ledger with no gap")
}

// A catch-up that fails after two committed batches, as a digest mismatch at
// chunk 2 does, leaves those batches applied, releases both holds so
// Ouroboros continues from chunk 1's tip, and backs off.
func TestCoordinatorErrorMidStream(t *testing.T) {
	t.Parallel()
	chunk1 := tipAt(1, 30)
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(0, 20), chunk1},
	}
	errMismatch := errors.New("immutable file 00002 digest mismatch")
	h := newCoordinatorHarness(t, l, 10, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		for range 2 {
			if _, err := applier.ApplyTrustedBatch(
				ctx, applier.Tip(), nil,
			); err != nil {
				return err
			}
		}
		return fmt.Errorf("downloading chunk 2: %w", errMismatch)
	})

	h.c.cycle(t.Context())
	holds, pauses, events := l.state()
	assert.Zero(t, holds)
	assert.Zero(t, pauses)
	assert.Equal(t,
		[]string{"hold", "pause", "apply", "apply", "resume", "release"},
		events,
	)
	assert.Equal(t, chunk1, l.Tip(), "committed batches must stay applied")
	assert.Equal(t,
		h.clock.Now().Add(h.c.cfg.RetryBackoff), h.c.suppressedUntil,
	)

	h.clock.Advance(h.c.cfg.CheckInterval)
	h.c.cycle(t.Context())
	probes, runs := h.src.counts()
	assert.Equal(t, 1, probes, "retried inside the backoff")
	assert.Equal(t, 1, runs)
}

// A caught-up node logs its every-check decision at Debug; a gap worth
// weighing is logged at Info whichever source wins.
func TestCoordinatorDecisionLogLevel(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		target uint64
		info   bool
	}{
		{"no gap", 2, false},
		{"gap", 10, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &fakeMithrilLedger{tip: tipAt(0, 10)}
			h := newCoordinatorHarness(t, l, tc.target, func(
				context.Context, TrustedBatchApplier,
			) error {
				return nil
			})
			var buf bytes.Buffer
			h.c.logger = slog.New(slog.NewTextHandler(
				&buf, &slog.HandlerOptions{Level: slog.LevelInfo},
			))
			h.c.cycle(t.Context())
			assert.Equal(t, tc.info,
				strings.Contains(buf.String(), "sync source decision"),
				buf.String())
		})
	}
}

// A catch-up prepares with Ouroboros still running and takes the hold only
// to apply, so a failure while preparing (a degraded aggregator, say) never
// holds Ouroboros at all, and still falls back with the backoff.
func TestCoordinatorPreparesUnheld(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{tip: tipAt(0, 10)}
	errSlow := errors.New("certificate request timed out")
	h := newCoordinatorHarness(t, l, 10, func(
		context.Context, TrustedBatchApplier,
	) error {
		return errors.New("fake: applied after a failed prepare")
	})
	var heldWhilePreparing int
	h.src.prepare = func(context.Context) error {
		heldWhilePreparing, _, _ = l.state()
		return errSlow
	}

	h.c.cycle(t.Context())
	_, _, events := l.state()
	assert.Zero(t, heldWhilePreparing)
	assert.Empty(t, events, "Ouroboros was held for a run that applied nothing")
	assert.Equal(t,
		h.clock.Now().Add(h.c.cfg.RetryBackoff), h.c.suppressedUntil,
	)
	assert.Zero(t, h.c.mithrilBPS)
}

// A catch-up that panics still hands the ledger back before the panic
// leaves the coordinator.
func TestCoordinatorPanicReleasesHolds(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{tip: tipAt(0, 10)}
	h := newCoordinatorHarness(t, l, 10, func(
		context.Context, TrustedBatchApplier,
	) error {
		panic("fake: catch-up panicked")
	})
	require.Panics(t, func() { h.c.cycle(t.Context()) })
	holds, pauses, events := l.state()
	assert.Zero(t, holds)
	assert.Zero(t, pauses)
	assert.Equal(t, []string{"hold", "pause", "resume", "release"}, events)
	h.c.runMu.Lock()
	defer h.c.runMu.Unlock()
	assert.Nil(t, h.c.runCancel, "a panicked run stays registered")
}

// A live Restore or Truncate aborts a run in progress from under
// liveLifecycleMu and waits for it to hand back. The abort is not a Mithril
// failure, so nothing backs off, and the next cycle holds the rebuilt ledger.
func TestCoordinatorAbortOnLiveLifecycle(t *testing.T) {
	t.Parallel()
	old := &fakeMithrilLedger{tip: tipAt(0, 10)}
	running := make(chan struct{})
	h := newCoordinatorHarness(t, old, 10, func(
		ctx context.Context, _ TrustedBatchApplier,
	) error {
		close(running)
		<-ctx.Done()
		return ctx.Err()
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.c.cycle(t.Context())
	}()
	testutil.RequireReceive(t, running, 5*time.Second, "catch-up did not start")

	rebuilt := &fakeMithrilLedger{tip: tipAt(0, 10)}
	h.lifecycle.Lock()
	h.c.abortRun()
	holds, pauses, _ := old.state()
	assert.Zero(t, holds, "abort returned before the hold was released")
	assert.Zero(t, pauses, "abort returned before the pause was resumed")
	h.setLedger(rebuilt)
	h.lifecycle.Unlock()
	testutil.RequireReceive(t, done, 5*time.Second, "cycle did not return")
	assert.True(t, h.c.suppressedUntil.IsZero(),
		"an aborted run must not back off")

	h.src.mu.Lock()
	h.src.catchUp = func(context.Context, TrustedBatchApplier) error {
		holds, _, _ := rebuilt.state()
		if holds != 1 {
			return errors.New("fake: rebuilt ledger not held")
		}
		return nil
	}
	h.src.mu.Unlock()
	h.clock.Advance(h.c.cfg.CheckInterval)
	h.c.cycle(t.Context())
	assert.True(t, h.c.suppressedUntil.IsZero(), "rebuilt ledger not held")
	_, _, events := rebuilt.state()
	assert.Equal(t, []string{"hold", "pause", "resume", "release"}, events)
	_, _, oldEvents := old.state()
	assert.Equal(t, []string{"hold", "pause", "resume", "release"}, oldEvents)
}

// A ledger rebuilt between the probe and the run is neither held on the old
// ledger's behalf nor on the new one's; the next check decides afresh.
func TestCoordinatorSkipsLedgerRebuiltAfterProbe(t *testing.T) {
	t.Parallel()
	old := &fakeMithrilLedger{tip: tipAt(0, 10)}
	rebuilt := &fakeMithrilLedger{tip: tipAt(0, 10)}
	h := newCoordinatorHarness(t, old, 10, nil)
	h.c.live = func() mithrilLive {
		probes, _ := h.src.counts()
		if probes == 0 {
			return mithrilLive{ledger: old}
		}
		return mithrilLive{ledger: rebuilt}
	}
	h.c.cycle(t.Context())
	_, runs := h.src.counts()
	assert.Zero(t, runs)
	_, _, events := old.state()
	assert.Empty(t, events)
	_, _, events = rebuilt.state()
	assert.Empty(t, events)
}

// Ouroboros' rate comes from ledger tip movement between checks, and Mithril
// is chosen over a known rate only past the hysteresis factor.
func TestCoordinatorWeighsMeasuredRates(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		mithrilBPS float64
		wantRun    bool
	}{
		{"mithril 1.2x", 120, false},
		{"mithril 1.3x", 130, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &fakeMithrilLedger{tip: tipAt(0, 0)}
			h := newCoordinatorHarness(t, l, 100, func(
				context.Context, TrustedBatchApplier,
			) error {
				return nil
			})
			h.c.mithrilBPS = tc.mithrilBPS
			// The first check samples the tip; the tip it measures
			// against is the one Ouroboros moved meanwhile.
			h.c.sample = mithrilTipSample{
				ledger: l, tip: l.Tip(), at: h.clock.Now(),
			}
			h.clock.Advance(10 * time.Second)
			l.mu.Lock()
			l.tip = tipAt(0, 1000)
			l.mu.Unlock()
			h.c.cycle(t.Context())
			_, runs := h.src.counts()
			assert.Equal(t, tc.wantRun, runs == 1)
		})
	}
}

// The coordinator starts only with a Mithril half to drive, and a Mithril
// half that rejects the configuration leaves the node on Ouroboros.
func TestStartMithrilCoordinator(t *testing.T) {
	t.Parallel()
	newNode := func(factory MithrilCatchUpFactory) *Node {
		return &Node{config: Config{
			cfg: &internalconfig.Config{Mithril: internalconfig.MithrilConfig{
				Enabled:       true,
				CheckInterval: time.Hour,
				RetryBackoff:  time.Hour,
			}},
			logger:         slog.New(slog.DiscardHandler),
			mithrilCatchUp: factory,
		}}
	}
	n := newNode(nil)
	n.mithrilSource = n.newMithrilSource()
	n.startMithrilCoordinator(t.Context())()
	assert.Nil(t, n.mithrilCoordinator, "started without a Mithril half")

	n = newNode(func(MithrilCatchUpParams) (MithrilCatchUp, error) {
		return nil, errors.New("no genesis key")
	})
	n.mithrilSource = n.newMithrilSource()
	n.startMithrilCoordinator(t.Context())()
	assert.Nil(t, n.mithrilCoordinator, "started on a rejected config")

	n = newNode(func(MithrilCatchUpParams) (MithrilCatchUp, error) {
		return nil, nil
	})
	n.mithrilSource = n.newMithrilSource()
	n.startMithrilCoordinator(t.Context())()
	assert.Nil(t, n.mithrilCoordinator, "started in manual mode")

	src := &fakeMithrilSource{}
	var got MithrilCatchUpParams
	n = newNode(func(p MithrilCatchUpParams) (MithrilCatchUp, error) {
		got = p
		return src, nil
	})
	n.config.dataDir = "/data/dir"
	n.mithrilSource = n.newMithrilSource()
	stop := n.startMithrilCoordinator(t.Context())
	require.NotNil(t, n.mithrilCoordinator)
	stop()
	assert.Equal(t, "/data/dir", got.DataDir)
	assert.Equal(t, StorageModeCore, got.StorageMode,
		"an unset storage mode is core")
	assert.True(t, got.Mithril.Enabled)
	probes, _ := src.counts()
	assert.Zero(t, probes, "checked with no ledger to hold")
}

// quiesceComponentStops aborts a Mithril run before anything else stops, so a
// live Restore, Truncate or shutdown never closes the ledger under one.
func TestQuiesceStopsMithrilRunFirst(t *testing.T) {
	t.Parallel()
	n := &Node{}
	assert.Empty(t, n.quiesceComponentStops())
	n.mithrilCoordinator = &mithrilCoordinator{}
	stops := n.quiesceComponentStops()
	require.NotEmpty(t, stops)
	assert.Equal(t, "mithril catch-up", stops[0].name)
	require.NoError(t, stops[0].stop())
}

// The Mithril rate comes only from a successful run long enough to measure,
// timed from when the hold and pause are in place.
func TestCoordinatorMithrilRate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		blocks  uint64
		err     error
		wantBPS float64
	}{
		{"long successful run", 2_000, nil, 200},
		{"short successful run", minMithrilRateBlocks - 1, nil, 0},
		{"long failed run", 2_000, errors.New("digest mismatch"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &fakeMithrilLedger{
				tip:  tipAt(0, 10),
				next: []ochainsync.Tip{tipAt(9, 10+tc.blocks)},
			}
			var h *coordinatorHarness
			h = newCoordinatorHarness(t, l, 100, func(
				ctx context.Context, applier TrustedBatchApplier,
			) error {
				if _, err := applier.ApplyTrustedBatch(
					ctx, applier.Tip(), nil,
				); err != nil {
					return err
				}
				h.clock.Advance(10 * time.Second)
				return tc.err
			})
			// An hour waiting for the ledger loop is not Mithril's time.
			l.onPause = func() { h.clock.Advance(time.Hour) }
			h.c.cycle(t.Context())
			assert.InDelta(t, tc.wantBPS, h.c.mithrilBPS, 0.001)
		})
	}
}

// A held run drains the consumed-UTxO cleanup its trusted batches still owe
// before it resumes chain ingest, so Ouroboros starts without that backlog.
func TestCoordinatorDrainsTrustedPruneBeforeResume(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(10, 1010)},
	}
	drained := make(chan error, 1)
	l.drain = func(ctx context.Context) error {
		drained <- ctx.Err()
		return nil
	}
	h := newCoordinatorHarness(t, l, 10, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		_, err := applier.ApplyTrustedBatch(ctx, applier.Tip(), nil)
		return err
	})
	h.c.cycle(t.Context())
	err := testutil.RequireReceive(t, drained, time.Second,
		"the release did not drain the trusted cleanup")
	require.NoError(t, err, "the drain ran on a cancelled context")
	_, _, events := l.state()
	assert.Equal(t,
		[]string{"hold", "pause", "apply", "drain", "resume", "release"},
		events,
	)
}

// A live Restore or Truncate that aborts a run while its release drains the
// trusted cleanup stops the drain, so abortRun, and the lifecycle quiesce
// waiting on it, does not wait the drain out on a database about to be
// replaced.
func TestCoordinatorAbortStopsTrustedPruneDrain(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(10, 1010)},
	}
	started := make(chan struct{})
	cause := make(chan error, 1)
	l.drain = func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return ctx.Err()
	}
	h := newCoordinatorHarness(t, l, 10, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		_, err := applier.ApplyTrustedBatch(ctx, applier.Tip(), nil)
		return err
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.c.cycle(t.Context())
	}()
	testutil.RequireReceive(t, started, 5*time.Second,
		"the release never drained the trusted cleanup")
	aborted := make(chan struct{})
	go func() {
		defer close(aborted)
		h.c.lifecycle.Lock()
		defer h.c.lifecycle.Unlock()
		h.c.abortRun()
	}()
	testutil.RequireReceive(t, aborted, 5*time.Second,
		"abortRun waited out the trusted cleanup drain")
	require.ErrorIs(t,
		testutil.RequireReceive(t, cause, time.Second, "the drain did not end"),
		errMithrilLifecycleAbort,
	)
	testutil.RequireReceive(t, done, 5*time.Second, "the run did not end")
	holds, pauses, _ := l.state()
	require.Zero(t, holds)
	require.Zero(t, pauses)
}

// A run a live Restore or Truncate aborted before its release does not drain
// the trusted cleanup at all: the operation replaces or rewrites the database
// it would prune.
func TestCoordinatorAbortSkipsTrustedPruneDrain(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{tip: tipAt(0, 10)}
	l.drain = func(context.Context) error { return nil }
	running := make(chan struct{})
	h := newCoordinatorHarness(t, l, 10, func(
		ctx context.Context, _ TrustedBatchApplier,
	) error {
		close(running)
		<-ctx.Done()
		return ctx.Err()
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.c.cycle(t.Context())
	}()
	testutil.RequireReceive(t, running, 5*time.Second, "catch-up did not start")
	h.lifecycle.Lock()
	h.c.abortRun()
	h.lifecycle.Unlock()
	testutil.RequireReceive(t, done, 5*time.Second, "cycle did not return")
	_, _, events := l.state()
	assert.Equal(t, []string{"hold", "pause", "resume", "release"}, events)
}
