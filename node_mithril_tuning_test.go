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
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/plannerstats"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingTuning records begin and end on the fake ledger's event list, and
// whether the ledger was still held at each.
type recordingTuning struct {
	l *fakeMithrilLedger
}

func (r recordingTuning) begin(
	_ context.Context,
	scope mithrilTuningScope,
) mithrilTuned {
	holds, pauses, _ := r.l.state()
	r.l.record(fmt.Sprintf("tune large=%t held=%t", scope.largeGap,
		holds > 0 && pauses > 0))
	return mithrilTuned{
		restore: func(ctx context.Context) {
			holds, pauses, _ := r.l.state()
			r.l.record(fmt.Sprintf("rebuild held=%t cancelled=%t",
				holds > 0 && pauses > 0, ctx.Err() != nil))
		},
		end: func(context.Context) {
			holds, pauses, _ := r.l.state()
			r.l.record(fmt.Sprintf("untune held=%t", holds > 0 || pauses > 0))
		},
	}
}

func runTunedCatchUp(
	t *testing.T,
	target uint64,
	panics bool,
	run func(*fakeMithrilLedger) func(context.Context, TrustedBatchApplier) error,
) []string {
	t.Helper()
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(target, 5000)},
	}
	h := newCoordinatorHarness(t, l, target, run(l))
	live := h.c.live
	h.c.live = func() mithrilLive {
		m := live()
		m.tuning = recordingTuning{l: l}
		return m
	}
	if panics {
		require.Panics(t, func() { h.c.cycle(t.Context()) })
	} else {
		h.c.cycle(t.Context())
	}
	_, runs := h.src.counts()
	require.Equal(t, 1, runs)
	holds, pauses, events := l.state()
	require.Zero(t, holds)
	require.Zero(t, pauses)
	return events
}

func applyOnce(
	*fakeMithrilLedger,
) func(context.Context, TrustedBatchApplier) error {
	return func(ctx context.Context, applier TrustedBatchApplier) error {
		_, err := applier.ApplyTrustedBatch(ctx, applier.Tip(), nil)
		return err
	}
}

// A run tunes the database once it holds the ledger, tunes it fully only
// for a gap of mithrilTunedCatchUpMinChunks or more, and undoes the tuning
// after the hold is released.
func TestCoordinatorTunesDatabaseInsideHold(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		target uint64
		large  bool
	}{
		{name: "large gap", target: mithrilTunedCatchUpMinChunks, large: true},
		{name: "small gap", target: mithrilTunedCatchUpMinChunks - 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			events := runTunedCatchUp(t, tc.target, false, applyOnce)
			assert.Equal(t, []string{
				"hold", "pause",
				fmt.Sprintf("tune large=%t held=true", tc.large),
				"apply", "rebuild held=true cancelled=false",
				"resume", "release",
				"untune held=false",
			}, events)
		})
	}
}

// A run that panics after the hold still undoes the tuning, after the hold.
func TestCoordinatorUntunesAfterPanic(t *testing.T) {
	t.Parallel()
	events := runTunedCatchUp(t, mithrilTunedCatchUpMinChunks, true,
		func(*fakeMithrilLedger) func(
			context.Context, TrustedBatchApplier,
		) error {
			return func(context.Context, TrustedBatchApplier) error {
				panic("fake: catch-up panic")
			}
		},
	)
	assert.Equal(t, []string{
		"hold", "pause", "tune large=true held=true",
		"rebuild held=true cancelled=false",
		"resume", "release", "untune held=false",
	}, events)
}

// Every way a held run ends on its own rebuilds the catch-up indexes before
// Ouroboros gets the ledger back. A node shutdown cancels the rebuild. A run
// a live Restore or Truncate aborted skips it, so the lifecycle operation
// does not wait on it; the reinitialization repairs whatever database it
// resumes on before chain ingest.
func TestCoordinatorRebuildsBeforeReleaseOnEveryExit(t *testing.T) {
	t.Parallel()
	failWith := func(err error) func(
		*fakeMithrilLedger,
	) func(context.Context, TrustedBatchApplier) error {
		return func(*fakeMithrilLedger) func(
			context.Context, TrustedBatchApplier,
		) error {
			return func(ctx context.Context, applier TrustedBatchApplier) error {
				if _, err := applier.ApplyTrustedBatch(
					ctx, applier.Tip(), nil,
				); err != nil {
					return err
				}
				return err
			}
		}
	}
	for _, tc := range []struct {
		name string
		run  func(*fakeMithrilLedger) func(
			context.Context, TrustedBatchApplier,
		) error
	}{
		{name: "error", run: failWith(errors.New("fake: download failed"))},
		{name: "watchdog", run: failWith(fmt.Errorf(
			"fake: %w", errFakeStreamStalled,
		))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			events := runTunedCatchUp(
				t, mithrilTunedCatchUpMinChunks, false, tc.run,
			)
			assert.Equal(t, []string{
				"hold", "pause", "tune large=true held=true", "apply",
				"rebuild held=true cancelled=false",
				"resume", "release", "untune held=false",
			}, events)
		})
	}
	for _, tc := range []struct {
		name      string
		cancelled bool
		rebuild   string
	}{
		{name: "restore or truncate abort"},
		{
			name: "shutdown", cancelled: true,
			rebuild: "rebuild held=true cancelled=true",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &fakeMithrilLedger{
				tip: tipAt(0, 10),
				next: []ochainsync.Tip{
					tipAt(mithrilTunedCatchUpMinChunks, 5000),
				},
			}
			applied := make(chan struct{})
			h := newCoordinatorHarness(t, l, mithrilTunedCatchUpMinChunks,
				func(ctx context.Context, applier TrustedBatchApplier) error {
					if _, err := applier.ApplyTrustedBatch(
						ctx, applier.Tip(), nil,
					); err != nil {
						return err
					}
					close(applied)
					<-ctx.Done()
					return ctx.Err()
				})
			live := h.c.live
			h.c.live = func() mithrilLive {
				m := live()
				m.tuning = recordingTuning{l: l}
				return m
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				h.c.cycle(ctx)
			}()
			testutil.RequireReceive(t, applied, testutil.AsyncWait,
				"the run never applied")
			if tc.cancelled {
				cancel()
			} else {
				h.c.lifecycle.Lock()
				h.c.abortRun()
				h.c.lifecycle.Unlock()
			}
			testutil.RequireReceive(t, done, testutil.AsyncWait,
				"the run did not end")
			holds, pauses, events := l.state()
			require.Zero(t, holds)
			require.Zero(t, pauses)
			want := []string{
				"hold", "pause", "tune large=true held=true", "apply",
			}
			if tc.rebuild != "" {
				want = append(want, tc.rebuild)
			}
			want = append(want, "resume", "release", "untune held=false")
			assert.Equal(t, want, events)
		})
	}
}

// errFakeStreamStalled stands in for mithril.ErrStreamStalled, which this
// package cannot import.
var errFakeStreamStalled = errors.New("fake: no batch within the timeout")

// countingPlannerUpdater counts planner-statistics runs on the real manager.
type countingPlannerUpdater struct {
	calls atomic.Int64
}

func (u *countingPlannerUpdater) OptimizePlannerStatsContext(
	context.Context, string,
) (types.PlannerStatsResult, error) {
	u.calls.Add(1)
	return types.PlannerStatsResult{Supported: true}, nil
}

func newTuningTestDatabase(t *testing.T, dir string) *database.Database {
	t.Helper()
	return newTuningTestDatabaseMode(t, dir, types.StorageModeCore)
}

func newTuningTestDatabaseMode(
	t *testing.T,
	dir string,
	mode string,
) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     dir,
		StorageMode: mode,
	})
	require.NoError(t, err)
	return db
}

func deferredIndexState(
	t *testing.T,
	db *database.Database,
) (missing []string, pending bool) {
	t.Helper()
	store := db.Metadata()
	lister, ok := store.(metadata.MissingDeferredIndexLister)
	require.True(t, ok)
	missing, err := lister.MissingDeferredIndexes()
	require.NoError(t, err)
	manager, ok := store.(metadata.DeferredIndexManager)
	require.True(t, ok)
	pending, err = manager.HasDeferredIndexesPending()
	require.NoError(t, err)
	return missing, pending
}

func indexNames(indexes []deferred.Index) []string {
	var names []string
	for _, index := range indexes {
		names = append(names, index.Name)
	}
	return names
}

func lazyIndexNames() []string {
	return indexNames(deferred.LazyManifest())
}

func catchUpIndexNames() []string {
	return indexNames(deferred.CatchUpDroppableManifest())
}

// A large-gap core run on SQLite drops the lazy and catch-up droppable
// indexes behind the recovery marker and holds planner maintenance back. The
// restore inside the hold rebuilds the droppable ones and keeps the marker;
// the end rebuilds the lazy ones, clears the marker and lets one planner run
// through. A small-gap run only holds the planner back.
func TestNodeMithrilTuningLazyIndexesAndPlanner(t *testing.T) {
	t.Parallel()
	db := newTuningTestDatabase(t, t.TempDir())
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	updater := &countingPlannerUpdater{}
	planner := plannerstats.NewManager(updater, bus, discardLogger())
	require.NoError(t, planner.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, planner.Stop()) })
	tuning := nodeMithrilTuning{
		db:      db,
		planner: planner,
		logger:  discardLogger(),
	}
	epoch := func() {
		bus.Publish(event.EpochTransitionEventType, event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{NewEpoch: 5, EpochNonce: []byte{1}},
		))
	}

	tuned := tuning.begin(t.Context(), fullScope)
	missing, pending := deferredIndexState(t, db)
	require.True(t, pending, "a dropped index must be recorded as pending")
	require.NotEmpty(t, catchUpIndexNames())
	require.ElementsMatch(t,
		append(lazyIndexNames(), catchUpIndexNames()...), missing,
		"only the lazy and catch-up droppable indexes may be dropped")
	epoch()
	require.Never(t, func() bool { return updater.calls.Load() != 0 },
		200*time.Millisecond, 10*time.Millisecond,
		"a planner run during the catch-up was not deferred")

	tuned.restore(t.Context())
	missing, pending = deferredIndexState(t, db)
	require.ElementsMatch(t, lazyIndexNames(), missing,
		"the restore must rebuild exactly the catch-up droppable indexes")
	require.True(t, pending, "the lazy indexes are still missing")
	require.Never(t, func() bool { return updater.calls.Load() != 0 },
		50*time.Millisecond, 10*time.Millisecond,
		"the planner ran before the run ended")

	tuned.end(t.Context())
	missing, pending = deferredIndexState(t, db)
	require.Empty(t, missing, "the lazy indexes were not rebuilt")
	require.False(t, pending, "the recovery marker was not cleared")
	require.Eventually(t, func() bool { return updater.calls.Load() == 1 },
		5*time.Second, 10*time.Millisecond,
		"the deferred planner run did not follow the rebuild")

	tuned = tuning.begin(t.Context(), mithrilTuningScope{})
	missing, pending = deferredIndexState(t, db)
	require.Empty(t, missing, "a small gap dropped an index")
	require.False(t, pending)
	epoch()
	require.Never(t, func() bool { return updater.calls.Load() != 1 },
		200*time.Millisecond, 10*time.Millisecond)
	tuned.restore(t.Context())
	tuned.end(t.Context())
	require.Eventually(t, func() bool { return updater.calls.Load() == 2 },
		5*time.Second, 10*time.Millisecond)
}

// A process that dies inside a large-gap run, before the restore, leaves the
// lazy and catch-up droppable indexes dropped and the marker set. The
// restarted node rebuilds every critical index before it builds the ledger
// that ingests, and deferred-index maintenance rebuilds the rest.
func TestNodeMithrilTuningCrashLeavesRepairableState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	db := newTuningTestDatabase(t, dir)
	tuning := nodeMithrilTuning{db: db, logger: discardLogger()}
	_ = tuning.begin(t.Context(), fullScope)
	require.NoError(t, dbtest.CloseDatabase(db))

	db = newTuningTestDatabase(t, dir)
	missing, pending := deferredIndexState(t, db)
	require.True(t, pending)
	require.ElementsMatch(t,
		append(lazyIndexNames(), catchUpIndexNames()...), missing)
	lister := db.Metadata().(metadata.MissingCriticalDeferredIndexLister)
	critical, err := lister.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.ElementsMatch(t, catchUpIndexNames(), critical)

	n := &Node{db: db}
	n.config.logger = discardLogger()
	n.config.storageMode = StorageModeCore
	require.NoError(t, n.repairCriticalIndexesBeforeIngest(t.Context()))
	critical, err = lister.MissingCriticalDeferredIndexes()
	require.NoError(t, err)
	require.Empty(t, critical,
		"startup left a rollback index missing for chain ingest")
	missing, pending = deferredIndexState(t, db)
	require.True(t, pending, "the lazy rebuild is still outstanding")
	require.ElementsMatch(t, lazyIndexNames(), missing)

	stop := n.startDeferredIndexMaintenance()
	t.Cleanup(stop)
	testutil.RequireReceive(t, n.deferredIndexMaintenanceDone, 10*time.Second,
		"startup maintenance did not finish")
	missing, pending = deferredIndexState(t, db)
	require.Empty(t, missing)
	require.False(t, pending)
}

// fullScope is a large-gap run that also drops the catch-up indexes.
var fullScope = mithrilTuningScope{largeGap: true, catchUpIndexes: true}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMithrilCatchUpTuningBackend(t *testing.T) {
	t.Parallel()
	for plugin, want := range map[string]bool{
		"":         true,
		"sqlite":   true,
		"postgres": false,
		"mysql":    false,
	} {
		assert.Equal(t, want, mithrilCatchUpTuningBackend(plugin), plugin)
	}
}

// A large-gap run on a database another backend produced keeps every index:
// those backends rebuild in one transaction that would stall Ouroboros.
func TestNodeMithrilTuningSkipsNonSQLiteBackends(t *testing.T) {
	t.Parallel()
	db := newTuningTestDatabase(t, t.TempDir())
	// The decision reads only the recorded plugin name, so relabelling a
	// SQLite database stands in for one PostgreSQL produced.
	db.Config().MetadataPlugin = "postgres"
	tuning := nodeMithrilTuning{db: db, logger: discardLogger()}
	tuned := tuning.begin(t.Context(), fullScope)
	missing, pending := deferredIndexState(t, db)
	require.Empty(t, missing, "a non-SQLite backend lost an index")
	require.False(t, pending)
	tuned.restore(t.Context())
	tuned.end(t.Context())
}

// The catch-up droppable indexes are dropped only for a core-mode database:
// API storage serves rollback-independent queries off them throughout, so an
// API database keeps them and loses only the lazy entries.
func TestNodeMithrilTuningKeepsCatchUpIndexesInAPIMode(t *testing.T) {
	t.Parallel()
	db := newTuningTestDatabaseMode(t, t.TempDir(), types.StorageModeAPI)
	tuning := nodeMithrilTuning{db: db, logger: discardLogger()}
	tuned := tuning.begin(t.Context(), fullScope)
	missing, pending := deferredIndexState(t, db)
	require.ElementsMatch(t, lazyIndexNames(), missing,
		"an API-mode run dropped a catch-up droppable index")
	require.True(t, pending)
	tuned.restore(t.Context())
	tuned.end(t.Context())
	missing, pending = deferredIndexState(t, db)
	require.Empty(t, missing)
	require.False(t, pending)
}

// clockTuning stands in for an in-hold rebuild that takes an hour.
type clockTuning struct {
	advance func()
}

func (c clockTuning) begin(
	context.Context,
	mithrilTuningScope,
) mithrilTuned {
	return mithrilTuned{
		restore: func(context.Context) { c.advance() },
		end:     func(context.Context) {},
	}
}

// The in-hold rebuild is not Mithril's throughput: the rate stops at the
// release, before the rebuild, so a long rebuild does not bias the next
// decision against Mithril.
func TestCoordinatorRateExcludesInHoldRebuild(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(mithrilTunedCatchUpMinChunks, 2_010)},
	}
	var h *coordinatorHarness
	h = newCoordinatorHarness(t, l, mithrilTunedCatchUpMinChunks, func(
		ctx context.Context, applier TrustedBatchApplier,
	) error {
		if _, err := applier.ApplyTrustedBatch(
			ctx, applier.Tip(), nil,
		); err != nil {
			return err
		}
		h.clock.Advance(10 * time.Second)
		return nil
	})
	live := h.c.live
	h.c.live = func() mithrilLive {
		m := live()
		m.tuning = clockTuning{advance: func() { h.clock.Advance(time.Hour) }}
		return m
	}
	h.c.cycle(t.Context())
	assert.InDelta(t, 200, h.c.mithrilBPS, 0.001)
}

// blockingTuning stands in for an in-hold rebuild that runs until its
// context ends, and records why it ended.
type blockingTuning struct {
	started chan struct{}
	cause   chan error
}

func (b blockingTuning) begin(
	context.Context,
	mithrilTuningScope,
) mithrilTuned {
	return mithrilTuned{
		restore: func(ctx context.Context) {
			close(b.started)
			<-ctx.Done()
			b.cause <- context.Cause(ctx)
		},
		end: func(context.Context) {},
	}
}

// A live Restore or Truncate that aborts a run already inside its in-hold
// rebuild, after the run ended on its own, stops the rebuild instead of
// waiting it out; the marker it leaves is repaired before chain ingest.
func TestCoordinatorAbortStopsInHoldRebuild(t *testing.T) {
	t.Parallel()
	l := &fakeMithrilLedger{
		tip:  tipAt(0, 10),
		next: []ochainsync.Tip{tipAt(mithrilTunedCatchUpMinChunks, 5000)},
	}
	h := newCoordinatorHarness(t, l, mithrilTunedCatchUpMinChunks,
		func(ctx context.Context, applier TrustedBatchApplier) error {
			if _, err := applier.ApplyTrustedBatch(
				ctx, applier.Tip(), nil,
			); err != nil {
				return err
			}
			return errors.New("fake: download failed")
		})
	tuning := blockingTuning{
		started: make(chan struct{}),
		cause:   make(chan error, 1),
	}
	live := h.c.live
	h.c.live = func() mithrilLive {
		m := live()
		m.tuning = tuning
		return m
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.c.cycle(t.Context())
	}()
	testutil.RequireReceive(t, tuning.started, testutil.AsyncWait,
		"the in-hold rebuild never started")
	aborted := make(chan struct{})
	go func() {
		defer close(aborted)
		h.c.lifecycle.Lock()
		defer h.c.lifecycle.Unlock()
		h.c.abortRun()
	}()
	testutil.RequireReceive(t, aborted, 5*time.Second,
		"abortRun waited out the in-hold rebuild")
	cause := testutil.RequireReceive(t, tuning.cause, time.Second,
		"the rebuild did not end")
	require.ErrorIs(t, cause, errMithrilLifecycleAbort)
	testutil.RequireReceive(t, done, 5*time.Second, "the run did not end")
	holds, pauses, _ := l.state()
	require.Zero(t, holds)
	require.Zero(t, pauses)
}

// A cancelled in-hold rebuild leaves the catch-up indexes missing behind the
// recovery marker, for the reinitialization or startup repair to finish.
func TestNodeMithrilTuningCancelledRestoreKeepsMarker(t *testing.T) {
	t.Parallel()
	db := newTuningTestDatabase(t, t.TempDir())
	tuning := nodeMithrilTuning{db: db, logger: discardLogger()}
	tuned := tuning.begin(t.Context(), fullScope)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errMithrilLifecycleAbort)
	tuned.restore(ctx)
	missing, pending := deferredIndexState(t, db)
	require.True(t, pending)
	require.Subset(t, missing, catchUpIndexNames())
}

func TestMithrilCatchUpIndexesWorthDropping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                       string
		gapFiles, localFile, block uint64
		want                       bool
	}{
		{"empty ledger", 100, 0, 0, true},
		// 100 files at 5000/301 blocks per file is ~1661 blocks, a third
		// of the chain.
		{"below the fraction", 100, 300, 5000, false},
		// 150 files at 3000/300 blocks per file is exactly half.
		{"at the fraction", 150, 299, 3000, true},
		{"above the fraction", 400, 299, 3000, true},
	} {
		assert.Equal(t, tc.want, mithrilCatchUpIndexesWorthDropping(
			tc.gapFiles, tc.localFile, tc.block,
		), tc.name)
	}
}

// scopeTuning records the scope each run was tuned with.
type scopeTuning struct {
	scopes chan mithrilTuningScope
}

func (s scopeTuning) begin(
	_ context.Context,
	scope mithrilTuningScope,
) mithrilTuned {
	s.scopes <- scope
	return mithrilTuned{
		restore: func(context.Context) {},
		end:     func(context.Context) {},
	}
}

// The coordinator drops the lazy indexes for any gap of
// mithrilTunedCatchUpMinChunks files, and the catch-up droppable ones only
// when the gap is also a large enough share of the chain the ledger holds.
func TestCoordinatorScopesCatchUpIndexDropByChainSize(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		tip    ochainsync.Tip
		target uint64
		want   mithrilTuningScope
	}{
		{
			name:   "fresh database",
			tip:    tipAt(0, 0),
			target: mithrilTunedCatchUpMinChunks,
			want:   fullScope,
		},
		{
			name:   "gap below the fraction of the chain",
			tip:    tipAt(300, 5000),
			target: 300 + mithrilTunedCatchUpMinChunks,
			want:   mithrilTuningScope{largeGap: true},
		},
		{
			name:   "gap at the fraction of the chain",
			tip:    tipAt(299, 3000),
			target: 299 + 150,
			want:   fullScope,
		},
		{
			name:   "small gap",
			tip:    tipAt(0, 0),
			target: mithrilTunedCatchUpMinChunks - 1,
			want:   mithrilTuningScope{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := &fakeMithrilLedger{
				tip:  tc.tip,
				next: []ochainsync.Tip{tipAt(tc.target, tc.tip.BlockNumber+1)},
			}
			h := newCoordinatorHarness(t, l, tc.target,
				func(ctx context.Context, applier TrustedBatchApplier) error {
					_, err := applier.ApplyTrustedBatch(
						ctx, applier.Tip(), nil,
					)
					return err
				})
			tuning := scopeTuning{scopes: make(chan mithrilTuningScope, 1)}
			live := h.c.live
			h.c.live = func() mithrilLive {
				m := live()
				m.tuning = tuning
				return m
			}
			h.c.cycle(t.Context())
			got := testutil.RequireReceive(t, tuning.scopes, time.Second,
				"the run was not tuned")
			assert.Equal(t, tc.want, got)
		})
	}
}

// A large-gap run whose gap is a small share of the chain drops only the
// lazy indexes, so nothing has to be rebuilt inside the hold.
func TestNodeMithrilTuningLazyOnlyScope(t *testing.T) {
	t.Parallel()
	db := newTuningTestDatabase(t, t.TempDir())
	tuning := nodeMithrilTuning{db: db, logger: discardLogger()}
	tuned := tuning.begin(t.Context(), mithrilTuningScope{largeGap: true})
	missing, pending := deferredIndexState(t, db)
	require.ElementsMatch(t, lazyIndexNames(), missing)
	require.True(t, pending)
	tuned.restore(t.Context())
	tuned.end(t.Context())
	missing, pending = deferredIndexState(t, db)
	require.Empty(t, missing)
	require.False(t, pending)
}
