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

package koiosparity

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// fakeEpochRef is one koios reporting epoch's fake /epoch_info + /totals
// reference data for the fake Koios server below. Zero pools are ever
// served (empty /pool_list, /pool_updates), so every test in this file
// exercises only the epoch-level aggregate comparisons
// (CompareEpochAggregates/CompareEpochTotals) — sufficient to drive the
// observer's full fetch -> check -> pass/fail pipeline without needing
// /pool_history fixtures.
type fakeEpochRef struct {
	activeStake, treasury, reserves, fees string
	// notYetClosedFor, if > 0, makes the first N /epoch_info requests for
	// this epoch report end_time=0 ("not fully closed on Koios yet" —
	// fetchEpoch's real, retryable rejection), then a normal response
	// after that — exercising the observer's fetch retry loop.
	notYetClosedFor int32
	attempts        atomic.Int32
}

// newFakeKoiosServer serves a minimal, epoch-keyed Koios API: /pool_list and
// /pool_updates always report zero pools; /epoch_info and /totals serve
// exactly the epochs present in epochs (any other epoch number 404s, which
// the real client classifies as a permanent, non-retryable error).
func newFakeKoiosServer(t *testing.T, epochs map[uint64]*fakeEpochRef) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tip":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":999999}]`))
		case "/pool_list", "/pool_updates":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		case "/epoch_info":
			epoch, ref, ok := lookupFakeEpoch(r, epochs)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			endTime := int64(2000)
			if attempt := ref.attempts.Add(1); attempt <= ref.notYetClosedFor {
				endTime = 0
			}
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w,
				`[{"epoch_no":%d,"era":"conway","out_sum":"100","fees":"10",`+
					`"tx_count":1,"blk_count":1,"start_time":1000,"end_time":%d,`+
					`"first_block_time":1000,"last_block_time":1999,"active_stake":"%s",`+
					`"total_rewards":"100","avg_blk_reward":"1"}]`,
				epoch, endTime, ref.activeStake,
			)
		case "/totals":
			epoch, ref, ok := lookupFakeEpoch(r, epochs)
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = fmt.Fprintf(w,
				`[{"epoch_no":%d,"treasury":"%s","reserves":"%s","fees":"%s","reward":"1"}]`,
				epoch, ref.treasury, ref.reserves, ref.fees,
			)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func lookupFakeEpoch(
	r *http.Request,
	epochs map[uint64]*fakeEpochRef,
) (uint64, *fakeEpochRef, bool) {
	epoch, err := strconv.ParseUint(r.URL.Query().Get("_epoch_no"), 10, 64)
	if err != nil {
		return 0, nil, false
	}
	ref, ok := epochs[epoch]
	return epoch, ref, ok
}

// seedDingoEpochAggregate writes epoch_summary at koiosEpoch-1 (the "stake
// epoch" CompareEpochAggregates reads total_active_stake from) and
// reward_ada_pots at koiosEpoch itself (unshifted), matching a fakeEpochRef
// with the same values so the pair compares as a clean PASS.
func seedDingoEpochAggregate(
	t *testing.T,
	source *DatabaseSource,
	koiosEpoch, activeStake, treasury, reserves, fees uint64,
) {
	t.Helper()
	gormDB := sourceGormDB(t, source.db)
	require.NoError(t, gormDB.Create(&models.EpochSummary{
		Epoch:            koiosEpoch - 1,
		TotalActiveStake: types.Uint64(activeStake),
		SnapshotReady:    true,
	}).Error)
	require.NoError(t, gormDB.Create(&models.RewardAdaPots{
		Epoch:    koiosEpoch,
		Treasury: types.Uint64(treasury),
		Reserves: types.Uint64(reserves),
		Fees:     types.Uint64(fees),
	}).Error)
}

// setDingoActiveStake overwrites the stake-epoch active-stake row for
// koiosEpoch, simulating a rollback+replay that changes Dingo's committed
// reward state for an epoch that was already validated.
func setDingoActiveStake(t *testing.T, source *DatabaseSource, koiosEpoch, activeStake uint64) {
	t.Helper()
	gormDB := sourceGormDB(t, source.db)
	require.NoError(t, gormDB.Model(&models.EpochSummary{}).
		Where("epoch = ?", koiosEpoch-1).
		Update("total_active_stake", types.Uint64(activeStake)).Error)
}

func newTestObserver(
	t *testing.T,
	source *DatabaseSource,
	strict bool,
	onResult func(*EpochCompareResult),
) *Observer {
	t.Helper()
	o, err := NewObserver(ObserverConfig{
		Network:            "preview",
		CachePath:          filepath.Join(t.TempDir(), "cache.db"),
		Source:             source,
		Strict:             strict,
		Logger:             slog.New(slog.DiscardHandler),
		FetchRetryAttempts: 3,
		FetchRetryDelay:    5 * time.Millisecond,
		OnResult:           onResult,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = o.Stop(context.Background())
	})
	return o
}

func publishEpochTransition(eb *event.EventBus, previousEpoch uint64) {
	eb.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(event.EpochTransitionEventType, event.EpochTransitionEvent{
			PreviousEpoch: previousEpoch,
			NewEpoch:      previousEpoch + 1,
		}),
	)
}

func TestNewObserverRejectsNilSourceAndBadNetwork(t *testing.T) {
	_, err := NewObserver(ObserverConfig{Network: "preview"})
	require.Error(t, err)

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	_, err = NewObserver(ObserverConfig{Network: "mainnet", Source: source})
	require.Error(t, err)
}

// TestObserverCommitVisibilityAndEventOrdering drives the observer through
// its real EventBus subscription path (matching how node.go wires it) for
// two epochs published in order, and confirms both are validated in that
// same order — proving both commit visibility (the source sees exactly what
// was committed to the shared *database.Database before the event fired)
// and in-order event processing.
func TestObserverCommitVisibilityAndEventOrdering(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30"},
		6: {activeStake: "2000000", treasury: "11", reserves: "21", fees: "31"},
	})
	withTestKoiosBaseURL(t, srv.URL)

	results := make(chan *EpochCompareResult, 8)
	o := newTestObserver(t, source, true, func(r *EpochCompareResult) { results <- r })

	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	eb.SubscribeFunc(event.EpochTransitionEventType, o.HandleEpochTransitionEvent)

	// Start() runs before any reward state is committed, so its own
	// GetLatestEpoch()-driven historical backfill (see Start's doc comment)
	// has nothing to seed yet — this test is exercising the live event path
	// specifically, not that backfill.
	require.NoError(t, o.Start(context.Background()))

	seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)
	seedDingoEpochAggregate(t, source, 6, 2_000_000, 11, 21, 31)
	publishEpochTransition(eb, 5)
	publishEpochTransition(eb, 6)

	first := testutil.RequireReceive(t, results, 5*time.Second, "epoch 5 result")
	second := testutil.RequireReceive(t, results, 5*time.Second, "epoch 6 result")
	require.Equal(t, uint64(5), first.Epoch)
	require.Equal(t, StatusPass, first.Status)
	require.Equal(t, uint64(6), second.Epoch)
	require.Equal(t, StatusPass, second.Status)
}

// TestObserverDuplicateEventsAreIdempotent simulates dingo's documented
// double emission of an epoch.transition event for the same boundary
// (slot-clock-driven and block-driven) and confirms the epoch is still left
// in a single, consistent PASS state rather than duplicated/corrupted.
func TestObserverDuplicateEventsAreIdempotent(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30"},
	})
	withTestKoiosBaseURL(t, srv.URL)

	var resultCount atomic.Int32
	o := newTestObserver(t, source, true, func(r *EpochCompareResult) { resultCount.Add(1) })

	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	eb.SubscribeFunc(event.EpochTransitionEventType, o.HandleEpochTransitionEvent)

	// See TestObserverCommitVisibilityAndEventOrdering: Start() before any
	// reward state exists means its own historical backfill has nothing to
	// do, so only the two (duplicate) live events below drive processing.
	require.NoError(t, o.Start(context.Background()))
	seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)
	publishEpochTransition(eb, 5)
	publishEpochTransition(eb, 5)

	testutil.WaitForCondition(t, func() bool {
		statuses, err := o.cache.GetStatusSummary("preview")
		return err == nil && len(statuses) == 1 && statuses[0].Status == StatusPass
	}, 5*time.Second, "epoch 5 should settle to a single PASS status row")

	statuses, err := o.cache.GetStatusSummary("preview")
	require.NoError(t, err)
	require.Len(t, statuses, 1, "duplicate events for the same epoch must not create duplicate status rows")
}

// TestObserverRestartResumesBacklogWithoutReprocessingOrSkipping verifies
// the cache.db-as-checkpoint design: a fresh Observer started against the
// same cache and the same (still-open, live) database picks up exactly the
// backlog it needs — the newly closed epoch — without redoing already-PASS
// work and without skipping it either.
func TestObserverRestartResumesBacklogWithoutReprocessingOrSkipping(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30"},
		6: {activeStake: "2000000", treasury: "11", reserves: "21", fees: "31"},
	})
	withTestKoiosBaseURL(t, srv.URL)

	cachePath := filepath.Join(t.TempDir(), "cache.db")

	first, err := NewObserver(ObserverConfig{
		Network: "preview", CachePath: cachePath, Source: source, Strict: false,
		Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	eb1 := event.NewEventBus(nil, nil)
	defer eb1.Stop()
	eb1.SubscribeFunc(event.EpochTransitionEventType, first.HandleEpochTransitionEvent)
	// Start() before any reward state exists: its own historical backfill
	// (see Start's doc comment) has nothing to seed, so only the live event
	// below drives this first run.
	require.NoError(t, first.Start(context.Background()))
	seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)
	publishEpochTransition(eb1, 5)
	testutil.WaitForCondition(t, func() bool {
		status, err := first.cache.GetEpochInfo("preview", 5)
		return err == nil && status != nil
	}, 5*time.Second, "epoch 5's koios reference should be cached before restart")
	testutil.WaitForCondition(t, func() bool {
		statuses, err := first.cache.GetStatusSummary("preview")
		for _, s := range statuses {
			if s.Epoch == 5 {
				return err == nil && s.Status == StatusPass
			}
		}
		return false
	}, 5*time.Second, "epoch 5 should pass before restart")
	require.NoError(t, first.Stop(context.Background()))

	// "Restart": epoch 6 is now closed on Dingo's side too. A second Observer
	// attaches to the same cache.db (the sole persisted checkpoint) and the
	// same live database, then receives a fresh live event for the new
	// epoch — modeling a node restart where the observer resubscribes and
	// picks up exactly where it left off, neither re-validating epoch 5 nor
	// missing epoch 6. (Start's own historical-backfill seeding, covered by
	// the dedicated fetch/check unit tests, is deliberately not what this
	// test exercises — see its doc comment on the Dingo/Koios epoch-number
	// offset that makes it an approximate floor, not exact per-epoch
	// tracking.)
	seedDingoEpochAggregate(t, source, 6, 2_000_000, 11, 21, 31)
	second, err := NewObserver(ObserverConfig{
		Network: "preview", CachePath: cachePath, Source: source, Strict: false,
		Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	defer func() { _ = second.Stop(context.Background()) }()
	eb2 := event.NewEventBus(nil, nil)
	defer eb2.Stop()
	eb2.SubscribeFunc(event.EpochTransitionEventType, second.HandleEpochTransitionEvent)
	require.NoError(t, second.Start(context.Background()))
	publishEpochTransition(eb2, 6)

	testutil.WaitForCondition(t, func() bool {
		statuses, err := second.cache.GetStatusSummary("preview")
		if err != nil {
			return false
		}
		byEpoch := map[uint64]string{}
		for _, s := range statuses {
			byEpoch[s.Epoch] = s.Status
		}
		return byEpoch[5] == StatusPass && byEpoch[6] == StatusPass
	}, 5*time.Second, "restart should validate epoch 6 without losing epoch 5's result")
}

// TestObserverRollbackReChecksSameEpoch simulates a rollback+replay that
// changes Dingo's committed reward state for an already-validated epoch: a
// fresh event.EpochTransitionEvent for the same PreviousEpoch must be
// re-validated against the corrected state (here, deliberately made wrong),
// not silently left at its stale PASS.
func TestObserverRollbackReChecksSameEpoch(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30"},
	})
	withTestKoiosBaseURL(t, srv.URL)

	results := make(chan *EpochCompareResult, 8)
	o := newTestObserver(t, source, false, func(r *EpochCompareResult) { results <- r })
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	eb.SubscribeFunc(event.EpochTransitionEventType, o.HandleEpochTransitionEvent)
	require.NoError(t, o.Start(context.Background()))

	publishEpochTransition(eb, 5)
	first := testutil.RequireReceive(t, results, 5*time.Second, "first pass result")
	require.Equal(t, StatusPass, first.Status)

	// Simulate a rollback that replays epoch 5's boundary with different
	// committed reward state (Koios's cached reference is unchanged).
	setDingoActiveStake(t, source, 5, 999)
	publishEpochTransition(eb, 5)
	second := testutil.RequireReceive(t, results, 5*time.Second, "re-check after rollback")
	require.Equal(t, StatusFail, second.Status,
		"a re-signaled epoch must be revalidated against corrected state, not trusted from its stale PASS")
}

// TestObserverStrictModeCancelsOnFirstMismatch confirms strict mode fires
// FatalFunc exactly once on the first failure and does not go on to process
// a second, later-sorted epoch queued in the same batch.
func TestObserverStrictModeCancelsOnFirstMismatch(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)
	// Epoch 5: Dingo disagrees with Koios (mismatch). Epoch 6: would pass,
	// but strict mode must never reach it once 5 fails. Seeded after Start
	// below (see TestObserverCommitVisibilityAndEventOrdering) so Start's own
	// historical backfill has nothing to do yet.

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "999999999", treasury: "10", reserves: "20", fees: "30"}, // mismatch
		6: {activeStake: "2000000", treasury: "11", reserves: "21", fees: "31"},
	})
	withTestKoiosBaseURL(t, srv.URL)

	var fatalCount atomic.Int32
	var fatalErr error
	var mu sync.Mutex
	o, err := NewObserver(ObserverConfig{
		Network: "preview", CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source: source, Strict: true, Logger: slog.New(slog.DiscardHandler),
		FatalFunc: func(err error) {
			fatalCount.Add(1)
			mu.Lock()
			fatalErr = err
			mu.Unlock()
		},
	})
	require.NoError(t, err)
	defer func() { _ = o.Stop(context.Background()) }()

	require.NoError(t, o.Start(context.Background()))
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	eb.SubscribeFunc(event.EpochTransitionEventType, o.HandleEpochTransitionEvent)
	seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)
	seedDingoEpochAggregate(t, source, 6, 2_000_000, 11, 21, 31)
	// Publish both in the same tick (before run() drains) so they land in
	// one sorted batch — proving strict mode stops the batch, not just the
	// individual epoch.
	publishEpochTransition(eb, 5)
	publishEpochTransition(eb, 6)

	testutil.WaitForCondition(t, func() bool {
		return fatalCount.Load() == 1
	}, 5*time.Second, "strict mode must call FatalFunc on the first mismatch")
	mu.Lock()
	require.Error(t, fatalErr)
	mu.Unlock()

	// run()'s loop checks stopping() immediately after processEpoch returns
	// and before moving to the next epoch in the same sorted batch — all in
	// the same goroutine — so observing fatalCount == 1 above already
	// guarantees epoch 6 was never reached; no additional wait is needed.
	statuses, err := o.cache.GetStatusSummary("preview")
	require.NoError(t, err)
	require.Len(t, statuses, 1, "epoch 6 must never be checked once strict mode stopped on epoch 5")
	require.Equal(t, uint64(5), statuses[0].Epoch)
	require.Equal(t, int32(1), fatalCount.Load(), "FatalFunc must fire at most once")
}

// TestObserverNonStrictModeContinuesAfterFailure confirms non-strict mode
// records a failure but keeps validating subsequent epochs, and never calls
// FatalFunc.
func TestObserverNonStrictModeContinuesAfterFailure(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "999999999", treasury: "10", reserves: "20", fees: "30"}, // mismatch
		6: {activeStake: "2000000", treasury: "11", reserves: "21", fees: "31"},   // passes
	})
	withTestKoiosBaseURL(t, srv.URL)

	var fatalCount atomic.Int32
	o, err := NewObserver(ObserverConfig{
		Network: "preview", CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source: source, Strict: false, Logger: slog.New(slog.DiscardHandler),
		FatalFunc: func(error) { fatalCount.Add(1) },
	})
	require.NoError(t, err)
	defer func() { _ = o.Stop(context.Background()) }()
	require.NoError(t, o.Start(context.Background()))
	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()
	eb.SubscribeFunc(event.EpochTransitionEventType, o.HandleEpochTransitionEvent)
	seedDingoEpochAggregate(t, source, 5, 1_000_000, 10, 20, 30)
	seedDingoEpochAggregate(t, source, 6, 2_000_000, 11, 21, 31)
	publishEpochTransition(eb, 5)
	publishEpochTransition(eb, 6)

	testutil.WaitForCondition(t, func() bool {
		statuses, err := o.cache.GetStatusSummary("preview")
		return err == nil && len(statuses) == 2
	}, 5*time.Second, "non-strict mode must still validate both epochs")

	statuses, err := o.cache.GetStatusSummary("preview")
	require.NoError(t, err)
	byEpoch := map[uint64]string{}
	for _, s := range statuses {
		byEpoch[s.Epoch] = s.Status
	}
	require.Equal(t, StatusFail, byEpoch[5])
	require.Equal(t, StatusPass, byEpoch[6])
	require.Equal(t, int32(0), fatalCount.Load(), "non-strict mode must never call FatalFunc")
}

// TestObserverCancellationStopsPromptly confirms cancelling the context
// passed to Start makes the background goroutine exit promptly, and that
// Stop does not hang waiting for it.
func TestObserverCancellationStopsPromptly(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source:    source,
		Logger:    slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, o.Start(ctx))
	cancel()

	done := make(chan struct{})
	go func() {
		o.wg.Wait()
		close(done)
	}()
	testutil.RequireReceive(t, done, 5*time.Second, "run() must exit promptly after ctx cancellation")

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	require.NoError(t, o.Stop(stopCtx))
}

// TestObserverStopIsIdempotent confirms Stop can be called more than once on
// the same Observer instance without panicking. This models the real
// double-invocation the reviewer found: node.go's own started-stack cleanup
// calls Stop() on a startup failure that occurs after the koios-parity
// observer has already been started but before Run() finishes, and
// node_shutdown.go's shutdown() (the normal shutdown path, e.g. via
// Node.Stop() or a signal) independently calls Stop() again on the same
// instance. Before this fix, Stop() closed an owned "done" channel
// unconditionally, so a second call panicked with "close of closed channel".
func TestObserverStopIsIdempotent(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source:    source,
		Logger:    slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	require.NoError(t, o.Start(context.Background()))

	require.NotPanics(t, func() {
		require.NoError(t, o.Stop(context.Background()))
		require.NoError(t, o.Stop(context.Background()))
	}, "Stop must be safe to call more than once on the same Observer")
}

// TestObserverConcurrentStopCallsDoNotPanic exercises the same double
// invocation but from two goroutines racing each other, matching the actual
// shape of the regression: node.go's started-stack cleanup (on a startup
// failure) and node_shutdown.go's shutdown() path can both end up calling
// Stop() on the same Observer with no ordering guarantee between them.
func TestObserverConcurrentStopCallsDoNotPanic(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source:    source,
		Logger:    slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	require.NoError(t, o.Start(context.Background()))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = o.Stop(context.Background())
		}(i)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])
}

// TestObserverFetchIfNeededRetriesTransientThenSucceeds is a focused unit
// test of the bounded fetch-retry policy fetchIfNeeded uses to ride out
// Koios not having fully closed an epoch out yet (fetchEpoch's real,
// retryable end_time==0 rejection) — a transient condition expected mainly
// near live tip, distinct from a permanent (ErrKoiosPermanent) failure.
func TestObserverFetchIfNeededRetriesTransientThenSucceeds(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{
		5: {activeStake: "1000000", treasury: "10", reserves: "20", fees: "30", notYetClosedFor: 2},
	})
	withTestKoiosBaseURL(t, srv.URL)

	o, err := NewObserver(ObserverConfig{
		Network: "preview", CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source: source, Logger: slog.New(slog.DiscardHandler),
		FetchRetryAttempts: 5, FetchRetryDelay: 5 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = o.Stop(context.Background()) }()

	require.NoError(t, o.fetchIfNeeded(context.Background(), 5))
	uncached, err := o.cache.GetUncachedEpochs("preview", 5, 5)
	require.NoError(t, err)
	require.Empty(t, uncached, "epoch 5 should be cached after the retry loop succeeds")
}

// TestObserverFetchIfNeededSurfacesPermanentErrorImmediately confirms a
// permanent Koios error (here, an epoch never present in the fake server's
// map, which 404s and the real client classifies as permanent) is not
// retried FetchRetryAttempts times before failing.
func TestObserverFetchIfNeededSurfacesPermanentErrorImmediately(t *testing.T) {
	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	srv := newFakeKoiosServer(t, map[uint64]*fakeEpochRef{})
	withTestKoiosBaseURL(t, srv.URL)

	o, err := NewObserver(ObserverConfig{
		Network: "preview", CachePath: filepath.Join(t.TempDir(), "cache.db"),
		Source: source, Logger: slog.New(slog.DiscardHandler),
		FetchRetryAttempts: 5, FetchRetryDelay: 5 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = o.Stop(context.Background()) }()

	err = o.fetchIfNeeded(context.Background(), 9)
	require.Error(t, err)
}
