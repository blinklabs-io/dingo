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

package plannerstats

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/require"
)

const testWait = 10 * time.Second

// fakeUpdater blocks every call until release is closed or its context ends.
type fakeUpdater struct {
	entered chan string
	release chan struct{}
	calls   atomic.Int64
	active  atomic.Int64
	maxSeen atomic.Int64
	err     error
}

func newFakeUpdater() *fakeUpdater {
	return &fakeUpdater{
		entered: make(chan string, 64),
		release: make(chan struct{}),
	}
}

func (f *fakeUpdater) OptimizePlannerStatsContext(
	ctx context.Context,
	trigger string,
) (types.PlannerStatsResult, error) {
	f.calls.Add(1)
	now := f.active.Add(1)
	defer f.active.Add(-1)
	for {
		prev := f.maxSeen.Load()
		if now <= prev || f.maxSeen.CompareAndSwap(prev, now) {
			break
		}
	}
	f.entered <- trigger
	select {
	case <-f.release:
	case <-ctx.Done():
		return types.PlannerStatsResult{Supported: true}, ctx.Err()
	}
	return types.PlannerStatsResult{Supported: true, Changed: true}, f.err
}

func receive[T any](t *testing.T, ch <-chan T, msg string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(testWait):
		t.Fatalf("timed out waiting for %s", msg)
		var zero T
		return zero
	}
}

func newTestManager(u *fakeUpdater, bus *event.EventBus) *Manager {
	return NewManager(u, bus, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func epochEvent(nonce []byte) event.Event {
	return event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{NewEpoch: 5, EpochNonce: nonce},
	)
}

func TestEpochTransitionsCoalesceToOneRunInFlightAndOneQueued(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	m := newTestManager(u, event.NewEventBus(nil, nil))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.NoError(t, m.Start(ctx))
	defer func() { require.NoError(t, m.Stop()) }()
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(u.release) }) }
	defer releaseOnce() // runs before Stop, so a failing test cannot hang it

	m.handleEpochTransition(epochEvent([]byte{1}))
	require.Equal(t, "epoch", receive(t, u.entered, "first run"))
	for range 19 {
		m.handleEpochTransition(epochEvent([]byte{1}))
	}
	releaseOnce()
	require.Equal(t, "epoch", receive(t, u.entered, "queued follow-up run"))
	require.Never(t, func() bool { return u.calls.Load() > 2 },
		200*time.Millisecond, 10*time.Millisecond,
		"nineteen transitions behind one run must collapse to one follow-up")
	require.Equal(t, int64(2), u.calls.Load())
	require.Equal(t, int64(1), u.maxSeen.Load(), "runs must never overlap")
}

func TestEpochTransitionWithoutNonceIsIgnored(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	m := newTestManager(u, event.NewEventBus(nil, nil))
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()

	m.handleEpochTransition(event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{NewEpoch: 5, ProtocolVersion: 10},
	))
	require.Never(t, func() bool { return u.calls.Load() != 0 },
		200*time.Millisecond, 10*time.Millisecond)
	m.handleEpochTransition(epochEvent([]byte{1}))
	receive(t, u.entered, "run for the ledger's transition")
}

func TestEpochTransitionPublishedOnBusTriggersRun(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	bus := event.NewEventBus(nil, nil)
	m := newTestManager(u, bus)
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()

	bus.Publish(event.EpochTransitionEventType, epochEvent([]byte{9}))
	require.Equal(
		t,
		"epoch",
		receive(t, u.entered, "run triggered through the bus"),
	)
}

func TestStopInterruptsRunInFlight(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	m := newTestManager(u, event.NewEventBus(nil, nil))
	require.NoError(t, m.Start(t.Context()))
	defer close(u.release)

	m.handleEpochTransition(epochEvent([]byte{1}))
	receive(t, u.entered, "run in flight")
	stopped := make(chan error, 1)
	go func() { stopped <- m.Stop() }()
	require.NoError(
		t,
		receive(t, stopped, "Stop to return with the run still blocked"),
	)
	require.Equal(
		t,
		int64(0),
		u.active.Load(),
		"the run must have observed cancellation",
	)
}

func TestRunStartupReportsResultAndError(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	m := newTestManager(u, event.NewEventBus(nil, nil))
	result, err := m.RunStartup(t.Context())
	require.NoError(t, err)
	require.True(t, result.Changed)
	require.Equal(t, "startup", receive(t, u.entered, "startup trigger"))

	failing := newFakeUpdater()
	close(failing.release)
	failing.err = errors.New("boom")
	_, err = newTestManager(
		failing,
		event.NewEventBus(nil, nil),
	).RunStartup(t.Context())
	require.ErrorContains(t, err, "boom")
}

func TestFailedRunDoesNotStopLaterRuns(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	u.err = errors.New("boom")
	m := newTestManager(u, event.NewEventBus(nil, nil))
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()
	m.handleEpochTransition(epochEvent([]byte{1}))
	receive(t, u.entered, "first failing run")
	m.handleEpochTransition(epochEvent([]byte{2}))
	receive(t, u.entered, "run after a failure")
}

// Byron has no epoch nonce, so the ledger's own Byron rollover events carry
// none; they must still trigger a run or a genesis sync of a chain with Byron
// epochs plans its UTxO spends without statistics until it leaves Byron.
func TestByronEpochTransitionWithoutNonceTriggersRun(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	m := newTestManager(u, event.NewEventBus(nil, nil))
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()

	m.handleEpochTransition(event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{NewEpoch: 5, ProtocolVersion: 1},
	))
	receive(t, u.entered, "run for a Byron rollover")
}

// startOnCancelUpdater calls Start on its manager when a run is canceled, so
// the call lands while Stop is draining the worker.
type startOnCancelUpdater struct {
	m       *Manager
	entered chan struct{}
	started chan error
	once    sync.Once
}

func (u *startOnCancelUpdater) OptimizePlannerStatsContext(
	ctx context.Context,
	_ string,
) (types.PlannerStatsResult, error) {
	u.entered <- struct{}{}
	<-ctx.Done()
	u.once.Do(func() {
		go func() { u.started <- u.m.Start(context.Background()) }()
		// Give the concurrent Start time to take effect before this worker
		// exits; a Start serialized behind Stop stays blocked.
		select {
		case <-u.started:
			u.started <- nil
		case <-time.After(200 * time.Millisecond):
		}
	})
	return types.PlannerStatsResult{Supported: true}, ctx.Err()
}

// A Start arriving while Stop drains the worker must not launch a worker Stop
// then waits on forever.
func TestStartDuringStopDoesNotHangStop(t *testing.T) {
	t.Parallel()
	u := &startOnCancelUpdater{
		entered: make(chan struct{}, 1),
		started: make(chan error, 1),
	}
	m := NewManager(
		u,
		event.NewEventBus(nil, nil),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	u.m = m
	require.NoError(t, m.Start(t.Context()))
	m.handleEpochTransition(epochEvent([]byte{1}))
	receive(t, u.entered, "run in flight")

	stopped := make(chan error, 1)
	go func() { stopped <- m.Stop() }()
	require.NoError(t, receive(t, stopped, "Stop to return"))
	require.NoError(t, receive(t, u.started, "the concurrent Start"))
	require.NoError(t, receive(t, asyncStop(m), "the restarted manager to stop"))
}

func asyncStop(m *Manager) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- m.Stop() }()
	return ch
}

// The periodic trigger runs the updater with no epoch transition published,
// so a table filled between rollovers is re-analyzed without waiting for one.
func TestPeriodicTickTriggersRun(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	m := newTestManager(u, event.NewEventBus(nil, nil))
	tick := make(chan time.Time)
	m.newTick = func() (<-chan time.Time, func()) {
		return tick, func() {}
	}
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()

	select {
	case tick <- time.Time{}:
	case <-time.After(testWait):
		t.Fatal("timed out handing the periodic tick to the worker")
	}
	require.Equal(
		t,
		types.PlannerStatsTriggerPeriodic,
		receive(t, u.entered, "run for the periodic tick"),
	)
}

// A tick that arrives while a run is in flight waits for it: runs never
// overlap, because the tick shares the worker with the epoch trigger.
func TestPeriodicTickDoesNotOverlapEpochRun(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	m := newTestManager(u, event.NewEventBus(nil, nil))
	// Buffered like time.Ticker's channel, so the tick is pending for the
	// whole window below rather than racing a sender goroutine.
	tick := make(chan time.Time, 1)
	m.newTick = func() (<-chan time.Time, func()) {
		return tick, func() {}
	}
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(u.release) }) }
	defer releaseOnce() // runs before Stop, so a failing test cannot hang it

	m.handleEpochTransition(epochEvent([]byte{1}))
	require.Equal(t, "epoch", receive(t, u.entered, "epoch run"))
	tick <- time.Time{}
	require.Never(t, func() bool { return u.calls.Load() > 1 },
		200*time.Millisecond, 10*time.Millisecond,
		"a tick must wait for the run in flight")
	releaseOnce()
	require.Equal(
		t,
		types.PlannerStatsTriggerPeriodic,
		receive(t, u.entered, "periodic run after the epoch run"),
	)
	require.Equal(t, int64(1), u.maxSeen.Load(), "runs must never overlap")
}

// TestDeferHoldsRunsUntilResumeThenRunsOnce defers every trigger while a
// catch-up holds the manager and collapses them into one run on resume.
func TestDeferHoldsRunsUntilResumeThenRunsOnce(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	tick := make(chan time.Time)
	m := newTestManager(u, event.NewEventBus(nil, nil))
	SetTickSource(m, tick)
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()

	resume := m.Defer()
	resumeNested := m.Defer()
	for range 3 {
		m.handleEpochTransition(epochEvent([]byte{1}))
		tick <- time.Now()
	}
	// The worker has consumed every tick, so it has seen the triggers.
	tick <- time.Now()
	require.Zero(t, u.calls.Load(), "a deferred trigger ran")

	resumeNested()
	// A tick the worker has received may still be in flight; sending another
	// would race the final resume and could run as a periodic refresh. The
	// window instead lets the worker skip it while one deferral is held.
	require.Never(t, func() bool { return u.calls.Load() > 0 },
		200*time.Millisecond, 10*time.Millisecond,
		"ran before the last resume")

	resume()
	resume() // idempotent
	require.Equal(t, "epoch", receive(t, u.entered, "the deferred run"))
	require.Never(t, func() bool { return u.calls.Load() > 1 },
		200*time.Millisecond, 10*time.Millisecond,
		"deferred triggers must collapse to one run")
}

// TestDeferWithoutTriggerQueuesNothing resumes a deferral nothing arrived
// during, which must not cost a run.
func TestDeferWithoutTriggerQueuesNothing(t *testing.T) {
	t.Parallel()
	u := newFakeUpdater()
	close(u.release)
	m := newTestManager(u, event.NewEventBus(nil, nil))
	SetTickSource(m, make(chan time.Time))
	require.NoError(t, m.Start(t.Context()))
	defer func() { require.NoError(t, m.Stop()) }()

	m.Defer()()
	require.Never(t, func() bool { return u.calls.Load() != 0 },
		200*time.Millisecond, 10*time.Millisecond)
	var nilManager *Manager
	nilManager.Defer()()
}
