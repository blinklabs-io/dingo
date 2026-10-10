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

	m.handleEpochTransition(epochEvent(nil))
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
	require.Equal(t, "epoch", receive(t, u.entered, "run triggered through the bus"))
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
	require.NoError(t, receive(t, stopped, "Stop to return with the run still blocked"))
	require.Equal(t, int64(0), u.active.Load(), "the run must have observed cancellation")
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
	_, err = newTestManager(failing, event.NewEventBus(nil, nil)).RunStartup(t.Context())
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
