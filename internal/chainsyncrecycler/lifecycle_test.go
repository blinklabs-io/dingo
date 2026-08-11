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

package chainsyncrecycler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newLifecycleRecycler(
	t *testing.T,
	components ComponentProvider,
) *Recycler {
	t.Helper()
	return New(Config{
		Components:   components,
		EventBus:     newFakePublisher(),
		Logger:       discardLogger(),
		StallTimeout: time.Minute,
		Interval:     time.Millisecond,
		Grace:        time.Second,
		Cooldown:     time.Minute,
	})
}

func TestStartRejectsIncompleteConfig(t *testing.T) {
	components := newFakeComponents(LiveComponents{})
	pub := newFakePublisher()

	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "missing components",
			cfg: Config{
				EventBus: pub,
				Interval: time.Millisecond,
			},
		},
		{
			name: "missing event bus",
			cfg: Config{
				Components: components,
				Interval:   time.Millisecond,
			},
		},
		{
			name: "non-positive interval",
			cfg: Config{
				Components: components,
				EventBus:   pub,
				Interval:   0,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := New(tc.cfg)
			require.Error(t, r.Start(t.Context()))
		})
	}
}

func TestStartStopExitsCleanly(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{}
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	})
	r := newLifecycleRecycler(t, components)

	require.NoError(t, r.Start(t.Context()))
	testutil.WaitForCondition(
		t,
		func() bool { return components.callCount() > 1 },
		2*time.Second,
		"recycler should tick after Start",
	)

	stopped := make(chan struct{})
	go func() {
		r.Stop()
		close(stopped)
	}()
	testutil.RequireReceive(
		t,
		stopped,
		2*time.Second,
		"Stop must return once the recycler goroutine exits",
	)

	// Stop returning means the goroutine is gone, so no further tick can
	// touch the components shutdown is about to tear down.
	after := components.callCount()
	r.Stop()
	assert.Equal(t, after, components.callCount())
}

func TestStopIsSafeWithoutStartAndIsIdempotent(t *testing.T) {
	r := newLifecycleRecycler(t, newFakeComponents(LiveComponents{}))
	r.Stop()
	r.Stop()

	ledger := &fakeLedger{tip: testTip(1, 1), atTip: true}
	r2 := newLifecycleRecycler(t, newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: &fakeChainsyncState{},
	}))
	require.NoError(t, r2.Start(t.Context()))
	r2.Stop()
	r2.Stop()
}

func TestStartIsRejectedTwice(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(1, 1), atTip: true}
	r := newLifecycleRecycler(t, newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: &fakeChainsyncState{},
	}))
	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(r.Stop)
	require.Error(t, r.Start(t.Context()), "double Start must be rejected")
}

func TestStopExitsWhenParentContextCancelled(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(1, 1), atTip: true}
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: &fakeChainsyncState{},
	})
	r := newLifecycleRecycler(t, components)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, r.Start(ctx))
	testutil.WaitForCondition(
		t,
		func() bool { return components.callCount() > 0 },
		2*time.Second,
		"recycler should tick after Start",
	)
	cancel()

	stopped := make(chan struct{})
	go func() {
		r.Stop()
		close(stopped)
	}()
	testutil.RequireReceive(
		t,
		stopped,
		2*time.Second,
		"Stop must return after the parent context is cancelled",
	)
}

func TestTicksAreSkippedWhileComponentsUnavailable(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{}
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	})
	components.setAvailable(false)
	r := newLifecycleRecycler(t, components)

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(r.Stop)

	testutil.WaitForCondition(
		t,
		func() bool { return components.callCount() > 2 },
		2*time.Second,
		"recycler should keep ticking while components are unavailable",
	)
	checks, rotations := state.counts()
	assert.Equal(
		t,
		0,
		checks,
		"a skipped tick must not touch chainsync state",
	)
	assert.Equal(t, 0, rotations)

	// Once the components come back, ticks resume without a restart.
	components.setAvailable(true)
	testutil.WaitForCondition(
		t,
		func() bool {
			checks, _ := state.counts()
			return checks > 0
		},
		2*time.Second,
		"recycler should resume ticking when components return",
	)
}

// panickyLedger panics on the first n Tip() calls, then behaves normally, so
// panic recovery inside a tick can be exercised deterministically.
type panickyLedger struct {
	fakeLedger
	remaining atomic.Int64
	panicked  chan struct{}
	once      sync.Once
}

func newPanickyLedger(panics int64) *panickyLedger {
	p := &panickyLedger{panicked: make(chan struct{}, 1)}
	p.tip = testTip(100, 50)
	p.atTip = true
	p.remaining.Store(panics)
	return p
}

func (p *panickyLedger) Tip() ochainsync.Tip {
	if p.remaining.Add(-1) >= 0 {
		p.once.Do(func() { close(p.panicked) })
		panic("boom")
	}
	return p.fakeLedger.Tip()
}

func TestTickPanicIsRecoveredAndTicksContinue(t *testing.T) {
	ledger := newPanickyLedger(1)
	state := &fakeChainsyncState{}
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	})
	r := newLifecycleRecycler(t, components)

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(r.Stop)

	testutil.RequireReceive(
		t,
		ledger.panicked,
		2*time.Second,
		"tick should have panicked",
	)
	testutil.WaitForCondition(
		t,
		func() bool {
			checks, _ := state.counts()
			return checks > 0
		},
		2*time.Second,
		"a panicking tick must not stop later ticks",
	)
}

// panickyComponents panics inside WithLiveComponents on the startup baseline
// read, which runs outside the per-tick recovery, exercising loop restart.
type panickyComponents struct {
	fakeComponents
	remaining atomic.Int64
	restarts  atomic.Int64
}

func (p *panickyComponents) WithLiveComponents(fn func(LiveComponents)) bool {
	if p.remaining.Add(-1) >= 0 {
		p.restarts.Add(1)
		panic("startup boom")
	}
	return p.fakeComponents.WithLiveComponents(fn)
}

func TestLoopPanicIsRecoveredAndLoopRestarts(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{}
	components := &panickyComponents{}
	components.live = LiveComponents{Ledger: ledger, ChainsyncState: state}
	components.available = true
	components.remaining.Store(1)

	r := newLifecycleRecycler(t, components)
	r.restartDelay = time.Millisecond

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(r.Stop)

	testutil.WaitForCondition(
		t,
		func() bool {
			checks, _ := state.counts()
			return components.restarts.Load() == 1 && checks > 0
		},
		2*time.Second,
		"a panicking loop must restart and resume ticking",
	)
}

func TestLoopStopsAfterPanicWhenCancelled(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	components := &panickyComponents{}
	components.live = LiveComponents{
		Ledger:         ledger,
		ChainsyncState: &fakeChainsyncState{},
	}
	components.available = true
	// Panic on every call so the loop can only exit via cancellation.
	components.remaining.Store(1 << 30)

	r := newLifecycleRecycler(t, components)
	r.restartDelay = 10 * time.Millisecond
	require.NoError(t, r.Start(t.Context()))

	testutil.WaitForCondition(
		t,
		func() bool { return components.restarts.Load() > 1 },
		2*time.Second,
		"loop should keep restarting after panics",
	)

	stopped := make(chan struct{})
	go func() {
		r.Stop()
		close(stopped)
	}()
	testutil.RequireReceive(
		t,
		stopped,
		2*time.Second,
		"Stop must interrupt the restart backoff",
	)
}

func TestStartupBaselineSkippedWhenComponentsUnavailable(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(5_000, 100), atTip: true}
	state := &fakeChainsyncState{}
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	})
	components.setAvailable(false)

	r := newLifecycleRecycler(t, components)
	st := newTickState()
	r.initProgressBaseline(st)

	assert.Equal(
		t,
		uint64(0),
		st.lastProgressSlot,
		"an unavailable baseline read leaves the plateau baseline at zero",
	)
	assert.False(t, st.lastProgressAt.IsZero())

	components.setAvailable(true)
	st2 := newTickState()
	r.initProgressBaseline(st2)
	assert.Equal(t, uint64(5_000), st2.lastProgressSlot)
}
