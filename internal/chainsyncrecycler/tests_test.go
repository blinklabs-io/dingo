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
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConnId(id uint) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: int(id),
		},
	}
}

func testTip(slot uint64, blockNumber uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte("hash")),
		BlockNumber: blockNumber,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// logSignalHandler signals on a per-message channel each time a matching
// record is logged, so tests can tell the tick-level panic recovery
// ("panic in stall checker tick, continuing") from the loop-level one
// ("panic in stall checker goroutine") instead of inferring it from the
// side effects, which are identical for both.
type logSignalHandler struct {
	signals map[string]chan struct{}
}

func newLogSignalHandler(messages ...string) logSignalHandler {
	signals := make(map[string]chan struct{}, len(messages))
	for _, message := range messages {
		signals[message] = make(chan struct{}, 1)
	}
	return logSignalHandler{signals: signals}
}

// signal returns the channel that fires when message is logged.
func (h logSignalHandler) signal(message string) chan struct{} {
	return h.signals[message]
}

func (h logSignalHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h logSignalHandler) Handle(_ context.Context, record slog.Record) error {
	ch, ok := h.signals[record.Message]
	if !ok {
		return nil
	}
	select {
	case ch <- struct{}{}:
	default:
	}
	return nil
}

func (h logSignalHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h logSignalHandler) WithGroup(string) slog.Handler {
	return h
}

// fakeLedger is a LedgerSource that reports fixed tips and records reconcile
// attempts, so plateau/backlog behavior can be exercised without a database.
type fakeLedger struct {
	mu                  sync.Mutex
	tip                 ochainsync.Tip
	atTip               bool
	securityParam       int
	primaryChainTipSlot uint64
	reconciled          bool
	reconcileErr        error
	reconcileReasons    []string
	reconcileConns      []ouroboros.ConnectionId
}

func (f *fakeLedger) Tip() ochainsync.Tip {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tip
}

func (f *fakeLedger) IsAtTip() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.atTip
}

func (f *fakeLedger) SecurityParam() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.securityParam
}

func (f *fakeLedger) PrimaryChainTipSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.primaryChainTipSlot
}

func (f *fakeLedger) setPrimaryChainTipSlot(slot uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.primaryChainTipSlot = slot
}

func (f *fakeLedger) ReconcileLivePrimaryChainLedgerDivergence(
	reason string,
	connId ouroboros.ConnectionId,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconcileReasons = append(f.reconcileReasons, reason)
	f.reconcileConns = append(f.reconcileConns, connId)
	return f.reconciled, f.reconcileErr
}

func (f *fakeLedger) reconcileCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reconcileReasons)
}

func (f *fakeLedger) lastReconcile() (string, ouroboros.ConnectionId) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reconcileReasons) == 0 {
		return "", ouroboros.ConnectionId{}
	}
	last := len(f.reconcileReasons) - 1
	return f.reconcileReasons[last], f.reconcileConns[last]
}

// fakeChainsyncState is a ChainsyncState returning a caller-controlled set of
// tracked clients.
type fakeChainsyncState struct {
	mu            sync.Mutex
	tracked       []chainsync.TrackedClient
	activeConn    *ouroboros.ConnectionId
	stalledChecks int
	rotationCalls int
	// impatient is returned, once, by the next CheckPatienceExhausted.
	impatient []ouroboros.ConnectionId
}

func (f *fakeChainsyncState) CheckPatienceExhausted() []ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	ret := f.impatient
	f.impatient = nil
	return ret
}

func (f *fakeChainsyncState) CheckStalledClients() []ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stalledChecks++
	return nil
}

func (f *fakeChainsyncState) AdvanceHeaderSyncRotation() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rotationCalls++
}

func (f *fakeChainsyncState) GetTrackedClients() []chainsync.TrackedClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tracked
}

func (f *fakeChainsyncState) GetClientConnId() *ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.activeConn
}

func (f *fakeChainsyncState) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stalledChecks, f.rotationCalls
}

// fakeChainSelector is a ChainSelector with a fixed best peer and peer tips.
type fakeChainSelector struct {
	mu                sync.Mutex
	bestPeer          *ouroboros.ConnectionId
	peerTips          map[string]*chainselection.PeerChainTip
	localTip          ochainsync.Tip
	securityParam     uint64
	securityParamSets int
}

func (f *fakeChainSelector) SetLocalTip(tip ochainsync.Tip) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.localTip = tip
}

func (f *fakeChainSelector) SetSecurityParam(k uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.securityParam = k
	f.securityParamSets++
}

func (f *fakeChainSelector) GetBestPeer() *ouroboros.ConnectionId {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bestPeer
}

func (f *fakeChainSelector) GetPeerTip(
	connId ouroboros.ConnectionId,
) *chainselection.PeerChainTip {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.peerTips[connId.String()]
}

func (f *fakeChainSelector) observed() (ochainsync.Tip, uint64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.localTip, f.securityParam, f.securityParamSets
}

// publishedEvent records one publish call against the fake publisher.
type publishedEvent struct {
	eventType event.EventType
	evt       event.Event
	async     bool
}

type fakePublisher struct {
	mu     sync.Mutex
	events []publishedEvent
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{}
}

func (f *fakePublisher) Publish(eventType event.EventType, evt event.Event) {
	f.record(publishedEvent{eventType: eventType, evt: evt})
}

func (f *fakePublisher) PublishAsync(
	eventType event.EventType,
	evt event.Event,
) bool {
	f.record(publishedEvent{eventType: eventType, evt: evt, async: true})
	return true
}

func (f *fakePublisher) record(pe publishedEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, pe)
}

func (f *fakePublisher) all() []publishedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]publishedEvent(nil), f.events...)
}

func (f *fakePublisher) byType(eventType event.EventType) []publishedEvent {
	var out []publishedEvent
	for _, pe := range f.all() {
		if pe.eventType == eventType {
			out = append(out, pe)
		}
	}
	return out
}

// fakeComponents is a ComponentProvider handing out a fixed component set. It
// mirrors the node adapter's contract: when available is false the callback is
// never invoked and the tick is skipped.
type fakeComponents struct {
	mu        sync.Mutex
	live      LiveComponents
	available bool
	calls     int
}

func newFakeComponents(live LiveComponents) *fakeComponents {
	return &fakeComponents{live: live, available: true}
}

func (f *fakeComponents) WithLiveComponents(fn func(LiveComponents)) bool {
	f.mu.Lock()
	available := f.available
	live := f.live
	f.calls++
	f.mu.Unlock()
	if !available {
		return false
	}
	fn(live)
	return true
}

func (f *fakeComponents) setAvailable(available bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.available = available
}

func (f *fakeComponents) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// stalledClient builds a tracked client in the stalled state.
func stalledClient(
	connId ouroboros.ConnectionId,
	observabilityOnly bool,
) chainsync.TrackedClient {
	return chainsync.TrackedClient{
		ConnId:            connId,
		Status:            chainsync.ClientStatusStalled,
		ObservabilityOnly: observabilityOnly,
		LastActivity:      time.Now(),
	}
}

// activeClient builds a tracked client that is not stalled.
func activeClient(
	connId ouroboros.ConnectionId,
	cursorSlot uint64,
) chainsync.TrackedClient {
	return chainsync.TrackedClient{
		ConnId:       connId,
		Cursor:       ocommon.NewPoint(cursorSlot, []byte("cursor")),
		Status:       chainsync.ClientStatusSyncing,
		LastActivity: time.Now(),
	}
}

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

// panickyLedger lets the first skip Tip() calls through, then panics on the
// next panics calls. The skip exists because loop() reads the ledger tip once
// for the plateau baseline (initProgressBaseline) before the first tick: a
// panic on call one lands in that read, which runLoop recovers, so it would
// exercise loop restart rather than the per-tick recovery.
type panickyLedger struct {
	fakeLedger
	skip      atomic.Int64
	remaining atomic.Int64
	panicked  chan struct{}
	once      sync.Once
}

func newPanickyLedger(skip int64, panics int64) *panickyLedger {
	p := &panickyLedger{panicked: make(chan struct{}, 1)}
	p.tip = testTip(100, 50)
	p.atTip = true
	p.skip.Store(skip)
	p.remaining.Store(panics)
	return p
}

func (p *panickyLedger) Tip() ochainsync.Tip {
	if p.skip.Add(-1) >= 0 {
		return p.fakeLedger.Tip()
	}
	if p.remaining.Add(-1) >= 0 {
		p.once.Do(func() { close(p.panicked) })
		panic("boom")
	}
	return p.fakeLedger.Tip()
}

func TestTickPanicIsRecoveredAndTicksContinue(t *testing.T) {
	// Skip one call so the panic lands inside a tick rather than in the
	// startup baseline read.
	ledger := newPanickyLedger(1, 1)
	state := &fakeChainsyncState{}
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	})
	const (
		tickMsg = "panic in stall checker tick, continuing"
		loopMsg = "panic in stall checker goroutine"
	)
	logs := newLogSignalHandler(tickMsg, loopMsg)
	r := New(Config{
		Components:   components,
		EventBus:     newFakePublisher(),
		Logger:       slog.New(logs),
		StallTimeout: time.Minute,
		Interval:     time.Millisecond,
		Grace:        time.Second,
		Cooldown:     time.Minute,
	})

	require.NoError(t, r.Start(t.Context()))
	t.Cleanup(r.Stop)

	testutil.RequireReceive(
		t,
		ledger.panicked,
		2*time.Second,
		"tick should have panicked",
	)
	// The panic must be recovered by the per-tick guard, not by the loop
	// guard: a loop restart would also resume ticking, so asserting only
	// "ticks continue" cannot tell the two recovery paths apart.
	testutil.RequireReceive(
		t,
		logs.signal(tickMsg),
		2*time.Second,
		"tick-level panic recovery",
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
	testutil.RequireNoReceive(
		t,
		logs.signal(loopMsg),
		50*time.Millisecond,
		"a recovered tick panic must not restart the loop",
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
	ledger := &fakeLedger{
		tip:                 testTip(5_000, 100),
		primaryChainTipSlot: 6_000,
		atTip:               true,
	}
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
	assert.Equal(t, uint64(0), st.lastPrimaryChainTipSlot)
	assert.False(t, st.lastProgressAt.IsZero())

	components.setAvailable(true)
	st2 := newTickState()
	r.initProgressBaseline(st2)
	assert.Equal(t, uint64(5_000), st2.lastProgressSlot)
	assert.Equal(t, uint64(6_000), st2.lastPrimaryChainTipSlot)
}
