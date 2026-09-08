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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlateauThreshold(t *testing.T) {
	assert.Equal(t, 4*time.Minute, plateauThreshold(time.Minute))
	assert.Equal(t, 4*time.Minute, plateauThreshold(2*time.Minute))
	assert.Equal(t, 6*time.Minute, plateauThreshold(3*time.Minute))
}

func TestShouldRecycleLocalTipPlateau(t *testing.T) {
	now := time.Now()
	recent := now.Add(-time.Second)

	assert.True(t, shouldRecycleLocalTipPlateau(
		now,
		now.Add(-10*time.Minute),
		100,
		200,
		nil,
		2*time.Minute,
		4*time.Minute,
	), "peer ahead and plateau beyond threshold should recycle")

	assert.False(t, shouldRecycleLocalTipPlateau(
		now,
		now.Add(-10*time.Minute),
		200,
		200,
		nil,
		2*time.Minute,
		4*time.Minute,
	), "peer not ahead should not recycle")

	assert.False(t, shouldRecycleLocalTipPlateau(
		now,
		now.Add(-time.Minute),
		100,
		200,
		nil,
		2*time.Minute,
		4*time.Minute,
	), "plateau shorter than threshold should not recycle")

	assert.False(t, shouldRecycleLocalTipPlateau(
		now,
		now.Add(-10*time.Minute),
		100,
		200,
		&recent,
		2*time.Minute,
		4*time.Minute,
	), "recycle inside cooldown should not recycle")
}

func TestIsLedgerApplicationBacklog(t *testing.T) {
	// Header chain caught up to the peer, huge apply backlog behind it.
	assert.True(t, isLedgerApplicationBacklog(1_488_398, 3_082_751, 3_082_751))
	// Header chain nearly caught up; backlog dominates the residual gap.
	assert.True(t, isLedgerApplicationBacklog(1_488_398, 3_082_700, 3_082_751))
	// Header chain not ahead of the applied tip: a genuine header stall.
	assert.False(t, isLedgerApplicationBacklog(1_488_398, 1_488_398, 3_082_751))
	// Header gap dominates the small backlog: still a header stall.
	assert.False(t, isLedgerApplicationBacklog(1_000, 1_100, 3_000))
	// Primary chain tip behind the applied tip.
	assert.False(t, isLedgerApplicationBacklog(100, 0, 120))
	assert.True(t, isLedgerApplicationBacklog(100, 150, 200))
}

// newTestRecycler builds a recycler over fakes with an at-tip ledger, so the
// catch-up multiplier does not scale thresholds unless a test opts in.
func newTestRecycler(
	t *testing.T,
	ledger *fakeLedger,
	state *fakeChainsyncState,
	selector ChainSelector,
	pub *fakePublisher,
	cfg Config,
) (*Recycler, *fakeComponents) {
	t.Helper()
	components := newFakeComponents(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	})
	cfg.Components = components
	cfg.EventBus = pub
	if cfg.Logger == nil {
		cfg.Logger = discardLogger()
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Millisecond
	}
	if cfg.StallTimeout == 0 {
		cfg.StallTimeout = 2 * time.Minute
	}
	if cfg.Grace == 0 {
		cfg.Grace = time.Second
	}
	if cfg.Cooldown == 0 {
		cfg.Cooldown = 2 * time.Minute
	}
	r := New(cfg)
	return r, components
}

func newTestTickState(
	lastProgressSlot uint64,
	lastProgressAt time.Time,
) *tickState {
	st := newTickState()
	st.lastProgressSlot = lastProgressSlot
	st.lastProgressAt = lastProgressAt
	return st
}

// runTickWith drives one tick against the fakes the way the run loop does.
func runTickWith(
	r *Recycler,
	st *tickState,
	live LiveComponents,
	now time.Time,
	localTipSlot uint64,
) {
	r.tick(now, st, live, localTipSlot)
}

func TestTickRecyclesStalledActiveConnection(t *testing.T) {
	connId := testConnId(1)
	connId2 := testConnId(2)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			stalledClient(connId, false),
			stalledClient(connId2, false),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now)
	// Already past its guarded recycle deadline.
	st.recycleAt[connId.String()] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	events := pub.byType(connmanager.ConnectionRecycleRequestedEventType)
	require.Len(t, events, 1)
	recycleEvt, ok := events[0].evt.Data.(connmanager.ConnectionRecycleRequestedEvent)
	require.True(t, ok)
	assert.Equal(t, connId, recycleEvt.ConnectionId)
	assert.Equal(t, "stalled_active_connection", recycleEvt.Reason)
	assert.True(
		t,
		events[0].async,
		"connection recycle must be published async",
	)
	assert.NotContains(t, st.recycleAt, connId.String())
	assert.Contains(t, st.lastRecycled, connId.String())
}

func TestTickRemovesStalledNonPrimaryConnection(t *testing.T) {
	connId := testConnId(1)
	other := testConnId(2)
	active := other
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			stalledClient(connId, false),
			stalledClient(other, false),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now)
	st.recycleAt[connId.String()] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	events := pub.byType(chainsync.ClientRemoveRequestedEventType)
	require.Len(t, events, 1)
	removeEvt, ok := events[0].evt.Data.(chainsync.ClientRemoveRequestedEvent)
	require.True(t, ok)
	assert.Equal(t, connId, removeEvt.ConnId)
	assert.Equal(t, "stalled_non_primary_connection", removeEvt.Reason)
	assert.Empty(t, pub.byType(connmanager.ConnectionRecycleRequestedEventType))
	assert.NotContains(
		t,
		st.lastRecycled,
		connId.String(),
		"removing a non-primary client must not consume the recycle cooldown",
	)
}

func TestTickRecyclesStalledClientWithNoActiveSelection(t *testing.T) {
	connId := testConnId(1)
	connId2 := testConnId(2)
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			stalledClient(connId, false),
			stalledClient(connId2, false),
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now)
	st.recycleAt[connId.String()] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	events := pub.byType(connmanager.ConnectionRecycleRequestedEventType)
	require.Len(t, events, 1)
	recycleEvt, ok := events[0].evt.Data.(connmanager.ConnectionRecycleRequestedEvent)
	require.True(t, ok)
	assert.Equal(
		t,
		"stalled_connection_no_active_selection",
		recycleEvt.Reason,
	)
}

func TestTickSkipsRecyclingOnlyEligiblePeer(t *testing.T) {
	connId := testConnId(1)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			stalledClient(connId, false),
			// Observability-only peers do not count toward eligibility.
			stalledClient(testConnId(9), true),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{
		Grace: 30 * time.Second,
	})

	now := time.Now()
	st := newTestTickState(100, now)
	st.recycleAt[connId.String()] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	assert.Empty(
		t,
		pub.byType(connmanager.ConnectionRecycleRequestedEventType),
		"the only eligible peer must never be recycled",
	)
	// The deadline is pushed out by the raw grace period so the peer is
	// re-evaluated later instead of being dropped from tracking.
	dueAt, ok := st.recycleAt[connId.String()]
	require.True(t, ok)
	assert.Equal(t, now.Add(30*time.Second), dueAt)
}

func TestTickSchedulesGuardedRecycleForNewlyStalledClient(t *testing.T) {
	connId := testConnId(1)
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{stalledClient(connId, false)},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{
		Grace: 30 * time.Second,
	})

	now := time.Now()
	st := newTestTickState(100, now)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	dueAt, ok := st.recycleAt[connId.String()]
	require.True(t, ok, "a newly stalled client must be scheduled")
	assert.Equal(t, now.Add(30*time.Second), dueAt)
	assert.Empty(t, pub.all(), "scheduling alone must not recycle")
}

func TestTickCatchUpExtendsGracePeriod(t *testing.T) {
	connId := testConnId(1)
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: false}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{stalledClient(connId, false)},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{
		Grace: 30 * time.Second,
	})

	now := time.Now()
	st := newTestTickState(100, now)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	dueAt, ok := st.recycleAt[connId.String()]
	require.True(t, ok)
	assert.Equal(
		t,
		now.Add(catchUpMultiplier*30*time.Second),
		dueAt,
		"grace is extended while catching up so bulk sync is not churned",
	)
}

func TestTickCatchUpDoesNotExtendPlateauThreshold(t *testing.T) {
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(100, 50),
		primaryChainTipSlot: 100,
		atTip:               false,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-5*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	r.tick(now, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, 100)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(t, events, 1)
	resyncEvt, ok := events[0].evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalTipPlateau,
		resyncEvt.Reason,
	)
}

func TestTickCatchUpDefersPlateauResyncWhilePrimaryChainAdvances(
	t *testing.T,
) {
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(100, 50),
		primaryChainTipSlot: 200,
		atTip:               false,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	plateauStarted := now.Add(-5 * time.Minute)
	st := newTestTickState(100, plateauStarted)
	r.tick(plateauStarted, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, 100)

	ledger.setPrimaryChainTipSlot(250)
	r.tick(now, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, 100)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"an advancing primary chain must not be treated as a stalled stream",
	)
}

func TestTickCatchUpTracksPrimaryChainChangesAfterRollback(t *testing.T) {
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(100, 50),
		primaryChainTipSlot: 200,
		atTip:               false,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-5*time.Minute))
	st.lastPrimaryChainTipSlot = 300
	r.tick(now, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, 100)
	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"a primary-chain rollback must refresh the plateau clock",
	)
	assert.Equal(t, now, st.lastProgressAt)

	ledger.setPrimaryChainTipSlot(250)
	advanceAt := now.Add(3 * time.Minute)
	r.tick(advanceAt, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, 100)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"progress after a primary-chain rollback must refresh the plateau clock",
	)
	assert.Equal(t, advanceAt, st.lastProgressAt)
}

func TestTickClearsScheduleWhenClientRecovers(t *testing.T) {
	connId := testConnId(1)
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{activeClient(connId, 100)},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now)
	st.recycleAt[connId.String()] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	assert.NotContains(t, st.recycleAt, connId.String())
	assert.Empty(t, pub.all())
}

func TestTickAdvancesProgressBaseline(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(200, 90), atTip: true}
	state := &fakeChainsyncState{}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-time.Hour))

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 200)

	assert.Equal(t, uint64(200), st.lastProgressSlot)
	assert.Equal(t, now, st.lastProgressAt)
	checks, rotations := state.counts()
	assert.Equal(t, 1, checks)
	assert.Equal(t, 1, rotations)
}

func TestTickPrunesExpiredCooldownEntries(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{
		Cooldown: time.Minute,
	})

	now := time.Now()
	st := newTestTickState(100, now)
	st.lastRecycled["expired"] = now.Add(-2 * time.Minute)
	st.lastRecycled["fresh"] = now.Add(-time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	assert.NotContains(t, st.lastRecycled, "expired")
	assert.Contains(t, st.lastRecycled, "fresh")
}

func TestTickPushesRecycleOutWhileInCooldown(t *testing.T) {
	connId := testConnId(1)
	connId2 := testConnId(2)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			stalledClient(connId, false),
			stalledClient(connId2, false),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{
		Cooldown: time.Minute,
	})

	now := time.Now()
	st := newTestTickState(100, now)
	st.recycleAt[connId.String()] = now.Add(-time.Second)
	st.lastRecycled[connId.String()] = now.Add(-20 * time.Second)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
	}, now, 100)

	assert.Empty(t, pub.byType(connmanager.ConnectionRecycleRequestedEventType))
	dueAt, ok := st.recycleAt[connId.String()]
	require.True(t, ok)
	assert.Equal(t, now.Add(40*time.Second), dueAt)
}

// plateauSelector builds a selector whose best peer sits ahead of the local tip.
func plateauSelector(
	connId ouroboros.ConnectionId,
	peerTipSlot uint64,
) *fakeChainSelector {
	best := connId
	return &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): {
				ConnectionId: connId,
				Tip:          testTip(peerTipSlot, peerTipSlot/2),
			},
		},
	}
}

func TestTickResyncsOnLocalTipPlateau(t *testing.T) {
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(t, events, 1)
	resyncEvt, ok := events[0].evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalTipPlateau,
		resyncEvt.Reason,
	)
	assert.False(
		t,
		events[0].async,
		"plateau resync is published synchronously",
	)
	assert.Equal(t, now, st.lastProgressAt, "plateau clock must reset")
	assert.Contains(t, st.lastRecycled, connId.String())
	assert.Equal(
		t,
		1,
		ledger.reconcileCallCount(),
		"a local ledger reconcile is always attempted first",
	)
	reason, reconciledConn := ledger.lastReconcile()
	assert.Equal(t, "local tip plateau", reason)
	assert.Equal(
		t,
		connId,
		reconciledConn,
		"reconcile must be attributed to the plateaued connection",
	)
}

// rollbackRegisteredPeer builds the chain-selection state a peer is left in
// immediately after a plateau resync: the connection was closed for a fresh
// chainsync, the peer re-intersected at the local tip and sent exactly one
// post-intersect RollBackward, so its DELIVERED frontier is that intersection
// point with no block number while its ADVERTISED tip is well ahead. This is
// the shape ApplyRollback produces for any rollback point outside the retained
// delivered-header history, and the shape #3989's rollback registration
// records for a peer chain selection has not seen a header from at all.
func rollbackRegisteredPeer(
	connId ouroboros.ConnectionId,
	intersectSlot uint64,
	advertisedSlot uint64,
) *chainselection.PeerChainTip {
	advertised := testTip(advertisedSlot, advertisedSlot/2)
	peerTip := chainselection.NewPeerChainTip(connId, advertised, nil)
	peerTip.ApplyRollback(
		ocommon.NewPoint(intersectSlot, []byte("intersect")),
		advertised,
	)
	return peerTip
}

// TestTickResyncsOnPlateauWhenBestPeerAwaitsFirstHeader is the watchdog's
// self-blinding case: the previous plateau resync reconnected the only
// upstream, which then delivered nothing but its post-intersect rollback. The
// peer's delivered frontier is therefore the stalled local tip, and comparing
// against it would disarm the watchdog for as long as the starvation lasts --
// three plateau cycles in twenty minutes were observed doing nothing at all.
// The peer's advertised tip is ahead and correct throughout, so the plateau
// must still fire.
func TestTickResyncsOnPlateauWhenBestPeerAwaitsFirstHeader(t *testing.T) {
	const (
		stalledSlot    = 2810012
		advertisedSlot = 2810823
	)
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(stalledSlot, 50),
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, stalledSlot)},
		activeConn: &active,
	}
	best := connId
	peerTip := rollbackRegisteredPeer(connId, stalledSlot, advertisedSlot)
	require.True(
		t,
		peerTip.AwaitingFirstHeader(),
		"fixture must reproduce a peer that has delivered no header",
	)
	require.Equal(
		t,
		uint64(stalledSlot),
		peerTip.SelectionTip().Point.Slot,
		"fixture must pin the delivered frontier at the stalled local tip",
	)
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, stalledSlot)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(
		t,
		events,
		1,
		"the plateau must still fire when the best peer has delivered no "+
			"header since the last resync",
	)
	resyncEvt, ok := events[0].evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalTipPlateau,
		resyncEvt.Reason,
	)
}

// TestTickDoesNotResyncWhenAwaitingPeerAdvertisesNoProgress pins the other
// side of the fallback: an advertised tip is only substituted when it is ahead
// of the local tip. A peer that has delivered no header and claims nothing
// beyond where we already are gives the watchdog no reason to act, so the
// untrusted advertised tip cannot manufacture a plateau on its own.
func TestTickDoesNotResyncWhenAwaitingPeerAdvertisesNoProgress(t *testing.T) {
	const stalledSlot = 2810012
	connId := testConnId(4)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(stalledSlot, 50),
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, stalledSlot)},
		activeConn: &active,
	}
	best := connId
	peerTip := rollbackRegisteredPeer(connId, stalledSlot, stalledSlot)
	require.True(t, peerTip.AwaitingFirstHeader())
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, stalledSlot)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"a peer advertising no progress must not trigger a plateau resync",
	)
	assert.Equal(t, 0, ledger.reconcileCallCount())
}

// TestTickIgnoresAdvertisedTipOnceBestPeerHasDeliveredAHeader pins that the
// fallback is confined to the awaiting-first-header window. Once a peer has
// delivered a header its delivered frontier is real evidence, and a peer that
// advertises a far tip while its chainsync cursor lags must not be able to
// drive the watchdog off that frontier -- the reason SelectionTip prefers the
// delivered value in the first place.
func TestTickIgnoresAdvertisedTipOnceBestPeerHasDeliveredAHeader(
	t *testing.T,
) {
	const stalledSlot = 2810012
	connId := testConnId(5)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(stalledSlot, 50),
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, stalledSlot)},
		activeConn: &active,
	}
	best := connId
	// Delivered frontier at the local tip WITH a block number: the peer has
	// shown us this header, so it is not awaiting its first one.
	peerTip := chainselection.NewPeerChainTip(
		connId,
		testTip(stalledSlot, 1405006),
		nil,
	)
	peerTip.UpdateTipWithObserved(
		testTip(stalledSlot+811, 1405411),
		testTip(stalledSlot, 1405006),
		nil,
	)
	require.False(t, peerTip.AwaitingFirstHeader())
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, stalledSlot)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"a delivered frontier that is not ahead of the local tip must keep "+
			"the watchdog disarmed, advertised tip notwithstanding",
	)
}

// TestTickPlateauFallbackNeverLowersTheBestPeerTip pins the fallback as
// strictly widening. A peer's advertised tip is untrusted input and nothing
// forces it to be consistent with the rollback point it just sent, so
// substituting it unconditionally would let a peer that rolled us forward
// while advertising a tip behind our own local tip disarm a watchdog that
// fires on main. The substitution therefore only ever raises the comparison
// value.
func TestTickPlateauFallbackNeverLowersTheBestPeerTip(t *testing.T) {
	const (
		localSlot      = 100
		rollbackSlot   = 200
		advertisedSlot = 50
	)
	connId := testConnId(6)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(localSlot, 50),
		primaryChainTipSlot: localSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, localSlot)},
		activeConn: &active,
	}
	best := connId
	peerTip := rollbackRegisteredPeer(connId, rollbackSlot, advertisedSlot)
	require.True(t, peerTip.AwaitingFirstHeader())
	require.Equal(
		t,
		uint64(rollbackSlot),
		peerTip.SelectionTip().Point.Slot,
		"fixture must put the delivered frontier ahead of the advertised tip",
	)
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(localSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, localSlot)

	require.Len(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		1,
		"a lower advertised tip must not suppress a plateau the delivered "+
			"frontier already justifies",
	)
}

// TestTickPlateauIgnoresAdvertisedTipOfAThirdPartyPeer pins the blast radius of
// trusting an advertised tip. The recycle targets the ACTIVE chainsync client,
// which need not be the peer that made the claim, so a peer that intersects at
// our local tip, fabricates a high advertised tip and then delivers nothing
// could otherwise drive a recycle of a different, honest upstream once per
// cooldown. The substitution is therefore confined to the connection the
// plateau would actually recycle: an awaiting peer can only ever spend its
// advertised tip on itself.
func TestTickPlateauIgnoresAdvertisedTipOfAThirdPartyPeer(t *testing.T) {
	const (
		stalledSlot    = 2810012
		fabricatedSlot = 9999999
	)
	liar := testConnId(7)
	// The honest upstream carrying our chainsync stream, which is NOT the peer
	// making the claim.
	activeConn := testConnId(8)
	active := activeConn
	ledger := &fakeLedger{
		tip:                 testTip(stalledSlot, 50),
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(activeConn, stalledSlot),
		},
		activeConn: &active,
	}
	best := liar
	peerTip := rollbackRegisteredPeer(liar, stalledSlot, fabricatedSlot)
	require.True(t, peerTip.AwaitingFirstHeader())
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			liar.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, stalledSlot)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"an unverified advertised tip must not recycle a different peer's "+
			"connection",
	)
	assert.Equal(t, 0, ledger.reconcileCallCount())
}

// TestTickPlateauUsesAdvertisedTipWithNoActiveClient covers the other half of
// that condition: with no active chainsync client the plateau falls back to the
// best peer as its target, so the awaiting peer is again the connection that
// gets recycled and its advertised tip is spent on itself. This is also the
// state a node reaches when the previous resync left it with no promoted
// client at all.
func TestTickPlateauUsesAdvertisedTipWithNoActiveClient(t *testing.T) {
	const (
		stalledSlot    = 2810012
		advertisedSlot = 2810823
	)
	connId := testConnId(9)
	ledger := &fakeLedger{
		tip:                 testTip(stalledSlot, 50),
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(connId, stalledSlot),
		},
	}
	best := connId
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): rollbackRegisteredPeer(
				connId,
				stalledSlot,
				advertisedSlot,
			),
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, stalledSlot)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(t, events, 1)
	resyncEvt, ok := events[0].evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(
		t,
		connId,
		resyncEvt.ConnectionId,
		"with no active client the plateau recycles the awaiting peer itself",
	)
}

// TestTickPlateauIgnoresAdvertisedTipOfAReplacedConnection pins that the
// own-connection check is ConnectionId equality, not the rendered address
// string. A replacement connection to the same peer (listen-port reuse) renders
// identically to the one it replaced while being a distinct connection, and
// chainselection keys peerTips by ConnectionId, so the selector can still be
// exposing the OLD peer's tip. Matching on the rendering would let that stale
// tip authorize recycling the replacement — which is the honest, active
// connection. The cooldown map keys on the rendering on purpose; an
// authorization must not.
func TestTickPlateauIgnoresAdvertisedTipOfAReplacedConnection(t *testing.T) {
	const (
		stalledSlot    = 2810012
		fabricatedSlot = 9999999
	)
	stale := testConnId(7)
	replacement := testConnId(7)
	require.Equal(
		t,
		stale.String(),
		replacement.String(),
		"fixture must render identically to exercise the string-match trap",
	)
	require.False(
		t,
		stale == replacement,
		"fixture must be a distinct connection identity",
	)

	active := replacement
	ledger := &fakeLedger{
		tip:                 testTip(stalledSlot, 50),
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(replacement, stalledSlot),
		},
		activeConn: &active,
	}
	best := stale
	peerTip := rollbackRegisteredPeer(stale, stalledSlot, fabricatedSlot)
	require.True(t, peerTip.AwaitingFirstHeader())
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			stale.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, stalledSlot)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"a stale peer tip must not authorize recycling the replacement "+
			"connection that merely renders the same",
	)
}

func TestTickPlateauRespectsCooldown(t *testing.T) {
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{
		Cooldown: 2 * time.Minute,
	})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))
	st.lastRecycled[connId.String()] = now.Add(-time.Minute)

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	assert.Empty(t, pub.byType(event.ChainsyncResyncEventType))
	assert.Equal(t, 0, ledger.reconcileCallCount())
}

func TestTickReconcileResolvesPlateauWithoutResync(t *testing.T) {
	connId := testConnId(8)
	active := connId
	ledger := &fakeLedger{
		tip:        testTip(100, 50),
		atTip:      true,
		reconciled: true,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	assert.Empty(
		t,
		pub.all(),
		"a successful local reconcile must not touch the connection",
	)
	assert.Equal(t, 1, ledger.reconcileCallCount())
	assert.Equal(t, now, st.lastProgressAt)
	assert.Contains(t, st.lastRecycled, connId.String())
}

func TestTickSuppressesResyncOnLedgerApplicationBacklog(t *testing.T) {
	connId := testConnId(8)
	active := connId
	ledger := &fakeLedger{
		tip: testTip(100, 50),
		// Header chain already caught up to the peer; the gap is
		// downloaded-but-not-yet-applied blocks.
		primaryChainTipSlot: 500,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 500)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	peerTip := selector.peerTips[connId.String()]
	require.NotNil(t, peerTip)
	peerTip.Tip = testTip(^uint64(0), ^uint64(0))
	peerTip.ObservedTip = testTip(500, 250)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	assert.Empty(
		t,
		pub.all(),
		"a ledger-application backlog must not recycle a healthy stream",
	)
	assert.Equal(
		t,
		1,
		ledger.reconcileCallCount(),
		"reconcile must run before the backlog heuristic is trusted",
	)
	assert.Equal(t, now, st.lastProgressAt)
}

func TestTickResyncsWhenReconcileFailsDespiteBacklog(t *testing.T) {
	connId := testConnId(8)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(100, 50),
		primaryChainTipSlot: 500,
		atTip:               true,
		reconcileErr:        assert.AnError,
	}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 500)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	require.Len(t, pub.byType(event.ChainsyncResyncEventType), 1)
}

func TestTickRealignsOtherPeersOnPlateau(t *testing.T) {
	connId := testConnId(3)
	aheadPeer := testConnId(4)
	behindPeer := testConnId(5)
	observability := testConnId(6)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	observabilityClient := activeClient(observability, 900)
	observabilityClient.ObservabilityOnly = true
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(connId, 100),
			activeClient(aheadPeer, 400),
			activeClient(behindPeer, 50),
			observabilityClient,
		},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(t, events, 2, "plateau resync plus one realign")
	realign := events[1].evt.Data.(event.ChainsyncResyncEvent)
	assert.Equal(
		t,
		aheadPeer,
		realign.ConnectionId,
		"only peers whose cursor raced ahead are realigned",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonPostPlateauRealign,
		realign.Reason,
	)
}

func TestTickSkipsRealignWithSingleEligiblePeer(t *testing.T) {
	connId := testConnId(3)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true}
	state := &fakeChainsyncState{
		tracked:    []chainsync.TrackedClient{activeClient(connId, 100)},
		activeConn: &active,
	}
	selector := plateauSelector(connId, 500)
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now.Add(-25*time.Minute))

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(
		t,
		events,
		1,
		"a single eligible peer still resyncs but has nothing to realign",
	)
}

func TestTickUpdatesChainSelectorLocalTipAndSecurityParam(t *testing.T) {
	ledger := &fakeLedger{
		tip:           testTip(100, 50),
		atTip:         true,
		securityParam: 432,
	}
	state := &fakeChainsyncState{}
	selector := &fakeChainSelector{}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	now := time.Now()
	st := newTestTickState(100, now)

	r.observeLocalTip(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	})
	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, 100)

	localTip, k, sets := selector.observed()
	assert.Equal(t, ledger.tip, localTip)
	assert.Equal(t, uint64(432), k)
	assert.Equal(t, 1, sets)
}

func TestTickSkipsSecurityParamWhenUnset(t *testing.T) {
	ledger := &fakeLedger{tip: testTip(100, 50), atTip: true, securityParam: 0}
	state := &fakeChainsyncState{}
	selector := &fakeChainSelector{}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{})

	r.observeLocalTip(LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	})

	_, _, sets := selector.observed()
	assert.Equal(t, 0, sets)
}
