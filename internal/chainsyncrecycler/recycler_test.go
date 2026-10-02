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
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The tests in this file drive a REAL chainselection.ChainSelector rather than
// fakeChainSelector, because the plateau fallback's whole premise is a claim
// about what chain selection holds immediately after a plateau resync. A fake
// that returns a peer tip for any caller cannot falsify that claim, and the
// unit tests built on it did not: they proved what the fallback does GIVEN an
// awaiting-first-header best peer, never that chain selection ever hands the
// watchdog one.
//
// It does, but only because of the rollback registration in chainselection.
// The sequence a plateau resync produces is:
//
//  1. the resync closes the connection (LocalTipPlateau is in
//     chainsyncResyncRequiresFreshConnection, ouroboros/chainsync.go);
//  2. the ConnectionClosedEvent subscription in node.go calls
//     ChainSelector.RemovePeer, which drops the peer tip and clears
//     bestPeerConn;
//  3. peer governance redials, and the replacement connection's first
//     chainsync traffic is the post-FindIntersect MsgRollBackward.
//
// Before, step 3 was dropped: HandlePeerRollbackEvent only updated an
// entry that already existed, and only a RollForward created one, so
// GetBestPeer() was nil and checkLocalTipPlateau returned before the fallback.
// registers the peer from that rollback and exempts an entry with no delivered
// header from the two behind-filters in isPeerSelectableLocked, which is what
// makes the peer selectable with a delivered block number of 0.

// realPlateauSelector builds a ChainSelector the way the node builds one, with
// every connection live, and returns it with the local tip already pushed in
// as the recycler's observeLocalTip does before each tick.
func realPlateauSelector(
	t *testing.T,
	securityParam uint64,
	localTip ochainsync.Tip,
) *chainselection.ChainSelector {
	t.Helper()
	sel := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		Logger:        discardLogger(),
		SecurityParam: securityParam,
		ConnectionLive: func(ouroboros.ConnectionId) bool {
			return true
		},
		// The rollbacks below are delivered synchronously by calling the
		// handler, so no bus subscription is wanted.
		DisableEventSubscriptions: true,
	})
	sel.SetLocalTip(localTip)
	return sel
}

// rollbackEvent builds the PeerRollbackEvent ouroboros publishes for a
// post-FindIntersect MsgRollBackward: the confirmed intersection point plus the
// peer's advertised tip.
func rollbackEvent(
	connId ouroboros.ConnectionId,
	intersect ocommon.Point,
	advertised ochainsync.Tip,
) event.Event {
	return event.NewEvent(
		chainselection.PeerRollbackEventType,
		chainselection.PeerRollbackEvent{
			ConnectionId: connId,
			Point:        intersect,
			Tip:          advertised,
		},
	)
}

// TestTickResyncsOnPlateauAfterRecycleWithRealChainSelector is the end-to-end
// case the test covers, driven through a real ChainSelector: a plateau
// resync closed the only upstream, the peer was removed on the
// ConnectionClosedEvent, the replacement reconnected and has sent nothing but
// its post-intersect rollback at the stalled local tip while advertising a tip
// well ahead. The watchdog must still fire on the next plateau window.
//
// Red without the fallback (the delivered frontier is the stalled local tip, so
// the plateau comparison sees no peer ahead), and red if the fix is reverted
// (the replacement is not tracked at all, so GetBestPeer is nil).
func TestTickResyncsOnPlateauAfterRecycleWithRealChainSelector(t *testing.T) {
	const (
		securityParam  = 2160
		stalledSlot    = 2810012
		stalledBlock   = 132455
		advertisedSlot = 2810823
	)
	oldConn := testConnId(3)
	replacement := testConnId(4)
	localTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(stalledSlot, []byte("local")),
		BlockNumber: stalledBlock,
	}
	sel := realPlateauSelector(t, securityParam, localTip)

	// The upstream was selected before the resync, on a real delivered
	// frontier.
	require.True(
		t,
		sel.UpdatePeerTip(oldConn, localTip, nil),
		"the pre-resync peer tip must be accepted",
	)
	require.Equal(t, &oldConn, sel.GetBestPeer())

	// The plateau resync closed that connection; node.go's
	// ConnectionClosedEvent subscription removes the peer.
	sel.RemovePeer(oldConn)
	require.Nil(
		t,
		sel.GetBestPeer(),
		"removing the only peer leaves chain selection with no best peer",
	)

	// The replacement connects and sends its post-intersect rollback: the
	// intersection is the stalled local tip, the advertised tip is ahead.
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(advertisedSlot, []byte("advertised")),
		BlockNumber: stalledBlock + 21,
	}
	sel.HandlePeerRollbackEvent(
		rollbackEvent(
			replacement,
			ocommon.NewPoint(stalledSlot, []byte("local")),
			advertised,
		),
	)
	best := sel.GetBestPeer()
	require.NotNil(
		t,
		best,
		"the replacement must be tracked and selectable from its rollback "+
			"alone (#3989); without that the watchdog cannot see it",
	)
	require.Equal(t, replacement, *best)
	bestTip := sel.GetPeerTip(*best)
	require.NotNil(t, bestTip)
	require.True(
		t,
		bestTip.AwaitingFirstHeader(),
		"the replacement has delivered no header yet",
	)
	require.Equal(
		t,
		uint64(stalledSlot),
		bestTip.SelectionTip().Point.Slot,
		"its delivered frontier is pinned at the stalled local tip, which is "+
			"what would disarm the watchdog",
	)

	active := replacement
	ledger := &fakeLedger{
		tip:                 localTip,
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
		securityParam:       securityParam,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(replacement, stalledSlot),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, sel, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = stalledSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  sel,
	}, now, stalledSlot)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(
		t,
		events,
		1,
		"the plateau must fire again for a replacement that has delivered no "+
			"header since the previous resync",
	)
	resyncEvt, ok := events[0].evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, replacement, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalTipPlateau,
		resyncEvt.Reason,
	)
	assert.Equal(t, now, st.lastProgressAt, "plateau clock must reset")
}

// TestTickDoesNotResyncOnceTheReplacementHasDeliveredAHeader pins the exit
// condition, and with it one half of the boundary of the whole fix: the
// advertised tip is substituted only while the peer has delivered NO header.
// Here the replacement registers from its rollback, delivers a header at the
// stalled local tip, then rolls back to a point inside its own retained
// delivered history -- so its delivered frontier carries a real block number
// again while its advertised tip is still far ahead. That is an ordinary peer
// serving our own chain, not a starved watchdog, and the untrusted
// advertisement must no longer be able to drive a recycle. The other half, a
// rollback OUTSIDE that history, which leaves no block number behind, is
// TestTickDoesNotResyncAfterADeliveredPeerRollsBackOutsideItsHistory.
//
// Red if AwaitingFirstHeader is forced true, which is the mutation that widens
// the fallback past its own precondition.
func TestTickDoesNotResyncOnceTheReplacementHasDeliveredAHeader(t *testing.T) {
	const (
		securityParam  = 2160
		stalledSlot    = 2810012
		stalledBlock   = 132455
		advertisedSlot = 2810823
	)
	replacement := testConnId(4)
	localTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(stalledSlot, []byte("local")),
		BlockNumber: stalledBlock,
	}
	sel := realPlateauSelector(t, securityParam, localTip)
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(advertisedSlot, []byte("advertised")),
		BlockNumber: stalledBlock + 21,
	}
	intersect := ocommon.NewPoint(stalledSlot, []byte("local"))
	sel.HandlePeerRollbackEvent(
		rollbackEvent(replacement, intersect, advertised),
	)

	// The replacement delivers its first header, at the frontier we are
	// already applied to, and then rolls back to it again -- a point inside
	// its own retained delivered history, so the block number is restored
	// rather than zeroed.
	require.True(t, sel.UpdatePeerTip(replacement, localTip, nil))
	sel.HandlePeerRollbackEvent(
		rollbackEvent(replacement, intersect, advertised),
	)

	bestTip := sel.GetPeerTip(replacement)
	require.NotNil(t, bestTip)
	require.False(
		t,
		bestTip.AwaitingFirstHeader(),
		"a rollback inside retained history keeps the delivered block number",
	)
	require.Greater(
		t,
		bestTip.Tip.Point.Slot,
		bestTip.SelectionTip().Point.Slot,
		"the advertised tip must still be ahead, so only the "+
			"awaiting-first-header precondition can be what stops the "+
			"substitution",
	)

	active := replacement
	ledger := &fakeLedger{
		tip:                 localTip,
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
		securityParam:       securityParam,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(replacement, stalledSlot),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, sel, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = stalledSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  sel,
	}, now, stalledSlot)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"a peer that has delivered a header is judged on its delivered "+
			"frontier, never on its advertisement, after a rollback inside "+
			"its retained history",
	)
}

// TestTickDoesNotResyncAfterADeliveredPeerRollsBackOutsideItsHistory pins the
// other half of that boundary. A peer that has delivered a header and then
// rolls back to a point OUTSIDE its retained delivered-header history ends up
// with the same bare frontier as a peer registered from its post-intersect
// rollback: ApplyRollback keeps the point but zeroes the delivered block
// number, because a RollBackward carries no block number of its own. It has
// nonetheless delivered headers on this connection, so it is not awaiting its
// first one, and the advertised-tip fallback must not apply to it.
//
// The local chain here is shallower than K. That is what keeps such a peer
// selectable at all: it is not exempt from the implausibly-behind filter in
// isPeerSelectableLocked (only a rollback-registered entry is), and on a chain
// deeper than K its zero block number filters it out, GetBestPeer is nil and
// the plateau check returns before reaching the fallback either way.
//
// Red if AwaitingFirstHeader is computed from the delivered block number
// rather than read from the flag chain selection sets only when it registers a
// peer from a rollback: this peer is where the two disagree.
func TestTickDoesNotResyncAfterADeliveredPeerRollsBackOutsideItsHistory(
	t *testing.T,
) {
	const (
		securityParam  = 2160
		stalledSlot    = 2810012
		stalledBlock   = 1000 // below K, so a zero block number is selectable
		rollbackSlot   = stalledSlot - 500
		advertisedSlot = 2810823
	)
	replacement := testConnId(4)
	localTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(stalledSlot, []byte("local")),
		BlockNumber: stalledBlock,
	}
	sel := realPlateauSelector(t, securityParam, localTip)
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(advertisedSlot, []byte("advertised")),
		BlockNumber: stalledBlock + 21,
	}
	intersect := ocommon.NewPoint(stalledSlot, []byte("local"))
	sel.HandlePeerRollbackEvent(
		rollbackEvent(replacement, intersect, advertised),
	)

	// The peer delivers a header at the local tip, then rolls back to an
	// earlier point it never delivered to us, so the point is outside its
	// retained delivered-header history.
	require.True(t, sel.UpdatePeerTip(replacement, localTip, nil))
	sel.HandlePeerRollbackEvent(
		rollbackEvent(
			replacement,
			ocommon.NewPoint(rollbackSlot, []byte("earlier")),
			advertised,
		),
	)

	best := sel.GetBestPeer()
	require.NotNil(
		t,
		best,
		"below K the peer must still be selectable, or this case would "+
			"not reach the fallback at all",
	)
	require.Equal(t, replacement, *best)
	bestTip := sel.GetPeerTip(replacement)
	require.NotNil(t, bestTip)
	require.Equal(
		t,
		uint64(0),
		bestTip.SelectionTip().BlockNumber,
		"a rollback outside retained history leaves a delivered frontier "+
			"with no block number -- the shape a rollback-registered peer has",
	)
	require.Equal(
		t,
		uint64(rollbackSlot),
		bestTip.SelectionTip().Point.Slot,
	)
	require.Greater(
		t,
		bestTip.Tip.Point.Slot,
		uint64(stalledSlot),
		"the advertised tip must be ahead of the local tip, so only the "+
			"awaiting-first-header precondition can be what stops the "+
			"substitution",
	)

	active := replacement
	ledger := &fakeLedger{
		tip:                 localTip,
		primaryChainTipSlot: stalledSlot,
		atTip:               true,
		securityParam:       securityParam,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(replacement, stalledSlot),
		},
		activeConn: &active,
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, sel, pub, Config{})

	now := time.Now()
	st := newTestTickState(stalledSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = stalledSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  sel,
	}, now, stalledSlot)

	assert.Empty(
		t,
		pub.byType(event.ChainsyncResyncEventType),
		"a peer that has delivered a header is judged on its delivered "+
			"frontier even after a rollback outside its retained history",
	)
	assert.False(
		t,
		bestTip.AwaitingFirstHeader(),
		"the peer has delivered a header on this connection",
	)
}

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

// TestTickDisconnectsPatienceExhaustedPeer pins that a Genesis Limit on
// Patience exhaustion closes the connection immediately, with its own reason,
// even for the only eligible peer and a client that is not stalled.
func TestTickDisconnectsPatienceExhaustedPeer(t *testing.T) {
	t.Parallel()
	connId := testConnId(1)
	active := connId
	ledger := &fakeLedger{tip: testTip(100, 50)}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{{
			ConnId: connId,
			Status: chainsync.ClientStatusSyncing,
		}},
		activeConn: &active,
		impatient:  []ouroboros.ConnectionId{connId},
	}
	pub := newFakePublisher()
	r, _ := newTestRecycler(t, ledger, state, nil, pub, Config{
		Grace:    time.Hour,
		Cooldown: time.Hour,
	})
	now := time.Now()
	st := newTestTickState(100, now)
	live := LiveComponents{Ledger: ledger, ChainsyncState: state}

	runTickWith(r, st, live, now, 100)

	events := pub.byType(connmanager.ConnectionRecycleRequestedEventType)
	require.Len(t, events, 1)
	recycleEvt, ok := events[0].evt.Data.(connmanager.ConnectionRecycleRequestedEvent)
	require.True(t, ok)
	assert.Equal(t, connId, recycleEvt.ConnectionId)
	assert.Equal(t, ReasonPatienceExhausted, recycleEvt.Reason)

	runTickWith(r, st, live, now.Add(time.Second), 100)
	assert.Len(
		t,
		pub.byType(connmanager.ConnectionRecycleRequestedEventType),
		1,
		"an exhausted client is disconnected once",
	)
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

// rollbackRegisteredPeer returns the peer tip a peer holds immediately after a
// plateau resync: its DELIVERED frontier is the point its session intersected
// at, carrying no block number, while its ADVERTISED tip is well ahead. It is
// obtained from a real ChainSelector that registers the peer from its
// post-FindIntersect rollback (registerPeerFromRollbackLocked), because only
// that path records the peer as awaiting its first header. Hand-building the
// shape with NewPeerChainTip plus ApplyRollback does not: a rollback outside
// the retained delivered-header history zeroes the delivered block number too,
// but that is a peer which HAS delivered a header, and it is not awaiting one
// (see TestTickDoesNotResyncAfterADeliveredPeerRollsBackOutsideItsHistory).
//
// The tests below pair the returned tip with fakeChainSelector, so they
// exercise the watchdog's decision GIVEN that peer tip and prove nothing about
// whether chain selection ever hands the watchdog one as its best peer. It
// does, but only through the rollback registration and the
// awaiting-first-header selectability exemption in chainselection; that half
// is covered end to end against a real ChainSelector in
// recycler_test.go, and reverting either of those two
// chainselection changes turns
// TestTickResyncsOnPlateauAfterRecycleWithRealChainSelector red.
func rollbackRegisteredPeer(
	connId ouroboros.ConnectionId,
	intersectSlot uint64,
	advertisedSlot uint64,
) *chainselection.PeerChainTip {
	sel := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		Logger: discardLogger(),
		ConnectionLive: func(ouroboros.ConnectionId) bool {
			return true
		},
		DisableEventSubscriptions: true,
	})
	sel.HandlePeerRollbackEvent(event.NewEvent(
		chainselection.PeerRollbackEventType,
		chainselection.PeerRollbackEvent{
			ConnectionId: connId,
			Point:        ocommon.NewPoint(intersectSlot, []byte("intersect")),
			Tip:          testTip(advertisedSlot, advertisedSlot/2),
		},
	))
	return sel.GetPeerTip(connId)
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

// plateauBacklogLogMsg is the INFO line the backlog branch logs in place of
// recycling a healthy chainsync stream.
const plateauBacklogLogMsg = "local tip plateau is a ledger-application backlog; header chain already caught up, not recycling chainsync"

// TestTickPlateauFallbackFeedsTheBacklogClassifier covers the interaction the
// other fallback cases leave untouched: the substituted advertised tip does not
// only decide whether the plateau arms, it flows on into
// isLedgerApplicationBacklog as bestPeerTipSlot and therefore sets headerGap
// and the backlog-versus-stall classification. Every other fallback case pins
// primaryChainTipSlot at the applied tip, which returns false on that
// predicate's first branch before headerGap is ever computed.
//
// Here the primary chain has covered the bulk of the distance to the
// advertised tip (applied 100, primary chain 900, advertised 1000), so the
// apply backlog of 800 dominates the residual header gap of 100: the plateau is
// real but the header stream is healthy and the ledger pipeline is simply
// draining. It must be logged, not recycled.
func TestTickPlateauFallbackFeedsTheBacklogClassifier(t *testing.T) {
	const (
		appliedSlot      = 100
		primaryChainSlot = 900
		advertisedSlot   = 1000
	)
	connId := testConnId(10)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(appliedSlot, 50),
		primaryChainTipSlot: primaryChainSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(connId, primaryChainSlot),
		},
		activeConn: &active,
	}
	best := connId
	peerTip := rollbackRegisteredPeer(connId, appliedSlot, advertisedSlot)
	require.True(t, peerTip.AwaitingFirstHeader())
	require.Equal(
		t,
		uint64(appliedSlot),
		peerTip.SelectionTip().Point.Slot,
		"fixture must pin the delivered frontier at the applied tip",
	)
	selector := &fakeChainSelector{
		bestPeer: &best,
		peerTips: map[string]*chainselection.PeerChainTip{
			connId.String(): peerTip,
		},
	}
	pub := newFakePublisher()
	logs := newLogSignalHandler(plateauBacklogLogMsg)
	r, _ := newTestRecycler(t, ledger, state, selector, pub, Config{
		Logger: slog.New(logs),
	})

	now := time.Now()
	st := newTestTickState(appliedSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, appliedSlot)

	// Without the fallback the delivered frontier equals the applied tip, the
	// plateau returns before reconciling and this count is zero -- which is
	// what makes this a regression test for the substitution rather than a
	// restatement of the backlog heuristic.
	require.Equal(
		t,
		1,
		ledger.reconcileCallCount(),
		"the advertised tip must arm the plateau far enough to reconcile",
	)
	assert.Empty(
		t,
		pub.all(),
		"an apply backlog that dominates the residual header gap must not "+
			"recycle a healthy chainsync stream",
	)
	select {
	case <-logs.signal(plateauBacklogLogMsg):
	default:
		t.Fatal("backlog plateau must be surfaced, not silently swallowed")
	}
	assert.Equal(t, now, st.lastProgressAt)
}

// TestTickPlateauFallbackResyncsWhenHeaderGapDominates is the other side of
// that classification. With the same applied tip and advertised tip but a
// primary chain that has barely moved (applied 100, primary chain 200,
// advertised 1000) the residual header gap of 800 dominates the apply backlog
// of 100: headers really are missing, so the substituted advertised tip must
// carry the plateau all the way to a resync rather than being absorbed by the
// backlog heuristic.
func TestTickPlateauFallbackResyncsWhenHeaderGapDominates(t *testing.T) {
	const (
		appliedSlot      = 100
		primaryChainSlot = 200
		advertisedSlot   = 1000
	)
	connId := testConnId(11)
	active := connId
	ledger := &fakeLedger{
		tip:                 testTip(appliedSlot, 50),
		primaryChainTipSlot: primaryChainSlot,
		atTip:               true,
	}
	state := &fakeChainsyncState{
		tracked: []chainsync.TrackedClient{
			activeClient(connId, primaryChainSlot),
		},
		activeConn: &active,
	}
	best := connId
	peerTip := rollbackRegisteredPeer(connId, appliedSlot, advertisedSlot)
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
	st := newTestTickState(appliedSlot, now.Add(-25*time.Minute))
	st.lastPrimaryChainTipSlot = ledger.primaryChainTipSlot

	runTickWith(r, st, LiveComponents{
		Ledger:         ledger,
		ChainsyncState: state,
		ChainSelector:  selector,
	}, now, appliedSlot)

	events := pub.byType(event.ChainsyncResyncEventType)
	require.Len(
		t,
		events,
		1,
		"a header gap that dominates the apply backlog must still resync, "+
			"even though the gap is only visible via the advertised tip",
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
