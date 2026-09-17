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
// It does, but only with the rollback registration this branch is stacked on
// (#3989). The sequence a plateau resync produces is:
//
//  1. the resync closes the connection (LocalTipPlateau is in
//     chainsyncResyncRequiresFreshConnection, ouroboros/chainsync.go);
//  2. the ConnectionClosedEvent subscription in node.go calls
//     ChainSelector.RemovePeer, which drops the peer tip and clears
//     bestPeerConn;
//  3. peer governance redials, and the replacement connection's first
//     chainsync traffic is the post-FindIntersect MsgRollBackward.
//
// On main, step 3 is dropped: HandlePeerRollbackEvent only updated an entry
// that already existed, and only a RollForward created one. GetBestPeer() is
// therefore nil and checkLocalTipPlateau returns before the fallback. #3989
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
// case #4139 claims to fix, driven through a real ChainSelector: a plateau
// resync closed the only upstream, the peer was removed on the
// ConnectionClosedEvent, the replacement reconnected and has sent nothing but
// its post-intersect rollback at the stalled local tip while advertising a tip
// well ahead. The watchdog must still fire on the next plateau window.
//
// Red without the fallback (the delivered frontier is the stalled local tip, so
// the plateau comparison sees no peer ahead), and red without #3989 underneath
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
// condition, and with it the boundary of the whole fix: the advertised tip is
// substituted only while the peer has delivered NO header. Here the
// replacement registers from its rollback, delivers a header at the stalled
// local tip, then rolls back to a point inside its own retained delivered
// history -- so its delivered frontier carries a real block number again while
// its advertised tip is still far ahead. That is an ordinary peer serving our
// own chain, not a starved watchdog, and the untrusted advertisement must no
// longer be able to drive a recycle.
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
	sel.HandlePeerRollbackEvent(rollbackEvent(replacement, intersect, advertised))

	// The replacement delivers its first header, at the frontier we are
	// already applied to, and then rolls back to it again -- a point inside
	// its own retained delivered history, so the block number is restored
	// rather than zeroed.
	require.True(t, sel.UpdatePeerTip(replacement, localTip, nil))
	sel.HandlePeerRollbackEvent(rollbackEvent(replacement, intersect, advertised))

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
			"frontier, never on its advertisement",
	)
}
