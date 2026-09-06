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

package chainselection

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRollbackEvent builds the PeerRollbackEvent the chainsync client publishes
// for a MsgRollBackward: the rollback point plus the peer's advertised tip.
func newRollbackEvent(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
) event.Event {
	return event.NewEvent(
		PeerRollbackEventType,
		PeerRollbackEvent{
			ConnectionId: connId,
			Point:        point,
			Tip:          tip,
		},
	)
}

// A connection recycle deletes the peer's tracked tip (RemovePeer). The
// replacement connection re-intersects and the server answers the first
// MsgRequestNext with MsgRollBackward to the intersection point, carrying its
// current tip. That rollback is the only chainsync traffic until the network
// mints its next block, so if it does not restore the peer to chain selection
// the node has no selectable peer for a whole block interval -- on a producer
// whose sole upstream was recycled, that is a total chain-selection outage
// ("chain selection stalled: no selectable peer" until the next RollForward).
func TestHandlePeerRollbackRegistersPeerAfterConnectionRecycle(t *testing.T) {
	connId := newTestConnectionId(1)
	live := true
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             2160,
		DisableEventSubscriptions: true,
		ConnectionLive: func(ouroboros.ConnectionId) bool {
			return live
		},
	})
	localTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 2614270, Hash: []byte("local-tip")},
		BlockNumber: 2614270,
	}
	cs.SetLocalTip(localTip)
	deliveredTip := ochainsync.Tip{
		Point:       localTip.Point,
		BlockNumber: localTip.BlockNumber,
	}
	require.True(t, cs.UpdatePeerTip(connId, deliveredTip, nil))
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())

	// The leios-fetch failover recycles the whole muxed connection.
	cs.RemovePeer(connId)
	require.Nil(t, cs.GetBestPeer())
	require.Equal(t, 0, cs.PeerCount())

	// The replacement connection intersects at the local tip and rolls back
	// to it, advertising the peer's current tip.
	advertisedTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 2614276,
			Hash: []byte("peer-advertised"),
		},
		BlockNumber: 2614276,
	}
	cs.HandlePeerRollbackEvent(
		newRollbackEvent(connId, localTip.Point, advertisedTip),
	)

	require.Equal(t, 1, cs.PeerCount(), "peer must be tracked again")
	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, advertisedTip, peerTip.Tip)
	assert.Equal(t, localTip.Point, peerTip.SelectionTip().Point)

	best := cs.GetBestPeer()
	require.NotNil(
		t,
		best,
		"peer registered from the post-intersect rollback must be selectable "+
			"without waiting for the next RollForward",
	)
	assert.Equal(t, connId, *best)
}

// A rollback can race the ConnectionClosedEvent that removed the peer. The
// roll-forward path drops tip updates from closed connections; registration
// from a rollback must do the same rather than resurrect a dead peer.
func TestHandlePeerRollbackDoesNotRegisterClosedConnection(t *testing.T) {
	connId := newTestConnectionId(1)
	var outcomes []RollbackRegistrationOutcome
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             2160,
		DisableEventSubscriptions: true,
		ConnectionLive: func(ouroboros.ConnectionId) bool {
			return false
		},
		OnRollbackRegistration: func(o RollbackRegistrationOutcome) {
			outcomes = append(outcomes, o)
		},
	})

	cs.HandlePeerRollbackEvent(
		newRollbackEvent(
			connId,
			ocommon.Point{Slot: 100, Hash: []byte("intersect")},
			ochainsync.Tip{
				Point:       ocommon.Point{Slot: 110, Hash: []byte("tip")},
				BlockNumber: 110,
			},
		),
	)

	assert.Equal(t, 0, cs.PeerCount())
	assert.Nil(t, cs.GetBestPeer())
	assert.Equal(
		t,
		[]RollbackRegistrationOutcome{RollbackRegistrationClosedConnection},
		outcomes,
	)
}

// A peer registered from a rollback has delivered nothing, so it must never
// displace a peer that has delivered headers, in either map iteration order.
func TestRollbackRegisteredPeerDoesNotOutrankDeliveredFrontier(t *testing.T) {
	for _, tc := range []struct {
		name            string
		rollbackFirst   bool
		deliveredConnId int
		rollbackConnId  int
	}{
		{name: "delivered peer first", deliveredConnId: 1, rollbackConnId: 2},
		{
			name:            "rollback peer first",
			rollbackFirst:   true,
			deliveredConnId: 2,
			rollbackConnId:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deliveredConn := newTestConnectionId(tc.deliveredConnId)
			rollbackConn := newTestConnectionId(tc.rollbackConnId)
			cs := NewChainSelector(ChainSelectorConfig{
				SecurityParam:             2160,
				DisableEventSubscriptions: true,
			})
			deliveredTip := ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 2614270,
					Hash: []byte("delivered"),
				},
				BlockNumber: 2614270,
			}
			register := func() {
				cs.HandlePeerRollbackEvent(
					newRollbackEvent(
						rollbackConn,
						ocommon.Point{
							Slot: 2614260,
							Hash: []byte("intersect"),
						},
						ochainsync.Tip{
							Point: ocommon.Point{
								Slot: 2614999,
								Hash: []byte("advertised"),
							},
							// A far-ahead advertisement must not buy
							// selection: only delivered frontiers rank.
							BlockNumber: 2614999,
						},
					),
				)
			}
			if tc.rollbackFirst {
				register()
				require.True(
					t,
					cs.UpdatePeerTip(deliveredConn, deliveredTip, nil),
				)
			} else {
				require.True(
					t,
					cs.UpdatePeerTip(deliveredConn, deliveredTip, nil),
				)
				register()
			}

			cs.EvaluateAndSwitch()
			best := cs.GetBestPeer()
			require.NotNil(t, best)
			assert.Equal(t, deliveredConn, *best)
			assert.Equal(t, 2, cs.PeerCount())
		})
	}
}

// Registration runs the same plausibility bound the roll-forward path applies
// to a new peer, so a newcomer cannot inject a far-ahead advertised tip while
// the node has no applied local tip to bound it against.
func TestHandlePeerRollbackRejectsImplausibleAdvertisedTipAtBootstrap(
	t *testing.T,
) {
	existingConn := newTestConnectionId(1)
	newConn := newTestConnectionId(2)
	var outcomes []RollbackRegistrationOutcome
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             10,
		DisableEventSubscriptions: true,
		OnRollbackRegistration: func(o RollbackRegistrationOutcome) {
			outcomes = append(outcomes, o)
		},
	})
	require.True(t, cs.UpdatePeerTip(existingConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("bootstrap")},
		BlockNumber: 100,
	}, nil))

	cs.HandlePeerRollbackEvent(
		newRollbackEvent(
			newConn,
			ocommon.Point{Slot: 100, Hash: []byte("bootstrap")},
			ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 9_000_000,
					Hash: []byte("inflated"),
				},
				BlockNumber: 9_000_000,
			},
		),
	)

	assert.Equal(t, 1, cs.PeerCount())
	assert.Nil(t, cs.GetPeerTip(newConn))
	assert.Equal(
		t,
		[]RollbackRegistrationOutcome{RollbackRegistrationImplausibleTip},
		outcomes,
	)
}

// Registration respects the tracked-peer capacity bound, refusing rather than
// growing the table past MaxTrackedPeers when nothing can be evicted.
func TestHandlePeerRollbackRefusesRegistrationAtCapacity(t *testing.T) {
	existingConn := newTestConnectionId(1)
	newConn := newTestConnectionId(2)
	var outcomes []RollbackRegistrationOutcome
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             10,
		MaxTrackedPeers:           1,
		DisableEventSubscriptions: true,
		OnRollbackRegistration: func(o RollbackRegistrationOutcome) {
			outcomes = append(outcomes, o)
		},
	})
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("delivered")},
		BlockNumber: 100,
	}
	require.True(t, cs.UpdatePeerTip(existingConn, tip, nil))
	cs.EvaluateAndSwitch()
	// The single tracked peer is the best peer, which evictLeastRecentPeerLocked
	// refuses to evict.
	require.NotNil(t, cs.GetBestPeer())

	cs.HandlePeerRollbackEvent(
		newRollbackEvent(
			newConn,
			ocommon.Point{Slot: 100, Hash: []byte("delivered")},
			tip,
		),
	)

	assert.Equal(t, 1, cs.PeerCount())
	assert.Nil(t, cs.GetPeerTip(newConn))
	assert.Equal(
		t,
		[]RollbackRegistrationOutcome{RollbackRegistrationAtCapacity},
		outcomes,
	)
	assert.Equal(t, existingConn, *cs.GetBestPeer())
}

// The behind-filter exemption lasts only until the peer delivers a header:
// once it has a real delivered frontier it is filtered like any other peer.
func TestRollbackRegisteredPeerLosesExemptionAfterFirstHeader(t *testing.T) {
	connId := newTestConnectionId(1)
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             10,
		DisableEventSubscriptions: true,
	})
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: []byte("local")},
		BlockNumber: 1000,
	})

	cs.HandlePeerRollbackEvent(
		newRollbackEvent(
			connId,
			ocommon.Point{Slot: 990, Hash: []byte("intersect")},
			ochainsync.Tip{
				Point:       ocommon.Point{Slot: 1010, Hash: []byte("tip")},
				BlockNumber: 1010,
			},
		),
	)
	require.NotNil(
		t,
		cs.GetBestPeer(),
		"a peer that has delivered nothing yet is not 'behind'",
	)

	// The peer then delivers a header from far behind the local tip. It is a
	// real delivered frontier now, so the implausibly-behind filter applies.
	behindTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10, Hash: []byte("far-behind")},
		BlockNumber: 10,
	}
	require.True(t, cs.UpdatePeerTip(connId, behindTip, nil))
	cs.EvaluateAndSwitch()

	assert.Nil(t, cs.GetBestPeer())
	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.False(t, peerTip.awaitingFirstHeader)
}

// A rollback for a tracked peer keeps its existing ApplyRollback path: the
// registration branch must not change how an already-tracked peer is handled.
func TestHandlePeerRollbackTrackedPeerStillAppliesRollback(t *testing.T) {
	connId := newTestConnectionId(1)
	var outcomes []RollbackRegistrationOutcome
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             10,
		DisableEventSubscriptions: true,
		OnRollbackRegistration: func(o RollbackRegistrationOutcome) {
			outcomes = append(outcomes, o)
		},
	})
	delivered := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("delivered")},
		BlockNumber: 100,
	}
	require.True(t, cs.UpdatePeerTip(connId, delivered, nil))

	rollbackPoint := ocommon.Point{Slot: 90, Hash: []byte("rollback")}
	advertised := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 105, Hash: []byte("advertised")},
		BlockNumber: 105,
	}
	cs.HandlePeerRollbackEvent(
		newRollbackEvent(connId, rollbackPoint, advertised),
	)

	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, advertised, peerTip.Tip)
	assert.Equal(t, rollbackPoint, peerTip.SelectionTip().Point)
	assert.False(
		t,
		peerTip.awaitingFirstHeader,
		"an already-tracked peer keeps its delivered-frontier semantics",
	)
	assert.Empty(t, outcomes, "no registration attempt for a tracked peer")
	assert.Equal(t, 1, cs.PeerCount())
}

// A peer registered from a rollback has delivered nothing, so it is not a
// plausibility reference: not for its own first header (which would otherwise
// be bounded by a reference of 0 and rejected) and not for another peer's
// (which would otherwise turn the bootstrap case into a reference of 0).
func TestRollbackRegisteredPeerIsNotAPlausibilityReference(t *testing.T) {
	rollbackConn := newTestConnectionId(1)
	otherConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             10,
		DisableEventSubscriptions: true,
	})
	// No local tip has been applied, so the catch-up relaxation cannot rescue
	// a rejected frontier: the reference bound is the only thing under test.
	cs.HandlePeerRollbackEvent(
		newRollbackEvent(
			rollbackConn,
			ocommon.Point{Slot: 5000, Hash: []byte("intersect")},
			ochainsync.Tip{
				Point:       ocommon.Point{Slot: 5001, Hash: []byte("tip")},
				BlockNumber: 5001,
			},
		),
	)
	require.Equal(t, 1, cs.PeerCount())

	// Another peer's first delivered header is still bootstrapped, not bounded
	// by the registered peer's zero delivered frontier.
	assert.True(t, cs.UpdatePeerTip(otherConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 5000, Hash: []byte("other")},
		BlockNumber: 5000,
	}, nil))

	// And the registered peer's own first header is bounded like a new peer's,
	// against the delivered frontier of the peers that have delivered one.
	assert.True(t, cs.UpdatePeerTip(rollbackConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 5001, Hash: []byte("first-header")},
		BlockNumber: 5001,
	}, nil))
	peerTip := cs.GetPeerTip(rollbackConn)
	require.NotNil(t, peerTip)
	assert.Equal(t, uint64(5001), peerTip.SelectionTip().BlockNumber)
	assert.False(t, peerTip.awaitingFirstHeader)
}

// A peer registered from a rollback has delivered no header, so it must not
// raise the Genesis exit horizon: its ObservedTip is the intersection point the
// node itself proposed, and crediting that as delivered evidence would let any
// peer that re-intersects at the local tip and advertises a tip within the
// Genesis window force an immediate Genesis-to-Praos transition, dropping the
// density protection that mode exists to provide.
func TestRollbackRegisteredPeerDoesNotForceGenesisExit(t *testing.T) {
	connId := newTestConnectionId(1)
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             100,
		GenesisMode:               true,
		GenesisWindowSlots:        300,
		DisableEventSubscriptions: true,
	})
	localPoint := ocommon.Point{Slot: 5000, Hash: []byte("local")}
	cs.SetLocalTip(ochainsync.Tip{Point: localPoint, BlockNumber: 5000})
	require.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	// Intersects at the local tip and advertises a tip inside the Genesis
	// window of it, without delivering anything.
	cs.HandlePeerRollbackEvent(
		newRollbackEvent(connId, localPoint, ochainsync.Tip{
			Point:       ocommon.Point{Slot: 5100, Hash: []byte("advertised")},
			BlockNumber: 5100,
		}),
	)
	require.Equal(t, 1, cs.PeerCount())

	assert.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	// Once the peer actually delivers headers up to its advertisement, the
	// horizon is real and the node exits Genesis as before.
	deliveredTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 5100, Hash: []byte("advertised")},
		BlockNumber: 5100,
	}
	require.True(t, cs.UpdatePeerTip(connId, deliveredTip, nil))
	assert.Equal(t, SelectionModePraos, cs.SelectionMode())
}

// ConnectionLive is supplied by the composition layer and reaches into the
// connection manager, so the registration path must evaluate it before taking
// cs.mutex. Holding the selector lock across that callback lets connection
// teardown ordering block -- or re-enter -- the chain-selection event path.
//
// The callback here re-enters the selector on the registration check and
// blocks until that re-entry completes, so if HandlePeerRollbackEvent held
// cs.mutex the whole handler would deadlock and the timeout below would fire.
func TestHandlePeerRollbackEvaluatesLivenessWithoutSelectorLock(t *testing.T) {
	connId := newTestConnectionId(1)
	var calls atomic.Int32
	reentered := make(chan struct{})
	var cs *ChainSelector
	cs = NewChainSelector(ChainSelectorConfig{
		SecurityParam:             2160,
		DisableEventSubscriptions: true,
		ConnectionLive: func(ouroboros.ConnectionId) bool {
			// Only the first call is the registration check; later calls come
			// from the evaluation path, which holds the lock by design.
			if calls.Add(1) != 1 {
				return true
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = cs.PeerCount()
				_ = cs.GetAllPeerTips()
			}()
			select {
			case <-done:
				close(reentered)
			case <-time.After(5 * time.Second):
			}
			return true
		},
	})

	handled := make(chan struct{})
	go func() {
		defer close(handled)
		cs.HandlePeerRollbackEvent(
			newRollbackEvent(
				connId,
				ocommon.Point{Slot: 2614270, Hash: []byte("intersect")},
				ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 2614276,
						Hash: []byte("peer-tip"),
					},
					BlockNumber: 2614276,
				},
			),
		)
	}()

	select {
	case <-handled:
	case <-time.After(20 * time.Second):
		t.Fatal(
			"HandlePeerRollbackEvent deadlocked: ConnectionLive must be " +
				"evaluated before cs.mutex is taken",
		)
	}
	select {
	case <-reentered:
	default:
		t.Fatal(
			"the selector lock was held while ConnectionLive ran: a callback " +
				"that touches the selector could not make progress",
		)
	}
	assert.Equal(t, 1, cs.PeerCount())
}
