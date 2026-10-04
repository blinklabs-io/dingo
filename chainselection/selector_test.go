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
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
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

// The registration outcome is reported after cs.mutex is released, so a
// subscriber can read selector state (for example to log the resulting peer
// count) from the callback without deadlocking. This is the same invariant as
// the liveness check above, for the other callback the registration path
// invokes: no configured callback may run with the selector lock held.
func TestHandlePeerRollbackReportsOutcomeWithoutSelectorLock(t *testing.T) {
	connId := newTestConnectionId(1)
	var observedPeers atomic.Int32
	var observedOutcome atomic.Value
	var cs *ChainSelector
	cs = NewChainSelector(ChainSelectorConfig{
		SecurityParam:             2160,
		DisableEventSubscriptions: true,
		ConnectionLive:            func(ouroboros.ConnectionId) bool { return true },
		OnRollbackRegistration: func(o RollbackRegistrationOutcome) {
			observedOutcome.Store(o)
			// Blocks forever if the outcome were reported under cs.mutex.
			// #nosec G115 -- a tracked-peer count fits int32 in this test.
			observedPeers.Store(int32(cs.PeerCount()))
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
			"HandlePeerRollbackEvent deadlocked: OnRollbackRegistration must " +
				"be reported outside cs.mutex",
		)
	}
	assert.Equal(
		t,
		RollbackRegistrationRegistered,
		observedOutcome.Load(),
	)
	assert.Equal(
		t,
		int32(1),
		observedPeers.Load(),
		"the callback must observe the peer it was told about",
	)
}

// The anti-flap incumbent pin exists to stop the chainsync+blockfetch pipeline
// from flapping between peers on sibling head-forks. A peer registered from a
// rollback has no head to fork from: its selection block number is 0, meaning
// "nothing delivered yet". Without an explicit release the pin's longer-chain
// escape (challenger more than catchUpPinHeadMargin blocks ahead) cannot fire
// for a challenger at block 1 or 2, so the node would keep the pipeline on a
// connection that has delivered no header while a peer with a real delivered
// frontier is available.
func TestRollbackRegisteredIncumbentDoesNotPinOutDeliveredChallenger(
	t *testing.T,
) {
	rollbackConn := newTestConnectionId(1)
	deliveredConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             2160,
		DisableEventSubscriptions: true,
	})
	// A local tip has been applied, so the pin is armed.
	localPoint := ocommon.Point{Slot: 1, Hash: []byte("local")}
	cs.SetLocalTip(ochainsync.Tip{Point: localPoint, BlockNumber: 1})

	// The recycled connection re-intersects at the local tip and becomes the
	// incumbent, because nothing better is tracked.
	cs.HandlePeerRollbackEvent(
		newRollbackEvent(rollbackConn, localPoint, tip(4, 4, "advertised")),
	)
	best := cs.GetBestPeer()
	require.NotNil(t, best)
	require.Equal(t, rollbackConn, *best)
	incumbentTip := cs.GetPeerTip(rollbackConn)
	require.NotNil(t, incumbentTip)
	require.True(t, incumbentTip.awaitingFirstHeader)
	require.Equal(t, uint64(0), incumbentTip.SelectionTip().BlockNumber)

	// Another peer delivers a real header, inside the head margin of the
	// incumbent's zero selection block number.
	require.True(
		t,
		cs.UpdatePeerTip(deliveredConn, tip(2, 2, "delivered"), nil),
	)
	cs.EvaluateAndSwitch()

	best = cs.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(
		t,
		deliveredConn,
		*best,
		"a delivered frontier must take the pipeline from a header-less "+
			"rollback incumbent",
	)
}

func newTestConnectionId(n int) ouroboros.ConnectionId {
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr(
		"tcp",
		fmt.Sprintf("127.0.0.1:%d", n+10000),
	)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

func setSelectorConnectionEligible(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	eligible bool,
) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.eligible[connId] = eligible
}

func setSelectorConnectionPriority(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	priority int,
) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	cs.priority[connId] = priority
}

func markSelectorPeerStale(
	t *testing.T,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
) {
	t.Helper()

	cs.mutex.Lock()
	defer cs.mutex.Unlock()

	peerTip, ok := cs.peerTips[connId]
	require.True(t, ok, "peer %s must exist before marking stale", connId)

	threshold := cs.config.StaleTipThreshold
	if threshold == 0 {
		threshold = defaultStaleTipThreshold
	}
	peerTip.LastUpdated = peerTip.now().Add(-(threshold + time.Millisecond))
}

// advancePastStale moves clk just past threshold and requires connId to be
// stale by it. The peer's staleness reads clk, so the result does not depend
// on how much wall-clock time the test has taken.
func advancePastStale(
	t *testing.T,
	cs *ChainSelector,
	clk *fakeClock,
	connId ouroboros.ConnectionId,
	threshold time.Duration,
) {
	t.Helper()
	clk.Advance(threshold + time.Millisecond)
	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	require.True(
		t,
		peerTip.IsStale(threshold),
		"peer %s should be stale",
		connId,
	)
}

func updatePeerTipWithPraosView(
	t *testing.T,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
	config PraosTiebreakerConfig,
) {
	t.Helper()

	accepted := cs.updatePeerTipObservedPraosView(
		connId,
		tip,
		tip,
		vrfOutput,
		PraosTiebreakerViewFromTip(tip, vrfOutput, config),
	)
	require.True(t, accepted)
}

func TestChainSelectorUpdatePeerTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(connId, tip, nil)

	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, tip.BlockNumber, peerTip.Tip.BlockNumber)
	assert.Equal(t, tip.Point.Slot, peerTip.Tip.Point.Slot)
	assert.Equal(t, 1, cs.PeerCount())
}

func TestChainSelectorIgnoresTipUpdateFromClosedConnection(t *testing.T) {
	connId := newTestConnectionId(1)
	cs := NewChainSelector(ChainSelectorConfig{
		ConnectionLive: func(candidate ouroboros.ConnectionId) bool {
			return candidate != connId
		},
	})

	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	accepted := cs.UpdatePeerTip(connId, tip, nil)
	assert.False(t, accepted)
	assert.Nil(t, cs.GetPeerTip(connId))
	assert.Equal(t, 0, cs.PeerCount())
}

func TestChainSelectorSkipsClosedTrackedPeerDuringEvaluation(t *testing.T) {
	live := make(map[string]bool)
	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)
	live[connId1.String()] = true
	live[connId2.String()] = true
	cs := NewChainSelector(ChainSelectorConfig{
		ConnectionLive: func(connId ouroboros.ConnectionId) bool {
			return live[connId.String()]
		},
	})

	cs.UpdatePeerTip(connId1, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("peer-1")},
		BlockNumber: 100,
	}, nil)
	cs.UpdatePeerTip(connId2, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("peer-2")},
		BlockNumber: 120,
	}, nil)

	bestPeer := cs.GetBestPeer()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId2, *bestPeer)

	live[connId2.String()] = false
	assert.True(t, cs.EvaluateAndSwitch())

	bestPeer = cs.GetBestPeer()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId1, *bestPeer)
}

func TestChainSelectorUpdateExistingPeerTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId := newTestConnectionId(1)
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test1")},
		BlockNumber: 50,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110, Hash: []byte("test2")},
		BlockNumber: 55,
	}

	cs.UpdatePeerTip(connId, tip1, nil)
	cs.UpdatePeerTip(connId, tip2, nil)

	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, tip2.BlockNumber, peerTip.Tip.BlockNumber)
	assert.Equal(t, tip2.Point.Slot, peerTip.Tip.Point.Slot)
	assert.Equal(t, 1, cs.PeerCount())
}

func TestChainSelectorPrefersMoreAdvancedObservedTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	laggingConn := newTestConnectionId(1)
	leadingConn := newTestConnectionId(2)

	laggingAdvertisedTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 200,
			Hash: []byte("lagging-advertised"),
		},
		BlockNumber: 200,
	}
	laggingObservedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("lagging-observed")},
		BlockNumber: 120,
	}
	leadingAdvertisedTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 180,
			Hash: []byte("leading-advertised"),
		},
		BlockNumber: 180,
	}
	leadingObservedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 150, Hash: []byte("leading-observed")},
		BlockNumber: 150,
	}

	cs.updatePeerTipObserved(
		laggingConn,
		laggingAdvertisedTip,
		laggingObservedTip,
		nil,
	)
	cs.updatePeerTipObserved(
		leadingConn,
		leadingAdvertisedTip,
		leadingObservedTip,
		nil,
	)

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, leadingConn, *bestPeer)
}

func TestChainSelectorRemovePeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(connId, tip, nil)
	setSelectorConnectionEligible(cs, connId, false)
	setSelectorConnectionPriority(cs, connId, 20)
	assert.Equal(t, 1, cs.PeerCount())

	cs.RemovePeer(connId)
	assert.Equal(t, 0, cs.PeerCount())
	assert.Nil(t, cs.GetPeerTip(connId))
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	_, eligibleFound := cs.eligible[connId]
	_, priorityFound := cs.priority[connId]
	assert.False(t, eligibleFound)
	assert.False(t, priorityFound)
}

func TestChainSelectorRemoveBestPeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(connId, tip, nil)
	cs.EvaluateAndSwitch()

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId, *cs.GetBestPeer())

	cs.RemovePeer(connId)
	assert.Nil(t, cs.GetBestPeer())
}

// drainChainSwitchesUntilBest consumes chain switch events up to and including
// the one that selects wantConn.
//
// ChainSelector.publishSelection hands events to an EventBus ordered lane
// instead of delivering them inline, so "whatever is queued at this instant"
// is not a barrier: a drain written as len(ch) can run ahead of the setup
// switches and then read one of them as the event under test. The switch to
// the peer the test has just asserted is best is a real barrier.
func drainChainSwitchesUntilBest(
	t *testing.T,
	evtCh <-chan event.Event,
	wantConn ouroboros.ConnectionId,
) {
	t.Helper()
	for {
		evt := testutil.RequireReceive(
			t,
			evtCh,
			5*time.Second,
			"chain switch event selecting the expected best peer",
		)
		data, ok := evt.Data.(ChainSwitchEvent)
		if ok && data.NewConnectionId.String() == wantConn.String() {
			return
		}
	}
}

func TestChainSelectorRemoveBestPeerEmitsChainSwitchEvent(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cs := NewChainSelector(ChainSelectorConfig{
		EventBus: eventBus,
	})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 60, // Best peer
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	// Subscribe to ChainSwitchEvent before adding peers
	_, evtCh := eventBus.Subscribe(ChainSwitchEventType)

	cs.UpdatePeerTip(connId1, tip1, nil)
	cs.UpdatePeerTip(connId2, tip2, nil)
	cs.EvaluateAndSwitch()

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	// Drain the events from the initial selection
	drainChainSwitchesUntilBest(t, evtCh, connId1)

	// Remove the best peer - this should emit ChainSwitchEvent
	cs.RemovePeer(connId1)

	// Verify the new best peer is selected
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())

	// Verify ChainSwitchEvent was emitted
	evt := testutil.RequireReceive(
		t,
		evtCh,
		5*time.Second,
		"expected ChainSwitchEvent was not emitted",
	)
	switchEvt, ok := evt.Data.(ChainSwitchEvent)
	require.True(t, ok, "expected ChainSwitchEvent")
	assert.Equal(t, connId1, switchEvt.PreviousConnectionId)
	assert.Equal(t, connId2, switchEvt.NewConnectionId)
	assert.Equal(t, tip2.BlockNumber, switchEvt.NewTip.BlockNumber)
}

func TestChainSelectorSelectBestChain(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)
	connId3 := newTestConnectionId(3)

	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 40,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 95, Hash: []byte("tip2")},
		BlockNumber: 50,
	}
	tip3 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip3")},
		BlockNumber: 45,
	}

	cs.UpdatePeerTip(connId1, tip1, nil)
	cs.UpdatePeerTip(connId2, tip2, nil)
	cs.UpdatePeerTip(connId3, tip3, nil)

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId2, *bestPeer)
}

func TestChainSelectorSelectBestChainEmpty(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})
	assert.Nil(t, cs.SelectBestChain())
}

func TestChainSelectorEvaluateAndSwitch(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 40,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	// UpdatePeerTip now automatically triggers evaluation when a better tip
	// is received, so after adding the first peer, it should be selected
	cs.UpdatePeerTip(connId1, tip1, nil)
	assert.Equal(t, connId1, *cs.GetBestPeer())

	// Adding a peer with a better tip automatically triggers evaluation
	cs.UpdatePeerTip(connId2, tip2, nil)
	assert.Equal(t, connId2, *cs.GetBestPeer())

	// Calling EvaluateAndSwitch again should return false since no change
	switched := cs.EvaluateAndSwitch()
	assert.False(t, switched)
}

func TestIncumbentAdvantageNoSwitchAtEqualBlockNumber(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	cs.UpdatePeerTip(connId1, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	cs.UpdatePeerTip(connId2, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("tip2")},
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())
}

func TestIncumbentAdvantageSwitchesWhenBehind(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	cs.UpdatePeerTip(connId1, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	cs.UpdatePeerTip(connId2, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("tip2")},
		BlockNumber: 52,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())
}

func TestIncumbentAdvantageSwitchesOnOneBlockLead(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	cs.UpdatePeerTip(connId1, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	cs.UpdatePeerTip(connId2, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("tip2")},
		BlockNumber: 51,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())
}

func TestNoOscillationWithMultiplePeersAtSameTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)
	connId3 := newTestConnectionId(3)

	for _, peer := range []struct {
		conn ouroboros.ConnectionId
		slot uint64
		hash []byte
	}{
		{conn: connId1, slot: 100, hash: []byte("tip1")},
		{conn: connId2, slot: 101, hash: []byte("tip2")},
		{conn: connId3, slot: 102, hash: []byte("tip3")},
	} {
		cs.UpdatePeerTip(peer.conn, ochainsync.Tip{
			Point:       ocommon.Point{Slot: peer.slot, Hash: peer.hash},
			BlockNumber: 50,
		}, nil)
	}

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	cs.UpdatePeerTip(connId2, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 103, Hash: []byte("tip2b")},
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())
}

func TestChainSelectorSkipsIneligiblePeers(t *testing.T) {
	ineligibleConn := newTestConnectionId(1)
	eligibleConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})
	setSelectorConnectionEligible(cs, ineligibleConn, false)

	cs.UpdatePeerTip(ineligibleConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("tip1")},
		BlockNumber: 60,
	}, nil)
	assert.Nil(t, cs.GetBestPeer())

	cs.UpdatePeerTip(eligibleConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110, Hash: []byte("tip2")},
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, eligibleConn, *cs.GetBestPeer())

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, eligibleConn, *bestPeer)
}

func TestChainSelectorDoesNotSwitchToIneligiblePeerAfterDisconnect(
	t *testing.T,
) {
	eligibleConn := newTestConnectionId(1)
	ineligibleConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})
	setSelectorConnectionEligible(cs, ineligibleConn, false)

	cs.UpdatePeerTip(eligibleConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 50,
	}, nil)
	cs.UpdatePeerTip(ineligibleConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110, Hash: []byte("tip2")},
		BlockNumber: 60,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, eligibleConn, *cs.GetBestPeer())

	cs.RemovePeer(eligibleConn)
	assert.Nil(t, cs.GetBestPeer())
}

func TestChainSelectorSwitchesAwayWhenIncumbentBecomesIneligible(t *testing.T) {
	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})

	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}
	cs.UpdatePeerTip(incumbentConn, equalTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	cs.UpdatePeerTip(challengerConn, equalTip, nil)
	setSelectorConnectionEligible(cs, incumbentConn, false)
	switched := cs.EvaluateAndSwitch()
	assert.True(t, switched)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challengerConn, *cs.GetBestPeer())
}

func TestChainSelectorDoesNotRestoreStaleIncumbent(t *testing.T) {
	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{
		StaleTipThreshold: 20 * time.Millisecond,
	})
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clk)

	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}
	cs.UpdatePeerTip(incumbentConn, equalTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	advancePastStale(t, cs, clk, incumbentConn, 20*time.Millisecond)

	cs.UpdatePeerTip(challengerConn, equalTip, nil)
	switched := cs.EvaluateAndSwitch()
	assert.True(t, switched)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challengerConn, *cs.GetBestPeer())
}

func TestChainSelectorSelectBestChainPrefersHigherPriorityPeerAtEqualTip(
	t *testing.T,
) {
	publicRootConn := newTestConnectionId(1)
	localRootConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})
	setSelectorConnectionPriority(cs, localRootConn, 20)
	setSelectorConnectionPriority(cs, publicRootConn, 10)

	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}

	cs.UpdatePeerTip(publicRootConn, equalTip, nil)
	cs.UpdatePeerTip(localRootConn, equalTip, nil)

	cs.mutex.Lock()
	cs.bestPeerConn = nil
	cs.mutex.Unlock()

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, localRootConn, *bestPeer)
}

func TestChainSelectorPreservesEqualTipIncumbentAtSamePriority(t *testing.T) {
	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})

	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}

	cs.UpdatePeerTip(connId2, equalTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())

	cs.UpdatePeerTip(connId1, equalTip, nil)

	switched := cs.EvaluateAndSwitch()
	assert.False(t, switched)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())
}

func TestChainSelectorSwitchesOnOneBlockObservedTipLead(t *testing.T) {
	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})

	confirmedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("confirmed")},
		BlockNumber: 50,
	}

	// Incumbent is established at the confirmed tip.
	cs.UpdatePeerTip(incumbentConn, confirmedTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	// Challenger has the same confirmed Tip but has received one block header
	// ahead via ObservedTip — simulating "announced header before incumbent did".
	oneAheadTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("one-ahead")},
		BlockNumber: 51,
	}
	// UpdatePeerTip takes cs.mutex itself, so the challenger must be registered
	// before the lock is acquired to reach its PeerChainTip directly.
	cs.UpdatePeerTip(challengerConn, confirmedTip, nil)
	func() {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		pt, ok := cs.peerTips[challengerConn]
		require.True(t, ok, "challenger must be registered before observing")
		pt.UpdateTipWithObserved(confirmedTip, oneAheadTip, nil)
	}()

	switched := cs.EvaluateAndSwitch()
	assert.True(
		t,
		switched,
		"reference implementation chain selection follows the longer chain",
	)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challengerConn, *cs.GetBestPeer())
}

func TestChainSelectorPreservesEqualTipIncumbentAtSamePriorityWithVRF(
	t *testing.T,
) {
	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})

	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}
	vrfHigher := make64ByteVRF(0xFF)
	vrfLower := make64ByteVRF(0x00)

	cs.UpdatePeerTip(incumbentConn, equalTip, vrfHigher)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	cs.UpdatePeerTip(challengerConn, equalTip, vrfLower)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	switched := cs.EvaluateAndSwitch()
	assert.False(t, switched)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())
}

func TestChainSelectorUpdatesConnectionStateViaSetters(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})
	connId := newTestConnectionId(1)

	// Default state: eligible (not in map), priority 0.
	assert.True(t, cs.isConnectionEligible(connId))
	assert.Equal(t, 0, cs.connectionPriority(connId))

	cs.SetConnectionEligible(connId, false)
	cs.SetConnectionPriority(connId, 40)

	assert.False(t, cs.isConnectionEligible(connId))
	assert.Equal(t, 40, cs.connectionPriority(connId))
}

func TestChainSelectorSwitchesImmediatelyOnEligibilityChange(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		EvaluationInterval: time.Hour,
	})
	ctx := t.Context()
	require.NoError(t, cs.Start(ctx))

	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}

	cs.UpdatePeerTip(incumbentConn, equalTip, nil)
	cs.UpdatePeerTip(challengerConn, equalTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	cs.SetConnectionEligible(incumbentConn, false)

	require.Eventually(t, func() bool {
		bestPeer := cs.GetBestPeer()
		return bestPeer != nil && *bestPeer == challengerConn
	}, time.Second, 5*time.Millisecond)
}

func TestChainSelectorDoesNotSwitchEqualTipIncumbentOnPriorityChange(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{
		EvaluationInterval: time.Hour,
	})
	ctx := t.Context()
	require.NoError(t, cs.Start(ctx))

	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}

	cs.UpdatePeerTip(incumbentConn, equalTip, nil)
	cs.UpdatePeerTip(challengerConn, equalTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	cs.SetConnectionPriority(challengerConn, 40)

	require.Never(t, func() bool {
		bestPeer := cs.GetBestPeer()
		return bestPeer != nil && *bestPeer == challengerConn
	}, 100*time.Millisecond, 5*time.Millisecond)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())
}

func TestChainSelectorDoesNotPreserveImplausiblyBehindIncumbent(t *testing.T) {
	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2,
	})

	cs.UpdatePeerTip(connId1, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 98, Hash: []byte("tip1")},
		BlockNumber: 98,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("local")},
		BlockNumber: 101,
	})

	cs.UpdatePeerTip(connId2, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 99, Hash: []byte("tip2")},
		BlockNumber: 99,
	}, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())
}

func TestChainSelectorKeepsIncumbentWhenPraosTiebreakerNotArmed(t *testing.T) {
	incumbentConn := newTestConnectionId(1)
	challengerConn := newTestConnectionId(2)
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 130, Hash: []byte("incumbent")},
		BlockNumber: 60,
	}
	challengerTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("challenger")},
		BlockNumber: 60,
	}

	cs.UpdatePeerTip(incumbentConn, incumbentTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	cs.UpdatePeerTip(challengerConn, challengerTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())
}

func TestChainSelectorStalePeerFiltering(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		StaleTipThreshold: 100 * time.Millisecond,
	})
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clk)

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 60,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(connId1, tip1, nil)

	advancePastStale(t, cs, clk, connId1, 100*time.Millisecond)

	cs.UpdatePeerTip(connId2, tip2, nil)

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId2, *bestPeer)
}

func TestChainSelectorStalePeerCleanupEmitsChainSwitchEvent(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          eventBus,
		StaleTipThreshold: 50 * time.Millisecond,
	})
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clk)

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 60, // Best peer
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	// Subscribe to ChainSwitchEvent before adding peers
	_, evtCh := eventBus.Subscribe(ChainSwitchEventType)

	cs.UpdatePeerTip(connId1, tip1, nil)
	cs.UpdatePeerTip(connId2, tip2, nil)
	cs.EvaluateAndSwitch()

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	// Drain the events from the initial selection
	drainChainSwitchesUntilBest(t, evtCh, connId1)

	// cleanupStalePeers removes peers stale by 2x StaleTipThreshold.
	advancePastStale(t, cs, clk, connId1, 100*time.Millisecond)

	// Keep peer2 fresh
	cs.UpdatePeerTip(connId2, tip2, nil)

	// Trigger cleanup - this should emit ChainSwitchEvent when best peer is
	// removed
	setSelectorConnectionEligible(cs, connId1, false)
	setSelectorConnectionPriority(cs, connId1, 10)
	cs.cleanupStalePeers()

	// Verify the new best peer is selected
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())

	// Verify ChainSwitchEvent was emitted
	evt := testutil.RequireReceive(
		t,
		evtCh,
		5*time.Second,
		"expected ChainSwitchEvent was not emitted",
	)
	switchEvt, ok := evt.Data.(ChainSwitchEvent)
	require.True(t, ok, "expected ChainSwitchEvent")
	assert.Equal(t, connId1, switchEvt.PreviousConnectionId)
	assert.Equal(t, connId2, switchEvt.NewConnectionId)
	assert.Equal(t, tip2.BlockNumber, switchEvt.NewTip.BlockNumber)
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	_, eligibleFound := cs.eligible[connId1]
	_, priorityFound := cs.priority[connId1]
	assert.False(t, eligibleFound)
	assert.False(t, priorityFound)
}

func TestChainSelectorGetAllPeerTips(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 40,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(connId1, tip1, nil)
	cs.UpdatePeerTip(connId2, tip2, nil)

	allTips := cs.GetAllPeerTips()
	assert.Len(t, allTips, 2)
	assert.Contains(t, allTips, connId1)
	assert.Contains(t, allTips, connId2)
}

func TestPeerChainTipIsStale(t *testing.T) {
	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	peerTip := NewPeerChainTip(connId, tip, nil)
	assert.False(t, peerTip.IsStale(100*time.Millisecond))

	// Wait until the peer tip actually becomes stale
	require.Eventually(t, func() bool {
		return peerTip.IsStale(100 * time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "peer tip should become stale")
}

func TestPeerChainTipUpdateTip(t *testing.T) {
	connId := newTestConnectionId(1)
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test1")},
		BlockNumber: 50,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110, Hash: []byte("test2")},
		BlockNumber: 55,
	}

	peerTip := NewPeerChainTip(connId, tip1, nil)
	oldTime := peerTip.LastUpdated

	// Wait briefly so the next UpdateTip call gets a different timestamp
	require.Eventually(t, func() bool {
		return time.Since(oldTime) > 0
	}, 1*time.Second, time.Millisecond, "time should advance")

	peerTip.UpdateTip(tip2, nil)
	assert.Equal(t, tip2.BlockNumber, peerTip.Tip.BlockNumber)
	assert.Equal(t, tip2.BlockNumber, peerTip.ObservedTip.BlockNumber)
	assert.True(t, peerTip.LastUpdated.After(oldTime))
}

func TestPeerChainTipUpdateTipWithObserved(t *testing.T) {
	connId := newTestConnectionId(1)
	initialTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 199, Hash: []byte("initial")},
		BlockNumber: 199,
	}
	advertisedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200, Hash: []byte("advertised")},
		BlockNumber: 200,
	}
	observedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 150, Hash: []byte("observed")},
		BlockNumber: 150,
	}

	peerTip := NewPeerChainTip(connId, initialTip, nil)
	peerTip.UpdateTipWithObserved(advertisedTip, observedTip, nil)

	assert.Equal(t, advertisedTip, peerTip.Tip)
	assert.Equal(t, observedTip, peerTip.ObservedTip)
	assert.Equal(t, observedTip.BlockNumber, peerTip.SelectionTip().BlockNumber)
}

func TestPeerChainTipTouch(t *testing.T) {
	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	peerTip := NewPeerChainTip(connId, tip, nil)
	require.Eventually(t, func() bool {
		return peerTip.IsStale(50 * time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "peer tip should become stale")
	oldTime := peerTip.LastUpdated

	peerTip.Touch()

	assert.True(t, peerTip.LastUpdated.After(oldTime))
	assert.False(t, peerTip.IsStale(50*time.Millisecond))
}

func TestChainSelectorTouchPeerActivityRevivesStaleBestPeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		StaleTipThreshold: 50 * time.Millisecond,
	})
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clk)

	bestConn := newTestConnectionId(1)
	freshConn := newTestConnectionId(2)
	bestTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("best")},
		BlockNumber: 60,
	}
	freshTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("fresh")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(bestConn, bestTip, nil)
	cs.UpdatePeerTip(freshConn, freshTip, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, bestConn, *cs.GetBestPeer())

	advancePastStale(t, cs, clk, bestConn, 50*time.Millisecond)

	cs.UpdatePeerTip(freshConn, freshTip, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, freshConn, *cs.GetBestPeer())

	cs.TouchPeerActivity(bestConn)

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, bestConn, *cs.GetBestPeer())
}

func TestChainSelectorTouchPeerActivitySwitchesToLongerChain(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		StaleTipThreshold: 50 * time.Millisecond,
	})
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clk)

	revivedConn := newTestConnectionId(1)
	incumbentConn := newTestConnectionId(2)
	revivedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("revived")},
		BlockNumber: 51,
	}
	incumbentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("incumbent")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(revivedConn, revivedTip, nil)
	cs.UpdatePeerTip(incumbentConn, incumbentTip, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, revivedConn, *cs.GetBestPeer())

	advancePastStale(t, cs, clk, revivedConn, 50*time.Millisecond)

	cs.UpdatePeerTip(incumbentConn, incumbentTip, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	cs.TouchPeerActivity(revivedConn)

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(
		t,
		revivedConn,
		*cs.GetBestPeer(),
		"a revived one-block-longer peer must win under reference implementation chain selection",
	)
}

func TestChainSelectorTouchPeerActivityEmitsChainSwitchEvent(t *testing.T) {
	t.Parallel()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          eventBus,
		StaleTipThreshold: 50 * time.Millisecond,
	})

	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clk)

	revivedConn := newTestConnectionId(1)
	incumbentConn := newTestConnectionId(2)
	revivedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("revived")},
		BlockNumber: 60,
	}
	incumbentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("incumbent")},
		BlockNumber: 50,
	}

	_, evtCh := eventBus.Subscribe(ChainSwitchEventType)

	cs.UpdatePeerTip(revivedConn, revivedTip, nil)
	cs.UpdatePeerTip(incumbentConn, incumbentTip, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, revivedConn, *cs.GetBestPeer())

	drainChainSwitchesUntilBest(t, evtCh, revivedConn)

	advancePastStale(t, cs, clk, revivedConn, 50*time.Millisecond)

	cs.UpdatePeerTip(incumbentConn, incumbentTip, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	drainChainSwitchesUntilBest(t, evtCh, incumbentConn)

	cs.TouchPeerActivity(revivedConn)

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, revivedConn, *cs.GetBestPeer())

	activityEvt := testutil.RequireReceive(
		t,
		evtCh,
		5*time.Second,
		"activity-driven switch should emit event",
	)
	switchEvt, ok := activityEvt.Data.(ChainSwitchEvent)
	require.True(t, ok, "expected ChainSwitchEvent")

	assert.Equal(t, incumbentConn, switchEvt.PreviousConnectionId)
	assert.Equal(t, revivedConn, switchEvt.NewConnectionId)
	assert.Equal(t, revivedTip, switchEvt.NewTip)
	assert.Equal(t, incumbentTip, switchEvt.PreviousTip)
	assert.Equal(t, ChainABetter, switchEvt.ComparisonResult)
	assert.Equal(t, int64(10), switchEvt.BlockDifference)
}

func TestChainSelectorVRFTiebreaker(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	// Two peers with competing tips at the same block number and slot.
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 50,
	}
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	// VRF outputs: lower wins (per Ouroboros Praos)
	// Must be exactly VRFOutputSize (64 bytes) to be valid
	vrfLower := make64ByteVRF(0x00)
	vrfHigher := make64ByteVRF(0x01)

	// Add peer with higher VRF first
	updatePeerTipWithPraosView(
		t,
		cs,
		connId1,
		tip1,
		vrfHigher,
		PraosTiebreakerConfigConway(),
	)
	// Add peer with lower VRF second
	updatePeerTipWithPraosView(
		t,
		cs,
		connId2,
		tip2,
		vrfLower,
		PraosTiebreakerConfigConway(),
	)

	// The peer with lower VRF should win
	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId2, *bestPeer, "peer with lower VRF should win")
}

func TestChainSelectorUpdatePeerTipPreservesEqualTipIncumbentOnVRF(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("same")},
		BlockNumber: 50,
	}
	vrfLower := make64ByteVRF(0x00)
	vrfHigher := make64ByteVRF(0x01)

	updatePeerTipWithPraosView(
		t,
		cs,
		connId1,
		tip,
		vrfHigher,
		PraosTiebreakerConfigConway(),
	)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	updatePeerTipWithPraosView(
		t,
		cs,
		connId2,
		tip,
		vrfLower,
		PraosTiebreakerConfigConway(),
	)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(
		t,
		connId1,
		*cs.GetBestPeer(),
		"equal-tip challenger should not steal the incumbent on VRF alone",
	)
}

func TestChainSelectorVRFTiebreakerWithNilVRF(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	// Two peers with identical tips
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("same")},
		BlockNumber: 50,
	}

	// One peer has VRF, one doesn't
	vrf := make64ByteVRF(0x01)

	cs.UpdatePeerTip(connId1, tip, vrf)
	cs.UpdatePeerTip(connId2, tip, nil)

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(
		t,
		connId1,
		*bestPeer,
		"nil VRF must not let the challenger replace the incumbent",
	)
}

func TestChainSelectorVRFDoesNotOverrideBlockNumber(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	// Peer 1: lower block number but lower VRF
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip1")},
		BlockNumber: 40,
	}
	// Peer 2: higher block number but higher VRF
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	vrfLower := make64ByteVRF(0x00)
	vrfHigher := make64ByteVRF(0xFF)

	cs.UpdatePeerTip(connId1, tip1, vrfLower)
	cs.UpdatePeerTip(connId2, tip2, vrfHigher)

	// Block number takes precedence - peer 2 should win despite higher VRF
	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(
		t,
		connId2,
		*bestPeer,
		"higher block number should win over lower VRF",
	)
}

func TestChainSelectorPraosVRFOverridesSlot(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	// Both have same block number
	// Peer 1: higher slot but lower VRF
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 105, Hash: []byte("tip1")},
		BlockNumber: 50,
	}
	// Peer 2: lower slot but higher VRF
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	vrfLower := make64ByteVRF(0x00)
	vrfHigher := make64ByteVRF(0xFF)

	updatePeerTipWithPraosView(
		t,
		cs,
		connId1,
		tip1,
		vrfLower,
		PraosTiebreakerConfigConway(),
	)
	updatePeerTipWithPraosView(
		t,
		cs,
		connId2,
		tip2,
		vrfHigher,
		PraosTiebreakerConfigConway(),
	)

	// Praos VRF takes precedence at equal block number.
	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId1, *bestPeer, "lower VRF should win over lower slot")
}

func TestChainSelectorUpdatesBestPeerWhenLaterSlotWinsVRF(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	incumbentTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 111962097, Hash: []byte("dingo")},
		BlockNumber: 4275844,
	}
	challengerTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 111962102, Hash: []byte("ref-impl")},
		BlockNumber: 4275844,
	}

	updatePeerTipWithPraosView(
		t,
		cs,
		connId1,
		incumbentTip,
		make64ByteVRFFirstByte(0xBD),
		PraosTiebreakerConfigConway(),
	)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer())

	updatePeerTipWithPraosView(
		t,
		cs,
		connId2,
		challengerTip,
		make64ByteVRFFirstByte(0x6E),
		PraosTiebreakerConfigConway(),
	)

	bestPeer := cs.GetBestPeer()
	require.NotNil(t, bestPeer)
	assert.Equal(
		t,
		connId2,
		*bestPeer,
		"equal-height later-slot challenger must replace the incumbent when VRF wins",
	)
}

func TestPeerChainTipVRFOutputStored(t *testing.T) {
	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}
	vrf := []byte{0x01, 0x02, 0x03, 0x04}

	peerTip := NewPeerChainTip(connId, tip, vrf)
	assert.Equal(t, vrf, peerTip.VRFOutput)

	// Update with new VRF
	newVRF := []byte{0x05, 0x06, 0x07, 0x08}
	peerTip.UpdateTip(tip, newVRF)
	assert.Equal(t, newVRF, peerTip.VRFOutput)
}

func TestUpdatePeerTipRejectsImplausibleTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160, // Cardano mainnet k
	})

	// Set local tip so the plausibility check is active
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	// Add a plausible peer first so the implausible check activates
	// (the check requires len(peerTips) > 0 to avoid blocking
	// initial sync where all peers are legitimately far ahead).
	existingConn := newTestConnectionId(2)
	plausibleTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100500, Hash: []byte("ok")},
		BlockNumber: 50500,
	}
	cs.UpdatePeerTip(existingConn, plausibleTip, nil)

	connId := newTestConnectionId(1)

	// A spoofed tip claiming to be far beyond local tip + k
	spoofedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: math.MaxUint64, Hash: []byte("spoof")},
		BlockNumber: math.MaxUint64,
	}

	accepted := cs.UpdatePeerTip(connId, spoofedTip, nil)
	assert.False(
		t,
		accepted,
		"implausibly high tip should be rejected",
	)
	assert.Nil(
		t,
		cs.GetPeerTip(connId),
		"rejected tip should not be tracked",
	)
	assert.Equal(
		t,
		1,
		cs.PeerCount(),
		"peer count should remain 1 (existing peer only)",
	)
}

func TestUpdatePeerTipAcceptsPlausibleTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	// Set local tip
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	connId := newTestConnectionId(1)

	// A tip that is ahead but within k blocks (plausible)
	plausibleTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 102000,
			Hash: []byte("plausible"),
		},
		BlockNumber: 51000, // 1000 ahead, well within k=2160
	}

	accepted := cs.UpdatePeerTip(connId, plausibleTip, nil)
	assert.True(t, accepted, "plausible tip should be accepted")
	assert.NotNil(
		t,
		cs.GetPeerTip(connId),
		"accepted tip should be tracked",
	)
	assert.Equal(t, 1, cs.PeerCount())
}

func TestUpdatePeerTipAcceptsTipAtExactBoundary(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	connId := newTestConnectionId(1)

	// Tip exactly at localTip.BlockNumber + securityParam (boundary)
	boundaryTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 104000,
			Hash: []byte("boundary"),
		},
		BlockNumber: 50000 + 2160, // Exactly at the limit
	}

	accepted := cs.UpdatePeerTip(connId, boundaryTip, nil)
	assert.True(
		t,
		accepted,
		"tip at exact boundary should be accepted",
	)
	assert.NotNil(t, cs.GetPeerTip(connId))
}

func TestUpdatePeerTipAcceptsTipAtExactBoundaryWithStaleReferencePeer(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:     2160,
		StaleTipThreshold: 50 * time.Millisecond,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	existingConn := newTestConnectionId(2)
	cs.UpdatePeerTip(existingConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100500, Hash: []byte("ok")},
		BlockNumber: 50500,
	}, nil)
	markSelectorPeerStale(t, cs, existingConn)

	peerTip := cs.GetPeerTip(existingConn)
	require.NotNil(t, peerTip)
	assert.True(t, peerTip.IsStale(50*time.Millisecond))

	connId := newTestConnectionId(1)
	boundaryTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 106000,
			Hash: []byte("boundary-stale"),
		},
		BlockNumber: 50500 + 2160, // Exactly at the reference peer limit
	}

	accepted := cs.UpdatePeerTip(connId, boundaryTip, nil)
	assert.True(
		t,
		accepted,
		"tip at exact boundary should be accepted when the reference peer is stale",
	)
	assert.NotNil(t, cs.GetPeerTip(connId))
}

func TestUpdatePeerTipRejectsTipOneOverBoundary(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	// Add a plausible peer first so the implausible check has a
	// reference point. The reference is the best known peer tip
	// (block 50500), not the local tip.
	existingConn := newTestConnectionId(2)
	cs.UpdatePeerTip(existingConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100500, Hash: []byte("ok")},
		BlockNumber: 50500,
	}, nil)

	connId := newTestConnectionId(1)

	// Tip one block past the boundary (reference peer block + k + 1)
	overBoundaryTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 106000,
			Hash: []byte("over"),
		},
		BlockNumber: 50500 + 2160 + 1,
	}

	accepted := cs.UpdatePeerTip(connId, overBoundaryTip, nil)
	assert.False(
		t,
		accepted,
		"tip one past boundary should be rejected",
	)
	assert.Nil(t, cs.GetPeerTip(connId))
}

func TestUpdatePeerTipAcceptsTipWhenSecurityParamZero(t *testing.T) {
	// During initial sync, securityParam may be 0 (not yet set).
	// All tips should be accepted in this case.
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 0,
	})

	// Even with a local tip set, if securityParam is 0,
	// plausibility check is skipped
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	connId := newTestConnectionId(1)

	highTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: math.MaxUint64, Hash: []byte("high")},
		BlockNumber: math.MaxUint64,
	}

	accepted := cs.UpdatePeerTip(connId, highTip, nil)
	assert.True(
		t,
		accepted,
		"all tips should be accepted when securityParam is 0",
	)
	assert.NotNil(t, cs.GetPeerTip(connId))
}

func TestUpdatePeerTipAcceptsTipWhenLocalTipZero(t *testing.T) {
	// During initial sync, local tip is at block 0.
	// All tips should be accepted in this case.
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	// Don't set local tip (remains zero value)

	connId := newTestConnectionId(1)

	highTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: math.MaxUint64, Hash: []byte("high")},
		BlockNumber: math.MaxUint64,
	}

	accepted := cs.UpdatePeerTip(connId, highTip, nil)
	assert.True(
		t,
		accepted,
		"all tips should be accepted when local tip is at block 0",
	)
	assert.NotNil(t, cs.GetPeerTip(connId))
}

func TestUpdatePeerTipAcceptsDuringInitialSyncNoPeers(t *testing.T) {
	// During genesis sync, local tip advances past 0 but the node
	// has no accepted peers yet. The implausible check must be
	// skipped so the first real peer can be accepted even though
	// its tip is millions of blocks ahead of our local chain.
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 432, // preview k
	})

	// Local tip at block 100 (just started syncing from genesis)
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 2000, Hash: []byte("local")},
		BlockNumber: 100,
	})

	connId := newTestConnectionId(1)

	// Real peer claiming tip at block 4M (far beyond 100 + 432)
	realTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 106000000, Hash: []byte("real")},
		BlockNumber: 4000000,
	}

	accepted := cs.UpdatePeerTip(connId, realTip, nil)
	assert.True(
		t,
		accepted,
		"first peer should be accepted during initial sync even if far ahead",
	)
	assert.NotNil(t, cs.GetPeerTip(connId))
}

func TestUpdatePeerTipAcceptsDuringCatchUp(t *testing.T) {
	// After a stall the node's recorded peer tips go stale. When the
	// network advances >K blocks, a known peer's tip update exceeds
	// the normal incremental check (Case 1). The catch-up relaxation
	// should accept the tip because it is within 2*K of the local tip.
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 432, // preview k
	})

	// Local tip: node was stalled at block 4153528
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000000, Hash: []byte("local")},
		BlockNumber: 4153528,
	})

	// First peer accepted (Case 3: bootstrap, no existing peers)
	conn1 := newTestConnectionId(1)
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 106000000, Hash: []byte("peer1")},
		BlockNumber: 4153528, // same as local — accepted trivially
	}
	accepted := cs.UpdatePeerTip(conn1, tip1, nil)
	assert.True(t, accepted, "first peer should be accepted (bootstrap)")

	// Peer advances by 568 blocks (>K=432) — Case 1 rejects this
	// without the catch-up fix. With fix: 4154096 <= 4153528 + 2*432
	// = 4154392, so it's accepted.
	tip1b := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 106001000, Hash: []byte("peer1b")},
		BlockNumber: 4154096, // 568 blocks ahead of stale prev tip
	}
	accepted = cs.UpdatePeerTip(conn1, tip1b, nil)
	assert.True(
		t,
		accepted,
		"known peer within 2*K of local tip should be accepted during catch-up",
	)

	// New peer claiming similar height — Case 2, within best+K
	conn2 := newTestConnectionId(2)
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 106001100, Hash: []byte("peer2")},
		BlockNumber: 4154100,
	}
	accepted = cs.UpdatePeerTip(conn2, tip2, nil)
	assert.True(t, accepted, "new peer near best peer should be accepted")

	// Truly implausible peer: beyond both best+K AND local+2*K
	conn3 := newTestConnectionId(3)
	tip3 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200000000, Hash: []byte("spoof")},
		BlockNumber: 4153528 + 2*432 + 4154096 + 432 + 1, // absurdly far
	}
	accepted = cs.UpdatePeerTip(conn3, tip3, nil)
	assert.False(
		t,
		accepted,
		"peer far beyond both thresholds should still be rejected",
	)
}

// TestUpdatePeerTipFarBehindHonestPeerPermanentlyRejectedAlone pins the
// lone-claim bound: a frontier beyond the localTip+2*K catch-up ceiling that no
// other connection corroborates stays rejected on every retry, because a
// rejected frontier is never recorded as a reference. The gap (>4M blocks at
// K=432) matches a live Preview report.
func TestUpdatePeerTipFarBehindHonestPeerPermanentlyRejectedAlone(
	t *testing.T,
) {
	t.Parallel()

	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 432, // Preview k
	})

	// Local tip: from-genesis sync, far behind the real network tip.
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000000, Hash: []byte("local")},
		BlockNumber: 175516,
	})

	// Another already-tracked peer whose own recorded frontier is also
	// near the local tip (itself stalled or freshly (re)connected) -- this
	// is what makes the reference stale and puts every later peer through
	// the catch-up branch rather than the ordinary Case 2 check.
	staleConn := newTestConnectionId(1)
	cs.UpdatePeerTip(staleConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000100, Hash: []byte("stale")},
		BlockNumber: 175000,
	}, nil)

	// The honest peer: its real, current tip is genuinely millions of
	// blocks ahead, not the result of any reorg or spoof.
	honestConn := newTestConnectionId(2)
	honestTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200000000, Hash: []byte("honest")},
		BlockNumber: 4687076,
	}

	accepted := cs.UpdatePeerTip(honestConn, honestTip, nil)
	assert.False(
		t,
		accepted,
		"a single far-ahead honest peer must not yet be trusted without corroboration",
	)

	// The peer keeps reporting the same real tip (chainsync has no way to
	// report anything else) and is rejected every single time: it never
	// became "known", so every update re-runs the same stale Case 2
	// comparison against the same stale reference. This is the ratchet.
	for range 5 {
		accepted = cs.UpdatePeerTip(honestConn, honestTip, nil)
		assert.False(
			t,
			accepted,
			"an uncorroborated far tip must stay rejected on every retry",
		)
	}
	assert.Nil(
		t,
		cs.GetPeerTip(honestConn),
		"a permanently-rejected peer must never be tracked",
	)
}

// TestUpdatePeerTipFarBehindCorroboratedAcrossPeersBreaksRatchet is the
// positive side of the same scenario: a second, independent connection
// reporting close to the same far tip corroborates the first, and both are
// then accepted. Without corroboration widening the catch-up ceiling, a
// second honest peer gains nothing -- it is rejected by the exact same
// stale reference as the first, and the node can never recover regardless
// of how many honest peers connect.
func TestUpdatePeerTipFarBehindCorroboratedAcrossPeersBreaksRatchet(
	t *testing.T,
) {
	t.Parallel()

	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 432, // Preview k
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000000, Hash: []byte("local")},
		BlockNumber: 175516,
	})

	staleConn := newTestConnectionId(1)
	cs.UpdatePeerTip(staleConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000100, Hash: []byte("stale")},
		BlockNumber: 175000,
	}, nil)

	honestConn1 := newTestConnectionId(2)
	honestTip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200000000, Hash: []byte("honest1")},
		BlockNumber: 4687076,
	}
	accepted := cs.UpdatePeerTip(honestConn1, honestTip1, nil)
	assert.False(t, accepted, "first honest peer is rejected alone")

	// A second, independent connection reports a tip within K of the
	// first's claim -- independent agreement the selector cannot fake by
	// itself.
	honestConn2 := newTestConnectionId(3)
	honestTip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200000100, Hash: []byte("honest2")},
		BlockNumber: 4687076 + 100,
	}
	accepted = cs.UpdatePeerTip(honestConn2, honestTip2, nil)
	assert.True(
		t,
		accepted,
		"a far tip corroborated by an independent connection must be accepted",
	)
	require.NotNil(t, cs.GetPeerTip(honestConn2))

	// The first peer's next update is no longer measured against the
	// stale reference: honestConn2 is now tracked with a live frontier
	// near the real tip, so the ordinary Case 2 check (not the catch-up
	// branch) accepts it directly. The ratchet is broken.
	accepted = cs.UpdatePeerTip(honestConn1, honestTip1, nil)
	assert.True(
		t,
		accepted,
		"the first peer must be accepted once corroborated",
	)
	require.NotNil(t, cs.GetPeerTip(honestConn1))
}

// TestUpdatePeerTipFarDeliveredFrontierCorroboration drives the corroboration
// path the way chainsync does: the advertised tip is the real network tip and
// the delivered header is just past the localTip+2*K catch-up ceiling. Two
// connections delivering within K of each other corroborate; two delivering
// more than K apart do not.
func TestUpdatePeerTipFarDeliveredFrontierCorroboration(t *testing.T) {
	t.Parallel()

	const (
		securityParam = 432
		localBlock    = 175516
		networkBlock  = 4687076
	)
	blockTip := func(block uint64) ochainsync.Tip {
		return ochainsync.Tip{
			Point: ocommon.Point{
				Slot: block * 20,
				Hash: fmt.Appendf(nil, "block-%d", block),
			},
			BlockNumber: block,
		}
	}
	newSelector := func() *ChainSelector {
		cs := NewChainSelector(ChainSelectorConfig{
			SecurityParam: securityParam,
		})
		cs.SetLocalTip(blockTip(localBlock))
		// A tracked peer whose frontier is behind the local tip makes every
		// reference stale, so later peers take the catch-up branch.
		require.True(t, cs.UpdatePeerTip(
			newTestConnectionId(1),
			blockTip(localBlock-500),
			nil,
		))
		return cs
	}
	advertised := blockTip(networkBlock)
	pastCeiling := uint64(localBlock + 2*securityParam + 36)

	t.Run("within K corroborates", func(t *testing.T) {
		t.Parallel()
		cs := newSelector()
		first := newTestConnectionId(2)
		second := newTestConnectionId(3)
		assert.False(t, cs.updatePeerTipObserved(
			first, advertised, blockTip(pastCeiling), nil,
		), "an uncorroborated far delivered frontier must be rejected")
		assert.True(t, cs.updatePeerTipObserved(
			second, advertised, blockTip(pastCeiling+50), nil,
		), "a delivered frontier within K of another connection's must be accepted")
		assert.True(t, cs.updatePeerTipObserved(
			first, advertised, blockTip(pastCeiling+1), nil,
		), "the first claimant must be accepted on its next header")
	})

	t.Run(
		"corroborated exactly K below keeps the claimant's next header",
		func(t *testing.T) {
			t.Parallel()
			cs := newSelector()
			first := newTestConnectionId(2)
			second := newTestConnectionId(3)
			claim := pastCeiling + securityParam
			assert.False(t, cs.updatePeerTipObserved(
				first, advertised, blockTip(claim), nil,
			))
			assert.True(t, cs.updatePeerTipObserved(
				second, advertised, blockTip(claim-securityParam), nil,
			), "a delivered frontier exactly K below another connection's must corroborate it")
			assert.True(t, cs.updatePeerTipObserved(
				first, advertised, blockTip(claim+1), nil,
			), "the corroborated claimant must be accepted on its next header even though it is more than K above the accepted frontier")
			require.NotNil(t, cs.GetPeerTip(first))
			third := newTestConnectionId(4)
			assert.False(t, cs.updatePeerTipObserved(
				third,
				advertised,
				blockTip(claim+securityParam+2),
				nil,
			), "the claimant's allowance must not widen the bound for another connection")
		},
	)

	t.Run("more than K apart does not corroborate", func(t *testing.T) {
		t.Parallel()
		cs := newSelector()
		assert.False(t, cs.updatePeerTipObserved(
			newTestConnectionId(2), advertised, blockTip(pastCeiling), nil,
		))
		assert.False(t, cs.updatePeerTipObserved(
			newTestConnectionId(3),
			advertised,
			blockTip(pastCeiling+securityParam+1),
			nil,
		), "frontiers more than K apart must not corroborate each other")
	})

	t.Run("a closed claimant no longer corroborates", func(t *testing.T) {
		t.Parallel()
		cs := newSelector()
		first := newTestConnectionId(2)
		assert.False(t, cs.updatePeerTipObserved(
			first, advertised, blockTip(pastCeiling), nil,
		))
		cs.RemovePeer(first)
		assert.False(t, cs.updatePeerTipObserved(
			newTestConnectionId(3), advertised, blockTip(pastCeiling+1), nil,
		), "a removed connection's far claim must not corroborate a new one")
	})

	t.Run(
		"a corroborated claimant is bounded by its claim plus K",
		func(t *testing.T) {
			t.Parallel()
			cs := newSelector()
			first := newTestConnectionId(2)
			second := newTestConnectionId(3)
			claim := pastCeiling + securityParam
			assert.False(t, cs.updatePeerTipObserved(
				first, advertised, blockTip(claim), nil,
			))
			assert.True(t, cs.updatePeerTipObserved(
				second, advertised, blockTip(claim-securityParam), nil,
			))
			assert.False(t, cs.updatePeerTipObserved(
				first, advertised, blockTip(claim+securityParam+1), nil,
			), "a corroborated claimant must be rejected more than K past its own claim")
			assert.True(t, cs.updatePeerTipObserved(
				first, advertised, blockTip(claim+securityParam), nil,
			), "a corroborated claimant must be accepted exactly K past its own claim")
		},
	)

	t.Run("claims are bounded by the tracked-peer limit", func(t *testing.T) {
		t.Parallel()
		const maxPeers = 3
		cs := NewChainSelector(ChainSelectorConfig{
			SecurityParam:   securityParam,
			MaxTrackedPeers: maxPeers,
		})
		cs.SetLocalTip(blockTip(localBlock))
		require.True(t, cs.UpdatePeerTip(
			newTestConnectionId(1),
			blockTip(localBlock-500),
			nil,
		))
		// Claims spaced 2*K apart never corroborate each other.
		var lastClaim uint64
		for i := range maxPeers {
			lastClaim = pastCeiling + uint64(i)*2*securityParam
			assert.False(t, cs.updatePeerTipObserved(
				newTestConnectionId(2+i), advertised, blockTip(lastClaim), nil,
			))
		}
		overflowClaim := pastCeiling + 20*securityParam
		assert.False(t, cs.updatePeerTipObserved(
			newTestConnectionId(10), advertised, blockTip(overflowClaim), nil,
		))
		assert.Len(t, cs.farTipClaims, maxPeers)
		assert.False(t, cs.updatePeerTipObserved(
			newTestConnectionId(11), advertised, blockTip(overflowClaim+1), nil,
		), "a claim dropped at capacity must not corroborate a later one")
		assert.True(t, cs.updatePeerTipObserved(
			newTestConnectionId(12), advertised, blockTip(lastClaim+1), nil,
		), "a frontier within K of a recorded claim must corroborate it while the claim table is full")
		assert.Len(t, cs.farTipClaims, maxPeers)
	})

	t.Run("an accepted connection no longer holds a claim", func(t *testing.T) {
		t.Parallel()
		cs := newSelector()
		first := newTestConnectionId(2)
		second := newTestConnectionId(3)
		assert.False(t, cs.updatePeerTipObserved(
			first, advertised, blockTip(pastCeiling), nil,
		))
		assert.True(t, cs.updatePeerTipObserved(
			second, advertised, blockTip(pastCeiling+1), nil,
		))
		assert.NotContains(t, cs.farTipClaims, second)
		assert.True(t, cs.updatePeerTipObserved(
			first, advertised, blockTip(pastCeiling+2), nil,
		))
		assert.Empty(t, cs.farTipClaims)
	})
}

func TestUpdatePeerTipAcceptsNextObservedBlockWhenAdvertisedTipIsFarAhead(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 432, // preview k
	})

	localTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 118000000, Hash: []byte("local")},
		BlockNumber: 4588334,
	}
	cs.SetLocalTip(localTip)

	connId := newTestConnectionId(1)
	staleAdvertisedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110000000, Hash: []byte("stale")},
		BlockNumber: 4260191,
	}
	require.True(t, cs.updatePeerTipObserved(
		connId,
		staleAdvertisedTip,
		localTip,
		nil,
	))

	advertisedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 118010000, Hash: []byte("network")},
		BlockNumber: 4589660,
	}
	nextObservedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 118000020, Hash: []byte("next")},
		BlockNumber: localTip.BlockNumber + 1,
	}

	require.True(
		t,
		cs.updatePeerTipObserved(
			connId,
			advertisedTip,
			nextObservedTip,
			nil,
		),
		"the next delivered block must not be rejected because the network tip is far ahead",
	)
	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, advertisedTip, peerTip.Tip)
	assert.Equal(t, nextObservedTip, peerTip.ObservedTip)
}

func TestAdvertisedTipOutlierDoesNotSuppressObservedFrontier(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 10,
	})
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1000, Hash: []byte("local")},
		BlockNumber: 1000,
	})

	outlierConn := newTestConnectionId(1)
	require.True(t, cs.updatePeerTipObserved(
		outlierConn,
		ochainsync.Tip{
			Point: ocommon.Point{
				Slot: math.MaxUint64,
				Hash: []byte("advertised-outlier"),
			},
			BlockNumber: math.MaxUint64,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 1001, Hash: []byte("observed-1")},
			BlockNumber: 1001,
		},
		nil,
	))

	honestConn := newTestConnectionId(2)
	require.True(t, cs.updatePeerTipObserved(
		honestConn,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 1004, Hash: []byte("honest")},
			BlockNumber: 1004,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 1004, Hash: []byte("honest")},
			BlockNumber: 1004,
		},
		nil,
	))

	bestPeer := cs.GetBestPeer()
	require.NotNil(t, bestPeer)
	assert.Equal(
		t,
		honestConn,
		*bestPeer,
		"an advertised outlier must not make a better delivered frontier unselectable",
	)
}

func TestChainSwitchEventIncludesObservedFrontier(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()
	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:      eventBus,
		SecurityParam: 10,
	})
	_, eventCh := eventBus.Subscribe(ChainSwitchEventType)

	advertisedTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: math.MaxUint64,
			Hash: []byte("advertised-outlier"),
		},
		BlockNumber: math.MaxUint64,
	}
	observedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1001, Hash: []byte("observed")},
		BlockNumber: 1001,
	}
	require.True(t, cs.updatePeerTipObserved(
		newTestConnectionId(1),
		advertisedTip,
		observedTip,
		nil,
	))

	evt := testutil.RequireReceive(
		t,
		eventCh,
		2*time.Second,
		"chain switch event",
	)
	switchEvent, ok := evt.Data.(ChainSwitchEvent)
	require.True(t, ok)
	assert.Equal(t, advertisedTip, switchEvent.NewTip)
	assert.Equal(t, observedTip, switchEvent.NewObservedTip)
	assert.True(
		t,
		switchEvent.NewObservedTipSet,
		"producers in this package always mark the frontier as present",
	)
}

func TestUpdatePeerTipRejectsKnownPeerJumpFromZero(t *testing.T) {
	// A known peer whose previous tip was at block 0 must still be
	// checked. Without this, a malicious peer could send tip=0 first,
	// then tip=MaxUint64 to bypass the implausible check.
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	connId := newTestConnectionId(1)

	// First tip at block 0 — accepted (first peer, no reference)
	zeroTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 0, Hash: []byte("zero")},
		BlockNumber: 0,
	}
	accepted := cs.UpdatePeerTip(connId, zeroTip, nil)
	assert.True(t, accepted, "first tip at block 0 should be accepted")

	// Second tip jumps to far beyond 0 + k
	spoofedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: math.MaxUint64, Hash: []byte("spoof")},
		BlockNumber: math.MaxUint64,
	}
	accepted = cs.UpdatePeerTip(connId, spoofedTip, nil)
	assert.False(
		t,
		accepted,
		"known peer jumping from block 0 to MaxUint64 should be rejected",
	)
	// Peer tip should still be the original block 0 tip
	pt := cs.GetPeerTip(connId)
	require.NotNil(t, pt)
	assert.Equal(t, uint64(0), pt.Tip.BlockNumber)
}

func TestUpdatePeerTipSpoofedPeerDoesNotBecomesBest(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	// Add a legitimate peer first
	legitimateConn := newTestConnectionId(1)
	legitimateTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 100100,
			Hash: []byte("legit"),
		},
		BlockNumber: 50050,
	}
	cs.UpdatePeerTip(legitimateConn, legitimateTip, nil)

	// Now a malicious peer tries to spoof
	maliciousConn := newTestConnectionId(2)
	spoofedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: math.MaxUint64, Hash: []byte("evil")},
		BlockNumber: math.MaxUint64,
	}

	accepted := cs.UpdatePeerTip(maliciousConn, spoofedTip, nil)
	assert.False(t, accepted, "spoofed tip should be rejected")

	// The legitimate peer should still be the best
	bestPeer := cs.GetBestPeer()
	require.NotNil(t, bestPeer)
	assert.Equal(
		t,
		legitimateConn,
		*bestPeer,
		"legitimate peer should remain best peer",
	)
}

func TestChainSelectorMaxTrackedPeersDefault(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})
	assert.Equal(
		t,
		DefaultMaxTrackedPeers,
		cs.maxTrackedPeers,
		"default max tracked peers should be applied",
	)
}

func TestChainSelectorMaxTrackedPeersCustom(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: 50,
	})
	assert.Equal(
		t,
		50,
		cs.maxTrackedPeers,
		"custom max tracked peers should be applied",
	)
}

func TestChainSelectorPeerEvictionAtCapacity(t *testing.T) {
	const maxPeers = 5
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: maxPeers,
	})

	// Pre-create connection IDs so the same pointers are reused for
	// map lookups (ConnectionId contains net.Addr interface fields).
	connIds := make([]ouroboros.ConnectionId, maxPeers+1)
	for i := range connIds {
		connIds[i] = newTestConnectionId(i)
	}

	// Fill to capacity with peers. Each peer gets a slightly higher slot
	// to ensure different LastUpdated timestamps (they are added
	// sequentially so time.Now() progresses).
	for i := range maxPeers {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: fmt.Appendf(nil, "tip%d", i),
			},
			BlockNumber: uint64(50 + i),
		}
		cs.UpdatePeerTip(connIds[i], tip, nil)
	}
	assert.Equal(t, maxPeers, cs.PeerCount(), "should be at capacity")

	// Peer 0 was added first and has the oldest LastUpdated.
	// It may or may not be the best peer (peer maxPeers-1 has highest
	// block number and becomes best via auto-evaluation). Verify peer 0
	// exists before we trigger eviction.
	setSelectorConnectionEligible(cs, connIds[0], false)
	setSelectorConnectionPriority(cs, connIds[0], 10)
	require.NotNil(
		t,
		cs.GetPeerTip(connIds[0]),
		"peer 0 should exist before eviction",
	)

	// Add one more peer beyond the limit
	newTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: uint64(100 + maxPeers),
			Hash: []byte("new"),
		},
		BlockNumber: uint64(50 + maxPeers),
	}
	cs.UpdatePeerTip(connIds[maxPeers], newTip, nil)

	// Count should still be at the limit
	assert.Equal(
		t,
		maxPeers,
		cs.PeerCount(),
		"peer count should not exceed max",
	)

	// The new peer should be present
	require.NotNil(
		t,
		cs.GetPeerTip(connIds[maxPeers]),
		"new peer should be tracked",
	)

	// The oldest peer (peer 0) should have been evicted since it is not
	// the best peer (peer maxPeers-1 has the highest block number).
	assert.Nil(
		t,
		cs.GetPeerTip(connIds[0]),
		"oldest peer should have been evicted",
	)
	cs.mutex.RLock()
	_, eligibleFound := cs.eligible[connIds[0]]
	_, priorityFound := cs.priority[connIds[0]]
	cs.mutex.RUnlock()
	assert.False(t, eligibleFound)
	assert.False(t, priorityFound)

	// Peers 1 through maxPeers-1 should still be present
	for i := 1; i < maxPeers; i++ {
		assert.NotNil(
			t,
			cs.GetPeerTip(connIds[i]),
			"peer %d should still be tracked",
			i,
		)
	}
}

func TestChainSelectorUpdateExistingPeerDoesNotEvict(t *testing.T) {
	const maxPeers = 3
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: maxPeers,
	})

	// Pre-create connection IDs so the same pointers are reused
	connIds := make([]ouroboros.ConnectionId, maxPeers)
	for i := range connIds {
		connIds[i] = newTestConnectionId(i)
	}

	// Fill to capacity
	for i := range maxPeers {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: fmt.Appendf(nil, "tip%d", i),
			},
			BlockNumber: uint64(50 + i),
		}
		cs.UpdatePeerTip(connIds[i], tip, nil)
	}
	assert.Equal(t, maxPeers, cs.PeerCount())

	// Update an existing peer (peer 1) with a new tip
	updatedTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200, Hash: []byte("updated")},
		BlockNumber: 100,
	}
	cs.UpdatePeerTip(connIds[1], updatedTip, nil)

	// Count should remain the same -- no eviction for existing peer updates
	assert.Equal(
		t,
		maxPeers,
		cs.PeerCount(),
		"updating existing peer should not change count",
	)

	// All original peers should still be present
	for i := range maxPeers {
		assert.NotNil(
			t,
			cs.GetPeerTip(connIds[i]),
			"peer %d should still be tracked after existing peer update",
			i,
		)
	}

	// Verify the update was applied
	peerTip := cs.GetPeerTip(connIds[1])
	require.NotNil(t, peerTip)
	assert.Equal(
		t,
		updatedTip.BlockNumber,
		peerTip.Tip.BlockNumber,
		"existing peer tip should be updated",
	)
}

func TestChainSelectorEvictionPreservesBestPeer(t *testing.T) {
	const maxPeers = 3
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: maxPeers,
	})

	// Pre-create connection IDs so the same pointers are reused
	connIds := make([]ouroboros.ConnectionId, maxPeers+1)
	for i := range connIds {
		connIds[i] = newTestConnectionId(i)
	}

	// Add peer 0 first (oldest) with the BEST tip
	bestTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("best")},
		BlockNumber: 999, // Highest block number = best chain
	}
	cs.UpdatePeerTip(connIds[0], bestTip, nil)

	// Trigger evaluation so peer 0 becomes the best peer
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connIds[0], *cs.GetBestPeer())

	// Add peers 1 and 2 (at capacity now)
	for i := 1; i < maxPeers; i++ {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: fmt.Appendf(nil, "tip%d", i),
			},
			BlockNumber: uint64(50 + i),
		}
		cs.UpdatePeerTip(connIds[i], tip, nil)
	}
	assert.Equal(t, maxPeers, cs.PeerCount())

	// Add a new peer beyond the limit. Peer 0 is oldest but is the best
	// peer, so peer 1 (next oldest) should be evicted instead.
	newTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: uint64(100 + maxPeers),
			Hash: []byte("new"),
		},
		BlockNumber: uint64(50 + maxPeers),
	}
	cs.UpdatePeerTip(connIds[maxPeers], newTip, nil)

	assert.Equal(t, maxPeers, cs.PeerCount())

	// Best peer (peer 0) must NOT have been evicted
	assert.NotNil(
		t,
		cs.GetPeerTip(connIds[0]),
		"best peer must not be evicted",
	)

	// One of the non-best peers (1 or 2) should have been evicted.
	// We don't assert which one because eviction among peers with equal
	// timestamps depends on map iteration order, which is non-deterministic.
	evictedCount := 0
	for i := 1; i < maxPeers; i++ {
		if cs.GetPeerTip(connIds[i]) == nil {
			evictedCount++
		}
	}
	assert.Equal(
		t,
		1,
		evictedCount,
		"exactly one non-best peer should be evicted",
	)

	// New peer should be present
	assert.NotNil(
		t,
		cs.GetPeerTip(connIds[maxPeers]),
		"new peer should be tracked",
	)
}

func TestChainSelectorEvictionEmitsPeerEvictedEvent(t *testing.T) {
	eb := event.NewEventBus(nil, nil)
	const maxPeers = 2
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: maxPeers,
		EventBus:        eb,
	})

	evictedCh := make(chan PeerEvictedEvent, 1)
	eb.SubscribeFunc(PeerEvictedEventType, func(evt event.Event) {
		e, ok := evt.Data.(PeerEvictedEvent)
		if ok {
			evictedCh <- e
		}
	})

	connIds := make([]ouroboros.ConnectionId, maxPeers+1)
	for i := range connIds {
		connIds[i] = newTestConnectionId(i)
	}

	// Fill to capacity
	for i := range maxPeers {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: fmt.Appendf(nil, "tip%d", i),
			},
			BlockNumber: uint64(50 + i),
		}
		cs.UpdatePeerTip(connIds[i], tip, nil)
	}

	// Add one more peer to trigger eviction
	newTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200, Hash: []byte("new")},
		BlockNumber: 60,
	}
	cs.UpdatePeerTip(connIds[maxPeers], newTip, nil)

	// Should receive a PeerEvictedEvent
	select {
	case evt := <-evictedCh:
		// The evicted peer should be one of the original peers
		assert.True(
			t,
			evt.ConnectionId == connIds[0] || evt.ConnectionId == connIds[1],
			"evicted peer should be one of the original peers",
		)
	case <-time.After(time.Second):
		t.Fatal("expected PeerEvictedEvent but none received")
	}
}

func TestChainSelectorEvictionFailsWhenOnlyBestPeer(t *testing.T) {
	// When maxTrackedPeers=1 and the sole peer is best, eviction cannot
	// proceed. The new peer should be rejected rather than exceeding the cap.
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: 1,
	})

	bestConn := newTestConnectionId(0)
	bestTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("best")},
		BlockNumber: 999,
	}
	cs.UpdatePeerTip(bestConn, bestTip, nil)
	cs.EvaluateAndSwitch()

	require.Equal(t, 1, cs.PeerCount())
	require.NotNil(t, cs.GetBestPeer())

	// Try to add a second peer — should be rejected
	newConn := newTestConnectionId(1)
	newTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 101, Hash: []byte("new")},
		BlockNumber: 50,
	}
	accepted := cs.UpdatePeerTip(newConn, newTip, nil)

	assert.False(t, accepted, "new peer should be rejected when eviction fails")
	assert.Equal(t, 1, cs.PeerCount(), "peer count must not exceed max")
	assert.Nil(t, cs.GetPeerTip(newConn), "rejected peer should not be tracked")
	assert.NotNil(t, cs.GetPeerTip(bestConn), "best peer must remain")
}

func TestChainSelectorNormalOperationWithinLimit(t *testing.T) {
	const maxPeers = 10
	cs := NewChainSelector(ChainSelectorConfig{
		MaxTrackedPeers: maxPeers,
	})

	expectedCount := maxPeers - 3

	// Pre-create connection IDs so the same pointers are reused
	connIds := make([]ouroboros.ConnectionId, expectedCount)
	for i := range connIds {
		connIds[i] = newTestConnectionId(i)
	}

	// Add fewer peers than the limit
	for i := range expectedCount {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: fmt.Appendf(nil, "tip%d", i),
			},
			BlockNumber: uint64(50 + i),
		}
		cs.UpdatePeerTip(connIds[i], tip, nil)
	}

	assert.Equal(
		t,
		expectedCount,
		cs.PeerCount(),
		"all peers should be tracked when below limit",
	)

	// All peers should be present
	for i := range expectedCount {
		assert.NotNil(
			t,
			cs.GetPeerTip(connIds[i]),
			"peer %d should be tracked",
			i,
		)
	}
}

func TestSelectBestChainSkipsImplausiblyBehindPeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	behindConn := newTestConnectionId(1)
	behindTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 70000, Hash: []byte("behind")},
		BlockNumber: 47000, // 3000 behind (>k)
	}
	cs.UpdatePeerTip(behindConn, behindTip, nil)

	bestPeer := cs.SelectBestChain()
	assert.Nil(t, bestPeer, "implausibly-behind peer should not be selected")
}

func TestSelectBestChainAllowsPlausiblyBehindPeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	behindConn := newTestConnectionId(1)
	behindTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 98000, Hash: []byte("behind-ok")},
		BlockNumber: 49000, // 1000 behind (<=k)
	}
	cs.UpdatePeerTip(behindConn, behindTip, nil)

	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, behindConn, *bestPeer)
}

// TestOmittedObservedFrontierIsNotPromotedToAdvertisedTip asserts that a peer
// tip update carrying no delivered frontier is recorded as having delivered
// nothing, rather than being credited with its untrusted advertised tip. The
// accompanying Praos view describes the absent delivered header, so promoting
// the advertised tip would also leave the stored view and the stored frontier
// on different slots, which UpdateTipWithObservedPraosView forbids.
func TestOmittedObservedFrontierIsNotPromotedToAdvertisedTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{SecurityParam: 10})
	cs.SetLocalTip(tip(99, 999, "local"))

	// First peer: no reference frontier exists yet, so the plausibility bound
	// cannot reject it. It advertises a tip far ahead but delivers no headers.
	farConn := newTestConnectionId(1)
	farAdvertised := tip(1_000_000, 5_000_000, "advertised-far")
	farVRF := bytes.Repeat([]byte{0x01}, VRFOutputSize)
	require.True(t, cs.updatePeerTipObservedPraosView(
		farConn,
		farAdvertised,
		ochainsync.Tip{},
		farVRF,
		NewPraosTiebreakerViewFull(
			ochainsync.Tip{},
			[]byte("issuer-far"),
			1,
			farVRF,
			PraosTiebreakerConfigBeforeConway(),
		),
	))

	farTip := cs.GetPeerTip(farConn)
	require.NotNil(t, farTip)
	assert.Equal(
		t,
		ochainsync.Tip{},
		farTip.SelectionTip(),
		"an omitted delivered frontier must not be replaced by the advertised tip",
	)

	// A peer that actually delivered a header must outrank the peer that
	// delivered nothing, and must not be measured for plausibility against the
	// undelivered claim.
	honestConn := newTestConnectionId(2)
	honestTip := tip(100, 1000, "honest")
	honestVRF := bytes.Repeat([]byte{0xff}, VRFOutputSize)
	require.True(t, cs.updatePeerTipObservedPraosView(
		honestConn,
		honestTip,
		honestTip,
		honestVRF,
		NewPraosTiebreakerViewFull(
			honestTip,
			[]byte("issuer-honest"),
			1,
			honestVRF,
			PraosTiebreakerConfigBeforeConway(),
		),
	))

	bestPeer := cs.GetBestPeer()
	require.NotNil(t, bestPeer)
	assert.Equal(
		t,
		honestConn,
		*bestPeer,
		"a peer that delivered no headers must not hold selection on its advertised tip",
	)
}

// panicLogHandler is an slog.Handler that panics on every Handle call, used
// to inject a deterministic panic into a locked section that logs. It
// otherwise delegates to inner, including WithAttrs/WithGroup, so a wrapped
// child logger (e.g. from Logger.With) still panics on Handle.
type panicLogHandler struct {
	inner slog.Handler
}

func (h *panicLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *panicLogHandler) Handle(context.Context, slog.Record) error {
	panic("intentional logger panic")
}

func (h *panicLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &panicLogHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *panicLogHandler) WithGroup(name string) slog.Handler {
	return &panicLogHandler{inner: h.inner.WithGroup(name)}
}

// TestChainSelectorSetLocalTipUnlocksOnPanic is a regression test for a bug
// where SetLocalTip (and SetSecurityParam, HandlePeerRollbackEvent) took
// cs.mutex.Lock() and called cs.mutex.Unlock() as a bare statement after the
// locked work instead of via defer. advanceSelectionModeLocked logs on a
// Genesis-mode exit while the lock is held; if that log call panics (a
// misbehaving Logger, but the same failure mode as any other panic in code
// reachable from inside the lock), the bare Unlock() was skipped and
// cs.mutex stayed locked forever -- deadlocking every future ChainSelector
// call, not just dropping the one event. This test drives a real Genesis
// exit with a Logger that panics on every Handle call and then verifies
// cs.mutex is still acquirable afterward.
func TestChainSelectorSetLocalTipUnlocksOnPanic(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: 10,
	})

	connId := newTestConnectionId(1)
	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("peer-100")},
		BlockNumber: 100,
	}, nil)
	require.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	// Installed under cs.mutex, after the setup UpdatePeerTip call above
	// (which itself logs), matching how every production read of
	// cs.config.Logger is itself guarded by cs.mutex.
	cs.mutex.Lock()
	cs.config.Logger = slog.New(
		&panicLogHandler{inner: slog.NewTextHandler(io.Discard, nil)},
	)
	cs.mutex.Unlock()

	func() {
		defer func() {
			require.NotNil(
				t,
				recover(),
				"expected the Genesis-exit log call to panic",
			)
		}()
		// Drives the local tip to within the Genesis window of the peer's
		// advertised tip (100), triggering advanceSelectionModeLocked's
		// Genesis-exit log call while cs.mutex is held.
		cs.SetLocalTip(ochainsync.Tip{
			Point:       ocommon.Point{Slot: 75, Hash: []byte("local-75")},
			BlockNumber: 75,
		})
	}()

	// If SetLocalTip's locked section left cs.mutex locked, this blocks
	// forever instead of closing unlocked.
	unlocked := make(chan struct{})
	go func() {
		cs.mutex.Lock()
		cs.mutex.Unlock()
		close(unlocked)
	}()
	select {
	case <-unlocked:
	case <-time.After(2 * time.Second):
		t.Fatal(
			"cs.mutex is still locked after the panic; " +
				"SetLocalTip must unlock via defer",
		)
	}
}

// TestChainSelectorOnPeerRollbackPanicPublishesEvent verifies onPeerRollbackPanic,
// the SubscribeFuncStrict onPanic hook NewChainSelector registers for
// PeerRollbackEventType: it must publish PeerRollbackHandlerPanicEventType
// carrying the recovered panic value, so a component that lost its rollback
// subscription to a handler panic (event.EventBus.SubscribeFuncStrict tears
// the subscription down) has a durable, observable signal instead of a
// generic log line.
func TestChainSelectorOnPeerRollbackPanicPublishesEvent(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:                  bus,
		DisableEventSubscriptions: true,
	})

	var received atomic.Value
	bus.SubscribeFunc(
		PeerRollbackHandlerPanicEventType,
		func(evt event.Event) {
			received.Store(evt.Data)
		},
	)

	cs.onPeerRollbackPanic(
		event.NewEvent(PeerRollbackEventType, PeerRollbackEvent{
			ConnectionId: newTestConnectionId(1),
		}),
		"intentional rollback handler panic",
	)

	require.Eventually(
		t,
		func() bool {
			return received.Load() != nil
		},
		2*time.Second,
		10*time.Millisecond,
		"a rollback handler panic must publish PeerRollbackHandlerPanicEventType",
	)
	got, ok := received.Load().(PeerRollbackHandlerPanicEvent)
	require.True(t, ok)
	assert.Equal(t, "intentional rollback handler panic", got.Panic)
}

// TestChainSelectorEvaluationPanicSurfacedAndLoopContinues verifies that a
// panic during a triggered evaluation is surfaced via
// EvaluationPanicEventType instead of silently dropping the failed
// transition, and that the evaluation loop remains usable for the next
// evaluation afterward -- runTriggeredEvaluation is the same panic-recovery
// wrapper the background evaluationLoop's triggered path uses, called
// directly here to keep the test deterministic instead of racing a
// ticker/channel.
func TestChainSelectorEvaluationPanicSurfacedAndLoopContinues(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	var received atomic.Value
	bus.SubscribeFunc(EvaluationPanicEventType, func(evt event.Event) {
		received.Store(evt.Data)
	})

	cs := NewChainSelector(ChainSelectorConfig{EventBus: bus})

	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10, Hash: []byte("slot-10")},
		BlockNumber: 10,
	}
	connA := newTestConnectionId(1)
	connB := newTestConnectionId(2)
	cs.UpdatePeerTip(connA, tip, nil)
	cs.UpdatePeerTip(connB, tip, nil)

	// Installed under cs.mutex, matching comparePeerTipsPraos's locked read
	// of cs.config.BlockfetchLatency. Reached only once both peers compare
	// as the same chain (equal tip and priority), which the two identical
	// UpdatePeerTip calls above set up.
	cs.mutex.Lock()
	cs.config.BlockfetchLatency = func(ouroboros.ConnectionId) (time.Duration, bool) {
		panic("blockfetch latency boom")
	}
	cs.mutex.Unlock()

	cs.runTriggeredEvaluation()

	require.Eventually(t, func() bool {
		return received.Load() != nil
	}, 2*time.Second, 10*time.Millisecond,
		"a panic during evaluation must publish EvaluationPanicEventType",
	)
	got, ok := received.Load().(EvaluationPanicEvent)
	require.True(t, ok)
	assert.Equal(t, "blockfetch latency boom", got.Panic)
	assert.True(t, got.Triggered)

	// The evaluation loop keeps running after a panic: clearing the
	// panicking latency func and evaluating again must succeed normally
	// rather than the earlier panic having wedged the selector.
	cs.mutex.Lock()
	cs.config.BlockfetchLatency = nil
	cs.mutex.Unlock()
	require.NotPanics(t, func() {
		cs.runTriggeredEvaluation()
	})
	require.NotNil(t, cs.GetBestPeer())
}

// TestChainSelectorRecoverEvaluationPanicToleratesPanickingLogger is a
// regression test for a bug where recoverEvaluationPanic's own logging call
// was not panic-safe: it runs after this function's own recover() has
// already consumed the evaluation panic, so nothing further up the stack
// could catch a second one. A misbehaving Logger panicking there would
// propagate out of the deferred call as a fresh, unrecovered panic --
// runTriggeredEvaluation/runEvaluationTick would never return, crashing the
// whole process once it unwound past evaluationLoop's for/select with
// nothing left to catch it, over what should have been one dropped
// transition. This drives a real evaluation panic with a Logger that panics
// on every Handle call and verifies the panic is fully contained.
func TestChainSelectorRecoverEvaluationPanicToleratesPanickingLogger(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	cs := NewChainSelector(ChainSelectorConfig{EventBus: bus})
	cs.mutex.Lock()
	cs.config.Logger = slog.New(
		&panicLogHandler{inner: slog.NewTextHandler(io.Discard, nil)},
	)
	cs.mutex.Unlock()

	require.NotPanics(t, func() {
		func() {
			defer cs.recoverEvaluationPanic(true)
			panic("evaluation boom")
		}()
	}, "a panicking Logger must not escape recoverEvaluationPanic")
}

// tipAt builds a chainsync tip with a distinct hash per block number.
func behindPeerTipAt(blockNumber uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: blockNumber * 2,
			Hash: []byte("behind-peer-" + string(rune('a'+blockNumber%26))),
		},
		BlockNumber: blockNumber,
	}
}

// A block producer with a single configured upstream must keep that upstream
// usable while it is behind on our own chain. The intersect ladder resolves a
// peer this far back to a rung past K, which the chainsync layer used to treat
// as an over-K fork and evict; chain selection itself has no such notion, and
// this pins the invariant the eviction was breaking: one behind-but-inside-K
// peer stays registered and selectable, so the producer never reaches the
// "chain selection stalled: no selectable peer" state, and it is selected
// immediately once it overtakes us.
func TestValencyOneUpstreamBehindStaysSelectable(t *testing.T) {
	const securityParam = 108
	const localBlock = 115195
	// 65 blocks behind: the point at which the intersect ladder's first rung
	// past K (128 at K=108) starts manufacturing an over-K rollback, while
	// the peer is still well inside K.
	const behindBlock = localBlock - 65

	cs := NewChainSelector(ChainSelectorConfig{})
	cs.SetSecurityParam(securityParam)
	cs.SetLocalTip(behindPeerTipAt(localBlock))

	connId := newTestConnectionId(1)
	require.True(t, cs.UpdatePeerTip(connId, behindPeerTipAt(behindBlock), nil))

	require.Equal(t, 1, cs.PeerCount(), "the only upstream must be retained")
	best := cs.SelectBestChain()
	require.NotNil(
		t,
		best,
		"a sole upstream inside K must remain selectable, otherwise the "+
			"producer stalls with no selectable peer",
	)
	assert.Equal(t, connId, *best)

	// The peer catches up and overtakes us; it must be selected.
	require.True(
		t,
		cs.UpdatePeerTip(connId, behindPeerTipAt(localBlock+5), nil),
	)
	cs.SetLocalTip(behindPeerTipAt(localBlock))
	best = cs.SelectBestChain()
	require.NotNil(t, best)
	assert.Equal(t, connId, *best)
}

// A sole upstream further behind than K is not worth syncing from, so chain
// selection declines to select it — but it must still be retained, because it
// is the only peer we have and it becomes selectable again the moment it
// catches back up to within K. Evicting it instead (the over-K chainsync
// denial) leaves the node with no upstream at all.
func TestValencyOneUpstreamBeyondKIsRetainedNotEvicted(t *testing.T) {
	const securityParam = 108
	const localBlock = 115195
	// The lag observed in the field: 119 blocks, past K.
	const behindBlock = localBlock - 119

	cs := NewChainSelector(ChainSelectorConfig{})
	cs.SetSecurityParam(securityParam)
	cs.SetLocalTip(behindPeerTipAt(localBlock))

	connId := newTestConnectionId(1)
	require.True(t, cs.UpdatePeerTip(connId, behindPeerTipAt(behindBlock), nil))

	assert.Equal(t, 1, cs.PeerCount(), "the only upstream must be retained")
	assert.Nil(
		t,
		cs.SelectBestChain(),
		"a peer more than K behind is not a useful sync source",
	)

	// It catches up to within K and becomes usable again without any
	// reconnect, denial cooldown, or operator intervention.
	require.True(
		t,
		cs.UpdatePeerTip(connId, behindPeerTipAt(localBlock-10), nil),
	)
	best := cs.SelectBestChain()
	require.NotNil(t, best)
	assert.Equal(t, connId, *best)
}

// TestChainSelectorByronEBBBeatsRegularIncumbentAtEqualBlockNumber exercises
// normal multi-peer selection: an incumbent peer
// on a Byron regular tip, and a second peer that delivers the EBB successor
// sharing the same protocol block number, the way Byron routes an EBB and
// its predecessor. Without the era-aware tiebreak this is exactly
// TestIncumbentAdvantageNoSwitchAtEqualBlockNumber's shape (Praos alone
// calls it ChainEqual and keeps the incumbent); the Byron EBB tiebreak must
// override that and switch to the peer with the boundary block.
func TestChainSelectorByronEBBBeatsRegularIncumbentAtEqualBlockNumber(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	const blockNumber = 50

	mainHeader := &byron.ByronMainBlockHeader{}
	mainHeader.ConsensusData.Difficulty.Value = blockNumber
	mainView, ok := GetPraosTiebreakerView(mainHeader)
	require.False(t, ok, "Byron header has no Praos select view")

	accepted := cs.updatePeerTipObservedPraosView(
		connId1,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("regular-tip")},
			BlockNumber: blockNumber,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("regular-tip")},
			BlockNumber: blockNumber,
		},
		nil,
		mainView,
	)
	require.True(t, accepted)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer(),
		"the first (and only) peer must be selected")

	ebbHeader := &byron.ByronEpochBoundaryBlockHeader{}
	ebbHeader.ConsensusData.Difficulty.Value = blockNumber
	ebbView, ok := GetPraosTiebreakerView(ebbHeader)
	require.False(t, ok, "Byron header has no Praos select view")

	accepted = cs.updatePeerTipObservedPraosView(
		connId2,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 101, Hash: []byte("ebb-tip")},
			BlockNumber: blockNumber,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 101, Hash: []byte("ebb-tip")},
			BlockNumber: blockNumber,
		},
		nil,
		ebbView,
	)
	require.True(t, accepted)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer(),
		"the peer reporting the Byron EBB successor must win over the "+
			"regular-tip incumbent at the same block number")
}

const (
	// previewSecurityParam is Preview's k, the value in the reproduction.
	previewSecurityParam = 432

	// canonicalAdvertisedBlock and canonicalAdvertisedSlot are the tip both
	// public roots advertised in the reproduction.
	canonicalAdvertisedBlock = 4625199
	canonicalAdvertisedSlot  = 121697834
)

// canonicalAdvertisedTip is the identical tip advertised by both canonical
// peers.
func canonicalAdvertisedTip() ochainsync.Tip {
	return tip(
		canonicalAdvertisedBlock,
		canonicalAdvertisedSlot,
		"canonical-preview-tip",
	)
}

// deliveredFrontier builds a delivered-header frontier. Both peers are on the
// same chain, so the same block number always carries the same slot and hash.
func deliveredFrontier(block uint64) ochainsync.Tip {
	return tip(block, 600000+block, "hdr-"+itoa(block))
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// chainSwitchBarrierTimeout bounds the wait for the barrier below. It is a
// deadlock bound, not a settling delay: the barrier is already queued behind
// whatever the selector decided by the time the wait starts, so the normal
// cost is one lane hand-off.
const chainSwitchBarrierTimeout = 30 * time.Second

// chainSwitchBarrier is a sentinel published through the chain-switch ordered
// lane so a test can tell "no switch was decided" from "the switch has not
// been delivered yet".
//
// ChainSelector.publishSelection routes chain switches through
// EventBus.PublishOrdered, so the call that drove
// the decision returns before the lane worker has handed the event to any
// subscriber. A lane is a FIFO drained by exactly one worker, so a sentinel
// enqueued after those switches is delivered after them: receiving it back is
// proof that every switch published earlier on this goroutine has already
// reached the subscription. Its Data type is not ChainSwitchEvent, so it is
// skipped rather than counted as a decision. Same construction as
// switchBarrier in ouroboros/tests_test.go.
type chainSwitchBarrier struct{}

// collectChainSwitchEvents returns every ChainSwitchEvent the selector has
// published so far, bounded by the barrier above. A non-blocking drain is not
// a bound: it reports a switch that has been published but not yet delivered
// as no switch at all, which turns this file's assertions into a race against
// the lane worker.
func collectChainSwitchEvents(
	t *testing.T,
	bus *event.EventBus,
	ch <-chan event.Event,
) []ChainSwitchEvent {
	t.Helper()
	require.True(
		t,
		bus.PublishOrdered(
			ChainSwitchEventType,
			event.NewEvent(ChainSwitchEventType, chainSwitchBarrier{}),
		),
		"event bus refused the chain-switch barrier",
	)
	var out []ChainSwitchEvent
	for {
		evt := testutil.RequireReceive(
			t,
			ch,
			chainSwitchBarrierTimeout,
			"chain-switch barrier",
		)
		switch data := evt.Data.(type) {
		case chainSwitchBarrier:
			return out
		case ChainSwitchEvent:
			out = append(out, data)
		default:
			// Only the selector and the barrier above publish on this
			// lane, so anything else is a bug in one of them. Skipping it
			// would still terminate -- the barrier is behind it in the
			// same FIFO -- but it would drop a switch decision the
			// assertions below then read as "never switched".
			t.Fatalf("unexpected %T on the chain_switch lane", evt.Data)
		}
	}
}

// peerSelectable exposes the selectability gate for assertions.
func peerSelectable(
	t *testing.T,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
) bool {
	t.Helper()
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	peerTip, ok := cs.peerTips[connId]
	require.True(t, ok, "peer must be tracked")
	return cs.isPeerSelectableLocked(connId, peerTip)
}

// TestSameChainFrontierLeadKeepsLaggingIncumbentSelectable pins the
// eligibility half of the interaction. The observed-frontier k-behind filter
// in isPeerSelectableLocked measures the DELIVERED frontier against the best
// delivered frontier. Two canonical peers advertising the identical tip
// differed by 742 delivered blocks (> Preview k=432), which marked the
// incumbent ineligible and skipped the anti-flap pin entirely.
func TestSameChainFrontierLeadKeepsLaggingIncumbentSelectable(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(deliveredFrontier(27900))

	advertised := canonicalAdvertisedTip()
	require.True(t, cs.updatePeerTipObserved(
		incumbent,
		advertised,
		deliveredFrontier(28029),
		nil,
	))
	require.True(t, cs.updatePeerTipObserved(
		challenger,
		advertised,
		deliveredFrontier(28029),
		nil,
	))

	// Walk the challenger's delivered frontier forward in steps within k so
	// the implausible-frontier bound accepts each one, until it leads by the
	// 742 blocks observed in the reproduction.
	for _, block := range []uint64{28461, 28771} {
		require.True(t, cs.updatePeerTipObserved(
			challenger,
			advertised,
			deliveredFrontier(block),
			nil,
		))
	}

	require.Equal(
		t,
		uint64(742),
		cs.GetPeerTip(challenger).SelectionTip().BlockNumber-
			cs.GetPeerTip(incumbent).SelectionTip().BlockNumber,
		"scenario must reproduce the observed 742-block frontier gap",
	)

	assert.True(
		t,
		peerSelectable(t, cs, incumbent),
		"a peer advertising the same canonical tip must stay selectable when it trails only in delivered headers",
	)
}

// TestCanonicalPeersWithCrossingFrontiersDoNotFlap is the regression test for
// the reported failure: equal canonical advertised tips with delivered
// frontiers that repeatedly cross, including a gap larger than k. The active
// connection must not change, and no peer-to-peer ChainSwitchEvent may be
// published — that event is what drives the ledger's fresh-cursor close of a
// selected peer that is ahead of the local tip
// (ledger.chainSwitchNeedsFreshCursorLocked returns false for an empty
// PreviousConnectionId, so only a peer-to-peer switch can trigger it).
func TestCanonicalPeersWithCrossingFrontiersDoNotFlap(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	t.Cleanup(eventBus.Stop)
	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:      eventBus,
		SecurityParam: previewSecurityParam,
	})
	_, switchCh := eventBus.Subscribe(ChainSwitchEventType)

	rootA := newTestConnectionId(1)
	rootB := newTestConnectionId(2)
	advertised := canonicalAdvertisedTip()

	cs.SetLocalTip(deliveredFrontier(27900))

	// Both roots have delivered the same header; A is established first and
	// becomes the incumbent.
	require.True(t, cs.updatePeerTipObserved(
		rootA,
		advertised,
		deliveredFrontier(27900),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, rootA, *cs.GetBestPeer())
	require.True(t, cs.updatePeerTipObserved(
		rootB,
		advertised,
		deliveredFrontier(27900),
		nil,
	))
	require.Equal(t, rootA, *cs.GetBestPeer())

	// Delivered frontiers now cross repeatedly. Every step keeps the two
	// advertised tips identical, so no step is a chain-quality change; the
	// last two crossings exceed k in both directions.
	steps := []struct {
		conn  ouroboros.ConnectionId
		block uint64
		note  string
	}{
		{rootB, 28029, "challenger leads by 129 (beyond the head margin)"},
		{rootA, 28100, "incumbent retakes the lead"},
		{rootB, 28461, "challenger leads by 361, still within k"},
		{rootB, 28842, "challenger leads by 742, beyond k"},
		{rootA, 28532, "incumbent catches up"},
		{rootA, 28964, "incumbent retakes the lead"},
		{rootB, 29274, "challenger leads by 310"},
		{rootB, 29706, "challenger leads by 742, beyond k again"},
	}
	for _, step := range steps {
		require.True(
			t,
			cs.updatePeerTipObserved(
				step.conn,
				advertised,
				deliveredFrontier(step.block),
				nil,
			),
			"delivered frontier %d must be accepted: %s",
			step.block,
			step.note,
		)
		best := cs.GetBestPeer()
		require.NotNil(t, best, "selection must not stall: %s", step.note)
		assert.Equal(
			t,
			rootA,
			*best,
			"active peer must not flap on delivered-frontier lead: %s",
			step.note,
		)
	}

	switchEvents := collectChainSwitchEvents(t, eventBus, switchCh)
	require.Len(
		t,
		switchEvents,
		1,
		"only the initial selection may publish a chain switch",
	)
	assert.Equal(
		t,
		ouroboros.ConnectionId{},
		switchEvents[0].PreviousConnectionId,
		"the only switch must be the initial selection, which cannot trigger a fresh-cursor close",
	)
}

// TestDivergentPeersStillSwitchOnLongerDeliveredChain is the AC #2 negative
// case for genuine chain divergence: the two peers advertise the same height
// and slot but DIFFERENT blocks, so they are not on the same chain. The
// delivered-frontier lead is then real chain quality and the selector must
// converge to the longer chain.
func TestDivergentPeersStillSwitchOnLongerDeliveredChain(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(deliveredFrontier(27900))

	incumbentAdvertised := tip(
		canonicalAdvertisedBlock,
		canonicalAdvertisedSlot,
		"fork-a-tip",
	)
	challengerAdvertised := tip(
		canonicalAdvertisedBlock,
		canonicalAdvertisedSlot,
		"fork-b-tip",
	)

	require.True(t, cs.updatePeerTipObserved(
		incumbent,
		incumbentAdvertised,
		deliveredFrontier(28029),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	require.True(t, cs.updatePeerTipObserved(
		challenger,
		challengerAdvertised,
		tip(28029, 628029, "fork-b-28029"),
		nil,
	))
	for _, block := range []uint64{28461, 28842} {
		require.True(t, cs.updatePeerTipObserved(
			challenger,
			challengerAdvertised,
			tip(block, 600000+block, "fork-b-"+itoa(block)),
			nil,
		))
	}

	best := cs.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(
		t,
		challenger,
		*best,
		"a genuinely divergent longer delivered chain must still win selection",
	)
}

// TestContradictedAdvertisedTipDoesNotExemptFrontierLag is the AC #2 negative
// case for a spoofed advertisement: a peer copies the honest peer's advertised
// tip but its delivered headers contradict the honest chain at a shared slot.
// The delivered history is the trusted signal, so the copied advertisement
// must not buy it the same-chain treatment.
func TestContradictedAdvertisedTipDoesNotExemptFrontierLag(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})

	honest := newTestConnectionId(1)
	spoofer := newTestConnectionId(2)
	advertised := canonicalAdvertisedTip()

	cs.SetLocalTip(deliveredFrontier(27900))

	// The spoofer delivers a conflicting block at slot 628029.
	require.True(t, cs.updatePeerTipObserved(
		spoofer,
		advertised,
		tip(28029, 628029, "spoofed-28029"),
		nil,
	))
	// The honest peer delivers the canonical block at the same slot and then
	// runs its frontier more than k ahead.
	require.True(t, cs.updatePeerTipObserved(
		honest,
		advertised,
		deliveredFrontier(28029),
		nil,
	))
	for _, block := range []uint64{28461, 28842} {
		require.True(t, cs.updatePeerTipObserved(
			honest,
			advertised,
			deliveredFrontier(block),
			nil,
		))
	}

	assert.False(
		t,
		peerSelectable(t, cs, spoofer),
		"a peer whose delivered headers contradict the leader must not be exempted by a copied advertised tip",
	)
	best := cs.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(t, honest, *best)
}

// TestSameChainExemptionDoesNotRescueImplausiblyBehindPeer is the AC #2
// negative case for the implausible-tip bound: the same-chain exemption
// applies only to the behind-best-frontier filter. A peer whose delivered
// frontier is more than k behind the APPLIED LOCAL tip is useless and must
// stay unselectable even when it advertises the canonical tip.
func TestSameChainExemptionDoesNotRescueImplausiblyBehindPeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})

	stuck := newTestConnectionId(1)
	advertised := canonicalAdvertisedTip()

	require.True(t, cs.updatePeerTipObserved(
		stuck,
		advertised,
		deliveredFrontier(27000),
		nil,
	))
	// Local tip advances well past the peer's delivered frontier.
	cs.SetLocalTip(deliveredFrontier(27000 + previewSecurityParam + 1))

	assert.False(
		t,
		peerSelectable(t, cs, stuck),
		"a peer more than k behind the applied local tip must stay unselectable",
	)
	cs.EvaluateAndSwitch()
	assert.Nil(
		t,
		cs.GetBestPeer(),
		"no peer may be selected when the only peer is implausibly behind",
	)
}

// TestSameChainPinStillReleasesOnLocalTipStall asserts the progress-aware
// escape survives the same-chain pin: an incumbent that stops driving local
// tip progress is released even though it advertises the same canonical tip as
// the challenger.
func TestSameChainPinStillReleasesOnLocalTipStall(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})
	clock := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	installFakeClock(cs, clock)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)
	advertised := canonicalAdvertisedTip()

	cs.SetLocalTip(deliveredFrontier(27900))
	require.True(t, cs.updatePeerTipObserved(
		incumbent,
		advertised,
		deliveredFrontier(27900),
		nil,
	))
	require.True(t, cs.updatePeerTipObserved(
		challenger,
		advertised,
		deliveredFrontier(27900),
		nil,
	))
	require.Equal(t, incumbent, *cs.GetBestPeer())

	require.True(t, cs.updatePeerTipObserved(
		challenger,
		advertised,
		deliveredFrontier(28332),
		nil,
	))
	require.Equal(
		t,
		incumbent,
		*cs.GetBestPeer(),
		"same-chain frontier lead alone must not release the pin",
	)

	// The applied local tip has not moved; past the stall timeout the pin
	// must release so the node can leave a non-progressing incumbent.
	clock.Advance(catchUpPinStallTimeout)
	require.True(t, cs.EvaluateAndSwitch())
	best := cs.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(
		t,
		challenger,
		*best,
		"the progress-aware stall escape must still release the same-chain pin",
	)
}

// TestDivergentLeaderDoesNotSuppressPeersAgreeingWithAnotherLeader asserts the
// same-chain check scans every peer holding the leading delivered frontier, not
// one arbitrary representative. With two equally tall leaders on different
// chains, a trailing peer that agrees with one of them must stay selectable
// regardless of map iteration order.
func TestDivergentLeaderDoesNotSuppressPeersAgreeingWithAnotherLeader(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})

	trailing := newTestConnectionId(1)
	honestLeader := newTestConnectionId(2)
	divergentLeader := newTestConnectionId(3)

	canonical := canonicalAdvertisedTip()
	divergent := tip(
		canonicalAdvertisedBlock,
		canonicalAdvertisedSlot,
		"divergent-tip",
	)

	cs.SetLocalTip(deliveredFrontier(27900))

	require.True(t, cs.updatePeerTipObserved(
		trailing,
		canonical,
		deliveredFrontier(28029),
		nil,
	))
	for _, block := range []uint64{28029, 28461, 28842} {
		require.True(t, cs.updatePeerTipObserved(
			honestLeader,
			canonical,
			deliveredFrontier(block),
			nil,
		))
		require.True(t, cs.updatePeerTipObserved(
			divergentLeader,
			divergent,
			tip(block, 600000+block, "divergent-"+itoa(block)),
			nil,
		))
	}

	assert.True(
		t,
		peerSelectable(t, cs, trailing),
		"a peer agreeing with one of the leading frontiers must stay selectable",
	)
}

// TestUnadvertisedTipIsNotSameChainEvidence pins the empty-hash guard in
// sameAdvertisedTip. Two peers that have not told us where their chains end
// both carry a zero advertised tip, which is identical but says nothing: it is
// the absence of an advertisement, not agreement on one. Without the guard the
// zero tips compare equal and every such pair is granted the same-chain
// exemption, so a peer that has advertised nothing and trails the leading
// frontier by more than k would stay selectable on no evidence at all.
func TestUnadvertisedTipIsNotSameChainEvidence(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: previewSecurityParam,
	})

	trailing := newTestConnectionId(1)
	leader := newTestConnectionId(2)
	var unadvertised ochainsync.Tip

	cs.SetLocalTip(deliveredFrontier(27900))

	require.True(t, cs.updatePeerTipObserved(
		trailing,
		unadvertised,
		deliveredFrontier(28029),
		nil,
	))
	for _, block := range []uint64{28029, 28461, 28771} {
		require.True(t, cs.updatePeerTipObserved(
			leader,
			unadvertised,
			deliveredFrontier(block),
			nil,
		))
	}

	// The gap is the reproduction's, and neither peer is filtered out by the
	// k-behind-the-applied-local-tip check, so the behind-best-frontier filter
	// is the only thing under test here.
	require.Equal(
		t,
		uint64(742),
		cs.GetPeerTip(leader).SelectionTip().BlockNumber-
			cs.GetPeerTip(trailing).SelectionTip().BlockNumber,
	)

	assert.False(
		t,
		peerSelectable(t, cs, trailing),
		"a zero advertised tip is an absent advertisement, not agreement, and must not exempt a peer from the behind-best-frontier filter",
	)
}

// peerActivityStallBuffer stands in for the production
// event.DefaultSubscriberBuffer (1024) used by the node's
// chainselection.peer_activity subscription. The mechanism under test is
// buffer-size independent: a handler that stops returning stops draining, and
// the buffer only decides how long that takes to become visible. In the
// Preview run the 1024-slot buffer took 12h31m of
// keepalive traffic to fill, which is exactly why the buffer is shrunk here
// rather than the events slowed down.
const peerActivityStallBuffer = 4

// blockedConsumer is a downstream EventBus consumer that parks inside its
// handler until the test releases it, modelling the ledger's
// chainselection.chain_switch subscriber blocked on ls.chainsyncMutex while
// the chainsync pipeline is not making progress.
type blockedConsumer struct {
	release   chan struct{}
	closeOnce sync.Once
	delivered atomic.Int64
	entered   atomic.Int64
}

func newBlockedConsumer(t *testing.T) *blockedConsumer {
	t.Helper()
	return &blockedConsumer{release: make(chan struct{})}
}

func (b *blockedConsumer) handle(event.Event) {
	b.entered.Add(1)
	<-b.release
	b.delivered.Add(1)
}

func (b *blockedConsumer) unblock() {
	b.closeOnce.Do(func() { close(b.release) })
}

// newStalledSelectionFixture builds a ChainSelector with one tracked best peer
// and a downstream chainselection.chain_selection consumer that never returns.
// chain_selection is published by publishSelectionEvents on every accepted
// TouchPeerActivity that has a best peer, so it is the deterministic member of
// the same synchronous fan-out that also carries chain_switch.
func newStalledSelectionFixture(
	t *testing.T,
) (*event.EventBus, *ChainSelector, *blockedConsumer, ouroboros.ConnectionId) {
	t.Helper()

	bus := event.NewEventBus(nil, nil)
	consumer := newBlockedConsumer(t)
	// LIFO cleanup: the consumer is released before the bus is stopped, or
	// EventBus.shutdown would wait forever on the parked dispatch goroutine.
	t.Cleanup(bus.Stop)
	t.Cleanup(consumer.unblock)

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          bus,
		StaleTipThreshold: time.Hour,
	})

	connId := newTestConnectionId(1)
	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("best")},
		BlockNumber: 50,
	}, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer(), "fixture needs a selected best peer")

	// Subscribed only after the setup publishes above have drained, so the
	// fixture itself cannot park on its own blocked consumer.
	//
	// SubscriberBackpressureBlock models a downstream that stays blocked:
	// under the default Detach the wait ends at event.channelDeliveryTimeout
	// and the subscriber is dropped, which is what the 92b2e3e6 build in the
	// issue did not do at all and is a different failure (permanent silent
	// loss) rather than the stall under test here.
	bus.SubscribeFuncWithBufferPolicy(
		ChainSelectionEventType,
		1,
		event.SubscriberBackpressureBlock,
		consumer.handle,
	)
	return bus, cs, consumer, connId
}

// A blocked downstream consumer must not stop the internal
// chainselection.peer_activity subscriber from draining. The Preview run in
// shows the opposite: the handler stopped returning,
// its 1024-slot buffer filled over the next 12h31m, and from then on every
// keepalive response parked a protocol goroutine inside EventBus.Publish
// (ouroboros/keepalive.go) with 299 of them blocked by the end of the log.
func TestPeerActivityHandlerKeepsDrainingWhileDownstreamConsumerBlocks(
	t *testing.T,
) {
	bus, cs, consumer, connId := newStalledSelectionFixture(t)

	var handled atomic.Int64
	// Mirrors the node's subscribeChainSelectorEvents wiring for
	// chainselection.peer_activity, with the blocking policy so a stalled
	// handler is measured rather than the detach timer.
	bus.SubscribeFuncWithBufferPolicy(
		PeerActivityEventType,
		peerActivityStallBuffer,
		event.SubscriberBackpressureBlock,
		func(evt event.Event) {
			cs.HandlePeerActivityEvent(evt)
			handled.Add(1)
		},
	)

	// Enough touches to overrun the subscriber buffer several times over.
	const touches = peerActivityStallBuffer * 4
	published := make(chan struct{})
	go func() {
		defer close(published)
		for range touches {
			bus.Publish(
				PeerActivityEventType,
				event.NewEvent(
					PeerActivityEventType,
					PeerActivityEvent{ConnectionId: connId},
				),
			)
		}
	}()

	// The negative case: a blocked downstream must not park the keepalive-side
	// publishers. Every publish has to return.
	testutil.RequireReceive(t, published, 10*time.Second,
		"keepalive publishers parked on the peer_activity subscriber: a "+
			"blocked downstream consumer must not park protocol goroutines",
	)
	testutil.WaitForCondition(t, func() bool {
		return handled.Load() == touches
	}, 10*time.Second,
		"the internal peer_activity subscriber stopped draining while a "+
			"downstream consumer was blocked",
	)
	require.Zero(t, consumer.delivered.Load(),
		"fixture invariant: the downstream consumer is still blocked",
	)

	// Releasing the downstream consumer must still deliver the selection
	// events the activity path produced: they are deferred, not discarded.
	consumer.unblock()
	testutil.WaitForCondition(t, func() bool {
		return consumer.delivered.Load() > 0
	}, 10*time.Second,
		"selection events produced by the activity path were never delivered",
	)
}

// Deferring publication must not reorder chain switches: a subscriber that
// acts on chainselection.chain_switch (the ledger repoints its chainsync
// cursor at NewConnectionId) ends up on the wrong peer if an older switch is
// delivered after a newer one.
//
// What this pins is publisher order, which is the whole of what the lane
// promises. All eight switches are decided and published from this goroutine,
// so decision order and publish order coincide here. They do not in general:
// every producer decides under cs.mutex and publishes after releasing it, so
// two goroutines can decide in one order and enqueue in the other, and no lane
// prevents that (wolf31o2 review). See publishSelection.
func TestChainSwitchEventsPreservePublishOrder(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	_, evtCh := bus.SubscribeWithBuffer(ChainSwitchEventType, 64)

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          bus,
		StaleTipThreshold: time.Hour,
	})

	connA := newTestConnectionId(1)
	connB := newTestConnectionId(2)

	const switches = 8
	want := make([]ouroboros.ConnectionId, 0, switches)
	blockNumber := uint64(50)
	for i := range switches {
		conn := connA
		if i%2 == 1 {
			conn = connB
		}
		blockNumber++
		cs.UpdatePeerTip(conn, ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 100 + blockNumber,
				Hash: []byte(conn.String()),
			},
			BlockNumber: blockNumber,
		}, nil)
		cs.EvaluateAndSwitch()
		want = append(want, conn)
	}

	got := make([]ouroboros.ConnectionId, 0, switches)
	for range switches {
		evt := testutil.RequireReceive(t, evtCh, 10*time.Second,
			"chain switch event was never delivered",
		)
		data, ok := evt.Data.(ChainSwitchEvent)
		require.True(t, ok, "expected ChainSwitchEvent, got %T", evt.Data)
		got = append(got, data.NewConnectionId)
	}
	require.Equal(t, want, got,
		"chain switch events must be delivered in publish order",
	)
}

// peerBlockNumber reads a tracked peer's current delivered-frontier block
// number.
func peerBlockNumber(
	t *testing.T,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
) uint64 {
	t.Helper()
	pt := cs.GetPeerTip(connId)
	require.NotNil(t, pt, "peer must be tracked")
	return pt.SelectionTip().BlockNumber
}

// TestSwitchBackCooldownBoundsOscillationFrequency reproduces the storm with
// two peers whose delivered frontiers repeatedly leapfrog each other by more
// than catchUpPinHeadMargin -- plausible on ordinary per-header delivery
// jitter near the tip, since a single newly delivered header can itself cross
// a margin of a couple of blocks -- and asserts the active connection does
// not flap between them faster than once per SwitchBackCooldown, while a
// challenger that was never the very recently abandoned incumbent is still
// adopted immediately.
func TestSwitchBackCooldownBoundsOscillationFrequency(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	peerA := newTestConnectionId(1)
	peerB := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "local"))

	// A becomes the incumbent (first ever selection: never debounced).
	require.True(t, cs.updatePeerTipObserved(
		peerA,
		tip(5001, 5001, "a-0"),
		tip(5001, 5001, "a-0"),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, peerA, *cs.GetBestPeer())

	// B leapfrogs A by more than the margin. A was never abandoned before, so
	// this first hand-off proceeds normally -- the fix must not block a
	// genuinely new challenger.
	require.True(t, cs.updatePeerTipObserved(
		peerB,
		tip(5001+catchUpPinHeadMargin+1, 5010, "b-0"),
		tip(5001+catchUpPinHeadMargin+1, 5010, "b-0"),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, peerB, *cs.GetBestPeer(),
		"a genuinely longer challenger must be adopted")

	// Now reproduce the storm: whichever peer is NOT currently active
	// leapfrogs the active one by margin+1 every iteration -- a genuine
	// crossing each time -- with no wall-clock time passing between updates
	// (the pathological case: bursty near-instantaneous header delivery).
	// Every attempted hand-off tries to switch back to the peer the active
	// connection most recently left.
	switches := 0
	for i := range 20 {
		active := *cs.GetBestPeer()
		challenger := peerA
		if active == peerA {
			challenger = peerB
		}
		next := peerBlockNumber(t, cs, active) + catchUpPinHeadMargin + 1
		slot := 10_000 + uint64(i)
		hash := fmt.Sprintf("burst-%d", i)
		require.True(t, cs.updatePeerTipObserved(
			challenger,
			tip(next, slot, hash),
			tip(next, slot, hash),
			nil,
		))
		if *cs.GetBestPeer() == challenger {
			switches++
		}
	}
	assert.Equal(
		t,
		0,
		switches,
		"switch-back debounce must suppress every immediate reversal in the burst",
	)
	assert.Equal(
		t,
		peerB,
		*cs.GetBestPeer(),
		"active connection must not flap during the burst",
	)

	// The cooldown is a rate limit, not a permanent pin: once it elapses, a
	// persistent lead is still adopted.
	clk.Advance(defaultSwitchBackCooldown)
	final := peerBlockNumber(t, cs, peerB) + catchUpPinHeadMargin + 1
	require.True(t, cs.updatePeerTipObserved(
		peerA,
		tip(final, 20_000, "a-final"),
		tip(final, 20_000, "a-final"),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(
		t,
		peerA,
		*cs.GetBestPeer(),
		"a persistent lead must still win once the cooldown has elapsed",
	)
}

// TestSwitchBackCooldownBoundsStallEscapeOscillation reproduces a second,
// structurally similar storm: once the applied local tip stalls,
// localTipStalledLocked stays true on every subsequent evaluation until
// progress resumes, so before this fix the progress-stall escape released
// the pin unconditionally forever -- providing no hysteresis at all,
// regardless of catchUpPinHeadMargin, for as long as the stall lasted. Live
// Preview reports showed exactly this: the local tip flatlined (confirmed by
// two direct metric reads with zero movement) while three connections
// oscillated sub-second-to-few-seconds apart, including a direct
// back-and-forth between the same two peers ~227ms apart -- well inside
// SwitchBackCooldown, which the first fix (gating only the longer-chain
// escape) did not protect because the stall escape released the pin before
// that gate was ever reached. The switching itself is what prevents any
// blockfetch batch from completing, so the stall never clears on its own: a
// self-sustaining livelock.
func TestSwitchBackCooldownBoundsStallEscapeOscillation(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	peerA := newTestConnectionId(1)
	peerB := newTestConnectionId(2)
	peerC := newTestConnectionId(3)

	cs.SetLocalTip(tip(5000, 5000, "local"))
	require.True(t, cs.updatePeerTipObserved(
		peerA,
		tip(5001, 5001, "a-0"),
		tip(5001, 5001, "a-0"),
		nil,
	))
	require.Equal(t, peerA, *cs.GetBestPeer())

	// The applied local tip never advances again: past the stall timeout,
	// localTipStalledLocked is true on every following evaluation for the
	// rest of the test.
	clk.Advance(catchUpPinStallTimeout + time.Second)

	// B edges A by a single block -- well within catchUpPinHeadMargin, so
	// only the (now-engaged) stall escape can release the pin here; the
	// longer-chain escape does not apply at this margin. This first switch
	// must still happen promptly: a genuinely new candidate is never
	// debounced, so the stall escape's liveness guarantee is unaffected.
	require.True(t, cs.updatePeerTipObserved(
		peerB,
		tip(5002, 5002, "b-0"),
		tip(5002, 5002, "b-0"),
		nil,
	))
	require.Equal(t, peerB, *cs.GetBestPeer(),
		"the stall escape must still release the pin for a new candidate")

	// Reproduce the storm: A and B keep leapfrogging by a single block, well
	// within the margin, driven entirely by the unconditional stall escape.
	switches := 0
	for i := range 20 {
		active := *cs.GetBestPeer()
		challenger := peerA
		if active == peerA {
			challenger = peerB
		}
		next := peerBlockNumber(t, cs, active) + 1
		slot := 10_000 + uint64(i)
		hash := fmt.Sprintf("stall-burst-%d", i)
		require.True(t, cs.updatePeerTipObserved(
			challenger,
			tip(next, slot, hash),
			tip(next, slot, hash),
			nil,
		))
		if *cs.GetBestPeer() == challenger {
			switches++
		}
	}
	assert.Equal(
		t,
		0,
		switches,
		"switch-back debounce must suppress every immediate reversal driven by the stall escape",
	)

	// A genuinely new peer (never recently active) is still subject to the
	// GLOBAL discretionary rate limit (switchBackRateLimitedLocked): the
	// burst loop above never advanced the clock, so the last discretionary
	// hand-off (the very first A->B switch) is still inside its cooldown
	// window, and C's arrival must not be exempted from it merely because C
	// itself was never the specific connection just abandoned. Exempting
	// "never seen as active" outright is exactly the gap that let a small
	// rotating set of real peers evade the per-connection debounce forever
	// (see TestSwitchBackRateLimitBoundsThreeWayRotation). Unambiguously
	// ahead of BOTH A and B (not merely tied with whichever one the burst
	// loop last bumped), so selectBestChainLocked's upstream pick cannot land
	// back on A or B by a map-iteration-order tiebreak among equal-height
	// peers.
	next := max(
		peerBlockNumber(t, cs, peerA),
		peerBlockNumber(t, cs, peerB),
	) + 50
	require.True(t, cs.updatePeerTipObserved(
		peerC,
		tip(next, 99_999, "c-0"),
		tip(next, 99_999, "c-0"),
		nil,
	))
	assert.Equal(t, peerB, *cs.GetBestPeer(),
		"a new candidate arriving inside the global cooldown window must still be rate-limited")

	// Once the global cooldown elapses, the new candidate is adopted.
	clk.Advance(defaultSwitchBackCooldown)
	require.True(t, cs.updatePeerTipObserved(
		peerC,
		tip(next+1, 100_000, "c-1"),
		tip(next+1, 100_000, "c-1"),
		nil,
	))
	assert.Equal(t, peerC, *cs.GetBestPeer(),
		"a genuinely new candidate must still be adopted once the global cooldown elapses")

	// The cooldown is a rate limit, not a permanent freeze: once it elapses
	// again, a connection abandoned earlier is reclaimable too.
	clk.Advance(defaultSwitchBackCooldown)
	next = peerBlockNumber(t, cs, peerC) + 1
	require.True(t, cs.updatePeerTipObserved(
		peerA,
		tip(next, 999_999, "a-final"),
		tip(next, 999_999, "a-final"),
		nil,
	))
	assert.Equal(t, peerA, *cs.GetBestPeer(),
		"a previously abandoned connection must be reclaimable once the cooldown elapses")
}

// TestSwitchBackCooldownDoesNotBlockGenuinelyNewChallenger asserts that the
// per-connection debounce (switchBackDebouncedLocked) does not, on its own,
// single out a third peer that was never the active connection: peerC is not
// blocked by recentlyLeft, since it has no entry there. It IS still bounded
// by the global discretionary rate limit (switchBackRateLimitedLocked), which
// applies regardless of which connection is involved -- exempting a
// "never-before-active" peer outright is exactly the gap that let a small
// rotating set of real peers dodge the per-connection debounce forever (see
// TestSwitchBackRateLimitBoundsThreeWayRotation). So peerC's switch is
// delayed until the global cooldown elapses, not blocked forever and not
// adopted mid-cooldown.
func TestSwitchBackCooldownDoesNotBlockGenuinelyNewChallenger(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	peerA := newTestConnectionId(1)
	peerB := newTestConnectionId(2)
	peerC := newTestConnectionId(3)

	cs.SetLocalTip(tip(5000, 5000, "local"))
	require.True(t, cs.updatePeerTipObserved(
		peerA,
		tip(5001, 5001, "a-0"),
		tip(5001, 5001, "a-0"),
		nil,
	))
	require.Equal(t, peerA, *cs.GetBestPeer())

	// B takes over (A is now within its cooldown window).
	require.True(t, cs.updatePeerTipObserved(
		peerB,
		tip(5001+catchUpPinHeadMargin+1, 5010, "b-0"),
		tip(5001+catchUpPinHeadMargin+1, 5010, "b-0"),
		nil,
	))
	require.Equal(t, peerB, *cs.GetBestPeer())

	// C, a peer that has never been active, genuinely outruns B by more than
	// the margin. The per-connection debounce alone would not block it (C has
	// no recentlyLeft entry), but the global discretionary rate limit still
	// applies: the A->B switch just above was itself a discretionary release,
	// and no time has passed since, so C's arrival mid-cooldown must still be
	// rate-limited.
	require.True(t, cs.updatePeerTipObserved(
		peerC,
		tip(5001+2*(catchUpPinHeadMargin+1), 5020, "c-0"),
		tip(5001+2*(catchUpPinHeadMargin+1), 5020, "c-0"),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, peerB, *cs.GetBestPeer(),
		"a new candidate arriving inside the global cooldown window must still be rate-limited")

	// Once the global cooldown elapses, C is adopted -- the rate limit delays
	// a genuinely new candidate, it does not exempt or permanently block it.
	clk.Advance(defaultSwitchBackCooldown)
	require.True(t, cs.updatePeerTipObserved(
		peerC,
		tip(5001+2*(catchUpPinHeadMargin+1)+1, 5021, "c-1"),
		tip(5001+2*(catchUpPinHeadMargin+1)+1, 5021, "c-1"),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, peerC, *cs.GetBestPeer(),
		"a genuinely new candidate must still be adopted once the global cooldown elapses")
}

// TestSwitchBackRateLimitBoundsThreeWayRotation reproduces the live incident
// this global rate limit exists for: not two peers alternating, but three (or
// more) real peer connections whose delivered frontiers take turns
// marginally leading each other under ordinary network jitter. Observed live:
// the active connection flipped among the same three peer connections roughly
// every ~2 seconds indefinitely, with applied block height completely frozen
// throughout.
//
// switchBackDebouncedLocked alone (per-connection: "was THIS challenger the
// specific connection just abandoned") does not bound this. With three
// peers rotating A -> B -> C -> A -> ..., by the time evaluation cycles back
// to a given connection it is essentially never "the one most recently left"
// -- some other peer was left more recently -- so the per-connection debounce
// never engages for it, even under realistic (non-zero) inter-arrival jitter.
// Driving that rotation with each hop's target advancing the clock by just
// over half of switchBackCooldown (so a specific peer's own recurrence
// interval -- two hops later -- safely clears its per-connection cooldown)
// demonstrates the gap directly: every single hop succeeds, so the number of
// switches equals the number of hops and the active connection never holds
// still for as long as switchBackCooldown.
//
// switchBackRateLimitedLocked (this fix) bounds the AGGREGATE hand-off rate
// to at most one per switchBackCooldown regardless of which peer is
// challenging or how many distinct peers are rotating, so it closes this gap
// even though every individual hop still looks, pairwise, like a "genuinely
// new" challenger.
func TestSwitchBackRateLimitBoundsThreeWayRotation(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	peers := []ouroboros.ConnectionId{
		newTestConnectionId(1),
		newTestConnectionId(2),
		newTestConnectionId(3),
	}

	cs.SetLocalTip(tip(5000, 5000, "local"))
	require.True(t, cs.updatePeerTipObserved(
		peers[0],
		tip(5001, 5001, "p0-0"),
		tip(5001, 5001, "p0-0"),
		nil,
	))
	require.Equal(t, peers[0], *cs.GetBestPeer())

	// Hop spacing is chosen so a specific peer's OWN recurrence interval (two
	// hops later, in a strict 3-way rotation) exceeds switchBackCooldown --
	// clearing the per-connection debounce on every single hop -- while
	// consecutive hops themselves remain well inside the cooldown, exactly
	// the "bursty relative to a single pair, spaced-out relative to any one
	// peer" cadence real network jitter produces.
	hopSpacing := defaultSwitchBackCooldown/2 + 100*time.Millisecond
	require.Less(
		t,
		defaultSwitchBackCooldown,
		2*hopSpacing,
		"test setup: a peer's own recurrence interval must clear its per-connection cooldown",
	)

	const rounds = 30
	switches := 0
	var lastSwitchAt time.Time
	activeIdx := 0
	block := uint64(5001)
	for i := range rounds {
		clk.Advance(hopSpacing)
		challengerIdx := (activeIdx + 1) % len(peers)
		block += catchUpPinHeadMargin + 1
		slot := 10_000 + uint64(i)
		hash := fmt.Sprintf("rotation-%d", i)
		require.True(t, cs.updatePeerTipObserved(
			peers[challengerIdx],
			tip(block, slot, hash),
			tip(block, slot, hash),
			nil,
		))
		if *cs.GetBestPeer() == peers[challengerIdx] {
			if !lastSwitchAt.IsZero() {
				assert.GreaterOrEqual(
					t,
					clk.now.Sub(lastSwitchAt),
					defaultSwitchBackCooldown,
					"two discretionary switches happened less than a cooldown apart at round %d",
					i,
				)
			}
			lastSwitchAt = clk.now
			switches++
			activeIdx = challengerIdx
		}
	}

	// Without the global rate limit, every one of the 30 hops succeeds (each
	// challenger clears the per-connection debounce by construction), so the
	// active connection would never hold still. With it, successive switches
	// are at least a cooldown apart, so over `rounds*hopSpacing` of simulated
	// time the count is bounded well below the hop count.
	maxExpectedSwitches := int(
		time.Duration(rounds)*hopSpacing/defaultSwitchBackCooldown,
	) + 1
	assert.Less(
		t,
		switches,
		rounds,
		"the global rate limit must reject at least some hops in the rotation",
	)
	assert.LessOrEqual(
		t,
		switches,
		maxExpectedSwitches,
		"switch count must stay within the global cooldown's rate bound",
	)
	assert.Greater(
		t,
		switches,
		0,
		"the rate limit must still allow forward progress, not freeze selection entirely",
	)
}

// fakeClock is a deterministic, mutable clock for exercising the progress-aware
// stall escape without sleeping.
type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// installFakeClock swaps the selector's nowFn for a deterministic clock and
// seeds the initial time. Must be called before any SetLocalTip that should
// record progress at a known instant.
func installFakeClock(cs *ChainSelector, c *fakeClock) {
	cs.mutex.Lock()
	cs.nowFn = c.Now
	cs.mutex.Unlock()
}

// tip is a small helper to build a chainsync tip.
func tip(block, slot uint64, hash string) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.Point{Slot: slot, Hash: []byte(hash)},
		BlockNumber: block,
	}
}

// TestPinNoSwitchOnMicroForkDuringCatchUp asserts that while the node is in
// deep catch-up (gap > catchUpPinBlockThreshold), the active connection does
// NOT flap across micro-forking equal-tip / 1-block-ahead peers.
func TestPinNoSwitchOnMicroForkDuringCatchUp(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	// Local tip far behind both peers: catch-up regime.
	cs.SetLocalTip(tip(1000, 1000, "local"))

	// Incumbent established at block 1200 (gap 200 > threshold 100).
	cs.UpdatePeerTip(incumbent, tip(1200, 1200, "inc-1200"), nil)
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Challenger leapfrogs to a sibling at the same height, then one block
	// ahead, repeatedly. These are head micro-forks; the active connection
	// must remain pinned to the incumbent.
	cs.UpdatePeerTip(challenger, tip(1200, 1201, "chal-1200"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"must not switch on same-height sibling during catch-up")

	cs.UpdatePeerTip(challenger, tip(1201, 1202, "chal-1201"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"must not switch on 1-block micro-fork during catch-up")

	// Incumbent leapfrogs back ahead; still pinned to incumbent.
	cs.UpdatePeerTip(incumbent, tip(1202, 1203, "inc-1202"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer())

	// Confirm the pin is in the catch-up regime.
	cs.mutex.RLock()
	catchingUp := cs.catchingUpLocked()
	cs.mutex.RUnlock()
	assert.True(t, catchingUp, "expected catch-up regime for this scenario")
}

// TestPinNoSwitchOnSiblingHeadForkAtTip asserts that at/near the live tip
// (gap < catchUpPinBlockThreshold), the active connection still does NOT flap
// across same-height sibling head-forks. This is the Part 2 tip-hold case.
func TestPinNoSwitchOnSiblingHeadForkAtTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	// Local tip at the head: gap is tiny, NOT catch-up.
	cs.SetLocalTip(tip(5000, 5000, "local"))

	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "inc-5001"), nil)
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Sibling head-fork at the same height (different hash/slot). With no VRF
	// data the Praos comparison is ChainEqual, so this is held by the existing
	// equal-tip preservation — the active connection must not flap.
	cs.UpdatePeerTip(challenger, tip(5001, 5002, "chal-5001"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"must not switch on same-height sibling at the tip")

	// Challenger one block ahead (head micro-fork): selectBestChain prefers the
	// taller challenger, and the anti-flap pin holds the incumbent.
	cs.UpdatePeerTip(challenger, tip(5002, 5003, "chal-5002"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"must not switch on 1-block head micro-fork at the tip")
	cs.UpdatePeerTip(challenger, tip(5003, 5004, "chal-5003"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer())

	// Confirm we are NOT in the catch-up regime (tip-hold path exercised).
	cs.mutex.RLock()
	catchingUp := cs.catchingUpLocked()
	cs.mutex.RUnlock()
	assert.False(t, catchingUp, "expected tip-hold (non-catch-up) regime")
}

// TestPinSwitchesToGenuinelyLongerChainCatchUp asserts the longer-chain escape
// fires during catch-up: a challenger beyond catchUpPinHeadMargin is a real
// longer chain and the active connection switches to it.
func TestPinSwitchesToGenuinelyLongerChainCatchUp(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(1000, 1000, "local"))
	cs.UpdatePeerTip(incumbent, tip(1200, 1200, "inc-1200"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Challenger genuinely longer: ahead by more than catchUpPinHeadMargin.
	cs.UpdatePeerTip(
		challenger,
		tip(1200+catchUpPinHeadMargin+1, 1300, "chal-long"),
		nil,
	)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"must converge to a genuinely longer chain during catch-up")
}

// TestPinSwitchesToGenuinelyLongerChainAtTip asserts the longer-chain escape
// also fires at the tip.
func TestPinSwitchesToGenuinelyLongerChainAtTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "local"))
	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "inc-5001"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	cs.UpdatePeerTip(
		challenger,
		tip(5001+catchUpPinHeadMargin+1, 5100, "chal-long"),
		nil,
	)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"must converge to a genuinely longer chain at the tip")
}

// TestPinAtMarginBoundary verifies the exact margin boundary: a challenger
// exactly catchUpPinHeadMargin ahead stays pinned, one block more releases.
func TestPinAtMarginBoundary(t *testing.T) {
	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	// Exactly at the margin: pinned.
	cs := NewChainSelector(ChainSelectorConfig{})
	cs.SetLocalTip(tip(5000, 5000, "local"))
	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "inc"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())
	cs.UpdatePeerTip(
		challenger,
		tip(5001+catchUpPinHeadMargin, 5050, "chal-at"),
		nil,
	)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"challenger exactly at the margin must stay pinned")

	// One block past the margin: released.
	cs2 := NewChainSelector(ChainSelectorConfig{})
	cs2.SetLocalTip(tip(5000, 5000, "local"))
	cs2.UpdatePeerTip(incumbent, tip(5001, 5001, "inc"), nil)
	require.Equal(t, incumbent, *cs2.GetBestPeer())
	cs2.UpdatePeerTip(
		challenger,
		tip(5001+catchUpPinHeadMargin+1, 5050, "chal-past"),
		nil,
	)
	assert.Equal(t, challenger, *cs2.GetBestPeer(),
		"challenger one block past the margin must release the pin")
}

// TestPinProgressStallEscape asserts the progress-aware escape: when the
// applied local tip stops advancing for catchUpPinStallTimeout, the pin
// releases and the active connection switches to the challenger sibling.
func TestPinProgressStallEscape(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	// Record forward progress at t0. The challenger runs one block ahead of
	// the incumbent (a head micro-fork within catchUpPinHeadMargin), so it is
	// the canonically Praos-better chain (higher block number). Only the
	// anti-flap pin keeps the incumbent active; once the pin releases the
	// taller challenger is selected.
	cs.SetLocalTip(tip(5000, 5000, "local"))
	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "inc-5001"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Challenger one block ahead (head micro-fork): pinned (no progress yet,
	// but not yet stalled).
	cs.UpdatePeerTip(challenger, tip(5002, 5002, "chal-5002"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer())

	// Repeated same-or-lower local tip updates must NOT reset the stall clock.
	cs.SetLocalTip(tip(5000, 5000, "local-again"))
	cs.mutex.RLock()
	progressAt := cs.localTipProgressAt
	cs.mutex.RUnlock()
	assert.Equal(t, clk.now, progressAt,
		"same-tip SetLocalTip must not reset the stall clock")

	// Just under the timeout: still pinned.
	clk.Advance(catchUpPinStallTimeout - time.Second)
	cs.UpdatePeerTip(challenger, tip(5002, 5003, "chal-5002b"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"must stay pinned before the stall timeout elapses")

	// Past the timeout: the applied tip has stalled, the pin releases and the
	// taller challenger is selected.
	clk.Advance(2 * time.Second)
	cs.UpdatePeerTip(challenger, tip(5003, 5004, "chal-5003"), nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"stall escape must release the pin after the timeout")
}

// TestPinStallClockResetsOnForwardProgress asserts that forward progress
// re-arms the stall clock, so a healthy, advancing incumbent stays pinned
// indefinitely across sibling head-forks.
func TestPinStallClockResetsOnForwardProgress(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "local"))
	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "inc"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())
	// Challenger one block ahead (head micro-fork) so the pin is what holds
	// the incumbent (selectBestChain prefers the taller challenger).
	cs.UpdatePeerTip(challenger, tip(5002, 5002, "chal"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Advance almost to the timeout, then apply a new block (forward progress).
	clk.Advance(catchUpPinStallTimeout - time.Second)
	cs.SetLocalTip(tip(5001, 5005, "local-advance"))

	// Advance another near-timeout window. Because progress re-armed the
	// clock, the incumbent is still pinned.
	clk.Advance(catchUpPinStallTimeout - time.Second)
	cs.UpdatePeerTip(challenger, tip(5002, 5006, "chal-b"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"forward progress must re-arm the stall clock and keep the pin")
}

// TestPinStallClockResetsOnPostRollbackProgress asserts that progress tracking
// compares against the previous applied local tip, not the all-time high-water
// mark. After 1000 -> 990 rollback, 991 is forward progress and must re-arm the
// stall clock.
func TestPinStallClockResetsOnPostRollbackProgress(t *testing.T) {
	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(1000, 1000, "local"))
	cs.UpdatePeerTip(incumbent, tip(1001, 1001, "inc"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())
	cs.UpdatePeerTip(challenger, tip(1002, 1002, "chal"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	clk.Advance(catchUpPinStallTimeout - time.Second)
	cs.SetLocalTip(tip(990, 1003, "local-rollback"))
	cs.mutex.RLock()
	rollbackProgressAt := cs.localTipProgressAt
	cs.mutex.RUnlock()
	assert.Equal(t, time.Unix(1_700_000_000, 0), rollbackProgressAt,
		"rollback must not reset the stall clock")

	cs.SetLocalTip(tip(991, 1004, "local-reapply"))
	cs.mutex.RLock()
	reapplyProgressAt := cs.localTipProgressAt
	cs.mutex.RUnlock()
	assert.Equal(t, clk.now, reapplyProgressAt,
		"post-rollback forward progress must reset the stall clock")

	clk.Advance(2 * time.Second)
	cs.UpdatePeerTip(challenger, tip(1002, 1005, "chal-b"), nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"post-rollback forward progress must keep the pin while moving again")
}

// TestPinReleasesWhenIncumbentNoLongerSelectable asserts that an ineligible
// incumbent releases the pin even on a head micro-fork.
func TestPinReleasesWhenIncumbentNoLongerSelectable(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "local"))
	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "inc"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())
	// Challenger one block ahead (head micro-fork): the pin holds the
	// incumbent while it is selectable.
	cs.UpdatePeerTip(challenger, tip(5002, 5002, "chal"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Incumbent becomes ineligible: pin must release.
	cs.SetConnectionEligible(incumbent, false)
	switched := cs.EvaluateAndSwitch()
	assert.True(t, switched)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"ineligible incumbent must release the pin")
}

// TestPinInactiveWithoutLocalTip asserts near-genesis behavior is unchanged:
// when SetLocalTip has never been called, the pin is inactive and a 1-block
// lead switches the active connection (matching the legacy behavior asserted
// by TestIncumbentAdvantageSwitchesOnOneBlockLead).
func TestPinInactiveWithoutLocalTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	// No SetLocalTip: localTip.BlockNumber == 0, pin inactive.
	cs.UpdatePeerTip(incumbent, tip(50, 100, "inc"), nil)
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// 1-block lead: must switch because the pin is inactive near genesis.
	cs.UpdatePeerTip(challenger, tip(51, 101, "chal"), nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"pin must be inactive without a local tip (near-genesis)")

	// Confirm catchingUpLocked is false with no local tip.
	cs.mutex.RLock()
	catchingUp := cs.catchingUpLocked()
	stalled := cs.localTipStalledLocked()
	cs.mutex.RUnlock()
	assert.False(t, catchingUp)
	assert.False(t, stalled,
		"stall must be false before any forward progress is recorded")
}
