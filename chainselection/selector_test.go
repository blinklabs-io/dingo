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
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

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
	peerTip.LastUpdated = time.Now().Add(-(threshold + time.Millisecond))
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

	equalTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("equal-tip")},
		BlockNumber: 60,
	}
	cs.UpdatePeerTip(incumbentConn, equalTip, nil)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, incumbentConn, *cs.GetBestPeer())

	require.Eventually(t, func() bool {
		cs.mutex.RLock()
		defer cs.mutex.RUnlock()
		peerTip := cs.peerTips[incumbentConn]
		return peerTip != nil && peerTip.IsStale(20*time.Millisecond)
	}, time.Second, 5*time.Millisecond)

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

	// Wait for peer1 to become stale (exceed threshold)
	require.Eventually(t, func() bool {
		peerTip := cs.GetPeerTip(connId1)
		return peerTip != nil && peerTip.IsStale(100*time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "peer1 should become stale")

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

	// Wait for peer1 to become "very stale" (2x threshold = 100ms)
	// cleanupStalePeers uses 2x StaleTipThreshold for removal
	require.Eventually(t, func() bool {
		peerTip := cs.GetPeerTip(connId1)
		return peerTip != nil && peerTip.IsStale(100*time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "peer1 should become very stale")

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

	require.Eventually(t, func() bool {
		peerTip := cs.GetPeerTip(bestConn)
		return peerTip != nil && peerTip.IsStale(50*time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "best peer should become stale")

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

	require.Eventually(t, func() bool {
		peerTip := cs.GetPeerTip(revivedConn)
		return peerTip != nil && peerTip.IsStale(50*time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "revived peer should become stale")

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
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          eventBus,
		StaleTipThreshold: 50 * time.Millisecond,
	})

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

	require.Eventually(t, func() bool {
		peerTip := cs.GetPeerTip(revivedConn)
		return peerTip != nil && peerTip.IsStale(50*time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "revived peer should become stale")

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
func TestChainSelectorRecoverEvaluationPanicToleratesPanickingLogger(t *testing.T) {
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
