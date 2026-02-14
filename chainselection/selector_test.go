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
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestChainSelectorRemovePeer(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId := newTestConnectionId(1)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("test")},
		BlockNumber: 50,
	}

	cs.UpdatePeerTip(connId, tip, nil)
	assert.Equal(t, 1, cs.PeerCount())

	cs.RemovePeer(connId)
	assert.Equal(t, 0, cs.PeerCount())
	assert.Nil(t, cs.GetPeerTip(connId))
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

	// Drain any events from the initial selection
	for len(evtCh) > 0 {
		<-evtCh
	}

	// Remove the best peer - this should emit ChainSwitchEvent
	cs.RemovePeer(connId1)

	// Verify the new best peer is selected
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())

	// Verify ChainSwitchEvent was emitted
	select {
	case evt := <-evtCh:
		switchEvt, ok := evt.Data.(ChainSwitchEvent)
		require.True(t, ok, "expected ChainSwitchEvent")
		assert.Equal(t, connId1, switchEvt.PreviousConnectionId)
		assert.Equal(t, connId2, switchEvt.NewConnectionId)
		assert.Equal(t, tip2.BlockNumber, switchEvt.NewTip.BlockNumber)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected ChainSwitchEvent was not emitted")
	}
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
		BlockNumber: 50,
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

	// Drain any events from the initial selection
	for len(evtCh) > 0 {
		<-evtCh
	}

	// Wait for peer1 to become "very stale" (2x threshold = 100ms)
	// cleanupStalePeers uses 2x StaleTipThreshold for removal
	require.Eventually(t, func() bool {
		peerTip := cs.GetPeerTip(connId1)
		return peerTip != nil && peerTip.IsStale(100*time.Millisecond)
	}, 2*time.Second, 5*time.Millisecond, "peer1 should become very stale")

	// Keep peer2 fresh
	cs.UpdatePeerTip(connId2, tip2, nil)

	// Drain any events from tip update
	for len(evtCh) > 0 {
		<-evtCh
	}

	// Trigger cleanup - this should emit ChainSwitchEvent when best peer is
	// removed
	cs.cleanupStalePeers()

	// Verify the new best peer is selected
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer())

	// Verify ChainSwitchEvent was emitted
	select {
	case evt := <-evtCh:
		switchEvt, ok := evt.Data.(ChainSwitchEvent)
		require.True(t, ok, "expected ChainSwitchEvent")
		assert.Equal(t, connId1, switchEvt.PreviousConnectionId)
		assert.Equal(t, connId2, switchEvt.NewConnectionId)
		assert.Equal(t, tip2.BlockNumber, switchEvt.NewTip.BlockNumber)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected ChainSwitchEvent was not emitted")
	}
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
	assert.True(t, peerTip.LastUpdated.After(oldTime))
}

func TestChainSelectorVRFTiebreaker(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	// Two peers with identical tips (same block number and slot)
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("same")},
		BlockNumber: 50,
	}

	// VRF outputs: lower wins (per Ouroboros Praos)
	// Must be exactly VRFOutputSize (64 bytes) to be valid
	vrfLower := make64ByteVRF(0x00)
	vrfHigher := make64ByteVRF(0x01)

	// Add peer with higher VRF first
	cs.UpdatePeerTip(connId1, tip, vrfHigher)
	// Add peer with lower VRF second
	cs.UpdatePeerTip(connId2, tip, vrfLower)

	// The peer with lower VRF should win
	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId2, *bestPeer, "peer with lower VRF should win")
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

	// When VRF comparison returns equal (due to nil), fall back to connection ID
	// connId1 vs connId2 - the one with lexicographically smaller string wins
	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	// Since VRF comparison returns ChainEqual when one is nil,
	// it falls back to connection ID comparison
	assert.Equal(
		t,
		connId1,
		*bestPeer,
		"should fall back to connection ID ordering when one peer has nil VRF",
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

func TestChainSelectorVRFDoesNotOverrideSlot(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	// Both have same block number
	// Peer 1: higher slot (less dense) but lower VRF
	tip1 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 110, Hash: []byte("tip1")},
		BlockNumber: 50,
	}
	// Peer 2: lower slot (more dense) but higher VRF
	tip2 := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("tip2")},
		BlockNumber: 50,
	}

	vrfLower := make64ByteVRF(0x00)
	vrfHigher := make64ByteVRF(0xFF)

	cs.UpdatePeerTip(connId1, tip1, vrfLower)
	cs.UpdatePeerTip(connId2, tip2, vrfHigher)

	// Slot takes precedence - peer 2 (lower slot) should win despite higher VRF
	bestPeer := cs.SelectBestChain()
	require.NotNil(t, bestPeer)
	assert.Equal(t, connId2, *bestPeer, "lower slot should win over lower VRF")
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
	assert.Equal(t, 0, cs.PeerCount(), "peer count should remain 0")
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

func TestUpdatePeerTipRejectsTipOneOverBoundary(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: 2160,
	})

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("local")},
		BlockNumber: 50000,
	})

	connId := newTestConnectionId(1)

	// Tip one block past the boundary
	overBoundaryTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 104001,
			Hash: []byte("over"),
		},
		BlockNumber: 50000 + 2160 + 1,
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
	for i := 0; i < maxPeers; i++ {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: []byte(fmt.Sprintf("tip%d", i)),
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
	for i := 0; i < maxPeers; i++ {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: []byte(fmt.Sprintf("tip%d", i)),
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
	for i := 0; i < maxPeers; i++ {
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
				Hash: []byte(fmt.Sprintf("tip%d", i)),
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
	for i := 0; i < maxPeers; i++ {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: []byte(fmt.Sprintf("tip%d", i)),
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
	for i := 0; i < expectedCount; i++ {
		tip := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: uint64(100 + i),
				Hash: []byte(fmt.Sprintf("tip%d", i)),
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
	for i := 0; i < expectedCount; i++ {
		assert.NotNil(
			t,
			cs.GetPeerTip(connIds[i]),
			"peer %d should be tracked",
			i,
		)
	}
}
