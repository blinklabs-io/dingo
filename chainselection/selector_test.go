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
	vrfLower := []byte{0x00, 0x00, 0x00, 0x01}
	vrfHigher := []byte{0x00, 0x00, 0x00, 0x02}

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
	vrf := []byte{0x00, 0x01, 0x02, 0x03}

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

	vrfLower := []byte{0x00, 0x00, 0x00, 0x01}
	vrfHigher := []byte{0xFF, 0xFF, 0xFF, 0xFF}

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

	vrfLower := []byte{0x00, 0x00, 0x00, 0x01}
	vrfHigher := []byte{0xFF, 0xFF, 0xFF, 0xFF}

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
