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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// genesisTip builds a chainsync tip at the given slot with a named hash.
func genesisTip(slot uint64, hash string, block uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.Point{Slot: slot, Hash: []byte(hash)},
		BlockNumber: block,
	}
}

// feedFrontier applies a sequence of tips to a peer so it accumulates an
// observed slot/point frontier for Genesis density and corroboration.
func feedFrontier(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	tips ...ochainsync.Tip,
) {
	for _, tip := range tips {
		cs.UpdatePeerTip(connId, tip, nil)
	}
}

// recordObservedPoint keeps the slot frontier and the point (slot+hash)
// frontier in lockstep, storing the current hash at each slot.
func TestRecordObservedPointTracksHashFrontier(t *testing.T) {
	pt := &PeerChainTip{}
	window := uint64(100)
	for _, p := range []ocommon.Point{
		{Slot: 10, Hash: []byte("h10")},
		{Slot: 20, Hash: []byte("h20")},
		{Slot: 30, Hash: []byte("h30")},
	} {
		pt.recordObservedPoint(p, window)
	}

	require.Equal(t, []uint64{10, 20, 30}, pt.observedSlots)
	require.Len(t, pt.observedPoints, 3)
	for i, want := range []ocommon.Point{
		{Slot: 10, Hash: []byte("h10")},
		{Slot: 20, Hash: []byte("h20")},
		{Slot: 30, Hash: []byte("h30")},
	} {
		assert.Equal(t, want.Slot, pt.observedPoints[i].Slot)
		assert.Equal(t, want.Hash, pt.observedPoints[i].Hash)
	}

	// Re-reporting the same slot with a new hash updates the frontier in place.
	pt.recordObservedPoint(ocommon.Point{Slot: 30, Hash: []byte("h30b")}, window)
	require.Equal(t, []uint64{10, 20, 30}, pt.observedSlots)
	require.Len(t, pt.observedPoints, 3)
	assert.Equal(t, []byte("h30b"), pt.observedPoints[2].Hash)
}

// ApplyRollback trims the slot and point frontiers together so they stay
// aligned after a rollback.
func TestApplyRollbackTrimsPointFrontier(t *testing.T) {
	pt := &PeerChainTip{}
	window := uint64(100)
	for _, p := range []ocommon.Point{
		{Slot: 10, Hash: []byte("h10")},
		{Slot: 20, Hash: []byte("h20")},
		{Slot: 30, Hash: []byte("h30")},
	} {
		pt.recordObservedPoint(p, window)
	}

	pt.ApplyRollback(
		ocommon.Point{Slot: 20, Hash: []byte("h20")},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 25, Hash: []byte("h25")},
			BlockNumber: 4,
		},
	)

	require.Equal(t, []uint64{10, 20}, pt.observedSlots)
	require.Len(t, pt.observedPoints, 2)
	assert.Equal(t, uint64(20), pt.observedPoints[1].Slot)
	assert.Equal(t, []byte("h20"), pt.observedPoints[1].Hash)

	// A subsequent point extends both frontiers consistently.
	pt.recordObservedPoint(ocommon.Point{Slot: 40, Hash: []byte("h40")}, window)
	require.Equal(t, []uint64{10, 20, 40}, pt.observedSlots)
	require.Len(t, pt.observedPoints, 3)
	assert.Equal(t, []byte("h40"), pt.observedPoints[2].Hash)
}

// sharesObservedPoint requires an identical slot AND hash; a peer on a divergent
// fork (same slots, different hashes) does not corroborate.
func TestSharesObservedPointHashSensitive(t *testing.T) {
	window := uint64(100)
	honestA := &PeerChainTip{}
	honestB := &PeerChainTip{}
	divergent := &PeerChainTip{}
	for _, p := range []ocommon.Point{
		{Slot: 10, Hash: []byte("h10")},
		{Slot: 20, Hash: []byte("h20")},
	} {
		honestA.recordObservedPoint(p, window)
		honestB.recordObservedPoint(p, window)
	}
	for _, p := range []ocommon.Point{
		{Slot: 10, Hash: []byte("x10")},
		{Slot: 20, Hash: []byte("x20")},
	} {
		divergent.recordObservedPoint(p, window)
	}

	assert.True(t, honestA.sharesObservedPoint(honestB))
	assert.False(t, honestA.sharesObservedPoint(divergent))
	assert.False(t, divergent.sharesObservedPoint(honestB))
}

// A dense fast source whose recent blocks are also reported by other eligible
// peers (shared block hashes at shared slots) is corroborated, so it is
// selected as the best chain in Genesis mode.
func TestGenesisHonestFastSourceIsCorroborated(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	fast := newTestConnectionId(1)
	corroboratorA := newTestConnectionId(2)
	corroboratorB := newTestConnectionId(3)

	// Fast source: three blocks in the window (densest).
	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)
	// Corroborators are on the same chain: they report the SAME hashes at the
	// slots they have both seen, just fewer blocks (they are slower sources).
	feedFrontier(cs, corroboratorA,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)
	feedFrontier(cs, corroboratorB,
		genesisTip(100, "h100", 100),
	)

	cs.EvaluateAndSwitch()

	best := cs.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(t, fast, *best, "corroborated dense fast source must be selected")
}

// A dense but divergent fast source — one on a chain no other eligible peer
// reports — is NOT corroborated and must not be selected, even though it is
// the densest/longest. The node follows the corroborated honest peers instead
// and publishes a corroboration-failure event for the divergent source.
func TestGenesisDivergentFastSourceNotSelected(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	var mu sync.Mutex
	var failures []GenesisCorroborationFailedEvent
	bus.SubscribeFunc(
		GenesisCorroborationFailedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(GenesisCorroborationFailedEvent)
			if !ok {
				return
			}
			mu.Lock()
			failures = append(failures, e)
			mu.Unlock()
		},
	)

	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
		EventBus:              bus,
	})

	divergent := newTestConnectionId(1)
	honestA := newTestConnectionId(2)
	honestB := newTestConnectionId(3)

	// Divergent source: densest/longest, but on a private fork — its block
	// hashes ("x...") are seen by nobody else.
	feedFrontier(cs, divergent,
		genesisTip(100, "x100", 100),
		genesisTip(105, "x105", 105),
		genesisTip(110, "x110", 110),
	)
	// Honest peers agree with each other on the real chain ("h..."), corroborating
	// one another but never the divergent source.
	feedFrontier(cs, honestA,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)
	feedFrontier(cs, honestB,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)

	cs.EvaluateAndSwitch()

	best := cs.GetBestPeer()
	require.NotNil(t, best, "an honest corroborated peer should be selectable")
	assert.NotEqual(
		t,
		divergent,
		*best,
		"uncorroborated divergent fast source must not steer selection",
	)
	assert.Contains(t, []ouroboros.ConnectionId{honestA, honestB}, *best)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, f := range failures {
			if f.ConnectionId == divergent {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond,
		"expected corroboration-failure event for divergent source")
}

// With no corroborating peers available, a fast source stalls: the selector
// refuses to select it, returning no best peer, rather than following an
// uncorroborated chain.
func TestGenesisInsufficientCorroborationStalls(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	fast := newTestConnectionId(1)
	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)

	cs.EvaluateAndSwitch()
	assert.Nil(
		t,
		cs.GetBestPeer(),
		"a lone uncorroborated fast source must stall, not be selected",
	)
}

// Corroboration requires a quorum: when the configured minimum is 2 but only
// one other peer agrees, the fast source is still uncorroborated and stalls.
func TestGenesisCorroborationQuorumNotMet(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 2,
	})

	fast := newTestConnectionId(1)
	corroborator := newTestConnectionId(2)

	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)
	feedFrontier(cs, corroborator,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)

	cs.EvaluateAndSwitch()
	assert.Nil(
		t,
		cs.GetBestPeer(),
		"quorum of 2 not met with a single corroborator; must stall",
	)
}

// Corroboration is opt-in. With MinCorroboratingPeers == 0 (the default), a
// lone dense source is selected exactly as before, preserving backward
// compatibility for nodes that do not enable the Genesis trust model.
func TestGenesisCorroborationDisabledByDefault(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: 20,
	})

	fast := newTestConnectionId(1)
	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)

	cs.EvaluateAndSwitch()
	best := cs.GetBestPeer()
	require.NotNil(t, best)
	assert.Equal(t, fast, *best)
}

// GenesisStatus exposes selection mode, window, the selected fast source, and
// per-peer density/corroboration for observability.
func TestGenesisStatusObservability(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         10, // window = 30
		MinCorroboratingPeers: 1,
	})

	fast := newTestConnectionId(1)
	corroborator := newTestConnectionId(2)
	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)
	feedFrontier(cs, corroborator,
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)
	cs.EvaluateAndSwitch()

	status := cs.GenesisStatus()
	assert.Equal(t, SelectionModeGenesis, status.Mode)
	assert.Equal(t, uint64(30), status.WindowSlots)
	assert.Equal(t, 1, status.MinCorroboratingPeers)
	require.NotNil(t, status.BestSource)
	assert.Equal(t, fast, *status.BestSource)

	byConn := make(map[ouroboros.ConnectionId]GenesisPeerStatus, len(status.Peers))
	for _, ps := range status.Peers {
		byConn[ps.ConnectionId] = ps
	}
	fastStatus, ok := byConn[fast]
	require.True(t, ok)
	assert.Equal(t, uint64(3), fastStatus.ObservedDensity)
	assert.GreaterOrEqual(t, fastStatus.CorroboratingPeers, 1)
	assert.True(t, fastStatus.Corroborated)
}

// When the local tip catches up into the Genesis window of the best known
// peer, the selector exits Genesis mode and publishes an exit event carrying
// the reason (local/best slots and window).
func TestGenesisModeExitEmitsEvent(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	var mu sync.Mutex
	var exits []GenesisModeExitedEvent
	bus.SubscribeFunc(
		GenesisModeExitedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(GenesisModeExitedEvent)
			if !ok {
				return
			}
			mu.Lock()
			exits = append(exits, e)
			mu.Unlock()
		},
	)

	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: 10, // window = 30
		EventBus:      bus,
	})

	connId := newTestConnectionId(1)
	cs.UpdatePeerTip(connId, genesisTip(100, "peer", 100), nil)
	require.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	// Local tip within window (100 - 30 = 70) triggers exit.
	cs.SetLocalTip(genesisTip(75, "local-75", 75))
	require.Equal(t, SelectionModePraos, cs.SelectionMode())

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(exits) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, uint64(75), exits[0].LocalSlot)
	assert.Equal(t, uint64(100), exits[0].BestKnownSlot)
	assert.Equal(t, uint64(30), exits[0].GenesisWindowSlots)
}
