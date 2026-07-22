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

// corrConn builds a connection id with a distinct remote HOST per n, so peers
// count as independent corroboration witnesses (which dedup by remote host).
func corrConn(n int) ouroboros.ConnectionId {
	return corrConnAddr(fmt.Sprintf("10.0.0.%d", n), 3001)
}

// corrConnAddr builds a connection id for an explicit remote host and port,
// used to model several connections sharing one host (a Sybil source).
func corrConnAddr(host string, port int) ouroboros.ConnectionId {
	localAddr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	remoteAddr, _ := net.ResolveTCPAddr(
		"tcp",
		fmt.Sprintf("%s:%d", host, port),
	)
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

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

// feedPoints records a sequence of points into a bare PeerChainTip with hash
// tracking enabled.
func feedPoints(pt *PeerChainTip, window uint64, points ...ocommon.Point) {
	for _, p := range points {
		pt.recordObservedPoint(p, window, true)
	}
}

// recordObservedPoint keeps the slot frontier and the point (slot+hash)
// frontier in lockstep, storing the current hash at each slot.
func TestRecordObservedPointTracksHashFrontier(t *testing.T) {
	pt := &PeerChainTip{}
	window := uint64(100)
	feedPoints(pt, window,
		ocommon.Point{Slot: 10, Hash: []byte("h10")},
		ocommon.Point{Slot: 20, Hash: []byte("h20")},
		ocommon.Point{Slot: 30, Hash: []byte("h30")},
	)

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
	pt.recordObservedPoint(
		ocommon.Point{Slot: 30, Hash: []byte("h30b")},
		window,
		true,
	)
	require.Equal(t, []uint64{10, 20, 30}, pt.observedSlots)
	require.Len(t, pt.observedPoints, 3)
	assert.Equal(t, []byte("h30b"), pt.observedPoints[2].Hash)
}

// With hash tracking disabled (corroboration inactive) only the slot frontier
// is kept; the hash frontier is not retained.
func TestRecordObservedPointSkipsHashesWhenInactive(t *testing.T) {
	pt := &PeerChainTip{}
	window := uint64(100)
	for _, p := range []ocommon.Point{
		{Slot: 10, Hash: []byte("h10")},
		{Slot: 20, Hash: []byte("h20")},
	} {
		pt.recordObservedPoint(p, window, false)
	}
	assert.Equal(t, []uint64{10, 20}, pt.observedSlots)
	assert.Empty(t, pt.observedPoints)

	// Enabling tracking starts populating the hash frontier again; disabling
	// clears it.
	pt.recordObservedPoint(ocommon.Point{Slot: 30, Hash: []byte("h30")}, window, true)
	assert.Len(t, pt.observedPoints, 1)
	pt.recordObservedPoint(ocommon.Point{Slot: 40, Hash: []byte("h40")}, window, false)
	assert.Empty(t, pt.observedPoints)
}

// ApplyRollback trims the slot and point frontiers together so they stay
// aligned after a rollback.
func TestApplyRollbackTrimsPointFrontier(t *testing.T) {
	pt := &PeerChainTip{}
	window := uint64(100)
	feedPoints(pt, window,
		ocommon.Point{Slot: 10, Hash: []byte("h10")},
		ocommon.Point{Slot: 20, Hash: []byte("h20")},
		ocommon.Point{Slot: 30, Hash: []byte("h30")},
	)

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
	pt.recordObservedPoint(ocommon.Point{Slot: 40, Hash: []byte("h40")}, window, true)
	require.Equal(t, []uint64{10, 20, 40}, pt.observedSlots)
	require.Len(t, pt.observedPoints, 3)
	assert.Equal(t, []byte("h40"), pt.observedPoints[2].Hash)
}

// confirmsRecentChain requires the witness's chain, as far as it reaches into
// the candidate's window, to match the candidate: same-chain peers confirm each
// other; a peer on a divergent fork does not.
func TestConfirmsRecentChainHashSensitive(t *testing.T) {
	window := uint64(100)
	honestA := &PeerChainTip{}
	honestB := &PeerChainTip{}
	divergent := &PeerChainTip{}
	feedPoints(honestA, window,
		ocommon.Point{Slot: 10, Hash: []byte("h10")},
		ocommon.Point{Slot: 20, Hash: []byte("h20")},
	)
	feedPoints(honestB, window,
		ocommon.Point{Slot: 10, Hash: []byte("h10")},
		ocommon.Point{Slot: 20, Hash: []byte("h20")},
	)
	feedPoints(divergent, window,
		ocommon.Point{Slot: 10, Hash: []byte("x10")},
		ocommon.Point{Slot: 20, Hash: []byte("x20")},
	)

	assert.True(t, honestA.confirmsRecentChain(honestB))
	assert.False(t, honestA.confirmsRecentChain(divergent))
	assert.False(t, divergent.confirmsRecentChain(honestB))
}

// Regression for the "shared ancestor then diverge" bypass: a fast source that
// agrees on one old block but then produces different blocks for the rest of the
// window is NOT confirmed by an honest witness, because the witness observed
// recent blocks the candidate does not have.
func TestConfirmsRecentChainRejectsSharedAncestorThenDiverge(t *testing.T) {
	window := uint64(100)
	fast := &PeerChainTip{}
	honest := &PeerChainTip{}
	// Both agree on slot 100, then the fast source forks (x...) while the
	// honest witness continues on the real chain (h...).
	feedPoints(fast, window,
		ocommon.Point{Slot: 100, Hash: []byte("shared-100")},
		ocommon.Point{Slot: 105, Hash: []byte("x105")},
		ocommon.Point{Slot: 110, Hash: []byte("x110")},
	)
	feedPoints(honest, window,
		ocommon.Point{Slot: 100, Hash: []byte("shared-100")},
		ocommon.Point{Slot: 106, Hash: []byte("h106")},
		ocommon.Point{Slot: 111, Hash: []byte("h111")},
	)

	assert.False(t, fast.confirmsRecentChain(honest),
		"a witness that diverges after the shared ancestor must not confirm")
}

// A witness whose frontier does not overlap the candidate's window cannot
// confirm it (fail-closed).
func TestConfirmsRecentChainRequiresOverlap(t *testing.T) {
	window := uint64(20)
	ahead := &PeerChainTip{}
	behind := &PeerChainTip{}
	feedPoints(ahead, window,
		ocommon.Point{Slot: 200, Hash: []byte("h200")},
		ocommon.Point{Slot: 210, Hash: []byte("h210")},
	)
	feedPoints(behind, window,
		ocommon.Point{Slot: 100, Hash: []byte("h100")},
		ocommon.Point{Slot: 105, Hash: []byte("h105")},
	)
	assert.False(t, ahead.confirmsRecentChain(behind))
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

	fast := corrConn(1)
	corroboratorA := corrConn(2)
	corroboratorB := corrConn(3)

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

	divergent := corrConn(1)
	honestA := corrConn(2)
	honestB := corrConn(3)

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

// In a realistic from-origin ChainSync flow a peer advertises a far-ahead tip
// (slot 100000) but has only delivered early headers (slot 1). Genesis-mode exit
// must key off the advertised network tip, not the observed frontier: otherwise
// the node leaves Genesis mode — and disables the corroboration gate — as soon
// as two peers corroborate an early header, long before the local tip is near
// the network tip.
func TestGenesisDoesNotExitOnEarlyObservedHeaders(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         10, // window = 30
		MinCorroboratingPeers: 1,
	})
	require.Equal(t, uint64(30), cs.GenesisWindowSlots())

	peerA := corrConn(1)
	peerB := corrConn(2)
	// Both peers advertise slot 100000 throughout; only their DELIVERED
	// (observed) frontier advances as sync progresses.
	advertised := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100000, Hash: []byte("net-tip")},
		BlockNumber: 100000,
	}
	deliver := func(slot uint64) {
		obs := ochainsync.Tip{
			Point: ocommon.Point{
				Slot: slot,
				Hash: []byte(fmt.Sprintf("h%d", slot)),
			},
			BlockNumber: slot,
		}
		require.True(t, cs.updatePeerTipObserved(peerA, advertised, obs, nil))
		require.True(t, cs.updatePeerTipObserved(peerB, advertised, obs, nil))
	}

	// Both peers advertise slot 100000 but have only delivered the slot-1
	// header. The far advertised tip has not been delivered, so the node must
	// stay in Genesis (no premature exit on early observed headers).
	deliver(1)
	assert.Equal(t, SelectionModeGenesis, cs.SelectionMode(),
		"must not exit Genesis on early observed headers")

	// Sync progresses: headers are delivered up to slot 50000 and the local tip
	// follows. Still far below the advertised tip, so stay in Genesis.
	deliver(50000)
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 50000, Hash: []byte("local-50000")},
		BlockNumber: 50000,
	})
	assert.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	// Only once the peers have DELIVERED headers up to within the window (30) of
	// their advertised tip — and the local tip has caught up — does the node
	// exit to Praos.
	deliver(99990)
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 99990, Hash: []byte("local-99990")},
		BlockNumber: 99990,
	})
	assert.Equal(t, SelectionModePraos, cs.SelectionMode())
}

// A corroborated peer can still lie about its advertised tip: it delivers a
// shared early header (passing corroboration) but advertises a slot near
// MaxUint64 it has not delivered. That undelivered advertised slot must not pin
// Genesis mode — the exit horizon is bound to delivered headers, so the node
// still exits once the local tip catches up to the delivered/corroborated tip.
func TestGenesisExitNotPinnedByCorroboratedLiarAdvertisingFarTip(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         10, // window = 30
		MinCorroboratingPeers: 1,
	})

	honest := corrConn(1)
	liar := corrConn(2)
	// Both deliver the same header at slot 100, so they corroborate each other.
	sharedObs := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("h100")},
		BlockNumber: 100,
	}
	// Honest advertises what it has delivered.
	require.True(t, cs.updatePeerTipObserved(
		honest,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("honest-tip")},
			BlockNumber: 100,
		},
		sharedObs,
		nil,
	))
	// Liar is corroborated on the shared slot-100 header but advertises an
	// extreme tip it has NOT delivered.
	const extremeSlot = uint64(1) << 62
	require.True(t, cs.updatePeerTipObserved(
		liar,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: extremeSlot, Hash: []byte("liar-tip")},
			BlockNumber: 100,
		},
		sharedObs,
		nil,
	))

	// Local catches up to the delivered/corroborated tip (slot 100).
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("local-100")},
		BlockNumber: 100,
	})

	// The liar's undelivered extreme advertised slot must not pin Genesis mode.
	assert.Equal(t, SelectionModePraos, cs.SelectionMode(),
		"a corroborated peer's undelivered advertised slot must not pin Genesis")
}

// The Genesis exit horizon must be bounded so a single uncorroborated peer
// cannot pin Genesis mode indefinitely. The advertised slot is untrusted and the
// implausible-tip check bounds only the advertised block number, so a peer can
// advertise a plausible block with a near-MaxUint64 slot. Because the horizon is
// built from corroborated peers only, that extreme slot is ignored and the node
// still exits once the local tip catches up to the corroborated network tip.
func TestGenesisExitNotPinnedByUncorroboratedExtremeSlot(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         10, // window = 30
		MinCorroboratingPeers: 1,
	})

	honestA := corrConn(1)
	honestB := corrConn(2)
	liar := corrConn(3)

	// Honest peers on the real chain at slot 200 / block 200, corroborating each
	// other.
	honestAdv := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200, Hash: []byte("real-tip")},
		BlockNumber: 200,
	}
	honestObs := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200, Hash: []byte("real-hdr")},
		BlockNumber: 200,
	}
	require.True(t, cs.updatePeerTipObserved(honestA, honestAdv, honestObs, nil))
	require.True(t, cs.updatePeerTipObserved(honestB, honestAdv, honestObs, nil))

	// Uncorroborated liar: a plausible block number (200, within K of the honest
	// peers) but an extreme advertised slot that nothing corroborates.
	const extremeSlot = uint64(1) << 62
	require.True(t, cs.updatePeerTipObserved(
		liar,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: extremeSlot, Hash: []byte("liar-tip")},
			BlockNumber: 200,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: extremeSlot, Hash: []byte("liar-hdr")},
			BlockNumber: 200,
		},
		nil,
	))

	// Local tip catches up to the corroborated honest tip (slot 200).
	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 200, Hash: []byte("local-200")},
		BlockNumber: 200,
	})

	// The liar's extreme advertised slot must NOT pin Genesis mode: the horizon
	// is the corroborated tip (200), so the node exits to Praos.
	assert.Equal(t, SelectionModePraos, cs.SelectionMode(),
		"an uncorroborated extreme advertised slot must not pin Genesis mode")
}

// Removing the only witness revokes the incumbent fast source's corroboration
// and forces a re-evaluation that drops it, even though the removed peer was not
// itself the selected best.
func TestGenesisCorroborationRevokedOnWitnessRemoval(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	fast := corrConn(1)
	witness := corrConn(2)
	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)
	feedFrontier(cs, witness,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, fast, *cs.GetBestPeer())

	// The witness disconnects; the fast source loses its only corroboration.
	// RemovePeer itself must re-evaluate here (the removed peer is a witness,
	// not the best), so GetBestPeer reflects the drop with no extra
	// EvaluateAndSwitch call — that internal re-evaluation is exactly the
	// behavior under test.
	cs.RemovePeer(witness)
	assert.Nil(
		t,
		cs.GetBestPeer(),
		"incumbent must be dropped once it loses corroboration",
	)
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

	fast := corrConn(1)
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

	fast := corrConn(1)
	corroborator := corrConn(2)

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

// Several connections from the SAME remote host cannot corroborate each other:
// witnesses are de-duplicated by host, so a Sybil fast source opening multiple
// connections still lacks independent corroboration and stalls.
func TestGenesisCorroborationDedupsBySybilHost(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	// Fast source and two "corroborators" that are really the same operator
	// host (10.9.9.9), just different ports.
	fast := corrConnAddr("10.9.9.9", 3001)
	sybilA := corrConnAddr("10.9.9.9", 3002)
	sybilB := corrConnAddr("10.9.9.9", 3003)
	for _, conn := range []ouroboros.ConnectionId{fast, sybilA, sybilB} {
		feedFrontier(cs, conn,
			genesisTip(100, "h100", 100),
			genesisTip(105, "h105", 105),
			genesisTip(110, "h110", 110),
		)
	}

	cs.EvaluateAndSwitch()
	assert.Nil(
		t,
		cs.GetBestPeer(),
		"same-host connections must not self-corroborate a Sybil fast source",
	)

	// Add a genuinely independent host on the same chain; now corroboration
	// succeeds.
	independent := corrConnAddr("10.0.0.42", 3001)
	feedFrontier(cs, independent,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer(),
		"an independent-host corroborator should satisfy the gate")
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

	fast := corrConn(1)
	corroborator := corrConn(2)
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
