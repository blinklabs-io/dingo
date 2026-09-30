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
// normal multi-peer selection (blinklabs-io/dingo#4413): an incumbent peer
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

// These tests pin the transport-frontier flapping reproduction from Preview
// from-genesis validation: two canonical public roots advertise the SAME chain
// tip while their locally delivered frontiers differ by more than k, purely
// because one has served us more headers than the other. A delivered-frontier
// difference between peers that agree on where the chain ends is a
// transport-delivery artifact, not a chain-quality difference, and must not
// drive the active-connection handoff.

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
// EventBus.PublishOrdered (blinklabs-io/dingo#3550), so the call that drove
// the decision returns before the lane worker has handed the event to any
// subscriber. A lane is a FIFO drained by exactly one worker, so a sentinel
// enqueued after those switches is delivered after them: receiving it back is
// proof that every switch published earlier on this goroutine has already
// reached the subscription. Its Data type is not ChainSwitchEvent, so it is
// skipped rather than counted as a decision. Same construction as
// switchBarrier in ouroboros/consensus_conformance_test.go.
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
	return cs.isPeerSelectableLocked(connId, peerTip, false)
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
// blinklabs-io/dingo#3550 Preview run the 1024-slot buffer took 12h31m of
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
// blinklabs-io/dingo#3550 shows the opposite: the handler stopped returning,
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

// This file pins the fix for the live Preview-testnet chain-switch storm:
// near the real chain tip, two peer connections repeatedly leapfrogged each
// other by a small margin, and the anti-flap pin's longer-chain escape
// (pinIncumbentDuringCatchUpLocked) handed the active connection back and
// forth between them multiple times per second with no net progress. Each
// hand-off is expensive downstream (ledger.handleChainSwitchEvent takes
// chainsyncMutex and chainsyncBlockfetchMutex and can request a fresh
// chainsync cursor), so an unbounded switch rate backed up the event bus.
//
// The longer-chain escape itself is correct and load-bearing: a challenger
// genuinely more than catchUpPinHeadMargin ahead must still be adopted (see
// TestPinSwitchesToGenuinelyLongerChainAtTip in tip_hold_test.go), so the fix
// does not touch that comparison. Instead it rate-limits repeated hand-offs
// between the same small set of connections: a peer the active connection
// just moved away from cannot reclaim it via the longer-chain escape again
// until SwitchBackCooldown has passed, even if it currently leads by more
// than the margin. A genuinely new challenger, or the same peer once the
// cooldown has elapsed, is never debounced.

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
