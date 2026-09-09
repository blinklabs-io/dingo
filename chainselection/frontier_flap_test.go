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
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

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

// chainSwitchDrainSentinelConnId names a connection no scenario in this file
// uses. drainChainSwitchEventsSynced publishes a ChainSwitchEvent carrying it
// through the same ordered lane the selector publishes on, to detect (by
// receiving it) that every event enqueued earlier has already been delivered.
var chainSwitchDrainSentinelConnId = newTestConnectionId(50000)

// drainChainSwitchEventsSynced collects every ChainSwitchEvent the scenario
// published so far, blocking until it is certain none remain in flight.
//
// The selector publishes ChainSwitchEvent through EventBus.PublishOrdered,
// which only enqueues onto the event type's lane before returning -- a
// dedicated goroutine (event.orderedWorker) delivers to subscribers later, on
// its own schedule (see event/ordered.go). A non-blocking read of the
// subscriber channel immediately after the driving calls return can therefore
// observe zero events even though the selector already decided to publish
// one: nothing has forced the delivery goroutine to run yet. That assumption
// is exactly what made this scenario's regression test flake under
// full-package parallelism, where CPU contention widens the gap
// between "enqueued" and "delivered" (blinklabs-io/dingo#4145).
//
// The fix is a handshake, not a wait: publish a sentinel ChainSwitchEvent
// through the same ordered lane after the driving calls return. The lane has
// exactly one worker draining it in FIFO order (event.orderedWorker), so the
// sentinel cannot be delivered ahead of any real event already enqueued
// before it. Receiving the sentinel is therefore proof, not a guess, that
// every earlier event has already reached the subscriber channel.
func drainChainSwitchEventsSynced(
	t *testing.T,
	eventBus *event.EventBus,
	ch <-chan event.Event,
) []ChainSwitchEvent {
	t.Helper()
	require.True(
		t,
		eventBus.PublishOrdered(
			ChainSwitchEventType,
			event.NewEvent(
				ChainSwitchEventType,
				ChainSwitchEvent{
					NewConnectionId: chainSwitchDrainSentinelConnId,
				},
			),
		),
		"sentinel publish must be accepted",
	)
	var out []ChainSwitchEvent
	for {
		evt := testutil.RequireReceive(
			t,
			ch,
			event.RemoteDeliverTimeout,
			"chain switch event drain",
		)
		switchEvent, ok := evt.Data.(ChainSwitchEvent)
		require.True(t, ok, "unexpected event payload on switch channel")
		if switchEvent.NewConnectionId == chainSwitchDrainSentinelConnId {
			return out
		}
		out = append(out, switchEvent)
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

	switchEvents := drainChainSwitchEventsSynced(t, eventBus, switchCh)
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
