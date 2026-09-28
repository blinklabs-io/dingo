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
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
