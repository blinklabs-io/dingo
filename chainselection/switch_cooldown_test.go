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

// TestSwitchBackCooldownDoesNotBlockGenuinelyNewChallenger asserts the
// debounce is keyed per-connection: a third peer that was never the active
// connection is adopted immediately even while the original incumbent is
// still within its own cooldown window.
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
	// the margin. It must be adopted immediately: it was never the
	// abandoned incumbent, so it is never debounced.
	require.True(t, cs.updatePeerTipObserved(
		peerC,
		tip(5001+2*(catchUpPinHeadMargin+1), 5020, "c-0"),
		tip(5001+2*(catchUpPinHeadMargin+1), 5020, "c-0"),
		nil,
	))
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, peerC, *cs.GetBestPeer(),
		"a peer that was never the active connection must not be debounced")
}
