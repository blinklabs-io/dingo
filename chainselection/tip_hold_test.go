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

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
