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

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gsmJudgementOnly runs the Genesis state machine for its caught-up judgement
// alone, as a node that selects chains by Praos rules does: the selection mode
// never leaves Praos.
func gsmJudgementOnly(_ *gsmFixture, cfg *ChainSelectorConfig) {
	cfg.GenesisMode = false
	cfg.GenesisStateMachine = false
}

// gsmReachCaughtUp applies h1..h5 and lets one peer idle at h5.
func gsmReachCaughtUp(
	t *testing.T,
	f *gsmFixture,
) ouroboros.ConnectionId {
	t.Helper()
	peer := corrConn(90)
	f.applyRange(1, 5)
	f.deliver(t, peer, 1, 5)
	f.idle(peer, gsmTip(5))
	return peer
}

func TestCaughtUpSelectorInGenesisModeIsSyncing(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t)
	assert.False(t, f.cs.CaughtUp())
	f.exitToPraos(t)
	assert.True(t, f.cs.CaughtUp())
}

func TestCaughtUpJudgementStartsSyncing(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	assert.False(t, f.cs.CaughtUp(), "no connection has been heard from")
	f.applyRange(1, 5)
	assert.False(t, f.cs.CaughtUp(), "no connection has been heard from")
	assert.Equal(t, SelectionModePraos, f.mode())
}

func TestCaughtUpJudgementNeedsEveryPeerIdle(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	idle, streaming := corrConn(90), corrConn(91)
	f.applyRange(1, 5)
	f.deliver(t, idle, 1, 5)
	f.deliver(t, streaming, 1, 9)
	f.idle(idle, gsmTip(5))
	assert.False(t, f.cs.CaughtUp(), "a peer that still streams blocks it")
	f.cs.RemovePeer(streaming)
	assert.True(t, f.cs.CaughtUp())
}

func TestCaughtUpJudgementNeedsNoBetterCandidate(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	peer := corrConn(90)
	f.applyRange(1, 5)
	f.deliver(t, peer, 1, 9)
	f.idle(peer, gsmTip(9))
	assert.False(t, f.cs.CaughtUp(),
		"a peer idle at a tip the node has not applied is a better candidate")
	f.applyRange(6, 9)
	assert.True(t, f.cs.CaughtUp())
}

func TestCaughtUpJudgementLeavesTheSelectionModeAlone(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	assert.Equal(t, SelectionModePraos, f.mode())
	active, _ := f.cs.GenesisSelectionState()
	assert.False(t, active)
}

// An idle peer at a tip too old for an honest caught-up peer is a Genesis
// disconnection rule; a selector that does not select by Genesis does not
// disconnect it.
func TestCaughtUpJudgementDisconnectsNoOne(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	gsmReachCaughtUp(t, f)
	f.setNow(gsmEpoch.Add(5*f.slotLen + DefaultHistoricityCutoff(30) +
		time.Minute))
	require.True(t, f.cs.CaughtUp())
	assert.Empty(t, f.historicConns())
}

func TestCaughtUpJudgementPublishesNoGenesisModeEvents(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, exited := bus.Subscribe(GenesisModeExitedEventType)
	_, entered := bus.Subscribe(GenesisModeEnteredEventType)
	f := newGSMFixture(t, gsmJudgementOnly, func(
		_ *gsmFixture,
		cfg *ChainSelectorConfig,
	) {
		cfg.EventBus = bus
	})
	gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	f.setNow(gsmEpoch.Add(24 * time.Hour))
	require.False(t, f.cs.CaughtUp())
	assert.Empty(t, exited)
	assert.Empty(t, entered)
}

// The state stays caught up while the peers stream again: a block arriving
// does not return the node to syncing.
func TestCaughtUpJudgementLatches(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	peer := gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	f.cs.ClearPeerIdling(peer, gsmTip(6).Point)
	f.deliver(t, peer, 6, 6)
	assert.True(t, f.cs.CaughtUp(), "a peer streaming a block is not syncing")
}

// The node stays caught up for the maximum caught-up age whatever its tip's
// age, then returns to syncing once the tip is too old.
func TestCaughtUpJudgementMinimumStayThenTooOld(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	minStay := f.now.Add(DefaultMaxCaughtUpAge)

	f.setNow(minStay.Add(-time.Second))
	assert.True(t, f.cs.CaughtUp(), "inside the minimum stay")
	f.setNow(minStay)
	assert.False(t, f.cs.CaughtUp(),
		"the tip is older than the maximum caught-up age")
	assert.Equal(t, SelectionModePraos, f.mode())
}

func TestCaughtUpJudgementTooOldAtDeadlinePlusBonus(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	f.slotLen = 4 * time.Minute
	gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	minStay := f.now.Add(DefaultMaxCaughtUpAge)
	deadline := gsmEpoch.Add(5*f.slotLen + DefaultMaxCaughtUpAge +
		120*time.Second)

	f.setNow(minStay)
	assert.True(t, f.cs.CaughtUp(), "the tip is still young at minimum stay")
	f.setNow(deadline.Add(-time.Second))
	assert.True(t, f.cs.CaughtUp())
	f.setNow(deadline)
	assert.False(t, f.cs.CaughtUp())
}

// A selection that moves on restarts the age measurement.
func TestCaughtUpJudgementTipChangeMovesDeadline(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	f.slotLen = 4 * time.Minute
	gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	minStay := f.now.Add(DefaultMaxCaughtUpAge)
	f.setNow(minStay)
	require.True(t, f.cs.CaughtUp())

	f.applyRange(6, 6)
	oldDeadline := gsmEpoch.Add(5*f.slotLen + DefaultMaxCaughtUpAge +
		120*time.Second)
	f.setNow(oldDeadline)
	assert.True(t, f.cs.CaughtUp(), "the newer tip has its own deadline")
	f.setNow(oldDeadline.Add(f.slotLen))
	assert.False(t, f.cs.CaughtUp())
}

// Syncing again, the judgement waits for the peers to be idle and the
// selection to be young once more.
func TestCaughtUpJudgementReturnsToCaughtUp(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	gsmReachCaughtUp(t, f)
	require.True(t, f.cs.CaughtUp())
	f.setNow(f.now.Add(DefaultMaxCaughtUpAge))
	require.False(t, f.cs.CaughtUp())
	assert.True(t, f.cs.CaughtUp(),
		"the peers are still idle at the selection, which ends syncing")
}

func TestCaughtUpJudgementWithoutSlotTimeIsSyncing(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly, func(
		_ *gsmFixture,
		cfg *ChainSelectorConfig,
	) {
		cfg.SlotTime = nil
	})
	gsmReachCaughtUp(t, f)
	assert.False(t, f.cs.CaughtUp(), "the age of the tip cannot be told")
}

// A block producer whose only peer is its relay has just adopted a block it
// forged: the relay is still idle at the parent, which is on the selection.
func TestCaughtUpJudgementRelayIdleAtTheParentOfAForgedBlock(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	relay := corrConn(90)
	f.applyRange(1, 5)
	f.deliver(t, relay, 1, 5)
	f.idle(relay, gsmTip(5))
	f.apply(gsmTip(6))
	assert.True(t, f.cs.CaughtUp())
}

// A relay idle on a one-block fork keeps the judgement syncing only until it
// adopts the selection; once caught up, a later fork does not undo it.
func TestCaughtUpJudgementRelayOnAShortForkDelaysOnly(t *testing.T) {
	t.Parallel()
	f := newGSMFixture(t, gsmJudgementOnly)
	relay := corrConn(90)
	fork := genesisTip(6, "f6", 6)
	f.applyRange(1, 6)
	f.deliver(t, relay, 1, 5)
	require.True(t, f.cs.UpdatePeerTip(relay, fork, nil))
	f.idle(relay, fork)
	assert.False(t, f.cs.CaughtUp(),
		"an idle peer off the selection counts as a better candidate")

	f.cs.HandlePeerRollbackEvent(event.Event{Data: PeerRollbackEvent{
		ConnectionId: relay,
		Point:        gsmTip(5).Point,
		Tip:          gsmTip(6),
	}})
	require.True(t, f.cs.UpdatePeerTip(relay, gsmTip(6), nil))
	f.idle(relay, gsmTip(6))
	require.True(t, f.cs.CaughtUp(), "the relay follows the selection")

	f.apply(gsmTip(7))
	fork = genesisTip(8, "f8", 8)
	require.True(t, f.cs.UpdatePeerTip(relay, gsmTip(7), nil))
	require.True(t, f.cs.UpdatePeerTip(relay, fork, nil))
	f.idle(relay, fork)
	assert.True(t, f.cs.CaughtUp(), "the judgement is latched")
}
