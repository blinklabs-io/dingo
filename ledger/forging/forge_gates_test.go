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

package forging

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newGateTestForger builds a production forger over an explicit slot
// clock so a test can place the chain tip ahead of, at, or behind the
// current slot and observe which pre-leader gate in
// checkAndForgeProduction fires.
func newGateTestForger(
	t *testing.T,
	clock forgerTestSlotClock,
	leader LeaderChecker,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	fence ForgeFenceStore,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestCheckAndForgeProductionEqualSlotReachesLeaderCheck pins the EQ
// half of the tip gate. A rival pool forging the same slot and winning
// the propagation race puts the chain tip AT the current slot, not past
// it. ouroboros-consensus treats that as a contested slot rather than a
// reason to stop: mkCurrentBlockContext declines only for GT
// (TraceBlockFromFuture) and, for EQ, forges an alternative to the block
// at the tip -- same block number, the tip's predecessor as parent -- so
// the leader VRF and chain selection arbitrate.
//
// Dingo folded EQ into the GT skip with `currentSlot <= tipSlot`, which
// returns before checkLeaderSafe. The slot never reached leader
// selection, so no leadership counter moved and the only trace was a
// Debug line.
//
// With the alternative path wired, the contested slot must reach leader
// selection, be counted as a leader slot and a slot battle, and produce
// a block built on the tip's predecessor rather than on the tip.
// TestCheckAndForgeProductionEqualSlotDeclinesWithoutTheAlternativePath
// covers the unwired case, which still declines and still accounts for
// the battle.
func TestCheckAndForgeProductionEqualSlotReachesLeaderCheck(t *testing.T) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	broadcaster := &forgerTestBroadcaster{}
	adopter := &forgerTestSiblingAdopter{adopted: true}
	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		broadcaster,
		nil,
		newAltTestChainContext(10),
		adopter,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(
		t,
		1,
		leader.callCount(),
		"a tip at the current slot is a contested slot, not a reason "+
			"to skip leader selection",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeNodeIsLeader),
		"the contested slot must be counted as a leader slot",
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a rival block occupying our leader slot is a slot battle",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a contested slot we did forge for is not a could-not-forge",
	)

	// The block must be built on the tip's PREDECESSOR. Binding the live
	// tip -- the rival's block at this same slot -- would carry a parent
	// whose slot equals its own and be rejected by
	// ledger.validateBlockOrder and by every Praos peer.
	require.Equal(
		t,
		1,
		builder.contextCalls,
		"a contested slot must build on an explicit block context",
	)
	assert.Zero(
		t,
		builder.calls,
		"must not bind a same-slot parent",
	)
	assert.Equal(
		t,
		altTestParentPoint(),
		builder.blockCtx.Parent,
		"the alternative's parent is the tip's predecessor",
	)
	assert.Equal(
		t,
		altTestRivalBlockNumber,
		builder.blockCtx.BlockNumber,
		"the alternative carries the rival's block number, not one past it",
	)
	assert.Equal(
		t,
		altTestRivalTip(10),
		builder.blockCtx.Rival,
		"the alternative names the tip it competes with",
	)

	// It goes to chain selection, not to the extend-only add path.
	assert.Equal(t, 1, adopter.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeForged),
	)
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeAdopted),
	)
}

// TestCheckAndForgeProductionEqualSlotDoesNotReForgeOurOwnSlot is the// TestCheckAndForgeProductionEqualSlotDoesNotReForgeOurOwnSlot is the
// anti-equivocation companion to the test above. The slot-aligned loop
// can re-enter a slot it already committed to (a clock that has not
// advanced, or a NextSlotTime already in the past), and after a
// successful forge the chain tip is our OWN block at that slot, so
// tipSlot == currentSlot there too.
//
// That case must stay a quiet skip: it is not a slot battle, it must
// not build a second block for a slot this node already signed for, and
// it must not report a could-not-forge for a slot that was in fact
// forged.
func TestCheckAndForgeProductionEqualSlotDoesNotReForgeOurOwnSlot(
	t *testing.T,
) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
	// The fence records slot 10 as already committed to by this node,
	// which is what "the block at the tip is ours" means before the
	// block has been adopted.
	fence := &fenceTestStore{slot: 10, present: true}
	forger := newGateTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		broadcaster,
		fence,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Zero(t, builder.calls, "must not forge a second block")
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, fence.stored, "must not advance the fence")
	assert.Zero(
		t,
		leader.callCount(),
		"our own block at our own slot needs no leader selection",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"our own block is not a rival",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot we forged must not report could-not-forge",
	)
}

// TestForgeSkipsLeaderSlotWhenUpstreamTargetUnknownEvenAtTip documents
// the cost of the upstream-sync gate's `upstreamTip == 0` disjunct on a
// node that is NOT behind.
//
// LedgerState.UpstreamSyncStatus returns (0, true) for the whole window
// between an active-connection switch and the new peer's first
// authenticated, admitted, trusted header: publishActiveUpstream stores
// the new connection key with targetSlot zero, and only
// publishAdmittedUpstreamTarget makes it non-zero. A best-peer switch
// therefore disables forging outright, independent of how fresh the
// local tip is and independent of forgeSyncToleranceSlots.
//
// The clock below is a healthy steady-state producer: the chain tip is
// the previous slot's block, i.e. the node is at tip. It is scheduled to
// lead the current slot. It forges nothing, and the only counter that
// moves is dingo_forge_sync_skip_total, which is shared with the
// genuinely-behind case, so the lost leader slot is not recoverable from
// /metrics.
//
// This test asserts the CURRENT behaviour, which is deliberate and is
// already pinned by
// TestCheckAndForgeProductionWaitsForUnknownActiveUpstreamTarget. It is
// written to make the trade-off concrete and reviewable rather than to
// change it; the fix is a separate discussion (see the linked issue).
// If the gate is later keyed on local tip freshness instead of on the
// upstream target being known, this test fails and names the decision
// that was revisited.
func TestForgeSkipsLeaderSlotWhenUpstreamTargetUnknownEvenAtTip(
	t *testing.T,
) {
	for _, tc := range []struct {
		name      string
		tolerance uint64
	}{
		// The tolerance is irrelevant to this branch of the gate: the
		// `upstreamTip == 0` disjunct is not compared against it.
		{name: "default tolerance", tolerance: 0},
		{name: "tolerance far wider than the lag", tolerance: 100000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leader := &forgerCountingLeader{}
			builder := &forgerTestBuilder{}
			broadcaster := &forgerTestBroadcaster{}
			forger, err := NewBlockForger(ForgerConfig{
				Mode: ModeProduction,
				Logger: slog.New(
					slog.NewJSONHandler(io.Discard, nil),
				),
				Credentials:      setupTestCredentials(t),
				LeaderChecker:    leader,
				BlockBuilder:     builder,
				BlockBroadcaster: broadcaster,
				SlotClock: forgerTestSlotClock{
					// At tip: the tip is the previous slot's block.
					currentSlot:  10,
					chainTipSlot: 9,
					// A peer switch has just happened, so the
					// corroborated upstream target is not yet known.
					upstreamTipSlot:   0,
					upstreamActive:    true,
					slotsPerKESPeriod: 100,
				},
				ForgeSyncToleranceSlots: tc.tolerance,
				PromRegistry:            prometheus.NewRegistry(),
			})
			require.NoError(t, err)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			assert.Zero(
				t,
				leader.callCount(),
				"leader selection is never reached, so the node cannot "+
					"know it just lost a leader slot",
			)
			assert.Zero(t, builder.calls)
			assert.Zero(t, broadcaster.calls)

			// The loss is invisible in every cardano-node-compatible
			// counter: about_to_lead moves, nothing else does.
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeAboutToLead),
			)
			assert.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.forgeNotLeader),
			)
			assert.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.forgeNodeIsLeader),
			)
			assert.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.forgeCouldNot),
			)
			// The one series that does move cannot distinguish "upstream
			// is ahead of us" from "we have not heard from the peer we
			// selected a moment ago".
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeSyncSkip),
			)
		})
	}
}

// forgerScheduleAwareLeader reports a fixed set of scheduled leader
// slots through NextLeaderSlot, the way Election answers from its
// precomputed VRF schedule, so a test can distinguish a skipped slot
// this node was due to lead from an ordinary one.
type forgerScheduleAwareLeader struct {
	scheduled map[uint64]struct{}
}

func (l *forgerScheduleAwareLeader) ShouldProduceBlock(slot uint64) bool {
	_, ok := l.scheduled[slot]
	return ok
}

func (l *forgerScheduleAwareLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	if _, ok := l.scheduled[fromSlot]; ok {
		return fromSlot, true
	}
	return 0, false
}

// newGateSkipLogForger builds a production forger whose logs are
// captured, over a leader checker that answers NextLeaderSlot from a
// fixed schedule, so a test can read back the level a pre-leader gate
// skip was logged at.
func newGateSkipLogForger(
	t *testing.T,
	clock forgerTestSlotClock,
	scheduled map[uint64]struct{},
) (*BlockForger, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(
			logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		)),
		Credentials: setupTestCredentials(t),
		LeaderChecker: &forgerScheduleAwareLeader{
			scheduled: scheduled,
		},
		BlockBuilder:     &forgerTestBuilder{},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, logs
}

// TestForgeGateSkipWarnsOnlyForScheduledLeaderSlots pins the log level
// of both gates that run before leader selection. Such a gate returns
// from checkAndForgeProduction before checkLeaderSafe, so the slot it
// drops moves about_to_lead and nothing else: no Forge_node_is_leader,
// no Forge_node_not_leader, no Forge_could_not_forge. A dropped
// scheduled leader slot is therefore indistinguishable at INFO from "we
// were simply not the leader", which is how a producer can decline its
// own leader slots while every standard SPO dashboard shows it healthy.
//
// A gate skip on an ordinary slot is routine and stays at Debug. A gate
// skip that swallows a slot this node was scheduled to lead is a lost
// block that nothing downstream will ever mention again, so it is
// raised to Warn and marked leader_slot=true.
func TestForgeGateSkipWarnsOnlyForScheduledLeaderSlots(t *testing.T) {
	// The leader slot under test is slot 10 in both gates.
	const leaderSlot = uint64(10)
	for _, gate := range []struct {
		name  string
		clock forgerTestSlotClock
		msg   string
	}{
		{
			// The chain tip has moved past the slot we would
			// produce for.
			name: "tip ahead",
			clock: forgerTestSlotClock{
				currentSlot:       leaderSlot,
				chainTipSlot:      leaderSlot + 1,
				slotsPerKESPeriod: 100,
			},
			msg: "forge skip: chain tip is ahead of the current slot",
		},
		{
			// The corroborated upstream target is unknown, so the
			// node is treated as still syncing even though it is at
			// its own tip.
			name: "upstream syncing",
			clock: forgerTestSlotClock{
				currentSlot:       leaderSlot,
				chainTipSlot:      leaderSlot - 1,
				upstreamTipSlot:   0,
				upstreamActive:    true,
				slotsPerKESPeriod: 100,
			},
			msg: "chain syncing from peer, skipping forge",
		},
	} {
		t.Run(gate.name, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				scheduled  map[uint64]struct{}
				wantLevel  string
				wantMarker bool
			}{
				{
					name:      "ordinary slot stays at debug",
					scheduled: map[uint64]struct{}{},
					wantLevel: "DEBUG",
				},
				{
					name: "scheduled leader slot warns",
					scheduled: map[uint64]struct{}{
						leaderSlot: {},
					},
					wantLevel:  "WARN",
					wantMarker: true,
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					forger, logs := newGateSkipLogForger(
						t,
						gate.clock,
						tc.scheduled,
					)

					require.NoError(
						t,
						forger.checkAndForgeProduction(
							context.Background(),
						),
					)

					out := logs.String()
					require.Contains(t, out, gate.msg)
					assert.Contains(
						t,
						out,
						`"level":"`+tc.wantLevel+`"`,
					)
					if tc.wantMarker {
						assert.Contains(
							t,
							out,
							`"leader_slot":true`,
						)
					} else {
						assert.NotContains(
							t,
							out,
							`"leader_slot":true`,
						)
					}
				})
			}
		})
	}
}

// TestForgeStaleGapSkipMarksScheduledLeaderSlots covers the sub-branch
// of the tip-ahead gate that the marker would otherwise miss. When the
// tip runs further ahead than forgeStaleGapThresholdSlots the gate
// diagnoses a stale database at Error instead of routing the skip
// through logGateSkip, but it drops the slot just as silently and just
// as far before leader selection. The severity and wording are the
// operator's stale-genesis signal and stay as they are; the slot is
// additionally marked leader_slot=true when it was one this node was
// scheduled to lead, so a block lost this way is still attributable.
func TestForgeStaleGapSkipMarksScheduledLeaderSlots(t *testing.T) {
	const leaderSlot = uint64(10)
	// Well past the default forgeStaleGapThresholdSlots of 1000, so
	// the gate takes the stale-database branch.
	clock := forgerTestSlotClock{
		currentSlot:       leaderSlot,
		chainTipSlot:      leaderSlot + 2000,
		slotsPerKESPeriod: 100,
	}
	const msg = "chain tip is far ahead of slot clock; " +
		"database may contain data from a different genesis"

	for _, tc := range []struct {
		name       string
		scheduled  map[uint64]struct{}
		wantMarker bool
	}{
		{
			name:      "ordinary slot is not marked",
			scheduled: map[uint64]struct{}{},
		},
		{
			name: "scheduled leader slot is marked",
			scheduled: map[uint64]struct{}{
				leaderSlot: {},
			},
			wantMarker: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forger, logs := newGateSkipLogForger(
				t,
				clock,
				tc.scheduled,
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(
					context.Background(),
				),
			)

			out := logs.String()
			require.Contains(t, out, msg)
			// The stale-genesis diagnosis keeps its severity
			// whether or not the slot was a scheduled one.
			assert.Contains(t, out, `"level":"ERROR"`)
			if tc.wantMarker {
				assert.Contains(
					t,
					out,
					`"leader_slot":true`,
				)
			} else {
				assert.NotContains(
					t,
					out,
					`"leader_slot":true`,
				)
			}
		})
	}
}

// newOwnTipGateForger builds a production forger over an explicit slot
// clock and an optional fence store, and captures its logs at Debug so a
// test can assert on the level a skip was emitted at. Passing a nil
// fence leaves the duplicate-slot fence in-memory and unloaded, which is
// how a wiring with no metadata store behaves.
func newOwnTipGateForger(
	t *testing.T,
	clock SlotClockProvider,
	leader LeaderChecker,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	fence ForgeFenceStore,
) (*BlockForger, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(
			logs,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.Equal(
		t,
		fence != nil,
		forger.fenceLoaded,
		"the fence must be loaded exactly when a fence store is wired",
	)
	// Construction itself warns that no fence store is wired. Drop it so
	// the buffer holds only what the forge cycle under test emitted.
	logs.Reset()
	return forger, logs
}

// TestEqualSlotOwnBlockIsIdentifiedByHashNotByFence pins that "the block
// at the tip is ours" is decided by identity, not by the fence
// bookkeeping alone.
//
// The fence is a high-water mark over slots this node committed to, and
// it is only guaranteed to be present when a durable ForgeFenceStore is
// wired: without one it is in-memory (see BlockForger.fenceStore), so
// there are wirings in which the forger holds proof that the block at the
// tip is its own — SlotTracker recorded the hash it forged for that slot
// — while the fence signal says nothing. Keying the quiet skip solely on
// the fence sends that slot to the contested-slot branch, where the node
// records a slot battle at Warn against its own block and reports a
// could-not-forge for a slot it did in fact forge.
//
// Both subtests run with no fence store at all, leaving the hash as the
// only signal, and require the forger to tell its own block from a
// rival's using it.
func TestEqualSlotOwnBlockIsIdentifiedByHashNotByFence(t *testing.T) {
	const slot = uint64(10)
	ourHash := bytes.Repeat([]byte{0xa1}, 32)
	rivalHash := bytes.Repeat([]byte{0xb2}, 32)

	t.Run("our own block at the tip is a quiet skip", func(t *testing.T) {
		leader := &forgerCountingLeader{}
		builder := &forgerTestBuilder{}
		broadcaster := &forgerTestBroadcaster{}
		forger, logs := newOwnTipGateForger(
			t,
			forgerTestSlotClock{
				currentSlot:       slot,
				chainTipSlot:      slot,
				chainTipHash:      ourHash,
				slotsPerKESPeriod: 100,
			},
			leader,
			builder,
			broadcaster,
			nil,
		)
		// We forged this slot and the block was adopted: the tip is
		// byte-for-byte our block.
		forger.slotTracker.RecordForgedBlock(slot, ourHash)

		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)

		assert.Zero(
			t,
			leader.callCount(),
			"our own block at our own slot needs no leader selection",
		)
		assert.Zero(t, builder.calls, "must not forge a second block")
		assert.Zero(t, broadcaster.calls)
		assert.Equal(
			t,
			float64(0),
			testutil.ToFloat64(forger.metrics.slotBattlesTotal),
			"our own block is not a rival, so this is not a slot battle",
		)
		assert.Equal(
			t,
			float64(0),
			testutil.ToFloat64(forger.metrics.forgeCouldNot),
			"a slot we forged must not report could-not-forge",
		)
		assert.NotContains(
			t,
			logs.String(),
			`"level":"WARN"`,
			"re-entering a slot whose tip block is ours is routine and "+
				"must not warn",
		)
		assert.Contains(
			t,
			logs.String(),
			"forge skip: slot already has our own block",
		)
		assert.Contains(
			t,
			logs.String(),
			`"matched_by":"forged_block_hash"`,
			"the skip must be attributed to the hash match, not to a "+
				"fence that was never loaded",
		)
	})

	t.Run(
		"a rival block at the tip is still a slot battle",
		func(t *testing.T) {
			leader := &forgerCountingLeader{}
			builder := &forgerTestBuilder{}
			broadcaster := &forgerTestBroadcaster{}
			forger, logs := newOwnTipGateForger(
				t,
				forgerTestSlotClock{
					currentSlot:       slot,
					chainTipSlot:      slot,
					chainTipHash:      rivalHash,
					slotsPerKESPeriod: 100,
				},
				leader,
				builder,
				broadcaster,
				nil,
			)
			// A tracker record for the slot must not be mistaken for
			// ownership of the block at the tip: we forged for this
			// slot, but a rival's block is what the chain adopted.
			forger.slotTracker.RecordForgedBlock(slot, ourHash)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			assert.Equal(
				t,
				1,
				leader.callCount(),
				"a rival's block at our slot is a contested slot",
			)
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
			)
			assert.Zero(t, builder.calls, "must not bind a same-slot parent")
			assert.Zero(t, broadcaster.calls)
			assert.Contains(
				t,
				logs.String(),
				`"level":"WARN"`,
				"a leader slot lost to a rival must be visible",
			)
		},
	)
}

// forgerMovingTipSlotClock is a slot clock whose chain tip moves between
// the read at the top of a forge cycle and the read taken next to the
// tip hash. The first ChainTipSlot call answers chainTipSlot, every
// later one answers movedTipSlot, which is what a rival block landing
// mid-cycle looks like to the forger.
type forgerMovingTipSlotClock struct {
	currentSlot       uint64
	chainTipSlot      uint64
	movedTipSlot      uint64
	chainTipHash      []byte
	slotsPerKESPeriod uint64

	mu        sync.Mutex
	tipReads  int
	hashReads int
}

func (c *forgerMovingTipSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c *forgerMovingTipSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c *forgerMovingTipSlotClock) ChainTipSlot() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tipReads++
	if c.tipReads == 1 {
		return c.chainTipSlot
	}
	return c.movedTipSlot
}

func (c *forgerMovingTipSlotClock) ChainTipHash() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hashReads++
	return c.chainTipHash
}

func (*forgerMovingTipSlotClock) NextSlotTime() (time.Time, error) {
	return time.Now(), nil
}

func (*forgerMovingTipSlotClock) UpstreamTipSlot() uint64 {
	return 0
}

func (*forgerMovingTipSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

func (c *forgerMovingTipSlotClock) reads() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tipReads, c.hashReads
}

// TestEqualSlotLostBattleUnderTheFenceIsNotSilent covers the case where
// the two ownership signals disagree: the fence says this node already
// committed to the slot, and the recorded forge hash says the block that
// actually won it belongs to someone else.
//
// The fence must still win the forging decision. A slot at or below the
// fence may already have a signed, diffused block behind it, and signing
// a second different block for it is equivocation; losing a slot battle
// is not a licence to equivocate. So: no build, no broadcast, no
// advance of the fence.
//
// What must change is that the loss stops being silent. Reporting it as
// "slot already has our own block" at Debug is both false and exactly
// the invisible leader-slot loss this PR exists to remove, so the
// declined leader slot is counted as a could-not-forge and logged at
// Warn with both hashes.
//
// slotBattlesTotal stays where it is: LedgerState.checkSlotBattle
// already counted this battle when the rival block was accepted, over
// the same SlotTracker and the same recorder, so the forger counting it
// again would double it.
func TestEqualSlotLostBattleUnderTheFenceIsNotSilent(t *testing.T) {
	const slot = uint64(10)
	ourHash := bytes.Repeat([]byte{0xa1}, 32)
	rivalHash := bytes.Repeat([]byte{0xb2}, 32)

	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
	fence := &fenceTestStore{slot: slot, present: true}
	forger, logs := newOwnTipGateForger(
		t,
		forgerTestSlotClock{
			currentSlot:       slot,
			chainTipSlot:      slot,
			chainTipHash:      rivalHash,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		broadcaster,
		fence,
	)
	// We forged for this slot, but the chain adopted a rival's block.
	forger.slotTracker.RecordForgedBlock(slot, ourHash)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	// The fence still refuses the slot.
	assert.Zero(t, builder.calls, "the fence must still forbid a second block")
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, fence.stored, "must not advance the fence")
	assert.Zero(
		t,
		leader.callCount(),
		"a slot the fence has already spent needs no leader selection",
	)

	// The battle itself is counted by LedgerState.checkSlotBattle, not
	// here. Reaching this case means a block other than ours was
	// accepted at a slot SlotTracker says we forged, and the only path
	// that accepts a peer's block already ran checkSlotBattle over the
	// same tracker and the same recorder — the node wiring points
	// ForgedBlockChecker at this forger's SlotTracker and
	// SlotBattleRecorder at the forger itself. Incrementing again here
	// would double the count, so the forger must leave it alone.
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"chainsync owns the slot-battle count for a rival block that "+
			"was accepted at a slot we forged; the forger must not "+
			"count it a second time",
	)

	// The declined leader slot is still visible, which is the point.
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a leader slot we did not hold must be visible as a "+
			"could-not-forge, not as silence",
	)
	logged := logs.String()
	assert.Contains(t, logged, `"level":"WARN"`)
	assert.Contains(
		t,
		logged,
		"slot battle lost: rival block at tip for a slot this node "+
			"already forged",
	)
	assert.Contains(
		t,
		logged,
		hex.EncodeToString(ourHash),
		"the log must name the block we forged",
	)
	assert.Contains(
		t,
		logged,
		hex.EncodeToString(rivalHash),
		"the log must name the block that won the slot",
	)
	assert.NotContains(
		t,
		logged,
		"forge skip: slot already has our own block",
		"the block at the tip is demonstrably not ours",
	)
}

// TestEqualSlotOwnershipIsInconclusiveWhenTheTipMoves pins the tip-slot
// re-read inside the ownership check.
//
// tipSlot is sampled once at the top of checkAndForgeProduction, and the
// chain can move before ChainTipHash is read. Comparing a hash from one
// tip against a slot from another decides ownership from two different
// blocks: here it would read a rival's hash and conclude we lost a
// battle at slot 10 when the tip has in fact already moved past it.
//
// Re-reading the tip slot next to the hash makes that disagreement
// inconclusive instead, and the decision falls back to the fence, which
// is the conservative answer. The reads are still not atomic, so this
// narrows the window rather than closing it; what it guarantees is that
// a moved tip cannot manufacture a slot-battle report.
func TestEqualSlotOwnershipIsInconclusiveWhenTheTipMoves(t *testing.T) {
	const slot = uint64(10)
	ourHash := bytes.Repeat([]byte{0xa1}, 32)
	rivalHash := bytes.Repeat([]byte{0xb2}, 32)

	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
	fence := &fenceTestStore{slot: slot, present: true}
	clock := &forgerMovingTipSlotClock{
		currentSlot: slot,
		// The cycle opens with the tip at the current slot...
		chainTipSlot: slot,
		// ...and it has moved on by the time the hash is read.
		movedTipSlot:      slot + 1,
		chainTipHash:      rivalHash,
		slotsPerKESPeriod: 100,
	}
	forger, logs := newOwnTipGateForger(
		t,
		clock,
		leader,
		builder,
		broadcaster,
		fence,
	)
	forger.slotTracker.RecordForgedBlock(slot, ourHash)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	tipReads, hashReads := clock.reads()
	assert.Equal(
		t,
		2,
		tipReads,
		"the tip slot must be re-read next to the hash, not reused "+
			"from the top of the cycle",
	)
	assert.Equal(t, 1, hashReads)

	assert.Zero(t, builder.calls)
	assert.Zero(t, broadcaster.calls)
	assert.Empty(t, fence.stored)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a tip that moved proves nothing about who held slot 10",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	logged := logs.String()
	assert.NotContains(
		t,
		logged,
		`"level":"WARN"`,
		"an inconclusive comparison must not be reported as a lost battle",
	)
	assert.Contains(
		t,
		logged,
		`"matched_by":"forge_fence"`,
		"the decision must fall back to the fence",
	)
}
