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
	"context"
	"io"
	"log/slog"
	"testing"

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
// (TraceBlockFromFuture) and, for EQ, builds a block context on the
// tip's predecessor so the node still participates.
//
// Dingo instead folded EQ into the GT skip with `currentSlot <=
// tipSlot`, which returns before checkLeaderSafe. The slot never
// reaches leader selection, so no leadership counter moves and the only
// trace is a Debug line: the loss is invisible on every dashboard.
//
// The slot must at minimum reach leader selection and be accounted for.
func TestCheckAndForgeProductionEqualSlotReachesLeaderCheck(t *testing.T) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{}
	broadcaster := &forgerTestBroadcaster{}
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
		nil,
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
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a leader slot we did not produce for must be visible as a "+
			"could-not-forge, not as silence",
	)

	// The block must NOT be built. BuildBlock binds the parent to the
	// live chain tip, which here is the rival's block at this same
	// slot; the resulting block would carry a parent whose slot equals
	// its own and be rejected by ledger.validateBlockOrder. Forging a
	// valid alternative needs the tip's predecessor as parent, which
	// the builder cannot express yet.
	assert.Zero(
		t,
		builder.calls,
		"must not bind a same-slot parent",
	)
	assert.Zero(t, broadcaster.calls)
}

// TestCheckAndForgeProductionEqualSlotDoesNotReForgeOurOwnSlot is the
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
