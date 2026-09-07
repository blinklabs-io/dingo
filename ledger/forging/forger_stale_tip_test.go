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
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newStaleTipTestForger builds a production forger whose leader check always
// says "leader", so the only thing that can stop it forging is a gate.
// frontierSlot is this node's own header frontier; chainTipSlot is the
// ledger-applied tip a forged block would be built on.
func newStaleTipTestForger(
	t *testing.T,
	currentSlot, chainTipSlot, frontierSlot uint64,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder, *forgerTestBroadcaster) {
	t.Helper()
	block := newForgerTestBlock(currentSlot, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       currentSlot,
			chainTipSlot:      chainTipSlot,
			frontierExplicit:  true,
			frontierSlot:      frontierSlot,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsWhenLedgerTipTrailsHeaderFrontier is the stale-tip-forge
// regression. The forge loop takes its parent from the LEDGER-APPLIED tip.
// When this node's own header frontier is further ahead, that parent is a
// block the node has already superseded, so the forged block enters a fork
// race it has already lost and is orphaned. The upstream sync guard does not
// catch it: it compares the applied tip against the network with a tolerance
// sized for catch-up, and here there is no upstream lag at all -- the node's
// own ledger pipeline is the thing behind.
//
// Before the fix the forger built and broadcast the block regardless.
func TestForgeSkipsWhenLedgerTipTrailsHeaderFrontier(t *testing.T) {
	var logs bytes.Buffer
	// Applied tip 83 slots behind the frontier: the field case.
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		100, // ledger-applied tip
		183, // header frontier
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls, "must not build on a superseded parent")
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
	)
	// Never silently: the skip is a WARN an operator can alert on.
	require.Contains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
	require.Contains(t, logs.String(), `"level":"WARN"`)
}

// TestForgeProceedsWithinHeaderFrontierTolerance pins the other side of the
// bound: the ledger pipeline commits in batches, so a gap of a slot or two is
// the normal steady state at the head of a fast chain and must not suppress
// forging.
func TestForgeProceedsWithinHeaderFrontierTolerance(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200,
		100,
		100+forgeHeaderFrontierToleranceSlots,
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipHashDiverged),
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
}

// TestForgeStaleTipToleranceIsConfigurable pins that the bound is a named,
// overridable parameter rather than a literal buried in the gate.
func TestForgeStaleTipToleranceIsConfigurable(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 200, 100, 120, &logs)
	require.Equal(
		t,
		uint64(forgeHeaderFrontierToleranceSlots),
		forger.forgeFrontierToleranceSlots,
	)
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)

	block := newForgerTestBlock(200, 2)
	wideBuilder := &forgerTestBuilder{block: block, cbor: block.cbor}
	wide, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(&logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     wideBuilder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       200,
			chainTipSlot:      100,
			frontierSlot:      120,
			slotsPerKESPeriod: 100,
		},
		ForgeHeaderFrontierToleranceSlots: 50,
		PromRegistry:                      prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	require.NoError(t, wide.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, wideBuilder.calls)
}

// TestTipGapGaugeReportsApplyBacklogOnEveryLeaderCheck is the observability
// half of the regression. dingo_forge_tip_gap_slots was reset to 0 at the top
// of every leader check and only set non-zero on the skip paths, so a producer
// forging tens of slots behind its own frontier reported a gap of exactly 0 --
// the one case where the gauge mattered was the one case it could not show.
func TestTipGapGaugeReportsApplyBacklogOnEveryLeaderCheck(t *testing.T) {
	var logs bytes.Buffer

	// Within tolerance: the forge proceeds, and the gauge still reports the
	// real backlog rather than 0.
	forger, builder, _ := newStaleTipTestForger(t, 200, 100, 103, &logs)
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.calls, "expected this check to forge")
	require.Equal(
		t,
		float64(3),
		testutil.ToFloat64(forger.metrics.tipGapSlots),
	)

	// Beyond tolerance: the gauge reports the backlog that caused the skip.
	skipping, _, _ := newStaleTipTestForger(t, 200, 100, 183, &logs)
	require.NoError(t, skipping.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		float64(83),
		testutil.ToFloat64(skipping.metrics.tipGapSlots),
	)

	// No backlog: zero, not a stale reading.
	caughtUp, _, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
	require.NoError(t, caughtUp.checkAndForgeProduction(context.Background()))
	require.Zero(t, testutil.ToFloat64(caughtUp.metrics.tipGapSlots))
}

// newEqualSlotForkTestForger builds a production forger whose applied tip and
// header frontier sit at the SAME slot but carry the given hashes.
func newEqualSlotForkTestForger(
	t *testing.T,
	appliedHash, frontierHash []byte,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder, *forgerTestBroadcaster) {
	t.Helper()
	block := newForgerTestBlock(200, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       200,
			chainTipSlot:      100,
			chainTipHash:      appliedHash,
			frontierExplicit:  true,
			frontierSlot:      100,
			frontierHash:      frontierHash,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsOnEqualSlotFrontierDivergence is the equal-slot fork the slot
// gap cannot see. Chain selection replaced the block at the applied tip's slot
// with a competing one at the SAME slot that the ledger has not applied, so
// the gap is 0 while the ledger state still describes the block that was
// replaced -- the builder would parent the block on one chain position while
// its transactions, protocol parameters and leader eligibility came from
// another.
func TestForgeSkipsOnEqualSlotFrontierDivergence(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newEqualSlotForkTestForger(
		t,
		bytes.Repeat([]byte{0xAA}, 32),
		bytes.Repeat([]byte{0xBB}, 32),
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls, "must not forge across an equal-slot fork")
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipHashDiverged),
	)
	// The slot-gap reason must not be charged for a divergence.
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
	)
	require.Contains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
	require.Contains(t, logs.String(), `"reason":"primary_tip_hash_diverged"`)
	require.Contains(t, logs.String(), `"level":"WARN"`)
	// The gauge is a slot gap and there is none; the divergence shows on the
	// counter, not here.
	require.Zero(t, testutil.ToFloat64(forger.metrics.tipGapSlots))
}

// TestForgeProceedsWhenFrontierMatchesAppliedTip pins the other side: the same
// slot with the same hash is the normal caught-up state and must forge.
func TestForgeProceedsWhenFrontierMatchesAppliedTip(t *testing.T) {
	var logs bytes.Buffer
	hash := bytes.Repeat([]byte{0xAA}, 32)
	forger, builder, broadcaster := newEqualSlotForkTestForger(
		t,
		hash,
		bytes.Clone(hash),
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipHashDiverged),
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
}

// TestForgeProceedsWhenEitherTipHashIsEmpty pins that a genesis or
// uninitialised primary chain -- where there is no hash to compare -- does not
// wedge a fresh node into never forging.
func TestForgeProceedsWhenEitherTipHashIsEmpty(t *testing.T) {
	for name, tc := range map[string]struct {
		applied, frontier []byte
	}{
		"frontier hash unknown": {
			applied:  bytes.Repeat([]byte{0xAA}, 32),
			frontier: []byte{},
		},
		"applied hash unknown": {
			applied:  []byte{},
			frontier: bytes.Repeat([]byte{0xBB}, 32),
		},
		"both at genesis": {applied: []byte{}, frontier: []byte{}},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			forger, builder, _ := newEqualSlotForkTestForger(
				t,
				tc.applied,
				tc.frontier,
				&logs,
			)
			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)
			require.Equal(t, 1, builder.calls)
			require.Zero(
				t,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipHashDiverged,
				),
			)
		})
	}
}

// TestForgeStaleTipSkipReasonsArePreMaterialized pins that every reason series
// exists before the first skip, so a dashboard is not looking at an absent
// series.
func TestForgeStaleTipSkipReasonsArePreMaterialized(t *testing.T) {
	var logs bytes.Buffer
	hash := bytes.Repeat([]byte{0xAA}, 32)
	forger, _, _ := newEqualSlotForkTestForger(
		t,
		hash,
		bytes.Clone(hash),
		&logs,
	)
	require.Equal(
		t,
		3,
		testutil.CollectAndCount(forger.metrics.forgeStaleTipSkip),
	)
}

// forgeStaleTipTestNonLeader never elects this node, so a test can separate
// "the stale-tip condition holds" from "a block was actually lost to it".
type forgeStaleTipTestNonLeader struct{}

func (forgeStaleTipTestNonLeader) ShouldProduceBlock(uint64) bool {
	return false
}

func (forgeStaleTipTestNonLeader) NextLeaderSlot(
	fromSlot uint64,
) (uint64, bool) {
	return fromSlot, false
}

// newStaleTipTestForgerWithLeader is newStaleTipTestForger with the leader
// checker and the two tip hashes made explicit.
func newStaleTipTestForgerWithLeader(
	t *testing.T,
	leader LeaderChecker,
	currentSlot, chainTipSlot, frontierSlot uint64,
	appliedHash, frontierHash []byte,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder, *forgerTestBroadcaster) {
	t.Helper()
	block := newForgerTestBlock(currentSlot, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       currentSlot,
			chainTipSlot:      chainTipSlot,
			chainTipHash:      appliedHash,
			frontierExplicit:  true,
			frontierSlot:      frontierSlot,
			frontierHash:      frontierHash,
			slotsPerKESPeriod: 100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsWhenFrontierAlreadyHasTheCurrentSlot covers the guard that
// asks "does a block already exist at this slot". It compared the current slot
// against the LEDGER-APPLIED tip, but the parent comes from the frontier, so
// inside the frontier tolerance a peer's block at the current slot could
// already be on the frontier while still unapplied. Forging then parents a
// block for slot S on a tip already at slot S -- a non-increasing slot,
// admitted locally and broadcast.
//
// The gap here is 2 slots, well inside the tolerance, so the stale-tip gate
// does not fire and this guard is the only thing that can catch it.
func TestForgeSkipsWhenFrontierAlreadyHasTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		198, // ledger-applied tip, still behind
		200, // frontier already carries a block at the current slot
		&logs,
	)
	require.LessOrEqual(
		t,
		uint64(2),
		forger.forgeFrontierToleranceSlots,
		"this test needs the 2-slot gap to be inside the tolerance",
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(
		t,
		builder.calls,
		"must not forge a non-increasing slot on top of the frontier",
	)
	require.Zero(t, broadcaster.calls)
	require.Contains(
		t,
		logs.String(),
		"forge skip: primary chain tip already has a block at this slot",
	)
	// Warned, not Debug: this gate runs before leader selection, so a slot
	// this node was scheduled to lead would otherwise vanish silently.
	require.Contains(t, logs.String(), `"level":"WARN"`)
}

// TestForgeSkipsWhenFrontierIsAheadOfTheCurrentSlot covers the case that
// falls through every other gate: the applied tip is behind the current slot,
// so the applied-tip comparison passes, but the FRONTIER is ahead of it. The
// builder parents on the frontier, so forging would produce a block for slot
// 200 whose parent already sits at slot 201 -- a block earlier than its own
// parent. Comparing the current slot against the applied tip alone cannot see
// this; comparing against max(applied, frontier) can.
func TestForgeSkipsWhenFrontierIsAheadOfTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		199, // applied tip, behind the current slot
		201, // frontier AHEAD of the current slot
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(
		t,
		builder.calls,
		"must not forge a block whose parent is at a later slot than itself",
	)
	require.Zero(t, broadcaster.calls)
	require.Contains(
		t,
		logs.String(),
		"forge skip: chain tip is ahead of the current slot",
	)
}

// TestForgeSkipsWhenAppliedTipAlreadyHasTheCurrentSlot pins that narrowing the
// past-slot comparison to a strict inequality did not re-open the plain
// equal-applied-tip case. Equal slots now fall through to the contested-slot
// handling, which is exactly why the comparison had to stop consuming them,
// and that handling still refuses the slot.
func TestForgeSkipsWhenAppliedTipAlreadyHasTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		200, // applied tip already at this slot
		200, // frontier agrees
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Contains(
		t,
		logs.String(),
		"forge skip: leader slot already holds another block",
	)
}

// TestForgeStaleTipSkipCountsLostBlocksNotLeaderChecks pins that the stale-tip
// gate sits after leader selection. The condition holds for as long as the
// pipeline is behind, so gating before the leader check made the WARN and the
// counter fire once per slot -- once a second on a 1s-slot chain -- and made
// the counter measure leader checks rather than lost blocks.
func TestForgeStaleTipSkipCountsLostBlocksNotLeaderChecks(t *testing.T) {
	t.Run("not leader: no warning, no counter", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder, _ := newStaleTipTestForgerWithLeader(
			t,
			forgeStaleTipTestNonLeader{},
			200, 100, 183,
			nil, nil,
			&logs,
		)

		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)

		require.Zero(t, builder.calls)
		require.Zero(
			t,
			testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
			"a slot this node was never going to forge is not a lost block",
		)
		require.NotContains(
			t,
			logs.String(),
			"forge skip: ledger tip stale vs primary chain tip",
		)
		// The backlog is still reported on every leader check.
		require.Equal(
			t,
			float64(83),
			testutil.ToFloat64(forger.metrics.tipGapSlots),
		)
	})

	t.Run("leader: warning and counter", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder, _ := newStaleTipTestForgerWithLeader(
			t,
			forgerTestLeader{},
			200, 100, 183,
			nil, nil,
			&logs,
		)

		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)

		require.Zero(t, builder.calls)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
		)
		require.Contains(
			t,
			logs.String(),
			"forge skip: ledger tip stale vs primary chain tip",
		)
	})
}

// TestForgeSkipsWhenFrontierIsBehindTheAppliedTip covers the third disagreement
// shape. applyGap is 0 (the frontier is not ahead) and the equal-slot hash
// check does not apply (the slots differ), so neither existing case sees it,
// yet the ledger describes a chain position ahead of the parent the builder
// would use. The ledger itself recognises this state and reconciles it at
// startup by rolling its tip back to the chain tip.
func TestForgeSkipsWhenFrontierIsBehindTheAppliedTip(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForgerWithLeader(
		t,
		forgerTestLeader{},
		300, // current slot
		200, // ledger-applied tip
		190, // frontier BEHIND the applied tip
		bytes.Repeat([]byte{0xAA}, 32),
		bytes.Repeat([]byte{0xBB}, 32),
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Zero(t, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipFrontierBehind),
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
	)
	require.Contains(t, logs.String(), `"reason":"primary_tip_behind_applied"`)
	require.Contains(t, logs.String(), `"level":"WARN"`)
}

// TestForgeProceedsWhenFrontierIsUninitialised pins that a node whose primary
// chain has no tip yet -- zero slot, empty hash -- is not caught by the
// frontier-behind case and can still forge.
func TestForgeProceedsWhenFrontierIsUninitialised(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForgerWithLeader(
		t,
		forgerTestLeader{},
		300, 200, 0,
		bytes.Repeat([]byte{0xAA}, 32),
		nil, // no frontier hash: chain not initialised
		&logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipFrontierBehind),
	)
}
