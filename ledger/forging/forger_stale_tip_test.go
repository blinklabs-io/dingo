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
// frontierSlot is this node's own primary chain tip; chainTipSlot is the
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

// TestForgeSkipsWhenLedgerTipTrailsPrimaryChainTip is the stale-tip-forge
// regression. The forge loop takes its parent from the LEDGER-APPLIED tip.
// When this node's own primary chain tip is further ahead, that parent is a
// block the node has already superseded, so the forged block enters a fork
// race it has already lost and is orphaned. The upstream sync guard does not
// catch it: it compares the applied tip against the network with a tolerance
// sized for catch-up, and here there is no upstream lag at all -- the node's
// own ledger pipeline is the thing behind.
//
// Before the fix the forger built and broadcast the block regardless.
func TestForgeSkipsWhenLedgerTipTrailsPrimaryChainTip(t *testing.T) {
	var logs bytes.Buffer
	// Applied tip 83 slots behind the frontier: the field case.
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		100, // ledger-applied tip
		183, // primary chain tip
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

// TestStaleTipSkipCountsCouldNotForge pins the cardano-node parity counters
// across this refusal. The gate runs after checkLeaderSafe and before
// forgeNodeIsLeader.Inc(), so a lost leader slot moves about_to_lead at the
// top of the check and then nothing: node_is_leader never increments,
// not_leader counts only !isLeader, and without this the Dingo-specific
// dingo_forge_stale_tip_skip_total would be the sole record of the loss. An
// operator alerting on could_not_forge -- registered as "slots where forging
// failed (syncing, build error, etc)" -- would see a flat line while the
// producer stopped forging.
func TestStaleTipSkipCountsCouldNotForge(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 200, 100, 183, &logs)
	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Zero(t, builder.calls)
	require.Equal(t, float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"lost leader slot must move cardano_node_metrics_Forge_could_not_forge_int")
}

// TestForgeProceedsWithinPrimaryChainTipTolerance pins the other side of the
// bound: the ledger pipeline commits in batches, so a gap of a slot or two is
// the normal steady state at the head of a fast chain and must not suppress
// forging.
func TestForgeProceedsWithinPrimaryChainTipTolerance(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200,
		100,
		100+forgePrimaryChainTipToleranceSlots,
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
	// The post-mortem line survives the forge path. It is emitted below the
	// credential recheck, i.e. after the last gate that can still refuse the
	// slot, so a "forge context" line is never followed by a skip for it.
	require.Contains(t, logs.String(), "forge context")
}

// TestForgeStaleTipToleranceIsConfigurable pins that the bound is a named,
// overridable parameter rather than a literal buried in the gate.
func TestForgeStaleTipToleranceIsConfigurable(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 200, 100, 120, &logs)
	require.Equal(
		t,
		uint64(forgePrimaryChainTipToleranceSlots),
		forger.forgePrimaryChainTipToleranceSlots,
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
			currentSlot:  200,
			chainTipSlot: 100,
			// Explicit, not merely non-zero: this test's whole verdict is
			// that a 20-slot gap is tolerated at a 50-slot bound, so it must
			// not depend on the double's value-based opt-in rule. If that
			// rule were ever tightened back to frontierExplicit alone the
			// frontier would mirror the applied tip, the gap would collapse
			// to 0, and this test would still pass while the tolerance knob
			// had stopped being honoured.
			frontierExplicit:  true,
			frontierSlot:      120,
			slotsPerKESPeriod: 100,
		},
		ForgePrimaryChainTipToleranceSlots: 50,
		PromRegistry:                       prometheus.NewRegistry(),
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
// primary chain tip sit at the SAME slot but carry the given hashes.
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
	// Reason-specific message: an equal-slot divergence is not a stale
	// ledger tip, and the shared message used to say it was.
	require.Contains(
		t,
		logs.String(),
		"forge skip: primary chain tip diverged from the applied tip at the same slot",
	)
	require.NotContains(
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
//
// Six series: the three tip disagreements (slot_gap,
// primary_tip_hash_diverged, primary_tip_behind_applied), the pre-leader-check
// unapplied_rival_at_leader_slot, and the two staleness bounds
// (eb_manifest_ahead, applied_tip_stale), which are pre-materialized even
// though both of applied_tip_stale's sources are off by default.
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
		6,
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
		forger.forgePrimaryChainTipToleranceSlots,
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
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipPrimaryTipBehind),
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
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipPrimaryTipBehind),
	)
}

// TestForgeCountsLeaderSlotLostToUnappliedRival pins the counter on the one
// refusal this gate adds that runs BEFORE leader selection: the primary chain
// tip already holds a block at the current slot while the ledger has not
// applied it. That path returns before checkLeaderSafe, so a slot this node
// was scheduled to lead moves about_to_lead and nothing else -- no
// node_is_leader, no not_leader, no could_not_forge.
//
// It matters because one real-world event splits across two paths purely on
// pipeline timing. A rival block at our leader slot that the ledger HAS
// applied reaches the contested-slot branch above and moves both
// slotBattlesTotal and could_not_forge; the same rival still unapplied lands
// here. Without this counter a dashboard sees the lost block in the first case
// and not in the second.
//
// The count is taken from isScheduledLeaderSlot, which this path already
// consults to pick the log level, so the counter moves on exactly the slots
// the WARN marks and never on an ordinary slot.
func TestForgeCountsLeaderSlotLostToUnappliedRival(t *testing.T) {
	const leaderSlot = uint64(200)
	for _, tc := range []struct {
		name      string
		scheduled map[uint64]struct{}
		wantCount float64
		wantLevel string
	}{
		{
			// Not a slot this node was due to lead: nothing was lost,
			// so nothing is counted and the skip stays routine.
			name:      "ordinary slot counts nothing",
			scheduled: map[uint64]struct{}{},
			wantCount: 0,
			wantLevel: `"level":"DEBUG"`,
		},
		{
			// A scheduled leader slot: a block this node would have
			// forged, dropped by a gate that no parity counter covers.
			name: "scheduled leader slot counts one",
			scheduled: map[uint64]struct{}{
				leaderSlot: {},
			},
			wantCount: 1,
			wantLevel: `"level":"WARN"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			// The applied tip is 2 slots back -- inside the tolerance, so
			// the post-leader-check stale-tip gate does not fire -- while
			// the primary chain tip already carries a block at the current
			// slot.
			forger, builder, broadcaster := newStaleTipTestForgerWithLeader(
				t,
				&forgerScheduleAwareLeader{scheduled: tc.scheduled},
				leaderSlot,   // current slot
				leaderSlot-2, // ledger-applied tip, still behind
				leaderSlot,   // primary chain tip holds this slot already
				bytes.Repeat([]byte{0xAA}, 32),
				bytes.Repeat([]byte{0xBB}, 32),
				&logs,
			)
			require.LessOrEqual(
				t,
				uint64(2),
				forger.forgePrimaryChainTipToleranceSlots,
				"this test needs the 2-slot gap to be inside the tolerance",
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			require.Zero(t, builder.calls)
			require.Zero(t, broadcaster.calls)
			require.Contains(
				t,
				logs.String(),
				"forge skip: primary chain tip already has a block at this slot",
			)
			require.Contains(t, logs.String(), tc.wantLevel)
			require.Equal(
				t,
				tc.wantCount,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipUnappliedRival,
				),
			)
			// The three post-leader-check reasons describe a different
			// refusal and must not move on this path.
			require.Zero(
				t,
				testutil.ToFloat64(forger.metrics.forgeStaleTipSkipSlotGap),
			)
			require.Zero(
				t,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipHashDiverged,
				),
			)
			require.Zero(
				t,
				testutil.ToFloat64(
					forger.metrics.forgeStaleTipSkipPrimaryTipBehind,
				),
			)
		})
	}
}

// newStalenessTestForger builds a production forger for the staleness gates,
// with the upstream target and the corroborated endorser-block slot explicit.
//
// upstreamStalenessSlots and ebStalenessSlots are explicit and every caller
// that exercises those bounds must pass a non-zero value: all three bounds are
// opt-in, so a helper that defaulted any of them would hide the very
// regressions TestForgeUpstreamStalenessIsOffByDefault and
// TestForgeEndorserBlockStalenessIsOffByDefault exist to catch.
func newStalenessTestForger(
	t *testing.T,
	currentSlot, chainTipSlot, frontierSlot, upstreamSlot uint64,
	ebSlot uint64,
	appliedStalenessSlots uint64,
	upstreamStalenessSlots uint64,
	ebStalenessSlots uint64,
	logs *bytes.Buffer,
) (*BlockForger, *forgerTestBuilder) {
	t.Helper()
	block := newForgerTestBlock(currentSlot, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			currentSlot:       currentSlot,
			chainTipSlot:      chainTipSlot,
			frontierExplicit:  true,
			frontierSlot:      frontierSlot,
			upstreamTipSlot:   upstreamSlot,
			slotsPerKESPeriod: 100,
		},
		LeiosVerifiedEbSlot:              func() uint64 { return ebSlot },
		ForgeAppliedTipStalenessSlots:    appliedStalenessSlots,
		ForgeUpstreamStalenessSlots:      upstreamStalenessSlots,
		ForgeEndorserBlockStalenessSlots: ebStalenessSlots,
		PromRegistry:                     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder
}

// TestForgeSkipsWhenNewestKnownBlockTrailsUpstream is the ghost the frontier
// gate could not see. When header admission and ledger application stall
// together the frontier equals the applied tip, so the frontier gap reads 0 and
// the gate passes -- while the node is many slots behind the network and
// forges on a parent the network has already built past.
//
// Measured against the corroborated upstream target rather than the wall clock,
// so it stays meaningful on a chain of any block rate.
//
// The bound is opt-in, so this test sets it explicitly. It cannot by itself
// distinguish "the network is 19 slots ahead" from "the block 19 slots after
// mine was just admitted and its body is still in flight" -- the upstream
// target is published at header admission while newestKnown counts blocks --
// which is precisely why the bound is not defaulted on. See
// TestForgeUpstreamStalenessIsOffByDefault.
func TestForgeSkipsWhenNewestKnownBlockTrailsUpstream(t *testing.T) {
	var logs bytes.Buffer
	// Primary chain tip == applied tip, so the gap is 0, but the network is
	// 19 slots ahead. Slot numbers are scaled down so the KES period stays
	// inside the test operational certificate.
	forger, builder := newStalenessTestForger(
		t, 300, 299, 299, 318, 0, 0, 5, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.tipGapSlots),
		"the frontier gap really is 0 here; that is the point",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	require.Contains(t, logs.String(), `"reason":"applied_tip_stale"`)
	require.Contains(t, logs.String(), `"upstream_target_slot":318`)
	// applied_tip_stale is reached from two independent bounds. Both are
	// logged, and stale_source names the one that fired, so a post-mortem
	// does not have to guess which term refused the slot.
	require.Contains(t, logs.String(), `"stale_source":"upstream"`)
	require.Contains(t, logs.String(), `"upstream_staleness_slots":5`)
	require.Contains(t, logs.String(), `"applied_staleness_slots":0`)
	// Reason-specific message: both local tips agree here, so the shared
	// "ledger tip stale vs primary chain tip" named the wrong pair.
	require.Contains(t, logs.String(), "forge skip: newest known block is stale")
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
}

// TestForgeProceedsOnAQuietChain pins that the staleness term does not punish a
// chain with a long block interval. The newest block is 500 slots old but the
// network agrees it is the newest, so nothing is wrong and the node must forge.
// A wall-clock bound would refuse here, which is why the default term is
// measured against upstream and the wall-clock one is off by default.
func TestForgeProceedsOnAQuietChain(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 600, 100, 100, 100, 0, 0, 5, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	// The forge-context line carries every input a post-mortem needs.
	require.Contains(t, logs.String(), `"msg":"forge context"`)
	require.Contains(t, logs.String(), `"newest_known_slot":100`)
}

// TestForgeAppliedTipStalenessKnobIsOptIn pins the wall-clock backstop: off by
// default, and refusing once an operator sets a bound.
func TestForgeAppliedTipStalenessKnobIsOptIn(t *testing.T) {
	t.Run("off by default", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder := newStalenessTestForger(
			t, 600, 100, 100, 0, 0, 0, 0, 0, &logs,
		)
		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)
		require.Equal(t, 1, builder.calls)
	})

	t.Run("refuses once set", func(t *testing.T) {
		var logs bytes.Buffer
		forger, builder := newStalenessTestForger(
			t, 600, 100, 100, 0, 0, 100, 0, 0, &logs,
		)
		require.NoError(
			t,
			forger.checkAndForgeProduction(context.Background()),
		)
		require.Zero(t, builder.calls)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(
				forger.metrics.forgeStaleTipSkipAppliedStale,
			),
		)
		// The other half of the pair: the wall-clock term fired, and the
		// line says so rather than logging an upstream bound of 0 with no
		// way to tell the two apart.
		require.Contains(t, logs.String(), `"stale_source":"wall_clock"`)
		require.Contains(t, logs.String(), `"applied_staleness_slots":100`)
		require.Contains(t, logs.String(), `"upstream_staleness_slots":0`)
	})
}

// TestForgeSkipsWhenCorroboratedEndorserBlockIsAhead covers the Leios signal: a
// corroborated endorser block shares its announcing ranking block's slot, so it
// is proof a ranking block exists there even though no header has arrived. The
// headers alone look caught up -- frontier equals the applied tip -- so only
// this evidence can refuse the forge.
//
// The bound is opt-in, so this test sets ForgeEndorserBlockStalenessSlots
// explicitly. It used to borrow forgePrimaryChainTipToleranceSlots and passed
// with both staleness bounds at 0, which is exactly the always-on refusal
// TestForgeEndorserBlockStalenessIsOffByDefault now forbids.
func TestForgeSkipsWhenCorroboratedEndorserBlockIsAhead(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 320, 300, 300, 0, 313, 0, 0, 5, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"reason":"eb_manifest_ahead"`)
	require.Contains(t, logs.String(), `"eb_slot":313`)
	// The refusal names its own bound and its own gap, not the local
	// block-against-block tolerance it used to borrow.
	require.Contains(t, logs.String(), `"eb_gap_slots":13`)
	require.Contains(t, logs.String(), `"eb_staleness_slots":5`)
	// Reason-specific message: the applied tip and the primary chain tip are
	// in exact agreement here, so "ledger tip stale vs primary chain tip"
	// would point an operator at the one pair of values that is fine.
	require.Contains(
		t,
		logs.String(),
		"forge skip: corroborated endorser block is ahead of the applied tip",
	)
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs primary chain tip",
	)
	require.Contains(t, logs.String(), `"gap_slots":0`)
}

// TestForgeEndorserBlockStalenessIsOffByDefault is the regression guard for the
// always-on endorser-block refusal, and the sibling of
// TestForgeUpstreamStalenessIsOffByDefault.
//
// The endorser-block watermark is a NETWORK-stage value: it advances at
// leios-notify announcement time, before a header for that slot has to arrive.
// It is also monotonic and never lowered on a fork. Compared against the
// locally applied tip with an always-on bound, an endorser block corroborated
// for a chain this node does not adopt refuses every leader slot for as long
// as the local chain sits below that slot -- with the applied tip and the
// primary chain tip in agreement and gap_slots reading 0, so every local
// indicator says the node is healthy while the producer goes quiet.
//
// So the bound is 0 (disabled) by default and the path never refuses without
// it. The shape below is the one that used to refuse: watermark far ahead,
// both local tips agreeing, both other staleness bounds off.
func TestForgeEndorserBlockStalenessIsOffByDefault(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 900, 300, 300, 0, 360, 0, 0, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"with no endorser-block bound configured, an advisory watermark "+
			"ahead of the local chain must not cost the leader slot",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.NotContains(t, logs.String(), `"reason":"eb_manifest_ahead"`)
}

// TestForgeProceedsWhenEndorserBlockIsWithinItsBound is the negative case for
// the endorser-block path: with the bound ON, a corroborated endorser block
// that leads the applied tip by less than the bound still forges.
//
// Without this, nothing distinguished "the bound refuses when it should" from
// "the path refuses whenever any endorser block is ahead at all".
func TestForgeProceedsWhenEndorserBlockIsWithinItsBound(t *testing.T) {
	var logs bytes.Buffer
	// eb 304 leads the applied tip at 300 by 4, under the bound of 5.
	forger, builder := newStalenessTestForger(
		t, 320, 300, 300, 0, 304, 0, 0, 5, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"an endorser block within the configured bound must still forge",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"msg":"forge context"`)
	require.Contains(t, logs.String(), `"eb_slot":304`)
}

// TestForgeEndorserBlockBoundDoesNotBorrowThePrimaryChainTipTolerance pins that
// the two knobs are independent in both directions: widening the local
// tolerance must not silence the endorser-block bound, and setting the
// endorser-block bound must not tighten the local coherence check.
func TestForgeEndorserBlockBoundDoesNotBorrowThePrimaryChainTipTolerance(
	t *testing.T,
) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 320, 300, 300, 0, 313, 0, 0, 5, &logs,
	)
	// Local tolerance wide open; only the endorser-block bound is tight.
	forger.forgePrimaryChainTipToleranceSlots = 1000

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(
		t,
		builder.calls,
		"the endorser-block bound is its own knob; widening the local "+
			"tolerance must not disable it",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"tolerance_slots":1000`)
	require.Contains(t, logs.String(), `"eb_staleness_slots":5`)
}

// TestForgeIgnoresEndorserBlockSlotBeyondTheCurrentSlot pins the clamp. A
// corroborated slot ahead of the current slot means this node's clock is
// behind, which is a different fault; laundering it into a forge refusal would
// let a clock skew silently stop block production.
func TestForgeIgnoresEndorserBlockSlotBeyondTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 310, 309, 309, 0, 400, 0, 0, 5, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipEbAhead),
	)
	require.Contains(t, logs.String(), `"eb_slot":0`)
}

// TestForgeStalenessDoesNotBlockWithoutAReference pins that a forge with no
// upstream reference proceeds rather than being refused. A node with no
// published target must not be prevented from forging by a bound that has
// nothing to measure against.
func TestForgeStalenessDoesNotBlockWithoutAReference(t *testing.T) {
	var logs bytes.Buffer
	forger, builder := newStalenessTestForger(
		t, 300, 299, 299, 0, 0, 0, 5, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls, "no reference must not block forging")
	require.Contains(t, logs.String(), `"msg":"forge context"`)
}

// TestForgeUpstreamStalenessIgnoresUnknownUpstreamTarget pins the state #4013
// made reachable here.
//
// LedgerState publishes (0, true) from UpstreamSyncStatus for the whole window
// between an active-connection switch and the newly selected peer's first
// admitted trusted header. Before #4013 the sync gate refused every slot in
// that window outright, so this bound never saw it. #4013 bounded that branch
// by the local tip's lag instead -- a node at tip forges, and the header it
// produces is what ends the window -- so a node at tip now arrives at this
// gate with a live upstream and a target of zero.
//
// The bound must stay quiet there. It does, because upstreamTarget >
// newestKnown cannot hold for a zero target, and NOT because anything below
// refuses the slot first. Substituting a value for the missing target -- the
// admitted header frontier was the obvious candidate, and an earlier revision
// did it -- would refuse leader slots in exactly the window #4013 opened them
// up for, which is the #4010 wedge again for any operator who set the knob.
func TestForgeUpstreamStalenessIgnoresUnknownUpstreamTarget(t *testing.T) {
	var logs bytes.Buffer
	block := newForgerTestBlock(300, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode: ModeProduction,
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: forgerTestSlotClock{
			// At tip: the previous slot's block, one slot behind the
			// current slot, so #4013's local-lag bound passes it through.
			currentSlot:      300,
			chainTipSlot:     299,
			frontierExplicit: true,
			frontierSlot:     299,
			// The reachable unknown-target state: active upstream, no
			// target published yet.
			upstreamTipSlot:   0,
			upstreamActive:    true,
			slotsPerKESPeriod: 100,
		},
		// The knob is ON and tight. A bound that treated the unknown target
		// as evidence would fire here.
		ForgeUpstreamStalenessSlots: 5,
		PromRegistry:                prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"an unpublished upstream target is not evidence of staleness; the "+
			"node is at tip and its header is what ends that window",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
		"the sync gate must not claim this slot either; if it does, this "+
			"test is no longer exercising the staleness bound",
	)
}

// TestForgeUpstreamStalenessIsOffByDefault is the regression guard for a
// default-on bound that forfeited leader slots during ordinary operation.
//
// newestKnown counts BLOCKS this node holds; the upstream target is published
// when a HEADER is admitted (recordAdmittedHeaderFrontier advances both the
// admitted frontier and the published target). Between a header's admission at
// slot S and its body being applied, the target reads S while newestKnown is
// still the previous block's slot -- a difference equal to the inter-block gap,
// which is normal operation, not staleness.
//
// With the bound defaulted to 5 every gap above 5 slots refused the leader
// slot: for exponentially distributed gaps with a 20-slot mean that is roughly
// 78% of blocks, on every network. So the default is 0 (disabled), and this
// test pins that a forger built without the knob forges in exactly that shape.
func TestForgeUpstreamStalenessIsOffByDefault(t *testing.T) {
	var logs bytes.Buffer
	// The ordinary header-ahead-of-body window: a header at 318 has been
	// admitted and published as the target, our newest BLOCK is still 299.
	forger, builder := newStalenessTestForger(
		t, 300, 299, 299, 318, 0, 0, 0, 0, &logs,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(
		t,
		1,
		builder.calls,
		"a header admitted ahead of its body is normal operation; with no "+
			"bound configured it must not cost the leader slot",
	)
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipAppliedStale),
	)
}
