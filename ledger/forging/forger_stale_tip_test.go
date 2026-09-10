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
// primaryTipSlot is this node's own primary chain tip; chainTipSlot is the
// ledger-applied tip a forged block would be built on.
func newStaleTipTestForger(
	t *testing.T,
	currentSlot, chainTipSlot, primaryTipSlot uint64,
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
			currentSlot:        currentSlot,
			chainTipSlot:       chainTipSlot,
			primaryTipExplicit: true,
			primaryTipSlot:     primaryTipSlot,
			slotsPerKESPeriod:  100,
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
	// Applied tip 83 slots behind the primary chain tip: the field case.
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
			// rule were ever tightened back to primaryTipExplicit alone the
			// primary tip would mirror the applied tip, the gap would
			// collapse to 0, and this test would still pass while the
			// tolerance knob had stopped being honoured.
			primaryTipExplicit: true,
			primaryTipSlot:     120,
			slotsPerKESPeriod:  100,
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
// forging tens of slots behind its own primary tip reported a gap of exactly
// 0 -- the one case where the gauge mattered was the one case it could not
// show.
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
	appliedHash, primaryTipHash []byte,
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
			currentSlot:        200,
			chainTipSlot:       100,
			chainTipHash:       appliedHash,
			primaryTipExplicit: true,
			primaryTipSlot:     100,
			primaryTipHash:     primaryTipHash,
			slotsPerKESPeriod:  100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsOnEqualSlotPrimaryTipDivergence is the equal-slot fork the
// slot gap cannot see. Chain selection replaced the block at the applied tip's
// slot with a competing one at the SAME slot that the ledger has not applied,
// so the gap is 0 while the ledger state still describes the block that was
// replaced -- the builder would parent the block on one chain position while
// its transactions, protocol parameters and leader eligibility came from
// another.
func TestForgeSkipsOnEqualSlotPrimaryTipDivergence(t *testing.T) {
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

// TestForgeProceedsWhenPrimaryTipMatchesAppliedTip pins the other side: the
// same slot with the same hash is the normal caught-up state and must forge.
func TestForgeProceedsWhenPrimaryTipMatchesAppliedTip(t *testing.T) {
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
		applied, primaryTip []byte
	}{
		"primary chain tip hash unknown": {
			applied:    bytes.Repeat([]byte{0xAA}, 32),
			primaryTip: []byte{},
		},
		"applied hash unknown": {
			applied:    []byte{},
			primaryTip: bytes.Repeat([]byte{0xBB}, 32),
		},
		"both at genesis": {applied: []byte{}, primaryTip: []byte{}},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			forger, builder, _ := newEqualSlotForkTestForger(
				t,
				tc.applied,
				tc.primaryTip,
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
// Four series: the three post-leader-check disagreements (slot_gap,
// primary_tip_hash_diverged, primary_tip_behind_applied) plus the pre-leader
// -check unapplied_rival_at_leader_slot.
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
		4,
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
	currentSlot, chainTipSlot, primaryTipSlot uint64,
	appliedHash, primaryTipHash []byte,
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
			currentSlot:        currentSlot,
			chainTipSlot:       chainTipSlot,
			chainTipHash:       appliedHash,
			primaryTipExplicit: true,
			primaryTipSlot:     primaryTipSlot,
			primaryTipHash:     primaryTipHash,
			slotsPerKESPeriod:  100,
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder, broadcaster
}

// TestForgeSkipsWhenPrimaryTipAlreadyHasTheCurrentSlot covers the guard that
// asks "does a block already exist at this slot". It compared the current slot
// against the LEDGER-APPLIED tip, but the parent comes from the primary tip,
// so inside the primary tip tolerance a peer's block at the current slot could
// already be on the primary tip while still unapplied. Forging then parents a
// block for slot S on a tip already at slot S -- a non-increasing slot,
// admitted locally and broadcast.
//
// The gap here is 2 slots, well inside the tolerance, so the stale-tip gate
// does not fire and this guard is the only thing that can catch it.
func TestForgeSkipsWhenPrimaryTipAlreadyHasTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		198, // ledger-applied tip, still behind
		200, // primary chain tip already carries a block at the current slot
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
		"must not forge a non-increasing slot on top of the primary chain tip",
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

// TestForgeSkipsWhenPrimaryTipIsAheadOfTheCurrentSlot covers the case that
// falls through every other gate: the applied tip is behind the current slot,
// so the applied-tip comparison passes, but the PRIMARY CHAIN TIP is ahead of
// it. The builder parents on the primary tip, so forging would produce a block
// for slot 200 whose parent already sits at slot 201 -- a block earlier than
// its own parent. Comparing the current slot against the applied tip alone
// cannot see this; comparing against max(applied, primaryTip) can.
func TestForgeSkipsWhenPrimaryTipIsAheadOfTheCurrentSlot(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForger(
		t,
		200, // current slot
		199, // applied tip, behind the current slot
		201, // primary chain tip AHEAD of the current slot
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
		200, // primary chain tip agrees
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

// TestForgeSkipsWhenPrimaryTipIsBehindTheAppliedTip covers the third
// disagreement shape. applyGap is 0 (the primary tip is not ahead) and the
// equal-slot hash check does not apply (the slots differ), so neither existing
// case sees it, yet the ledger describes a chain position ahead of the parent
// the builder would use. The ledger itself recognises this state and
// reconciles it at startup by rolling its tip back to the chain tip.
func TestForgeSkipsWhenPrimaryTipIsBehindTheAppliedTip(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, broadcaster := newStaleTipTestForgerWithLeader(
		t,
		forgerTestLeader{},
		300, // current slot
		200, // ledger-applied tip
		190, // primary chain tip BEHIND the applied tip
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

// TestForgeProceedsWhenPrimaryTipIsUninitialised pins that a node whose
// primary chain has no tip yet -- zero slot, empty hash -- is not caught by
// the primary-tip-behind case and can still forge.
func TestForgeProceedsWhenPrimaryTipIsUninitialised(t *testing.T) {
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForgerWithLeader(
		t,
		forgerTestLeader{},
		300, 200, 0,
		bytes.Repeat([]byte{0xAA}, 32),
		nil, // no primary chain tip hash: chain not initialised
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
