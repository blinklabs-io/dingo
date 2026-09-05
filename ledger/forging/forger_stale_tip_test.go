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
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: forgerTestSlotClock{
			currentSlot:       currentSlot,
			chainTipSlot:      chainTipSlot,
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
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkip),
	)
	// Never silently: the skip is a WARN an operator can alert on.
	require.Contains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs header frontier",
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
	require.Zero(t, testutil.ToFloat64(forger.metrics.forgeStaleTipSkip))
	require.NotContains(
		t,
		logs.String(),
		"forge skip: ledger tip stale vs header frontier",
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
