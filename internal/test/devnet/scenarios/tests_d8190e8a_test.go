//go:build linux && devnet

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

package scenarios

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/stretchr/testify/require"
)

// minTxBearingBodySize is the block body size above which a block is
// taken to be carrying transactions.
//
// An empty Conway block body is a four-element CBOR array of empty
// arrays — a handful of bytes. The smallest transaction txpump submits
// is a signed payment well over 200 bytes. Anything above this threshold
// therefore cannot be an empty block, and any block with a transaction
// in it clears the threshold comfortably. Body size is used because the
// mixed cardano-node topology publishes no node-to-client port, so the
// Node-to-Node header stream is the only evidence available in both
// topologies.
const minTxBearingBodySize = 128

// TestAcceleratedScenarioTimeline is the fast, event-driven scenario used
// for scheduled and release integration evidence.
//
// One timeline covers readiness, block and transaction propagation, chain
// agreement, an epoch transition, a controlled peer interruption with
// recovery, and a relay restart. Every wait is a condition over observed
// ChainSync events bounded by a deadline on that single shared clock, so
// phases hand their slack forward instead of each one paying for its own
// relative slot window. The canonical-timing DevNet keeps the statistical
// block-rate and long-wall-clock checks; nothing here samples a rate.
//
// It runs in both supported topologies — all-Dingo producers plus relay,
// and Dingo beside cardano-node — because it derives every node it acts
// on from LoadEndpoints rather than naming containers.
func TestAcceleratedScenarioTimeline(t *testing.T) {
	if os.Getenv("DEVNET_ACCELERATED") != "1" {
		t.Skip(
			"accelerated scenario requires the accelerated network; run" +
				" internal/test/devnet/run-tests.sh --accelerated",
		)
	}

	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load the devnet network spec")
	require.NoError(t, cfg.Validate(), "network spec is not internally valid")

	plan, err := devnet.NewScenarioPlan(cfg)
	require.NoError(t, err)
	require.True(t, plan.FitsReferenceBudget(),
		"this network's timing cannot meet the reference runner budget"+
			" (%s > %s); use an accelerated spec",
		plan.Total(), devnet.ReferenceRunnerBudget,
	)
	t.Log(plan.String())

	endpoints := devnet.LoadEndpoints()
	require.NotEmpty(t, endpoints)

	ctl, err := devnet.NewNodeControl(t.Logf)
	require.NoError(t, err,
		"the scenario must be able to interrupt and restart nodes")

	// One clock for the whole scenario, with the hard timeout the issue
	// asks for wrapped around it.
	start := time.Now()
	scenarioCtx, cancelScenario := context.WithTimeout(
		context.Background(), plan.HardTimeout,
	)
	defer cancelScenario()

	observers := devnet.StartObservers(
		scenarioCtx, endpoints, cfg.NetworkMagic, t.Logf,
	)
	defer observers.Stop()
	group := observers.Group()

	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		// Preserve the evidence before the network is torn down: what
		// each node's chain actually did, container status, and logs.
		capCtx, capCancel := context.WithTimeout(
			context.Background(), time.Minute,
		)
		defer capCancel()
		services := make([]string, 0, len(endpoints))
		for _, ep := range endpoints {
			if ep.Container != "" {
				services = append(services, ep.Container)
			}
		}
		ctl.CaptureFailureArtifacts(
			capCtx, "accelerated-timeline", group.Snapshots(), services,
		)
	})

	// phase returns a context that expires at the phase's deadline on the
	// shared timeline, not after a fresh per-phase duration.
	phase := func(name string) (context.Context, context.CancelFunc) {
		ph, ok := plan.Phase(name)
		require.True(t, ok, "unknown phase %q", name)
		remaining := time.Until(start.Add(ph.Deadline))
		t.Logf(
			"=== phase %s: deadline %s from start (%s remaining)",
			name, ph.Deadline, remaining.Round(time.Millisecond),
		)
		return context.WithDeadline(scenarioCtx, start.Add(ph.Deadline))
	}

	// --- readiness ---------------------------------------------------
	readyCtx, cancel := phase(devnet.PhaseReadiness)
	require.NoError(t, group.Await(readyCtx,
		"every node accepted a chain-sync session and sent a header",
		func(snaps []devnet.ChainSnapshot) bool {
			for _, s := range snaps {
				if !s.Connected || s.RollForwards == 0 {
					return false
				}
			}
			return true
		}))
	cancel()
	t.Logf("readiness: %d nodes streaming chain events", len(endpoints))

	// --- block and transaction propagation ---------------------------
	// Baseline at the highest tip the *nodes* report, not the highest an
	// observer has replayed to. An observer intersects at origin and
	// walks history first, so a baseline from its own tip would let an
	// already-replayed header pass as a newly forged one.
	baseline := devnet.MaxServerTipSlot(group.Snapshots())
	propCtx, cancel := phase(devnet.PhasePropagation)

	var propagated devnet.ObservedHeader
	require.NoError(t, group.Await(propCtx,
		fmt.Sprintf("a block forged above slot %d reaches every node",
			baseline),
		func(snaps []devnet.ChainSnapshot) bool {
			h, ok := devnet.AgreedHeaderAbove(snaps, baseline)
			if ok {
				propagated = h
			}
			return ok
		}))
	t.Logf(
		"block propagation: slot %d block %d reached all %d nodes",
		propagated.Slot, propagated.BlockNumber, len(endpoints),
	)

	var carrying devnet.ObservedHeader
	require.NoError(t, group.Await(propCtx,
		"a block carrying transactions reaches every node",
		func(snaps []devnet.ChainSnapshot) bool {
			from := baseline
			for {
				h, ok := devnet.AgreedHeaderAbove(snaps, from)
				if !ok {
					return false
				}
				if h.BodySize >= minTxBearingBodySize {
					carrying = h
					return true
				}
				from = h.Slot
			}
		}))
	cancel()
	t.Logf(
		"transaction propagation: slot %d carries a %d-byte body on all"+
			" nodes", carrying.Slot, carrying.BodySize,
	)

	// --- chain agreement ---------------------------------------------
	agreeCtx, cancel := phase(devnet.PhaseAgreement)
	requireAgreement(t, agreeCtx, group, "steady state")
	cancel()
	requireBoundedRollbacks(t, group, cfg)

	// --- epoch transition --------------------------------------------
	// Derive the boundary from where the chain actually is, so the
	// scenario crosses a real transition whether it attached at genesis
	// or to a network that has been up for a while. NextEpochBoundary is
	// strictly greater than its input, so taking the nodes' reported tip
	// puts the target in the future rather than on a boundary the chain
	// has already passed.
	boundary := cfg.NextEpochBoundary(
		devnet.MaxServerTipSlot(group.Snapshots()),
	)
	target := boundary + plan.EpochMargin
	epochCtx, cancel := phase(devnet.PhaseEpochTransition)
	require.NoError(t, group.Await(epochCtx,
		fmt.Sprintf(
			"every node past slot %d, i.e. %d slots beyond the epoch"+
				" boundary at %d", target, plan.EpochMargin, boundary),
		func(snaps []devnet.ChainSnapshot) bool {
			return devnet.MinTipSlot(snaps) >= target
		}))
	// Agreement on a header built after the boundary exercises the new
	// epoch nonce, not just the boundary block itself.
	require.NoError(t, group.Await(epochCtx,
		fmt.Sprintf("every node agrees on a header above the epoch"+
			" boundary at %d", boundary),
		func(snaps []devnet.ChainSnapshot) bool {
			// AgreedHeaderAbove only returns headers above its bound, so
			// finding one is already proof of agreement past the boundary.
			_, ok := devnet.AgreedHeaderAbove(snaps, boundary)
			return ok
		}))
	cancel()
	t.Logf("epoch transition: crossed the boundary at slot %d", boundary)

	// --- controlled peer interruption and recovery -------------------
	// The last producer is deliberately not the one txpump submits to,
	// so mempool traffic keeps flowing through the outage.
	victim := interruptionVictim(t, endpoints)
	interruptCtx, cancel := phase(devnet.PhasePeerInterruption)
	runOutage(t, interruptCtx, ctl, group, plan, victim, "peer interruption")
	cancel()

	// --- relay restart -----------------------------------------------
	relay := relayEndpoint(t, endpoints)
	relayCtx, cancel := phase(devnet.PhaseRelayRestart)
	runOutage(t, relayCtx, ctl, group, plan, relay, "relay restart")
	cancel()

	requireBoundedRollbacks(t, group, cfg)
	t.Logf(
		"accelerated scenario completed in %s (hard timeout %s)",
		time.Since(start).Round(time.Millisecond), plan.HardTimeout,
	)
}

// runOutage stops a node, holds it down until the rest of the network has
// visibly moved on, brings it back, and requires it to rejoin and
// reconverge.
//
// The outage length is measured in observed blocks rather than wall
// clock: that keeps the disruption equally meaningful on a fast and a
// slow runner, and keeps it inside what the security parameter can
// reconcile.
func runOutage(
	t *testing.T,
	ctx context.Context,
	ctl *devnet.NodeControl,
	group *devnet.ChainGroup,
	plan *devnet.ScenarioPlan,
	ep devnet.NodeEndpoint,
	label string,
) {
	t.Helper()
	require.NotEmpty(t, ep.Container,
		"%s: %s has no container to control", label, ep.Name)

	chain := group.Chain(ep.Name)
	require.NotNil(t, chain, "%s: no observer for %s", label, ep.Name)

	before := chain.Snapshot()
	survivorBlock := maxBlockExcluding(group.Snapshots(), ep.Name)

	require.NoError(t, ctl.Stop(ctx, ep.Container),
		"%s: stopping %s", label, ep.Container)

	// The dropped session is the observed evidence that the interruption
	// actually happened, rather than an assumption that the stop worked.
	require.NoError(t, chain.Await(ctx,
		label+": chain-sync session to "+ep.Name+" dropped",
		func(s devnet.ChainSnapshot) bool {
			return s.Disconnects > before.Disconnects
		}))

	require.NoError(t, group.Await(ctx,
		fmt.Sprintf("%s: the network advances %d blocks while %s is down",
			label, plan.OutageBlocks, ep.Name),
		func(snaps []devnet.ChainSnapshot) bool {
			return maxBlockExcluding(snaps, ep.Name) >=
				survivorBlock+plan.OutageBlocks
		}))

	require.NoError(t, ctl.Start(ctx, ep.Container),
		"%s: starting %s", label, ep.Container)

	require.NoError(t, group.Await(ctx,
		label+": "+ep.Name+" rejoins and catches up to the network",
		func(snaps []devnet.ChainSnapshot) bool {
			target := maxBlockExcluding(snaps, ep.Name)
			for _, s := range snaps {
				if s.Node != ep.Name {
					continue
				}
				// Two blocks of slack: the network keeps producing while
				// the node syncs, so requiring an exact match would chase
				// a moving tip forever.
				return s.Connected && s.Tip.BlockNumber+2 >= target
			}
			return false
		}))

	requireAgreement(t, ctx, group, label+": after recovery")

	// Agreement alone only says the nodes converged; it does not say the
	// network resumed producing. Without this the next phase can stack a
	// second outage onto a network that has converged but stalled, and
	// then blame the second outage for the stall. Requiring forward
	// progress first means each disruption starts from a healthy
	// baseline.
	resumed := devnet.MaxBlockNumber(group.Snapshots())
	require.NoError(t, group.Await(ctx,
		fmt.Sprintf("%s: block production resumed after recovery"+
			" (past block %d)", label, resumed),
		func(snaps []devnet.ChainSnapshot) bool {
			return devnet.MaxBlockNumber(snaps) > resumed
		}))

	after := chain.Snapshot()
	t.Logf(
		"%s: %s recovered (connects %d->%d, tip slot %d, block %d)",
		label, ep.Name, before.Connects, after.Connects,
		after.Tip.SlotNumber, after.Tip.BlockNumber,
	)
}

// requireAgreement waits until every node agrees on the chain at the
// deepest slot they have all observed.
func requireAgreement(
	t *testing.T,
	ctx context.Context,
	group *devnet.ChainGroup,
	label string,
) {
	t.Helper()
	var result devnet.AgreementResult
	require.NoError(t, group.Await(ctx,
		label+": nodes agree at their deepest common slot",
		func(snaps []devnet.ChainSnapshot) bool {
			res, ok := devnet.AgreementAtDeepestCommonSlot(snaps)
			if !ok {
				return false
			}
			result = res
			return res.Agree
		}))
	t.Logf("%s: %s", label, result)
}

// requireBoundedRollbacks fails if any node reverted more than the
// security parameter allows, which would mean chain selection went
// further back than Praos permits rather than merely recovering.
func requireBoundedRollbacks(
	t *testing.T,
	group *devnet.ChainGroup,
	cfg *devnet.DevNetConfig,
) {
	t.Helper()
	for _, s := range group.Snapshots() {
		require.LessOrEqualf(t, s.MaxRollbackDepth, cfg.SecurityParam,
			"%s rolled back %d headers, beyond k=%d",
			s.Node, s.MaxRollbackDepth, cfg.SecurityParam,
		)
	}
}

// maxBlockExcluding returns the highest observed block height among every
// node other than the named one.
func maxBlockExcluding(snaps []devnet.ChainSnapshot, node string) uint64 {
	var maxBlock uint64
	for _, s := range snaps {
		if s.Node == node {
			continue
		}
		if s.Tip.BlockNumber > maxBlock {
			maxBlock = s.Tip.BlockNumber
		}
	}
	return maxBlock
}

// interruptionVictim returns the producer to stop during the peer
// interruption phase: the last one in the endpoint list.
//
// txpump submits to the first producer in both topologies, so choosing the
// last one keeps transactions flowing through the outage. That ordering is
// a property of LoadEndpoints and docker-compose rather than something the
// types enforce, so it is asserted here instead of assumed: if the list is
// ever reordered so that the victim is also the submission target, this
// fails loudly rather than silently removing the mempool traffic the phase
// depends on.
func interruptionVictim(
	t *testing.T,
	endpoints []devnet.NodeEndpoint,
) devnet.NodeEndpoint {
	t.Helper()
	var first, last devnet.NodeEndpoint
	for _, ep := range endpoints {
		if ep.Role != "producer" {
			continue
		}
		if first.Name == "" {
			first = ep
		}
		last = ep
	}
	require.NotEmpty(t, last.Name, "no producer endpoint configured")
	require.NotEqual(t, first.Name, last.Name,
		"the interruption victim must not be the producer txpump submits"+
			" to, or the outage also stops mempool traffic")
	return last
}

func relayEndpoint(
	t *testing.T,
	endpoints []devnet.NodeEndpoint,
) devnet.NodeEndpoint {
	t.Helper()
	for _, ep := range endpoints {
		if ep.Role == "relay" {
			return ep
		}
	}
	t.Fatal("no relay endpoint configured")
	return devnet.NodeEndpoint{}
}

// TestBasicBlockForging verifies that the Dingo producer forges blocks
// and that all nodes in the DevNet reach consensus.
//
// This test:
//  1. Connects to all nodes in the active network (all producers plus the
//     relay - three Dingo producers plus a Dingo relay in dingo mode, or a
//     Dingo producer, cardano-node producer, and cardano-node relay in
//     conformance mode)
//  2. Waits for Dingo to advance past genesis (bootstrap grace period)
//  3. Waits for the chain to advance 10 slots beyond the current tip
//  4. Verifies that Dingo's selected chain advances
//  5. Verifies that all nodes agree on the chain tip within tolerance
func TestBasicBlockForging(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")
	t.Logf(
		"devnet config: activeSlotsCoeff=%.2f slotLength=%.1fs"+
			" epochLength=%d securityParam=%d networkMagic=%d"+
			" expectedBlockTime=%s",
		cfg.ActiveSlotsCoeff, cfg.SlotLength,
		cfg.EpochLength, cfg.SecurityParam, cfg.NetworkMagic,
		cfg.ExpectedBlockTime(),
	)

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	// Step 1: Wait for all nodes to become reachable
	t.Log("waiting for all DevNet nodes to become ready...")
	h.WaitForAllNodesReady(60 * time.Second)
	t.Log("all nodes are ready")

	// Step 2: Wait for Dingo to advance past genesis. On a cold start
	// dingo needs time to connect peers, sync, and compute the leader
	// schedule before it can forge or relay blocks.
	dingoEndpoint := h.DingoNode()
	t.Log("waiting for Dingo to advance past genesis...")
	bootstrapTimeout := cfg.SlotDuration()*120 + cfg.ExpectedBlockTime()*10
	h.WaitForNodeSlot(dingoEndpoint, 1, bootstrapTimeout)

	// Step 3: Record the initial tip from the Dingo producer
	initialTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get initial Dingo chain tip")
	t.Logf(
		"initial Dingo tip: slot=%d block=%d",
		initialTip.SlotNumber, initialTip.BlockNumber,
	)

	// Step 4: Wait for at least 10 slots beyond the initial tip.
	// Timeout: 10 slots of wall-clock time + margin to account for
	// fork recovery pauses that occur when both producers create blocks
	// for the same slot.
	const advanceSlots = 10
	targetSlot := initialTip.SlotNumber + advanceSlots
	slotTimeout := time.Duration(advanceSlots)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	t.Logf("waiting for Dingo to advance to slot %d...", targetSlot)
	h.WaitForNodeSlot(dingoEndpoint, targetSlot, slotTimeout)
	t.Logf("Dingo has reached slot %d", targetSlot)

	// Step 5: Verify Dingo's selected chain advances. Valid Praos
	// tiebreaks may roll back an early local block and leave the selected
	// chain at the same block height at the target slot, so wait for
	// eventual height growth instead of sampling once.
	growthTimeout := cfg.ExpectedBlockTime() * 20
	dingoTip := h.WaitForNodeBlockAbove(
		dingoEndpoint,
		initialTip.BlockNumber,
		growthTimeout,
	)
	t.Logf(
		"Dingo tip after growth: slot=%d block=%d",
		dingoTip.SlotNumber, dingoTip.BlockNumber,
	)

	// Step 6: Verify all nodes converge within 3*securityParam slots.
	// The 3x multiplier accounts for CI variability and the fact that
	// in a multi-producer DevNet, dingo and cardano-node may maintain
	// different chain tips during active block production due to
	// propagation delays and competing slot leaders. A tighter
	// tolerance (e.g. 1x) causes false failures in CI.
	t.Log("verifying chain consensus across all nodes...")
	tolerance := 3 * cfg.SecurityParam
	h.VerifyChainConsensus(tolerance, cfg.ExpectedBlockTime()*20)
	t.Log("all nodes are in consensus")
}

// TestDingoChainAdvances is a simpler test that just verifies
// the Dingo node's selected chain is advancing.
func TestDingoChainAdvances(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	dingoEndpoint := h.DingoNode()

	// Wait for the chain to actually start. Waiting for "slot >= 0" was
	// a no-op — every tip satisfies it, including the slot 0 / block 0 a
	// node reports for ~30s before genesis — so the slot-derived budget
	// below was charged against the pre-genesis wait and this test only
	// passed when an earlier test in the package had already absorbed it.
	h.WaitForChainStart(devnet.ChainStartTimeout)

	// Get initial tip
	initialTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get initial tip")

	// Wait for the chain to advance by at least 10 slots.
	// Timeout: 10 slots of wall-clock time + 5 expected block times margin.
	const advanceSlots = 10
	targetSlot := initialTip.SlotNumber + advanceSlots
	slotTimeout := time.Duration(advanceSlots)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForNodeSlot(dingoEndpoint, targetSlot, slotTimeout)

	// Verify selected-chain height advances. Do not require the block
	// number to be greater at a single target slot: Praos selection
	// compatible with the reference implementation can roll back an early
	// block in favor of an equal-height lower-VRF competitor before the
	// chain grows again.
	growthTimeout := cfg.ExpectedBlockTime() * 20
	newTip := h.WaitForNodeBlockAbove(
		dingoEndpoint,
		initialTip.BlockNumber,
		growthTimeout,
	)
	require.Greater(t, newTip.SlotNumber, initialTip.SlotNumber,
		"Dingo chain should have advanced",
	)

	t.Logf(
		"Dingo chain advanced from slot %d to %d (blocks: %d -> %d)",
		initialTip.SlotNumber, newTip.SlotNumber,
		initialTip.BlockNumber, newTip.BlockNumber,
	)
}

// TestChainGrowthRate verifies that the chain is growing at an expected
// rate over a measurement window.
//
// With activeSlotsCoeff=0.4 and slotLength=1s, each slot has a 40% chance
// of producing a block. With pools sharing stake equally the per-pool
// leader probability per slot is approximately 1-(1-f)^σ; illustrative for
// the 2-pool conformance case, σ=0.5 gives ≈0.225, while the 3-pool dingo
// case (σ=1/3) gives ≈0.157. The network-wide probability is 1-(1-f)^1 =
// 0.4 regardless of pool count, since network-wide rate is activeSlotsCoeff.
// In a 100-slot window we therefore expect ~40 blocks network-wide. We
// require at least 8 (20% of expected) as a conservative lower bound; this
// threshold is pool-count independent. This accounts for: random VRF
// variance, chain rollbacks (multiple pools can win the same slot),
// catch-up delays after rollback/resync, and the fact that a single
// endpoint only sees its own view of the chain which may lag behind the
// network tip.
func TestChainGrowthRate(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")
	t.Logf(
		"devnet config: activeSlotsCoeff=%.2f slotLength=%.1fs"+
			" epochLength=%d securityParam=%d expectedBlockTime=%s",
		cfg.ActiveSlotsCoeff, cfg.SlotLength,
		cfg.EpochLength, cfg.SecurityParam, cfg.ExpectedBlockTime(),
	)

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	dingoEndpoint := h.DingoNode()

	// Wait for the chain to stabilize at slot 50.
	// Timeout: 50 slots + 5 expected block times for margin, measured
	// from chain start — readiness alone is reached before genesis, and
	// the budget below only counts chain time.
	h.WaitForAllNodesReady(60 * time.Second)
	h.WaitForChainStart(devnet.ChainStartTimeout)
	const stabilizeSlot = 50
	stabilizeTimeout := time.Duration(stabilizeSlot)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForNodeSlot(dingoEndpoint, stabilizeSlot, stabilizeTimeout)

	// Record the starting point
	startTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get start tip")
	t.Logf(
		"start tip: slot=%d block=%d",
		startTip.SlotNumber, startTip.BlockNumber,
	)

	// Wait for a measurement window of 100 slots. A longer window
	// reduces the impact of random variance in VRF-based leader election.
	// Timeout: 100 slots + 5 expected block times for margin.
	const measureSlots = 100
	targetSlot := startTip.SlotNumber + measureSlots
	measureTimeout := time.Duration(measureSlots)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5

	// With activeSlotsCoeff=0.4 we expect ~40 blocks in 100 slots.
	// Require at least 8 (20% of expected) as a conservative lower bound
	// that accounts for random variance and possible rollbacks.
	const minBlocks = uint64(8)
	minBlock := startTip.BlockNumber + minBlocks
	var endTip devnet.ChainTip
	require.Eventually(t, func() bool {
		tip, err := h.GetChainTip(dingoEndpoint)
		if err != nil {
			t.Logf("growth check: error querying %s: %v",
				dingoEndpoint.Name, err)
			return false
		}
		endTip = tip
		t.Logf(
			"growth check: %s at slot %d, block %d",
			dingoEndpoint.Name, tip.SlotNumber, tip.BlockNumber,
		)
		return tip.SlotNumber >= targetSlot &&
			tip.BlockNumber >= minBlock
	}, measureTimeout, 2*time.Second,
		"%s did not reach slot %d and block %d within %s",
		dingoEndpoint.Name, targetSlot, minBlock, measureTimeout,
	)
	t.Logf(
		"end tip: slot=%d block=%d",
		endTip.SlotNumber, endTip.BlockNumber,
	)

	// The require.Eventually above already guarantees
	// endTip.BlockNumber >= startTip.BlockNumber + minBlocks, so the
	// subtraction below cannot underflow and a rollback below the start tip
	// would have failed the Eventually rather than reaching here.
	blocksProduced := endTip.BlockNumber - startTip.BlockNumber
	slotsElapsed := endTip.SlotNumber - startTip.SlotNumber
	if slotsElapsed == 0 {
		t.Fatal("no slots elapsed during measurement window")
	}
	t.Logf(
		"chain growth: %d blocks in %d slots (%.1f%% slot utilization,"+
			" expected ~%.1f%%)",
		blocksProduced,
		slotsElapsed,
		float64(blocksProduced)/float64(slotsElapsed)*100,
		cfg.ExpectedBlocksPerSlot()*100,
	)

	require.GreaterOrEqual(t, blocksProduced, minBlocks,
		"chain should produce at least %d blocks in %d slots "+
			"(activeSlotsCoeff=%.2f)",
		minBlocks, measureSlots, cfg.ActiveSlotsCoeff,
	)
}

// TestRelayPropagation verifies that blocks produced by any producer
// reach the relay node. The relay connects to all producers (three in
// dingo mode, two in conformance mode) and should see blocks from all
// of them.
func TestRelayPropagation(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	relayEndpoint := h.Relay()

	// Wait for all nodes including relay, then for the chain to start:
	// the slot-derived timeout below only counts chain time and must not
	// be charged against the pre-genesis wait.
	h.WaitForAllNodesReady(60 * time.Second)
	h.WaitForChainStart(devnet.ChainStartTimeout)

	// Wait for the relay to advance to slot 30.
	// Timeout: 30 slots + 5 expected block times for margin.
	const targetSlot = uint64(30)
	timeout := time.Duration(targetSlot)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForNodeSlot(relayEndpoint, targetSlot, timeout)

	relayTip, err := h.GetChainTip(relayEndpoint)
	require.NoError(t, err, "failed to get relay chain tip")
	t.Logf(
		"relay tip: slot=%d block=%d",
		relayTip.SlotNumber, relayTip.BlockNumber,
	)

	// The relay should have received blocks (it doesn't produce its own)
	require.Greater(t, relayTip.BlockNumber, uint64(0),
		"relay should have received blocks from producers",
	)
}

// TestSustainedConsensus verifies that all nodes maintain consensus
// over multiple checkpoint intervals. This catches intermittent
// consensus failures that might not appear in a single-point check.
func TestSustainedConsensus(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	h.WaitForAllNodesReady(60 * time.Second)
	// The checkpoints below are relative to this baseline but their
	// timeouts are not, so the chain has to be live before it is taken.
	h.WaitForChainStart(devnet.ChainStartTimeout)

	// Get the current tip to compute relative checkpoints.
	// The chain may already be well past genesis when the tests run.
	baseTip, err := h.GetChainTip(h.DingoNode())
	require.NoError(t, err, "failed to get initial tip")
	baseSlot := baseTip.SlotNumber

	// Check consensus at 3 relative checkpoints (20, 50, 100 slots ahead).
	// Each checkpoint timeout is offset * slotDuration + margin.
	offsets := []uint64{20, 50, 100}
	for _, offset := range offsets {
		targetSlot := baseSlot + offset
		t.Logf("checking consensus at slot %d...", targetSlot)
		slotTimeout := time.Duration(offset)*cfg.SlotDuration() +
			cfg.ExpectedBlockTime()*5
		h.WaitForSlot(targetSlot, slotTimeout)

		// With activeSlotsCoeff=0.4 and 1s slots, propagation between
		// two producers through a relay can introduce delays. Use
		// 3*securityParam as the tolerance (same as the stability window
		// divided by f, which is the maximum acceptable fork length).
		// Scale the convergence timeout by the checkpoint offset so
		// later checkpoints (which may require longer resync/rollback
		// recovery) get proportionally more time.
		tolerance := 3 * cfg.SecurityParam
		convergenceTimeout := cfg.ExpectedBlockTime() *
			time.Duration(max(20, offset/2))
		h.VerifyChainConsensus(tolerance, convergenceTimeout)
		t.Logf("consensus verified at slot %d", targetSlot)
	}
}

// TestEpochBoundaryConsensus verifies the producers remain in consensus
// across at least one full epoch transition. With epochLength=500, this
// exercises the candidate-nonce freeze, lab nonce roll, and VRF
// verification with the new epoch nonce — the same code path that has
// been observed to wedge on preview after a Mithril bootstrap.
//
// Failure mode this catches: a producer's tip falls behind the others by
// more than slotTolerance after the boundary because every header in
// the new epoch fails VRF verification (chain stops advancing on that
// producer while the others continue forging).
func TestEpochBoundaryConsensus(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(
		t, err, "failed to load devnet config from testnet.yaml",
	)
	t.Logf(
		"devnet config: epochLength=%d slotLength=%.1fs"+
			" activeSlotsCoeff=%.2f securityParam=%d",
		cfg.EpochLength, cfg.SlotLength,
		cfg.ActiveSlotsCoeff, cfg.SecurityParam,
	)

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	h.WaitForAllNodesReady(60 * time.Second)
	// The budget below covers 1.4 epochs of chain time and nothing more.
	// Without this gate it is also charged the ~30s spent waiting for
	// genesis, which leaves under a second of margin: an isolated run
	// measured 724.04s against its own 725s timeout.
	h.WaitForChainStart(devnet.ChainStartTimeout)

	// Target a slot well past the first epoch boundary so we have
	// blocks on both sides of the rollover. 1.4 * epochLength puts
	// us ~200 slots into epoch 1.
	targetSlot := cfg.EpochLength + cfg.EpochLength*2/5
	timeout := time.Duration(targetSlot)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*10
	t.Logf(
		"waiting for slot %d (past epoch boundary at slot %d), timeout %s",
		targetSlot, cfg.EpochLength, timeout,
	)

	observed := h.DingoNode()
	h.WaitForNodeSlot(observed, targetSlot, timeout)

	// Both producers must agree on the chain after crossing the
	// boundary. K is the initial catch-up tolerance; anything larger
	// means one side has stalled, which is what the bug looks like.
	tolerance := cfg.SecurityParam
	convergeTimeout := cfg.ExpectedBlockTime() * 60
	h.VerifyChainConsensus(tolerance, convergeTimeout)

	// Once the boundary has passed and the nodes have had one K window to
	// catch up, require near-immediate convergence. This catches Dingo
	// staying materially behind cardano-node after new-epoch VRF checks.
	tightTolerance := uint64(1)
	tightConvergeTimeout := cfg.ExpectedBlockTime() * 10
	h.VerifyChainConsensus(tightTolerance, tightConvergeTimeout)

	// Every producer's tip slot must be past the boundary. If VRF
	// verification fails on every new-epoch header, a node's chain stalls
	// a few slots before slot=epochLength.
	for _, p := range h.Producers() {
		tip, terr := h.GetChainTip(p)
		require.NoError(t, terr, "failed to get %s tip after boundary", p.Name)
		t.Logf(
			"post-boundary: %s slot=%d block=%d",
			p.Name,
			tip.SlotNumber,
			tip.BlockNumber,
		)
		require.Greater(t, tip.SlotNumber, cfg.EpochLength,
			"%s did not advance past first epoch boundary "+
				"(stuck before slot %d) - likely VRF verification "+
				"failure on epoch 1 headers", p.Name, cfg.EpochLength)
	}
}
