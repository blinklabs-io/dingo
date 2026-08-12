//go:build devnet

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
