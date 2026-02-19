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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/devnet"
	"github.com/stretchr/testify/require"
)

// TestChainGrowthRate verifies that the chain is growing at an expected
// rate over a measurement window.
//
// With activeSlotsCoeff=0.2 and slotLength=1s, each slot has a 20% chance
// of producing a block. With 2 pools sharing stake equally the per-pool
// leader probability per slot is approximately 1-(1-f)^σ ≈ 0.106, and the
// network-wide probability is 1-(1-f)^1 = 0.2. In a 50-slot window we
// therefore expect ~10 blocks network-wide. We require at least 5 (50% of
// expected) as a conservative lower bound that accounts for random variance
// and startup delay.
func TestChainGrowthRate(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")
	t.Logf(
		"devnet config: activeSlotsCoeff=%.2f slotLength=%.1fs"+
			" epochLength=%d securityParam=%d",
		cfg.ActiveSlotsCoeff, cfg.SlotLength,
		cfg.EpochLength, cfg.SecurityParam,
	)

	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	dingoEndpoint := endpoints[0]

	// Wait for the chain to stabilize at slot 50.
	// At 1s/slot this is 50 seconds; allow 120s of wall-clock time to
	// account for startup, connection setup, and initial sync.
	h.WaitForAllNodesReady(60 * time.Second)
	const stabilizeSlot = 50
	stabilizeTimeout := time.Duration(stabilizeSlot)*cfg.SlotDuration() +
		30*time.Second
	h.WaitForSlot(stabilizeSlot, stabilizeTimeout)

	// Record the starting point
	startTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get start tip")
	t.Logf(
		"start tip: slot=%d block=%d",
		startTip.SlotNumber, startTip.BlockNumber,
	)

	// Wait for a measurement window of 50 more slots (~50 seconds at 1s/slot).
	// Allow 120s of wall-clock time as a generous margin.
	const measureSlots = 50
	targetSlot := startTip.SlotNumber + measureSlots
	measureTimeout := time.Duration(measureSlots)*cfg.SlotDuration() +
		30*time.Second
	h.WaitForNodeSlot(dingoEndpoint, targetSlot, measureTimeout)

	// Record the ending point
	endTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get end tip")
	t.Logf(
		"end tip: slot=%d block=%d",
		endTip.SlotNumber, endTip.BlockNumber,
	)

	require.GreaterOrEqual(t, endTip.BlockNumber, startTip.BlockNumber,
		"end block number should not be less than start (possible rollback)",
	)
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

	// With activeSlotsCoeff=0.2 we expect ~10 blocks in 50 slots.
	// Require at least 5 (50% of expected) as a conservative lower bound.
	const minBlocks = uint64(5)
	require.GreaterOrEqual(t, blocksProduced, minBlocks,
		"chain should produce at least %d blocks in %d slots "+
			"(activeSlotsCoeff=%.2f)",
		minBlocks, measureSlots, cfg.ActiveSlotsCoeff,
	)
}

// TestRelayPropagation verifies that blocks produced by either producer
// reach the relay node. The relay connects to both producers and should
// see blocks from both.
func TestRelayPropagation(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	relayEndpoint := endpoints[2] // cardano-relay

	// Wait for all nodes including relay
	h.WaitForAllNodesReady(60 * time.Second)

	// Wait for the relay to advance to slot 30.
	// At 1s/slot this is 30 seconds; allow 60s of wall-clock time.
	const targetSlot = uint64(30)
	timeout := time.Duration(targetSlot)*cfg.SlotDuration() + 30*time.Second
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

	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	h.WaitForAllNodesReady(60 * time.Second)

	// Get the current tip to compute relative checkpoints.
	// The chain may already be well past genesis when the tests run.
	baseTip, err := h.GetChainTip(endpoints[0])
	require.NoError(t, err, "failed to get initial tip")
	baseSlot := baseTip.SlotNumber

	// Check consensus at 3 relative checkpoints (20, 50, 100 slots ahead).
	// Each checkpoint timeout is offset * slotDuration + 30s margin.
	offsets := []uint64{20, 50, 100}
	for _, offset := range offsets {
		targetSlot := baseSlot + offset
		t.Logf("checking consensus at slot %d...", targetSlot)
		timeout := time.Duration(offset)*cfg.SlotDuration() + 30*time.Second
		h.WaitForSlot(targetSlot, timeout)

		// Log tips from all nodes
		for _, ep := range endpoints {
			tip, err := h.GetChainTip(ep)
			if err != nil {
				t.Logf(
					"  %s: error getting tip: %v",
					ep.Name, err,
				)
				continue
			}
			t.Logf(
				"  %s: slot=%d block=%d",
				ep.Name, tip.SlotNumber, tip.BlockNumber,
			)
		}

		// With activeSlotsCoeff=0.2 and 1s slots, propagation between
		// two producers through a relay can introduce delays. Use
		// 3*securityParam as the tolerance (same as the stability window
		// divided by f, which is the maximum acceptable fork length).
		tolerance := 3 * cfg.SecurityParam
		h.VerifyChainConsensus(tolerance)
		t.Logf("consensus verified at slot %d", targetSlot)
	}
}

// TestCardanoProducerChainAdvances verifies the cardano-node producer
// is also forging blocks, serving as a baseline comparison.
func TestCardanoProducerChainAdvances(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	cardanoEndpoint := endpoints[1] // cardano-producer

	h.WaitForNodeSlot(cardanoEndpoint, 0, 60*time.Second)

	initialTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get initial cardano-producer tip")

	// Wait for the chain to advance by at least 10 slots.
	// At 1s/slot this takes ~10 seconds; allow 30s for margin.
	targetSlot := initialTip.SlotNumber + 10
	timeout := 10*cfg.SlotDuration() + 30*time.Second
	h.WaitForNodeSlot(cardanoEndpoint, targetSlot, timeout)

	newTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get new cardano-producer tip")
	require.Greater(t, newTip.BlockNumber, initialTip.BlockNumber,
		"cardano-producer should have forged new blocks",
	)

	t.Logf(
		"cardano-producer chain advanced from slot %d to %d"+
			" (blocks: %d -> %d)",
		initialTip.SlotNumber, newTip.SlotNumber,
		initialTip.BlockNumber, newTip.BlockNumber,
	)
}
