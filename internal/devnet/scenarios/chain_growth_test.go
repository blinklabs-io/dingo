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
// With activeSlotsCoeff=0.4 and slotLength=1s, each slot has a 40% chance
// of producing a block. With 2 pools sharing stake equally the per-pool
// leader probability per slot is approximately 1-(1-f)^σ ≈ 0.225, and the
// network-wide probability is 1-(1-f)^1 = 0.4. In a 100-slot window we
// therefore expect ~40 blocks network-wide. We require at least 8 (20% of
// expected) as a conservative lower bound. This accounts for: random VRF
// variance, chain rollbacks (both pools can win the same slot), catch-up
// delays after rollback/resync, and the fact that a single endpoint only
// sees its own view of the chain which may lag behind the network tip.
func TestChainGrowthRate(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")
	t.Logf(
		"devnet config: activeSlotsCoeff=%.2f slotLength=%.1fs"+
			" epochLength=%d securityParam=%d expectedBlockTime=%s",
		cfg.ActiveSlotsCoeff, cfg.SlotLength,
		cfg.EpochLength, cfg.SecurityParam, cfg.ExpectedBlockTime(),
	)

	endpoints := devnet.DefaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	dingoEndpoint := endpoints[0]

	// Wait for the chain to stabilize at slot 50.
	// Timeout: 50 slots + 5 expected block times for margin.
	h.WaitForAllNodesReady(60 * time.Second)
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

	// With activeSlotsCoeff=0.4 we expect ~40 blocks in 100 slots.
	// Require at least 8 (20% of expected) as a conservative lower bound
	// that accounts for random variance and possible rollbacks.
	const minBlocks = uint64(8)
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

	endpoints := devnet.DefaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	relayEndpoint := endpoints[2] // cardano-relay

	// Wait for all nodes including relay
	h.WaitForAllNodesReady(60 * time.Second)

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

	endpoints := devnet.DefaultEndpoints()
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

// TestCardanoProducerChainAdvances verifies the cardano-node producer
// is also forging blocks, serving as a baseline comparison.
func TestCardanoProducerChainAdvances(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.DefaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	cardanoEndpoint := endpoints[1] // cardano-producer

	h.WaitForNodeSlot(cardanoEndpoint, 0, 60*time.Second)

	initialTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get initial cardano-producer tip")

	// Wait for the chain to advance by at least 10 slots.
	// Timeout: 10 slots + 5 expected block times for margin.
	const advanceSlots = 10
	targetSlot := initialTip.SlotNumber + advanceSlots
	timeout := time.Duration(advanceSlots)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
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
