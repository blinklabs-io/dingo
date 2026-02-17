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
// rate over a measurement window. With activeSlotsCoeff=1.0 and
// slotLength=0.1s, we expect approximately 10 blocks per second (though
// slot battles between two producers will reduce the effective rate).
//
// This test measures growth over a 5-second window and asserts that at
// least 20 blocks were produced (conservatively accounting for slot battles
// and propagation overhead).
func TestChainGrowthRate(t *testing.T) {
	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(t, endpoints)

	dingoEndpoint := endpoints[0]

	// Wait for the chain to stabilize
	h.WaitForAllNodesReady(60 * time.Second)
	h.WaitForSlot(50, 30*time.Second)

	// Record the starting point
	startTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get start tip")
	t.Logf(
		"start tip: slot=%d block=%d",
		startTip.SlotNumber, startTip.BlockNumber,
	)

	// Wait for a measurement window of 50 more slots (~5 seconds)
	targetSlot := startTip.SlotNumber + 50
	h.WaitForNodeSlot(dingoEndpoint, targetSlot, 30*time.Second)

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
		"chain growth: %d blocks in %d slots (%.1f%% slot utilization)",
		blocksProduced,
		slotsElapsed,
		float64(blocksProduced)/float64(slotsElapsed)*100,
	)

	// With activeSlotsCoeff=1.0 and two competing producers, we expect
	// some slot battles. Require at least 20 blocks in 50 slots (40%).
	require.GreaterOrEqual(t, blocksProduced, uint64(20),
		"chain should produce at least 20 blocks in 50 slots",
	)
}

// TestRelayPropagation verifies that blocks produced by either producer
// reach the relay node. The relay connects to both producers and should
// see blocks from both.
func TestRelayPropagation(t *testing.T) {
	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(t, endpoints)

	relayEndpoint := endpoints[2] // cardano-relay

	// Wait for all nodes including relay
	h.WaitForAllNodesReady(60 * time.Second)

	// Wait for the relay to advance
	h.WaitForNodeSlot(relayEndpoint, 30, 30*time.Second)

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
	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(t, endpoints)

	h.WaitForAllNodesReady(60 * time.Second)

	// Check consensus at 3 different points
	checkpoints := []uint64{20, 50, 100}
	for _, slot := range checkpoints {
		t.Logf("checking consensus at slot %d...", slot)
		h.WaitForSlot(slot, 60*time.Second)

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

		// With activeSlotsCoeff=1.0 and two producers sharing keys,
		// there will be slot battles. Allow up to 10 slots of tolerance
		// since the losing fork may take time to resolve.
		h.VerifyChainConsensus(10)
		t.Logf("consensus verified at slot %d", slot)
	}
}

// TestCardanoProducerChainAdvances verifies the cardano-node producer
// is also forging blocks, serving as a baseline comparison.
func TestCardanoProducerChainAdvances(t *testing.T) {
	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(t, endpoints)

	cardanoEndpoint := endpoints[1] // cardano-producer

	h.WaitForNodeSlot(cardanoEndpoint, 0, 60*time.Second)

	initialTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get initial cardano-producer tip")

	targetSlot := initialTip.SlotNumber + 10
	h.WaitForNodeSlot(cardanoEndpoint, targetSlot, 30*time.Second)

	newTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get new cardano-producer tip")
	require.Greater(t, newTip.BlockNumber, initialTip.BlockNumber,
		"cardano-producer should have forged new blocks",
	)

	t.Logf(
		"cardano-producer chain advanced from slot %d to %d (blocks: %d -> %d)",
		initialTip.SlotNumber, newTip.SlotNumber,
		initialTip.BlockNumber, newTip.BlockNumber,
	)
}
