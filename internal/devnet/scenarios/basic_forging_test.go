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

// TestBasicBlockForging verifies that the Dingo producer forges blocks
// and that all nodes in the DevNet reach consensus.
//
// This test:
//  1. Connects to all 3 nodes (dingo-producer, cardano-producer, cardano-relay)
//  2. Waits for Dingo to advance past genesis (bootstrap grace period)
//  3. Waits for the chain to advance 10 slots beyond the current tip
//  4. Verifies that Dingo forged at least one block (chain tip advances)
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

	endpoints := devnet.DefaultEndpoints()
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
	dingoEndpoint := endpoints[0]
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

	// Step 5: Verify Dingo forged at least one block
	dingoTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get Dingo chain tip after wait")
	t.Logf(
		"Dingo tip after wait: slot=%d block=%d",
		dingoTip.SlotNumber, dingoTip.BlockNumber,
	)
	require.Greater(t, dingoTip.BlockNumber, initialTip.BlockNumber,
		"Dingo producer should have forged at least one block",
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
// the Dingo node's chain is advancing (forging blocks).
func TestDingoChainAdvances(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config from testnet.yaml")

	endpoints := devnet.DefaultEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	dingoEndpoint := endpoints[0]

	// Wait for Dingo to be reachable
	h.WaitForNodeSlot(dingoEndpoint, 0, 60*time.Second)

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

	// Verify chain advanced
	newTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get new tip")
	require.Greater(t, newTip.SlotNumber, initialTip.SlotNumber,
		"Dingo chain should have advanced",
	)
	require.Greater(t, newTip.BlockNumber, initialTip.BlockNumber,
		"Dingo should have forged new blocks",
	)

	t.Logf(
		"Dingo chain advanced from slot %d to %d (blocks: %d -> %d)",
		initialTip.SlotNumber, newTip.SlotNumber,
		initialTip.BlockNumber, newTip.BlockNumber,
	)
}
