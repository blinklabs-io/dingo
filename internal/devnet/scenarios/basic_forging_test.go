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
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/devnet"
	"github.com/stretchr/testify/require"
)

// defaultEndpoints returns the standard DevNet endpoints.
func defaultEndpoints() []devnet.NodeEndpoint {
	dingoAddr := os.Getenv("DEVNET_DINGO_ADDR")
	if dingoAddr == "" {
		dingoAddr = "localhost:3001"
	}
	cardanoAddr := os.Getenv("DEVNET_CARDANO_ADDR")
	if cardanoAddr == "" {
		cardanoAddr = "localhost:3011"
	}
	relayAddr := os.Getenv("DEVNET_RELAY_ADDR")
	if relayAddr == "" {
		relayAddr = "localhost:3012"
	}
	return []devnet.NodeEndpoint{
		{Name: "dingo-producer", Address: dingoAddr},
		{Name: "cardano-producer", Address: cardanoAddr},
		{Name: "cardano-relay", Address: relayAddr},
	}
}

// TestBasicBlockForging verifies that the Dingo producer forges blocks
// and that all nodes in the DevNet reach consensus.
//
// This test:
//  1. Connects to all 3 nodes (dingo-producer, cardano-producer, cardano-relay)
//  2. Waits for slot 20 (~2 seconds with 0.1s slots)
//  3. Verifies that Dingo forged at least one block (chain tip advances)
//  4. Verifies that all nodes agree on the chain tip within tolerance
func TestBasicBlockForging(t *testing.T) {
	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(t, endpoints)

	// Step 1: Wait for all nodes to become reachable
	t.Log("waiting for all DevNet nodes to become ready...")
	h.WaitForAllNodesReady(60 * time.Second)
	t.Log("all nodes are ready")

	// Step 2: Record the initial tip from the Dingo producer
	dingoEndpoint := endpoints[0]
	initialTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get initial Dingo chain tip")
	t.Logf(
		"initial Dingo tip: slot=%d block=%d",
		initialTip.SlotNumber, initialTip.BlockNumber,
	)

	// Step 3: Wait for slot 20 (with 0.1s slots, this is ~2 seconds of chain time)
	t.Log("waiting for chain to advance to slot 20...")
	h.WaitForSlot(20, 30*time.Second)
	t.Log("chain has reached slot 20")

	// Step 4: Verify Dingo forged at least one block
	dingoTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get Dingo chain tip after wait")
	t.Logf(
		"Dingo tip after wait: slot=%d block=%d",
		dingoTip.SlotNumber, dingoTip.BlockNumber,
	)
	require.Greater(t, dingoTip.BlockNumber, initialTip.BlockNumber,
		"Dingo producer should have forged at least one block",
	)

	// Step 5: Verify all nodes agree on the chain tip within a tolerance
	// of 5 slots to account for propagation delays.
	t.Log("verifying chain consensus across all nodes...")
	h.VerifyChainConsensus(5)
	t.Log("all nodes are in consensus")
}

// TestDingoChainAdvances is a simpler test that just verifies
// the Dingo node's chain is advancing (forging blocks).
func TestDingoChainAdvances(t *testing.T) {
	endpoints := defaultEndpoints()
	h := devnet.NewTestHarness(t, endpoints)

	dingoEndpoint := endpoints[0]

	// Wait for Dingo to be reachable
	h.WaitForNodeSlot(dingoEndpoint, 0, 60*time.Second)

	// Get initial tip
	initialTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get initial tip")

	// Wait for the chain to advance by at least 10 slots
	targetSlot := initialTip.SlotNumber + 10
	h.WaitForNodeSlot(dingoEndpoint, targetSlot, 30*time.Second)

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
