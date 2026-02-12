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

package devnet

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// defaultEndpoints returns the standard DevNet endpoints.
// These can be overridden via environment variables for CI flexibility.
func defaultEndpoints() []NodeEndpoint {
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
	return []NodeEndpoint{
		{Name: "dingo-producer", Address: dingoAddr},
		{Name: "cardano-producer", Address: cardanoAddr},
		{Name: "cardano-relay", Address: relayAddr},
	}
}

func TestHarnessGetChainTip(t *testing.T) {
	endpoints := defaultEndpoints()
	h := NewTestHarness(t, endpoints)

	// Wait for all nodes to become reachable
	h.WaitForAllNodesReady(60 * time.Second)

	// Query each node's chain tip
	for _, ep := range endpoints {
		tip, err := h.GetChainTip(ep)
		require.NoError(t, err, "failed to get chain tip from %s", ep.Name)
		t.Logf("%s: slot=%d block=%d", ep.Name, tip.SlotNumber, tip.BlockNumber)
	}
}

func TestHarnessWaitForSlot(t *testing.T) {
	endpoints := defaultEndpoints()
	h := NewTestHarness(t, endpoints)

	h.WaitForAllNodesReady(60 * time.Second)

	// With 0.1s slots, slot 10 should be reached within a few seconds
	h.WaitForSlot(10, 30*time.Second)
}

func TestHarnessVerifyConsensus(t *testing.T) {
	endpoints := defaultEndpoints()
	h := NewTestHarness(t, endpoints)

	h.WaitForAllNodesReady(60 * time.Second)

	// Let the chain advance a bit before checking consensus
	h.WaitForSlot(20, 30*time.Second)

	// Allow up to 5 slots of propagation delay
	h.VerifyChainConsensus(5)
}
