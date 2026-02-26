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

//go:build devnet

// Package devnet provides a test harness for running integration tests
// against a private Cardano DevNet consisting of Dingo and cardano-node
// instances connected via Docker Compose.
package devnet

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/require"
)

// DefaultNetworkMagic is the network magic for the devnet as configured
// in shelley-genesis.json.
const DefaultNetworkMagic = 42

// NodeEndpoint describes a node that the test harness can connect to
// using the Ouroboros Node-to-Node mini-protocol over TCP.
type NodeEndpoint struct {
	Name    string
	Address string // host:port
}

// DefaultEndpoints returns the standard DevNet endpoints.
// These can be overridden via environment variables for CI flexibility.
func DefaultEndpoints() []NodeEndpoint {
	dingoAddr := os.Getenv("DEVNET_DINGO_ADDR")
	if dingoAddr == "" {
		dingoAddr = "localhost:3010"
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

// ChainTip holds the chain tip information retrieved from a node.
type ChainTip struct {
	SlotNumber  uint64
	BlockNumber uint64
	Hash        []byte
}

// TestHarness manages connections to DevNet nodes and provides
// helper methods for querying chain state and verifying consensus.
type TestHarness struct {
	t            *testing.T
	endpoints    []NodeEndpoint
	networkMagic uint32
}

// NewTestHarness creates a new test harness for the given endpoints.
// The default network magic (42) is used unless overridden with
// WithNetworkMagic.
func NewTestHarness(
	t *testing.T,
	endpoints []NodeEndpoint,
	opts ...HarnessOptionFunc,
) *TestHarness {
	t.Helper()
	h := &TestHarness{
		t:            t,
		endpoints:    endpoints,
		networkMagic: DefaultNetworkMagic,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HarnessOptionFunc configures a TestHarness.
type HarnessOptionFunc func(*TestHarness)

// WithNetworkMagic overrides the default network magic value.
func WithNetworkMagic(magic uint32) HarnessOptionFunc {
	return func(h *TestHarness) {
		h.networkMagic = magic
	}
}

// GetChainTip connects to the specified node using the Ouroboros N2N
// protocol and retrieves the current chain tip via ChainSync.
// Each call establishes a fresh connection to ensure the returned tip
// reflects the node's current state (cardano-node does not update
// the tip on persistent connections).
func (h *TestHarness) GetChainTip(
	endpoint NodeEndpoint,
) (ChainTip, error) {
	conn, err := net.DialTimeout("tcp", endpoint.Address, 10*time.Second)
	if err != nil {
		return ChainTip{}, fmt.Errorf(
			"failed to connect to %s (%s): %w",
			endpoint.Name, endpoint.Address, err,
		)
	}
	defer conn.Close()

	oConn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(conn),
		ouroboros.WithNetworkMagic(h.networkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
	)
	if err != nil {
		return ChainTip{}, fmt.Errorf(
			"failed to establish ouroboros connection to %s: %w",
			endpoint.Name, err,
		)
	}
	defer oConn.Close()

	tip, err := oConn.ChainSync().Client.GetCurrentTip()
	if err != nil {
		return ChainTip{}, fmt.Errorf(
			"failed to get chain tip from %s: %w",
			endpoint.Name, err,
		)
	}

	return ChainTip{
		SlotNumber:  tip.Point.Slot,
		BlockNumber: tip.BlockNumber,
		Hash:        tip.Point.Hash,
	}, nil
}

// WaitForSlot polls all endpoints until at least one reports a chain
// tip at or beyond the target slot, or the timeout expires.
func (h *TestHarness) WaitForSlot(
	targetSlot uint64,
	timeout time.Duration,
) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		for _, ep := range h.endpoints {
			tip, err := h.GetChainTip(ep)
			if err != nil {
				h.t.Logf(
					"WaitForSlot: error querying %s: %v",
					ep.Name, err,
				)
				continue
			}
			h.t.Logf(
				"WaitForSlot: %s at slot %d, block %d",
				ep.Name, tip.SlotNumber, tip.BlockNumber,
			)
			if tip.SlotNumber >= targetSlot {
				return true
			}
		}
		return false
	}, timeout, 2*time.Second,
		"no node reached slot %d within %s", targetSlot, timeout,
	)
}

// WaitForNodeSlot polls a specific endpoint until it reports a chain
// tip at or beyond the target slot, or the timeout expires.
func (h *TestHarness) WaitForNodeSlot(
	endpoint NodeEndpoint,
	targetSlot uint64,
	timeout time.Duration,
) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		tip, err := h.GetChainTip(endpoint)
		if err != nil {
			h.t.Logf(
				"WaitForNodeSlot: error querying %s: %v",
				endpoint.Name, err,
			)
			return false
		}
		h.t.Logf(
			"WaitForNodeSlot: %s at slot %d, block %d",
			endpoint.Name, tip.SlotNumber, tip.BlockNumber,
		)
		return tip.SlotNumber >= targetSlot
	}, timeout, 2*time.Second,
		"%s did not reach slot %d within %s",
		endpoint.Name, targetSlot, timeout,
	)
}

// VerifyChainConsensus polls all nodes until their chain tips are within
// slotTolerance of each other, or the timeout expires. This accounts for
// propagation delays and temporary divergence during catch-up.
//
// Fork detection compares block hashes only when two nodes report the
// same slot (slotDiff == 0). Tips at adjacent slots within the tolerance
// are not compared by hash, so a large slotTolerance (e.g. 3*K) can
// mask short forks. Callers needing stricter fork detection should use
// a smaller tolerance or perform additional cross-slot checks.
//
// Unreachable endpoints are skipped rather than failing the round,
// provided at least 2 nodes return valid tips for comparison.
func (h *TestHarness) VerifyChainConsensus(
	slotTolerance uint64,
	timeout time.Duration,
) {
	h.t.Helper()
	require.Eventually(h.t, func() bool {
		tips := make(map[string]ChainTip)
		for _, ep := range h.endpoints {
			tip, err := h.GetChainTip(ep)
			if err != nil {
				h.t.Logf(
					"VerifyChainConsensus: error querying %s: %v",
					ep.Name, err,
				)
				continue
			}
			tips[ep.Name] = tip
		}
		// Need at least 2 responsive nodes for a meaningful comparison.
		if len(tips) < 2 {
			return false
		}

		// Build sorted name list for deterministic iteration
		names := make([]string, 0, len(tips))
		for name := range tips {
			names = append(names, name)
		}
		sort.Strings(names)

		// Log all tips in sorted order
		for _, name := range names {
			tip := tips[name]
			h.t.Logf(
				"VerifyChainConsensus: %s at slot %d, block %d",
				name, tip.SlotNumber, tip.BlockNumber,
			)
		}

		// Compare all tips pairwise. The hash mismatch check only
		// runs when slotDiff == 0, so this verifies proximity
		// (within slotTolerance) and same-slot agreement rather
		// than full fork detection across different slots.
		for i := 0; i < len(names); i++ {
			for j := i + 1; j < len(names); j++ {
				tipA := tips[names[i]]
				tipB := tips[names[j]]
				var slotDiff uint64
				if tipA.SlotNumber > tipB.SlotNumber {
					slotDiff = tipA.SlotNumber - tipB.SlotNumber
				} else {
					slotDiff = tipB.SlotNumber - tipA.SlotNumber
				}
				if slotDiff > slotTolerance {
					return false
				}
				// When tips are at the same slot, verify they
				// agree on the block hash (detect forks).
				if slotDiff == 0 &&
					!bytes.Equal(tipA.Hash, tipB.Hash) {
					h.t.Logf(
						"VerifyChainConsensus: fork detected: "+
							"%s and %s at slot %d have different hashes",
						names[i], names[j], tipA.SlotNumber,
					)
					return false
				}
			}
		}
		return true
	}, timeout, 2*time.Second,
		"nodes did not reach consensus within %s (tolerance: %d slots)",
		timeout, slotTolerance,
	)
}

// WaitForAllNodesReady polls all endpoints until each one is reachable
// and returns a valid chain tip. The timeout is shared across all
// endpoints so the total wait is bounded by timeout, not N*timeout.
func (h *TestHarness) WaitForAllNodesReady(timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for _, ep := range h.endpoints {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			h.t.Fatalf(
				"deadline expired before checking %s", ep.Name,
			)
		}
		require.Eventually(h.t, func() bool {
			_, err := h.GetChainTip(ep)
			if err != nil {
				h.t.Logf(
					"WaitForAllNodesReady: %s not ready: %v",
					ep.Name, err,
				)
				return false
			}
			return true
		}, remaining, 2*time.Second,
			"%s did not become ready within %s", ep.Name, timeout,
		)
	}
}
