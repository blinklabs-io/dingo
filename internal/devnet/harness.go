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
	"fmt"
	"net"
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
	}, timeout, 200*time.Millisecond,
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
	}, timeout, 200*time.Millisecond,
		"%s did not reach slot %d within %s",
		endpoint.Name, targetSlot, timeout,
	)
}

// VerifyChainConsensus checks that all nodes report chain tips within
// a configurable slot tolerance, accounting for minor propagation
// delays. When two nodes are at the same slot, their block hashes
// must also match (i.e. they are on the same fork).
func (h *TestHarness) VerifyChainConsensus(slotTolerance uint64) {
	h.t.Helper()
	tips := make(map[string]ChainTip)
	for _, ep := range h.endpoints {
		tip, err := h.GetChainTip(ep)
		require.NoError(h.t, err,
			"failed to get chain tip from %s", ep.Name,
		)
		tips[ep.Name] = tip
		h.t.Logf(
			"VerifyChainConsensus: %s at slot %d, block %d",
			ep.Name, tip.SlotNumber, tip.BlockNumber,
		)
	}

	// Compare all tips pairwise
	names := make([]string, 0, len(tips))
	for name := range tips {
		names = append(names, name)
	}
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
			require.LessOrEqual(h.t, slotDiff, slotTolerance,
				"slot difference between %s (slot %d) and %s (slot %d) "+
					"exceeds tolerance of %d",
				names[i], tipA.SlotNumber,
				names[j], tipB.SlotNumber,
				slotTolerance,
			)
			// When slots match exactly, hashes must agree
			// (same slot implies same block on a single chain).
			if slotDiff == 0 {
				require.Equal(h.t, tipA.Hash, tipB.Hash,
					"nodes %s and %s are at the same slot %d "+
						"but have different block hashes",
					names[i], names[j], tipA.SlotNumber,
				)
			}
		}
	}
}

// WaitForAllNodesReady polls all endpoints until each one is reachable
// and returns a valid chain tip.
func (h *TestHarness) WaitForAllNodesReady(timeout time.Duration) {
	h.t.Helper()
	for _, ep := range h.endpoints {
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
		}, timeout, 500*time.Millisecond,
			"%s did not become ready within %s", ep.Name, timeout,
		)
	}
}
