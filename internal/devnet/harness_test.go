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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHarnessGetChainTip(t *testing.T) {
	endpoints := DefaultEndpoints()
	h := NewTestHarness(t, endpoints,
		WithNetworkMagic(DefaultNetworkMagic),
	)

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
	// Set path relative to this package (internal/devnet/)
	t.Setenv("DEVNET_TESTNET_YAML", "../test/devnet/testnet.yaml")
	cfg, err := LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config")

	endpoints := DefaultEndpoints()
	h := NewTestHarness(t, endpoints,
		WithNetworkMagic(cfg.NetworkMagic),
	)

	h.WaitForAllNodesReady(60 * time.Second)

	// Wait for slot 10. Timeout: 10 slots worth of wall-clock time
	// plus margin for startup variance.
	const targetSlot = 10
	timeout := time.Duration(targetSlot)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForSlot(targetSlot, timeout)
}

func TestHarnessVerifyConsensus(t *testing.T) {
	// Set path relative to this package (internal/devnet/)
	t.Setenv("DEVNET_TESTNET_YAML", "../test/devnet/testnet.yaml")
	cfg, err := LoadDevNetConfig()
	require.NoError(t, err, "failed to load devnet config")

	endpoints := DefaultEndpoints()
	h := NewTestHarness(t, endpoints,
		WithNetworkMagic(cfg.NetworkMagic),
	)

	h.WaitForAllNodesReady(60 * time.Second)

	// Let the chain advance to slot 20
	const targetSlot = 20
	slotTimeout := time.Duration(targetSlot)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*5
	h.WaitForSlot(targetSlot, slotTimeout)

	// Wait for nodes to converge. Use 3*securityParam as the tolerance
	// (same as the scenario tests) since early chain lifecycle has higher
	// divergence as nodes bootstrap and connect. Allow 20 expected block
	// times for convergence.
	tolerance := 3 * cfg.SecurityParam
	h.VerifyChainConsensus(tolerance, cfg.ExpectedBlockTime()*20)
}
