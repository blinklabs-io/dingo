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

	"github.com/blinklabs-io/dingo/internal/test/devnet"
	"github.com/stretchr/testify/require"
)

// TestEpochBoundaryConsensus verifies the producers remain in consensus
// across at least one full epoch transition. With epochLength=500, this
// exercises the candidate-nonce freeze, lab nonce roll, and VRF
// verification with the new epoch nonce — the same code path that has
// been observed to wedge on preview after a Mithril bootstrap.
//
// Failure mode this catches: a producer's tip falls behind the others by
// more than slotTolerance after the boundary because every header in
// the new epoch fails VRF verification (chain stops advancing on that
// producer while the others continue forging).
func TestEpochBoundaryConsensus(t *testing.T) {
	cfg, err := devnet.LoadDevNetConfig()
	require.NoError(
		t, err, "failed to load devnet config from testnet.yaml",
	)
	t.Logf(
		"devnet config: epochLength=%d slotLength=%.1fs"+
			" activeSlotsCoeff=%.2f securityParam=%d",
		cfg.EpochLength, cfg.SlotLength,
		cfg.ActiveSlotsCoeff, cfg.SecurityParam,
	)

	endpoints := devnet.LoadEndpoints()
	h := devnet.NewTestHarness(
		t, endpoints,
		devnet.WithNetworkMagic(cfg.NetworkMagic),
	)

	h.WaitForAllNodesReady(60 * time.Second)

	// Target a slot well past the first epoch boundary so we have
	// blocks on both sides of the rollover. 1.4 * epochLength puts
	// us ~200 slots into epoch 1.
	targetSlot := cfg.EpochLength + cfg.EpochLength*2/5
	timeout := time.Duration(targetSlot)*cfg.SlotDuration() +
		cfg.ExpectedBlockTime()*10
	t.Logf(
		"waiting for slot %d (past epoch boundary at slot %d), timeout %s",
		targetSlot, cfg.EpochLength, timeout,
	)

	observed := h.DingoNode()
	h.WaitForNodeSlot(observed, targetSlot, timeout)

	// Both producers must agree on the chain after crossing the
	// boundary. K is the initial catch-up tolerance; anything larger
	// means one side has stalled, which is what the bug looks like.
	tolerance := cfg.SecurityParam
	convergeTimeout := cfg.ExpectedBlockTime() * 60
	h.VerifyChainConsensus(tolerance, convergeTimeout)

	// Once the boundary has passed and the nodes have had one K window to
	// catch up, require near-immediate convergence. This catches Dingo
	// staying materially behind cardano-node after new-epoch VRF checks.
	tightTolerance := uint64(1)
	tightConvergeTimeout := cfg.ExpectedBlockTime() * 10
	h.VerifyChainConsensus(tightTolerance, tightConvergeTimeout)

	// Every producer's tip slot must be past the boundary. If VRF
	// verification fails on every new-epoch header, a node's chain stalls
	// a few slots before slot=epochLength.
	for _, p := range h.Producers() {
		tip, terr := h.GetChainTip(p)
		require.NoError(t, terr, "failed to get %s tip after boundary", p.Name)
		t.Logf("post-boundary: %s slot=%d block=%d", p.Name, tip.SlotNumber, tip.BlockNumber)
		require.Greater(t, tip.SlotNumber, cfg.EpochLength,
			"%s did not advance past first epoch boundary "+
				"(stuck before slot %d) - likely VRF verification "+
				"failure on epoch 1 headers", p.Name, cfg.EpochLength)
	}
}
