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

// TestEpochBoundaryConsensus verifies Dingo and cardano-node remain in
// consensus across at least one full epoch transition. With
// epochLength=500, this exercises the candidate-nonce freeze, lab nonce
// roll, and VRF verification with the new epoch nonce — the same code
// path that has been observed to wedge on preview after a Mithril
// bootstrap.
//
// Failure mode this catches: Dingo's tip falls behind cardano-node's by
// more than slotTolerance after the boundary because every header in
// the new epoch fails VRF verification (chain stops advancing on Dingo
// while cardano-node continues forging).
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

	endpoints := devnet.DefaultEndpoints()
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

	cardanoEndpoint := endpoints[1]
	h.WaitForNodeSlot(cardanoEndpoint, targetSlot, timeout)

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

	dingoEndpoint := endpoints[0]
	dingoTip, err := h.GetChainTip(dingoEndpoint)
	require.NoError(t, err, "failed to get dingo tip after boundary")
	cardanoTip, err := h.GetChainTip(cardanoEndpoint)
	require.NoError(t, err, "failed to get cardano tip after boundary")
	t.Logf(
		"post-boundary: dingo slot=%d block=%d, cardano slot=%d block=%d",
		dingoTip.SlotNumber, dingoTip.BlockNumber,
		cardanoTip.SlotNumber, cardanoTip.BlockNumber,
	)

	// Dingo's tip slot must be past the boundary. If VRF verification
	// fails on every new-epoch header, Dingo's chain stalls a few
	// slots before slot=epochLength.
	require.Greater(
		t, dingoTip.SlotNumber, cfg.EpochLength,
		"dingo did not advance past first epoch boundary "+
			"(stuck before slot %d) — likely VRF verification "+
			"failure on epoch 1 headers",
		cfg.EpochLength,
	)
}
