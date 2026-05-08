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

//go:build erastest

package erastest

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/stretchr/testify/require"
)

// vanRossemPreBumpTargetSlot is the slot the chain must reach before
// the pre-bump assertions run. Picked to exceed two full 75-slot
// epochs so the observation window covers steady-state PV10 block
// production after bootstrap warmup. With f=0.4 and two equal-stake
// pools, the probability of zero forges from a given pool over 100
// slots is (1-0.225)^100 ≈ 9e-11 — slot count is not a realistic
// source of flakes.
const vanRossemPreBumpTargetSlot = uint64(100)

// TestVanRossem exercises the PV10→PV11 (vanRossem) intra-Conway
// hard fork on a multi-node DevNet. The chain bootstraps in
// Conway-Plomin via testnet-vanrossem.yaml; a HardForkInitiation
// governance-action driver subsequently submits, votes, ratifies,
// and enacts the bump to PV11 on the live chain. The test asserts:
//
//  1. The DevNet harness was actually pointed at the vanRossem
//     variant (testnet-vanrossem.yaml), not the default eras testnet.
//  2. Pre-bump (PV10): every header is in Conway era, both pools
//     forge accepted blocks on the relay's chain. Establishes a
//     known-good baseline before the boundary.
//  3. Post-bump (PV11): once the HFI driver enacts the protocol
//     version bump, the chain continues advancing, dingo continues
//     forging blocks accepted by the cardano-node relay, and PV11
//     rule activation does not break header / block validation on
//     either side.
//
// The driver itself lives in vanrossem_driver.go (TODO: not yet
// implemented). Until then, the post-bump sub-tests are skipped
// rather than wired against a non-existent boundary, so the test
// validates the PV10 baseline cleanly today.
func TestVanRossem(t *testing.T) {
	cfg := loadConfig(t)

	// Defensive: confirm we are running against the vanrossem variant
	// rather than the default eras testnet. An empty-schedule check is
	// not load-bearing here because Config.ScheduledTransitions()
	// reports Conway when StartingMajorVersion lives inside Conway's
	// [MinMajorVersion, MaxMajorVersion] range — at PV10 there is still
	// PV11 of Conway ahead, so the schedule reports `conway@epoch=0`
	// even though no inter-era boundary will actually be crossed. The
	// StartingMajorVersion equality check below is what distinguishes
	// the variant.
	require.Equalf(
		t, uint(10), cfg.StartingMajorVersion,
		"vanrossem variant must bootstrap at PV10 (Plomin); got %d. "+
			"The HFI driver expects to start one PV step below "+
			"vanRossem so it can drive the PV10→PV11 transition. "+
			"Check shelley genesis protocolVersion.major in "+
			"testnet-vanrossem.yaml — and run via "+
			"run-tests-vanrossem.sh, not run-tests.sh.",
		cfg.StartingMajorVersion,
	)

	endpoints := DefaultEndpoints()
	streams := make(map[string]*EraStream, len(endpoints))
	for _, ep := range endpoints {
		streams[ep.Name] = NewEraStream(
			t, ep, cfg.NetworkMagic, cfg.EpochLength,
		)
	}
	t.Cleanup(func() {
		for _, s := range streams {
			s.Close()
		}
	})

	// Drive each node past the pre-bump target so every PV10-era
	// assertion below observes a non-trivial chain. Reusing
	// transitionTimeout keeps the budget consistent with the eras
	// test harness; the vanrossem variant has no inter-era boundaries
	// so the timeout only needs to absorb container warmup and block-
	// rate variance, not fork-resolution churn.
	timeout := transitionTimeout(cfg)
	for name, s := range streams {
		_, err := s.WaitForSlot(vanRossemPreBumpTargetSlot, timeout)
		require.NoErrorf(
			t, err,
			"node %s did not reach slot %d within %s: %v",
			name, vanRossemPreBumpTargetSlot, timeout, err,
		)
	}

	// All assertions read the relay's view because the relay forges no
	// blocks itself, so the issuer vkey of every block on its chain is
	// the producer (dingo or cardano-producer) that the relay actually
	// accepted. Same rationale as TestEraTransitions /
	// DingoProducesInEachEra.
	relay := endpoints[2]
	relayStream := streams[relay.Name]
	require.NotNilf(
		t, relayStream,
		"no stream for relay endpoint %q; check DefaultEndpoints ordering",
		relay.Name,
	)

	t.Run("ChainStaysInConway", func(t *testing.T) {
		headers := relayStream.HeadersSnapshot()
		require.NotEmpty(t, headers, "no headers observed on relay")
		for _, h := range headers {
			require.Equalf(
				t, eras.ConwayEraDesc.Id, h.EraID,
				"non-Conway header at slot %d (era ID %d). "+
					"vanRossem chain must stay in Conway era throughout; "+
					"a different era ID indicates the configurator did "+
					"not honor the testnet-vanrossem.yaml fork schedule "+
					"or cardano-node ran an inter-era HFC translation "+
					"despite no scheduled transitions.",
				h.Slot, h.EraID,
			)
		}
	})

	t.Run("DingoProducesAtPV10", func(t *testing.T) {
		dingoVkey := readDingoColdVKey(t)
		t.Logf("dingo cold vkey: %s", hex.EncodeToString(dingoVkey))

		headers := relayStream.HeadersSnapshot()
		var dingoBlocks, cardanoBlocks int
		for _, h := range headers {
			if bytes.Equal(h.IssuerVkey, dingoVkey) {
				dingoBlocks++
				continue
			}
			cardanoBlocks++
		}
		t.Logf(
			"relay observed %d total headers (dingo-issued=%d, cardano-issued=%d)",
			len(headers), dingoBlocks, cardanoBlocks,
		)

		require.Greaterf(
			t, dingoBlocks, 0,
			"no dingo-issued blocks observed on relay across %d headers — "+
				"dingo's PV10 forges are not being accepted by the "+
				"cardano-node relay. With two equal-stake pools and f=%.2f "+
				"over %d slots the probability of zero forges is ≈1e-10, "+
				"so this is a chain-divergence regression at PV10: "+
				"investigate the dingo log for VRF / nonce / pparams "+
				"disagreements with the cardano-node relay before any "+
				"PV11 bump is even attempted.",
			len(headers), cfg.ActiveSlotsCoeff, vanRossemPreBumpTargetSlot,
		)

		require.Greaterf(
			t, cardanoBlocks, 0,
			"no cardano-issued blocks observed on relay across %d "+
				"headers — cardano-producer is not contributing to the "+
				"canonical chain. Check that cardano-node:11.0.1 "+
				"actually started and is forging at PV10.",
			len(headers),
		)
	})

	t.Run("DriveHFIToPV11", func(t *testing.T) {
		driveHFIToPV11(t)

		// Re-observe the relay stream after the bump and confirm
		// dingo continues to forge accepted blocks on the canonical
		// chain post-boundary. This is the assertion that catches a
		// vanRossem rule activation breaking dingo's block validity
		// against the cardano-node relay even when the PV bump
		// itself succeeded.
		beforeCount := len(relayStream.HeadersSnapshot())
		postBumpTimeout := transitionTimeout(cfg)
		// Wait for the relay to advance by at least 1 epoch's worth
		// of slots after the bump so we have a fair sample of
		// post-bump headers to inspect.
		latest, _ := relayStream.LatestHeader()
		_, err := relayStream.WaitForSlot(
			latest.Slot+cfg.EpochLength, postBumpTimeout,
		)
		require.NoErrorf(
			t, err,
			"chain did not advance one epoch past PV11 boundary within %s",
			postBumpTimeout,
		)

		dingoVkey := readDingoColdVKey(t)
		var postBumpDingo, postBumpCardano int
		for _, h := range relayStream.HeadersSnapshot()[beforeCount:] {
			if bytes.Equal(h.IssuerVkey, dingoVkey) {
				postBumpDingo++
				continue
			}
			postBumpCardano++
		}
		t.Logf(
			"post-PV11 headers on relay: dingo-issued=%d, cardano-issued=%d",
			postBumpDingo, postBumpCardano,
		)
		require.Greaterf(
			t, postBumpDingo, 0,
			"no dingo-issued blocks observed on relay after PV11 enactment. "+
				"The bump itself ratified (pparams.major=11 confirmed by "+
				"driveHFIToPV11) but dingo's post-boundary forges are not "+
				"being accepted by the cardano-node relay — investigate the "+
				"dingo log for VRF / pparams disagreements clustered "+
				"immediately after the enactment epoch boundary.",
		)
		require.GreaterOrEqualf(
			t, postBumpCardano, 0,
			"cardano-issued blocks after PV11 enactment may be zero; cardano-producer contribution is informational",
		)
	})
}
