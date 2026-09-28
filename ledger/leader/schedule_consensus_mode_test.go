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

package leader_test

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/gouroboros/consensus"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// TestCalculateSchedule_TPraosErasMatchGouroborosTPraos is the failing
// proof of the eras-DevNet VRF rejection cascade. The cardano-relay log
// shows blocks dingo forged in Shelley being rejected with:
//
//	OverlayFailure (VRFLeaderValueTooBig (OutputVRF {…}) (1 % 2)
//	  (ActiveSlotCoeff {unActiveSlotVal = 2 % 5}))
//
// i.e. cardano-node ran a TPraos leader-eligibility check on dingo's
// header, computed a leader value above the TPraos threshold, and
// declared the producer was not a leader for that slot. Every dingo
// forge in a TPraos era (Shelley/Allegra/Mary/Alonzo) then disconnects
// the peer and never reaches the relay's chain — which is the seed of
// the per-era chain divergence that ultimately produces VRF
// verification failures on dingo's side at later boundaries.
//
// dingo's leader.Calculator.CalculateSchedule today computes the
// schedule using consensus.ConsensusModeCPraos for every era. CPraos
// and TPraos differ in TWO independent ways: the VRF input
// construction (CPraos uses MkInputVrf(slot, eta0); TPraos uses
// MkSeedTPraos(slot, eta0, SeedL())) and the threshold/leader-value
// derivation (CPraos compares BLAKE2b-256("L" || output) against
// 2^256·T; TPraos compares the raw 64-byte output against 2^512·T).
// With the same VRF key and same epoch nonce, the two modes select
// completely independent slot-leader sets — dingo's CPraos schedule
// for a TPraos epoch lists slots that aren't TPraos leaders.
//
// This test demonstrates the disagreement directly: it compares the
// leader set produced by dingo's CalculateSchedule for a 75-slot
// epoch against the leader set produced by gouroboros's authoritative
// IsSlotLeaderWithMode(ConsensusModeTPraos) over the same slot range
// with the same key and epoch nonce. If they ever disagree, the test
// fails — which they do under current dingo, with multiple slots in
// dingo's CPraos schedule that TPraos rejects (and vice versa).
//
// Once CalculateSchedule learns to take a ConsensusMode parameter and
// the caller passes ConsensusModeTPraos for Shelley/Allegra/Mary/
// Alonzo epochs, the two leader sets agree exactly and dingo's
// forges in TPraos eras stop being rejected by cardano-node.
func TestCalculateSchedule_TPraosErasMatchGouroborosTPraos(t *testing.T) {
	// Deterministic 32-byte VRF seed.
	vrfSeed := bytes.Repeat([]byte{0x42}, 32)
	// Deterministic 32-byte epoch nonce.
	epochNonce := bytes.Repeat([]byte{0x91}, 32)
	// Deterministic 28-byte pool key hash.
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], bytes.Repeat([]byte{0xab}, 28))
	// Concrete stake/f matching the eras testnet (sigma = 1/2, f = 0.4).
	const (
		poolStake     uint64 = 1
		totalStake    uint64 = 2
		slotsPerEpoch uint64 = 75
		epoch         uint64 = 0
	)
	activeSlotCoeff := big.NewRat(2, 5)
	epochRange := leader.EpochSlotRange{
		StartSlot: epoch * slotsPerEpoch,
		SlotCount: slotsPerEpoch,
	}

	// dingo's calculator, called with the era-correct mode.
	calc := leader.NewCalculator(0.4)
	dingoSchedule, err := calc.CalculateSchedule(
		epoch,
		epochRange,
		poolID,
		vrfSeed,
		poolStake,
		totalStake,
		epochNonce,
		consensus.ConsensusModeTPraos,
	)
	require.NoError(t, err)

	dingoLeaders := map[uint64]struct{}{}
	for _, slot := range dingoSchedule.LeaderSlotsSnapshot() {
		dingoLeaders[slot] = struct{}{}
	}

	// Authoritative TPraos leader set per gouroboros. cardano-node
	// uses this exact algorithm to validate the header VRF when a
	// peer announces a Shelley/Allegra/Mary/Alonzo block.
	tpraosLeaders := map[uint64]struct{}{}
	signer, err := consensus.NewSimpleVRFSigner(vrfSeed)
	require.NoError(t, err)
	for slot := epochRange.StartSlot; slot < epochRange.StartSlot+epochRange.SlotCount; slot++ {
		res, err := consensus.IsSlotLeaderWithMode(
			slot,
			epochNonce,
			poolStake,
			totalStake,
			activeSlotCoeff,
			signer,
			consensus.ConsensusModeTPraos,
		)
		require.NoError(t, err)
		if res.Eligible {
			tpraosLeaders[slot] = struct{}{}
		}
	}

	// Slots dingo says we forge but TPraos says we don't lead. These
	// are the slots where forging produces the cardano-node
	// VRFLeaderValueTooBig rejection.
	var dingoOnly []uint64
	for slot := range dingoLeaders {
		if _, ok := tpraosLeaders[slot]; !ok {
			dingoOnly = append(dingoOnly, slot)
		}
	}
	// And the symmetric set, slots TPraos counts as leader but dingo's
	// CPraos schedule does not list. Forging is missed entirely at
	// these slots, which is the other half of how the run ends with
	// dingo-issued=0 across the canonical chain.
	var tpraosOnly []uint64
	for slot := range tpraosLeaders {
		if _, ok := dingoLeaders[slot]; !ok {
			tpraosOnly = append(tpraosOnly, slot)
		}
	}

	require.Emptyf(
		t, dingoOnly,
		"dingo's CalculateSchedule reports leadership at slots %v "+
			"that the TPraos leader-election rule rejects. Forging "+
			"any of these in a Shelley/Allegra/Mary/Alonzo epoch "+
			"produces the cardano-node OverlayFailure "+
			"(VRFLeaderValueTooBig …) we see in eras-cardano-relay. "+
			"Fix: thread ConsensusMode through CalculateSchedule and "+
			"pass ConsensusModeTPraos for TPraos eras.",
		dingoOnly,
	)
	require.Emptyf(
		t, tpraosOnly,
		"dingo's CalculateSchedule misses leadership at slots %v "+
			"where the TPraos rule says we are leader. cardano-node "+
			"will not reject forges here, but dingo never forges "+
			"them either, leaving canonical-chain blocks at these "+
			"slots either to the peer or to nobody at all.",
		tpraosOnly,
	)
}
