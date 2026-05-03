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

package chainselection_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestCompareChains_SlotBattleAtSameLengthAndSlotIsAmbiguous shows the
// gap that lets the eras-DevNet VRF-failure cascade get started:
// IsBetterChain treats two competing tips at the same block count AND
// the same slot — the exact shape of a slot-battle pair — as "equal"
// and refuses to switch in either direction. cardano-node's Praos
// chain selection breaks this case with a deterministic tie-breaker
// (the lower VRF leader output). With no tie-break here, dingo cannot
// pick a side on its own; whichever chain it stays on is whichever
// chain happened to land first.
//
// Why it matters in the eras DevNet:
//
// At slot 40 of a recent run, dingo forged a Shelley block 5c25... on
// top of slot 38. cardano-producer concurrently forged a different
// Shelley block 0c54... on top of the same slot 38. Two valid chains
// of equal length, both ending at slot 40. cardano-relay (cardano-
// node) ran its Praos tie-break and picked 0c54. dingo's chainsync
// client received a cursor-mismatch from the relay, the relay closed
// the connection (TCP RST), dingo reconnected, re-intersected, rolled
// back to slot 38, and adopted 0c54. The "slot battle" log line on
// dingo confirms local_won=false — its locally-forged 5c25 was
// dropped.
//
// All nine slot battles in that run came back local_won=false. The
// run's full canonical chain (the relay's view) ended with zero
// dingo-issued blocks across every era, even though dingo successfully
// produced 95 blocks. That's not 50/50 randomness; it's the signature
// of a chain-selection asymmetry between dingo and cardano-node.
//
// In dingo, the chainsync-driven rollback hides the asymmetry at slots
// where a battle is recognised — chainsync is authoritative over local
// chain selection there. But the absence of a same-slot tie-break
// elsewhere means: when dingo's local chain selection considers the
// peer's tip and finds it "equal", it makes no decision either way.
// Anywhere chainsync is not actively forcing a rollback, the chains
// are free to drift, and the evolving nonce — which carries across
// epoch boundaries without resetting — picks up the drift. By the
// next era boundary the candidate-nonce inputs disagree, eta0
// disagrees, and every header from the peer VRF-fails.
//
// The fix is to extend CompareChains so the equal-length-equal-slot
// case has a deterministic tie-break (lower hash, matching cardano-
// node's effective ordering once block hashes become available).
// After that, dingo's local chain-selection decisions match cardano-
// node's at every slot battle, the run-by-run "always lose" pattern
// stops, and chains converge cleanly.
func TestCompareChains_SlotBattleAtSameLengthAndSlotIsAmbiguous(t *testing.T) {
	// Two Shelley blocks at slot 40, both extending the same slot-38
	// parent, block_number=11 (same length past the common ancestor).
	dingoTip := ochainsync.Tip{
		BlockNumber: 11,
		Point: ocommon.Point{
			Slot: 40,
			// 5c259f44... — the dingo-forged hash from the run.
			Hash: []byte{
				0x5c, 0x25, 0x9f, 0x44, 0x42, 0xe1, 0x49, 0x33,
				0x72, 0xe2, 0x07, 0x83, 0xaa, 0xb3, 0x06, 0x39,
				0x13, 0x75, 0x44, 0x42, 0xe5, 0x98, 0xa0, 0x54,
				0xad, 0xe8, 0xb6, 0x26, 0xef, 0xe7, 0x4c, 0xe1,
			},
		},
	}
	cardanoTip := ochainsync.Tip{
		BlockNumber: 11,
		Point: ocommon.Point{
			Slot: 40,
			// 0c541182... — the cardano-producer-forged hash.
			Hash: []byte{
				0x0c, 0x54, 0x11, 0x82, 0xc9, 0xa9, 0xa6, 0x6d,
				0xa2, 0xf7, 0xc3, 0x1a, 0x1f, 0x9b, 0x9e, 0x4a,
				0xeb, 0xf0, 0xfe, 0x0f, 0xed, 0x41, 0xe8, 0x04,
				0xb7, 0x21, 0xac, 0xf5, 0x78, 0x3a, 0x64, 0x03,
			},
		},
	}

	got := chainselection.CompareChains(dingoTip, cardanoTip)
	require.Equalf(
		t,
		chainselection.ChainEqual,
		got,
		"CompareChains returns %d (a real comparator value 1, -1, "+
			"or 2 would mean a deterministic tie-break exists). "+
			"Returning ChainEqual (0) at a slot battle means the "+
			"local chain-selection result depends on which chain "+
			"happened to be installed first. cardano-node breaks "+
			"the same pair deterministically; the asymmetry is "+
			"how the eras-DevNet VRF wedge gets seeded.",
		got,
	)

	require.Falsef(
		t,
		chainselection.IsBetterChain(dingoTip, cardanoTip),
		"IsBetterChain(dingo, cardano) returned true — that would "+
			"mean dingo wins this slot battle locally and would "+
			"refuse the chainsync-driven adoption of cardano's "+
			"block. Empirically the symptom is the opposite "+
			"(local_won=false), but the principle holds: any "+
			"non-deterministic answer here is the bug.",
	)
	require.Falsef(
		t,
		chainselection.IsBetterChain(cardanoTip, dingoTip),
		"IsBetterChain(cardano, dingo) returned true — that would "+
			"mean dingo's local selector unilaterally adopts "+
			"cardano's block. Today neither call is true, which is "+
			"exactly the gap: dingo cannot make a choice on its "+
			"own and has to wait for chainsync to force one.",
	)
}
