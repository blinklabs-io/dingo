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

package forging

import (
	"testing"

	"github.com/blinklabs-io/dingo/consensus/praos"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestForgeSyncToleranceLargerThanShortTestnetEpoch documents the
// proximate cause of DingoProducesInEachEra/Shelley=0 in the eras
// DevNet: dingo's default forge-sync gate is intentionally generous
// (100 slots) for production-network bootstrap, but in a short-epoch
// testnet (epochLength=75) it is wider than the entire first epoch,
// so the gate never withholds a forge regardless of how far behind
// dingo's local chain is from upstream.
//
// The forge gate in checkAndForgeProduction is:
//
//	if upstreamTip > 0 &&
//	    upstreamTip > tipSlot &&
//	    upstreamTip-tipSlot > forgeSyncToleranceSlots {
//	    skip
//	}
//
// "Cold start, upstream at end of epoch 0": tipSlot=0 (genesis),
// upstreamTip=74 (the last slot before the Allegra fork). The gap is
// 74. With the default tolerance of 100 the gate evaluates 74 > 100 →
// false → forge proceeds. So dingo, having just joined the network
// and barely begun chain-sync, forges its leader slots immediately
// against a stale tipSlot=0 view of the chain. Every such forge
// extends a one-block chain that has to compete with cardano-
// producer's longer chain (cardano started forging immediately at
// genesis). Praos chain selection ranks chains by block count first,
// then lower slot — and dingo's local chain has fewer blocks than
// cardano's at the point chainsync delivers cardano's chain — so
// dingo's bootstrap forges lose.
//
// The companion test below (TestChainSelectionPrefersLongerChainOverLowerSlot)
// pins the chainselection arm of the same mechanism.
func TestForgeSyncToleranceLargerThanShortTestnetEpoch(t *testing.T) {
	// Default tolerance, baked into the package as
	// forgeSyncToleranceSlots = 100.
	const epochLength uint64 = 75 // matches internal/test/erastest/testnet.yaml

	require.Greaterf(
		t,
		uint64(forgeSyncToleranceSlots),
		epochLength,
		"forge-sync tolerance (%d) is larger than the testnet's "+
			"epoch length (%d), so the gate is INACTIVE for the "+
			"entire first epoch even with the most adversarial "+
			"sync state (tipSlot=0, upstreamTip=epochLength-1). "+
			"For short-epoch tests, set DINGO_FORGE_SYNC_TOLERANCE_SLOTS "+
			"to something tighter than epochLength to make dingo "+
			"actually wait for chainsync to catch up before forging "+
			"competing chains during bootstrap.",
		forgeSyncToleranceSlots, epochLength,
	)

	// And document the gate evaluation explicitly: the worst-case
	// in-epoch lag (74) must be < tolerance (100), or the gate
	// would fire. That's the line the bootstrap fork race rides.
	const worstInEpochLag uint64 = 74
	require.LessOrEqualf(
		t,
		worstInEpochLag,
		uint64(forgeSyncToleranceSlots),
		"sanity: worst possible in-epoch lag must fit under the "+
			"tolerance, otherwise the gate would actually fire and "+
			"this whole class of failures would not exist",
	)
}

// TestChainSelectionPrefersLongerChainOverLowerSlot is the chainselection
// half of the Shelley=0 mechanism. Once the forge gate has waved
// dingo's leader slots through during bootstrap (above), the surviving
// blocks are picked by Praos chain selection. Longer chain wins, so a dingo
// local chain whose forge count lags cardano-producer's loses every fork
// resolution regardless of how low its tip slot is.
//
// Concretely: dingo joins late, forges one block at slot 5; chainsync
// then delivers cardano-producer's three-block chain ending at slot 7.
// Dingo's tip is at the lower slot (5 < 7), but cardano's chain is
// longer (3 blocks > 1). Block count wins: dingo rolls back its slot-5
// block and adopts cardano's chain. Repeat across every dingo forge
// that gets caught in this race during bootstrap and you get
// Shelley=0 on the canonical chain.
func TestChainSelectionPrefersLongerChainOverLowerSlot(t *testing.T) {
	dingoLocalTip := ochainsync.Tip{
		BlockNumber: 1,
		Point: ocommon.Point{
			Slot: 5,
			Hash: []byte("dingo-slot-5"),
		},
	}
	cardanoIncomingTip := ochainsync.Tip{
		BlockNumber: 3,
		Point: ocommon.Point{
			Slot: 7,
			Hash: []byte("cardano-slot-7"),
		},
	}

	require.Truef(
		t,
		praos.ComparePraosTips(
			cardanoIncomingTip,
			dingoLocalTip,
			praos.PraosTiebreakerView{},
			praos.PraosTiebreakerView{},
		) == praos.ChainABetter,
		"cardano's longer (block_number=3) chain must beat dingo's "+
			"local (block_number=1) chain even though dingo's tip "+
			"slot is lower (%d < %d).",
		dingoLocalTip.Point.Slot, cardanoIncomingTip.Point.Slot,
	)
	require.Falsef(
		t,
		praos.ComparePraosTips(
			dingoLocalTip,
			cardanoIncomingTip,
			praos.PraosTiebreakerView{},
			praos.PraosTiebreakerView{},
		) == praos.ChainABetter,
		"dingo's local chain at lower slot but fewer blocks must "+
			"NOT win against cardano's longer chain. If it did, "+
			"dingo would entrench a forked solo-extension chain "+
			"against a longer peer chain — even worse than the "+
			"current Shelley=0 outcome.",
	)
}
