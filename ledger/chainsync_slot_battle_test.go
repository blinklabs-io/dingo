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

package ledger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleEventChainsyncBlockHeaderRoutesSlotBattleToForkResolution
// pins the wire-up the eras-DevNet Babbage→Conway VRF wedge depended
// on. Two equal-stake pools regularly forge at the same slot (a "slot
// battle"); both forges arrive at the relay, the relay's Praos
// selector picks one via the deterministic tiebreak, and the loser
// must reach this side via chainsync as a fork-resolution request so
// chainselection can adopt the same winner the relay did. If dingo
// adopted its own forge first and then the peer's competing same-slot
// forge arrives, the chainsync header handler MUST fall through to
// the fork-resolution path — dropping the header as "stale roll
// forward behind local tip" because the slots are equal locks dingo
// onto whichever block it forged or received first, diverges its
// chain from the relay's at the slot battle, and the divergent
// block's VRF output then folds into dingo's evolving nonce. By the
// next epoch boundary the eta0 each side derives disagrees, every
// peer Conway header VRF-fails on dingo, and dingo's own Conway
// forges symmetrically VRF-fail on the relay.
//
// The fixture builds a chain ending at slot 20 with a known hash,
// then delivers a chainsync header at the SAME slot 20 with a
// different hash whose prevHash is the ancestor at slot 10. The
// existing handler treated the equal-slot case as stale and bailed
// before incrementing headerMismatchCount or invoking tryResolveFork.
// Asserting that headerMismatchCount becomes 1 demonstrates the
// header took the not-stale branch and reached the fork-resolution
// gate where chainselection can pick the rule-3 lower-hash winner.
func TestHandleEventChainsyncBlockHeaderRoutesSlotBattleToForkResolution(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)

	// Competing forge at the same slot as fixture.currentTip but a
	// different hash. Its prevHash is the common ancestor (slot 10)
	// so the fork-resolution path can identify the rollback target.
	competingHash := testHashBytes("slot-battle-loser")
	competingHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(competingHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber,
		slot:        fixture.currentTip.Point.Slot,
	}

	// Sanity-check: chain.AddBlockHeader returns
	// BlockNotFitChainTipError for this header — i.e. the underlying
	// gate the chainsync handler needs to reach. If this assertion
	// ever stops holding, the test no longer exercises the handler's
	// not-fit branch and the slot-battle assertion below is
	// vacuously satisfied; fail loudly here instead.
	addErr := fixture.ls.chain.AddBlockHeader(competingHeader)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAsf(
		t, addErr, &notFitErr,
		"expected chain.AddBlockHeader to reject the competing "+
			"same-slot header with BlockNotFitChainTipError so the "+
			"handler hits its not-fit gate; got err=%v", addErr,
	)
	require.Truef(
		t,
		bytes.Equal(
			fixture.currentTip.Point.Hash,
			fixture.ls.chain.Tip().Point.Hash,
		),
		"chain tip hash must still equal currentTip after the "+
			"failed AddBlockHeader; otherwise the test's notion of "+
			"\"slot-battle at the tip\" no longer holds",
	)

	// Replay the same not-fit scenario through the chainsync handler
	// and assert it routes through the fork-resolution gate, not the
	// stale-drop early return.
	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		BlockHeader:  competingHeader,
		Point: ocommon.NewPoint(
			competingHeader.SlotNumber(),
			competingHeader.Hash().Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				competingHeader.SlotNumber(),
				competingHeader.Hash().Bytes(),
			),
			BlockNumber: competingHeader.BlockNumber(),
		},
	})
	require.NoError(t, err)

	// The fork-resolution path either:
	//   - rolls back to the common ancestor (slot 10) and adopts the
	//     peer's competing header → chain.Tip() moves off the
	//     original currentTip; chainsyncState flips to
	//     RollbackChainsyncState.
	//   - declines via IsBetterChain (peer not better under the
	//     rule-3 lower-hash tiebreak) → headerMismatchCount stays at
	//     1 because nothing in tryResolveFork's success branches
	//     reset it.
	//
	// The stale-drop early return takes neither path: chain stays
	// put, chainsyncState stays SyncingChainsyncState, AND
	// headerMismatchCount stays at zero. Asserting "at least one of
	// those three witnesses changed" pins the routing without
	// over-specifying which side of the tiebreak this particular
	// pair of hashes lands on.
	chainAdvanced := !bytes.Equal(
		fixture.currentTip.Point.Hash,
		fixture.ls.chain.Tip().Point.Hash,
	)
	rolledBack := fixture.ls.chainsyncState == RollbackChainsyncState
	mismatchTracked := fixture.ls.headerMismatchCount > 0
	assert.Truef(
		t,
		chainAdvanced || rolledBack || mismatchTracked,
		"slot-battle header took the stale-drop early return: "+
			"chain.Tip() unchanged, chainsyncState still Syncing, "+
			"and headerMismatchCount=%d. The handler dropped the "+
			"peer's competing same-slot forge before "+
			"chainselection could pick a winner; chains will then "+
			"diverge at every slot battle and the divergence will "+
			"fold into the evolving nonce.",
		fixture.ls.headerMismatchCount,
	)
}
