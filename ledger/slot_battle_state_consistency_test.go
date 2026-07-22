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

package ledger_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestSlotBattleResolution_BlockNonceTableIsFullyCleaned exercises the
// block_nonce-table half of the slot-battle resolution path, which is
// where the residual eras-DevNet VRF errors at later eras most plausibly
// originate from.
//
// Scenario: dingo forges block X at slot N, stores its block-nonce row,
// then loses the slot battle to a peer block Y at the same slot. The
// chainsync rollback runs, calling DeleteBlockNoncesAfterPoint for the
// common-ancestor point at slot N-1 (its hash, not slot N). After the
// rollback dingo accepts Y via chainsync and stores Y's nonce row. The
// table must contain Y's row at slot N and NOT X's; otherwise a later
// candidate-nonce computation that walks the per-block nonces could pick
// up a value from a block that no longer exists on the chain, and the
// downstream eta0 derivation would diverge from cardano-node's view —
// which is what we see when dingo fails to verify cardano-node's headers
// at later epoch boundaries.
//
// This test is intentionally narrow: it pins exactly one piece (the
// metadata-store state after the rollback API is called as the chainsync
// path calls it) so a regression there fails with a clear signal.
func TestSlotBattleResolution_BlockNonceTableIsFullyCleaned(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer func() { _ = dbtest.CloseDatabase(db) }()

	const ancestorSlot uint64 = 4
	const battleSlot uint64 = 5

	ancestorHash := bytes.Repeat([]byte{0xa0}, 32)
	dingoHash := bytes.Repeat([]byte{0xd1}, 32)
	cardanoHash := bytes.Repeat([]byte{0xc2}, 32)

	ancestorNonce := bytes.Repeat([]byte{0x01}, 32)
	dingoNonce := bytes.Repeat([]byte{0x0d}, 32)
	cardanoNonce := bytes.Repeat([]byte{0x0c}, 32)

	require.NoError(t, db.SetBlockNonce(
		ancestorHash, ancestorSlot, ancestorNonce, true, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		dingoHash, battleSlot, dingoNonce, false, nil,
	))

	// chainsync's rollback path calls DeleteBlockNoncesAfterPoint with
	// the common ancestor as target. The metadata-store contract is
	// "keep slot == point.Slot && hash == point.Hash; remove
	// everything else" — including same-slot competing rows above the
	// target.
	require.NoError(t, db.DeleteBlockNoncesAfterPoint(
		ocommon.Point{Slot: ancestorSlot, Hash: ancestorHash}, nil,
	))
	dingoLeftover, err := db.GetBlockNonce(
		ocommon.Point{Slot: battleSlot, Hash: dingoHash}, nil,
	)
	require.NoError(t, err)
	require.Empty(
		t, dingoLeftover,
		"dingo's pre-rollback block-nonce row at the battle slot must "+
			"be removed; otherwise the candidate-nonce computation can "+
			"later resolve a slot to dingo's stale nonce instead of the "+
			"canonical (cardano-relay-accepted) block, and dingo's eta0 "+
			"diverges from peers at the next epoch boundary",
	)

	// chainsync now delivers cardano-node's block at the same slot
	// and the per-block nonce gets stored fresh.
	require.NoError(t, db.SetBlockNonce(
		cardanoHash, battleSlot, cardanoNonce, false, nil,
	))

	got, err := db.GetBlockNonce(
		ocommon.Point{Slot: battleSlot, Hash: cardanoHash}, nil,
	)
	require.NoError(t, err)
	require.Equal(t, cardanoNonce, got, "cardano-node's nonce must persist")

	staleByDingoHash, err := db.GetBlockNonce(
		ocommon.Point{Slot: battleSlot, Hash: dingoHash}, nil,
	)
	require.NoError(t, err)
	require.Empty(
		t, staleByDingoHash,
		"the pre-rollback dingo nonce row must remain absent after the "+
			"replacement, even when looked up by its own hash",
	)

	rows, err := db.GetBlockNoncesInSlotRange(
		ancestorSlot, battleSlot+1, nil,
	)
	require.NoError(t, err)
	require.Len(
		t, rows, 2,
		"exactly two rows expected after resolution: the ancestor at "+
			"slot %d and cardano's block at slot %d. Any third row "+
			"means a stale entry slipped past the rollback cleanup.",
		ancestorSlot, battleSlot,
	)
	gotHashes := map[string][]byte{}
	for _, r := range rows {
		gotHashes[string(r.Hash)] = append([]byte(nil), r.Nonce...)
	}
	require.Equal(t, ancestorNonce, gotHashes[string(ancestorHash)])
	require.Equal(t, cardanoNonce, gotHashes[string(cardanoHash)])
	require.NotContains(
		t, gotHashes, string(dingoHash),
		"dingo's pre-rollback hash must not appear in the slot-range "+
			"scan — that scan is what computeCandidateNonce iterates "+
			"to derive the candidate, and any stale entry there is the "+
			"direct cause of an eta0 divergence",
	)
}
