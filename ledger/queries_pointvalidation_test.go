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
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedBlockAtSlot writes a minimal block index entry for slot/hash, enough
// for database.BlockBySlot to find it -- Query's verifyPointOnChain doesn't
// decode the block, only compares Hash, so no real CBOR content is needed.
func seedBlockAtSlot(t *testing.T, ls *LedgerState, slot uint64, hash []byte) {
	t.Helper()
	require.NoError(t, ls.db.BlockCreate(models.Block{
		ID:   slot,
		Slot: slot,
		Hash: hash,
	}, nil))
}

// TestQuery_PinnedPointOnChain_Succeeds covers the common case: a pinned
// point naming a block this node's current chain actually has at that slot
// must be accepted, dispatching through to the query as normal
// (blinklabs-io/dingo#382, #5 in the follow-up review).
func TestQuery_PinnedPointOnChain_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	hash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 100, hash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, hash),
	}, nil))

	_, err := ls.Query(utxoWholeQuery(), QueryPoint{Slot: 100, Hash: hash})
	require.NoError(t, err)
}

// TestQuery_PinnedPointWrongHash_Rejected covers a rollback/fork scenario:
// the caller acquired a point at slot 100 naming hash A, but this node's
// current chain now has a different block (hash B) at slot 100 -- e.g. a
// rollback happened between Acquire and Query. Before verifyPointOnChain
// existed, a purely slot-keyed reconstruction would have silently answered
// against the new fork's data; it must instead fail with ErrPointNotOnChain.
// The tip is set at slot 100 (not left at origin) so this exercises the
// hash-mismatch rejection specifically, not the separate above-the-tip
// rejection TestQuery_PinnedPointAboveTip_Rejected covers.
func TestQuery_PinnedPointWrongHash_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	actualHash := bytes.Repeat([]byte{0xAB}, 32)
	acquiredHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, actualHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, actualHash),
	}, nil))

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 100, Hash: acquiredHash},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// TestQuery_PinnedPointNoBlockAtSlot_Rejected covers a point naming a slot
// this node has no block for at all (never seen it, or it was never a real
// chain point) -- must fail with ErrPointNotOnChain rather than silently
// treating "no block" as equivalent to "empty state as of that slot". The
// tip is set past slot 999 (via a block at a later slot) so this exercises
// the no-block-found rejection specifically, not the above-the-tip one.
func TestQuery_PinnedPointNoBlockAtSlot_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	laterHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 1000, laterHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1000, laterHash),
	}, nil))

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 999, Hash: bytes.Repeat([]byte{0xEE}, 32)},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPointNotOnChain))
}

// TestQuery_PinnedPointAboveTip_Rejected covers the gap a purely
// slot-keyed lookup left open: database.BlockBySlot has no notion of
// "applied tip" at all, so a block retained in the blob store at a slot
// ahead of what has actually been applied to ledger state (e.g. a
// header-ahead-of-ledger buffer entry) could satisfy both the slot and
// hash check even though the ledger state a query is about to read from
// has not incorporated it. A point naming that block must be rejected
// as not on the (applied) chain, regardless of whether the block itself
// is retained.
func TestQuery_PinnedPointAboveTip_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	tipHash := bytes.Repeat([]byte{0xAB}, 32)
	aheadHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, tipHash)
	seedBlockAtSlot(t, ls, 200, aheadHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, tipHash),
	}, nil))

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 200, Hash: aheadHash},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// TestQuery_SlotZeroWithHashIsPinned_Rejected covers QueryPoint.pinned()'s
// own predicate: a slot-0 point with a nonempty Hash is a real chain point
// (a real Byron genesis-adjacent block could sit at slot 0), not the origin
// sentinel (QueryPoint{}, both fields zero), and must still go through
// verifyPointOnChain -- not be silently treated as unpinned and answered
// from live state. This node has no such block, so it must be rejected the
// same way any other non-existent point would be.
func TestQuery_SlotZeroWithHashIsPinned_Rejected(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	_, err := ls.Query(
		utxoWholeQuery(),
		QueryPoint{Slot: 0, Hash: bytes.Repeat([]byte{0xEE}, 32)},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPointNotOnChain)
}

// epochNoQuery wraps the leaf query the way the wire delivers GetEpochNo.
func epochNoQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyEpochNoQuery{},
		},
	}
}

// TestQuery_SlotZeroPinned_DispatchesHistorically goes one step past
// TestQuery_SlotZeroWithHashIsPinned_Rejected: this node genuinely has a
// block at slot 0 matching the acquired hash, so verifyPointOnChain accepts
// it -- proving pinned-ness survives all the way through dispatch to the
// handler is the real point of this test. Every point-aware handler was
// passed a bare `asOfSlot uint64` derived from at.Slot, and every one of
// them treated asOfSlot == 0 as "live" -- so a point genuinely pinned at
// slot 0 passed validation only to have the handler silently ignore the
// pin and answer with the live epoch (6) instead of slot 0's own epoch
// (0). Handlers now take the whole QueryPoint and check at.pinned()
// instead of comparing a bare slot against zero.
func TestQuery_SlotZeroPinned_DispatchesHistorically(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	genesisHash := bytes.Repeat([]byte{0xAB}, 32)
	seedBlockAtSlot(t, ls, 0, genesisHash)
	seedEpochs(t, ls, map[uint64]uint64{0: 0, 600: 6})
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.Query(
		epochNoQuery(),
		QueryPoint{Slot: 0, Hash: genesisHash},
	)
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	assert.Equal(
		t, uint64(0), arr[0],
		"a point pinned at slot 0 must resolve slot 0's own epoch, not "+
			"silently fall through to the live epoch",
	)
}

// TestQuery_UnpinnedSkipsPointValidation covers the live path: a zero-value
// QueryPoint must not trigger verifyPointOnChain at all (no block need
// exist at slot 0), preserving every existing unpinned query's behavior.
func TestQuery_UnpinnedSkipsPointValidation(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	_, err := ls.Query(utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
}
