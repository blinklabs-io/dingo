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

package lifecycle_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// buildTestChain creates n blocks (IDs and Numbers 1..n) and sets the tip
// to the last one.
func buildTestChain(t *testing.T, n uint64) *chainFixture {
	t.Helper()
	db := newTestDB(t)
	blocks := make([]models.Block, 0, n)
	for id := uint64(1); id <= n; id++ {
		block := testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	return &chainFixture{db: db, blocks: blocks}
}

type chainFixture struct {
	db     *database.Database
	blocks []models.Block
}

// buildSparseTestChain creates blocks at the given, deliberately
// non-contiguous internal IDs (Slot/Number scaled from each ID the same
// way testBlock does) and sets the tip to the last one -- simulating the
// real gap Mithril bootstrap/drain import can legitimately leave in the
// block-ID space (see database.BlockAtOrAfterIndex's doc comment and its
// own TestBlockAtOrAfterIndexSkipsSparseIndexes).
func buildSparseTestChain(t *testing.T, ids []uint64) *chainFixture {
	t.Helper()
	db := newTestDB(t)
	blocks := make([]models.Block, 0, len(ids))
	for _, id := range ids {
		block := testBlock(id, byte(id))
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: last.Slot, Hash: last.Hash},
		BlockNumber: last.Number,
	}, nil))
	return &chainFixture{db: db, blocks: blocks}
}

// TestResolveTargetByHash verifies that a block hash resolves to the
// matching block.
func TestResolveTargetByHash(t *testing.T) {
	f := buildTestChain(t, 5)
	target, err := lifecycle.ResolveTargetByHash(f.db, f.blocks[2].Hash)
	require.NoError(t, err)
	require.Equal(t, f.blocks[2].ID, target.ID)
}

// TestResolveTargetBySlot verifies that a slot with its own block
// resolves to that exact block.
func TestResolveTargetBySlot(t *testing.T) {
	f := buildTestChain(t, 5)
	target, err := lifecycle.ResolveTargetBySlot(f.db, f.blocks[2].Slot)
	require.NoError(t, err)
	require.Equal(t, f.blocks[2].ID, target.ID)
}

// TestResolveTargetBySlotResolvesToNearestAncestor exercises the common
// case for a real chain: the vast majority of slots have no block of
// their own (average ~20s slot time), so a slot in between two blocks
// must resolve to the highest-slot block at or before it, not error out.
func TestResolveTargetBySlotResolvesToNearestAncestor(t *testing.T) {
	f := buildTestChain(t, 5) // blocks[i].Slot == (i+1)*10

	target, err := lifecycle.ResolveTargetBySlot(f.db, f.blocks[2].Slot+5)
	require.NoError(t, err)
	require.Equal(t, f.blocks[2].ID, target.ID)

	// A slot past the tip resolves to the tip itself (no-op truncate),
	// rather than erroring — mirrors Truncate's own idempotency at tip.
	tipTarget, err := lifecycle.ResolveTargetBySlot(f.db, f.blocks[4].Slot+100)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].ID, tipTarget.ID)
}

// TestResolveTargetByNumber verifies that every block number in the
// chain resolves back to its own block.
func TestResolveTargetByNumber(t *testing.T) {
	f := buildTestChain(t, 5)
	for _, b := range f.blocks {
		target, err := lifecycle.ResolveTargetByNumber(f.db, b.Number)
		require.NoError(t, err)
		require.Equal(t, b.ID, target.ID)
	}
}

// TestResolveTargetByNumberAheadOfTipErrors verifies that a block number
// ahead of the current tip returns an error.
func TestResolveTargetByNumberAheadOfTipErrors(t *testing.T) {
	f := buildTestChain(t, 5)
	_, err := lifecycle.ResolveTargetByNumber(f.db, 100)
	require.Error(t, err)
}

// TestResolveTargetBySlotSkipsSparseIndexGap guards against comment-32's
// original gap: the binary search used to treat any missing intermediate
// block ID as fatal, immediately erroring out instead of adapting — but
// Mithril bootstrap/drain import can legitimately leave large gaps in
// the block-ID sequence. A target low in the surviving ID range forces
// the search's very first probe to land inside the gap (with tip ID
// 1002 and a target at ID 2, the first midpoint is ~501, squarely inside
// the empty 4-999 range) — this alone only proves missing-ID probes
// don't hard-fail; TestResolveTargetBySlotSkipsSparseIndexGapAboveGap
// below additionally proves the search can still find a target *above*
// the gap, which a naive "treat a miss as answer-must-be-lower" fallback
// (without an actual seek-forward) would get wrong.
func TestResolveTargetBySlotSkipsSparseIndexGap(t *testing.T) {
	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})

	target, err := lifecycle.ResolveTargetBySlot(f.db, f.blocks[1].Slot)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].ID, target.ID)
}

// TestResolveTargetBySlotSkipsSparseIndexGapAboveGap targets a block
// *above* the gap (ID 1001, out of 1000-1002). A fallback that merely
// treats a missing-ID probe as "the answer must be below this point"
// (shrinking hi without ever seeking forward) converges toward the low
// 1-3 range and never finds this target at all; only an actual
// seek-forward (BlockAtOrAfterIndex) resolves it correctly.
func TestResolveTargetBySlotSkipsSparseIndexGapAboveGap(t *testing.T) {
	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})

	target, err := lifecycle.ResolveTargetBySlot(f.db, f.blocks[4].Slot)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].ID, target.ID)
}

// TestResolveTargetByNumberSkipsSparseIndexGap is
// TestResolveTargetBySlotSkipsSparseIndexGap's counterpart for
// ResolveTargetByNumber, which has the identical binary-search structure.
func TestResolveTargetByNumberSkipsSparseIndexGap(t *testing.T) {
	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})

	target, err := lifecycle.ResolveTargetByNumber(f.db, f.blocks[1].Number)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].ID, target.ID)
}

// TestResolveTargetByNumberSkipsSparseIndexGapAboveGap is
// TestResolveTargetBySlotSkipsSparseIndexGapAboveGap's counterpart for
// ResolveTargetByNumber.
func TestResolveTargetByNumberSkipsSparseIndexGapAboveGap(t *testing.T) {
	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})

	target, err := lifecycle.ResolveTargetByNumber(f.db, f.blocks[4].Number)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].ID, target.ID)
}

// TestTruncateReportsActualDeletedCountForSparseIndex guards against
// comment-36's original gap: blocksRemoved used to be computed as
// tipBlock.ID - target.ID, which is only a valid count when every ID in
// that range is a real block — for a chain with Mithril bootstrap/drain
// gaps, that range is merely an upper bound, and subtracting index
// values there wildly overcounts. With a gap of ~997 missing IDs between
// target (ID 3) and tip (ID 1002), the old formula would report 999
// blocks removed even though only 3 (IDs 1000-1002) actually exist and
// get deleted.
func TestTruncateReportsActualDeletedCountForSparseIndex(t *testing.T) {
	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})
	target := f.blocks[2] // ID 3

	blocksRemoved, err := lifecycle.Truncate(context.Background(), f.db, target, 0)
	require.NoError(t, err)
	require.Equal(
		t, uint64(3), blocksRemoved,
		"must count the 3 blocks (IDs 1000-1002) actually deleted, not "+
			"tipBlock.ID-target.ID (999)",
	)

	for _, b := range f.blocks[:3] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
	for _, b := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
}

// TestTruncateRemovesBlocksAndIsIdempotentAtTip verifies that Truncate
// removes everything past the target and is a zero-block no-op if repeated.
func TestTruncateRemovesBlocksAndIsIdempotentAtTip(t *testing.T) {
	f := buildTestChain(t, 5)
	target := f.blocks[2] // truncate to block 3, removing 4 and 5

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(), f.db, target, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)

	for _, b := range f.blocks[:3] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
	for _, b := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}

	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Point.Slot)

	// Truncating again to the same (now current) target is a no-op, not
	// an error, and reports zero blocks removed.
	blocksRemoved, err = lifecycle.Truncate(
		context.Background(), f.db, target, 0,
	)
	require.NoError(t, err)
	require.Zero(t, blocksRemoved)
}

// TestTruncateRejectsTargetAheadOfTip verifies that a target block ahead
// of the current tip is rejected with an error.
func TestTruncateRejectsTargetAheadOfTip(t *testing.T) {
	f := buildTestChain(t, 3)
	aheadTarget := testBlock(99, 0x63)
	_, err := lifecycle.Truncate(context.Background(), f.db, aheadTarget, 0)
	require.Error(t, err)
}

// TestTruncateRejectsTargetWithMismatchedHash guards against comment-24's
// defense-in-depth gap: ResolveTargetBySlot/ResolveTargetByNumber return a
// block found by binary-searching the blob store's own contiguous ID
// space, so their result is structurally guaranteed to be the block
// genuinely occupying that ID on the current chain — but a target built
// some other way (e.g. by hash, via ResolveTargetByHash, which has no such
// structural guarantee) could in principle carry an ID/hash pair that
// doesn't actually match what's stored at that ID. Truncate must verify
// this itself rather than trust the caller, since DeleteBlocksAfter
// deletes blob-store blocks by ID range while database.TruncateAfterSlot
// deletes metadata by slot cutoff — the two only describe the same
// rollback when target is genuinely the block at its own ID.
func TestTruncateRejectsTargetWithMismatchedHash(t *testing.T) {
	f := buildTestChain(t, 5)

	target := f.blocks[2]
	target.Hash = bytes.Repeat([]byte{0xFF}, 32) // does not match what's stored at this ID

	_, err := lifecycle.Truncate(context.Background(), f.db, target, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	// Nothing must have been touched: every original block still present.
	for _, b := range f.blocks {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTargetBeforeMithrilBoundary verifies that a target
// before the recorded Mithril floor is refused, but exactly at it is allowed.
func TestTruncateRejectsTargetBeforeMithrilBoundary(t *testing.T) {
	f := buildTestChain(t, 5)
	boundarySlot := f.blocks[3].Slot
	require.NoError(t, f.db.SetSyncState(
		"mithril_ledger_slot", strconv.FormatUint(boundarySlot, 10), nil,
	))

	// blocks[2] is before the boundary; truncating there must be refused.
	_, err := lifecycle.Truncate(context.Background(), f.db, f.blocks[2], 0)
	require.Error(t, err)

	// blocks[3] is exactly at the boundary and must be allowed.
	blocksRemoved, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[3], 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), blocksRemoved)
}
