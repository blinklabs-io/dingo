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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
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

// TestResolveTargetBySlotSkipsSparseIndexGap verifies the binary search
// tolerates missing intermediate block IDs rather than treating a missing
// probe as fatal — Mithril bootstrap/drain import can legitimately leave
// large gaps in the block-ID sequence. A target low in the surviving ID
// range forces the search's very first probe to land inside the gap (with
// tip ID 1002 and a target at ID 2, the first midpoint is ~501, squarely
// inside the empty 4-999 range) — this alone only proves missing-ID probes
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

// TestTruncateReportsActualDeletedCountForSparseIndex verifies that
// blocksRemoved counts blocks actually deleted, not tipBlock.ID -
// target.ID: that subtraction is only a valid count when every ID in the
// range is a real block, but for a chain with Mithril bootstrap/drain gaps
// that range is merely an upper bound. With a gap of ~997 missing IDs
// between target (ID 3) and tip (ID 1002), an ID-subtraction count would
// report 999 blocks removed even though only 3 (IDs 1000-1002) actually
// exist and get deleted.
func TestTruncateReportsActualDeletedCountForSparseIndex(t *testing.T) {
	f := buildSparseTestChain(t, []uint64{1, 2, 3, 1000, 1001, 1002})
	target := f.blocks[2] // ID 3

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0,
		false,
		0,
	)
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
		context.Background(), f.db, target, 0, false, 0,
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
		context.Background(), f.db, target, 0, false, 0,
	)
	require.NoError(t, err)
	require.Zero(t, blocksRemoved)
}

// TestTruncateRemovesBlobTailAheadOfMetadataTip reproduces the live truncate
// state where blockfetch has persisted blocks beyond the last block applied to
// ledger metadata. The operator target can equal the metadata tip while the
// indexed blob chain still has a speculative tail; that tail must be removed
// rather than treating the truncate as a no-op.
func TestTruncateRemovesBlobTailAheadOfMetadataTip(t *testing.T) {
	f := buildTestChain(t, 5)
	target := f.blocks[2]
	require.NoError(t, f.db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: target.Slot,
			Hash: target.Hash,
		},
		BlockNumber: target.Number,
	}, nil))

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0,
		false,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)

	for _, block := range f.blocks[:3] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.NoError(t, err)
	}
	for _, block := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, target.Slot, tip.Point.Slot)
	require.Equal(t, target.Hash, tip.Point.Hash)
}

func TestTruncateRejectsTargetInUnappliedBlobTail(t *testing.T) {
	f := buildTestChain(t, 5)
	metadataTarget := f.blocks[2]
	require.NoError(t, f.db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: metadataTarget.Slot,
			Hash: metadataTarget.Hash,
		},
		BlockNumber: metadataTarget.Number,
	}, nil))

	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		f.blocks[3],
		0,
		false,
		0,
	)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	for _, block := range f.blocks {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTargetAheadOfTip verifies that a target block ahead
// of the current tip is rejected with an error.
func TestTruncateRejectsTargetAheadOfTip(t *testing.T) {
	f := buildTestChain(t, 3)
	aheadTarget := testBlock(99, 0x63)
	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		aheadTarget,
		0,
		false,
		0,
	)
	require.Error(t, err)
}

// TestTruncateRejectsTargetWithMismatchedHash verifies Truncate defends
// against a target whose ID/hash pair doesn't actually match what's stored
// at that ID on the current chain. ResolveTargetBySlot/ResolveTargetByNumber
// return a block found by binary-searching the blob store's own contiguous
// ID space, so their result is structurally guaranteed to be the block
// genuinely occupying that ID — but a target built some other way (e.g. by
// hash, via ResolveTargetByHash, which has no such structural guarantee)
// could in principle carry an ID/hash pair that doesn't actually match.
// Truncate must verify this itself rather than trust the caller, since
// DeleteBlocksAfter deletes blob-store blocks by ID range while
// database.TruncateAfterSlot deletes metadata by slot cutoff — the two
// only describe the same rollback when target is genuinely the block at
// its own ID.
func TestTruncateRejectsTargetWithMismatchedHash(t *testing.T) {
	f := buildTestChain(t, 5)

	target := f.blocks[2]
	target.Hash = bytes.Repeat(
		[]byte{0xFF},
		32,
	) // does not match what's stored at this ID

	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0,
		false,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	// Nothing must have been touched: every original block still present.
	for _, b := range f.blocks {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTargetWithMismatchedSlot verifies Truncate rejects a
// target with a valid ID/Hash pair but a forged (or otherwise wrong) Slot,
// not just a mismatched Hash. DeleteBlocksAfter cuts blob-store history by
// target.ID, while database.TruncateAfterSlot cuts metadata history by
// target.Slot directly (not by ID) — so a mismatched Slot would make the
// two deletes cut at different points, silently diverging blob and
// metadata history even though the Hash matched.
func TestTruncateRejectsTargetWithMismatchedSlot(t *testing.T) {
	f := buildTestChain(t, 5)

	target := f.blocks[2]
	target.Slot = target.Slot + 1 // does not match what's stored at this ID

	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0,
		false,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	// Nothing must have been touched: every original block still present.
	for _, b := range f.blocks {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTipIDTargetWithMismatchedHash guards against the
// target.ID == tipBlock.ID no-op path reporting success without ever
// verifying target actually names the current tip: a stale or malformed
// caller could pass the tip's own ID paired with a Hash that does not
// match what is genuinely stored there, and without validating on-lineage
// slot/hash before that early return, Truncate would report success (zero
// blocks removed) for a target that was never proven canonical.
func TestTruncateRejectsTipIDTargetWithMismatchedHash(t *testing.T) {
	f := buildTestChain(t, 5)

	target := f.blocks[len(f.blocks)-1] // the tip's own ID
	target.Hash = bytes.Repeat([]byte{0xFF}, 32)

	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0,
		false,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	for _, b := range f.blocks {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTipIDTargetWithMismatchedSlot is
// TestTruncateRejectsTipIDTargetWithMismatchedHash's counterpart for a
// forged Slot rather than a mismatched Hash, at the tip's own ID.
func TestTruncateRejectsTipIDTargetWithMismatchedSlot(t *testing.T) {
	f := buildTestChain(t, 5)

	target := f.blocks[len(f.blocks)-1] // the tip's own ID
	target.Slot = target.Slot + 1

	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		target,
		0,
		false,
		0,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)

	for _, b := range f.blocks {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsTargetImmediatelyFollowedBySameSlotBlock guards
// against a real blob/metadata divergence bug: if the block
// immediately after target shares its exact slot (the Byron epoch-
// boundary-block pattern), DeleteBlocksAfter would remove that later
// block from the blob store by ID, while database.TruncateAfterSlot's
// slot-based cutoff would keep its metadata rows, since they share
// target's own slot -- leaving transactions/certificates/UTxOs and other
// metadata for a block no longer present in the blob store. Truncate must
// refuse such a target outright, before deleting anything, rather than
// let that divergence happen.
func TestTruncateRejectsTargetImmediatelyFollowedBySameSlotBlock(t *testing.T) {
	db := newTestDB(t)

	block1 := testBlock(1, 0x01)
	require.NoError(t, db.BlockCreate(block1, nil))

	// block2 shares block1's slot -- the Byron pattern where an epoch
	// boundary block and the epoch's first regular block occupy the same
	// slot -- and immediately follows it in the ID space.
	block2 := testBlock(2, 0x02)
	block2.Slot = block1.Slot
	require.NoError(t, db.BlockCreate(block2, nil))

	block3 := testBlock(3, 0x03)
	require.NoError(t, db.BlockCreate(block3, nil))

	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: block3.Slot, Hash: block3.Hash},
		BlockNumber: block3.Number,
	}, nil))

	_, err := lifecycle.Truncate(context.Background(), db, block1, 0, false, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorContains(t, err, "same slot")

	// Nothing must have been touched: block2 and block3 must still exist.
	_, err = database.BlockByHash(db, block2.Hash)
	require.NoError(t, err, "block2 must survive a rejected truncate")
	_, err = database.BlockByHash(db, block3.Hash)
	require.NoError(t, err, "block3 must survive a rejected truncate")

	// A target with no same-slot successor (block2 itself: block3's slot
	// differs) must still be allowed -- the rejection is specific to the
	// same-slot case, not a blanket block on this chain.
	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		db,
		block2,
		0,
		false,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), blocksRemoved)
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
	_, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		f.blocks[2],
		0,
		false,
		0,
	)
	require.Error(t, err)

	// blocks[3] is exactly at the boundary and must be allowed.
	blocksRemoved, err := lifecycle.Truncate(
		context.Background(), f.db, f.blocks[3], 0, false, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1), blocksRemoved)
}

// TestTruncateDetectsAndResumesInterruptedBatchedDelete verifies that a
// cancellation after one blob-delete batch cannot leave an undetectable
// database with metadata pointing at missing blocks. The durable marker must
// survive the failure, and a later call must finish the original operation
// without needing to resolve the now-missing old tip first.
func TestTruncateDetectsAndResumesInterruptedBatchedDelete(t *testing.T) {
	f := buildTestChain(t, 5)
	// n=3: Truncate's own pre-mutation ctx.Err() check (verifying a
	// pre-cancelled caller never reaches setPendingTruncate) consumes the
	// first free check, then DeleteBlocksAfter's outer-loop check and the
	// first batch's one in-range item each consume one more -- three free
	// checks land the cancellation right after the first block's delete
	// commits, exactly like the two-check budget did before that
	// pre-mutation check existed.
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}

	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, false, 0)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, f.blocks[1].ID, pending.TargetID)
	require.Equal(t, f.blocks[4].ID, pending.TipID)

	// The first batch committed, while metadata still reports the old tip.
	_, err = f.db.BlockByIndex(f.blocks[2].ID, nil)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].Slot, tip.Point.Slot)

	// A retry resumes from the marker. The target argument is deliberately
	// empty because the durable marker is now the authoritative operation.
	_, err = lifecycle.Truncate(
		context.Background(),
		f.db,
		models.Block{},
		1,
		false,
		0,
	)
	require.NoError(t, err)

	pending, err = lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(t, pending)
	tip, err = f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].Slot, tip.Point.Slot)
	for _, block := range f.blocks[2:] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
}

// pendingTruncateSyncKey mirrors the unexported constant of the same name
// in truncate.go -- reproduced here the same way
// TestTruncateRejectsTargetBeforeMithrilBoundary reproduces
// "mithril_ledger_slot", so this external test package can write a raw
// (possibly corrupted) marker value directly, the same way a bit-flipped
// or truncated on-disk value could occur in practice.
const pendingTruncateSyncKey = "database_lifecycle_truncate_pending"

// TestGetPendingTruncateRejectsMarkerWithZeroTargetID guards a real gap: a
// partially corrupted marker (e.g. only "tipId" survived) decodes as valid
// JSON with TargetID at Go's zero value, which used to pass the only
// check GetPendingTruncate had (TipID >= TargetID, trivially true against
// zero). Resuming with TargetID=0 would delete from the very first block
// -- a full, unintended chain wipe -- instead of failing loudly.
func TestGetPendingTruncateRejectsMarkerWithZeroTargetID(t *testing.T) {
	f := buildTestChain(t, 5)
	require.NoError(t, f.db.SetSyncState(
		pendingTruncateSyncKey, `{"tipId":5}`, nil,
	))

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.Error(t, err)
	require.Nil(t, pending)
}

// TestGetPendingTruncateRejectsMarkerNotMatchingActualBlock guards the
// second half of the same gap: a marker whose fields are all individually
// non-zero and internally consistent (TipID >= TargetID) but whose
// TargetSlot/TargetHash no longer match what is actually stored at
// TargetID on the current chain -- e.g. a corrupted (not merely absent)
// field, or a stale marker left over from before some other operation
// altered the chain. Resuming this would still diverge blob and metadata
// deletion at the wrong point, the same way a fresh (non-resumed) target
// with a forged slot/hash would (see TestTruncateRejectsTargetWith
// MismatchedHash/Slot) -- this is that same on-lineage check, applied to
// a resumed marker instead of a fresh target.
func TestGetPendingTruncateRejectsMarkerNotMatchingActualBlock(t *testing.T) {
	f := buildTestChain(t, 5)
	target := f.blocks[1]
	forgedHash := bytes.Repeat([]byte{0xFF}, 32)
	marker := fmt.Sprintf(
		`{"targetId":%d,"targetSlot":%d,"targetHash":%q,"tipId":%d}`,
		target.ID, target.Slot, base64.StdEncoding.EncodeToString(forgedHash),
		f.blocks[4].ID,
	)
	require.NoError(t, f.db.SetSyncState(pendingTruncateSyncKey, marker, nil))

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.Error(t, err)
	require.Nil(t, pending)
}

// TestGetPendingTruncateRejectsMarkerWithCorruptedTipID verifies that changing
// the deletion upper bound in an otherwise valid durable marker is detected.
func TestGetPendingTruncateRejectsMarkerWithCorruptedTipID(t *testing.T) {
	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, false, 0)
	require.ErrorIs(t, err, context.Canceled)

	marker, err := f.db.GetSyncState(pendingTruncateSyncKey, nil)
	require.NoError(t, err)
	var pending lifecycle.PendingTruncate
	require.NoError(t, json.Unmarshal([]byte(marker), &pending))
	pending.TipID--
	corruptedMarker, err := json.Marshal(pending)
	require.NoError(t, err)
	require.NoError(t, f.db.SetSyncState(
		pendingTruncateSyncKey,
		string(corruptedMarker),
		nil,
	))

	got, err := lifecycle.GetPendingTruncate(f.db)
	require.Error(t, err)
	require.ErrorContains(t, err, "checksum mismatch")
	require.Nil(t, got)

	// Reading the corrupted marker must not continue the interrupted delete.
	for _, b := range f.blocks[3:] {
		_, err := f.db.BlockByIndex(b.ID, nil)
		require.NoError(t, err)
	}
}

// TestTruncateRejectsPendingMarkerAfterBlobTipAdvance verifies that a valid
// marker cannot resume against blobs appended after its authenticated tip.
func TestTruncateRejectsPendingMarkerAfterBlobTipAdvance(t *testing.T) {
	f := buildTestChain(t, 5)
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, false, 0)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, f.blocks[4].ID, pending.TipID)

	advancedTip := testBlock(6, 0x06)
	require.NoError(t, f.db.BlockCreate(advancedTip, nil))

	_, err = lifecycle.Truncate(
		context.Background(),
		f.db,
		models.Block{},
		1,
		false,
		0,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "does not match newer current blob tip")

	// Recovery must fail before deleting any additional blobs or truncating
	// metadata. The already-committed first batch remains the only mutation.
	for _, block := range []models.Block{
		f.blocks[3],
		f.blocks[4],
		advancedTip,
	} {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.NoError(t, err)
	}
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[4].Slot, tip.Point.Slot)
}

// TestTruncateResumesWithStaleHighestIndexAfterPartialCloudDelete models a
// cloud DeleteBlock failure after the block object is removed but before its
// "bi" index is removed. Recovery must authenticate the remaining index
// directly and retry cleanup instead of requiring the missing block object.
func TestTruncateResumesWithStaleHighestIndexAfterPartialCloudDelete(
	t *testing.T,
) {
	f := buildTestChain(t, 5)
	target := f.blocks[1]
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, target, 1, false, 0)
	require.ErrorIs(t, err, context.Canceled)

	tip := f.blocks[4]
	txn := f.db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return f.db.Blob().Delete(
			txn.Blob(),
			types.BlockBlobKey(tip.Slot, tip.Hash),
		)
	}))

	// The highest "bi" entry still exists, but resolving it through the
	// deleted block object reproduces the state that previously blocked
	// GetPendingTruncate before cleanup could retry.
	_, err = f.db.BlockByIndex(tip.ID, nil)
	require.Error(t, err)

	blocksRemoved, err := lifecycle.Truncate(
		context.Background(),
		f.db,
		models.Block{},
		1,
		false,
		0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), blocksRemoved)

	for _, block := range f.blocks[2:] {
		_, err := f.db.BlockByIndex(block.ID, nil)
		require.Error(t, err)
	}
	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(t, pending)
}

// TestTruncateResumesWhenBlobTipWasAheadOfMetadataTip verifies that marker
// validation does not confuse the latest downloaded blob with the applied
// metadata tip. BlockFetch is allowed to persist such a speculative tail.
func TestTruncateResumesWhenBlobTipWasAheadOfMetadataTip(t *testing.T) {
	f := buildTestChain(t, 5)
	appliedTip := f.blocks[3]
	require.NoError(t, f.db.SetTip(ochainsync.Tip{
		Point: ocommon.Point{
			Slot: appliedTip.Slot,
			Hash: appliedTip.Hash,
		},
		BlockNumber: appliedTip.Number,
	}, nil))

	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 3}
	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 1, false, 0)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Equal(t, f.blocks[4].ID, pending.TipID)

	_, err = lifecycle.Truncate(
		context.Background(), f.db, models.Block{}, 1, false, 0,
	)
	require.NoError(t, err)
	tip, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.blocks[1].Slot, tip.Point.Slot)
}

// TestTruncateRejectsPreCancelledContextWithoutRecordingMarker verifies
// that a context already cancelled before Truncate ever attempts a
// mutation is reported the same way every other pre-mutation validation
// failure is -- ErrTruncateNotStarted-wrapped -- and, critically, does
// not leave a durable pending marker behind: without this, a caller would
// be forced to "resume" an operation that in fact never touched the
// database at all.
func TestTruncateRejectsPreCancelledContextWithoutRecordingMarker(
	t *testing.T,
) {
	f := buildTestChain(t, 5)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := lifecycle.Truncate(ctx, f.db, f.blocks[1], 0, false, 0)
	require.Error(t, err)
	require.ErrorIs(t, err, lifecycle.ErrTruncateNotStarted)
	require.ErrorIs(t, err, context.Canceled)

	pending, err := lifecycle.GetPendingTruncate(f.db)
	require.NoError(t, err)
	require.Nil(
		t,
		pending,
		"a pre-cancelled Truncate must not record a pending marker",
	)
}
