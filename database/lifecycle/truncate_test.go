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
