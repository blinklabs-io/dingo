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
	"testing"

	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stageInFlightBlockfetchBatch puts the fixture in the state that exists while
// a blockfetch batch requested for the rival's continuation is in flight: the
// batch carries the rollback generation current at request time, and one
// fetched block has been delivered and is waiting to be flushed onto the
// chain.
//
// The block is delivered through the real arrival handler rather than pushed
// onto the pending slice, so the test covers what arrival itself is allowed to
// conclude. A range-failure record is seeded for that exact range first, as
// noteBlockfetchRangeUnavailable would leave it after a miss: arrival must not
// clear it, because a delivered block that is never applied leaves the queued
// header just as stuck as one that was never delivered.
func stageInFlightBlockfetchBatch(
	t *testing.T,
	ls *LedgerState,
	block gledger.Block,
) {
	t.Helper()
	point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	ls.blockfetchBatchRollbackGeneration = ls.blockfetchRollbackGeneration.Load()
	ls.blockfetchRangeFailure = blockfetchRangeFailureState{
		slot:  point.Slot,
		hash:  string(point.Hash),
		count: 1,
	}
	connId := testRecycleConnId()
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	require.NoError(t, ls.handleEventBlockfetchBlockDeferred(
		BlockfetchEvent{
			ConnectionId: connId,
			Block:        block,
			Point:        point,
			Type:         uint(block.Type()),
		},
		nil,
	))
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	require.Equal(
		t,
		1,
		ls.blockfetchRangeFailure.count,
		"arrival is not range progress: the block has not been applied yet",
	)
}

// TestStaleBlockfetchBatchIsDiscardedAfterLocalSiblingAdoption covers the race
// between an equal-slot alternative winning chain selection and a blockfetch
// batch that was already in flight for the chain the alternative displaces.
//
// The adoption rolls the rival off the tip while that batch is still being
// delivered, and delivery runs under chainsyncBlockfetchMutex rather than the
// chainsyncMutex the adoption holds, so the flush can land at any point during
// or after the truncation. Its blocks were fetched for headers descending from
// the rival -- the segment the rollback abandoned -- and must not be applied.
//
// The chainsync rollback paths supersede such a batch by re-queueing the
// winning fork's headers and restarting blockfetch. This path does neither, so
// the batch is invalidated by generation: the rollback publishes a newer one
// before truncating, and the flush drops anything still carrying the old one.
func TestStaleBlockfetchBatchIsDiscardedAfterLocalSiblingAdoption(
	t *testing.T,
) {
	// The block the in-flight batch is delivering: the rival's continuation.
	newContinuation := func(t *testing.T, f *siblingFixture) gledger.Block {
		t.Helper()
		return newSiblingTestBlock(
			t,
			f.rival.BlockNumber()+1,
			f.rival.SlotNumber()+10,
			f.rival.Hash(),
			0x44,
			0x44,
			1,
		)
	}

	t.Run("control: the batch lands with no rollback", func(t *testing.T) {
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)

		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))
		assert.Equal(
			t,
			continuation.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"without a rollback the fetched block extends the chain",
		)
		assert.Zero(
			t,
			f.ls.blockfetchRangeFailure.count,
			"a block that extends the chain clears the range failure record",
		)
	})

	t.Run("stale batch after the alternative wins", func(t *testing.T) {
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)
		before := f.ls.blockfetchRollbackGeneration.Load()

		// Lower VRF output wins, so the alternative replaces the rival.
		ours := f.newSibling(t, siblingRivalVrfSeed-1)
		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.True(t, adopted)
		require.Greater(
			t,
			f.ls.blockfetchRollbackGeneration.Load(),
			before,
			"the adoption must publish a new rollback generation",
		)

		// The batch flushes only now, after the rollback and the adoption.
		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))

		assert.Equal(
			t,
			ours.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"a superseded batch must not move the tip off the adopted alternative",
		)
		assert.Empty(
			t,
			f.ls.pendingBlockfetchEvents,
			"the discarded batch must not be left queued for a later flush",
		)
		assert.Equal(
			t,
			1,
			f.ls.blockfetchRangeFailure.count,
			"a discarded block is not range progress: clearing the record "+
				"would reset the count that unsticks the queued header "+
				"blocking local forging",
		)
	})

	t.Run("stale batch inside the rollback window", func(t *testing.T) {
		// The window the generation exists for. The adoption publishes
		// the generation before it truncates, and the flush holds a
		// different mutex, so it can land while the chain is still the
		// one the batch was fetched for. Here the truncation is refused
		// after the generation is published (the fork point is below the
		// Mithril boundary), which leaves the chain at the rival with
		// the batch's block still fitting its tip perfectly.
		//
		// Without the generation the block lands and re-extends a chain
		// the node has already decided to abandon; the tip-fit check
		// cannot see anything wrong with it.
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)
		f.ls.mithrilLedgerSlot = siblingParentSlot + 1
		before := f.ls.blockfetchRollbackGeneration.Load()

		ours := f.newSibling(t, siblingRivalVrfSeed-1)
		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.ErrorIs(t, err, ErrRollbackExceedsMithrilBoundary)
		require.False(t, adopted)
		require.Greater(
			t,
			f.ls.blockfetchRollbackGeneration.Load(),
			before,
			"the generation must be published before the truncation",
		)
		require.Equal(
			t,
			f.rival.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"precondition: the chain still holds the rival, so the "+
				"batch's block fits its tip",
		)

		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))
		assert.Equal(
			t,
			f.rival.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"a batch superseded by a published rollback generation must "+
				"not extend the chain even when its block still fits",
		)
		assert.Equal(
			t,
			1,
			f.ls.blockfetchRangeFailure.count,
			"a discarded block is not range progress",
		)
	})

	t.Run("stale batch after the alternative loses", func(t *testing.T) {
		// A losing alternative performs no rollback, so the batch it
		// raced is still valid and must still land. Invalidating on the
		// attempt rather than on the truncation would stall the chain
		// every time a slot battle is lost.
		f := newSiblingFixture(t)
		continuation := newContinuation(t, f)
		stageInFlightBlockfetchBatch(t, f.ls, continuation)
		before := f.ls.blockfetchRollbackGeneration.Load()

		// Higher VRF output loses.
		ours := f.newSibling(t, siblingRivalVrfSeed+1)
		adopted, err := f.ls.AdoptLocalForgedSibling(ours)
		require.NoError(t, err)
		require.False(t, adopted)
		require.Equal(
			t,
			before,
			f.ls.blockfetchRollbackGeneration.Load(),
			"a lost slot battle truncates nothing and must not invalidate a batch",
		)

		require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))
		assert.Equal(
			t,
			continuation.Hash().Bytes(),
			f.ls.chain.Tip().Point.Hash,
			"the batch the losing alternative raced must still land",
		)
	})
}
