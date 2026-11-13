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
	"context"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// forgeThenRestartAtOwnTipSlot forges slot 10 with a first forger over fence,
// then models the backward clock step that returns the wall slot to slot 10
// while the chain tip is that very block, by building a restarted forger over
// restartFence. It returns the restarted forger's builder and fence.
func forgeThenRestartAtOwnTipSlot(
	t *testing.T,
	fence *fenceTestStore,
	restartFence *fenceTestStore,
) (*forgerTestBuilder, *forgerTestSiblingAdopter) {
	t.Helper()
	const slot = uint64(10)
	own := newForgerTestBlock(slot, altTestRivalBlockNumber)

	first := &forgerTestBuilder{block: own, cbor: []byte{0x01}}
	firstForger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       slot,
			chainTipSlot:      slot - 1,
			slotsPerKESPeriod: 100,
		},
		&forgerCountingLeader{},
		first,
		&forgerTestBroadcaster{},
		fence,
		nil,
		nil,
	)
	require.NoError(
		t, firstForger.checkAndForgeProduction(context.Background()),
	)
	require.Equal(t, 1, first.calls, "the first forge must sign slot 10")

	// The tip is now this pool's own block at slot 10. The restarted forger
	// has an empty in-memory SlotTracker, so it cannot recognise the block.
	ownHash := own.Hash().Bytes()
	chainContext := &forgerTestChainContext{
		parent: altTestParentPoint(),
		tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(slot, ownHash),
			BlockNumber: altTestRivalBlockNumber,
		},
		ok: true,
	}
	second := &forgerTestBuilder{block: own, cbor: []byte{0x01}}
	adopter := &forgerTestSiblingAdopter{adopted: true}
	restarted := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:         slot,
			chainTipSlot:        slot,
			chainTipHash:        ownHash,
			chainTipBlockNumber: altTestRivalBlockNumber,
			slotsPerKESPeriod:   100,
		},
		&forgerCountingLeader{},
		second,
		&forgerTestBroadcaster{},
		restartFence,
		chainContext,
		adopter,
	)
	require.NoError(
		t, restarted.checkAndForgeProduction(context.Background()),
	)
	return second, adopter
}

// TestClockRollbackWithIntactFenceDoesNotForgeSecondBlock is the control: the
// durable fence survives the restart, so a wall slot that returns to the slot
// of this pool's own tip block forges nothing.
func TestClockRollbackWithIntactFenceDoesNotForgeSecondBlock(t *testing.T) {
	t.Parallel()
	fence := &fenceTestStore{}
	second, adopter := forgeThenRestartAtOwnTipSlot(t, fence, fence)

	assert.Zero(t, second.calls)
	assert.Zero(t, second.contextCalls, "must not sign a second block for slot 10")
	assert.Zero(t, adopter.calls)
}

// TestClockRollbackWithLostFenceDoesNotForgeSecondBlock pins the correct
// behaviour when the fence is lost together with the chain data's
// bookkeeping: the wall slot returning to the slot of this pool's own tip
// block must not produce a second, different block for that slot. The equal-slot
// branch has no way to tell its own block from a rival's once the fence and
// SlotTracker are empty, so it forges an "alternative" to its own block.
func TestClockRollbackWithLostFenceDoesNotForgeSecondBlock(t *testing.T) {
	t.Parallel()
	t.Skip("known defect, see https://github.com/blinklabs-io/dingo/issues/6243")
	second, adopter := forgeThenRestartAtOwnTipSlot(
		t, &fenceTestStore{}, &fenceTestStore{},
	)

	assert.Zero(t, second.calls)
	assert.Zero(
		t,
		second.contextCalls,
		"a second block was signed for a slot whose tip block is our own",
	)
	assert.Zero(t, adopter.calls, "our own tip block must not be displaced")
}
