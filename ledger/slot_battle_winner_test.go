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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingSlotBattleRecorder struct{ n atomic.Int64 }

func (r *countingSlotBattleRecorder) RecordSlotBattle() { r.n.Add(1) }

// slotBattleHarness drives slot battles through the real blockfetch flush on a
// real chain. The chain starts at the shared parent with neither contender on
// it, and the security parameter is 2.
type slotBattleHarness struct {
	f      *siblingFixture
	forged gledger.Block
	events <-chan event.Event
	recs   *countingSlotBattleRecorder
}

func newSlotBattleHarness(t *testing.T) *slotBattleHarness {
	t.Helper()
	f := newSiblingFixture(t)
	f.ls.config.CardanoNodeConfig = newTestShelleyGenesisCfgWithK(t, 2)
	f.ls.currentEra = eras.ConwayEraDesc
	f.ls.publishSnapshotsLocked()
	require.NoError(t, f.ls.chain.Rollback(
		context.Background(),
		ocommon.NewPoint(f.parent.SlotNumber(), f.parent.Hash().Bytes()),
	))
	bus := newTestEventBus(t, nil)
	f.ls.config.EventBus = bus
	forged := f.newSibling(t, 0x01)
	f.ls.config.ForgedBlockChecker = &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{siblingSlot: forged.Hash().Bytes()},
	}
	recs := &countingSlotBattleRecorder{}
	f.ls.SetSlotBattleRecorder(recs)
	_, evtCh := bus.Subscribe(forging.SlotBattleEventType)
	return &slotBattleHarness{f: f, forged: forged, events: evtCh, recs: recs}
}

// deliver hands a block to the chain the way blockfetch does.
func (h *slotBattleHarness) deliver(t *testing.T, block gledger.Block) {
	t.Helper()
	h.f.ls.pendingBlockfetchEvents = []BlockfetchEvent{{
		Block: block,
		Point: ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
	}}
	require.NoError(t, h.f.ls.flushPendingBlockfetchBlocksDeferred(nil))
	if string(h.f.ls.chain.Tip().Point.Hash) == string(block.Hash().Bytes()) {
		h.chainUpdate(block)
	}
}

// chainUpdate hands the ledger the chain.update a primary-chain addition
// publishes. The fixture's chain has no event bus, so the harness delivers
// what the ledger's subscription would receive.
func (h *slotBattleHarness) chainUpdate(block gledger.Block) {
	h.f.ls.handleEventChainUpdate(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{
			Point: ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes()),
			Block: models.Block{
				Hash:   block.Hash().Bytes(),
				Slot:   block.SlotNumber(),
				Number: block.BlockNumber(),
			},
		},
	))
}

// forge adds n locally forged blocks on top of base the way the forger does
// and returns the last one.
func (h *slotBattleHarness) forge(
	t *testing.T,
	base gledger.Block,
	n int,
) gledger.Block {
	t.Helper()
	for range n {
		base = newSiblingTestBlock(
			t, base.BlockNumber()+1, base.SlotNumber()+10,
			base.Hash(), 0x55, 0x30, 1,
		)
		require.NoError(t, h.f.ls.chain.AddLocalBlock(
			context.Background(), base,
		))
		h.chainUpdate(base)
	}
	return base
}

// extend delivers n blocks on top of base and returns the last one.
func (h *slotBattleHarness) extend(
	t *testing.T,
	base gledger.Block,
	n int,
) gledger.Block {
	t.Helper()
	for range n {
		base = newSiblingTestBlock(
			t, base.BlockNumber()+1, base.SlotNumber()+10,
			base.Hash(), 0x44, 0x90, 1,
		)
		h.deliver(t, base)
	}
	return base
}

func (h *slotBattleHarness) requireNoEvent(t *testing.T, msg string) {
	t.Helper()
	testutil.RequireNoReceive(t, h.events, 100*time.Millisecond, msg)
}

func (h *slotBattleHarness) requireWon(t *testing.T, want bool) {
	t.Helper()
	evt := testutil.RequireReceive(
		t, h.events, testutil.AsyncWait,
		"timeout waiting for SlotBattleEvent",
	)
	battle, ok := evt.Data.(forging.SlotBattleEvent)
	require.True(t, ok)
	assert.Equal(t, siblingSlot, battle.Slot)
	assert.Equal(t, h.forged.Hash().Bytes(), battle.LocalBlockHash)
	assert.Equal(t, h.f.rival.Hash().Bytes(), battle.RemoteBlockHash)
	assert.Equal(t, want, battle.Won)
}

// The rival is accepted while our block is off the chain, so a verdict taken
// when it arrives says we lost. Chain selection then switches to our block;
// the battle must report that, and only once the slot is settled.
func TestSlotBattleWinnerFollowsLaterChainSwitch(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	h.deliver(t, h.f.rival)
	assert.Equal(t, int64(1), h.recs.n.Load(),
		"the battle is counted when detected")
	h.requireNoEvent(t, "winner must not be reported on arrival")

	pc := h.f.ls.chain
	require.NoError(t, pc.Rollback(
		context.Background(),
		ocommon.NewPoint(h.f.parent.SlotNumber(), h.f.parent.Hash().Bytes()),
	))
	require.NoError(t, pc.AddBlock(context.Background(), h.forged, nil))

	last := h.extend(t, h.forged, 1)
	h.requireNoEvent(t, "winner must wait for the slot to settle")
	h.extend(t, last, 1)
	h.requireWon(t, true)
}

// The rival is accepted and stays the chain: the forged block lost.
func TestSlotBattleWinnerRivalKeepsChain(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	h.deliver(t, h.f.rival)
	last := h.extend(t, h.f.rival, 1)
	h.requireNoEvent(t, "winner must wait for the slot to settle")
	h.extend(t, last, 1)
	h.requireWon(t, false)
}

// Our block is the tip when the rival arrives, so the chain refuses the
// rival; the battle is still counted and reports the forged block as winner.
func TestSlotBattleWinnerRefusedRivalLeavesForgedOnChain(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	require.NoError(t, h.f.ls.chain.AddBlock(
		context.Background(), h.forged, nil,
	))
	h.deliver(t, h.f.rival)
	assert.Equal(t, int64(1), h.recs.n.Load())
	last := h.extend(t, h.forged, 1)
	h.requireNoEvent(t, "winner must wait for the slot to settle")
	h.extend(t, last, 1)
	h.requireWon(t, true)
}

// After the rival, the chain grows only by blocks this node forges. They never
// pass through blockfetch, so settlement must not depend on a peer block
// arriving after the battle.
func TestSlotBattleWinnerSettlesOnLocalChainGrowth(t *testing.T) {
	t.Parallel()

	h := newSlotBattleHarness(t)
	require.NoError(t, h.f.ls.chain.AddLocalBlock(
		context.Background(), h.forged,
	))
	h.deliver(t, h.f.rival)
	assert.Equal(t, int64(1), h.recs.n.Load())
	last := h.forge(t, h.forged, 1)
	h.requireNoEvent(t, "winner must wait for the slot to settle")
	h.forge(t, last, 1)
	h.requireWon(t, true)
}
