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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// loadTestBlocksWithTxs returns the first want blocks from the immutable
// testdata that actually carry transactions, so a rollback over them produces
// TransactionEvents to assert ordering on.
func loadTestBlocksWithTxs(t *testing.T, want int) []models.Block {
	t.Helper()

	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: nil})
	require.NoError(t, err)
	t.Cleanup(func() { _ = iter.Close() })

	blocks := make([]models.Block, 0, want)
	for len(blocks) < want {
		immBlock, err := iter.Next()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				break
			}
			t.Fatalf("iterate testdata blocks: %v", err)
		}
		if immBlock == nil {
			break
		}
		blk, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		require.NoError(t, err)
		if len(blk.Transactions()) == 0 {
			continue
		}
		blocks = append(blocks, models.Block{
			Slot:   blk.SlotNumber(),
			Hash:   blk.Hash().Bytes(),
			Number: blk.BlockNumber(),
			Type:   uint(blk.Type()),
			Cbor:   immBlock.Cbor,
		})
	}
	require.Len(t, blocks, want, "immutable testdata blocks carrying txs")
	return blocks
}

// TestChainUpdateHandlerPublishesNoTransactionEvents pins the structural
// invariant the ordering guarantee rests on: the chain-update handler must not
// be a ledger.tx producer.
//
// The handler is a SubscribeFunc dispatch on its own goroutine, reached only
// after chain.Rollback has published and returned. By then the block-apply
// goroutine is already free to apply the post-rollback chain and publish
// forward transaction events on the same lane, so anything the handler emits
// races it and loses. Emitting undo events here reproducibly delivered the
// forward event first. See blinklabs-io/dingo#2287.
func TestChainUpdateHandlerPublishesNoTransactionEvents(t *testing.T) {
	t.Parallel()

	blocks := loadTestBlocksWithTxs(t, 2)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 4096)
	require.NotEqual(t, event.EventSubscriberId(0), txSubID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	blockSubID, blockCh := bus.SubscribeWithBuffer(BlockEventType, 4096)
	require.NotEqual(t, event.EventSubscriberId(0), blockSubID)
	t.Cleanup(func() { bus.Unsubscribe(BlockEventType, blockSubID) })

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	ls.handleEventChainUpdate(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainRollbackEvent{RolledBackBlocks: blocks},
	))

	// Block undo events are still the handler's job, one per block.
	for i := range blocks {
		evt := testutil.RequireReceive(
			t, blockCh, 2*time.Second,
			fmt.Sprintf("block undo event %d", i),
		)
		be, ok := evt.Data.(BlockEvent)
		require.True(t, ok)
		require.Equal(t, BlockActionUndo, be.Action)
	}

	// Transaction events are not.
	testutil.RequireNoReceive(
		t, txCh, 250*time.Millisecond,
		"chain-update handler must not publish ledger.tx events",
	)
}

// TestRollbackTxEventsPrecedeLaterForwardTxEvents is the ordering contract
// itself, asserted on the emission path the rollback actually uses: undo
// events are on the lane before the block-apply goroutine's forward events,
// and they carry the reverse of application order.
func TestRollbackTxEventsPrecedeLaterForwardTxEvents(t *testing.T) {
	t.Parallel()

	blocks := loadTestBlocksWithTxs(t, 2)
	// Newest first, as blocksAboveSlot yields them.
	undoOrder := []models.Block{blocks[1], blocks[0]}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	subID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 4096)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	wantRollback := 0
	for _, b := range undoOrder {
		blk, err := b.Decode()
		require.NoError(t, err)
		wantRollback += len(blk.Transactions())
	}
	require.Positive(t, wantRollback)

	// The rollback path emits before chain.Rollback truncates; the
	// block-apply goroutine publishes forward events only after it
	// observes that truncation, i.e. strictly afterwards.
	ls.emitRollbackTransactionEvents(undoOrder)

	forwardBlk, err := blocks[0].Decode()
	require.NoError(t, err)
	forwardTxs := forwardBlk.Transactions()
	require.NotEmpty(t, forwardTxs)
	ls.publishTransactionEvent(TransactionEvent{
		Transaction: forwardTxs[0],
		Point: ocommon.Point{
			Slot: blocks[0].Slot,
			Hash: blocks[0].Hash,
		},
		BlockNumber: blocks[0].Number,
		TxIndex:     0,
		Rollback:    false,
	})

	got := make([]TransactionEvent, 0, wantRollback+1)
	deadline := time.After(10 * time.Second)
	for len(got) < wantRollback+1 {
		select {
		case evt := <-txCh:
			te, ok := evt.Data.(TransactionEvent)
			require.True(t, ok, "unexpected payload %T", evt.Data)
			got = append(got, te)
		case <-deadline:
			t.Fatalf(
				"timed out after %d of %d transaction events",
				len(got), wantRollback+1,
			)
		}
	}

	for i, te := range got[:wantRollback] {
		require.True(
			t, te.Rollback,
			"event %d: forward event overtook the rollback (slot %d idx %d)",
			i, te.Point.Slot, te.TxIndex,
		)
	}
	require.False(t, got[wantRollback].Rollback, "trailing forward event")

	// Undo order is the reverse of application order: newest block first,
	// and within each block the last transaction first.
	pos := 0
	for _, b := range undoOrder {
		blk, err := b.Decode()
		require.NoError(t, err)
		txs := blk.Transactions()
		for i := range slices.Backward(txs) {
			require.Equal(
				t, b.Slot, got[pos].Point.Slot,
				"undo event %d block", pos,
			)
			require.Equal(
				t, uint32(i), got[pos].TxIndex,
				"undo event %d tx index", pos,
			)
			pos++
		}
	}
}

// TestRollbackAndForwardTxEventsStayOrderedAcrossRepeatedCycles guards the
// mechanism rather than one lucky interleaving. Reintroducing a detached
// goroutine for the rollback emission can win a single race by chance; it
// cannot win a hundred consecutive ones, because the goroutine has to decode
// each rolled-back block before it publishes while the forward event that
// follows is published immediately.
func TestRollbackAndForwardTxEventsStayOrderedAcrossRepeatedCycles(
	t *testing.T,
) {
	t.Parallel()

	const cycles = 100
	blocks := loadTestBlocksWithTxs(t, 1)
	blk, err := blocks[0].Decode()
	require.NoError(t, err)
	cycleTxs := blk.Transactions()
	require.NotEmpty(t, cycleTxs)
	txsPerRollback := len(cycleTxs)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	subID, txCh := bus.SubscribeWithBuffer(
		TransactionEventType,
		cycles*(txsPerRollback+1)+16,
	)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	blockSubID, blockCh := bus.SubscribeWithBuffer(BlockEventType, cycles+16)
	require.NotEqual(t, event.EventSubscriberId(0), blockSubID)
	t.Cleanup(func() { bus.Unsubscribe(BlockEventType, blockSubID) })
	go func() {
		for range blockCh {
		}
	}()

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	point := ocommon.Point{Slot: blocks[0].Slot, Hash: blocks[0].Hash}
	for range cycles {
		ls.emitRollbackTransactionEvents(blocks)
		ls.publishTransactionEvent(TransactionEvent{
			Transaction: cycleTxs[0],
			Point:       point,
			BlockNumber: blocks[0].Number,
			TxIndex:     0,
			Rollback:    false,
		})
	}

	// Each cycle must appear as its rollback events followed by its one
	// forward event, with no bleed between cycles.
	want := cycles * (txsPerRollback + 1)
	deadline := time.After(10 * time.Second)
	for i := range want {
		wantRollback := i%(txsPerRollback+1) != txsPerRollback
		select {
		case evt := <-txCh:
			te, ok := evt.Data.(TransactionEvent)
			require.True(t, ok, "unexpected payload %T", evt.Data)
			require.Equal(
				t,
				wantRollback,
				te.Rollback,
				"event %d of cycle %d out of order",
				i%(txsPerRollback+1),
				i/(txsPerRollback+1),
			)
		case <-deadline:
			t.Fatalf("timed out after %d of %d transaction events", i, want)
		}
	}
}

// TestRollbackChainAndStateEmitsUndoEventsBeforeTruncating pins the wiring the
// whole ordering guarantee depends on: the rollback path itself emits the
// per-transaction undo events, and does so while the discarded blocks are
// still readable -- i.e. before chain.Rollback truncates them away.
//
// The fixture's blocks carry stub CBOR, so the emitter takes its decode-failure
// branch and reports a LedgerErrorEvent per block. That is precisely the
// signal wanted here: the event can only be produced if blocksAboveSlot found
// the block, which it can only do before the truncation. Wire the emitter in
// after chain.Rollback instead and no event is produced at all.
func TestRollbackChainAndStateEmitsUndoEventsBeforeTruncating(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	// A ledger.tx subscriber must exist: with none, the rollback path
	// deliberately skips reading the discarded blocks at all.
	txSubID, _ := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotEqual(t, event.EventSubscriberId(0), txSubID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	errSubID, errCh := bus.SubscribeWithBuffer(LedgerErrorEventType, 64)
	require.NotEqual(t, event.EventSubscriberId(0), errSubID)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, errSubID) })

	require.NoError(t, ls.rollbackChainAndState(fixture.ancestorTip.Point))

	// The block above the rollback point was visited by the undo emitter.
	evt := testutil.RequireReceive(
		t, errCh, 2*time.Second,
		"undo-event decode error for the rolled-back block",
	)
	le, ok := evt.Data.(LedgerErrorEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, "rollback_tx_undo_decode", le.Operation)
	require.Equal(
		t,
		fixture.currentTip.Point.Slot,
		le.Point.Slot,
		"undo events must cover the block the rollback discards",
	)
}

// TestRejectedRollbackEmitsNoUndoEvents covers the corruption window: a
// rollback the chain will reject must not tell subscribers to undo blocks that
// stay applied. Emitting has to happen before the truncation for ordering, so
// the rejection is checked first.
func TestRejectedRollbackEmitsNoUndoEvents(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotEqual(t, event.EventSubscriberId(0), txSubID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	errSubID, errCh := bus.SubscribeWithBuffer(LedgerErrorEventType, 64)
	require.NotEqual(t, event.EventSubscriberId(0), errSubID)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, errSubID) })

	// A point at the ancestor's slot but carrying a hash no block has:
	// ValidateRollback rejects it, and crucially it sits *below* the tip,
	// so blocksAboveSlot would find the block at the tip and emit undo
	// events for it if the rejection were not checked first. A point above
	// the tip would not exercise this at all -- there is nothing above it
	// to emit for.
	badPoint := ocommon.NewPoint(
		fixture.ancestorTip.Point.Slot,
		testHashBytes("no-such-block"),
	)
	require.Error(t, ls.rollbackChainAndState(badPoint))

	testutil.RequireNoReceive(
		t, txCh, 250*time.Millisecond,
		"a rejected rollback must not publish undo events",
	)
	testutil.RequireNoReceive(
		t, errCh, 250*time.Millisecond,
		"a rejected rollback must not publish undo decode errors",
	)

	// The chain still holds the block the rollback would have discarded.
	require.Equal(
		t,
		fixture.currentTip.Point.Slot,
		ls.chain.Tip().Point.Slot,
		"chain must be untouched by a rejected rollback",
	)
}

// TestBlocksAboveSlotServesLedgerErrorOnlySubscribers guards the
// no-subscriber fast path against suppressing decode failures: a consumer
// watching only ledger.error still needs to see them.
func TestBlocksAboveSlotServesLedgerErrorOnlySubscribers(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	// Deliberately no ledger.tx subscriber.
	errSubID, errCh := bus.SubscribeWithBuffer(LedgerErrorEventType, 64)
	require.NotEqual(t, event.EventSubscriberId(0), errSubID)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, errSubID) })

	require.NoError(t, ls.rollbackChainAndState(fixture.ancestorTip.Point))

	evt := testutil.RequireReceive(
		t, errCh, 2*time.Second,
		"decode error must reach a ledger.error-only subscriber",
	)
	le, ok := evt.Data.(LedgerErrorEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, "rollback_tx_undo_decode", le.Operation)
}
