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
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
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

// TestRollbackTxEventsPrecedeLaterForwardTxEvents is the regression test for
// blinklabs-io/dingo#2287. handleEventChainUpdate used to emit per-transaction
// rollback events from a detached goroutine, so a forward transaction event
// published after the rollback returned could reach a subscriber first. A
// subscriber maintaining derived state then applied the undo after the redo
// and was left permanently inconsistent.
//
// The contract asserted here: every TransactionEvent for a rollback is
// delivered before any transaction event the ledger emits afterwards, and the
// rollback's own events keep their reverse-of-application order.
func TestRollbackTxEventsPrecedeLaterForwardTxEvents(t *testing.T) {
	t.Parallel()

	blocks := loadTestBlocksWithTxs(t, 2)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	subID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 4096)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	// Block events share the handler's goroutine; drain them so the
	// handler is never blocked by an unread subscriber.
	blockSubID, blockCh := bus.SubscribeWithBuffer(BlockEventType, 4096)
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

	// Count the transactions the rollback will undo.
	wantRollback := 0
	for _, b := range blocks {
		blk, err := b.Decode()
		require.NoError(t, err)
		wantRollback += len(blk.Transactions())
	}
	require.Positive(t, wantRollback, "rolled-back transactions")

	// Roll back both blocks, then immediately emit a forward transaction
	// event the way a newly applied block does.
	ls.handleEventChainUpdate(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainRollbackEvent{RolledBackBlocks: blocks},
	))

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
				len(got),
				wantRollback+1,
			)
		}
	}

	for i, te := range got[:wantRollback] {
		require.True(
			t,
			te.Rollback,
			"event %d: forward event overtook the rollback (got slot %d idx %d)",
			i,
			te.Point.Slot,
			te.TxIndex,
		)
	}
	require.False(t, got[wantRollback].Rollback, "trailing forward event")

	// Rollback events undo each block in reverse transaction order.
	pos := 0
	for _, b := range blocks {
		blk, err := b.Decode()
		require.NoError(t, err)
		txs := blk.Transactions()
		for i := range slices.Backward(txs) {
			require.Equal(
				t,
				b.Slot,
				got[pos].Point.Slot,
				"rollback event %d block", pos,
			)
			require.Equal(
				t,
				uint32(i),
				got[pos].TxIndex,
				"rollback event %d tx index", pos,
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
		ls.handleEventChainUpdate(event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainRollbackEvent{RolledBackBlocks: blocks},
		))
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
