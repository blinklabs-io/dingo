// Copyright 2025 Blink Labs Software
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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (ls *LedgerState) handleEventChainUpdate(evt event.Event) {
	switch data := evt.Data.(type) {
	case chain.ChainBlockEvent:
		ls.publishBlockEvent(BlockActionApply, data.Block)
	case chain.ChainRollbackEvent:
		for _, blk := range data.RolledBackBlocks {
			ls.publishBlockEvent(BlockActionUndo, blk)
		}
		// Per-transaction undo events are deliberately NOT emitted here.
		// This handler is a SubscribeFunc dispatch on its own goroutine,
		// reached only after chain.Rollback has already published and
		// returned, so anything it emits races the block-apply goroutine
		// that publishes forward transaction events -- and loses, since
		// that goroutine is free to apply the post-rollback chain the
		// moment the truncation lands. emitRollbackTransactionEvents is
		// called from the rollback path instead, before the truncation.
		// See blinklabs-io/dingo#2287.
	}
}

// emitRollbackTransactionEvents emits a TransactionEvent for every
// transaction in blocks, letting subscribers undo the state those
// transactions produced.
//
// # Ordering contract
//
// Events are emitted newest block first and, within a block, in reverse
// transaction order -- the reverse of how they were applied -- and every one
// of them is on the ledger.tx lane before this returns.
//
// The caller must invoke this *before* the chain is truncated, which is what
// orders these events against the forward transaction events published by the
// block-apply goroutine. That goroutine can only apply a post-rollback block
// after it observes the truncation, so an undo enqueued before the truncation
// is necessarily already ahead of any forward event for a block applied after
// it. Emitting after the truncation -- or from a different goroutine that
// merely learns of it later, as the chain-update handler does -- loses that
// race and lets a subscriber apply an undo after the redo that followed it.
func (ls *LedgerState) emitRollbackTransactionEvents(
	blocks []models.Block,
) {
	if ls.config.EventBus == nil {
		return
	}

	for _, block := range blocks {
		blk, err := block.Decode()
		if err != nil {
			blockPoint := ocommon.Point{
				Slot: block.Slot,
				Hash: block.Hash,
			}
			decodeErr := fmt.Errorf(
				"decode rolled-back block for tx undo events: %w",
				err,
			)
			ls.config.Logger.Error(
				"failed to decode block for rollback tx undo events",
				"component", "ledger",
				"error", decodeErr,
				"slot", block.Slot,
				"hash", hex.EncodeToString(block.Hash),
				"block_number", block.Number,
			)
			ls.config.EventBus.Publish(
				LedgerErrorEventType,
				event.NewEvent(
					LedgerErrorEventType,
					LedgerErrorEvent{
						Error:     decodeErr,
						Operation: "rollback_tx_undo_decode",
						Point:     blockPoint,
					},
				),
			)
			continue
		}

		blockPoint := ocommon.Point{
			Slot: block.Slot,
			Hash: block.Hash,
		}

		txs := blk.Transactions()
		if len(txs) == 0 {
			continue
		}
		for i, tx := range slices.Backward(txs) {
			ls.publishTransactionEvent(TransactionEvent{
				Transaction: tx,
				Point:       blockPoint,
				BlockNumber: block.Number,
				TxIndex:     uint32(i), //nolint:gosec
				Rollback:    true,
			})
		}
	}
}

// publishTransactionEvent publishes a TransactionEvent on the ledger.tx
// ordered lane. Ordering is the point: subscribers derive state from these
// events, and applying a block's transactions out of order -- or applying a
// rollback's undo after the redo that followed it -- leaves that state wrong
// in ways no later event corrects. PublishAsync cannot be used here because
// the shared worker pool reorders (blinklabs-io/dingo#2287).
//
// This stays asynchronous rather than becoming a PublishBlocking like
// publishBlockEvent: the forward-path caller runs inside the block-apply
// database transaction, so parking it on subscriber backpressure would hold
// that transaction open.
func (ls *LedgerState) publishTransactionEvent(evt TransactionEvent) {
	if ls.config.EventBus == nil {
		return
	}
	// publishCtx is cancelled as Close begins. Without it a publish parked
	// on a full lane is released only by the EventBus stopping, and a live
	// restore/truncate closes the LedgerState while deliberately keeping
	// the bus running -- so Close would wait unbounded on a subscriber that
	// stopped draining.
	ctx := ls.publishCtx
	if ctx == nil {
		ctx = context.Background()
	}
	ls.config.EventBus.PublishOrderedContext(
		ctx,
		TransactionEventType,
		event.NewEvent(TransactionEventType, evt),
	)
}

func (ls *LedgerState) publishBlockEvent(
	action BlockAction,
	block models.Block,
) {
	if ls.config.EventBus == nil {
		return
	}
	evt := BlockEvent{
		Action: action,
		Block:  block,
		Point: ocommon.Point{
			Slot: block.Slot,
			Hash: block.Hash,
		},
	}
	if err := ls.config.EventBus.PublishBlocking(
		BlockEventType,
		event.NewEvent(BlockEventType, evt),
	); err != nil {
		// ErrEventBusStopped is expected during teardown when the bus shuts
		// down before LedgerState finishes draining its last events.
		if errors.Is(err, event.ErrEventBusStopped) {
			return
		}
		publishErr := fmt.Errorf(
			"publish %s block event at slot %d block %d: %w",
			action,
			block.Slot,
			block.Number,
			err,
		)
		ls.config.Logger.Error(
			"failed to publish ledger block event",
			"component", "ledger",
			"error", publishErr,
			"action", action,
			"slot", block.Slot,
			"block_number", block.Number,
		)
		if ls.config.FatalErrorFunc != nil {
			ls.config.FatalErrorFunc(publishErr)
		}
	}
}

// blocksAboveSlot returns the blocks a rollback to slot would discard,
// newest first, or nil when they cannot be read.
//
// The descending order matters: it is the reverse of the order the blocks
// were applied in, which is the order their effects have to be undone in, and
// it matches the order chain.rollbackLocked itself reports rolled-back blocks
// (it walks the chain down from the tip). BlocksAfterSlotTxn returns ascending
// slot order, so the result is reversed here.
//
// It must be called before the chain is truncated, while those blocks still
// exist. A read failure is logged and yields no undo events rather than
// failing the rollback: the rollback itself is what keeps the ledger correct,
// and refusing to roll back because a notification could not be built would
// trade a subscriber's derived state for the node's own.
func (ls *LedgerState) blocksAboveSlot(slot uint64) []models.Block {
	if ls.config.EventBus == nil || ls.db == nil {
		return nil
	}
	// Skip the read entirely when nothing consumes ledger.tx, which is the
	// default node: this runs under chainsyncMutex on every rollback, and
	// reading plus decoding up to a security parameter's worth of blocks
	// there to build events for no one is pure cost on the rollback path.
	//
	// A subscriber attaching between this check and the publish misses the
	// events, but it would have missed them anyway -- it was not subscribed
	// when the rollback began, and Publish to zero subscribers is already a
	// no-op. So this weakens no guarantee that existed.
	if !ls.config.EventBus.HasSubscribers(TransactionEventType) {
		return nil
	}
	var blocks []models.Block
	txn := ls.db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		blocks, err = database.BlocksAfterSlotTxn(txn, slot)
		return err
	})
	if err != nil {
		ls.config.Logger.Warn(
			"failed to read rolled-back blocks for tx undo events",
			"component", "ledger",
			"error", err,
			"slot", slot,
		)
		return nil
	}
	slices.Reverse(blocks)
	return blocks
}
