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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/blinklabs-io/dingo/chain"
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
		if ls.closed.Load() {
			return
		}
		// Emit per-transaction rollback events on this goroutine, not a
		// detached one. handleEventChainUpdate is a SubscribeFunc
		// dispatch, so its invocations are already serialized in
		// chain-update order; emitting inline is what puts the undo
		// events ahead of whatever the ledger publishes for the next
		// chain update. See the ordering contract on
		// emitTransactionRollbackEvents (blinklabs-io/dingo#2287).
		ls.emitTransactionRollbackEvents(data)
	}
}

// emitTransactionRollbackEvents emits TransactionEvent for each transaction
// in the rolled-back blocks, allowing subscribers to undo any state changes.
//
// Ordering contract: the events for one rollback are emitted newest block
// first and, within a block, in reverse transaction order -- the reverse of
// how they were applied -- and all of them are handed to the bus before this
// returns, so every one of them precedes any transaction event the ledger
// emits for a later chain update. publishTransactionEvent carries that order
// through to subscribers. A subscriber maintaining derived state can therefore
// apply the undos and the next block's redos in the order they happened.
func (ls *LedgerState) emitTransactionRollbackEvents(
	rollbackEvt chain.ChainRollbackEvent,
) {
	if ls.config.EventBus == nil {
		return
	}

	for _, block := range rollbackEvt.RolledBackBlocks {
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
	ls.config.EventBus.PublishOrdered(
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
