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
	"fmt"

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
		// Emit per-transaction rollback events.
		// Hold rollbackMu so Close cannot start Wait between our
		// closed check and Add(1).
		ls.rollbackMu.Lock()
		if ls.closed.Load() {
			ls.rollbackMu.Unlock()
			return
		}
		ls.rollbackWG.Add(1)
		ls.rollbackMu.Unlock()
		go ls.emitTransactionRollbackEvents(data)
	}
}

// emitTransactionRollbackEvents emits TransactionEvent for each transaction
// in the rolled-back blocks, allowing subscribers to undo any state changes.
func (ls *LedgerState) emitTransactionRollbackEvents(
	rollbackEvt chain.ChainRollbackEvent,
) {
	defer ls.rollbackWG.Done()

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
		for i := len(txs) - 1; i >= 0; i-- {
			ls.config.EventBus.PublishAsync(
				TransactionEventType,
				event.NewEvent(
					TransactionEventType,
					TransactionEvent{
						Transaction: txs[i],
						Point:       blockPoint,
						BlockNumber: block.Number,
						TxIndex:     uint32(i), //nolint:gosec
						Rollback:    true,
					},
				),
			)
		}
	}
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
	ls.config.EventBus.Publish(
		BlockEventType,
		event.NewEvent(BlockEventType, evt),
	)
}
