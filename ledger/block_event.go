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
// publishBlockEvent: the forward path calls it from a database AfterCommit
// callback. Enqueueing here keeps subscriber work out of Commit while still
// ensuring the transaction is durable before any Apply becomes visible.
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
		// down before LedgerState finishes draining its last events. An ordinary
		// subscriber may also be detached after it stops draining; that is an
		// isolated optional-consumer failure, not a reason to stop the node or
		// report the block as unpublished to the lossless subscribers.
		if errors.Is(err, event.ErrEventBusStopped) ||
			errors.Is(err, event.ErrEventSubscriberStalled) {
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

// validateAndEmitRollbackUndo rejects a rollback that the chain will not
// accept, then emits the undo events for the blocks it is about to discard.
//
// The two steps belong together and callers must use this rather than pairing
// them by hand. The emit has to happen before the truncation for ordering (see
// emitRollbackTransactionEvents), which means emitting first and letting
// chain.Rollback reject afterwards tells every ledger.tx consumer to undo
// blocks that are still applied -- the exact corruption the ordering exists to
// prevent. Keeping the validate inseparable from the emit is what stops a new
// rollback path from acquiring the emit without the guard, which is how
// rollbackPrimaryChainInSecurityParamWindows initially shipped it.
//
// It does not close the window completely: the chain can still grow between
// this validation and the rollback and push the rollback past the security
// parameter, and an I/O failure mid-truncation is not predictable at all.
// Both leave the chain needing recovery regardless.
func (ls *LedgerState) validateAndEmitRollbackUndo(
	point ocommon.Point,
) error {
	if err := ls.chain.ValidateRollback(point); err != nil {
		return err
	}
	ls.emitRollbackTransactionEvents(ls.blocksAboveSlot(point.Slot))
	return nil
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
	// LedgerErrorEventType counts too: a decode failure while building the
	// undo events is reported there, and a consumer subscribed only to
	// ledger.error would otherwise stop seeing rollback decode failures.
	if !ls.config.EventBus.HasSubscribers(TransactionEventType) &&
		!ls.config.EventBus.HasSubscribers(LedgerErrorEventType) {
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

// reconciliationUndoBlocks returns the blocks the ledger itself applied
// between ancestor (exclusive) and ledgerTipSlot (inclusive), newest first,
// for the primary-chain/ledger divergence reconciler (issue #3516).
//
// It deliberately does not reuse blocksAboveSlot: that helper reads
// whatever the primary chain's blob store currently holds above a slot,
// which is correct for a live, not-yet-applied rollback (the blocks being
// discarded are still there), but wrong here. By the time this reconciler
// runs, chain selection has already replaced the primary chain's content
// between ancestor and the ledger's old tip with a different, competing
// branch -- the blocks the ledger actually applied are gone from that
// index, not merely about to be removed. Reading "whatever occupies these
// slots now" would build undo events for the new branch's blocks, which the
// ledger never applied, instead of the old branch's, which it did.
//
// The durable block_nonce rows are the ledger's own record of which points
// it applied (see durableAppliedFloor), independent of what the primary
// chain currently holds, so this resolves each one by point through
// ChainManager.BlockByPoint -- which checks the manager's retained
// block-cache before the database, so a still-cached abandoned block
// resolves even after chain selection removed it from the active index.
//
// The block cache does not survive a restart, so a point chain selection
// already replaced before this process started (and whose blob row is
// therefore already gone) cannot be resolved at all -- there is no other
// durable copy of an abandoned block's bytes to fall back to. Rather than
// fail the reconciliation over a notification gap -- which, at this
// function's three call sites, means refusing to start the node, halting
// the block-processing reader goroutine, or dropping a live chainsync
// connection, all considerably worse than an incomplete notification -- an
// unresolved point is skipped, logged at error level, and counted via
// reconciliationUndoUnresolved so the gap is observable rather than silent.
// This is the same best-effort degradation blocksAboveSlot uses for a
// decode failure: the reconciliation is what keeps the ledger correct, and
// it must not fail because a notification could not be built.
//
// A block_nonce row only exists for a block whose era has a
// CalculateEtaVFunc (see ledger/eras): Byron's BFT/PoA consensus has no VRF
// nonce to evolve, so a Byron block is never in this query's result at
// all, not merely unresolvable -- reconciliationUndoUnresolved cannot even
// see it to count it. This is not new to this function: durableAppliedFloor
// and latestLedgerPrimaryChainAncestor already key the same reconciliation's
// applied-point search on block_nonce rows, so a divergence spanning Byron
// blocks already has no era-agnostic durable record of applied points to
// resolve an ancestor from, let alone build undo events for. Closing that
// would mean adding an era-agnostic applied-block record the rest of the
// reconciler doesn't have either -- out of scope for issue #3516, which
// bounds and correctly sources this rewind's data, not the reconciler's
// pre-existing era coverage.
func (ls *LedgerState) reconciliationUndoBlocks(
	ancestor ocommon.Point,
	ledgerTipSlot uint64,
) []models.Block {
	if ls.config.EventBus == nil || ls.db == nil ||
		ls.config.ChainManager == nil {
		return nil
	}
	if !ls.config.EventBus.HasSubscribers(TransactionEventType) &&
		!ls.config.EventBus.HasSubscribers(LedgerErrorEventType) {
		return nil
	}
	if ledgerTipSlot <= ancestor.Slot {
		return nil
	}
	nonceRows, err := ls.db.GetBlockNoncesInSlotRange(
		ancestor.Slot,
		ledgerTipSlot+1,
		nil,
	)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to read applied block points for reconciliation undo events",
			"component", "ledger",
			"error", err,
			"ancestor_slot", ancestor.Slot,
			"ledger_tip_slot", ledgerTipSlot,
		)
		return nil
	}
	blocks := make([]models.Block, 0, len(nonceRows))
	for _, row := range slices.Backward(nonceRows) {
		if row.Slot <= ancestor.Slot {
			// The ancestor's own row: it is being kept, not undone.
			continue
		}
		block, err := ls.config.ChainManager.BlockByPoint(
			ocommon.NewPoint(row.Slot, row.Hash),
			nil,
		)
		if err != nil {
			ls.config.Logger.Error(
				"reconciliation cannot build an undo event for an applied "+
					"block: it is no longer resolvable (likely already "+
					"replaced by chain selection and, after a restart, no "+
					"longer cached either); ledger.tx subscribers will not "+
					"see this block undone",
				"component", "ledger",
				"error", err,
				"slot", row.Slot,
				"hash", hex.EncodeToString(row.Hash),
			)
			if ls.metrics.reconciliationUndoUnresolved != nil {
				ls.metrics.reconciliationUndoUnresolved.Inc()
			}
			continue
		}
		blocks = append(blocks, block)
	}
	return blocks
}
