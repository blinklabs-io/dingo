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
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/ledgerdb/dbadapter"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (ls *LedgerState) handleEventChainUpdate(evt event.Event) {
	switch data := evt.Data.(type) {
	case chain.ChainBlockEvent:
		ls.publishBlockEvent(BlockActionApply, data.Block)
		// Every primary-chain addition arrives here, fetched or forged, so
		// a battle settles even when only this node's blocks follow it.
		ls.resolveSlotBattles()
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
	ctx context.Context,
	blocks []models.Block,
) {
	if ls.config.EventBus == nil {
		return
	}
	endorser := ls.rollbackEndorserUndo(blocks)

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

		for i, tx := range slices.Backward(blk.Transactions()) {
			ls.publishTransactionEvent(ctx, TransactionEvent{
				Transaction: tx,
				Point:       blockPoint,
				BlockNumber: block.Number,
				TxIndex:     uint32(i), //nolint:gosec
				Rollback:    true,
			})
		}
		ls.emitEndorserUndo(ctx, block, endorser[string(block.Hash)])
	}
}

// rollbackEndorserUndo returns the endorser transactions the pending rollback
// intent recorded for blocks. Every caller of emitRollbackTransactionEvents
// writes that intent before the truncation, while the rows that name the
// applying block still exist; a rollback too large for the intent loses this
// undo along with its crash replay.
func (ls *LedgerState) rollbackEndorserUndo(
	blocks []models.Block,
) endorserUndo {
	if !slices.ContainsFunc(blocks, func(b models.Block) bool {
		return b.Type == gledger.BlockTypeDijkstra
	}) {
		return nil
	}
	endorser, err := loadRollbackIntentEndorserUndo(ls.db)
	if err != nil {
		ls.config.Logger.Error(
			"failed to load endorser transactions for rollback undo",
			"component", "ledger",
			"error", err,
		)
	}
	return endorser
}

// publishTransactionEvent publishes a TransactionEvent on the ledger.tx
// ordered lane. Ordering is the point: subscribers derive state from these
// events, and applying a block's transactions out of order -- or applying a
// rollback's undo after the redo that followed it -- leaves that state wrong
// in ways no later event corrects. PublishAsync cannot be used here because
// the shared worker pool reorders.
//
// This stays asynchronous rather than becoming a PublishBlocking like
// publishBlockEvent: the forward path calls it from a database AfterCommit
// callback. Enqueueing here keeps subscriber work out of Commit while still
// ensuring the transaction is durable before any Apply becomes visible.
func (ls *LedgerState) publishTransactionEvent(
	ctx context.Context,
	evt TransactionEvent,
) {
	ls.publishTransactionEventOnly(ctx, evt)
	if !evt.Rollback {
		ls.publishUtxoEvent(ctx, utxoEventForTransaction(evt))
	}
}

// publishTransactionEventOnly publishes evt on ledger.tx without its UTxO
// change, for a caller that delivered that change when the change committed.
func (ls *LedgerState) publishTransactionEventOnly(
	ctx context.Context,
	evt TransactionEvent,
) {
	if ls.config.EventBus == nil {
		return
	}
	// publishCtx is cancelled as Close begins. Without it a publish parked
	// on a full lane is released only by the EventBus stopping, and a live
	// restore/truncate closes the LedgerState while deliberately keeping
	// the bus running -- so Close would wait unbounded on a subscriber that
	// stopped draining.
	publishCtx := ls.publishCtx //nolint:contextcheck // the ledger lifecycle context bounds the publish
	if publishCtx == nil {
		publishCtx = ctx
	}
	ls.config.EventBus.PublishOrderedContext(
		publishCtx,
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
// accept, durably records the block bodies needed for undo notification
// recovery, then emits the undo events for the blocks it is about to discard.
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
// The durable record is written before the emit and before chain rollback. A
// chain rollback deletes the block bodies, so persisting only the point after
// the chain mutation would leave recovery unable to reconstruct the events if
// metadata truncation then fails.
//
// The durable record lets startup recovery repeat an undo if the chain or
// metadata rollback fails after publication. Reconciliation uses the same
// outbox with blocks captured from applied ledger state before it publishes
// its undo events.
func (ls *LedgerState) validateAndEmitRollbackUndo(ctx context.Context,
	point ocommon.Point,
) error {
	_, err := ls.validateAndEmitRollbackUndoEmitted(ctx, point)
	return err
}

// validateAndEmitRollbackUndoEmitted is validateAndEmitRollbackUndo, also
// reporting whether it read any block to undo.
//
// A caller that retries the same rollback after the chain rejects it needs
// that answer. Retrying is only sound while nothing was published: a second
// pass emits undo events for blocks the first pass already covered, so a
// ledger.tx consumer would be told to undo them twice. Reporting the read
// rather than the publish is deliberately conservative -- a block that decodes
// to no transactions publishes nothing, and a caller that treats it as emitted
// merely declines a retry it could have taken.
func (ls *LedgerState) validateAndEmitRollbackUndoEmitted(ctx context.Context,
	point ocommon.Point,
) (bool, error) {
	existing, _, pending, loadErr := loadRollbackIntent(ls.db)
	if loadErr != nil || (pending && !pointMatches(existing, point) &&
		point.Slot >= existing.Slot) {
		if err := ls.recoverRollbackIntentLocked(ctx); err != nil {
			return false, fmt.Errorf(
				"complete previous rollback intent: %w",
				err,
			)
		}
	}
	if err := ls.chain.ValidateRollback(ctx, point); err != nil {
		return false, err
	}
	ls.RLock()
	currentTip := ls.currentTip
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	resolved, err := ls.resolveRollbackTarget(
		ctx,
		point,
		currentTip,
	)
	if err != nil {
		return false, err
	}
	if mithrilLedgerSlot > 0 && resolved.Slot < mithrilLedgerSlot {
		return false, ErrRollbackExceedsMithrilBoundary
	}
	belowPruneFloor, pruneFloor, err := ls.rollbackBelowConsumedUtxoPruneFloor(
		resolved,
	)
	if err != nil {
		return false, fmt.Errorf("determine consumed UTxO prune floor: %w", err)
	}
	if belowPruneFloor {
		return false, fmt.Errorf(
			"%w: target slot %d, prune floor %d",
			ErrRollbackBelowUtxoPruneFloor,
			resolved.Slot,
			pruneFloor,
		)
	}
	// The primary chain can be ahead of the applied ledger during catch-up.
	// No ledger state will be rolled back in that case, so do not persist an
	// intent that a crash could later misinterpret as an interrupted rollback.
	durableTip, err := ls.ledgerDB().GetTip(nil)
	if err != nil {
		return false, fmt.Errorf("read durable ledger tip: %w", err)
	}
	if point.Slot > durableTip.Point.Slot {
		return false, nil
	}
	blocks, err := ls.readBlocksAboveSlot(ctx, point.Slot)
	if err != nil {
		return false, fmt.Errorf("read rollback undo blocks: %w", err)
	}
	blocks = blocksAppliedUpTo(blocks, durableTip.Point)
	if len(blocks) == 0 {
		return false, nil
	}
	if err := ls.ensureRollbackIntent(ctx, point, blocks); err != nil {
		if !errors.Is(err, errRollbackIntentTooLarge) {
			return false, err
		}
		ls.config.Logger.Warn(
			"rollback undo payload exceeds durable outbox limit; continuing with live delivery",
			"component", "ledger",
			"error", err,
		)
	}
	ls.emitRollbackTransactionEvents(ctx, blocks)
	return len(blocks) > 0, nil
}

// readBlocksAboveSlot returns the blocks a rollback to slot would discard,
// newest first. It returns storage errors rather than degrading to a partial
// read, so a caller can fail before mutating the chain without having captured
// a durable undo payload.
func (ls *LedgerState) readBlocksAboveSlot(
	ctx context.Context,
	slot uint64,
) ([]models.Block, error) {
	if ls.config.EventBus == nil || ls.db == nil {
		return nil, nil
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
		return nil, nil
	}
	var blocks []models.Block
	txn := ls.ledgerDB().Begin(ctx, false)
	err := ls.ledgerDB().Run(txn, func(txn ledgerdb.Txn) error {
		var err error
		blocks, err = database.BlocksAfterSlotTxn(dbadapter.Unwrap(txn), slot)
		return err
	})
	if err != nil {
		return nil, err
	}
	slices.Reverse(blocks)
	return blocks, nil
}

// blocksAppliedUpTo keeps the blocks at or below tip's slot. The primary chain
// can hold blocks the ledger has not applied yet, and a rollback must not undo
// what no consumer was ever told was applied.
func blocksAppliedUpTo(
	blocks []models.Block,
	tip ocommon.Point,
) []models.Block {
	return slices.DeleteFunc(slices.Clone(blocks), func(b models.Block) bool {
		return b.Slot > tip.Slot
	})
}

// reconciliationUndoBlocks returns the blocks the ledger itself applied
// between ancestor (exclusive) and ledgerTipSlot (inclusive), newest first,
// for the primary-chain/ledger divergence reconciler.
//
// It deliberately does not reuse readBlocksAboveSlot: that helper reads
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
// This is the same best-effort degradation emitRollbackTransactionEvents
// uses for a
// decode failure: the reconciliation is what keeps the ledger correct, and
// it must not fail because a notification could not be built.
//
// Block application writes a block_nonce row for every applied block, with
// no nonce for an era that has no CalculateEtaVFunc (see ledger/eras):
// Byron's BFT/PoA consensus has no VRF nonce to evolve, yet its blocks are
// still applied points here, and durableAppliedFloor and
// latestLedgerPrimaryChainAncestor read the same rows.
//
// A database written before application recorded every era's points has no
// row for an applied Byron block. Such a block has nothing to iterate over,
// so this cannot name it the way an unresolvable block is named -- but it
// can detect that one is missing: the block-number gap between ancestor and
// ledgerTipBlockNumber is independent of block_nonce entirely, so comparing
// it against how many nonce rows accounted for that gap reveals a
// block-number's worth of applied history this function had no record of,
// without guessing at which block or reading it from the primary chain's
// current index (which would risk resolving the wrong branch's block for
// that slot -- the exact failure mode this function exists to avoid, see
// above). Logged and counted via reconciliationUndoMissingRecord, distinctly
// from reconciliationUndoUnresolved, so an operator can tell "we know what's
// missing but can't reach it" apart from "we don't even have a record of it
// existing". Skipped when ancestor's own block cannot be resolved (e.g.,
// after a restart) rather than reporting a false gap from a missing
// baseline.
func (ls *LedgerState) reconciliationUndoBlocks(
	ctx context.Context,
	ancestor ocommon.Point,
	ledgerTipSlot uint64,
	ledgerTipBlockNumber uint64,
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
	nonceTxn := ls.ledgerDB().BeginLedgerOnly(ctx, false)
	nonceRows, err := ls.ledgerDB().GetBlockNoncesInSlotRange(
		ancestor.Slot,
		ledgerTipSlot+1,
		nonceTxn,
	)
	nonceTxn.Release()
	if err != nil {
		ls.config.Logger.Warn(
			"failed to read applied block points for reconciliation undo events",
			"component",
			"ledger",
			"error",
			err,
			"ancestor_slot",
			ancestor.Slot,
			"ledger_tip_slot",
			ledgerTipSlot,
		)
		return nil
	}
	blocks := make([]models.Block, 0, len(nonceRows))
	accountedRows := 0
	for _, row := range slices.Backward(nonceRows) {
		if row.Slot <= ancestor.Slot {
			// The ancestor's own row: it is being kept, not undone.
			continue
		}
		accountedRows++
		block, err := ls.config.ChainManager.BlockByPoint(
			ctx,
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
	if ancestorBlock, err := ls.config.ChainManager.BlockByPoint(ctx, ancestor, nil); err == nil &&
		ledgerTipBlockNumber > ancestorBlock.Number {
		expectedRows := ledgerTipBlockNumber - ancestorBlock.Number
		if expectedRows > uint64(accountedRows) {
			missing := expectedRows - uint64(accountedRows)
			ls.config.Logger.Error(
				"reconciliation undo range contains applied blocks with no "+
					"block_nonce row at all (not merely unresolvable) -- "+
					"likely blocks applied before applied points were "+
					"recorded for every era, whose ledger.tx undo events "+
					"cannot be built",
				"component", "ledger",
				"ancestor_slot", ancestor.Slot,
				"ledger_tip_slot", ledgerTipSlot,
				"missing_records", missing,
			)
			if ls.metrics.reconciliationUndoMissingRecord != nil {
				ls.metrics.reconciliationUndoMissingRecord.Add(
					float64(missing),
				)
			}
		}
	}
	return blocks
}

type transactionEventMutexHeldKey struct{}

func withTransactionEventMutexHeld(ctx context.Context) context.Context {
	return context.WithValue(ctx, transactionEventMutexHeldKey{}, true)
}

func transactionEventMutexHeld(ctx context.Context) bool {
	held, _ := ctx.Value(transactionEventMutexHeldKey{}).(bool)
	return held
}

// rollbackAppliedStateWithUndo rolls metadata back while publishing undo
// events from the ledger's applied-block record. Recovery paths use this when
// the primary chain may still extend beyond the target or may already contain
// a different fork, so reading the active chain range would identify blocks
// that the ledger never applied.
func (ls *LedgerState) rollbackAppliedStateWithUndo(
	ctx context.Context,
	point ocommon.Point,
	repairSameTip bool,
) error {
	// Serialize the applied-tip snapshot, Undo enqueue, and metadata truncation
	// against a block-apply commit and its AfterCommit Apply publication.
	if !transactionEventMutexHeld(ctx) {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()
		ctx = withTransactionEventMutexHeld(ctx)
	}

	ls.RLock()
	currentTip := ls.currentTip
	ls.RUnlock()
	resolved, err := ls.resolveRollbackTarget(ctx, point, currentTip)
	if err != nil {
		return err
	}
	undoBlocks := ls.reconciliationUndoBlocks(
		ctx,
		resolved,
		currentTip.Point.Slot,
		currentTip.BlockNumber,
	)
	return ls.rollbackWithBlocksAndIntentOptions(
		ctx,
		resolved,
		undoBlocks,
		repairSameTip,
		true,
		false,
		true,
	)
}

// publishUtxoEvent publishes evt on the ledger.utxo ordered lane. Like
// publishTransactionEvent it is asynchronous and ordered: subscribers rebuild
// the live set from the sequence.
func (ls *LedgerState) publishUtxoEvent(ctx context.Context, evt UtxoEvent) {
	if ls.config.EventBus == nil ||
		(len(evt.Added) == 0 && len(evt.Removed) == 0) {
		return
	}
	publishCtx := ls.publishCtx //nolint:contextcheck // the ledger lifecycle context bounds the publish
	if publishCtx == nil {
		publishCtx = ctx
	}
	ls.config.EventBus.PublishOrderedContext(
		publishCtx,
		UtxoEventType,
		event.NewEvent(UtxoEventType, evt),
	)
}

// publishUtxoEventAfterCommit publishes evt once txn commits, or at once when
// there is no txn.
func (ls *LedgerState) publishUtxoEventAfterCommit(
	ctx context.Context,
	txn ledgerdb.Txn,
	evt UtxoEvent,
	tip *ocommon.Point,
) {
	ls.stageUtxoSet(
		txn,
		tip,
		evt,
	) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
	if ls.config.EventBus == nil {
		return
	}
	if txn == nil {
		ls.publishUtxoEvent(ctx, evt)
		return
	}
	txn.AfterCommit(func() { ls.publishUtxoEvent(ctx, evt) })
}

// utxoEventForTransaction is the UTxO change a forward transaction event
// makes.
func utxoEventForTransaction(te TransactionEvent) UtxoEvent {
	return utxoEventFor(te.Point, te.TxIndex, te.Transaction)
}

// utxoEventFor is the UTxO change applying tx at point makes: its consumed
// inputs removed and its produced outputs added. For an invalid transaction
// those are its collateral and collateral return.
func utxoEventFor(
	point ocommon.Point,
	txIndex uint32,
	tx lcommon.Transaction,
) UtxoEvent {
	evt := UtxoEvent{
		Point:   point,
		Cause:   UtxoChangeTransaction,
		TxIndex: txIndex,
	}
	for _, in := range tx.Consumed() {
		id := in.Id()
		evt.Removed = append(evt.Removed, models.UtxoId{
			Hash: append([]byte(nil), id[:]...),
			Idx:  in.Index(),
		})
	}
	for _, u := range tx.Produced() {
		id := u.Id.Id()
		evt.Added = append(evt.Added, UtxoOutput{
			Ref: models.UtxoId{
				Hash: append([]byte(nil), id[:]...),
				Idx:  u.Id.Index(),
			},
			Cbor: outputCbor(u.Output),
		})
	}
	return evt
}

// outputCbor returns an output's original CBOR, or encodes it when the
// output was built rather than decoded.
func outputCbor(out lcommon.TransactionOutput) []byte {
	if c := out.Cbor(); len(c) > 0 {
		return c
	}
	c, err := cbor.Encode(out)
	if err != nil {
		return nil
	}
	return c
}

// stageUtxoRollbackEvent reads what a rollback to point does to the live
// UTxO set, before the truncation in txn erases it, and publishes it once
// txn commits. Reading it from the store, rather than from the rolled-back
// blocks, makes it exact on every rollback path.
func (ls *LedgerState) stageUtxoRollbackEvent(
	ctx context.Context,
	txn ledgerdb.Txn,
	point ocommon.Point,
) error {
	if ls.config.UtxoSetOnly {
		// The metadata store holds no utxo rows; the set rolls back from
		// its own journal after the commit and its net change is published.
		ls.stageUtxoSetRollback(
			txn,
			point,
		) //nolint:contextcheck // the change is published after the commit, under the ledger lifecycle
		return nil
	}
	if ls.config.UtxoSet == nil && (ls.config.EventBus == nil ||
		!ls.config.EventBus.HasSubscribers(UtxoEventType)) {
		return nil
	}
	removed, restored, err := ls.ledgerDB().UtxoRollbackChanges(
		ctx,
		point.Slot,
		txn,
	)
	if err != nil {
		return fmt.Errorf("read UTxO rollback changes: %w", err)
	}
	// An empty change still moves the live UTxO set's tip to point.
	if len(removed) == 0 && len(restored) == 0 && ls.config.UtxoSet == nil {
		return nil
	}
	evt := UtxoEvent{
		Point:   point,
		Cause:   UtxoChangeRollback,
		Removed: removed,
		Added:   make([]UtxoOutput, 0, len(restored)),
	}
	for _, u := range restored {
		evt.Added = append(evt.Added, UtxoOutput{
			Ref:  models.UtxoId{Hash: u.TxId, Idx: u.OutputIdx},
			Cbor: u.Cbor,
		})
	}
	ls.publishUtxoEventAfterCommit(ctx, txn, evt, &point)
	return nil
}
