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
	"bytes"
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
	"github.com/blinklabs-io/dingo/internal/safedecode"
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
// transaction in blocks, and for every endorser-block transaction a
// certifying block applied, letting subscribers undo the state those
// transactions produced.
//
// # Ordering contract
//
// Events are emitted newest block first and, within a block, in reverse
// application order -- the block's own transactions in reverse, then its
// certified endorser block's in reverse -- and every one of them is on the
// ledger.tx lane before this returns.
//
// The caller must invoke this *before* the chain is truncated, which is what
// orders these events against the forward transaction events published by the
// block-apply goroutine. That goroutine can only apply a post-rollback block
// after it observes the truncation, so an undo enqueued before the truncation
// is necessarily already ahead of any forward event for a block applied after
// it. Emitting after the truncation -- or from a different goroutine that
// merely learns of it later, as the chain-update handler does -- loses that
// race and lets a subscriber apply an undo after the redo that followed it.
//
// An unavailable endorser block and a block that fails to decode are reported
// as a LedgerErrorEvent and skipped. Transaction events are notifications, so an incomplete undo
// must not prevent the ledger and primary chain from following a valid
// rollback.
func (ls *LedgerState) emitRollbackTransactionEvents(
	ctx context.Context,
	blocks []models.Block,
) error {
	if ls.config.EventBus == nil {
		return nil
	}
	undoBlocks := ls.rollbackUndoBlocks(
		ctx,
		blocks,
		ls.rollbackEndorserUndo(blocks),
	)
	// Resolve every certifying block's endorser transactions before publishing
	// anything so the successfully resolved undo events retain their ordering.
	for _, u := range undoBlocks {
		if u.endorserErr == nil {
			continue
		}
		undoErr := fmt.Errorf(
			"resolve certified endorser block for tx undo events: %w",
			u.endorserErr,
		)
		ls.config.Logger.Error(
			"failed to resolve endorser block for rollback tx undo events",
			"component", "ledger",
			"error", undoErr,
			"slot", u.block.Slot,
			"hash", hex.EncodeToString(u.block.Hash),
			"block_number", u.block.Number,
		)
		ls.config.EventBus.Publish(
			LedgerErrorEventType,
			event.NewEvent(
				LedgerErrorEventType,
				LedgerErrorEvent{
					Error:     undoErr,
					Operation: "rollback_tx_undo_endorser_block",
					Point: ocommon.Point{
						Slot: u.block.Slot,
						Hash: u.block.Hash,
					},
				},
			),
		)
	}

	for _, u := range undoBlocks {
		block := u.block
		blockPoint := ocommon.Point{
			Slot: block.Slot,
			Hash: block.Hash,
		}
		if u.decodeErr != nil {
			decodeErr := fmt.Errorf(
				"decode rolled-back block for tx undo events: %w",
				u.decodeErr,
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

		txs := u.blk.Transactions()
		for i, tx := range slices.Backward(txs) {
			ls.publishTransactionEvent(ctx, TransactionEvent{
				Transaction: tx,
				Point:       blockPoint,
				BlockNumber: block.Number,
				TxIndex:     uint32(i), //nolint:gosec
				Rollback:    true,
			})
		}
		// A certifying block applied its endorser block's transactions
		// before its own, so they are undone after them.
		for _, ebTx := range slices.Backward(u.endorserTxs) {
			ls.publishTransactionEvent(ctx, TransactionEvent{
				Transaction: ebTx.tx,
				Point:       blockPoint,
				BlockNumber: block.Number,
				TxIndex:     uint32(ebTx.index), //nolint:gosec
				Rollback:    true,
			})
		}
	}
	return nil
}

type endorserTransaction struct {
	tx    lcommon.Transaction
	index int
}

type rollbackUndoBlock struct {
	block       models.Block
	blk         gledger.Block
	decodeErr   error
	endorserTxs []endorserTransaction
	endorserErr error
}

// rollbackUndoBlocks decodes blocks, newest first, and resolves the
// endorser-block transactions each certifying block applied.
//
// Resolution walks the set oldest first, in application order, recording
// every transaction an older block applied. A replayed rollback runs after
// the metadata dropped the rolled-back blocks' transaction rows, so the set
// itself is then the only record that an older block already applied a
// transaction a newer endorser block repeats; without it the repeat would be
// undone twice.
//
// A block the rollback outbox recorded endorser transactions for uses them:
// they were read from the rows that name the applying block before the
// truncation removed those rows. Any other block is resolved from the endorser
// block provider and the metadata store.
func (ls *LedgerState) rollbackUndoBlocks(
	ctx context.Context,
	blocks []models.Block,
	recorded endorserUndo,
) []rollbackUndoBlock {
	undo := make([]rollbackUndoBlock, len(blocks))
	rolledBack := make(map[string]models.Block, len(blocks))
	for i, block := range blocks {
		undo[i].block = block
		undo[i].blk, undo[i].decodeErr = block.Decode()
		rolledBack[string(block.Hash)] = block
	}
	applied := make(map[string]struct{})
	for i := len(undo) - 1; i >= 0; i-- {
		u := &undo[i]
		if u.decodeErr != nil {
			continue
		}
		if txs, ok := recorded[string(u.block.Hash)]; ok {
			u.endorserTxs, u.endorserErr = recordedEndorserTransactions(
				txs,
				applied,
			)
		} else {
			u.endorserTxs, u.endorserErr = ls.certifiedEndorserTransactions(
				ctx,
				u.blk,
				u.block.Hash,
				rolledBack,
				applied,
			)
		}
		for _, ebTx := range u.endorserTxs {
			applied[string(ebTx.tx.Hash().Bytes())] = struct{}{}
		}
		for _, tx := range u.blk.Transactions() {
			applied[string(tx.Hash().Bytes())] = struct{}{}
		}
	}
	return undo
}

// recordedEndorserTransactions decodes the endorser transactions the rollback
// outbox recorded for a ranking block, omitting one an older block of the same
// rollback already applied.
func recordedEndorserTransactions(
	recorded []durableEndorserTx,
	applied map[string]struct{},
) ([]endorserTransaction, error) {
	txs := make([]endorserTransaction, 0, len(recorded))
	for _, etx := range recorded {
		tx, err := safedecode.DijkstraBlockTransaction(etx.Cbor)
		if err != nil {
			return nil, fmt.Errorf(
				"decode recorded endorser tx %d: %w",
				etx.Index,
				err,
			)
		}
		if _, older := applied[string(tx.Hash().Bytes())]; older {
			continue
		}
		txs = append(txs, endorserTransaction{tx: tx, index: int(etx.Index)})
	}
	return txs, nil
}

// certifiedEndorserTransactions returns the endorser-block transactions a
// ranking block applied, in application order. Apply records a transaction
// once, so an occurrence repeated within the endorser block, one already
// recorded by another block, or one in applied (recorded by an older block of
// the same rollback) is omitted. A transaction with no recorded row is
// otherwise kept, so an undo replayed after its row was rolled back is still
// emitted.
func (ls *LedgerState) certifiedEndorserTransactions(
	ctx context.Context,
	blk gledger.Block,
	blockHash []byte,
	rolledBack map[string]models.Block,
	applied map[string]struct{},
) ([]endorserTransaction, error) {
	ebHash, ebSlot, referenced, err := ls.leiosEndorserBlockForRollback(
		ctx,
		blk,
		rolledBack,
	)
	if err != nil || !referenced {
		return nil, err
	}
	if ls.config.EndorserBlockProvider == nil {
		return nil, errors.New("no endorser block provider")
	}
	rawTxs, ok := ls.config.EndorserBlockProvider(ebHash.Bytes(), ebSlot)
	if !ok {
		return nil, fmt.Errorf("endorser block %s unavailable", ebHash)
	}
	seen := make(map[string]struct{}, len(rawTxs))
	txs := make([]endorserTransaction, 0, len(rawTxs))
	for i, raw := range rawTxs {
		txCbor, _, err := decodeEndorserTxEnvelope(raw)
		if err != nil {
			return nil, fmt.Errorf("endorser tx %d: %w", i, err)
		}
		tx, err := safedecode.DijkstraBlockTransaction(txCbor)
		if err != nil {
			return nil, fmt.Errorf("decode endorser tx %d: %w", i, err)
		}
		if _, dup := seen[string(tx.Hash().Bytes())]; dup {
			continue
		}
		seen[string(tx.Hash().Bytes())] = struct{}{}
		if _, older := applied[string(tx.Hash().Bytes())]; older {
			continue
		}
		txs = append(txs, endorserTransaction{tx: tx, index: i})
	}
	if len(txs) == 0 {
		return nil, nil
	}
	hashes := make([][]byte, len(txs))
	for i, ebTx := range txs {
		hashes[i] = ebTx.tx.Hash().Bytes()
	}
	recorded, err := ls.db.GetTransactionsByHashes(ctx, hashes, nil)
	if err != nil {
		return nil, fmt.Errorf("look up endorser transactions: %w", err)
	}
	elsewhere := make(map[string]struct{}, len(recorded))
	for _, row := range recorded {
		if !bytes.Equal(row.BlockHash, blockHash) {
			elsewhere[string(row.Hash)] = struct{}{}
		}
	}
	return slices.DeleteFunc(txs, func(ebTx endorserTransaction) bool {
		_, skip := elsewhere[string(ebTx.tx.Hash().Bytes())]
		return skip
	}), nil
}

// leiosEndorserBlockForRollback is leiosEndorserBlockForApply for a block
// being rolled back. The parent that announced the endorser block may be in
// the same rollback, and a replayed rollback runs after the chain dropped it
// from the store, so the rolled-back blocks are consulted first. Only an entry
// at an earlier slot can be the parent.
func (ls *LedgerState) leiosEndorserBlockForRollback(
	ctx context.Context,
	blk gledger.Block,
	rolledBack map[string]models.Block,
) (hash lcommon.Blake2b256, expectedSlot uint64, announced bool, err error) {
	certifier, ok := blk.Header().(leiosEndorserBlockCertifier)
	if !ok {
		return lcommon.Blake2b256{}, 0, false, nil
	}
	certified, present := certifier.LeiosCertified()
	if !present || !certified {
		return lcommon.Blake2b256{}, 0, false, nil
	}
	prevHash := blk.PrevHash().Bytes()
	parent, ok := rolledBack[string(prevHash)]
	if !ok || parent.Slot >= blk.SlotNumber() {
		return ls.leiosCertifiedAnnouncementFromParent(ctx, prevHash)
	}
	if err := leiosStoredBlockDecodes(parent.Cbor); err != nil {
		return lcommon.Blake2b256{}, 0, false, fmt.Errorf(
			"resolve certifying block parent: decode rolled-back block: %w",
			err,
		)
	}
	hash, announced = leiosAnnouncementFromBlockCbor(parent.Cbor)
	return hash, parent.Slot, announced, nil
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
		ls.publishUtxoEventForTransaction(ctx, evt)
	}
}

// publishUtxoEventForTransaction publishes the UTxO change of a forward
// transaction event. An output that cannot be encoded is logged and the event
// is not published, rather than announcing an output with empty CBOR.
func (ls *LedgerState) publishUtxoEventForTransaction(
	ctx context.Context,
	evt TransactionEvent,
) {
	utxoEvt, err := utxoEventForTransaction(evt)
	if err != nil {
		ls.config.Logger.Error(
			"failed to build ledger.utxo event",
			"component", "ledger",
			"error", err,
			"slot", evt.Point.Slot,
			"tx_index", evt.TxIndex,
		)
		return
	}
	ls.publishUtxoEvent(ctx, utxoEvt)
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
	if resolved.Slot > durableTip.Point.Slot {
		return false, nil
	}
	if !ls.hasRollbackUndoSubscribers() {
		return false, nil
	}
	blocks, err := ls.readAppliedBlocksBetween(
		ctx,
		durableTip.Point,
		resolved,
	)
	if err != nil {
		return false, fmt.Errorf("read rollback undo blocks: %w", err)
	}
	if len(blocks) == 0 {
		return false, nil
	}
	if err := ls.ensureRollbackIntent(ctx, resolved, blocks); err != nil {
		if !errors.Is(err, errRollbackIntentTooLarge) {
			return false, err
		}
		ls.config.Logger.Warn(
			"rollback undo payload exceeds durable outbox limit; continuing with live delivery",
			"component", "ledger",
			"error", err,
		)
	}
	if err := ls.emitRollbackTransactionEvents(ctx, blocks); err != nil {
		return false, err
	}
	return len(blocks) > 0, nil
}

func (ls *LedgerState) hasRollbackUndoSubscribers() bool {
	return ls.config.EventBus != nil &&
		(ls.config.EventBus.HasSubscribers(TransactionEventType) ||
			ls.config.EventBus.HasSubscribers(LedgerErrorEventType))
}

// readAppliedBlocksBetween reads the applied ancestry from one blob snapshot,
// newest first. Hash ancestry selection avoids admitting retained competing
// branches and includes a real slot-zero block when target is origin.
func (ls *LedgerState) readAppliedBlocksBetween(
	ctx context.Context,
	tip ocommon.Point,
	target ocommon.Point,
) ([]models.Block, error) {
	return ls.readAppliedBlocksBetweenIncluding(ctx, tip, target, nil)
}

func (ls *LedgerState) readAppliedBlocksBetweenIncluding(
	ctx context.Context,
	tip ocommon.Point,
	target ocommon.Point,
	captured []models.Block,
) ([]models.Block, error) {
	var candidates []models.Block
	txn := ls.ledgerDB().Begin(ctx, false)
	err := ls.ledgerDB().Run(txn, func(txn ledgerdb.Txn) error {
		stored, err := database.BlocksAtOrAfterSlotTxn(
			dbadapter.Unwrap(txn),
			target.Slot,
		)
		if err != nil {
			return err
		}
		candidates = append(stored, captured...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return blocksAppliedBetween(candidates, tip, target)
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
	if !ls.hasRollbackUndoSubscribers() {
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

var errRollbackUndoAncestry = errors.New("rollback undo blocks do not contain the applied ancestry")

// blocksAppliedBetween returns the exact applied ancestry from tip down to,
// but excluding, target. The blob store can retain competing branches at any
// slot, so slot bounds alone cannot identify blocks whose transaction effects
// consumers were previously told to apply.
func blocksAppliedBetween(
	blocks []models.Block,
	tip ocommon.Point,
	target ocommon.Point,
) ([]models.Block, error) {
	byHash := make(map[string]models.Block, len(blocks))
	for _, block := range blocks {
		if block.Slot >= target.Slot && block.Slot <= tip.Slot {
			byHash[string(block.Hash)] = block
		}
	}
	return walkAppliedBlocks(
		tip,
		target,
		func(hash []byte) (models.Block, error) {
			block, ok := byHash[string(hash)]
			if !ok {
				return models.Block{}, models.ErrBlockNotFound
			}
			return block, nil
		},
	)
}

func walkAppliedBlocks(
	tip ocommon.Point,
	target ocommon.Point,
	lookup func([]byte) (models.Block, error),
) ([]models.Block, error) {
	if pointMatches(tip, target) {
		return nil, nil
	}
	if len(tip.Hash) == 0 || target.Slot > tip.Slot {
		return nil, fmt.Errorf(
			"%w: target %d is after tip %d",
			errRollbackUndoAncestry,
			target.Slot,
			tip.Slot,
		)
	}

	ret := make([]models.Block, 0)
	seen := make(map[string]struct{})
	expectedHash := tip.Hash
	previousSlot := tip.Slot
	for {
		key := string(expectedHash)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf(
				"%w: cycle at hash %x",
				errRollbackUndoAncestry,
				expectedHash,
			)
		}
		seen[key] = struct{}{}
		block, err := lookup(expectedHash)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: resolve block %x: %w",
				errRollbackUndoAncestry,
				expectedHash,
				err,
			)
		}
		if !bytes.Equal(block.Hash, expectedHash) ||
			block.Slot > previousSlot || block.Slot < target.Slot {
			return nil, fmt.Errorf(
				"%w: invalid block %d/%x",
				errRollbackUndoAncestry,
				block.Slot,
				block.Hash,
			)
		}
		ret = append(ret, block)
		if bytes.Equal(block.PrevHash, target.Hash) {
			return ret, nil
		}
		expectedHash = block.PrevHash
		previousSlot = block.Slot
	}
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
	nonceRows, err := ls.blockNoncesInSlotRange(
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
	ls.stageUtxoSet(txn, tip, evt) //nolint:contextcheck // the set applies after commit, under the ledger lifecycle context
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
func utxoEventForTransaction(te TransactionEvent) (UtxoEvent, error) {
	return utxoEventFor(te.Point, te.TxIndex, te.Transaction)
}

// utxoEventFor is the UTxO change applying tx at point makes: its consumed
// inputs removed and its produced outputs added. For an invalid transaction
// those are its collateral and collateral return.
func utxoEventFor(
	point ocommon.Point,
	txIndex uint32,
	tx lcommon.Transaction,
) (UtxoEvent, error) {
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
		c, err := outputCbor(u.Output)
		if err != nil {
			return UtxoEvent{}, fmt.Errorf(
				"encode output %x#%d: %w", id[:], u.Id.Index(), err,
			)
		}
		evt.Added = append(evt.Added, UtxoOutput{
			Ref: models.UtxoId{
				Hash: append([]byte(nil), id[:]...),
				Idx:  u.Id.Index(),
			},
			Cbor: c,
		})
	}
	return evt, nil
}

// outputCbor returns an output's original CBOR, or encodes it when the
// output was built rather than decoded. An output that cannot be encoded is an
// error: staging it with empty CBOR would put an unreadable row in the live
// UTxO set.
func outputCbor(out lcommon.TransactionOutput) ([]byte, error) {
	if c := out.Cbor(); len(c) > 0 {
		return c, nil
	}
	c, err := cbor.Encode(out)
	if err != nil {
		return nil, fmt.Errorf("encode transaction output: %w", err)
	}
	return c, nil
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
		ls.stageUtxoSetRollback(txn, point) //nolint:contextcheck // the change is published after the commit, under the ledger lifecycle
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
