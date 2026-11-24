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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// The rollback intent is an outbox for the undo notification of a rollback
// that is being coordinated with chain and ledger-state truncation. It names
// the rolled-back blocks; the block store keeps their bodies until the intent
// clears, and the Leios store keeps the endorser transactions they applied.
const maxRollbackIntentBlocks = database.MaxRollbackIntentBlocks

var (
	errInvalidRollbackIntent  = database.ErrInvalidRollbackIntent
	errRollbackIntentConflict = errors.New("rollback intent conflict")
	errRollbackIntentTooLarge = database.ErrRollbackIntentTooLarge
)

// durableEndorserTx is one applied endorser-block transaction: its position
// in the endorser block, which its forward event carried as TxIndex, and its
// block_transaction CBOR once resolved from the endorser block.
type durableEndorserTx struct {
	Cbor  []byte
	Index uint32
}

// endorserApplied is the endorser block a ranking block certified and the
// transactions of it that the ranking block applied, in application order.
type endorserApplied struct {
	ebHash []byte
	txs    []durableEndorserTx
	ebSlot uint64
}

// endorserUndo maps a ranking block hash to the endorser-block transactions
// it applied.
type endorserUndo map[string]endorserApplied

func rollbackIntentStore(db *database.Database) bool {
	return db != nil && db.Blob() != nil
}

func restoreRollbackIntent(
	db *database.Database,
	point ocommon.Point,
	blocks []models.Block,
	endorser endorserUndo,
	pending bool,
) error {
	if !pending {
		return clearRollbackIntent(db)
	}
	return persistRollbackIntent(db, point, blocks, endorser)
}

func intentBlocks(
	blocks []models.Block,
	endorser endorserUndo,
) []database.RollbackIntentBlock {
	ret := make([]database.RollbackIntentBlock, 0, len(blocks))
	for _, block := range blocks {
		named := database.RollbackIntentBlock{
			Hash: block.Hash, PrevHash: block.PrevHash,
			ID: block.ID, Slot: block.Slot, Number: block.Number,
			Type: block.Type,
		}
		if applied, ok := endorser[string(block.Hash)]; ok {
			named.EndorserHash = applied.ebHash
			named.EndorserSlot = applied.ebSlot
			for _, tx := range applied.txs {
				last := len(named.EndorserTxs) - 1
				if last >= 0 && named.EndorserTxs[last].Start+
					named.EndorserTxs[last].Count == tx.Index {
					named.EndorserTxs[last].Count++
					continue
				}
				named.EndorserTxs = append(
					named.EndorserTxs,
					database.RollbackIntentTxRange{Start: tx.Index, Count: 1},
				)
			}
		}
		ret = append(ret, named)
	}
	return ret
}

// persistRollbackIntent records a rollback to point that removes blocks.
//
// A body the chain no longer holds and the block store has not kept is
// written first, so the record never names a body that exists nowhere. A body
// still on the chain is kept by the truncation that removes it.
func persistRollbackIntent(
	db *database.Database,
	point ocommon.Point,
	blocks []models.Block,
	endorser endorserUndo,
) error {
	if !rollbackIntentStore(db) {
		return nil
	}
	if len(blocks) > maxRollbackIntentBlocks {
		return fmt.Errorf(
			"%w: %d blocks exceeds %d",
			errRollbackIntentTooLarge,
			len(blocks),
			maxRollbackIntentBlocks,
		)
	}
	for _, block := range blocks {
		if len(block.Cbor) == 0 || len(block.Hash) == 0 {
			continue
		}
		_, held, err := db.RolledBackBlock(block.Slot, block.Hash)
		if err != nil {
			return fmt.Errorf("persist rollback intent: %w", err)
		}
		if held {
			continue
		}
		if err := db.RetainRolledBackBlock(
			block.Slot, block.Hash, block.Cbor,
		); err != nil {
			return fmt.Errorf("persist rollback intent: %w", err)
		}
	}
	if err := db.SetRollbackIntent(database.RollbackIntent{
		Point:  point,
		Blocks: intentBlocks(blocks, endorser),
	}); err != nil {
		if errors.Is(err, errRollbackIntentTooLarge) {
			return err
		}
		return fmt.Errorf("persist rollback intent: %w", err)
	}
	return nil
}

func clearRollbackIntent(db *database.Database) error {
	if !rollbackIntentStore(db) {
		return nil
	}
	return db.ClearRollbackIntent()
}

// releaseRolledBackBlocks deletes the rolled-back bodies the block store
// kept, once no intent is pending to name them. A failure only delays the
// delete to the next rollback or start.
func (ls *LedgerState) releaseRolledBackBlocks(ctx context.Context) {
	if !rollbackIntentStore(ls.db) {
		return
	}
	if _, pending, err := ls.db.GetRollbackIntent(); err != nil || pending {
		return
	}
	if _, err := ls.db.ReleaseRolledBackBlocks(ctx); err != nil {
		ls.config.Logger.Warn(
			"failed to release rolled-back blocks",
			"component", "ledger",
			"error", err,
		)
	}
}

// ReleaseRolledBackBlocks deletes the rolled-back bodies that no rollback
// intent names and no hold keeps. It runs under the rollback boundary, so it
// never overlaps a rollback that is retaining bodies.
func (ls *LedgerState) ReleaseRolledBackBlocks(ctx context.Context) {
	_ = ls.withRollbackBoundary(func() error {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()
		ls.releaseRolledBackBlocks(ctx)
		return nil
	})
}

// ensureRollbackIntent records the undo payload for a rollback to point.
//
// A nil rollbackBlocks means "resolve the payload from the chain", which is
// only correct before the bodies above point are deleted. Callers that rewind
// the primary chain first -- reconcilePrimaryChainTipWithLedgerTip's two
// branches instead pass their captured applied blocks here and retain the
// record through undo delivery. Recovery rewinds that reject a block before
// its apply event was published have no transaction undo to deliver.
//
// A pending record for a different point is completed or merged, never
// discarded: at or above the pending slot the caller recovers the record and
// retries, and below it the two sets of blocks are merged, because the older
// record's blocks are no longer on the chain to be found there.
func (ls *LedgerState) ensureRollbackIntent(
	ctx context.Context,
	point ocommon.Point,
	rollbackBlocks []models.Block,
) error {
	// The rows that say which block applied an endorser transaction may
	// already be truncated for an existing intent's blocks, so what they
	// applied comes from the intent.
	existing, existingBlocks, endorser, pending, err := loadIntentWithEndorser(
		ls.db,
	)
	if err != nil {
		return err
	}
	if pending {
		if existing.Slot != point.Slot ||
			!bytes.Equal(existing.Hash, point.Hash) {
			if point.Slot >= existing.Slot {
				return fmt.Errorf(
					"%w: intent at slot %d must complete before rollback to slot %d",
					errRollbackIntentConflict,
					existing.Slot,
					point.Slot,
				)
			}
			if rollbackBlocks == nil {
				rollbackBlocks, err = ls.readBlocksAboveSlot(ctx, point.Slot)
				if err != nil {
					return fmt.Errorf("read rollback undo blocks: %w", err)
				}
			}
			rollbackBlocks = mergeRollbackBlocks(
				existingBlocks,
				rollbackBlocks,
			)
		} else {
			return nil
		}
	}
	if rollbackBlocks == nil {
		rollbackBlocks, err = ls.readBlocksAboveSlot(ctx, point.Slot)
		if err != nil {
			return fmt.Errorf("read rollback undo blocks: %w", err)
		}
	}
	if len(rollbackBlocks) == 0 {
		return nil
	}
	endorser = ls.captureEndorserUndo(ctx, rollbackBlocks, endorser)
	return persistRollbackIntent(ls.db, point, rollbackBlocks, endorser)
}

func (ls *LedgerState) prepareRollbackIntent(
	ctx context.Context,
	point ocommon.Point,
	rollbackBlocks []models.Block,
) error {
	if err := ls.ensureRollbackIntent(ctx, point, rollbackBlocks); err != nil {
		if !errors.Is(err, errRollbackIntentConflict) {
			return err
		}
		if recoverErr := ls.recoverRollbackIntentLocked(ctx); recoverErr != nil {
			return fmt.Errorf(
				"complete previous rollback intent: %w",
				recoverErr,
			)
		}
		if retryErr := ls.ensureRollbackIntent(ctx, point, rollbackBlocks); retryErr != nil {
			return retryErr
		}
	}
	return nil
}

func mergeRollbackBlocks(
	existing []models.Block,
	additional []models.Block,
) []models.Block {
	ret := append([]models.Block(nil), existing...)
	seen := make(map[string]struct{}, len(ret))
	for _, block := range ret {
		seen[string(block.Hash)] = struct{}{}
	}
	for _, block := range additional {
		if _, ok := seen[string(block.Hash)]; ok {
			continue
		}
		seen[string(block.Hash)] = struct{}{}
		ret = append(ret, block)
	}
	return ret
}

func loadRollbackIntent(
	db *database.Database,
) (ocommon.Point, []models.Block, bool, error) {
	point, blocks, _, pending, err := loadIntentWithEndorser(db)
	return point, blocks, pending, err
}

// pendingRollbackIntent returns the pending intent's point and how many
// blocks it names, without reading their bodies.
func pendingRollbackIntent(
	db *database.Database,
) (ocommon.Point, int, bool, error) {
	if !rollbackIntentStore(db) {
		return ocommon.Point{}, 0, false, nil
	}
	intent, pending, err := db.GetRollbackIntent()
	if err != nil {
		return ocommon.Point{}, 0, false, wrapLoadIntentError(err)
	}
	return intent.Point, len(intent.Blocks), pending, nil
}

func wrapLoadIntentError(err error) error {
	if errors.Is(err, errInvalidRollbackIntent) {
		return err
	}
	return fmt.Errorf("load rollback intent: %w", err)
}

// loadIntentWithEndorser returns the pending intent, its blocks with their
// bodies read from the block store, and the endorser transactions it names
// per ranking block, without their CBOR. A block whose body the block store
// no longer holds is returned without one.
func loadIntentWithEndorser(
	db *database.Database,
) (ocommon.Point, []models.Block, endorserUndo, bool, error) {
	if !rollbackIntentStore(db) {
		return ocommon.Point{}, nil, nil, false, nil
	}
	intent, pending, err := db.GetRollbackIntent()
	if err != nil {
		return ocommon.Point{}, nil, nil, false, wrapLoadIntentError(err)
	}
	if !pending {
		return ocommon.Point{}, nil, nil, false, nil
	}
	blocks := make([]models.Block, 0, len(intent.Blocks))
	for _, named := range intent.Blocks {
		block := models.Block{
			Hash: named.Hash, PrevHash: named.PrevHash,
			ID: named.ID, Slot: named.Slot, Number: named.Number,
			Type: named.Type,
		}
		if len(named.Hash) > 0 {
			block.Cbor, _, err = db.RolledBackBlock(named.Slot, named.Hash)
			if err != nil {
				return ocommon.Point{}, nil, nil, false, fmt.Errorf(
					"load rollback intent: %w", err,
				)
			}
		}
		blocks = append(blocks, block)
	}
	return intent.Point, blocks, intentEndorserUndo(intent), true, nil
}

// intentEndorserUndo returns the endorser transactions the intent names per
// ranking block, or nil when it names none.
func intentEndorserUndo(intent database.RollbackIntent) endorserUndo {
	var ret endorserUndo
	for _, block := range intent.Blocks {
		if len(block.EndorserTxs) == 0 {
			continue
		}
		applied := endorserApplied{
			ebHash: block.EndorserHash,
			ebSlot: block.EndorserSlot,
		}
		// The record bounds every range and their sum when it is read.
		for _, r := range block.EndorserTxs {
			for i := range r.Count {
				applied.txs = append(
					applied.txs, durableEndorserTx{Index: r.Start + i},
				)
			}
		}
		if ret == nil {
			ret = make(endorserUndo)
		}
		ret[string(block.Hash)] = applied
	}
	return ret
}

// loadRollbackIntentEndorserUndo returns the endorser-block transactions the
// pending intent names per ranking block, with their CBOR read from the
// endorser block, or nil without an intent. A ranking block whose endorser
// block cannot be read is omitted.
func (ls *LedgerState) loadRollbackIntentEndorserUndo() (endorserUndo, error) {
	if !rollbackIntentStore(ls.db) {
		return nil, nil
	}
	intent, pending, err := ls.db.GetRollbackIntent()
	if err != nil || !pending {
		return nil, err
	}
	named := intentEndorserUndo(intent)
	for hash, applied := range named {
		raws, err := ls.endorserBlockTxs(applied.ebHash, applied.ebSlot)
		if err == nil {
			for i := range applied.txs {
				idx := int(applied.txs[i].Index)
				if idx >= len(raws) {
					err = fmt.Errorf(
						"endorser tx %d is beyond the block's %d",
						idx, len(raws),
					)
					break
				}
				applied.txs[i].Cbor = raws[idx]
			}
		}
		if err != nil {
			ls.config.Logger.Error(
				"failed to read endorser transactions for rollback undo",
				"component", "ledger",
				"error", err,
				"endorser_slot", applied.ebSlot,
				"endorser_hash", hex.EncodeToString(applied.ebHash),
			)
			delete(named, hash)
			continue
		}
		named[hash] = applied
	}
	return named, nil
}

// endorserBlockTxs returns an endorser block's transactions from the
// provider, and from the Leios store when the provider is not serving: a
// rollback recovered at start runs before the network stack exists.
func (ls *LedgerState) endorserBlockTxs(
	ebHash []byte,
	ebSlot uint64,
) ([]cbor.RawMessage, error) {
	if ls.config.EndorserBlockProvider != nil {
		if raws, ok := ls.config.EndorserBlockProvider(ebHash, ebSlot); ok {
			return raws, nil
		}
	}
	return ls.db.GetLeiosEBTxs(ebHash, ebSlot)
}

// recoverRollbackIntent finishes a rollback whose durable undo outbox was
// written but whose chain or metadata mutation did not complete. The block
// store still holds the bodies the outbox names, whether or not the
// primary-chain rewind already removed them from the chain.
func (ls *LedgerState) recoverRollbackIntent(ctx context.Context) error {
	// rollbackMutex before transactionEventMutex, matching
	// rollbackChainAndStateDeferred and reconcilePrimaryChainTipWithLedgerTip.
	// The in-rollback callers already hold both and reach the Locked variant
	// directly.
	return ls.withRollbackBoundary(func() error {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()
		return ls.recoverRollbackIntentLocked(ctx)
	})
}

func (ls *LedgerState) recoverRollbackIntentLocked(ctx context.Context) error {
	point, blocks, pending, err := loadRollbackIntent(ls.db)
	if errors.Is(err, errInvalidRollbackIntent) {
		ls.config.Logger.Error(
			"discarding unreadable rollback intent",
			"component", "ledger",
			"error", err,
		)
		if err := clearRollbackIntent(ls.db); err != nil {
			return err
		}
		ls.releaseRolledBackBlocks(ctx)
		return nil
	}
	if err != nil {
		return err
	}
	if !pending {
		// A stop after an intent cleared and before its bodies were deleted
		// leaves them behind with nothing naming them.
		ls.releaseRolledBackBlocks(ctx)
		return nil
	}
	ls.config.Logger.Warn(
		"recovering interrupted ledger rollback",
		"component", "ledger",
		"slot", point.Slot,
	)

	ls.RLock()
	current := ls.currentTip.Point
	ls.RUnlock()
	if current.Slot < point.Slot {
		// No metadata rollback is possible below the applied tip, but the
		// bodies the record names are kept only while it is pending.
		// Deliver the undo before retiring the record, as the
		// off-primary-chain branch below does, so the at-least-once contract
		// holds on every exit.
		ls.config.Logger.Warn(
			"replaying rollback undo whose intent leads the applied ledger tip",
			"component", "ledger", "ledger_tip_slot", current.Slot,
			"intent_slot", point.Slot,
		)
		if err := ls.emitRollbackTransactionEvents(ctx, blocks); err != nil {
			return err
		}
		return ls.finishRollbackIntent(ctx)
	}
	if point.Slot > 0 || len(point.Hash) > 0 {
		contains, err := ls.primaryChainContainsPoint(ctx, point)
		if err != nil {
			return fmt.Errorf("validate rollback intent point: %w", err)
		}
		if !contains {
			ls.config.Logger.Warn(
				"replaying rollback undo whose point left the primary chain",
				"component", "ledger",
				"intent_slot", point.Slot,
			)
			if err := ls.emitRollbackTransactionEvents(ctx, blocks); err != nil {
				return err
			}
			return ls.finishRollbackIntent(ctx)
		}
	}
	if ls.config.ChainManager != nil {
		if err := ls.config.ChainManager.RewindPrimaryChainToPoint(ctx, point); err != nil {
			return fmt.Errorf("recover primary chain rollback: %w", err)
		}
	}

	if err := ls.emitRollbackTransactionEvents(ctx, blocks); err != nil {
		return err
	}
	if err := ls.rollbackWithBlocks(ctx, point, blocks, false); err != nil {
		return fmt.Errorf("recover rollback intent: %w", err)
	}
	return nil
}

// tipRevertPendingAt reports whether the block store marks a metadata revert
// to point as unconfirmed.
func (ls *LedgerState) tipRevertPendingAt(point ocommon.Point) (bool, error) {
	if ls.db == nil {
		return false, nil
	}
	pending, found, err := ls.db.TipRevertPending()
	if err != nil || !found {
		return false, err
	}
	return pointMatches(pending.Point, point), nil
}

// repeatInterruptedTipRevert runs the metadata revert again when a stop came
// between a rollback lowering the stored tip and its metadata commit. The tip
// then already names the target, so nothing else would notice that the rows
// of the abandoned blocks are still there: with no undo subscriber a rollback
// writes no intent. A mark that names another point than the stored tip
// describes a revert the ledger has since moved past, and is removed.
func (ls *LedgerState) repeatInterruptedTipRevert(ctx context.Context) error {
	if ls.db == nil {
		return nil
	}
	pending, found, err := ls.db.TipRevertPending()
	if err != nil || !found {
		return err
	}
	return ls.withRollbackBoundary(func() error {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()
		durable, err := ls.ledgerDB().GetTip(nil)
		if err != nil {
			return fmt.Errorf("read durable ledger tip: %w", err)
		}
		ls.RLock()
		current := ls.currentTip.Point
		ls.RUnlock()
		if !pointMatches(pending.Point, durable.Point) ||
			!pointMatches(current, durable.Point) {
			return ls.db.ClearTipRevertPending()
		}
		ls.config.Logger.Warn(
			"repeating interrupted ledger revert",
			"component", "ledger",
			"slot", durable.Point.Slot,
		)
		return ls.rollbackWithBlocksAndIntent(
			ctx, durable.Point, nil, true, false, false,
		)
	})
}

func (ls *LedgerState) finishRollbackIntent(ctx context.Context) error {
	_, blocks, pending, err := pendingRollbackIntent(ls.db)
	if err != nil {
		return err
	}
	if !pending {
		ls.releaseRolledBackBlocks(ctx)
		return nil
	}
	if blocks > 0 && ls.config.EventBus != nil {
		publishCtx := ls.publishCtx //nolint:contextcheck // the ledger lifecycle context bounds the flush
		if publishCtx == nil {
			publishCtx = ctx
		}
		if !ls.config.EventBus.FlushOrderedContext(
			publishCtx,
			TransactionEventType,
		) || !ls.config.EventBus.FlushOrderedContext(
			publishCtx,
			UtxoEventType,
		) {
			ls.config.Logger.Warn(
				"retaining rollback intent until ordered undo delivery completes",
				"component",
				"ledger",
			)
			return nil
		}
	}
	if err := clearRollbackIntent(ls.db); err != nil {
		return err
	}
	// Deleting the bodies comes after the clear: until then a restart still
	// replays the undo from them.
	ls.releaseRolledBackBlocks(ctx)
	return nil
}

func (ls *LedgerState) finishRollbackIntentForPoint(
	ctx context.Context,
	point ocommon.Point,
) error {
	pendingPoint, _, pending, err := pendingRollbackIntent(ls.db)
	if err != nil {
		return err
	}
	if !pending {
		ls.releaseRolledBackBlocks(ctx)
		return nil
	}
	if !pointMatches(pendingPoint, point) {
		return nil
	}
	return ls.finishRollbackIntent(ctx)
}
