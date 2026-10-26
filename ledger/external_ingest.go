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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

// ErrStaleBatch reports that the ledger tip is no longer the point a trusted
// batch was prepared against. The caller discards the batch and prepares it
// again from the current tip.
var ErrStaleBatch = errors.New("ledger tip moved since batch was prepared")

// ErrChainIngestNotPaused reports an ApplyTrustedBatch call made outside a
// PauseChainIngest/resume pair.
var ErrChainIngestNotPaused = errors.New(
	"trusted batch applied while chain ingest is not paused",
)

// ErrTrustedBatchTooLarge reports a batch longer than MaxTrustedBatchBlocks.
var ErrTrustedBatchTooLarge = errors.New("trusted batch exceeds block limit")

// ErrTrustedBatchBelowMithrilBoundary reports a batch that reaches at or below
// the Mithril trust boundary (mithril_ledger_slot). The apply loop treats
// such blocks as already reflected in the imported ledger state and skips
// them, so applying them here would silently do nothing.
var ErrTrustedBatchBelowMithrilBoundary = errors.New(
	"trusted batch at or below the Mithril trust boundary",
)

// MaxTrustedBatchBlocks is the most blocks one ApplyTrustedBatch call
// accepts. The whole batch is one coordinated transaction, and its blob half
// (a transaction-offset key per transaction and a UTxO-offset key per output)
// is a single Badger transaction, which Badger refuses once it exceeds 15% of
// the memtable (about 20 MB and 200,000 entries at the default 128 MB). At a
// few hundred keys for a busy mainnet block, eight live-loop transactions'
// worth of blocks is expected to stay inside that, but block density is not
// bounded by it, so ApplyTrustedBatch splits a batch Badger refuses rather
// than relying on this bound alone. A mainnet immutable chunk holds about
// 1,100 blocks, so callers split a chunk into several batches.
const MaxTrustedBatchBlocks = 8 * batchSize

// externalIngestState lets an external source replace the chain-iterator loop
// as the ledger's block-apply writer for a while.
type externalIngestState struct {
	// gate holds one token while either the loop is applying a read result
	// or chain ingest is paused. It is created lazily so a LedgerState built
	// without NewLedgerState still has a usable gate.
	gateOnce sync.Once
	gate     chan struct{}
	// pauseMu guards pauses, the number of outstanding PauseChainIngest
	// calls. The first takes the gate and the last returns it.
	pauseMu sync.Mutex
	pauses  int
	// applyMu serializes ApplyTrustedBatch callers.
	applyMu sync.Mutex
	// gen advances whenever an external ingest moves the ledger tip. The
	// loop compares it against the value it loaded before its reader took
	// a tip, to discard a read positioned at a tip that no longer exists.
	gen atomic.Uint64
	// trusted is set for the duration of an ApplyTrustedBatch call so the
	// apply helpers outside processBlocksFromSource treat it as
	// TrustedReplay.
	trusted atomic.Bool
	// spends is the running call's intra-batch spend tracker, which names
	// the outputs earlier block-apply transactions of the call committed.
	spends atomic.Pointer[intraBatchSpends]
	// overlayOff keeps the rest of a call on the per-row UTxO path after
	// the batch UTxO overlay hit a write conflict.
	overlayOff atomic.Bool
	// overlayApplied and perRowApplied record how the current block-apply
	// transaction's delta batches applied, for its beforeCommit.
	overlayApplied atomic.Bool
	perRowApplied  atomic.Bool
}

func (e *externalIngestState) recordApply(overlay bool) {
	if overlay {
		e.overlayApplied.Store(true)
	} else {
		e.perRowApplied.Store(true)
	}
}

// takeOverlayOnly reports whether every delta batch of the transaction
// being committed applied through the overlay, and clears the record. A
// stale record left by a failed transaction errs toward the per-row delete,
// which is correct after either path.
func (e *externalIngestState) takeOverlayOnly() bool {
	overlay := e.overlayApplied.Swap(false)
	perRow := e.perRowApplied.Swap(false)
	return overlay && !perRow
}

func (ls *LedgerState) chainIngestGate() chan struct{} {
	ls.externalIngest.gateOnce.Do(func() {
		ls.externalIngest.gate = make(chan struct{}, 1)
	})
	return ls.externalIngest.gate
}

func (ls *LedgerState) acquireChainIngestGate(ctx context.Context) error {
	gate := ls.chainIngestGate()
	select {
	case gate <- struct{}{}:
		return nil
	default:
	}
	if ls.chainIngestGateWait != nil {
		ls.chainIngestGateWait()
	}
	select {
	case gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ls *LedgerState) releaseChainIngestGate() {
	<-ls.chainIngestGate()
}

// trustedReplayActive reports whether blocks are being applied for their
// effects only: either the ledger was built for trusted replay, or an
// ApplyTrustedBatch call is in progress.
func (ls *LedgerState) trustedReplayActive() bool {
	return ls.config.TrustedReplay || ls.externalIngest.trusted.Load()
}

// PauseChainIngest stops the ledger's chain-iterator loop from applying blocks
// until resume is called. It waits for the loop to finish the read result it
// is applying, so on return no block-apply transaction of the loop is in
// flight. Nested pauses are counted: the loop resumes when every resume has
// been called, and calling one resume more than once has no further effect.
//
// While paused the loop may still read the chain ahead of the ledger tip, but
// it applies nothing. A read taken before ApplyTrustedBatch moved the tip is
// discarded on resume and the loop restarts from the new tip.
//
// It must not be called from the chain-iterator loop's goroutine or from a
// ledger event handler the loop waits on: the call waits for the loop to
// finish its current read result, so it would wait on itself.
func (ls *LedgerState) PauseChainIngest() (resume func()) {
	e := &ls.externalIngest
	e.pauseMu.Lock()
	if e.pauses == 0 {
		ls.chainIngestGate() <- struct{}{}
	}
	e.pauses++
	e.pauseMu.Unlock()
	return sync.OnceFunc(ls.resumeChainIngest)
}

func (ls *LedgerState) resumeChainIngest() {
	e := &ls.externalIngest
	e.pauseMu.Lock()
	defer e.pauseMu.Unlock()
	e.pauses--
	if e.pauses == 0 {
		ls.releaseChainIngestGate()
	}
}

// ApplyTrustedBatch applies blocks, which must extend expected, as trusted
// history: their effects are applied as under TrustedReplay, with no rule or
// script validation. The caller vouches for the blocks, for example because
// they lie at or below a certified immutable tip. It returns the new ledger
// tip.
//
// It must be called between PauseChainIngest and its resume, which makes it
// the ledger's only block-apply writer. If the ledger tip is not expected it
// returns ErrStaleBatch and changes nothing. A tip that moves after that check
// -- a rollback while the batch applies, or a block appended to the primary
// chain while paused -- also surfaces as ErrStaleBatch, with no metadata
// committed. A batch longer than MaxTrustedBatchBlocks, or one reaching at or
// below the Mithril trust boundary, is refused before anything is stored.
//
// The blocks are stored in the primary chain first, in blob-only
// transactions, because UTxO and transaction rows hold offsets into the
// stored block CBOR. Their effects are then committed in one metadata
// transaction, in which an output both created and spent by the batch is
// removed before commit. An epoch boundary inside the batch is the exception:
// the blocks before it, the epoch rollover and the blocks after it commit in
// three transactions, as they do on every other apply path.
//
// A batch whose commit Badger refuses as too big for one transaction is
// applied again as two halves, each its own commit with the same tip guard,
// recursively down to a single block, so a dense range is applied in smaller
// commits instead of failing on every attempt. Nothing is skipped: a single
// block Badger still refuses fails the call.
//
// After a batch commits, spent UTxO rows up to its tip are hard-deleted with
// the routine consumed-UTxO cleanup (see pruneConsumedUtxosAfterTrustedBatch).
// API storage mode keeps them.
//
// A failure after the blocks are stored leaves the ledger tip where it was.
// Calling again with the same expected point and blocks reuses the stored
// blocks; nothing else needs undoing.
func (ls *LedgerState) ApplyTrustedBatch(
	ctx context.Context,
	expected ocommon.Point,
	blocks []ledger.Block,
) (ocommon.Point, error) {
	e := &ls.externalIngest
	// Holding a pause of its own keeps the gate held for the whole call,
	// even if the caller's resume races it.
	e.pauseMu.Lock()
	paused := e.pauses > 0
	if paused {
		e.pauses++
	}
	e.pauseMu.Unlock()
	if !paused {
		return ls.ledgerTipPoint(), ErrChainIngestNotPaused
	}
	defer ls.resumeChainIngest()
	e.applyMu.Lock()
	defer e.applyMu.Unlock()

	before := ls.ledgerTipPoint()
	if !pointMatches(before, expected) {
		return before, ErrStaleBatch
	}
	if len(blocks) == 0 {
		return before, nil
	}
	if len(blocks) > MaxTrustedBatchBlocks {
		return before, fmt.Errorf(
			"%w: %d blocks, limit %d",
			ErrTrustedBatchTooLarge,
			len(blocks),
			MaxTrustedBatchBlocks,
		)
	}
	ls.RLock()
	mithrilSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	// Blocks ascend, so the first is the lowest.
	if mithrilSlot > 0 && blocks[0].SlotNumber() <= mithrilSlot {
		return before, fmt.Errorf(
			"%w: block at slot %d, boundary slot %d",
			ErrTrustedBatchBelowMithrilBoundary,
			blocks[0].SlotNumber(),
			mithrilSlot,
		)
	}
	if ls.afterTrustedBatchGuard != nil {
		ls.afterTrustedBatchGuard()
	}
	defer func() {
		if !pointMatches(ls.ledgerTipPoint(), before) {
			e.gen.Add(1)
		}
	}()
	tip, err := ls.applyTrustedSplitting(ctx, expected, blocks)
	if err == nil {
		ls.pruneConsumedUtxosAfterTrustedBatch(ctx)
	}
	return tip, err
}

// applyTrustedSplitting applies blocks, which extend expected, and on a
// commit Badger refuses as too big applies whatever did not commit in
// halves. An epoch boundary commits the blocks before it on their own, so
// part of the batch may have committed when the refusal comes; the ledger
// tip says how much.
func (ls *LedgerState) applyTrustedSplitting(
	ctx context.Context,
	expected ocommon.Point,
	blocks []ledger.Block,
) (ocommon.Point, error) {
	tip, err := ls.applyTrustedBlocks(ctx, expected, blocks)
	if err == nil || !isTxnTooBig(err) || len(blocks) < 2 {
		return tip, err
	}
	done := 0
	if !pointMatches(tip, expected) {
		done = -1
		for i, b := range blocks {
			if pointMatches(
				tip, ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes()),
			) {
				done = i + 1
				break
			}
		}
		if done < 0 {
			return tip, err
		}
	}
	rest := blocks[done:]
	if ls.trustedBatchSplit != nil {
		ls.trustedBatchSplit(len(rest))
	}
	if len(rest) == 1 {
		return ls.applyTrustedSplitting(ctx, tip, rest)
	}
	mid := len(rest) / 2
	tip, err = ls.applyTrustedSplitting(ctx, tip, rest[:mid])
	if err != nil {
		return tip, err
	}
	return ls.applyTrustedSplitting(ctx, tip, rest[mid:])
}

// isTxnTooBig reports whether err is Badger refusing a transaction for its
// size. Some storage layers format the cause with %v rather than wrap it, so
// the message is matched as well.
func isTxnTooBig(err error) bool {
	return errors.Is(err, badger.ErrTxnTooBig) ||
		strings.Contains(err.Error(), badger.ErrTxnTooBig.Error())
}

// applyTrustedBlocks stores blocks and commits their effects in one metadata
// transaction (three across an epoch boundary).
func (ls *LedgerState) applyTrustedBlocks(
	ctx context.Context,
	expected ocommon.Point,
	blocks []ledger.Block,
) (ocommon.Point, error) {
	e := &ls.externalIngest
	if err := ls.storeTrustedBatchBlocks(ctx, expected, blocks); err != nil {
		return ls.ledgerTipPoint(), fmt.Errorf(
			"store trusted batch blocks: %w", err,
		)
	}

	e.trusted.Store(true)
	defer e.trusted.Store(false)
	// API storage mode keeps spent rows for historical queries, so only the
	// other modes drop the outputs a batch creates and spends.
	spends := newIntraBatchSpends(
		ls.db.StorageMode() != types.StorageModeAPI,
	)
	e.spends.Store(spends)
	defer e.spends.Store(nil)
	defer e.overlayOff.Store(false)
	e.overlayApplied.Store(false)
	e.perRowApplied.Store(false)
	err := ls.applyTrustedSource(ctx, blocks, spends)
	tip := ls.ledgerTipPoint()
	if err != nil && errors.Is(err, types.ErrBatchUtxoOverlayConflict) {
		// Nothing of the failed transaction committed; earlier epoch
		// sides may have. The conflict fires before beforeCommit, so
		// spends still describes exactly what committed.
		rest := blocksAfter(blocks, tip, expected)
		ls.config.Logger.Warn(
			"trusted batch UTxO overlay hit a write conflict; "+
				"applying the batch on the per-row path",
			"error", err,
			"blocks", len(rest),
			"tip_slot", tip.Slot,
			"component", "ledger",
		)
		ls.metrics.incTrustedBatchOverlayFallbacks()
		e.overlayOff.Store(true)
		err = nil
		if len(rest) > 0 {
			err = ls.applyTrustedSource(ctx, rest, spends)
			tip = ls.ledgerTipPoint()
		}
	}
	if err != nil {
		if errors.Is(err, errStaleChainIterator) {
			return tip, fmt.Errorf("%w: %w", ErrStaleBatch, err)
		}
		return tip, err
	}
	last := blocks[len(blocks)-1]
	if !pointMatches(
		tip,
		ocommon.NewPoint(last.SlotNumber(), last.Hash().Bytes()),
	) {
		return tip, fmt.Errorf(
			"trusted batch stopped at slot %d before its last block at slot %d",
			tip.Slot,
			last.SlotNumber(),
		)
	}
	return tip, nil
}

// applyTrustedSource commits blocks' effects through processBlocksFromSource.
func (ls *LedgerState) applyTrustedSource(
	ctx context.Context,
	blocks []ledger.Block,
	spends *intraBatchSpends,
) error {
	source := make(chan readChainResult, 1)
	source <- readChainResult{blocks: blocks}
	close(source)
	return ls.processBlocksFromSource(ctx, source, blockSourceOptions{
		trusted:   true,
		chunkSize: len(blocks),
		beforeCommit: func(txn *database.Txn, applied []ledger.Block) error {
			if err := spends.removeFromTxn(
				ls.db, txn, applied,
				ls.externalIngest.takeOverlayOnly(),
			); err != nil {
				return fmt.Errorf("drop outputs spent within batch: %w", err)
			}
			if ls.trustedBatchBeforeCommit != nil {
				return ls.trustedBatchBeforeCommit(applied)
			}
			return nil
		},
	})
}

// blocksAfter returns the blocks that follow tip, all of them when tip is
// expected, or nil when tip is none of them.
func blocksAfter(
	blocks []ledger.Block,
	tip, expected ocommon.Point,
) []ledger.Block {
	if pointMatches(tip, expected) {
		return blocks
	}
	for i, b := range blocks {
		if pointMatches(
			tip, ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes()),
		) {
			return blocks[i+1:]
		}
	}
	return nil
}

func (ls *LedgerState) ledgerTipPoint() ocommon.Point {
	ls.RLock()
	defer ls.RUnlock()
	return cloneTip(ls.currentTip).Point
}

// storeTrustedBatchBlocks adds the batch to the primary chain. Blocks the
// chain already holds -- stored by an earlier attempt whose metadata commit
// failed, or fetched from a peer before ingest paused -- are kept rather than
// stored again.
func (ls *LedgerState) storeTrustedBatchBlocks(
	ctx context.Context,
	expected ocommon.Point,
	blocks []ledger.Block,
) error {
	held := 0
	for held < len(blocks) {
		blk := blocks[held]
		if !ls.chain.HoldsPoint(
			ctx,
			ocommon.NewPoint(blk.SlotNumber(), blk.Hash().Bytes()),
		) {
			break
		}
		held++
	}
	if held == len(blocks) {
		return nil
	}
	anchor := expected
	if held > 0 {
		prev := blocks[held-1]
		anchor = ocommon.NewPoint(prev.SlotNumber(), prev.Hash().Bytes())
	}
	// A chain that holds other blocks past the anchor moved under the
	// batch, and resolving that fork is chain selection's job, so the caller
	// replays from whatever tip results.
	if tip := ls.chain.Tip(); !pointMatches(tip.Point, anchor) {
		return fmt.Errorf(
			"%w: primary chain tip at slot %d does not join the batch at slot %d",
			ErrStaleBatch,
			tip.Point.Slot,
			anchor.Slot,
		)
	}
	return ls.chain.AddBlocks(ctx, blocks[held:])
}

// intraBatchSpends tracks the outputs a trusted batch has produced so far, so
// the transaction that applies a spend of one can remove its row before
// commit.
type intraBatchSpends struct {
	enabled  bool
	produced map[utxoref.Key]struct{}
}

func newIntraBatchSpends(enabled bool) *intraBatchSpends {
	return &intraBatchSpends{
		enabled:  enabled,
		produced: make(map[utxoref.Key]struct{}),
	}
}

// committedAmong returns the inputs that name outputs an earlier
// block-apply transaction of the batch committed and nothing has spent yet.
func (s *intraBatchSpends) committedAmong(
	inputs []models.UtxoId,
) []models.UtxoId {
	if !s.enabled || len(s.produced) == 0 {
		return nil
	}
	var ret []models.UtxoId
	for _, input := range inputs {
		key := utxoref.Key{Index: input.Idx}
		copy(key.TxId[:], input.Hash)
		if _, ok := s.produced[key]; ok {
			ret = append(ret, input)
		}
	}
	return ret
}

// removeFromTxn removes, inside txn, the rows of outputs produced earlier in
// the batch and spent by the applied blocks. A transaction's inputs are taken
// before its outputs, matching the order the ledger applies them. When the
// transaction applied through a batch UTxO overlay, overlay is set: the
// overlay already deleted or never wrote those rows, so only their blob keys
// and the prune floor are left.
func (s *intraBatchSpends) removeFromTxn(
	db *database.Database,
	txn *database.Txn,
	applied []ledger.Block,
	overlay bool,
) error {
	if !s.enabled || len(applied) == 0 {
		return nil
	}
	var spent []models.UtxoId
	for _, blk := range applied {
		for _, tx := range blk.Transactions() {
			for _, input := range tx.Consumed() {
				key := utxoref.ForInput(input)
				if _, ok := s.produced[key]; !ok {
					continue
				}
				delete(s.produced, key)
				spent = append(spent, models.UtxoId{
					Hash: input.Id().Bytes(),
					Idx:  input.Index(),
				})
			}
			for _, utxo := range tx.Produced() {
				s.produced[utxoref.ForUtxo(utxo)] = struct{}{}
			}
		}
	}
	slot := applied[len(applied)-1].SlotNumber()
	if overlay {
		return db.UtxosDropSpentInBatchBlobs(spent, slot, txn)
	}
	return db.UtxosDeleteSpentInBatch(spent, slot, txn)
}
