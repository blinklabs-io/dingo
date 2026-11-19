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

// Package indexer fills the IndexerDB from the ledger's canonical chain.
//
// One goroutine pulls canonical blocks from the IndexerDB's own tip to the
// ledger's durable tip. A ledger tip signal only wakes it: every pass starts
// from the two tips, so a missed or coalesced signal, or blocks applied before
// the writer started, cost nothing. Each batch's rows and the new tip commit
// in one SQL transaction.
package indexer

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// DefaultBatchBlocks is the default number of blocks per transaction.
const DefaultBatchBlocks = 500

// ChainSource is the writer's view of the ledger and its canonical chain.
type ChainSource interface {
	// LedgerTip returns the ledger's durable tip. The writer never indexes
	// past it.
	LedgerTip(ctx context.Context) (ochainsync.Tip, error)
	// BlocksAfter returns up to limit canonical blocks that follow the block
	// after, ascending, through throughSlot inclusive. A zero after starts at
	// the first block. Positioning by the block rather than by its number
	// keeps Byron epoch boundary blocks, which share a number with a
	// neighbour, from being skipped or repeated.
	BlocksAfter(
		ctx context.Context,
		after indexerdb.Tip,
		throughSlot uint64,
		limit int,
	) ([]models.Block, error)
	// IsCanonical reports whether the block with this hash is on the
	// canonical chain.
	IsCanonical(ctx context.Context, hash []byte) (bool, error)
	// BlockByHash returns a stored block, canonical or not.
	BlockByHash(ctx context.Context, hash []byte) (models.Block, error)
}

// BlockIndexer writes the IndexerDB rows for blocks inside the writer's
// transaction. It must not commit or roll back txn.
type BlockIndexer interface {
	IndexBlocks(
		ctx context.Context,
		txn *database.Txn,
		blocks []models.Block,
	) error
	// RollbackTo removes the rows of every block above point inside txn.
	RollbackTo(
		ctx context.Context,
		txn *database.Txn,
		point ocommon.Point,
	) error
}

// IndexAborter is an optional BlockIndexer capability: discarding the running
// state built up across IndexBlocks calls. The writer calls it whenever its
// transaction fails, since the rollback takes rows the running state covered.
type IndexAborter interface {
	AbortIndexing()
}

// RollbackFloorSetter is an optional BlockIndexer capability: the lowest slot
// a rollback may reach, the Mithril anchor recorded in the IndexerDB. The
// IndexerDB carries no trust boundary of its own, so the writer supplies it.
type RollbackFloorSetter interface {
	SetRollbackFloor(slot uint64)
}

// ErrRollbackBelowAnchor is returned for a rollback past the Mithril anchor:
// the IndexerDB's history below it was imported, not replayed from blocks the
// ledger still holds, and cannot be rebuilt.
var ErrRollbackBelowAnchor = errors.New(
	"indexer: rollback target is below the Mithril anchor",
)

// WithdrawalDebiter is an optional BlockIndexer capability: choosing whether
// the withdrawals of the blocks it indexes debit account reward balances.
// The writer enables it for every batch that replays the ledger's reward
// credits, so a balance is the credits less the withdrawals, and disables it
// for the replay up to a Mithril anchor, whose balances are set from the
// imported state afterwards.
type WithdrawalDebiter interface {
	DebitWithdrawals(on bool)
}

// Writer follows the ledger into the IndexerDB.
type Writer struct {
	store    *indexerdb.Store
	src      ChainSource
	idx      BlockIndexer
	facts    FactsSource
	genesis  func(ctx context.Context, txn *database.Txn) error
	logger   *slog.Logger
	batch    int
	wake     chan struct{}
	runMu    sync.Mutex
	progress func(indexerdb.Tip, ochainsync.Tip)
	// ancestorLost is called when a rollback is refused with
	// ErrNoRollbackAncestor.
	ancestorLost func()
	// failures is called after each Run pass with the number of
	// consecutive passes that failed with the same error, zero on success.
	failures   func(int)
	afterBatch func(indexerdb.Tip) // test seam: called after each committed batch

	// commitSync makes the IndexerDB's commits durable; nil when they are
	// durable on return.
	commitSync       func(ctx context.Context) error
	syncEveryBatches int
	// unsynced counts the batches committed since the last durability sync.
	unsynced int

	dropIndexes  func(ctx context.Context) error
	buildIndexes func(ctx context.Context) error
	// failed is a verification failure at the anchor. It is returned again
	// without redoing the work until the writer restarts, since every ledger
	// tip signal would otherwise repeat the digest of both UTxO sets.
	failed error
}

// NewWriter builds a writer. batchBlocks <= 0 selects DefaultBatchBlocks.
func NewWriter(
	store *indexerdb.Store,
	src ChainSource,
	idx BlockIndexer,
	logger *slog.Logger,
	batchBlocks int,
) *Writer {
	if batchBlocks <= 0 {
		batchBlocks = DefaultBatchBlocks
	}
	if logger == nil {
		logger = slog.Default()
	}
	w := &Writer{
		store:  store,
		src:    src,
		idx:    idx,
		logger: logger,
		batch:  batchBlocks,
		wake:   make(chan struct{}, 1),
	}
	if syncer, ok := store.Database().Metadata().(metadata.CommitSyncer); ok {
		w.commitSync = syncer.SyncCommits
	}
	return w
}

// WithFacts makes the writer copy ledger facts alongside blocks. Without it
// only block-derived rows are written.
func (w *Writer) WithFacts(f FactsSource) *Writer {
	w.facts = f
	return w
}

// WithProgress sets a function called with the IndexerDB tip and the ledger
// tip after each committed batch and whenever a pass finds the IndexerDB
// caught up, for lag reporting.
func (w *Writer) WithProgress(fn func(indexerdb.Tip, ochainsync.Tip)) *Writer {
	w.progress = fn
	return w
}

// WithRollbackAncestorLost sets a function called each time the writer stops
// with ErrNoRollbackAncestor, for alerting.
func (w *Writer) WithRollbackAncestorLost(fn func()) *Writer {
	w.ancestorLost = fn
	return w
}

// WithFailureReport sets a function called after each Run pass with the count
// of consecutive passes that failed with the same error, and zero once a pass
// succeeds. A batch the IndexerDB cannot take stalls the writer behind the
// same error indefinitely, which only a rising count shows.
func (w *Writer) WithFailureReport(fn func(int)) *Writer {
	w.failures = fn
	return w
}

// GenesisChain is an optional ChainSource capability: it reports whether the
// canonical chain starts at genesis, as opposed to a Mithril-imported state
// whose genesis outputs are already reflected in the ledger.
type GenesisChain interface {
	FromGenesis(ctx context.Context) (bool, error)
}

// WithGenesisSeed sets the function that writes the from-genesis state in the
// transaction of the first batch, when the IndexerDB is empty and the source
// chain starts at genesis.
func (w *Writer) WithGenesisSeed(
	seed func(ctx context.Context, txn *database.Txn) error,
) *Writer {
	w.genesis = seed
	return w
}

// WithDeferredIndexes sets how the replay to a Mithril anchor drops the
// IndexerDB's deferred indexes before it starts and builds them once it has
// reached the anchor. Both must be idempotent: a replay resumed after a stop
// drops again, and a build resumed after a stop builds what is missing.
func (w *Writer) WithDeferredIndexes(
	drop, build func(ctx context.Context) error,
) *Writer {
	w.dropIndexes, w.buildIndexes = drop, build
	return w
}

// Wake asks Run for another pass. Signals coalesce.
func (w *Writer) Wake() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// Run catches up, then again after each Wake, until ctx ends. A pass that
// fails is logged and retried on the next wake.
func (w *Writer) Run(ctx context.Context) error {
	var lastErr string
	var consecutive int
	for {
		err := w.CatchUp(ctx)
		switch {
		case err == nil:
			lastErr, consecutive = "", 0
		case ctx.Err() == nil:
			if msg := err.Error(); msg == lastErr {
				consecutive++
			} else {
				lastErr, consecutive = msg, 1
			}
			w.logger.Error(
				"indexer catch-up failed",
				"error", err, "consecutive_failures", consecutive,
			)
		}
		if w.failures != nil && ctx.Err() == nil {
			w.failures(consecutive)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.wake:
		}
	}
}

// errForked reports that the chain changed under a batch.
var errForked = errors.New("indexer: chain changed under batch")

// CatchUp runs one bounded pass: it moves the IndexerDB to the ledger's
// durable tip, rolling back first when its own tip left the canonical chain.
// It is also the form a one-off history replay takes.
func (w *Writer) CatchUp(ctx context.Context) error {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	if err := w.adoptLegacyCheckpoint(ctx); err != nil {
		return err
	}
	if err := w.bootstrap(ctx); err != nil {
		if errors.Is(err, ErrAnchorMismatch) {
			w.failed = err
		}
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := w.step(ctx, nil)
		if errors.Is(err, errForked) {
			continue
		}
		if err != nil {
			return err
		}
		if done {
			w.syncAndAck(ctx)
			return nil
		}
	}
}

// DurableAckBatches is how many committed batches pass between durability
// syncs while the writer is behind. A caught-up pass always syncs.
const DurableAckBatches = 64

// FactsAcker is an optional capability of a FactsSource whose ledger retains
// what the writer has not copied. FactsCommitted notes a committed batch;
// AckFacts releases the retention of every committed batch, and must be called
// only once those commits are durable in the IndexerDB.
type FactsAcker interface {
	FactsCommitted()
	AckFacts() error
}

// factsCommitted notes a committed batch and syncs once enough have passed.
// The acknowledgement lets the ledger prune the records the batch copied, and
// a commit the IndexerDB can still lose to a power failure would then be lost
// for good, so it waits for syncAndAck.
func (w *Writer) factsCommitted(ctx context.Context) {
	a, ok := w.facts.(FactsAcker)
	if !ok {
		return
	}
	a.FactsCommitted()
	w.unsynced++
	every := w.syncEveryBatches
	if every <= 0 {
		every = DurableAckBatches
	}
	if w.unsynced >= every {
		w.syncAndAck(ctx)
	}
}

// syncAndAck makes the committed batches durable and acknowledges them. A
// failure only leaves the acknowledgement pending: the ledger keeps what it
// holds a little longer and the next sync retries.
func (w *Writer) syncAndAck(ctx context.Context) {
	a, ok := w.facts.(FactsAcker)
	if !ok || ctx.Err() != nil {
		return
	}
	if w.commitSync != nil {
		if err := w.commitSync(ctx); err != nil {
			if ctx.Err() == nil {
				w.logger.Warn("make indexer commits durable", "error", err)
			}
			return
		}
	}
	w.unsynced = 0
	if err := a.AckFacts(); err != nil {
		w.logger.Warn("acknowledge reward journal", "error", err)
	}
}

// step indexes one batch. A non-nil ceiling is a replay to that block: the
// ledger tip is read for reporting only, no block past the ceiling is indexed,
// and a store with no tip is seeded with the chain's origin.
func (w *Writer) step(
	ctx context.Context,
	ceiling *indexerdb.Tip,
) (bool, error) {
	tip, err := w.store.Tip(nil)
	if err != nil {
		return false, fmt.Errorf("read indexer tip: %w", err)
	}
	if !tip.IsZero() {
		canonical, err := w.src.IsCanonical(ctx, tip.Hash)
		if err != nil {
			return false, fmt.Errorf("check indexer tip: %w", err)
		}
		if !canonical {
			return false, w.rollback(ctx, tip)
		}
	}
	ledgerTip, err := w.src.LedgerTip(ctx)
	if err != nil {
		return false, fmt.Errorf("read ledger tip: %w", err)
	}
	target := ledgerTip
	if ceiling != nil {
		target = ochainsync.Tip{
			Point:       ocommon.NewPoint(ceiling.Slot, ceiling.Hash),
			BlockNumber: ceiling.BlockNumber,
		}
	}
	if !tip.IsZero() &&
		(bytes.Equal(tip.Hash, target.Point.Hash) ||
			tip.Slot > target.Point.Slot) {
		if refresh, ok := w.facts.(FactsRefresher); ok && ceiling == nil &&
			bytes.Equal(tip.Hash, target.Point.Hash) {
			txn := w.store.Database().Transaction(ctx, true)
			if err := txn.Do(func(txn *database.Txn) error {
				return refresh.RefreshFacts(ctx, txn, tip.Slot)
			}); err != nil {
				return false, fmt.Errorf("refresh ledger facts at tip: %w", err)
			}
		}
		w.report(tip, ledgerTip)
		return true, nil
	}
	blocks, err := w.src.BlocksAfter(ctx, tip, target.Point.Slot, w.batch)
	if err != nil {
		return false, fmt.Errorf("read canonical blocks: %w", err)
	}
	if len(blocks) == 0 {
		return true, nil
	}
	// A chain switch between reading the tip and the blocks shows as a gap
	// in the links; rewind and read again rather than index across it.
	prev := tip.Hash
	for i, b := range blocks {
		if (i > 0 || !tip.IsZero()) && !bytes.Equal(b.PrevHash, prev) {
			return false, errForked
		}
		prev = b.Hash
	}
	last := blocks[len(blocks)-1]
	newTip := indexerdb.Tip{
		Slot:        last.Slot,
		BlockNumber: last.Number,
		Hash:        last.Hash,
	}
	segments, err := w.segments(ctx, tip, blocks)
	if err != nil {
		return false, err
	}
	seed := false
	if tip.IsZero() && w.genesis != nil && ceiling != nil {
		// A replay starts at the chain's origin by construction, whatever
		// state the ledger was later imported at.
		seed = true
	} else if tip.IsZero() && w.genesis != nil {
		if gc, ok := w.src.(GenesisChain); ok {
			if seed, err = gc.FromGenesis(ctx); err != nil {
				return false, fmt.Errorf("read chain origin: %w", err)
			}
		}
	}
	if d, ok := w.idx.(WithdrawalDebiter); ok {
		d.DebitWithdrawals(ceiling == nil)
	}
	txn := w.store.Database().Transaction(ctx, true)
	err = txn.Do(func(txn *database.Txn) error {
		if seed {
			if err := w.genesis(ctx, txn); err != nil {
				return fmt.Errorf("seed genesis state: %w", err)
			}
			if ceiling != nil {
				if err := w.countSeeded(ctx, txn); err != nil {
					return err
				}
			}
		}
		for _, seg := range segments {
			if w.facts != nil {
				if err := w.facts.CopyFacts(ctx, txn, seg.from, seg.through); err != nil {
					return fmt.Errorf("copy ledger facts: %w", err)
				}
			}
			if len(seg.blocks) > 0 {
				if err := w.idx.IndexBlocks(ctx, txn, seg.blocks); err != nil {
					return err
				}
				if ceiling != nil {
					if err := w.countReplayed(txn); err != nil {
						return err
					}
				}
			}
			if w.facts != nil {
				if err := w.facts.PatchFacts(ctx, txn, seg.from, seg.through); err != nil {
					return fmt.Errorf("patch ledger facts: %w", err)
				}
			}
		}
		if err := w.store.AddCheckpoint(newTip, txn); err != nil {
			return err
		}
		return w.store.SetTip(newTip, txn)
	})
	if err != nil {
		// Whatever step failed, the transaction rolled back every row the
		// indexer wrote, so the state it carried between calls is stale.
		if a, ok := w.idx.(IndexAborter); ok {
			a.AbortIndexing()
		}
		return false, fmt.Errorf("index blocks up to %d: %w", last.Number, err)
	}
	w.factsCommitted(ctx)
	w.report(newTip, ledgerTip)
	if w.afterBatch != nil {
		w.afterBatch(newTip)
	}
	// Volatile blocks can leave the chain while the batch commits; the next
	// step's canonical check catches that and rolls back.
	return false, nil
}

func (w *Writer) report(tip indexerdb.Tip, ledgerTip ochainsync.Tip) {
	if w.progress != nil {
		w.progress(tip, ledgerTip)
	}
}

// segment is a run of blocks inside one epoch, with the slot range of the
// ledger facts that belong with it.
type segment struct {
	blocks        []models.Block
	from, through uint64
}

// segments splits blocks at each epoch boundary the ledger crossed, so a
// boundary's reward credits land between the blocks before and after it
// rather than after the whole batch. Without facts the batch is one segment.
func (w *Writer) segments(
	ctx context.Context,
	tip indexerdb.Tip,
	blocks []models.Block,
) ([]segment, error) {
	from := uint64(0)
	if !tip.IsZero() {
		from = tip.Slot + 1
	}
	through := blocks[len(blocks)-1].Slot
	if w.facts == nil {
		return []segment{{blocks: blocks, from: from, through: through}}, nil
	}
	boundaries, err := w.facts.Boundaries(ctx, from, through)
	if err != nil {
		return nil, fmt.Errorf("read epoch boundaries: %w", err)
	}
	var out []segment
	start, startSlot := 0, from
	for _, b := range boundaries {
		if b <= blocks[0].Slot {
			continue
		}
		cut := start + sort.Search(len(blocks)-start, func(i int) bool {
			return blocks[start+i].Slot >= b
		})
		if cut == start || cut >= len(blocks) {
			continue
		}
		out = append(out, segment{
			blocks: blocks[start:cut], from: startSlot, through: b - 1,
		})
		start, startSlot = cut, b
	}
	return append(out, segment{
		blocks: blocks[start:], from: startSlot, through: through,
	}), nil
}

// rollback moves the IndexerDB to a block on the canonical chain that its
// tip descends from, in one transaction with the row removal. The exact
// ancestor is found by walking parent links while the abandoned blocks are
// still stored; once the ledger has dropped them, the newest checkpoint still
// on the chain stands in, which can be earlier but never wrong.
func (w *Writer) rollback(ctx context.Context, tip indexerdb.Tip) error {
	ancestor, found, err := w.ancestorByParents(ctx, tip)
	if err != nil {
		return err
	}
	if !found {
		ancestor, err = w.ancestorByCheckpoint(ctx)
		if err != nil {
			return err
		}
		if ancestor.IsZero() {
			if err := w.requireReplayableOrigin(ctx, tip); err != nil {
				return err
			}
		}
	}
	return w.moveBack(ctx, tip, ancestor)
}

// ErrNoRollbackAncestor is returned when the IndexerDB's tip left the
// canonical chain and no stored block or checkpoint is still on it, on a chain
// that does not start at genesis. Rolling back to the origin would discard
// history the ledger cannot replay, so the writer stops instead.
var ErrNoRollbackAncestor = errors.New(
	"indexer: no rollback ancestor on a chain that does not start at genesis",
)

// requireReplayableOrigin refuses a rollback to the origin when the chain
// cannot be indexed again from its first block.
func (w *Writer) requireReplayableOrigin(
	ctx context.Context,
	tip indexerdb.Tip,
) error {
	gc, ok := w.src.(GenesisChain)
	if !ok {
		return nil
	}
	fromGenesis, err := gc.FromGenesis(ctx)
	if err != nil {
		return fmt.Errorf("read chain origin: %w", err)
	}
	if fromGenesis {
		return nil
	}
	w.logger.Error(
		"indexer cannot roll back: no ancestor on the canonical chain",
		"tip_slot", tip.Slot, "tip_hash", hex.EncodeToString(tip.Hash),
	)
	if w.ancestorLost != nil {
		w.ancestorLost()
	}
	return fmt.Errorf("%w: tip slot %d", ErrNoRollbackAncestor, tip.Slot)
}

// RollbackTo removes everything the IndexerDB holds past target, a block of
// the ledger's canonical chain, in one transaction. It does nothing when the
// IndexerDB's tip is not past target. A tip that left the canonical chain is
// first rolled back to its canonical ancestor, so the rows kept at or below
// target are canonical too.
//
// It runs while the ledger still stores the blocks being abandoned, which is
// what lets the first step find the exact ancestor by parent links. It must
// not run beside Run.
func (w *Writer) RollbackTo(ctx context.Context, target indexerdb.Tip) error {
	w.runMu.Lock()
	defer w.runMu.Unlock()
	tip, err := w.store.Tip(nil)
	if err != nil {
		return fmt.Errorf("read indexer tip: %w", err)
	}
	if tip.IsZero() {
		return nil
	}
	canonical, err := w.src.IsCanonical(ctx, tip.Hash)
	if err != nil {
		return fmt.Errorf("check indexer tip: %w", err)
	}
	if !canonical {
		if err := w.rollback(ctx, tip); err != nil {
			return err
		}
		if tip, err = w.store.Tip(nil); err != nil {
			return fmt.Errorf("read indexer tip: %w", err)
		}
	}
	if tip.IsZero() || tip.Slot <= target.Slot {
		return nil
	}
	return w.moveBack(ctx, tip, target)
}

// moveBack removes the rows past ancestor and sets the tip to it, in one
// transaction.
func (w *Writer) moveBack(
	ctx context.Context,
	tip, ancestor indexerdb.Tip,
) error {
	anchor, err := w.store.Anchor(nil)
	if err != nil {
		return fmt.Errorf("read Mithril anchor: %w", err)
	}
	if !anchor.IsZero() && ancestor.Slot < anchor.Slot {
		return fmt.Errorf(
			"%w: target slot %d, anchor slot %d",
			ErrRollbackBelowAnchor, ancestor.Slot, anchor.Slot,
		)
	}
	if f, ok := w.idx.(RollbackFloorSetter); ok {
		f.SetRollbackFloor(anchor.Slot)
	}
	point := ocommon.Point{Slot: ancestor.Slot, Hash: ancestor.Hash}
	txn := w.store.Database().Transaction(ctx, true)
	err = txn.Do(func(txn *database.Txn) error {
		if err := w.idx.RollbackTo(ctx, txn, point); err != nil {
			return err
		}
		if w.facts != nil {
			if err := w.facts.RollbackFacts(ctx, txn, point); err != nil {
				return fmt.Errorf("roll back ledger facts: %w", err)
			}
		}
		if err := w.store.DropCheckpointsAbove(ancestor.BlockNumber, txn); err != nil {
			return err
		}
		return w.store.SetTip(ancestor, txn)
	})
	if err != nil {
		return fmt.Errorf(
			"roll back indexer to slot %d: %w",
			ancestor.Slot,
			err,
		)
	}
	w.logger.Info(
		"indexer rolled back",
		"from_slot", tip.Slot, "to_slot", ancestor.Slot,
	)
	return nil
}

// ancestorByParents walks parent links from tip to the first canonical
// block. found is false when a block on the way is no longer stored.
func (w *Writer) ancestorByParents(
	ctx context.Context,
	tip indexerdb.Tip,
) (ancestor indexerdb.Tip, found bool, err error) {
	cur, err := w.src.BlockByHash(ctx, tip.Hash)
	if errors.Is(err, models.ErrBlockNotFound) {
		return indexerdb.Tip{}, false, nil
	}
	if err != nil {
		return indexerdb.Tip{}, false, fmt.Errorf(
			"load indexer tip block: %w",
			err,
		)
	}
	// An empty PrevHash is the first block: roll back to nothing.
	for len(cur.PrevHash) != 0 {
		parent, err := w.src.BlockByHash(ctx, cur.PrevHash)
		if errors.Is(err, models.ErrBlockNotFound) {
			return indexerdb.Tip{}, false, nil
		}
		if err != nil {
			return indexerdb.Tip{}, false, fmt.Errorf(
				"walk back from indexer tip: %w",
				err,
			)
		}
		canonical, err := w.src.IsCanonical(ctx, parent.Hash)
		if err != nil {
			return indexerdb.Tip{}, false, fmt.Errorf("check ancestor: %w", err)
		}
		if canonical {
			return indexerdb.Tip{
				Slot: parent.Slot, BlockNumber: parent.Number, Hash: parent.Hash,
			}, true, nil
		}
		cur = parent
	}
	return indexerdb.Tip{}, true, nil
}

// ancestorByCheckpoint returns the newest recorded checkpoint on the
// canonical chain, or the zero tip when none is.
func (w *Writer) ancestorByCheckpoint(
	ctx context.Context,
) (indexerdb.Tip, error) {
	checkpoints, err := w.store.Checkpoints(nil)
	if err != nil {
		return indexerdb.Tip{}, fmt.Errorf("read indexer checkpoints: %w", err)
	}
	for _, cp := range checkpoints {
		canonical, err := w.src.IsCanonical(ctx, cp.Hash)
		if err != nil {
			return indexerdb.Tip{}, fmt.Errorf("check checkpoint: %w", err)
		}
		if canonical {
			return cp, nil
		}
	}
	return indexerdb.Tip{}, nil
}

// legacyBackfillPhase is the phase name the former history backfill used for
// its checkpoint.
const legacyBackfillPhase = "metadata"

// adoptLegacyCheckpoint seeds an empty tip from a checkpoint a previous
// history backfill left in this store, so an adopted database resumes where
// that backfill stopped instead of replaying from genesis. A checkpoint whose
// block is no longer canonical is ignored: the rows past the fork point are
// not reachable from a tip.
func (w *Writer) adoptLegacyCheckpoint(ctx context.Context) error {
	tip, err := w.store.Tip(nil)
	if err != nil {
		return fmt.Errorf("read indexer tip: %w", err)
	}
	if !tip.IsZero() {
		return nil
	}
	cp, err := w.store.Database().Metadata().GetBackfillCheckpoint(
		legacyBackfillPhase, nil,
	)
	if err != nil {
		return fmt.Errorf("read legacy backfill checkpoint: %w", err)
	}
	if cp == nil || len(cp.LastBlockHash) == 0 {
		return nil
	}
	canonical, err := w.src.IsCanonical(ctx, cp.LastBlockHash)
	if err != nil {
		return fmt.Errorf("check legacy checkpoint: %w", err)
	}
	if !canonical {
		return nil
	}
	blk, err := w.src.BlockByHash(ctx, cp.LastBlockHash)
	if err != nil {
		return fmt.Errorf("load legacy checkpoint block: %w", err)
	}
	seeded := indexerdb.Tip{
		Slot:        blk.Slot,
		BlockNumber: blk.Number,
		Hash:        blk.Hash,
	}
	txn := w.store.Database().Transaction(ctx, true)
	return txn.Do(func(txn *database.Txn) error {
		return w.store.SetTip(seeded, txn)
	})
}
