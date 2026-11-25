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

package node

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/indexer"
	dledger "github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var (
	_ indexer.BlockIndexer      = (*Backfill)(nil)
	_ indexer.IndexAborter      = (*Backfill)(nil)
	_ indexer.WithdrawalDebiter = (*Backfill)(nil)
)

// DebitWithdrawals sets whether the withdrawals IndexBlocks replays debit
// account reward balances.
func (b *Backfill) DebitWithdrawals(on bool) { b.debitWithdrawals = on }

// AbortIndexing drops the running nonce and epoch state, so the next
// IndexBlocks call rebuilds it from the stored parent.
func (b *Backfill) AbortIndexing() {
	b.indexPrimed = false
}

// IndexBlocks writes the history rows of consecutive canonical blocks inside
// txn, the per-block work of Run without its checkpointing, so the indexer
// writer can commit rows and tip together. The first block of a call must
// follow the previous call's last block, or follow the writer's tip after a
// rollback; the running nonce and epoch state are rebuilt from the stored
// parent in the second case.
func (b *Backfill) IndexBlocks(
	ctx context.Context,
	txn *database.Txn,
	blocks []models.Block,
) (retErr error) {
	b.indexedTxs = 0
	if len(blocks) == 0 {
		return nil
	}
	defer func() {
		// The writer rolls txn back on error, taking the rows with it; the
		// running nonce and epoch tracking must not keep what they covered.
		if retErr != nil {
			b.indexPrimed = false
		}
	}()
	if err := b.refreshEpochs(txn); err != nil {
		return err
	}
	if !b.indexPrimed {
		if err := b.primeIndexing(ctx, txn, blocks[0]); err != nil {
			return err
		}
	}
	b.batchTxnEstimatedBytes = 0
	if b.delegatorInactivityEnabled {
		b.witnessed = &witnessedAccounts{
			epoch: map[string]uint64{},
			refs:  map[string]models.StakeCredentialRef{},
		}
		defer func() { b.witnessed = nil }()
	}
	acc := b.db.NewBatchAccumulator()
	if tracker, ok := acc.(interface{ EnableSizeTracking() }); ok {
		tracker.EnableSizeTracking()
	}
	dec := b.decodeBlocks(ctx, blocks)
	defer dec.stop()
	for i, blk := range blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case <-dec.out[i].ready:
		case <-ctx.Done():
			return ctx.Err()
		}
		b.batchTxnEstimatedBytes += int64(len(blk.Cbor))
		err := b.indexBlock(ctx, txn, acc, blk, &dec.out[i])
		dec.release(i)
		if err != nil {
			return fmt.Errorf("indexing block at slot %d: %w", blk.Slot, err)
		}
	}
	if err := b.db.FlushBatch(acc, txn); err != nil {
		return fmt.Errorf("flushing indexed rows: %w", err)
	}
	if b.witnessed != nil {
		if err := b.renewWitnessed(ctx, txn); err != nil {
			return err
		}
	}
	b.indexBatches++
	return nil
}

// decodedBlock is a block decoded ahead of the write transaction, with the
// offsets of its transactions when it has any. ready is closed once the
// fields are set.
type decodedBlock struct {
	parsed     gledger.Block
	offsets    *database.BlockIngestionResult
	err        error
	offsetsErr error
	ready      chan struct{}
}

// blockDecoder decodes a batch's blocks ahead of the writer.
type blockDecoder struct {
	out []decodedBlock
	// slots holds one token per block claimed by a worker and not yet
	// released by the writer, bounding the decoded blocks held at once.
	slots chan struct{}
	quit  chan struct{}
	wg    sync.WaitGroup
}

// decodeBlocks decodes a batch's blocks and computes their transaction
// offsets on several goroutines, in block order, and returns at once. Both
// depend only on the block bytes, so they need not run on the write
// goroutine. The writer waits on each block's ready channel as it reaches
// it, and errors stay with their block, so failures are reported where the
// serial loop would have met them. The workers stay within decodeWindow
// blocks of the writer, which calls release for each block it is done with.
// The caller must call stop once the writer is done.
func (b *Backfill) decodeBlocks(
	ctx context.Context,
	blocks []models.Block,
) *blockDecoder {
	workers := b.decodeWorkers
	if workers <= 0 {
		workers = max(runtime.GOMAXPROCS(0)/2, 1)
	}
	workers = min(workers, 8, len(blocks))
	d := &blockDecoder{
		out:   make([]decodedBlock, len(blocks)),
		slots: make(chan struct{}, decodeWindow(workers)),
		quit:  make(chan struct{}),
	}
	for i := range d.out {
		d.out[i].ready = make(chan struct{})
	}
	var next atomic.Int64
	for range workers {
		d.wg.Go(func() {
			for {
				// Taking the token before claiming the index keeps the
				// claimed blocks a contiguous run from the writer's
				// position, so the block the writer waits on always holds a
				// token and the window cannot deadlock.
				select {
				case d.slots <- struct{}{}:
				case <-d.quit:
					return
				}
				i := int(next.Add(1) - 1)
				if i >= len(blocks) {
					return
				}
				out := &d.out[i]
				if !d.stopped() && ctx.Err() == nil {
					blk := blocks[i]
					out.parsed, out.err = models.DecodeBlockCbor(
						blk.Type, blk.Cbor,
						lcommon.VerifyConfig{SkipBodyHashValidation: true},
					)
					if out.err == nil && len(out.parsed.Transactions()) != 0 {
						out.offsets, out.offsetsErr = b.computeBlockOffsets(
							blk.Slot, blk.Hash, blk.Cbor, out.parsed,
						)
					}
				}
				close(out.ready)
			}
		})
	}
	return d
}

func (d *blockDecoder) stopped() bool {
	select {
	case <-d.quit:
		return true
	default:
		return false
	}
}

// release drops the writer's reference to block i, which it has received
// from ready, and lets the workers claim one more block.
func (d *blockDecoder) release(i int) {
	d.out[i] = decodedBlock{}
	<-d.slots
}

// stop ends the workers early and waits for them.
func (d *blockDecoder) stop() {
	close(d.quit)
	d.wg.Wait()
}

// decodeWindow is how many blocks the decode workers may hold decoded or in
// flight ahead of the writer.
func decodeWindow(workers int) int {
	return 2 * workers
}

func (b *Backfill) indexBlock(
	ctx context.Context,
	txn *database.Txn,
	acc database.BatchAccumulator,
	blk models.Block,
	decoded *decodedBlock,
) error {
	parsed, err := decoded.parsed, decoded.err
	if err == nil && parsed == nil {
		// A worker skipped this block after stop or cancellation.
		if err = ctx.Err(); err == nil {
			parsed, err = models.DecodeBlockCbor(
				blk.Type, blk.Cbor,
				lcommon.VerifyConfig{SkipBodyHashValidation: true},
			)
		}
	}
	if err != nil {
		return fmt.Errorf("parsing block: %w", err)
	}
	point := ocommon.NewPoint(blk.Slot, blk.Hash)
	// One row per block its issuer's certificate counter, as the ledger
	// records it when it applies the block. Every block has one, including
	// those without transactions.
	pool, issueNumber, hasOpCert, err := dledger.BlockOpCertCounter(parsed)
	if err != nil {
		return fmt.Errorf("reading block op-cert counter: %w", err)
	}
	if hasOpCert {
		if err := b.db.UpdatePoolOpCertSequenceBatched(
			ctx, pool, issueNumber, blk.Slot, acc, txn,
		); err != nil {
			return fmt.Errorf("recording op-cert counter: %w", err)
		}
	}
	closure, err := dledger.LoadCertifiedClosure(
		ctx, b.closureSource(), parsed,
	)
	if err != nil {
		return fmt.Errorf("loading certified closure: %w", err)
	}
	if closure != nil && len(parsed.Transactions()) != 0 {
		return errors.New(
			"certifying block also contains ranking-block transactions",
		)
	}
	epochId, eraId := b.slotToEpoch(blk.Slot)
	isNewEpoch := epochId != b.lastEpochId
	if closure != nil && isNewEpoch {
		// The ledger applies the closure of a ranking block that starts an
		// epoch to the parent's ledger, before the epoch boundary, so it sees
		// the previous epoch's parameters and is dated at the parent's slot.
		prevEra, prevPP := b.blockEraParams(
			b.lastEpochId, b.currentEraId, uint(parsed.Era().Id),
		)
		parentSlot := closure.EndorserBlockSlot
		if err := b.indexClosure(
			ctx, txn, acc, closure, point, b.lastEpochId, prevEra, prevPP,
			&parentSlot,
		); err != nil {
			return fmt.Errorf("indexing certified closure: %w", err)
		}
	}
	if isNewEpoch {
		if err := b.resolvePParamsThrough(ctx, epochId, txn); err != nil {
			return fmt.Errorf(
				"resolving protocol parameters for epoch %d: %w", epochId, err,
			)
		}
		b.processEpochBoundary(epochId, eraId)
	}
	if err := b.computeBlockNonce(
		parsed, point, eraId, isNewEpoch || b.indexChainStart, acc, txn,
	); err != nil {
		return err
	}
	b.indexChainStart = false
	if closure != nil {
		if isNewEpoch {
			return nil
		}
		txEraId, pp := b.blockEraParams(epochId, eraId, uint(parsed.Era().Id))
		if err := b.indexClosure(
			ctx, txn, acc, closure, point, epochId, txEraId, pp, nil,
		); err != nil {
			return fmt.Errorf("indexing certified closure: %w", err)
		}
		return nil
	}
	txs := parsed.Transactions()
	if len(txs) == 0 {
		return nil
	}
	txEraId, pp := b.blockEraParams(epochId, eraId, uint(parsed.Era().Id))
	offsets, err := decoded.offsets, decoded.offsetsErr
	if offsets == nil && err == nil {
		offsets, err = b.computeBlockOffsets(
			blk.Slot, blk.Hash, blk.Cbor, parsed,
		)
	}
	if err != nil {
		return fmt.Errorf("computing offsets: %w", err)
	}
	// Rows and tip commit together, so no committed row can lie past the
	// tip; consumed-input recovery is needed only for the first batch after
	// the writer adopted rows from an interrupted backfill.
	return b.processBlockTxsBatched(
		ctx, txs, point, epochId, txEraId, pp, offsets, acc, txn, nil,
		b.indexBatches > 0,
	)
}

// refreshEpochs re-reads the epoch list, which grows as the ledger crosses
// boundaries, keeping the resolved protocol parameters. It reads through txn:
// the epoch rows of a batch are copied inside it and not yet committed.
func (b *Backfill) refreshEpochs(txn *database.Txn) error {
	epochs, err := b.db.GetEpochs(txn)
	if err != nil {
		return fmt.Errorf("loading epochs: %w", err)
	}
	b.epochs = epochs
	if b.pparamsCache == nil {
		b.pparamsCache = make(map[uint64]lcommon.ProtocolParameters)
	}
	return nil
}

// primeIndexing sets the running state to the point before first: the
// beginning of the chain, or the stored parent block.
func (b *Backfill) primeIndexing(
	ctx context.Context,
	txn *database.Txn,
	first models.Block,
) error {
	b.epochIdx = 0
	b.runningNonce = nil
	b.indexBatches = 0
	var parent models.Block
	var err error
	if len(first.PrevHash) != 0 {
		parent, err = database.BlockByHash(ctx, b.db, first.PrevHash)
	}
	// A first block with no stored parent starts the chain: the generated
	// genesis block names a parent hash that was never a block.
	if len(first.PrevHash) == 0 || errors.Is(err, models.ErrBlockNotFound) {
		b.initializeFromFirstEpoch()
		b.indexChainStart = true
		// resolvePParamsThrough bootstraps the era chain from the second era
		// on; a chain whose first epoch is Shelley starts from its genesis
		// parameters here.
		if len(b.epochs) > 0 && b.epochs[0].EraId == 1 &&
			b.currentPParams == nil && b.nodeCfg != nil {
			if err := b.bootstrapEraChain(b.epochs[0].EraId); err != nil {
				return fmt.Errorf("bootstrapping genesis parameters: %w", err)
			}
			delete(b.pparamsCache, b.lastEpochId)
		}
		// The first epoch never counts as new, so its parameters, which the
		// certificate deposits of its blocks depend on, are resolved here.
		if err := b.resolvePParamsThrough(ctx, b.lastEpochId, txn); err != nil {
			return fmt.Errorf(
				"resolving first epoch protocol parameters: %w",
				err,
			)
		}
		if pp := b.getPParams(b.lastEpochId); pp != nil {
			b.currentPParams = pp
		}
		b.indexPrimed = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("loading parent of first block: %w", err)
	}
	point := ocommon.NewPoint(parent.Slot, parent.Hash)
	epochId, eraId := b.slotToEpoch(parent.Slot)
	if err := b.resolvePParamsThrough(ctx, epochId, txn); err != nil {
		return fmt.Errorf("resolving protocol parameters: %w", err)
	}
	b.recoverNonce(point)
	b.lastEpochId = epochId
	b.currentEraId = eraId
	if pp := b.getPParams(epochId); pp != nil {
		b.currentPParams = pp
	}
	b.indexPrimed = true
	return nil
}

// SeedGenesis writes the from-genesis state into txn: the genesis
// transactions and UTxOs, pools, delegations, governance, constitution and
// committee. It needs the Cardano node configuration.
func (b *Backfill) SeedGenesis(ctx context.Context, txn *database.Txn) error {
	if b.nodeCfg == nil {
		return errors.New(
			"genesis seeding needs the Cardano node configuration",
		)
	}
	return dledger.SeedIndexerGenesis(ctx, b.nodeCfg, b.db, b.logger, txn)
}

// RollbackTo removes the rows of every block above point inside txn.
func (b *Backfill) RollbackTo(
	ctx context.Context,
	txn *database.Txn,
	point ocommon.Point,
) error {
	if txn == nil {
		return errors.New("rollback requires a transaction")
	}
	b.indexPrimed = false
	// Epochs above point are gone and will be written again, so parameter
	// resolution restarts rather than skipping their new rows.
	b.resolveStarted = false
	b.resolveNext = 0
	return b.db.RollbackMetadataAfterSlot(ctx, point, txn)
}
