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
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// The transaction ID omits witnesses, so the full transaction distinguishes
// preverification results for identical bodies with different signature sets.
type preverifiedSignatureSet map[lcommon.Blake2b256]*lcommon.PreverifiedSignatures

func preverifyTransactionSignatures(
	transactions []lcommon.Transaction,
) preverifiedSignatureSet {
	if len(transactions) == 0 {
		return nil
	}
	results := make([]*lcommon.PreverifiedSignatures, len(transactions))
	workers := min(runtime.GOMAXPROCS(0), len(transactions))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for idx := range jobs {
				results[idx] = lcommon.PreverifySignatures(transactions[idx])
			}
		})
	}
	for idx := range transactions {
		jobs <- idx
	}
	close(jobs)
	wg.Wait()

	ret := make(preverifiedSignatureSet, len(transactions))
	for idx, transaction := range transactions {
		ret[transaction.LeiosHash()] = results[idx]
	}
	return ret
}

type preverifiedSignaturesContextKey struct{}

func withPreverifiedSignatures(
	ctx context.Context,
	signatures preverifiedSignatureSet,
) context.Context {
	if len(signatures) == 0 {
		return ctx
	}
	return context.WithValue(ctx, preverifiedSignaturesContextKey{}, signatures)
}

func preverifiedSignaturesFrom(
	ctx context.Context,
	transaction lcommon.Transaction,
) *lcommon.PreverifiedSignatures {
	signatures, _ := ctx.Value(preverifiedSignaturesContextKey{}).(preverifiedSignatureSet)
	return signatures[transaction.LeiosHash()]
}

type blockPreverifiedSignaturesContextKey struct{}

// withBlockPreverifiedSignatures attaches the signature results
// signaturePreverifyAhead computed for the block about to be applied, indexed
// by transaction position in the block.
func withBlockPreverifiedSignatures(
	ctx context.Context,
	signatures []*lcommon.PreverifiedSignatures,
) context.Context {
	if len(signatures) == 0 {
		return ctx
	}
	return context.WithValue(
		ctx,
		blockPreverifiedSignaturesContextKey{},
		signatures,
	)
}

// preverifiedSignaturesForTx returns the preverified signature result for the
// transaction at position index of the block being applied: the block's
// positional result when signaturePreverifyAhead supplied one, otherwise a
// trusted-replay result keyed by the full transaction hash, otherwise nil.
// ahead reports that the result came from signaturePreverifyAhead. A result
// that belongs to another transaction is harmless: gouroboros checks its
// binding to the body and witness set and verifies inline on a mismatch.
func preverifiedSignaturesForTx(
	ctx context.Context,
	index int,
	transaction lcommon.Transaction,
) (signatures *lcommon.PreverifiedSignatures, ahead bool) {
	if positional, ok := ctx.Value(
		blockPreverifiedSignaturesContextKey{},
	).([]*lcommon.PreverifiedSignatures); ok {
		if index >= 0 && index < len(positional) &&
			positional[index] != nil {
			return positional[index], true
		}
		return nil, false
	}
	return preverifiedSignaturesFrom(ctx, transaction), false
}

// signaturePreverifyAhead verifies the vkey and bootstrap witness signatures of
// a block-apply chunk's validated transactions on worker goroutines, so the
// Ed25519 work of block k+1 and later runs while block k validates and writes
// in the chunk's database transaction.
//
// Signature verification reads no ledger state, so a result computed ahead is
// the result inline verification would produce. Each result is bound to its
// transaction body and witness set, so it can only stand in for the
// cryptographic check of the transaction it was computed from; witness
// presence and every other rule still run against the write transaction.
// Results live in memory and are discarded with the chunk, so a failed or
// rolled-back chunk leaves nothing behind.
type signaturePreverifyAhead struct {
	ctx    context.Context
	cancel context.CancelFunc
	wanted []bool
	// results[k][i] is block k's transaction i. Row k is written by the
	// feeder goroutine and its entries by the workers, and both happen
	// before ready[k] is closed.
	results [][]*lcommon.PreverifiedSignatures
	// ready[k] is closed once every result of block k is stored.
	ready []chan struct{}
	done  chan struct{}
	once  sync.Once
}

type signaturePreverifyJob struct {
	tx        lcommon.Transaction
	result    **lcommon.PreverifiedSignatures
	remaining *atomic.Int32
	ready     chan struct{}
}

// startSignaturePreverifyAhead launches the workers for a chunk. wanted[k]
// reports whether block k is validated. A feeder goroutine extracts each
// wanted block's transactions and queues them in block order, so the apply
// loop pays for neither the extraction nor the verification, and earlier
// blocks finish first. The caller must call stop on every exit path.
func startSignaturePreverifyAhead(
	ctx context.Context,
	blocks []ledger.Block,
	wanted []bool,
) *signaturePreverifyAhead {
	//nolint:gosec // cancel is stored in p.cancel and called by stop
	ctx, cancel := context.WithCancel(ctx)
	p := &signaturePreverifyAhead{
		ctx:     ctx,
		cancel:  cancel,
		wanted:  make([]bool, len(blocks)),
		results: make([][]*lcommon.PreverifiedSignatures, len(blocks)),
		ready:   make([]chan struct{}, len(blocks)),
		done:    make(chan struct{}),
	}
	wantsAny := false
	for k := range blocks {
		p.ready[k] = make(chan struct{})
		p.wanted[k] = k < len(wanted) && wanted[k]
		if !p.wanted[k] {
			close(p.ready[k])
		}
		wantsAny = wantsAny || p.wanted[k]
	}
	if !wantsAny {
		close(p.done)
		return p
	}
	queue := make(chan signaturePreverifyJob)
	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for job := range queue {
				*job.result = lcommon.PreverifySignatures(job.tx)
				if job.remaining.Add(-1) == 0 {
					close(job.ready)
				}
			}
		})
	}
	go func() {
		defer close(p.done)
		defer wg.Wait()
		defer close(queue)
		for k, block := range blocks {
			if !p.wanted[k] {
				continue
			}
			txs := block.Transactions()
			p.results[k] = make([]*lcommon.PreverifiedSignatures, len(txs))
			if len(txs) == 0 {
				close(p.ready[k])
				continue
			}
			remaining := new(atomic.Int32)
			remaining.Store(int32(len(txs))) //nolint:gosec // block tx count
			for i, tx := range txs {
				select {
				case queue <- signaturePreverifyJob{
					tx:        tx,
					result:    &p.results[k][i],
					remaining: remaining,
					ready:     p.ready[k],
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return p
}

// forBlock returns block k's results by transaction position, waiting for the
// workers to finish that block. It returns nil when block k is not validated,
// when p is nil, or when the workers were stopped before finishing it; the
// caller then verifies every signature inline.
func (p *signaturePreverifyAhead) forBlock(
	k int,
) []*lcommon.PreverifiedSignatures {
	if p == nil || k < 0 || k >= len(p.wanted) || !p.wanted[k] {
		return nil
	}
	select {
	case <-p.ready[k]:
		return p.results[k]
	case <-p.ctx.Done():
		// Workers may have finished the block before stopping.
		select {
		case <-p.ready[k]:
			return p.results[k]
		default:
			return nil
		}
	}
}

// stop cancels the workers and waits for them to exit. It is safe to call
// more than once.
func (p *signaturePreverifyAhead) stop() {
	if p == nil {
		return
	}
	p.once.Do(p.cancel)
	<-p.done
}

// signatureCheckedBlocks narrows validated to the blocks whose signature rule
// can use a preverified result. Byron witnesses are always verified inline
// because their signing context is not part of the common witness interface.
func signatureCheckedBlocks(
	chunk []ledger.Block,
	validated []bool,
) []bool {
	checked := make([]bool, len(chunk))
	for k, blk := range chunk {
		checked[k] = k < len(validated) && validated[k] &&
			blk.Era().Id != byron.EraIdByron
	}
	return checked
}
