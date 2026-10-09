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
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/utxoref"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// utxoPrefetchAhead resolves the input UTxOs of block k+1 on its own goroutine
// and a read-pool transaction while block k applies in the chunk's write
// transaction. The write transaction is never shared with the goroutine.
//
// The read transaction sees the state committed before the chunk, so it can
// serve a UTxO that an earlier block of the chunk has already spent. Every
// block's result therefore excludes everything the chunk's earlier blocks
// consume, computed from the blocks themselves rather than from apply
// progress, so the exclusion cannot race with the writer. Outputs created
// earlier in the chunk are absent from the snapshot and are resolved by the
// caller's fallback read in the write transaction.
type utxoPrefetchAhead struct {
	ls     *LedgerState
	blocks []ledger.Block
	wanted []bool
	// slots[k] receives block k's result; slot 0 is never filled because the
	// first block of a chunk has no earlier block to overlap with.
	slots  []chan map[utxoref.Key]lcommon.Utxo
	allow  chan int
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	// readTxn is the goroutine's read transaction, kept so tests can check it
	// is released once done is closed. Production code does not read it.
	readTxn *database.Txn
}

// startUtxoPrefetchAhead launches the prefetch goroutine for a chunk. wanted[k]
// reports whether block k is validated, and so reads its inputs. The caller
// must call stop on every exit path.
func (ls *LedgerState) startUtxoPrefetchAhead(
	ctx context.Context,
	blocks []ledger.Block,
	wanted []bool,
) *utxoPrefetchAhead {
	//nolint:gosec // cancel is stored in p.cancel and called by stop
	ctx, cancel := context.WithCancel(ctx)
	p := &utxoPrefetchAhead{
		ls:     ls,
		blocks: blocks,
		wanted: wanted,
		slots:  make([]chan map[utxoref.Key]lcommon.Utxo, len(blocks)),
		allow:  make(chan int, len(blocks)+1),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	for i := range p.slots {
		p.slots[i] = make(chan map[utxoref.Key]lcommon.Utxo, 1)
	}
	go p.run(ctx)
	return p
}

func (p *utxoPrefetchAhead) run(ctx context.Context) {
	defer close(p.done)
	// The read transaction is opened lazily and released on every exit,
	// including cancellation while the goroutine is idle.
	defer func() {
		if p.readTxn != nil {
			p.readTxn.Release()
		}
	}()
	consumed := make(map[utxoref.Key]struct{})
	addConsumed := func(block ledger.Block) {
		for _, tx := range block.Transactions() {
			collectConsumedInputs(consumed, tx)
		}
	}
	if len(p.blocks) == 0 {
		return
	}
	addConsumed(p.blocks[0])
	limit := 0
	for k := 1; k < len(p.blocks); k++ {
		// Stay at most one block ahead of the block being applied.
		for limit < k {
			select {
			case v := <-p.allow:
				limit = max(limit, v)
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		var result map[utxoref.Key]lcommon.Utxo
		if p.wanted[k] {
			if p.readTxn == nil {
				p.readTxn = p.ls.db.Transaction(ctx, false)
			}
			result = p.ls.prefetchBlockUtxos(
				ctx,
				p.readTxn,
				p.blocks[k].Transactions(),
				nil,
				consumed,
			)
		}
		p.slots[k] <- result
		addConsumed(p.blocks[k])
	}
}

// take returns block k's prefetched UTxOs, waiting for the goroutine if it is
// still resolving them. It returns nil when block k was not prefetched, the
// goroutine stopped first, or p is nil; the caller then reads every input
// itself.
func (p *utxoPrefetchAhead) take(
	k int,
) map[utxoref.Key]lcommon.Utxo {
	if p == nil || k <= 0 || k >= len(p.slots) {
		return nil
	}
	// Permit the goroutine to resolve block k+1 while block k applies.
	select {
	case p.allow <- k + 1:
	default:
	}
	select {
	case m := <-p.slots[k]:
		return m
	case <-p.done:
		select {
		case m := <-p.slots[k]:
			return m
		default:
			return nil
		}
	}
}

// stop cancels the goroutine and waits for it to exit, releasing its read
// transaction. It is safe to call more than once.
func (p *utxoPrefetchAhead) stop() {
	if p == nil {
		return
	}
	p.once.Do(p.cancel)
	<-p.done
}

// collectConsumedInputs adds every input and collateral input of tx, at every
// level, to dst. A Dijkstra transaction's Inputs() omits its sub-transactions'
// inputs. Both kinds are added whatever the validity flag: over-adding only
// turns a prefetch hit into a fallback read.
func collectConsumedInputs(
	dst map[utxoref.Key]struct{},
	tx lcommon.Transaction,
) {
	for _, level := range TransactionLevels(tx) {
		for _, in := range level.Inputs() {
			dst[utxoref.ForInput(in)] = struct{}{}
		}
		for _, in := range level.Collateral() {
			dst[utxoref.ForInput(in)] = struct{}{}
		}
	}
}

type aheadUtxosKey struct{}

// withAheadUtxos returns a context carrying the UTxOs utxoPrefetchAhead
// resolved for the block about to be applied, or ctx itself when there are
// none. It rides the context because ledgerProcessBlock is called with
// positional arguments from many tests, and only the apply loop has a result
// to pass.
func withAheadUtxos(
	ctx context.Context,
	utxos map[utxoref.Key]lcommon.Utxo,
) context.Context {
	if utxos == nil {
		return ctx
	}
	return context.WithValue(ctx, aheadUtxosKey{}, utxos)
}

// aheadUtxosFrom returns the UTxOs withAheadUtxos attached to ctx, or nil.
func aheadUtxosFrom(ctx context.Context) map[utxoref.Key]lcommon.Utxo {
	utxos, _ := ctx.Value(aheadUtxosKey{}).(map[utxoref.Key]lcommon.Utxo)
	return utxos
}
