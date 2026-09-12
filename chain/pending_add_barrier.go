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

package chain

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// pendingAddDrainTimeout bounds how long a rollback waits for block adds whose
// store write lives in a caller-supplied transaction.
//
// It is a safety valve, not a synchronisation point: the wait ends the instant
// the last such transaction concludes, and every transaction the node opens
// concludes in well under a second. Bounding it is what keeps the barrier's
// worst case no worse than the bug it replaces. A caller that abandons its
// transaction, or that rolls the chain back from inside one it has not
// finished, would otherwise block that rollback -- and, because batchCommitMutex
// is writer-preferring, every chain mutation queued behind it -- for the life of
// the process. On expiry the wait returns an error so the rollback can abort
// before it reaches the removal loop.
const pendingAddDrainTimeout = 30 * time.Second

// pendingAddBarrier keeps the rollback paths that delete blocks by index from
// resolving an index whose block is still sitting in an uncommitted
// caller-supplied transaction.
//
// Chain.addBlockLocked writes the new block through whichever transaction it is
// given and then advances c.tipBlockIndex, c.currentTip and c.headers. With a
// nil transaction Database.BlockCreate opens and commits its own before the tip
// moves, so the store is never behind memory. With a caller-supplied
// transaction the chain neither performs nor observes the commit, so between
// the tip advancing and the caller committing there is an index the in-memory
// chain legitimately holds and the store cannot serve:
// ChainManager.removeBlockByIndex opens its own transaction, and no transaction
// sees another's uncommitted writes. rollbackLocked's removal loop starts at
// c.tipBlockIndex, so it failed its very first iteration with
// models.ErrBlockNotFound.
//
// Chain.batchCommitMutex closes the same window for the batch transactions the
// chain owns, by holding its read side from before the batch mutates memory
// until txn.Do returns. That shape is not available here, because the chain
// does not own the commit. Instead an add records its transaction in this set
// before mutating memory and releases the record from database.Txn.OnFinish,
// which fires on commit *and* on rollback -- AfterCommit alone would strand the
// record for good whenever the caller's transaction rolled back. The removal
// paths hold batchCommitMutex for write, which excludes new records, and then
// wait here for the records already in flight.
//
// Only adds carrying a caller-supplied transaction record anything. Every
// in-tree caller -- including the per-block blockfetch path that runs at chain
// tip -- passes a nil transaction and touches this barrier not at all.
type pendingAddBarrier struct {
	// mu guards pending and drained. pending is a set, not a count: one
	// transaction may carry any number of adds, and its single OnFinish
	// callback releases all of them at once. drained is closed when pending
	// empties, and is nil whenever pending is empty.
	mu      sync.Mutex
	pending map[*database.Txn][]callerTxnAdd
	drained chan struct{}
}

// callerTxnAdd records the chain state immediately before one block was added
// through a caller-owned transaction. If that transaction aborts, restoring
// the last compatible snapshot keeps the in-memory tip from naming a block
// that the store never committed.
type callerTxnAdd struct {
	tip        ochainsync.Tip
	tipIndex   uint64
	generation uint64
	headers    []queuedHeader
	blocks     []ocommon.Point
}

// holds reports whether txn is currently recorded as carrying an in-flight add.
func (b *pendingAddBarrier) holds(txn *database.Txn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.pending[txn]
	return ok
}

// hold records txn as carrying an in-flight add. It reports whether this is the
// first hold for txn, which is when -- and only when -- the caller must arrange
// for release. Repeat adds on the same transaction are covered by the hold
// already recorded, because it is released by that transaction concluding
// rather than by any one add finishing.
func (b *pendingAddBarrier) hold(txn *database.Txn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[txn]; ok {
		return false
	}
	if b.pending == nil {
		b.pending = make(map[*database.Txn][]callerTxnAdd)
	}
	if len(b.pending) == 0 {
		b.drained = make(chan struct{})
	}
	b.pending[txn] = nil
	return true
}

func (b *pendingAddBarrier) record(txn *database.Txn, add callerTxnAdd) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[txn]; ok {
		b.pending[txn] = append(b.pending[txn], add)
	}
}

func (b *pendingAddBarrier) discardLast(txn *database.Txn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	adds, ok := b.pending[txn]
	if !ok || len(adds) == 0 {
		return
	}
	b.pending[txn] = adds[:len(adds)-1]
}

func (b *pendingAddBarrier) adds(txn *database.Txn) []callerTxnAdd {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]callerTxnAdd(nil), b.pending[txn]...)
}

// release drops txn's hold. A release for a transaction that holds nothing is a
// no-op rather than a double close of drained, so the caller need not reason
// about whether its OnFinish callback ran inline at registration time (which it
// does when the transaction had already finished) or from the transaction's own
// terminal path.
func (b *pendingAddBarrier) release(txn *database.Txn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[txn]; !ok {
		return
	}
	delete(b.pending, txn)
	if len(b.pending) == 0 && b.drained != nil {
		close(b.drained)
		b.drained = nil
	}
}

// heldCount reports how many transactions currently hold the barrier.
func (b *pendingAddBarrier) heldCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

// awaitDrained waits for every recorded hold to be released, or for timeout to
// expire. It returns the number of holds still outstanding and whether the set
// drained. See pendingAddDrainTimeout for why the wait is bounded.
func (b *pendingAddBarrier) awaitDrained(timeout time.Duration) (int, bool) {
	b.mu.Lock()
	ch := b.drained
	b.mu.Unlock()
	if ch == nil {
		return 0, true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return 0, true
	case <-timer.C:
		return b.heldCount(), false
	}
}

// beginCallerTxnAdd records txn as carrying an in-flight add and returns the
// function that ends the add's exclusion against the removal paths. The
// returned function must be called before the add returns; the record itself
// outlives it and is dropped when txn concludes.
//
// The exclusion is c.batchCommitMutex's read side, the same barrier the
// chain-owned batches take, so a removal path holding it for write cannot
// observe a record appearing underneath it. The read hold ends with the add,
// not with the transaction: holding it until the caller committed would let a
// caller that adds a block and then rolls the chain back on the same goroutine
// deadlock against its own hold.
//
// An add on a transaction already recorded skips the exclusion entirely. A
// rollback draining right now is already waiting for that transaction, so the
// add widens nothing -- while queueing it behind the rollback's write lock
// would pair a rollback waiting on a transaction with the goroutine that owns
// that transaction waiting on the rollback, and neither would move until the
// drain expired.
func (c *Chain) beginCallerTxnAdd(txn *database.Txn) func() {
	// A nil transaction leaves the store write to the chain, which commits it
	// before the tip advances. A non-persistent chain writes to the manager's
	// block cache rather than the store, and its rollback deletes from that
	// same cache, so neither has a window to close.
	if txn == nil || !c.persistent {
		return func() {}
	}
	if c.pendingAdds.holds(txn) {
		return func() {}
	}
	c.batchCommitMutex.RLock()
	if c.pendingAdds.hold(txn) {
		txn.OnFinish(func() { c.finishCallerTxnAdd(txn) })
	}
	return c.batchCommitMutex.RUnlock
}

// beginStandaloneAdd prevents a standalone add from being built on a
// caller-owned block that may later roll back. The write side remains held
// through the add, so a caller add cannot interpose between the wait and the
// standalone add's c.mutex acquisition.
func (c *Chain) beginStandaloneAdd() (func(), error) {
	c.batchCommitMutex.Lock()
	if err := c.awaitPendingCallerAdds(); err != nil {
		c.batchCommitMutex.Unlock()
		return nil, err
	}
	return c.batchCommitMutex.Unlock, nil
}

// recordCallerTxnAdd saves the pre-add chain state. The caller must hold
// c.mutex, and must have begun the caller-transaction barrier already.
func (c *Chain) recordCallerTxnAdd(txn *database.Txn) {
	if txn == nil || !c.persistent {
		return
	}
	c.pendingAdds.record(txn, callerTxnAdd{
		tip:        c.currentTip,
		tipIndex:   c.tipBlockIndex,
		generation: c.mutationGeneration,
		headers:    append([]queuedHeader(nil), c.headers...),
	})
}

// finishCallerTxnAdd releases the barrier and restores state for an aborted
// caller transaction. Restoration is conditional: another chain mutation may
// have advanced the tip after this add, in which case overwriting it would
// resurrect a state that no longer describes the store.
func (c *Chain) finishCallerTxnAdd(txn *database.Txn) {
	adds := c.pendingAdds.adds(txn)
	if txn.IsCommitted() || len(adds) == 0 {
		c.pendingAdds.release(txn)
		return
	}
	c.mutex.Lock()
	for i := len(adds) - 1; i >= 0; i-- {
		add := adds[i]
		if c.tipBlockIndex != add.tipIndex+1 || c.mutationGeneration != add.generation+1 {
			slog.Default().Error(
				"skipped in-memory restore after caller transaction rollback: chain moved under the add",
				"component", "chain",
				"chain_id", c.id,
				"add_tip_block_index", add.tipIndex+1,
				"tip_block_index", c.tipBlockIndex,
			)
			break
		}
		c.currentTip = add.tip
		c.tipBlockIndex = add.tipIndex
		c.mutationGeneration = add.generation
		c.headers = add.headers
		if !c.persistent {
			c.blocks = add.blocks
		}
	}
	c.mutex.Unlock()
	c.pendingAdds.release(txn)
}

// awaitPendingCallerAdds waits for the adds whose store write is still in a
// caller-supplied transaction, so a removal loop that follows only ever asks
// the store for indices it has been given. Callers must already hold
// c.batchCommitMutex for write, which is what keeps a further such add from
// being recorded while this waits.
func (c *Chain) awaitPendingCallerAdds() error {
	outstanding, drained := c.pendingAdds.awaitDrained(pendingAddDrainTimeout)
	if drained {
		return nil
	}
	slog.Default().Error(
		"aborting rollback while caller-supplied add transactions are still open",
		"component", "chain",
		"chain_id", c.id,
		"outstanding_transactions", outstanding,
		"timeout", pendingAddDrainTimeout.String(),
	)
	return fmt.Errorf(
		"caller-supplied add transactions still open after %s: %d outstanding",
		pendingAddDrainTimeout,
		outstanding,
	)
}
