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
//
// The bound is charged once per abandoned transaction, not once per removal
// path. A hold is never evicted -- the restoration snapshots it carries are
// owed to that transaction whenever it concludes -- so without that rule an
// abandoned transaction would tax every later rollback and
// RewindPrimaryChainToPoint the full timeout for the life of the process
// rather than exposing the one removal that met it. awaitDrained marks a hold
// that outlives a wait and fails later waits immediately.
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
	// mu guards pending, active, discarded, and drained. pending is a set, not
	// a count: one transaction may carry any number of adds, and an OnFinish
	// callback releases all of them at once. active counts add attempts that
	// have reserved the shared exclusion but have not returned. drained closes
	// when pending
	// empties, and is nil whenever pending is empty.
	//
	// expired holds the subset of pending that outlived a removal path's
	// wait. It exists so that wait is paid once rather than once per
	// removal; see awaitDrained.
	mu        sync.Mutex
	pending   map[*database.Txn][]callerTxnAdd
	active    map[*database.Txn]int
	discarded map[*database.Txn]int
	drained   chan struct{}
	expired   map[*database.Txn]struct{}
}

// callerTxnAdd records the chain state immediately before one block was added
// through a caller-owned transaction. If that transaction aborts, restoring
// the last compatible snapshot keeps the in-memory tip from naming a block
// that the store never committed.
type callerTxnAdd struct {
	tip              ochainsync.Tip
	tipIndex         uint64
	generation       uint64
	headerGeneration uint64
	headers          []queuedHeader
}

// hold records txn as carrying an in-flight add. It reports whether this is the
// first hold for txn, which is when -- and only when -- the caller must arrange
// for release. Repeat adds on the same transaction are covered by the hold
// already recorded, because it is released by that transaction concluding
// rather than by any one add finishing.
func (b *pendingAddBarrier) hold(txn *database.Txn) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active == nil {
		b.active = make(map[*database.Txn]int)
	}
	b.active[txn]++
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

// discardLast drops the snapshot recorded for an add that then failed.
//
// The active-attempt reservation keeps a concurrent attempt from losing its
// future record when this snapshot is discarded. completeAttempt drops the
// hold only once every attempt has returned and no successful snapshot remains.
func (b *pendingAddBarrier) discardLast(txn *database.Txn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	adds, ok := b.pending[txn]
	if !ok || len(adds) == 0 {
		return
	}
	b.pending[txn] = adds[:len(adds)-1]
	if b.discarded == nil {
		b.discarded = make(map[*database.Txn]int)
	}
	b.discarded[txn]++
}

// completeAttempt drops one add attempt's active reservation. A rejected-only
// transaction releases its otherwise-empty hold once every concurrent attempt
// has left the add path; a successful attempt keeps the hold through finish.
func (b *pendingAddBarrier) completeAttempt(txn *database.Txn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active[txn] > 1 {
		b.active[txn]--
		return
	}
	delete(b.active, txn)
	if len(b.pending[txn]) == 0 && b.discarded[txn] > 0 {
		b.releaseLocked(txn)
	}
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
	b.releaseLocked(txn)
}

// releaseLocked drops txn's hold and any expiry mark it carried. Callers must
// hold b.mu. Dropping the mark with the hold is what lets the barrier recover:
// a transaction that outlived a wait and then concluded stops failing later
// waits.
func (b *pendingAddBarrier) releaseLocked(txn *database.Txn) {
	if _, ok := b.pending[txn]; !ok {
		return
	}
	delete(b.pending, txn)
	delete(b.active, txn)
	delete(b.discarded, txn)
	delete(b.expired, txn)
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
//
// A hold that outlives one wait is marked expired, and every later wait fails
// immediately while it is still held. The barrier cannot evict the hold to
// achieve that: the restoration snapshots it carries are still owed to that
// transaction if it ever concludes, and dropping them would leave the
// in-memory chain naming a block the store never received. Marking it instead
// charges pendingAddDrainTimeout to the first removal path that meets an
// abandoned transaction rather than to every one of them for the life of the
// process. The mark is dropped with the hold, so a transaction that concludes
// late returns the barrier to normal.
func (b *pendingAddBarrier) awaitDrained(timeout time.Duration) (int, bool) {
	b.mu.Lock()
	if len(b.expired) > 0 {
		outstanding := len(b.pending)
		b.mu.Unlock()
		return outstanding, false
	}
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
		outstanding := b.markExpired()
		return outstanding, outstanding == 0
	}
}

// markExpired records every hold still outstanding as having outlived a
// removal path's wait, and reports how many there were. A set that drained
// between the timer firing and this lock marks nothing and reports zero, which
// awaitDrained reads as the drain it was.
func (b *pendingAddBarrier) markExpired() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.pending) == 0 {
		return 0
	}
	if b.expired == nil {
		b.expired = make(map[*database.Txn]struct{}, len(b.pending))
	}
	for txn := range b.pending {
		b.expired[txn] = struct{}{}
	}
	return len(b.pending)
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
// Every attempt takes the exclusion before reserving the transaction's hold.
// Otherwise a failed first attempt can release the hold after a concurrent
// second attempt has observed it but before that second attempt records its
// snapshot, leaving the successful add invisible to rollback.
func (c *Chain) beginCallerTxnAdd(txn *database.Txn) func() {
	// A nil transaction leaves the store write to the chain, which commits it
	// before the tip advances. A non-persistent chain writes to the manager's
	// block cache rather than the store, and its rollback deletes from that
	// same cache, so neither has a window to close.
	if txn == nil || !c.persistent {
		return func() {}
	}
	c.batchCommitMutex.RLock()
	if c.pendingAdds.hold(txn) {
		txn.OnFinish(func() { c.finishCallerTxnAdd(txn) })
	}
	return func() {
		c.pendingAdds.completeAttempt(txn)
		c.batchCommitMutex.RUnlock()
	}
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
		tip:              c.currentTip,
		tipIndex:         c.tipBlockIndex,
		generation:       c.mutationGeneration,
		headerGeneration: c.headerMutationGeneration,
		headers:          append([]queuedHeader(nil), c.headers...),
	})
}

// finishCallerTxnAdd releases the barrier and restores state for an aborted
// caller transaction. Restoration is conditional: another chain mutation may
// have advanced the tip after this add, in which case overwriting it would
// resurrect a state that no longer describes the store.
func (c *Chain) finishCallerTxnAdd(txn *database.Txn) {
	adds := c.pendingAdds.adds(txn)
	committed := txn.IsCommitted()
	if committed || len(adds) == 0 {
		c.finishPendingTxnEvents(txn, committed)
		c.pendingAdds.release(txn)
		c.PublishPendingChainUpdates()
		return
	}
	c.mutex.Lock()
	for i := len(adds) - 1; i >= 0; i-- {
		add := adds[i]
		if c.tipBlockIndex != add.tipIndex+1 ||
			c.mutationGeneration != add.generation+1 {
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
		if c.headerMutationGeneration == add.headerGeneration+1 {
			c.headerMutationGeneration = add.headerGeneration
			c.headers = add.headers
		}
	}
	c.mutex.Unlock()
	c.finishPendingTxnEvents(txn, false)
	c.pendingAdds.release(txn)
	c.PublishPendingChainUpdates()
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
