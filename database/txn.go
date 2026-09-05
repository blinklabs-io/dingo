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

package database

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/types"
)

// PartialCommitError is returned when blob commits but metadata fails.
// This indicates the database is in an inconsistent state requiring recovery.
type PartialCommitError struct {
	MetadataErr     error // The underlying metadata commit error
	CommitTimestamp int64 // Timestamp written to the blob store
}

func (e PartialCommitError) Error() string {
	return fmt.Sprintf(
		"partial commit at timestamp %d: metadata failed: %v",
		e.CommitTimestamp,
		e.MetadataErr,
	)
}

func (e PartialCommitError) Unwrap() error {
	return e.MetadataErr
}

// Is allows errors.Is(err, types.ErrPartialCommit) to match this error.
func (e PartialCommitError) Is(target error) bool {
	return target == types.ErrPartialCommit
}

// Txn is a wrapper that coordinates both metadata and blob transactions.
// Metadata and blob are first-class siblings, not nested.
type Txn struct {
	db          *Database
	blobTxn     types.Txn
	metadataTxn types.Txn
	lock        sync.Mutex
	finished    bool
	committed   bool
	readWrite   bool
	afterCommit []func()
	dispatching bool

	// barrierHeld records whether this Txn holds the shared side of
	// db.commitBarrier (see acquireCommitBarrier). Guarded by lock.
	barrierHeld bool
}

// acquireCommitBarrier holds the shared (read) side of db.commitBarrier
// for the lifetime of a read-write Txn that opens a metadata write
// transaction, from construction through Commit/Rollback/Release. It
// must be taken before the underlying transaction is opened below, not
// just around the eventual commit: the metadata plugin's write
// connection pool is sized to exactly one connection (see
// sqlite.database.go), so an already-BEGUN but not-yet-committed
// transaction holds that one connection regardless of whether Commit()
// has been called yet. Database.PauseCommits (used by
// database/lifecycle.Snapshot around its blob+metadata backup calls)
// takes the exclusive side; if this barrier were only held during Commit,
// PauseCommits could acquire its lock while such a transaction sits
// BEGUN-but-uncommitted, and Snapshot's metadata backup (VACUUM INTO,
// which needs that same one connection) would then deadlock against it —
// the writer can't reach Commit's RLock to finish, and Snapshot can't
// release its Lock until the backup call returns.
//
// hasMetadataWrite must be false for a blob-only Txn (NewBlobOnlyTxn):
// unlike sqlite's metadata store, badger natively supports concurrent
// read-write transactions, so a blob-only Txn never contends for the
// single connection PauseCommits protects, and its own commit never
// writes the commit timestamp PauseCommits keeps consistent (see
// Txn.Commit — that update only runs when both blobTxn and metadataTxn
// are set). Acquiring the barrier here anyway would be needless *and*
// actively dangerous: several callers (e.g. deleteUtxoBlobs,
// deleteTxBlobs) open batched blob-only Txns while already holding an
// outer read-write Txn open on the same goroutine. Go's sync.RWMutex
// isn't reentrant — once a PauseCommits caller's Lock() is queued, a
// second RLock() from the same goroutine that already holds the first
// blocks too, and the outer Txn can never reach Commit/Rollback to
// release the first RLock. Skipping the barrier for blob-only Txns
// avoids that self-deadlock entirely rather than trying to detect it.
func acquireCommitBarrier(t *Txn, hasMetadataWrite bool) {
	if t.readWrite && hasMetadataWrite && t.db != nil {
		t.db.commitBarrier.RLock()
		t.barrierHeld = true
	}
}

// releaseCommitBarrierLocked releases the barrier acquired by
// acquireCommitBarrier, if held. Callers must hold t.lock.
func (t *Txn) releaseCommitBarrierLocked() {
	if t.barrierHeld {
		t.barrierHeld = false
		t.db.commitBarrier.RUnlock()
	}
}

// finishLocked marks the transaction terminal and releases its commit
// barrier hold. Every path in Commit and rollback that sets finished must
// go through it, because finished is also what makes a later
// Rollback/Release a no-op: a terminal path that sets the flag without
// releasing strands the shared hold for the process's lifetime, and the
// caller's deferred Release cannot repair it. That is not a bounded cost —
// PauseCommits then waits forever for a reader that will never release,
// and the barrier's writer preference blocks every read-write Txn
// constructed behind it. Releasing exactly once is safe to route
// everywhere because releaseCommitBarrierLocked clears barrierHeld before
// unlocking, so a repeat call is a no-op rather than an unmatched RUnlock
// (which cancellableBarrier panics on). Callers must hold t.lock.
func (t *Txn) finishLocked() {
	t.finished = true
	t.releaseCommitBarrierLocked()
}

func NewTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t, db.Metadata() != nil)
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	if ms := db.Metadata(); ms != nil {
		// Use the read connection pool for read-only transactions to
		// avoid contending with the SQLite write connection. This
		// prevents chainsync FindIntersect and snapshot calculations
		// from blocking on concurrent block processing.
		//
		// context.Background(): NewTxn itself takes no ctx, and none of
		// its own callers (Database.Transaction and its ~100 call sites
		// across ledger/api/mempool) have one to offer yet either -- this
		// is the current propagation boundary between the metadata
		// store's own ctx-aware Transaction/ReadTransaction and the rest
		// of the node, not a gap within the metadata store itself.
		// Threading a real ctx from callers into this boundary is a
		// separate, distinctly larger change than this metadata-store
		// specific one.
		if readWrite {
			t.metadataTxn = ms.Transaction(context.Background())
		} else {
			t.metadataTxn = ms.ReadTransaction(context.Background())
		}
		if t.metadataTxn == nil {
			db.logger.Warn(
				"metadata transaction is nil; callers must nil-check txn.Metadata()",
			)
		}
	}
	return t
}

func NewBlobOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t, false)
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	return t
}

func NewMetadataOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t, db.Metadata() != nil)
	if ms := db.Metadata(); ms != nil {
		// See NewTxn's matching comment: context.Background() here is the
		// current propagation boundary, not a metadata-store-internal gap.
		if readWrite {
			t.metadataTxn = ms.Transaction(context.Background())
		} else {
			t.metadataTxn = ms.ReadTransaction(context.Background())
		}
		if t.metadataTxn == nil {
			db.logger.Warn(
				"metadata transaction is nil; callers must nil-check txn.Metadata()",
			)
		}
	}
	return t
}

func (t *Txn) DB() *Database {
	return t.db
}

// Metadata returns the underlying metadata transaction handle
func (t *Txn) Metadata() types.Txn {
	return t.metadataTxn
}

// Blob returns the blob transaction handle
func (t *Txn) Blob() types.Txn {
	return t.blobTxn
}

// IsReadWrite reports whether the transaction was opened for writing.
func (t *Txn) IsReadWrite() bool {
	return t.readWrite
}

// AfterCommit registers fn to run after this transaction commits durably.
// Callbacks run in registration order, once, only on a successful Commit; a
// rollback or a failed commit never fires them. Use it for side effects that
// must reflect committed state only — e.g. metrics that must not count work a
// rollback discards. Registration concurrent with, or after, a successful
// Commit joins the serialized callback drain instead of being lost. Callbacks
// run without the transaction lock held, so they may register another callback.
// A callback that panics has its panic recovered and logged: it does not
// propagate to Commit's caller, abort the other callbacks in the drain, or
// wedge the dispatch loop for callbacks registered afterward.
func (t *Txn) AfterCommit(fn func()) {
	if fn == nil {
		return
	}
	t.lock.Lock()
	if t.finished && !t.committed {
		t.lock.Unlock()
		return
	}
	t.afterCommit = append(t.afterCommit, fn)
	if !t.committed || t.dispatching {
		t.lock.Unlock()
		return
	}
	t.dispatching = true
	t.lock.Unlock()
	t.dispatchAfterCommit()
}

func (t *Txn) dispatchAfterCommit() {
	for {
		t.lock.Lock()
		callbacks := t.afterCommit
		t.afterCommit = nil
		if len(callbacks) == 0 {
			t.dispatching = false
			t.lock.Unlock()
			return
		}
		t.lock.Unlock()
		for _, fn := range callbacks {
			t.runAfterCommitCallback(fn)
		}
	}
}

// ErrTxnPanic identifies an error produced by recovering a panic raised by
// transaction-related work, as opposed to an ordinary error a caller
// returned deliberately. Every transaction worker that can convert a panic
// into an error return (Txn.Do; ledger.DatabaseWorkerPool.executeOperation)
// wraps it with this sentinel via NewTxnPanicError, so a caller can tell
// "the underlying operation failed" (an ordinary error) apart from
// "something the operation didn't expect to fail this way panicked" (this)
// with a single errors.Is check, regardless of which worker recovered it.
var ErrTxnPanic = errors.New("transaction worker panicked")

// NewTxnPanicError formats a recovered panic value r (from the given
// worker/context label) into an error wrapping ErrTxnPanic. It is the
// shared error half of the panic contract documented below; logTxnPanic is
// the shared logging half.
func NewTxnPanicError(context string, r any) error {
	return fmt.Errorf("%w: %s: %v", ErrTxnPanic, context, r)
}

// Panic contract for transaction workers (this function and Do, below,
// plus ledger.DatabaseWorkerPool.executeOperation): a panic raised by
// transaction-related work is always recovered and logged with its stack
// trace via logTxnPanic, and -- when the worker has anywhere to put it -- an
// error wrapping ErrTxnPanic via NewTxnPanicError, before the recovering
// deferred func decides what happens next. What differs between workers is
// only that next step, and the difference tracks whether the worker has a
// caller to hand the result to:
//   - Do and executeOperation both run underneath a caller that is
//     synchronously waiting on a result (Do's caller on the stack;
//     executeOperation's via its result channel), so both convert the
//     panic into a returned ErrTxnPanic-wrapped error instead of crashing
//     that caller's goroutine out from under it. Do additionally rolls
//     back before returning, since it -- unlike executeOperation, whose
//     OpFunc owns any transaction it opened -- is itself the transaction
//     boundary.
//   - runAfterCommitCallback runs detached on the dispatch loop, after the
//     registering caller has already moved on and after the transaction
//     has already durably committed; there is nobody left to hand a
//     result to, and it may be only one of several callbacks in the
//     current drain. Returning or re-panicking here would drop every
//     other callback already dequeued for this drain and strand every
//     callback registered afterward (see the comment below), so it logs
//     and continues instead -- the one case where the contract's error
//     half has nowhere to go.
//
// logTxnPanic implements the shared logging half of that contract so every
// path reports a panic identically; it never itself panics, even when
// logger is nil (a bare Txn built directly for a test may have no db, and
// so no logger, but must still finish the corresponding cleanup).
func logTxnPanic(logger *slog.Logger, msg string, r any) {
	if logger == nil {
		return
	}
	logger.Error(
		msg,
		"panic", fmt.Sprintf("%v", r),
		"stack", string(debug.Stack()),
	)
}

// runAfterCommitCallback runs a single after-commit callback. See the panic
// contract above Do: a panic here is logged, not propagated, because a
// panic escaping the drain loop would leave dispatching=true, silently
// stranding every callback registered afterward, and would drop the
// callbacks already dequeued for this drain.
func (t *Txn) runAfterCommitCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			logTxnPanic(t.logger(), "panic in after-commit callback", r)
		}
	}()
	fn()
}

// logger returns the transaction's logger, or nil if this Txn was built
// without a db (as bare Txn{} literals in tests do).
func (t *Txn) logger() *slog.Logger {
	if t.db == nil {
		return nil
	}
	return t.db.logger
}

type savepointTxn interface {
	SavePoint(string) error
	RollbackTo(string) error
}

// SavePoint creates a metadata transaction savepoint. Blob stores do not expose
// savepoints, so callers that write blob keys before rolling back to a savepoint
// must explicitly clean those keys up.
func (t *Txn) SavePoint(name string) error {
	if t.metadataTxn == nil {
		return types.ErrNilTxn
	}
	savepointer, ok := t.metadataTxn.(savepointTxn)
	if !ok {
		return types.ErrTxnWrongType
	}
	return savepointer.SavePoint(name)
}

// RollbackTo rolls the metadata transaction back to a previous savepoint. Blob
// writes are unaffected; see SavePoint.
func (t *Txn) RollbackTo(name string) error {
	if t.metadataTxn == nil {
		return types.ErrNilTxn
	}
	savepointer, ok := t.metadataTxn.(savepointTxn)
	if !ok {
		return types.ErrTxnWrongType
	}
	return savepointer.RollbackTo(name)
}

// Do executes the specified function in the context of the transaction. Any
// errors returned will result in the transaction being rolled back. If the
// function panics, the transaction is rolled back and Do returns an error
// wrapping ErrTxnPanic instead of letting the panic escape -- see the panic
// contract above runAfterCommitCallback for how this fits the same contract
// as executeOperation and why runAfterCommitCallback itself cannot do the
// same.
func (t *Txn) Do(fn func(*Txn) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logTxnPanic(
				t.logger(),
				"panic in transaction function, ensuring rollback",
				r,
			)
			// Attempt rollback to ensure transaction is cleaned up. This
			// call is itself already inside Do's one recovery defer, so a
			// second panic from Rollback (or the underlying store's
			// Rollback it calls) would otherwise propagate straight out
			// of this already-executing deferred function -- the
			// outermost frame in Do -- and crash the goroutine instead of
			// returning the converted error below. safeRollback recovers
			// that second panic too, so Do always returns rather than
			// ever re-panicking.
			if rbErr := safeRollback(t); rbErr != nil {
				logTxnPanic(t.logger(), "rollback failed after panic", rbErr)
				err = fmt.Errorf(
					"%w (rollback also failed: %w)",
					NewTxnPanicError("transaction function", r),
					rbErr,
				)
				return
			}
			err = NewTxnPanicError("transaction function", r)
		}
	}()

	if fnErr := fn(t); fnErr != nil {
		if rbErr := t.Rollback(); rbErr != nil {
			return fmt.Errorf(
				"rollback failed: %w: original error: %w",
				rbErr,
				fnErr,
			)
		}
		return fnErr
	}
	if commitErr := t.Commit(); commitErr != nil {
		return fmt.Errorf("commit failed: %w", commitErr)
	}
	return nil
}

// safeRollback calls t.Rollback(), recovering and converting to an error
// any panic Rollback itself (or the underlying blob/metadata store's
// Rollback it calls) raises. See the comment at its call site in Do for
// why this recovery must be nested rather than relying on Do's own.
func safeRollback(t *Txn) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("rollback panicked: %v", r)
		}
	}()
	return t.Rollback()
}

func (t *Txn) Commit() error {
	t.lock.Lock()
	dispatchAfterUnlock := false
	defer func() {
		// A provider may panic while updating the commit timestamp or
		// committing either store. Release both synchronization primitives
		// before that panic reaches Txn.Do's recovery handler, so its Rollback
		// can reacquire t.lock and finish the underlying transactions. Do not
		// mark the transaction finished here: the recovery rollback still owns
		// that transition. releaseCommitBarrierLocked is idempotent, so normal
		// terminal paths may already have released it through finishLocked.
		t.releaseCommitBarrierLocked()
		t.lock.Unlock()
		if dispatchAfterUnlock {
			t.dispatchAfterCommit()
		}
	}()
	if t.finished {
		return nil
	}
	// Fail fast if neither store is available for a read-write transaction
	if t.readWrite && t.blobTxn == nil && t.metadataTxn == nil {
		t.finishLocked()
		return types.ErrNoStoreAvailable
	}
	// No need to commit for read-only, but we do want to free up resources
	if !t.readWrite {
		return t.rollback()
	}
	// Update the commit timestamp in both DBs if using both.
	// Track timestamp for error reporting if partial commit occurs.
	var commitTimestamp int64
	if t.blobTxn != nil && t.metadataTxn != nil {
		commitTimestamp = time.Now().UnixMilli()
		if err := t.db.updateCommitTimestamp(t, commitTimestamp); err != nil {
			// Rollback both transactions on timestamp update failure
			_ = t.blobTxn.Rollback()
			_ = t.metadataTxn.Rollback()
			t.finishLocked()
			return fmt.Errorf("failed to update commit timestamp: %w", err)
		}
	}
	// Commit blob transaction first (so if this fails, metadata never commits)
	if t.blobTxn != nil {
		if err := t.blobTxn.Commit(); err != nil {
			// Blob commit failed - rollback metadata only
			// Note: Most DB engines auto-rollback on commit failure
			if t.metadataTxn != nil {
				_ = t.metadataTxn.Rollback()
			}
			t.finishLocked()
			return fmt.Errorf("blob commit failed: %w", err)
		}
		// Make the blob commit durable before the metadata commit that
		// references it. Committing blob first only keeps the blob store ahead
		// of the metadata tip in memory; on disk the two stores flush on very
		// different schedules (SQLite at WAL checkpoints, Badger when its
		// 128MiB memtable rotates, which at chain tip can take hours), so
		// without this barrier an unclean host shutdown leaves a durable
		// metadata tip pointing at blocks the blob store discarded. Startup
		// reconciliation can trim a blob store that is ahead but cannot rebuild
		// blocks missing beneath the ledger tip; it rolls the ledger back
		// instead, and that rollback is far more destructive than one fsync per
		// commit. Only combined transactions pay the cost -- blob-only bulk
		// paths sync at their own barriers, and Sync is a store-wide flush, so
		// the next combined commit also makes those batches durable.
		if blobStore := t.db.Blob(); blobStore != nil && t.metadataTxn != nil {
			if syncErr := blobStore.Sync(); syncErr != nil {
				_ = t.metadataTxn.Rollback()
				t.finishLocked()
				// The blob transaction is committed and carries the new commit
				// timestamp while metadata does not, which is the same
				// inconsistency a failed metadata commit leaves behind. Report
				// it as a partial commit so the caller runs the existing
				// recovery that trims the blob store back to the metadata tip,
				// rather than leaving an un-reconciled timestamp mismatch for
				// the next startup to trip over.
				err := fmt.Errorf("blob sync failed: %w", syncErr)
				t.db.logger.Error(
					"partial commit: blob committed, blob sync failed",
					"error", syncErr,
					"commit_timestamp", commitTimestamp,
				)
				ret := PartialCommitError{
					MetadataErr:     err,
					CommitTimestamp: commitTimestamp,
				}
				return ret
			}
		}
	}
	// Commit metadata transaction
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Commit(); err != nil {
			_ = t.metadataTxn.Rollback()
			t.finishLocked()
			// Only return PartialCommitError when blob was actually committed.
			// Per docstring, this error type signifies "blob commits but metadata fails."
			// When t.blobTxn == nil (metadata-only txn), no blob was committed.
			if t.blobTxn != nil {
				t.db.logger.Error(
					"partial commit: blob committed, metadata failed",
					"error", err,
					"commit_timestamp", commitTimestamp,
				)
				// Return PartialCommitError so callers can detect with
				// errors.Is(err, types.ErrPartialCommit) and trigger recovery
				ret := PartialCommitError{
					MetadataErr:     err,
					CommitTimestamp: commitTimestamp,
				}
				return ret
			}
			return fmt.Errorf("metadata commit failed: %w", err)
		}
	}
	t.committed = true
	t.dispatching = true
	t.finishLocked()
	dispatchAfterUnlock = true
	return nil
}

func (t *Txn) Rollback() error {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.rollback()
}

func (t *Txn) rollback() error {
	if t.finished {
		return nil
	}
	// Deferred, not a plain trailing call: a panicking provider Rollback
	// below (blobTxn or metadataTxn) must not skip marking the
	// transaction finished and releasing its commit barrier hold, or
	// that hold leaks for the process's lifetime -- see finishLocked's
	// own doc comment. Do's safeRollback recovers the panic that skips
	// past this defer's own call site, but only after this defer has
	// already run during the panic unwind.
	defer t.finishLocked()
	var errs []error
	if t.blobTxn != nil {
		if err := safeProviderRollback(t.blobTxn); err != nil {
			errs = append(errs, fmt.Errorf("blob rollback: %w", err))
		}
	}
	if t.metadataTxn != nil {
		if err := safeProviderRollback(t.metadataTxn); err != nil {
			errs = append(errs, fmt.Errorf("metadata rollback: %w", err))
		}
	}
	return errors.Join(errs...)
}

// safeProviderRollback calls txn.Rollback(), recovering and converting to
// an error any panic it raises. Without this, a panic from the blob
// store's Rollback would skip the metadata store's Rollback entirely (and
// vice versa): rollback's own finishLocked defer would still mark the
// transaction finished, but finished is exactly what makes a later
// Rollback/Release call a no-op, so there would be no way to ever retry
// the provider whose Rollback never ran -- silently leaking its
// connection/transaction for the process's lifetime.
func safeProviderRollback(txn types.Txn) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panicked: %v", r)
		}
	}()
	return txn.Rollback()
}

// Release releases transaction resources. For read-only transactions, this
// releases locks and resources. For read-write transactions, this is equivalent
// to Rollback. Use this in defer statements for clean resource cleanup.
// Errors are logged but not returned, making this safe for deferred calls.
func (t *Txn) Release() {
	if err := t.Rollback(); err != nil {
		t.db.logger.Debug(
			"transaction release failed",
			"error", err,
			"read_write", t.readWrite,
		)
	}
}
