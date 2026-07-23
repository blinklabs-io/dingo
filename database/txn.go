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
	"errors"
	"fmt"
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
// for the lifetime of a read-write Txn, from construction through
// Commit/Rollback/Release. It must be taken before the underlying
// blob/metadata transactions are opened below, not just around the
// eventual commit: the metadata plugin's write connection pool is sized
// to exactly one connection (see sqlite.database.go), so an already-BEGUN
// but not-yet-committed transaction holds that one connection regardless
// of whether Commit() has been called yet. Database.PauseCommits (used by
// database/lifecycle.Snapshot around its blob+metadata backup calls)
// takes the exclusive side; if this barrier were only held during Commit,
// PauseCommits could acquire its lock while such a transaction sits
// BEGUN-but-uncommitted, and Snapshot's metadata backup (VACUUM INTO,
// which needs that same one connection) would then deadlock against it —
// the writer can't reach Commit's RLock to finish, and Snapshot can't
// release its Lock until the backup call returns.
func acquireCommitBarrier(t *Txn) {
	if t.readWrite && t.db != nil {
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

func NewTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t)
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	if ms := db.Metadata(); ms != nil {
		// Use the read connection pool for read-only transactions to
		// avoid contending with the SQLite write connection. This
		// prevents chainsync FindIntersect and snapshot calculations
		// from blocking on concurrent block processing.
		if readWrite {
			t.metadataTxn = ms.Transaction()
		} else {
			t.metadataTxn = ms.ReadTransaction()
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
	acquireCommitBarrier(t)
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	return t
}

func NewMetadataOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t)
	if ms := db.Metadata(); ms != nil {
		if readWrite {
			t.metadataTxn = ms.Transaction()
		} else {
			t.metadataTxn = ms.ReadTransaction()
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

// runAfterCommitCallback runs a single after-commit callback, recovering and
// logging any panic. Callbacks run detached from the transaction (after the
// durable commit, without the txn lock), so a panic must not escape the drain
// loop: an escaping panic would leave dispatching=true, silently stranding
// every callback registered afterward, and would drop the callbacks already
// dequeued for this drain. Panics are logged, not propagated.
func (t *Txn) runAfterCommitCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil && t.db != nil {
			t.db.logger.Error(
				"panic in after-commit callback",
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
		}
	}()
	fn()
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

// Do executes the specified function in the context of the transaction. Any errors returned will result
// in the transaction being rolled back. If the function panics, the transaction is rolled back and the
// panic is re-raised after logging.
func (t *Txn) Do(fn func(*Txn) error) error {
	defer func() {
		if r := recover(); r != nil {
			// Log the panic before attempting rollback
			t.db.logger.Error(
				"panic in transaction function, ensuring rollback",
				"panic", fmt.Sprintf("%v", r),
				"stack", string(debug.Stack()),
			)
			// Attempt rollback to ensure transaction is cleaned up
			if err := t.Rollback(); err != nil {
				t.db.logger.Error(
					"rollback failed after panic",
					"panic", fmt.Sprintf("%v", r),
					"rollback_error", err,
				)
			}
			// Re-panic to propagate the error up the stack
			panic(r)
		}
	}()

	if err := fn(t); err != nil {
		if err2 := t.Rollback(); err2 != nil {
			return fmt.Errorf(
				"rollback failed: %w: original error: %w",
				err2,
				err,
			)
		}
		return err
	}
	if err := t.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}
	return nil
}

func (t *Txn) Commit() error {
	t.lock.Lock()
	if t.finished {
		t.lock.Unlock()
		return nil
	}
	// Fail fast if neither store is available for a read-write transaction
	if t.readWrite && t.blobTxn == nil && t.metadataTxn == nil {
		t.finished = true
		t.releaseCommitBarrierLocked()
		t.lock.Unlock()
		return types.ErrNoStoreAvailable
	}
	// No need to commit for read-only, but we do want to free up resources
	if !t.readWrite {
		err := t.rollback()
		t.lock.Unlock()
		return err
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
			t.finished = true
			t.releaseCommitBarrierLocked()
			t.lock.Unlock()
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
			t.finished = true
			t.releaseCommitBarrierLocked()
			t.lock.Unlock()
			return fmt.Errorf("blob commit failed: %w", err)
		}
	}
	// Commit metadata transaction
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Commit(); err != nil {
			_ = t.metadataTxn.Rollback()
			t.finished = true
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
				t.releaseCommitBarrierLocked()
				t.lock.Unlock()
				return ret
			}
			t.releaseCommitBarrierLocked()
			t.lock.Unlock()
			return fmt.Errorf("metadata commit failed: %w", err)
		}
	}
	t.finished = true
	t.committed = true
	t.dispatching = true
	t.releaseCommitBarrierLocked()
	t.lock.Unlock()
	t.dispatchAfterCommit()
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
	var errs []error
	if t.blobTxn != nil {
		if err := t.blobTxn.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("blob rollback: %w", err))
		}
	}
	if t.metadataTxn != nil {
		if err := t.metadataTxn.Rollback(); err != nil {
			errs = append(errs, fmt.Errorf("metadata rollback: %w", err))
		}
	}
	t.finished = true
	t.releaseCommitBarrierLocked()
	return errors.Join(errs...)
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
