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
	"runtime/debug"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
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
	// blobStore is the blob store this transaction opened blobTxn on, and
	// the store every blob operation inside the transaction must use. It
	// is set once at construction and never cleared, so it stays readable
	// (and correct) even after the transaction is finished. blobPin is the
	// pin that keeps that store from being drained out from under the
	// transaction; it is guarded by lock and cleared by
	// releaseBlobPinLocked. See blob_store.go.
	blobStore   blob.BlobStore
	blobPin     *blobStoreRef
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
	t.releaseBlobPinLocked()
}

// releaseBlobPinLocked drops this transaction's pin on the blob store it
// opened on, if still held. Like releaseCommitBarrierLocked it clears the
// field before releasing, so routing it through finishLocked from every
// terminal path releases exactly once rather than panicking on an unmatched
// WaitGroup.Done. t.blobStore is deliberately left set: BlobStore must keep
// answering after the transaction finishes. Callers must hold t.lock.
func (t *Txn) releaseBlobPinLocked() {
	if t.blobPin == nil {
		return
	}
	pin := t.blobPin
	t.blobPin = nil
	pin.release()
}

func NewTxn(db *Database, readWrite bool) *Txn {
	return NewTxnContext(context.Background(), db, readWrite)
}

// NewTxnContext creates a coordinated transaction whose metadata operations
// are canceled with ctx. Blob operations do not accept contexts, so callers
// doing long mixed-store scans must also check ctx between blob reads.
func NewTxnContext(ctx context.Context, db *Database, readWrite bool) *Txn {
	if ctx == nil {
		ctx = context.Background()
	}
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t, db.Metadata() != nil)
	pinBlobStoreForTxn(t, db)
	if bs := t.blobStore; bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	if ms := db.Metadata(); ms != nil {
		// Use the read connection pool for read-only transactions to
		// avoid contending with the SQLite write connection. This
		// prevents chainsync FindIntersect and snapshot calculations
		// from blocking on concurrent block processing.
		//
		if readWrite {
			t.metadataTxn = ms.Transaction(ctx)
		} else {
			t.metadataTxn = ms.ReadTransaction(ctx)
		}
		if t.metadataTxn == nil {
			db.logger.Warn(
				"metadata transaction is nil; callers must nil-check txn.Metadata()",
			)
		}
	}
	return t
}

// NewReadSnapshotContext creates a coordinated read transaction and returns
// the metadata tip that anchors it. PauseCommitsContext brackets construction
// with both the logical destructive-transition barrier and the physical commit
// barrier, so neither a multi-transaction rollback nor a combined write can
// change blob data between opening the metadata and blob views. Both holds are
// released as soon as the views are fixed; they are not held for the lifetime
// of the read.
func NewReadSnapshotContext(
	ctx context.Context,
	db *Database,
) (*Txn, ochainsync.Tip, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resume, err := db.PauseCommitsContext(ctx)
	if err != nil {
		return nil, ochainsync.Tip{}, fmt.Errorf(
			"pause commits for read snapshot: %w",
			err,
		)
	}
	defer resume()

	t := &Txn{db: db}
	var tip ochainsync.Tip
	if ms := db.Metadata(); ms != nil {
		t.metadataTxn = ms.ReadTransaction(ctx)
		if t.metadataTxn == nil {
			return nil, tip, types.ErrNilTxn
		}
		var err error
		tip, err = ms.GetTip(t.metadataTxn)
		if err != nil {
			_ = t.Rollback()
			return nil, tip, fmt.Errorf(
				"anchor metadata read snapshot: %w",
				err,
			)
		}
	}
	if bs := db.Blob(); bs != nil {
		t.blobTxn = bs.NewTransaction(false)
	}
	return t, tip, nil
}

func NewBlobOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t, false)
	pinBlobStoreForTxn(t, db)
	if bs := t.blobStore; bs != nil {
		t.blobTxn = bs.NewTransaction(readWrite)
	}
	return t
}

func NewMetadataOnlyTxn(db *Database, readWrite bool) *Txn {
	t := &Txn{db: db, readWrite: readWrite}
	acquireCommitBarrier(t, db.Metadata() != nil)
	// A metadata-only transaction opens no blob transaction, but it still
	// pins: BlobStore has to answer for it too, because helpers that take a
	// *Txn (recordBlobOrphansOnCommit and the blob-delete paths it counts
	// for) are reached with whichever transaction the caller happens to
	// hold.
	pinBlobStoreForTxn(t, db)
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

// pinBlobStoreForTxn pins db's currently installed blob store for t's
// lifetime and records which store that was. Every constructor calls it
// before opening anything, so a transaction's blob work and the store that
// work runs against are chosen together, once — re-reading Database.Blob
// later could hand back a different store than the one blobTxn belongs to.
func pinBlobStoreForTxn(t *Txn, db *Database) {
	if db == nil {
		return
	}
	t.blobPin = db.pinBlobStore()
	t.blobStore = t.blobPin.blobStore()
}

func (t *Txn) DB() *Database {
	return t.db
}

// BlobStore returns the blob store this transaction was opened on, which is
// the store its Blob transaction handle belongs to and the one every blob
// operation in the transaction must use. It is stable for the transaction's
// lifetime even if the database's installed store is replaced meanwhile, and
// it is nil when no blob store was installed at construction.
func (t *Txn) BlobStore() blob.BlobStore {
	return t.blobStore
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
	dispatchAfterUnlock := false
	defer func() {
		// A provider may panic while updating the commit timestamp or
		// committing either store. Release both synchronization primitives
		// before that panic reaches Txn.Do's recovery handler, so its Rollback
		// can reacquire t.lock and finish the underlying transactions. Do not
		// mark the transaction finished here: the recovery rollback still owns
		// that transition. releaseCommitBarrierLocked is idempotent, so normal
		// terminal paths may already have released it through finishLocked.
		// The blob-store pin is deliberately not released here: that recovery
		// rollback still rolls back blobTxn, which belongs to the pinned
		// store, so dropping the pin early could let a concurrent
		// SetBlobStore's drain return -- and its caller close that store --
		// while the rollback is still running against it. finishLocked
		// releases it once the rollback concludes.
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
		if blobStore := t.blobStore; blobStore != nil && t.metadataTxn != nil {
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
	t.finishLocked()
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
