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

package database

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	metadataSqlite "github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type blockingAfterCommitTxn struct {
	commitStarted chan struct{}
	allowCommit   chan struct{}
}

func (t *blockingAfterCommitTxn) Commit() error {
	close(t.commitStarted)
	<-t.allowCommit
	return nil
}

func (*blockingAfterCommitTxn) Rollback() error {
	return nil
}

// TestTxnAfterCommitRunsOnCommit verifies after-commit callbacks fire, once and
// in registration order, only after a read-write transaction commits.
func TestTxnAfterCommitRunsOnCommit(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	txn := db.Transaction(true)
	var order []int
	txn.AfterCommit(func() { order = append(order, 1) })
	txn.AfterCommit(func() { order = append(order, 2) })
	// A nil callback is ignored rather than panicking at commit time.
	txn.AfterCommit(nil)

	require.Empty(t, order, "callbacks must not fire before commit")
	require.NoError(t, txn.Commit())
	require.Equal(t, []int{1, 2}, order,
		"callbacks must run once, in registration order, after commit")
}

// TestTxnAfterCommitSkippedOnRollback verifies a rolled-back transaction never
// fires its after-commit callbacks, so callers may register side effects that
// must reflect committed state only.
func TestTxnAfterCommitSkippedOnRollback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	txn := db.Transaction(true)
	fired := false
	txn.AfterCommit(func() { fired = true })

	require.NoError(t, txn.Rollback())
	require.False(t, fired, "rollback must not fire after-commit callbacks")
}

// TestTxnAfterCommitSkippedOnReadOnly verifies committing a read-only
// transaction (which only releases resources) does not fire callbacks, since no
// durable commit occurred.
func TestTxnAfterCommitSkippedOnReadOnly(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)

	txn := db.Transaction(false)
	fired := false
	txn.AfterCommit(func() { fired = true })

	require.NoError(t, txn.Commit())
	require.False(t, fired,
		"read-only commit must not fire after-commit callbacks")
}

func TestTxnAfterCommitConcurrentWithCommitIsNotLost(t *testing.T) {
	t.Parallel()

	backend := &blockingAfterCommitTxn{
		commitStarted: make(chan struct{}),
		allowCommit:   make(chan struct{}),
	}
	txn := &Txn{
		metadataTxn: backend,
		readWrite:   true,
	}
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- txn.Commit()
	}()
	testutil.RequireReceive(
		t,
		backend.commitStarted,
		time.Second,
		"backend commit started",
	)

	callbackFired := make(chan struct{})
	registrationStarted := make(chan struct{})
	registrationDone := make(chan struct{})
	go func() {
		close(registrationStarted)
		txn.AfterCommit(func() { close(callbackFired) })
		close(registrationDone)
	}()

	testutil.RequireReceive(
		t,
		registrationStarted,
		time.Second,
		"concurrent callback registration started",
	)
	close(backend.allowCommit)
	require.NoError(t, testutil.RequireReceive(
		t,
		commitDone,
		time.Second,
		"transaction commit",
	))
	testutil.RequireReceive(
		t,
		registrationDone,
		time.Second,
		"concurrent callback registration",
	)
	testutil.RequireReceive(
		t,
		callbackFired,
		time.Second,
		"concurrently registered callback",
	)
}

func TestTxnAfterCommitRegisteredAfterCommitRuns(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	txn := db.Transaction(true)
	require.NoError(t, txn.Commit())

	fired := false
	txn.AfterCommit(func() { fired = true })
	require.True(t, fired,
		"registration after a successful commit must dispatch immediately")
}

func TestTxnAfterCommitCallbackCanRegisterCallback(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	txn := db.Transaction(true)
	var order []int
	txn.AfterCommit(func() {
		order = append(order, 1)
		txn.AfterCommit(func() { order = append(order, 2) })
	})

	require.NoError(t, txn.Commit())
	require.Equal(t, []int{1, 2}, order)
}

// TestTxnAfterCommitPanicIsContained verifies a panicking after-commit callback
// is contained: it neither aborts the remaining callbacks in the same drain nor
// permanently wedges the dispatch machinery for callbacks registered afterward.
// A panic that escaped dispatchAfterCommit would leave dispatching=true, so
// every later AfterCommit registration would silently append and return without
// ever draining.
func TestTxnAfterCommitPanicIsContained(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	txn := db.Transaction(true)

	var ran []string
	txn.AfterCommit(func() { ran = append(ran, "a") })
	txn.AfterCommit(func() {
		ran = append(ran, "b-panic")
		panic("boom")
	})
	txn.AfterCommit(func() { ran = append(ran, "c") })

	// Commit must not propagate the callback panic, and every callback in the
	// batch must run despite one of them panicking.
	require.NotPanics(t, func() { require.NoError(t, txn.Commit()) })
	require.Equal(t, []string{"a", "b-panic", "c"}, ran,
		"a panicking callback must not abort the other callbacks in the drain")

	// A registration after the panicking drain must still dispatch immediately,
	// proving dispatching was reset rather than left stuck true.
	postCommitRan := false
	txn.AfterCommit(func() { postCommitRan = true })
	require.True(t, postCommitRan,
		"registration after a panicking drain must still dispatch")
}

// timestampFailingBlobStore fails SetCommitTimestamp, which is what
// Commit's updateCommitTimestamp step calls before either store commits.
type timestampFailingBlobStore struct {
	*mockBlobStore
	err error
}

func (s *timestampFailingBlobStore) SetCommitTimestamp(
	int64,
	types.Txn,
) error {
	return s.err
}

// commitFailingMetadata is a metadata store whose write transactions fail
// to commit. Only the handful of methods Txn.Commit reaches are
// implemented; the embedded nil interface would panic on anything else,
// which is the intent — a case that starts touching a different method
// should fail loudly rather than silently exercise a different path.
type commitFailingMetadata struct {
	metadata.MetadataStore
	err error
}

func (m *commitFailingMetadata) Transaction(context.Context) types.Txn {
	return &commitFailingTxn{err: m.err}
}

func (m *commitFailingMetadata) ReadTransaction(context.Context) types.Txn {
	return &commitFailingTxn{}
}

func (m *commitFailingMetadata) SetCommitTimestamp(
	int64,
	types.Txn,
) error {
	return nil
}

type commitFailingTxn struct {
	err error
}

func (t *commitFailingTxn) Commit() error   { return t.err }
func (t *commitFailingTxn) Rollback() error { return nil }

// serializedBlobStore guards mockBlobStore's unsynchronized counters so
// the concurrent case below measures the commit barrier rather than
// reporting a data race in the test double. Sync is reimplemented instead
// of delegated: mockBlobStore.Sync also reads the first blob
// transaction's commit counter, which a different goroutine's Commit
// writes outside this mutex.
type serializedBlobStore struct {
	*mockBlobStore
	mu sync.Mutex
}

func (s *serializedBlobStore) NewTransaction(readWrite bool) types.Txn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mockBlobStore.NewTransaction(readWrite)
}

func (s *serializedBlobStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncCount++
	return s.syncErr
}

// barrierReaders reports the commit barrier's current shared-holder
// count. A terminal Txn that leaked its barrier hold leaves this above
// zero; one that released twice would already have panicked inside
// cancellableBarrier.RUnlock.
func barrierReaders(db *Database) int {
	db.commitBarrier.mu.Lock()
	defer db.commitBarrier.mu.Unlock()
	return db.commitBarrier.readers
}

// requireCommitBarrierFree proves no shared hold survives a terminal
// path: the reader count is back to zero and the exclusive side can
// actually be acquired. PauseCommits blocks forever against a leaked
// reader, so it runs in a goroutine bounded by RequireReceive rather than
// inline — a regression must fail fast instead of hanging the package.
func requireCommitBarrierFree(t *testing.T, db *Database) {
	t.Helper()
	require.Zero(
		t,
		barrierReaders(db),
		"terminal path leaked its commit barrier hold",
	)
	paused := make(chan func(), 1)
	go func() {
		paused <- db.PauseCommits()
	}()
	resume := testutil.RequireReceive(
		t,
		paused,
		5*time.Second,
		"PauseCommits must acquire the barrier after a terminal path",
	)
	resume()
}

// TestFailedBlobSyncDoesNotBlockTheNextWriter is the regression this
// commit fixes. Commit's blob-sync failure path marked the transaction
// finished without releasing the commit barrier's shared side. Because
// the release only ever happened under that finished flag, the caller's
// deferred Rollback/Release was then a no-op, so the hold survived for
// the process's lifetime: the next PauseCommits (database/lifecycle's
// Snapshot, Restore, and Truncate all take it) waited forever for a
// reader that would never release, and writer preference then blocked
// every read-write Txn constructed behind it.
func TestFailedBlobSyncDoesNotBlockTheNextWriter(t *testing.T) {
	t.Parallel()

	syncErr := errors.New("fsync failed")
	store := &mockBlobStore{syncErr: syncErr}
	db := newSyncBarrierTestDB(t, store)

	txn := db.Transaction(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
	err := txn.Commit()
	require.ErrorIs(t, err, types.ErrPartialCommit)
	require.ErrorIs(t, err, syncErr)
	// What a caller's defer does. It cannot repair the leak: rollback
	// returns early on an already-finished transaction.
	txn.Release()

	requireCommitBarrierFree(t, db)

	// The next writer must still be able to open and commit.
	store.syncErr = nil
	next := make(chan *Txn, 1)
	go func() {
		next <- db.Transaction(true)
	}()
	nextTxn := testutil.RequireReceive(
		t,
		next,
		5*time.Second,
		"a new read-write Txn must open after a failed blob sync",
	)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), nextTxn))
	require.NoError(t, nextTxn.Commit())
	requireCommitBarrierFree(t, db)
}

// TestTerminalTxnPathsReleaseCommitBarrierExactlyOnce audits every way a
// Txn can end, not just the blob-sync failure that motivated the fix.
// Each case runs its terminal path and then calls Rollback and Release
// again: a path that releases the barrier twice drives the reader count
// negative, which cancellableBarrier.RUnlock panics on, so the "exactly
// once" invariant is checked in both directions.
func TestTerminalTxnPathsReleaseCommitBarrierExactlyOnce(t *testing.T) {
	t.Parallel()

	injected := errors.New("injected failure")

	for _, tc := range []struct {
		name string
		// newDB builds the database for the case.
		newDB func(t *testing.T) *Database
		// run performs the terminal path and returns the Txn it ended,
		// so the shared double-release check below can re-terminate it.
		run func(t *testing.T, db *Database) *Txn
	}{
		{
			name: "successful combined commit",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
				require.NoError(t, txn.Commit())
				return txn
			},
		},
		{
			name: "successful metadata-only commit",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.MetadataTxn(true)
				require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
				require.NoError(t, txn.Commit())
				return txn
			},
		},
		{
			name: "read-only commit",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(false)
				require.NoError(t, txn.Commit())
				return txn
			},
		},
		{
			name: "rollback",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
				require.NoError(t, txn.Rollback())
				return txn
			},
		},
		{
			name: "release",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				txn.Release()
				return txn
			},
		},
		{
			name: "commit timestamp failure",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &timestampFailingBlobStore{
					mockBlobStore: &mockBlobStore{},
					err:           injected,
				})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
				err := txn.Commit()
				require.ErrorIs(t, err, injected)
				require.ErrorContains(
					t,
					err,
					"failed to update commit timestamp",
				)
				return txn
			},
		},
		{
			name: "blob commit failure",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{
					commitErrs: []error{injected},
				})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
				err := txn.Commit()
				require.ErrorIs(t, err, injected)
				require.ErrorContains(t, err, "blob commit failed")
				return txn
			},
		},
		{
			name: "blob sync failure",
			newDB: func(t *testing.T) *Database {
				return newSyncBarrierTestDB(t, &mockBlobStore{
					syncErr: injected,
				})
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
				err := txn.Commit()
				require.ErrorIs(t, err, types.ErrPartialCommit)
				require.ErrorIs(t, err, injected)
				return txn
			},
		},
		{
			name: "partial commit: metadata commit failure",
			newDB: func(t *testing.T) *Database {
				logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
				return &Database{
					blobRef:  newBlobStoreRef(&mockBlobStore{}),
					metadata: &commitFailingMetadata{err: injected},
					logger:   logger,
					config:   &Config{Logger: logger},
				}
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				err := txn.Commit()
				require.ErrorIs(t, err, types.ErrPartialCommit)
				require.ErrorIs(t, err, injected)
				return txn
			},
		},
		{
			name: "metadata-only commit failure",
			newDB: func(t *testing.T) *Database {
				logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
				return &Database{
					metadata: &commitFailingMetadata{err: injected},
					logger:   logger,
					config:   &Config{Logger: logger},
				}
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.MetadataTxn(true)
				err := txn.Commit()
				require.ErrorIs(t, err, injected)
				require.ErrorContains(t, err, "metadata commit failed")
				require.NotErrorIs(
					t,
					err,
					types.ErrPartialCommit,
					"no blob was committed, so this is not partial",
				)
				return txn
			},
		},
		{
			name: "no store available",
			newDB: func(t *testing.T) *Database {
				logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
				return &Database{
					logger: logger,
					config: &Config{Logger: logger},
				}
			},
			run: func(t *testing.T, db *Database) *Txn {
				txn := db.Transaction(true)
				require.ErrorIs(
					t,
					txn.Commit(),
					types.ErrNoStoreAvailable,
				)
				return txn
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := tc.newDB(t)
			txn := tc.run(t, db)
			requireCommitBarrierFree(t, db)
			// Re-terminating an already finished Txn must not release a
			// second time. RUnlock panics on an unmatched release, so a
			// double release fails this test rather than silently
			// unblocking a live PauseCommits elsewhere.
			require.NoError(t, txn.Rollback())
			txn.Release()
			requireCommitBarrierFree(t, db)
		})
	}
}

// TestCommitBarrierSurvivesConcurrentFailingCommits pins the invariant
// under concurrency, which is where a miscounted barrier actually bites:
// the reader count must return to exactly zero after a batch of
// read-write transactions that each fail their blob sync, so a
// PauseCommits issued afterwards still acquires. A leak leaves the count
// high; an over-release panics in RUnlock.
func TestCommitBarrierSurvivesConcurrentFailingCommits(t *testing.T) {
	t.Parallel()

	store := &serializedBlobStore{
		mockBlobStore: &mockBlobStore{syncErr: errors.New("fsync failed")},
	}
	db := newSyncBarrierTestDB(t, store)

	const writers = 8
	done := make(chan struct{}, writers)
	for range writers {
		go func() {
			defer func() { done <- struct{}{} }()
			txn := db.Transaction(true)
			defer txn.Release()
			// The commit result is not asserted here: only one writer at
			// a time holds the metadata write connection, so the others
			// can fail earlier than the blob sync. Either way the
			// barrier accounting must balance.
			_ = txn.Commit()
		}()
	}
	for range writers {
		testutil.RequireReceive(
			t,
			done,
			30*time.Second,
			"every writer must finish its transaction",
		)
	}

	requireCommitBarrierFree(t, db)
}

// TestOnFinishFiresOnEveryTerminalPath pins the property that separates
// OnFinish from AfterCommit: a hold taken for a transaction's lifetime is
// released whichever way that transaction ends. AfterCommit fires only on a
// durable commit, so releasing from it strands the hold for the life of the
// process on every rollback.
func TestOnFinishFiresOnEveryTerminalPath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		readWrite bool
		finish    func(*Txn)
	}{
		{"commit", true, func(txn *Txn) { require.NoError(t, txn.Commit()) }},
		{"rollback", true, func(txn *Txn) { require.NoError(t, txn.Rollback()) }},
		{"release", true, func(txn *Txn) { txn.Release() }},
		{
			// Commit on a read-only transaction rolls back rather than
			// committing, and still has to report the transaction over.
			"read-only commit",
			false,
			func(txn *Txn) { require.NoError(t, txn.Commit()) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			txn := db.BlobTxn(tc.readWrite)
			var calls int
			var mu sync.Mutex
			txn.OnFinish(func() {
				mu.Lock()
				defer mu.Unlock()
				calls++
			})
			mu.Lock()
			require.Equal(
				t,
				0,
				calls,
				"callback fired before the transaction ended",
			)
			mu.Unlock()
			tc.finish(txn)
			mu.Lock()
			require.Equal(t, 1, calls, "callback did not fire exactly once")
			mu.Unlock()
			// A repeat terminal call is a no-op on the transaction and must
			// not re-fire the callback.
			require.NoError(t, txn.Rollback())
			mu.Lock()
			require.Equal(
				t,
				1,
				calls,
				"callback re-fired on a finished transaction",
			)
			mu.Unlock()
		})
	}
}

// TestOnFinishAfterFinishRunsImmediately pins that an acquire-then-register
// sequence cannot lose its release to a transaction that concluded in between.
func TestOnFinishAfterFinishRunsImmediately(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.Commit())
	fired := false
	txn.OnFinish(func() { fired = true })
	require.True(
		t,
		fired,
		"registration on a finished transaction dropped the callback",
	)
}

// TestOnFinishRunsInRegistrationOrderAndContainsPanics pins that one caller's
// panicking callback cannot strand another caller's hold.
func TestOnFinishRunsInRegistrationOrderAndContainsPanics(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	var order []string
	txn.OnFinish(func() { order = append(order, "first") })
	txn.OnFinish(func() { panic("callback boom") })
	txn.OnFinish(func() { order = append(order, "third") })
	require.NotPanics(t, func() { require.NoError(t, txn.Rollback()) })
	require.Equal(t, []string{"first", "third"}, order)
}

// TestOnFinishCallbackMayTakeLocks pins that callbacks run without the
// transaction lock held, which is what lets them release a lock of their own.
func TestOnFinishCallbackMayTakeLocks(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	var held sync.Mutex
	held.Lock()
	txn.OnFinish(func() {
		// Re-entering the transaction from a callback would deadlock if the
		// dispatch still held txn.lock.
		txn.OnFinish(func() { held.Unlock() })
	})
	require.NoError(t, txn.Commit())
	require.True(t, held.TryLock(), "nested callback never ran")
}

// TestOnFinishNestedRegistrationRunsImmediately documents the intentional
// exception to pre-finish registration order: once finished is set, a nested
// registration runs inline rather than joining callbacks already dequeued.
func TestOnFinishNestedRegistrationRunsImmediately(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	txn := db.BlobTxn(true)
	var order []string
	txn.OnFinish(func() {
		order = append(order, "first")
		txn.OnFinish(func() { order = append(order, "nested") })
	})
	txn.OnFinish(func() { order = append(order, "second") })
	require.NoError(t, txn.Rollback())
	require.Equal(t, []string{"first", "nested", "second"}, order)
}

type panicCommitTxn struct {
	rollbackCount int
}

func (*panicCommitTxn) Commit() error {
	panic("commit panic")
}

func (t *panicCommitTxn) Rollback() error {
	t.rollbackCount++
	return nil
}

// TestTxnDoCommitPanicReleasesLockAndBarrier proves that a panic raised by an
// underlying store's Commit does not strand Txn.lock or the shared commit
// barrier. Txn.Do must finish its recovery rollback and return an error
// wrapping ErrTxnPanic instead of letting the panic escape; lifecycle code
// must then be able to pause commits, and a writer queued behind that pause
// must open promptly after resume.
func TestTxnDoCommitPanicReleasesLockAndBarrier(t *testing.T) {
	t.Parallel()

	db := &Database{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	backend := &panicCommitTxn{}
	txn := &Txn{
		db:          db,
		metadataTxn: backend,
		readWrite:   true,
	}
	acquireCommitBarrier(txn, true)

	err := txn.Do(func(*Txn) error { return nil })
	require.ErrorIs(t, err, ErrTxnPanic,
		"Do must convert the panic into an ErrTxnPanic-wrapped error "+
			"rather than letting it escape")
	require.ErrorContains(t, err, "commit panic")
	require.Equal(t, 1, backend.rollbackCount,
		"Txn.Do must roll back the underlying store before returning")

	paused := make(chan func(), 1)
	go func() { paused <- db.PauseCommits() }()
	resume := testutil.RequireReceive(
		t,
		paused,
		time.Second,
		"PauseCommits must acquire after panic cleanup",
	)
	var resumeOnce sync.Once
	safeResume := func() { resumeOnce.Do(resume) }

	writerOpened := make(chan struct{})
	writerRelease := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		next := &Txn{db: db, readWrite: true}
		acquireCommitBarrier(next, true)
		close(writerOpened)
		<-writerRelease
		_ = next.Rollback()
	}()
	var writerReleaseOnce sync.Once
	safeReleaseWriter := func() {
		writerReleaseOnce.Do(func() { close(writerRelease) })
	}
	t.Cleanup(func() {
		safeResume()
		safeReleaseWriter()
		select {
		case <-writerDone:
		case <-time.After(time.Second):
			t.Errorf("timeout cleaning up queued writer")
		}
	})
	testutil.RequireNoReceive(
		t,
		writerOpened,
		100*time.Millisecond,
		"the pause must still exclude a new writer",
	)
	safeResume()
	testutil.RequireReceive(
		t,
		writerOpened,
		time.Second,
		"the next writer must open after resume",
	)
	safeReleaseWriter()
	testutil.RequireReceive(
		t,
		writerDone,
		time.Second,
		"the next writer must roll back during cleanup",
	)
}

type panicCommitAndRollbackTxn struct{}

func (*panicCommitAndRollbackTxn) Commit() error {
	panic("commit panic")
}

func (*panicCommitAndRollbackTxn) Rollback() error {
	panic("rollback panic")
}

// TestTxnDoCommitAndRollbackBothPanicReturnsErrorAndReleasesBarrier proves
// Do never re-panics even when its own recovery rollback panics too: Do's
// top-level recover already consumed the Commit panic, so a second,
// unrelated panic from t.Rollback() (or the underlying store's Rollback it
// calls) would otherwise propagate straight out of that already-executing
// deferred function -- the outermost frame in Do -- and crash the
// goroutine instead of returning. It also proves the commit barrier is
// still released: rollback() releases it via a defer (finishLocked), which
// runs during the panic unwind through rollback() regardless of whether
// the underlying store's Rollback call panicked partway through.
func TestTxnDoCommitAndRollbackBothPanicReturnsErrorAndReleasesBarrier(
	t *testing.T,
) {
	t.Parallel()

	db := &Database{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	txn := &Txn{
		db:          db,
		metadataTxn: &panicCommitAndRollbackTxn{},
		readWrite:   true,
	}
	acquireCommitBarrier(txn, true)

	var err error
	require.NotPanics(t, func() {
		err = txn.Do(func(*Txn) error { return nil })
	})
	require.ErrorIs(t, err, ErrTxnPanic)
	require.ErrorContains(t, err, "commit panic")
	require.ErrorContains(t, err, "rollback panic")

	// The barrier must not be leaked: PauseCommits must still be able to
	// acquire it, and a writer queued behind that pause must open
	// promptly after resume, exactly as in
	// TestTxnDoCommitPanicReleasesLockAndBarrier above.
	paused := make(chan func(), 1)
	go func() { paused <- db.PauseCommits() }()
	resume := testutil.RequireReceive(
		t,
		paused,
		time.Second,
		"PauseCommits must acquire after panic cleanup",
	)

	writerOpened := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		next := &Txn{db: db, readWrite: true}
		acquireCommitBarrier(next, true)
		close(writerOpened)
		_ = next.Rollback()
	}()
	testutil.RequireNoReceive(
		t,
		writerOpened,
		100*time.Millisecond,
		"the pause must still exclude a new writer",
	)
	resume()
	testutil.RequireReceive(
		t,
		writerOpened,
		time.Second,
		"the next writer must open after resume",
	)
	testutil.RequireReceive(
		t,
		writerDone,
		time.Second,
		"the next writer must roll back during cleanup",
	)
}

type panicRollbackTxn struct{}

func (*panicRollbackTxn) Commit() error   { return nil }
func (*panicRollbackTxn) Rollback() error { panic("blob rollback panic") }

type trackingRollbackTxn struct {
	rollbackCount int
}

func (*trackingRollbackTxn) Commit() error { return nil }
func (t *trackingRollbackTxn) Rollback() error {
	t.rollbackCount++
	return nil
}

// TestTxnRollbackAttemptsBothStoresWhenOnePanics proves a panic from one
// provider's Rollback (blobTxn here) does not prevent the other
// (metadataTxn) from being rolled back too. Without this, finished would
// still end up true (finishLocked's own defer runs during the panic
// unwind), but finished is exactly what makes a later Rollback/Release
// call a no-op -- so metadataTxn's own Rollback would never run at all,
// silently leaking its connection/transaction for the process's lifetime
// with no way to ever retry it.
func TestTxnRollbackAttemptsBothStoresWhenOnePanics(t *testing.T) {
	t.Parallel()

	metadataTxn := &trackingRollbackTxn{}
	txn := &Txn{
		db: &Database{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		blobTxn:     &panicRollbackTxn{},
		metadataTxn: metadataTxn,
		readWrite:   true,
	}

	var err error
	require.NotPanics(t, func() {
		err = txn.Rollback()
	})
	require.ErrorContains(t, err, "blob rollback")
	require.ErrorContains(t, err, "panicked")
	require.ErrorIs(t, err, ErrTxnPanic,
		"a panicking provider Rollback must be identifiable via "+
			"errors.Is(err, ErrTxnPanic) like every other path in the "+
			"panic contract, not just the ones Do's own recover catches")
	require.Equal(t, 1, metadataTxn.rollbackCount,
		"the metadata store's Rollback must still be attempted even "+
			"though the blob store's Rollback panicked")
	require.True(t, txn.finished,
		"the transaction must still be marked finished so it isn't "+
			"rolled back twice")
}

// TestTxnDoOrdinaryErrorThenRollbackPanicIsIdentifiableAsTxnPanic proves the
// panic contract holds on Do's *ordinary*-error path too, not just its own
// recovery path: when fn returns a normal error and the resulting Rollback
// then panics, the error Do returns must still satisfy
// errors.Is(err, ErrTxnPanic), since a real panic did occur during
// cleanup -- even though Do's own top-level recover never fires for this
// path (only fn's ordinary error return does).
func TestTxnDoOrdinaryErrorThenRollbackPanicIsIdentifiableAsTxnPanic(
	t *testing.T,
) {
	t.Parallel()

	txn := &Txn{
		db: &Database{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		metadataTxn: &panicCommitAndRollbackTxn{},
		readWrite:   true,
	}

	fnErr := errors.New("ordinary function error")
	var err error
	require.NotPanics(t, func() {
		err = txn.Do(func(*Txn) error { return fnErr })
	})
	require.ErrorIs(t, err, fnErr)
	require.ErrorIs(t, err, ErrTxnPanic)
	require.ErrorContains(t, err, "rollback panic")
}

// panicFnTxn is a no-op metadata Txn used where the panic under test comes
// from the function passed to Do rather than from Commit.
type panicFnTxn struct{}

func (*panicFnTxn) Commit() error   { return nil }
func (*panicFnTxn) Rollback() error { return nil }

// TestTxnDoFunctionPanicWrapsNonStringValue proves the ErrTxnPanic
// conversion handles an arbitrary recovered value, not just a string: a
// panic(err) (a common Go pattern) must still produce an error that
// identifies as ErrTxnPanic and preserves the original error's text.
func TestTxnDoFunctionPanicWrapsNonStringValue(t *testing.T) {
	t.Parallel()

	db := &Database{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	txn := &Txn{db: db, metadataTxn: &panicFnTxn{}, readWrite: true}

	boom := errors.New("boom")
	err := txn.Do(func(*Txn) error {
		panic(boom)
	})
	require.ErrorIs(t, err, ErrTxnPanic)
	require.ErrorContains(t, err, "boom")
}

// TestTxnDoOrdinaryErrorIsNotWrappedAsPanic proves Do only attaches
// ErrTxnPanic to a recovered panic, never to an ordinary error the function
// returns deliberately -- the two failure modes stay distinguishable via
// errors.Is.
func TestTxnDoOrdinaryErrorIsNotWrappedAsPanic(t *testing.T) {
	t.Parallel()

	db := &Database{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	txn := &Txn{db: db, metadataTxn: &panicFnTxn{}, readWrite: true}

	ordinary := errors.New("ordinary failure")
	err := txn.Do(func(*Txn) error { return ordinary })
	require.ErrorIs(t, err, ordinary)
	require.False(t, errors.Is(err, ErrTxnPanic),
		"an ordinary returned error must not be identified as a panic")
}

// unsyncedBlobStore drops the durability barrier so the benchmarks below can
// price it against an otherwise identical on-disk store.
type unsyncedBlobStore struct {
	blob.BlobStore
}

func (unsyncedBlobStore) Sync() error { return nil }

func benchCombinedCommitDB(b *testing.B, sync bool) *Database {
	b.Helper()
	db, err := newTestDatabase(b, &Config{
		DataDir: b.TempDir(),
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	if err != nil {
		b.Fatalf("new test database: %v", err)
	}
	if !sync {
		db.SetBlobStore(unsyncedBlobStore{db.Blob()})
	}
	b.Cleanup(func() {
		if err := db.Close(); err != nil {
			b.Fatalf("close database: %v", err)
		}
	})
	return db
}

// benchmarkCombinedCommit measures a blob+metadata commit carrying a
// mainnet-sized block body plus a tip advance, which is the shape of every
// applied block. Compare WithSync against WithoutSync to price the cross-store
// durability barrier: SQLite runs synchronous=NORMAL and so does not fsync per
// commit, making the Badger sync the only fsync on this path.
func benchmarkCombinedCommit(b *testing.B, sync bool) {
	db := benchCombinedCommitDB(b, sync)
	blobStore := db.Blob()
	// Roughly a mainnet block body, so the sync has representative dirty data
	// to flush rather than a bare tip row.
	body := bytes.Repeat([]byte{0x5a}, 16*1024)

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		slot := uint64(i) + 1
		hash := bytes.Repeat([]byte{byte(i), byte(i >> 8)}, 16)
		txn := db.Transaction(true)
		if err := blobStore.SetBlock(
			txn.Blob(), slot, hash, body, slot, 6, slot, hash,
		); err != nil {
			b.Fatalf("set block: %v", err)
		}
		if err := db.SetTip(ochainsync.Tip{
			Point:       ocommon.Point{Slot: slot, Hash: hash},
			BlockNumber: slot,
		}, txn); err != nil {
			b.Fatalf("set tip: %v", err)
		}
		if err := txn.Commit(); err != nil {
			b.Fatalf("commit: %v", err)
		}
	}
}

func BenchmarkCombinedCommitWithSync(b *testing.B) {
	benchmarkCombinedCommit(b, true)
}

func BenchmarkCombinedCommitWithoutSync(b *testing.B) {
	benchmarkCombinedCommit(b, false)
}

// BenchmarkBlobSync isolates the barrier itself on an on-disk Badger store.
func BenchmarkBlobSync(b *testing.B) {
	db := benchCombinedCommitDB(b, true)
	blobStore := db.Blob()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := blobStore.Sync(); err != nil {
			b.Fatalf("sync: %v", err)
		}
	}
}

// newSyncBarrierTestDB builds a Database pairing the given blob store with a
// real in-memory sqlite metadata store, which is the combination the
// combined-commit durability barrier applies to. The parameter is the
// interface so a case can inject a store whose Sync or SetCommitTimestamp
// fails without changing the shared mockBlobStore.
func newSyncBarrierTestDB(
	t *testing.T,
	store blob.BlobStore,
) *Database {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	sqliteStore, err := metadataSqlite.NewSQLStore(
		metadataSqlite.Config{},
		metadata.ProviderDependencies{Logger: logger},
	)
	require.NoError(t, err)
	require.NoError(t, sqliteStore.Start(context.Background()))
	db := &Database{
		blobRef:  newBlobStoreRef(store),
		metadata: sqliteStore,
		logger:   logger,
		config:   &Config{Logger: logger},
	}
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return db
}

func syncBarrierTestTip() ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: 193600907,
			Hash: bytes.Repeat([]byte{0x41}, 32),
		},
		BlockNumber: 12345,
	}
}

// TestCommitSyncsBlobAfterBlobCommit covers the cross-store durability
// ordering. Committing the blob transaction before the metadata transaction
// only keeps the blob store ahead of the metadata tip in memory: the metadata
// store fsyncs per commit while the blob store buffers, so without a sync
// barrier an unclean shutdown leaves a durable metadata tip referencing blocks
// the blob store discarded. Startup reconciliation can trim a blob store that
// is ahead but cannot rebuild blocks missing beneath the ledger tip, so the
// barrier must run on every combined commit, after the blob commit.
func TestCommitSyncsBlobAfterBlobCommit(t *testing.T) {
	t.Parallel()

	store := &mockBlobStore{}
	db := newSyncBarrierTestDB(t, store)

	txn := db.Transaction(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
	require.NoError(t, txn.Commit())

	require.Equal(
		t,
		1,
		store.syncCount,
		"combined commit should sync the blob store exactly once",
	)
	require.Equal(
		t,
		1,
		store.syncAtBlobCommitCount,
		"sync must run after the blob commit, so the commit it makes durable is included",
	)

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, syncBarrierTestTip().Point.Slot, tip.Point.Slot)
}

// TestCommitDoesNotSyncMetadataOnlyTransaction pins that the fsync is scoped to
// transactions that span both stores. A metadata-only transaction has no blob
// write whose durability the metadata commit could outrun, so paying an fsync
// for it would be pure cost.
func TestCommitDoesNotSyncMetadataOnlyTransaction(t *testing.T) {
	t.Parallel()

	store := &mockBlobStore{}
	db := newSyncBarrierTestDB(t, store)

	txn := db.MetadataTxn(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))
	require.NoError(t, txn.Commit())

	require.Zero(
		t,
		store.syncCount,
		"metadata-only commit should not sync the blob store",
	)
}

// TestCommitFailedBlobSyncDoesNotCommitMetadata is the guarantee that makes the
// barrier worth anything: if the blob store cannot be made durable, the
// metadata tip must not advance past it. The blob transaction is already
// committed and carries the new commit timestamp at that point, which is the
// same inconsistency a failed metadata commit leaves, so it is reported as a
// partial commit to drive the existing blob-trimming recovery.
func TestCommitFailedBlobSyncDoesNotCommitMetadata(t *testing.T) {
	t.Parallel()

	syncErr := errors.New("fsync failed")
	store := &mockBlobStore{syncErr: syncErr}
	db := newSyncBarrierTestDB(t, store)

	txn := db.Transaction(true)
	require.NoError(t, db.SetTip(syncBarrierTestTip(), txn))

	err := txn.Commit()
	require.Error(t, err)
	require.ErrorIs(t, err, syncErr)
	require.ErrorContains(t, err, "blob sync failed")
	require.ErrorIs(
		t,
		err,
		types.ErrPartialCommit,
		"a committed blob with an un-synced tip should route through partial-commit recovery",
	)

	tip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.NotEqual(
		t,
		syncBarrierTestTip().Point.Slot,
		tip.Point.Slot,
		"metadata tip must not advance when the blob store could not be synced",
	)
}
