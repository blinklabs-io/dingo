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
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

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
					blob:     &mockBlobStore{},
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
