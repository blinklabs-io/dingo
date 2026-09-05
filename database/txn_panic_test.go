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
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, 1, metadataTxn.rollbackCount,
		"the metadata store's Rollback must still be attempted even "+
			"though the blob store's Rollback panicked")
	require.True(t, txn.finished,
		"the transaction must still be marked finished so it isn't "+
			"rolled back twice")
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
