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
	"sync"
	"testing"
	"time"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// TestPauseCommitsBlocksNewReadWriteTxns proves the mechanism
// database/lifecycle.Snapshot relies on to keep its blob and metadata
// backup calls describing the same set of committed writes: a new
// read-write Txn must not even open while paused, and must open promptly
// once resumed.
//
// The barrier is held from Txn construction (not just around the eventual
// Commit) because the metadata plugin's write connection pool is sized to
// exactly one connection: an already-BEGUN-but-uncommitted transaction
// holds that connection regardless of whether Commit has been called. If
// the barrier only guarded Commit, PauseCommits could acquire its lock
// while such a transaction sat open, and Snapshot's metadata backup
// (VACUUM INTO, which needs that same connection) would deadlock against
// it — the open transaction can't reach Commit's release because
// PauseCommits already holds the exclusive side.
func TestPauseCommitsBlocksNewReadWriteTxns(t *testing.T) {
	db := openTestDB(t)

	resume := db.PauseCommits()
	// resume calls commitBarrier.Unlock with this acquisition's token,
	// which panics if called twice -- this test also calls it explicitly
	// below (to observe the paused-vs-resumed transition partway through,
	// unlike TestPauseCommitsAllowsConcurrentReads' single deferred
	// call), so sync.Once makes the two calls safe together: if an
	// assertion above the explicit call fails and this defer becomes the
	// only call to actually run, the barrier is still released instead of
	// leaking commit-paused for the rest of the test binary.
	var resumeOnce sync.Once
	safeResume := func() { resumeOnce.Do(resume) }
	defer safeResume()

	started := make(chan struct{})
	opened := make(chan *Txn, 1)
	go func() {
		close(started)
		opened <- db.Transaction(true)
	}()
	testutil.RequireReceive(t, started, time.Second, "goroutine must start")

	testutil.RequireNoReceive(
		t,
		opened,
		150*time.Millisecond,
		"a new read-write Txn must not open while commits are paused",
	)

	safeResume()

	txn := testutil.RequireReceive(
		t,
		opened,
		time.Second,
		"a new read-write Txn must open once resumed",
	)
	require.NoError(t, db.BlockCreate(testIndexedBlock(10, 1, 0x01), txn))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10},
		BlockNumber: 1,
	}, txn))
	require.NoError(t, txn.Commit())
}

// TestBlobOnlyTxnDoesNotBlockOnPendingPauseCommits guards against a real
// deadlock this package used to have: deleteUtxoBlobs and deleteTxBlobs
// open batched, blob-only Txns (NewBlobOnlyTxn) while the caller's outer
// read-write Txn (e.g. from TruncateAfterSlot/ls.rollback) is still open
// on the very same goroutine. Go's sync.RWMutex is not reentrant and is
// writer-preferring: once a PauseCommits Lock() call is queued, any
// further RLock() blocks too — including one from the same goroutine
// that already holds an RLock via the outer Txn. If NewBlobOnlyTxn also
// acquired the commit barrier, that nested RLock would block forever
// waiting for PauseCommits' Lock(), while PauseCommits itself waits
// forever for the outer Txn's RLock to release — a release the stuck
// goroutine can never reach. Blob-only Txns must therefore never
// acquire the barrier at all (they hold no metadata write connection and
// their commits never touch the commit timestamp PauseCommits protects,
// see acquireCommitBarrier's doc comment) — this test proves a
// blob-only Txn opens promptly even while a PauseCommits call is queued
// and blocked behind an unrelated open read-write Txn.
func TestBlobOnlyTxnDoesNotBlockOnPendingPauseCommits(t *testing.T) {
	db := openTestDB(t)

	outer := db.Transaction(true)
	defer outer.Rollback() //nolint:errcheck

	pauseStarted := make(chan struct{})
	paused := make(chan func(), 1)
	go func() {
		close(pauseStarted)
		paused <- db.PauseCommits()
	}()
	testutil.RequireReceive(t, pauseStarted, time.Second, "PauseCommits goroutine must start")
	testutil.RequireNoReceive(
		t,
		paused,
		150*time.Millisecond,
		"PauseCommits must block while the outer read-write Txn is open",
	)

	blobTxnOpened := make(chan *Txn, 1)
	go func() {
		blobTxnOpened <- NewBlobOnlyTxn(db, true)
	}()
	blobTxn := testutil.RequireReceive(
		t,
		blobTxnOpened,
		time.Second,
		"a blob-only Txn must open even while a PauseCommits call is queued",
	)
	require.NoError(t, blobTxn.Rollback())

	require.NoError(t, outer.Rollback())
	resume := testutil.RequireReceive(
		t,
		paused,
		time.Second,
		"PauseCommits must return once the outer read-write Txn releases",
	)
	resume()
}

// TestPauseCommitsContextReturnsPromptlyWhenCancelled guards against
// comment-31's original gap: PauseCommits blocks on a plain
// sync.RWMutex.Lock() with no way for a caller to abandon that wait, so a
// caller like lifecycle.Snapshot — which already accepts a ctx for its
// own work — had no way to give up on a Snapshot call stuck waiting
// behind a long-running write transaction. This holds an outer
// read-write Txn open (occupying the barrier's read side), then confirms
// PauseCommitsContext with an already-cancelled ctx returns ctx.Err()
// promptly instead of blocking, and that the barrier is not left
// permanently stuck once the outer Txn eventually releases — a
// subsequent PauseCommits() call must still succeed, proving the
// abandoned acquisition attempt underneath self-released rather than
// leaving the barrier stranded held with no one left to call resume.
func TestPauseCommitsContextReturnsPromptlyWhenCancelled(t *testing.T) {
	db := openTestDB(t)

	outer := db.Transaction(true)
	// Rollback() is idempotent (checks t.finished), so this defer is safe
	// alongside the explicit Rollback() call further down: if any
	// assertion between here and that explicit call fails, this defer
	// still releases outer's RLock on the commit barrier instead of
	// leaking it -- matching the pattern
	// TestBlobOnlyTxnDoesNotBlockOnPendingPauseCommits already uses for
	// the same reason.
	defer outer.Rollback() //nolint:errcheck

	ctx, cancel := context.WithCancel(context.Background())

	type pauseResult struct {
		resume func()
		err    error
	}
	resultCh := make(chan pauseResult, 1)
	go func() {
		resume, err := db.PauseCommitsContext(ctx)
		resultCh <- pauseResult{resume, err}
	}()

	// Confirm the call is genuinely blocked behind the outer read-write
	// Txn (not just racing to cancel before it ever reaches Lock) before
	// cancelling it.
	testutil.RequireNoReceive(
		t, resultCh, 150*time.Millisecond,
		"PauseCommitsContext must block while the outer read-write Txn is open",
	)

	cancel()

	result := testutil.RequireReceive(
		t, resultCh, time.Second,
		"PauseCommitsContext must return promptly once ctx is cancelled, "+
			"even while still blocked behind the outer read-write Txn",
	)
	require.ErrorIs(t, result.err, context.Canceled)
	require.Nil(t, result.resume)

	require.NoError(t, outer.Rollback())

	laterPaused := make(chan func(), 1)
	go func() {
		laterPaused <- db.PauseCommits()
	}()
	laterResume := testutil.RequireReceive(
		t, laterPaused, time.Second,
		"the barrier must not be left permanently held by the abandoned "+
			"acquisition attempt once the outer Txn releases",
	)
	laterResume()
}

// TestPauseCommitsContextCancellationDoesNotStallLaterTxns guards against
// comment-48's original bug: PauseCommitsContext's cancellation path used
// to just stop waiting on the result of a background goroutine's blocked
// sync.RWMutex.Lock() call — that Lock() call itself kept running (and
// stayed queued) regardless. Because sync.RWMutex gives writers
// preference, a queued Lock() blocks every subsequent RLock() (i.e. every
// new read-write Txn) even though the original PauseCommitsContext caller
// had already given up — so a cancelled snapshot attempt still stalled
// new Txns for as long as the pre-existing long-running write transaction
// it was waiting behind took to finish, exactly as if the cancellation
// had never happened.
//
// This holds the barrier's read side directly (db.commitBarrier.RLock),
// not via a real Transaction(true) Txn: the metadata plugin's write
// connection pool is sized to exactly one connection (see
// acquireCommitBarrier's doc comment), so a second concurrent
// Transaction(true) call would block on that pool regardless of the
// commitBarrier's own behavior, confounding the very thing this test
// needs to isolate. Simulating the reader directly keeps the check
// scoped to the barrier mechanism alone: a cancelled PauseCommitsContext
// must let a brand new RLock proceed promptly, even while the original,
// unrelated reader is still held. Only a barrier that fully withdraws
// the cancelled writer's claim (see cancellableBarrier) can pass this: a
// naive "just stop waiting" cancellation leaves the new RLock blocked
// until the original reader eventually releases.
func TestPauseCommitsContextCancellationDoesNotStallLaterTxns(t *testing.T) {
	db := openTestDB(t)

	db.commitBarrier.RLock()
	defer db.commitBarrier.RUnlock()

	ctx, cancel := context.WithCancel(context.Background())

	pauseResultCh := make(chan error, 1)
	go func() {
		_, err := db.PauseCommitsContext(ctx)
		pauseResultCh <- err
	}()

	testutil.RequireNoReceive(
		t, pauseResultCh, 150*time.Millisecond,
		"PauseCommitsContext must block while the simulated reader is held",
	)

	cancel()

	err := testutil.RequireReceive(
		t, pauseResultCh, time.Second,
		"PauseCommitsContext must return promptly once ctx is cancelled",
	)
	require.ErrorIs(t, err, context.Canceled)

	// The simulated reader is deliberately still held here -- this is the
	// discriminating check: a naive cancellation that leaves a phantom
	// queued writer behind would block this new RLock until the deferred
	// RUnlock above runs, not before it.
	newRLockDone := make(chan struct{})
	go func() {
		db.commitBarrier.RLock()
		close(newRLockDone)
	}()
	testutil.RequireReceive(
		t, newRLockDone, time.Second,
		"a new RLock must succeed promptly after the cancelled "+
			"PauseCommitsContext call, even while the original, unrelated "+
			"reader is still held",
	)
	db.commitBarrier.RUnlock()
}

// TestPauseCommitsContextSucceedsWhenUncontended verifies the ordinary,
// non-cancelled path still works exactly like PauseCommits when nothing
// is holding the barrier.
func TestPauseCommitsContextSucceedsWhenUncontended(t *testing.T) {
	db := openTestDB(t)
	resume, err := db.PauseCommitsContext(context.Background())
	require.NoError(t, err)
	require.NotNil(t, resume)
	resume()
}

// TestUnlockPanicsOnDoubleUnlock verifies that calling a resume func a
// second time panics instead of silently succeeding as a no-op, matching
// the one-shot contract PauseCommits/PauseCommitsContext document.
func TestUnlockPanicsOnDoubleUnlock(t *testing.T) {
	db := openTestDB(t)
	resume := db.PauseCommits()
	resume()
	require.Panics(t, resume, "calling resume a second time must panic, "+
		"not silently succeed as a no-op")
}

// TestUnlockRejectsStaleTokenAfterLaterAcquisition guards against
// comment-66's original bug: Unlock unconditionally cleared the
// barrier's held state regardless of which acquisition it was called
// for, so a duplicate/stale resume call -- one whose own Lock/
// LockContext call already released -- would silently release whatever
// DIFFERENT, unrelated PauseCommits acquisition happens to be current by
// the time the stale call runs, reopening that later holder's critical
// section to new read-write Txns before its own resume was ever called.
func TestUnlockRejectsStaleTokenAfterLaterAcquisition(t *testing.T) {
	db := openTestDB(t)

	resume1 := db.PauseCommits()
	resume1()

	resume2 := db.PauseCommits()
	var resume2Once sync.Once
	safeResume2 := func() { resume2Once.Do(resume2) }
	defer safeResume2()

	require.Panics(t, resume1, "a stale resume call must be rejected, not "+
		"silently release a later, unrelated holder's lock")

	done := make(chan *Txn, 1)
	go func() {
		done <- db.Transaction(true)
	}()
	testutil.RequireNoReceive(
		t, done, 150*time.Millisecond,
		"the later holder's lock must still be held after a stale "+
			"duplicate Unlock call was rejected",
	)

	safeResume2()
	txn := testutil.RequireReceive(
		t, done, time.Second,
		"a new read-write Txn must open once the later holder's own "+
			"resume is called",
	)
	require.NoError(t, txn.Rollback())
}

// TestPauseCommitsAllowsConcurrentReads verifies PauseCommits only blocks
// new read-write transactions, not reads — a paused snapshot must not
// stall unrelated read-only query traffic against the same database.
func TestPauseCommitsAllowsConcurrentReads(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, db.BlockCreate(testIndexedBlock(10, 1, 0x01), nil))

	resume := db.PauseCommits()
	defer resume()

	done := make(chan *Txn, 1)
	go func() {
		done <- db.Transaction(false)
	}()

	txn := testutil.RequireReceive(
		t,
		done,
		time.Second,
		"a read-only Txn must open while commits are paused",
	)
	require.NoError(t, txn.Rollback())
}
