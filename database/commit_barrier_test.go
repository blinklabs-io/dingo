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
	// resume is db.commitBarrier.Unlock directly, which panics if called
	// twice -- this test also calls it explicitly below (to observe the
	// paused-vs-resumed transition partway through, unlike
	// TestPauseCommitsAllowsConcurrentReads' single deferred call), so
	// sync.Once makes the two calls safe together: if an assertion above
	// the explicit call fails and this defer becomes the only call to
	// actually run, the barrier is still released instead of leaking
	// commit-paused for the rest of the test binary.
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
