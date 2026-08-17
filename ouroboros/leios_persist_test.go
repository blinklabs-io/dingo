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

package ouroboros

import (
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

// TestLeiosPersistAsyncCoalescesManifestThenComplete mirrors the backfiller's
// two-call pattern for one endorser block — a manifest-only store followed by a
// complete (manifest + txs) store — and verifies that after the async writer
// drains, the blob store holds the COMPLETE endorser block (manifest + all
// txs), i.e. the later complete write is not lost and the redundant manifest
// write is harmless. This exercises the asynchronous persistence path and the
// merged single-commit SetLeiosEB writer.
func TestLeiosPersistAsyncCoalescesManifestThenComplete(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)
	txsRaw := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
	}

	o := newTestOuroborosWithLeiosDB(t)

	// First the manifest only (no txs yet), as the backfiller's manifest fetch
	// does; then the complete block once its txs are fetched.
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, txsRaw))

	// Drain the async writer so all queued persistence has committed.
	o.StopLeiosPersistWriter()

	db := o.leiosDatabase()
	require.NotNil(t, db)

	slot, manifest, err := db.GetLeiosEBManifest(point.Hash)
	require.NoError(t, err)
	require.Equal(t, point.Slot, slot)
	require.Equal(t, []byte(blockRaw), manifest)

	gotTxs, err := db.GetLeiosEBTxs(point.Hash)
	require.NoError(t, err)
	require.Equal(t, txsRaw, gotTxs)
}

// TestLeiosPersistWriterStopIsSafeWithoutStart verifies StopLeiosPersistWriter
// is a no-op when no endorser block was ever fetched (the writer never started)
// and is safe to call more than once.
func TestLeiosPersistWriterStopIsSafeWithoutStart(t *testing.T) {
	o := newTestOuroborosWithLeiosDB(t)
	require.NotPanics(t, func() {
		o.StopLeiosPersistWriter()
		o.StopLeiosPersistWriter()
	})
}

// TestLeiosPersistStopDrainTimesOut verifies that the shutdown drain wait is
// bounded: if the writer's drain is stuck (e.g. the blob store hangs inside
// SetLeiosEB, so leiosPersistDone is never closed), stopLeiosPersistWriter
// returns after the drain timeout instead of blocking graceful shutdown
// forever, and still closes the stop channel so the writer can exit later.
func TestLeiosPersistStopDrainTimesOut(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	// Simulate a started writer whose drain never completes.
	o.leiosPersistStarted.Store(true)
	o.leiosPersistStop = make(chan struct{})
	o.leiosPersistDone = make(chan struct{}) // deliberately never closed

	returned := make(chan struct{})
	var drained bool
	go func() {
		drained = o.stopLeiosPersistWriter(50 * time.Millisecond)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("stopLeiosPersistWriter hung past the bounded drain timeout")
	}
	require.False(t, drained, "drain must be reported unconfirmed on timeout")

	// The stop channel must still be closed so the writer goroutine can observe
	// the stop and exit once the blob store unblocks.
	select {
	case <-o.leiosPersistStop:
	default:
		t.Fatal("stop channel was not closed")
	}
}

// TestLeiosPersistEnqueueAfterStopIsRejected verifies that once the writer is
// stopping, a new enqueue is rejected rather than silently stranded in the
// pending map (where no drain would ever pick it up), so shutdown cannot report
// completion while a freshly fetched endorser block is left unpersisted.
func TestLeiosPersistEnqueueAfterStopIsRejected(t *testing.T) {
	o := newTestOuroborosWithLeiosDB(t)

	// Start the writer via a real enqueue, then drain and stop it.
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 1)
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	o.StopLeiosPersistWriter()

	// The drained map must be empty now.
	o.leiosPersistMu.Lock()
	pendingAfterStop := len(o.leiosPersistPending)
	o.leiosPersistMu.Unlock()
	require.Zero(t, pendingAfterStop)

	// An enqueue after stop must not add a job that would never be drained.
	point2, blockRaw2 := testLeiosEndorserBlockRawWithRefs(t, 11, 1)
	o.enqueueLeiosPersist(point2, blockRaw2, nil)

	o.leiosPersistMu.Lock()
	pending := len(o.leiosPersistPending)
	o.leiosPersistMu.Unlock()
	require.Zero(
		t,
		pending,
		"enqueue after stop must not strand a job in the pending map",
	)
}

// TestLeiosPersistPauseForLiveLifecycleOpDrainsOldDBAndRestartsOnNewDB
// guards the gap a live Restore/Truncate used to leave open: unlike
// StopLeiosPersistWriter (a genuine, permanent shutdown --
// TestLeiosPersistEnqueueAfterStopIsRejected above documents that a
// post-stop enqueue is rejected forever), PauseLeiosPersistWriterForLive
// LifecycleOp must (1) flush whatever was already queued against the
// CURRENT database before anything reassigns LedgerState -- so a
// pre-operation write never lands after the database has moved on -- and
// (2) still accept and eventually persist a job enqueued afterward, once
// LedgerState has been reassigned to a new database, mirroring
// node_lifecycle.go's live restore/truncate reinit reassigning
// n.ouroboros.LedgerState.
func TestLeiosPersistPauseForLiveLifecycleOpDrainsOldDBAndRestartsOnNewDB(
	t *testing.T,
) {
	o := newTestOuroborosWithLeiosDB(t)
	oldDB := o.leiosDatabase()
	require.NotNil(t, oldDB)

	point1, blockRaw1 := testLeiosEndorserBlockRawWithRefs(t, 20, 1)
	o.enqueueLeiosPersist(point1, blockRaw1, nil)

	// Pause immediately -- this must drain the just-queued job against
	// oldDB before returning, exactly like a live Restore/Truncate's
	// quiesce step pausing right before the database closes.
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())

	_, manifest1, err := oldDB.GetLeiosEBManifest(point1.Hash)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw1), manifest1)

	// Simulate reinitializeAndResume reassigning LedgerState to a freshly
	// built database, the way node_lifecycle.go's reinit does.
	newOuroboros := newTestOuroborosWithLeiosDB(t)
	newDB := newOuroboros.leiosDatabase()
	require.NotNil(t, newDB)
	o.LedgerState = newOuroboros.LedgerState

	// A job enqueued after the pause must actually be accepted (not
	// silently dropped, unlike a plain post-Stop enqueue) and land in the
	// NEW database, proving the writer actually restarted rather than
	// staying permanently paused.
	point2, blockRaw2 := testLeiosEndorserBlockRawWithRefs(t, 21, 1)
	o.enqueueLeiosPersist(point2, blockRaw2, nil)
	o.StopLeiosPersistWriter()

	_, manifest2, err := newDB.GetLeiosEBManifest(point2.Hash)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw2), manifest2)

	// And it must not have leaked into the old database.
	_, _, err = oldDB.GetLeiosEBManifest(point2.Hash)
	require.Error(t, err)
}

// TestLeiosPersistPauseForLiveLifecycleOpFailsClosedOnUnconfirmedDrain guards
// the use-after-close/stolen-job race a timed-out pause used to leave open:
// if the writer's drain cannot be confirmed, PauseLeiosPersistWriterForLive
// LifecycleOp must return ErrLeiosPersistDrainUnconfirmed and leave
// leiosPersistOnce/leiosPersistStopOnce/leiosPersistStarted untouched --
// resetting them here, with the old writer goroutine still potentially
// running drainLeiosPersist against the old database, would let the very
// next enqueue start a second writer against a freshly reset pending map
// while the old one is still reading and deleting from that same map
// (now repointed) under the shared mutex.
func TestLeiosPersistPauseForLiveLifecycleOpFailsClosedOnUnconfirmedDrain(
	t *testing.T,
) {
	origTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 20 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = origTimeout })

	o := newTestOuroborosWithLeiosDB(t)
	// Simulate an already-started writer whose drain never completes, with
	// no real goroutine involved (mirroring TestLeiosPersistStopDrainTimesOut)
	// so there's nothing else touching these fields concurrently.
	o.leiosPersistStarted.Store(true)
	o.leiosPersistStop = make(chan struct{})
	o.leiosPersistDone = make(chan struct{}) // deliberately never closed
	o.leiosPersistPending = map[string]*leiosPersistJob{"stuck": {slot: 1}}
	// Mark leiosPersistOnce as already used, matching a real prior start.
	o.leiosPersistOnce.Do(func() {})

	pauseErr := o.PauseLeiosPersistWriterForLiveLifecycleOp()
	require.ErrorIs(t, pauseErr, ErrLeiosPersistDrainUnconfirmed)
	require.True(
		t, o.leiosPersistStarted.Load(),
		"started flag must not be reset on unconfirmed drain",
	)

	// The real regression: a later enqueue must not start a second writer
	// against a freshly reset pending map. leiosPersistStop is still
	// closed (stopLeiosPersistWriter always closes it) and leiosPersistOnce
	// was not reset, so this enqueue is correctly rejected rather than
	// replacing the still-referenced pending map out from under whatever
	// (real, in production) writer might still be draining it.
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 31, 1)
	o.enqueueLeiosPersist(point, blockRaw, nil)
	// A fresh map from a second startLeiosPersistWriter call would be
	// empty (make(map[string]*leiosPersistJob)); the sentinel "stuck"
	// entry surviving proves leiosPersistPending was never replaced.
	_, stillPresent := o.leiosPersistPending["stuck"]
	require.True(
		t, stillPresent,
		"a second writer must not start against a fresh pending map "+
			"while the old drain is unconfirmed",
	)
}
