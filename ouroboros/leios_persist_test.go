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
	go func() {
		o.stopLeiosPersistWriter(50 * time.Millisecond)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("stopLeiosPersistWriter hung past the bounded drain timeout")
	}

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
