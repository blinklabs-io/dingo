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
	"sync"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newTestOuroborosWithPausedLeiosPersistWriter builds an Ouroboros with a real
// Leios blob store and pre-consumes leiosPersistOnce with an initializer that
// sets up the same queue state as startLeiosPersistWriter but does NOT launch
// the background writer goroutine.
//
// These tests exercise queue admission, and a running writer drains jobs --
// releasing their byte reservations -- concurrently with the assertions, so
// there would be no stable queue state to assert against. Tests that need the
// drain call drainLeiosPersist directly instead.
//
// leiosPersistStarted is deliberately left false: nothing here consults it on
// the enqueue path, and leaving it false keeps a stray StopLeiosPersistWriter
// a no-op rather than a five-second wait on a leiosPersistDone that no writer
// will ever close.
func newTestOuroborosWithPausedLeiosPersistWriter(t *testing.T) *Ouroboros {
	t.Helper()
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosPersistOnce.Do(func() {
		o.leiosPersistPending = make(map[string]*leiosPersistJob)
		o.leiosPersistSignal = make(chan struct{}, 1)
		o.leiosPersistStop = make(chan struct{})
		o.leiosPersistDone = make(chan struct{})
	})
	return o
}

// withLowerLeiosPersistQueueBudget temporarily lowers the aggregate queue byte
// budget so a test can exercise admission without allocating hundreds of
// megabytes, mirroring withLowerLeiosEndorserBlockCacheBudgets.
func withLowerLeiosPersistQueueBudget(t *testing.T, maxBytes int) {
	t.Helper()
	orig := leiosPersistMaxQueueBytes
	leiosPersistMaxQueueBytes = maxBytes
	t.Cleanup(func() { leiosPersistMaxQueueBytes = orig })
}

// leiosPersistTestEntry builds one endorser block plus a complete transaction
// set of txCount bodies of roughly txBytes each, and returns the retained size
// a persistence job for it holds. The size is summed here from the payload the
// test itself built -- hash + manifest + every transaction body -- rather than
// by calling the production leiosPersistJobSize, so an assertion against the
// queue's accounting is checked against an independent measurement instead of
// against the same function that produced it.
//
// Each transaction body is a real CBOR byte string, not an arbitrary buffer:
// the drain path re-encodes txsRaw through cbor.Encode, which fails on
// malformed members and would take the manifest write down with it.
func leiosPersistTestEntry(
	t *testing.T,
	idx int,
	txCount int,
	txBytes int,
) (ocommon.Point, cbor.RawMessage, *leiosEndorserBlockData, int) {
	t.Helper()
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, idx, txCount)
	size := len(point.Hash) + len(blockRaw)
	txsRaw := make([]cbor.RawMessage, 0, txCount)
	for i := range txCount {
		body := make([]byte, txBytes)
		body[0] = byte(idx)
		if txBytes > 1 {
			body[1] = byte(i)
		}
		encoded, err := cbor.Encode(body)
		require.NoError(t, err)
		txsRaw = append(txsRaw, cbor.RawMessage(encoded))
		size += len(encoded)
	}
	data := &leiosEndorserBlockData{
		point:    point,
		blockRaw: blockRaw,
		txsRaw:   txsRaw,
		txCount:  txCount,
	}
	return point, blockRaw, data, size
}

// leiosPersistQueueState reads the queue's accounting under its own mutex.
func leiosPersistQueueState(o *Ouroboros) (entries, bytes, reserved int) {
	o.leiosPersistMu.Lock()
	defer o.leiosPersistMu.Unlock()
	return len(o.leiosPersistPending), o.leiosPersistBytes,
		o.leiosPersistReserved
}

// Absence case for the byte budget: an endorser block that fits is admitted
// normally, holding exactly its own size against the budget and leaving no
// in-flight reservation behind.
func TestLeiosPersistQueueAdmitsEntryWithinByteBudget(t *testing.T) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, size := leiosPersistTestEntry(t, 40, 4, 512)
	withLowerLeiosPersistQueueBudget(t, size)

	o.enqueueLeiosPersist(point, blockRaw, data)

	entries, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, 1, entries, "an entry within budget must be queued")
	require.Equal(
		t, size, bytes,
		"the queued job must hold exactly its own size against the budget",
	)
	require.Zero(t, reserved, "no reservation may remain in flight")
	require.Zero(
		t, o.leiosPersistDropped.Load(),
		"an entry within budget must not be counted as a drop",
	)

	job := o.leiosPersistPending[string(point.Hash)]
	require.NotNil(t, job)
	require.Equal(t, []byte(blockRaw), job.manifestRaw)
	require.Equal(t, data.txsRaw, job.txsRaw)
}

// An endorser block that does not fit the remaining aggregate budget is
// dropped, and the already-queued entry it did not fit alongside is left
// untouched. Before the budget existed, leiosPersistMaxPending alone would
// have admitted both.
func TestLeiosPersistQueueRejectsEntryOverByteBudget(t *testing.T) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	firstPoint, firstRaw, firstData, firstSize := leiosPersistTestEntry(
		t, 41, 4, 512,
	)
	secondPoint, secondRaw, secondData, _ := leiosPersistTestEntry(
		t, 42, 4, 512,
	)
	// Room for exactly one of the two.
	withLowerLeiosPersistQueueBudget(t, firstSize)

	o.enqueueLeiosPersist(firstPoint, firstRaw, firstData)
	o.enqueueLeiosPersist(secondPoint, secondRaw, secondData)

	entries, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(
		t, 1, entries,
		"the second endorser block must be dropped, not queued past the budget",
	)
	require.Equal(t, firstSize, bytes)
	require.Zero(t, reserved)
	_, firstQueued := o.leiosPersistPending[string(firstPoint.Hash)]
	require.True(t, firstQueued, "the admitted entry must be left in place")
	_, secondQueued := o.leiosPersistPending[string(secondPoint.Hash)]
	require.False(t, secondQueued)
	require.Equal(t, uint64(1), o.leiosPersistDropped.Load())
}

// Concurrent oversize case: many connections offering endorser blocks that
// each exceed the whole queue budget are all rejected, and every rejection
// gives its reservation back -- an oversize entry that leaked its reservation
// would permanently consume the capacity it was refused, so a few of them
// would close the queue to legitimate writes.
func TestLeiosPersistQueueRejectsOversizeEntriesConcurrently(t *testing.T) {
	const enqueuers = 16
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	// Every entry below is 8 txs of 1 KiB plus its manifest, so a 1 KiB
	// budget cannot hold even one of them.
	withLowerLeiosPersistQueueBudget(t, 1<<10)

	type entry struct {
		point    ocommon.Point
		blockRaw cbor.RawMessage
		data     *leiosEndorserBlockData
	}
	entries := make([]entry, 0, enqueuers)
	for i := range enqueuers {
		point, blockRaw, data, size := leiosPersistTestEntry(
			t, 100+i, 8, 1<<10,
		)
		require.Greater(
			t, size, leiosPersistMaxQueueBytes,
			"test entry must exceed the whole queue budget",
		)
		entries = append(entries, entry{point, blockRaw, data})
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		go func(e entry) {
			defer wg.Done()
			<-start
			o.enqueueLeiosPersist(e.point, e.blockRaw, e.data)
		}(e)
	}
	close(start)
	wg.Wait()

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, queued, "no oversize entry may be queued")
	require.Zero(
		t, bytes,
		"a rejected oversize entry must leave no bytes reserved",
	)
	require.Zero(t, reserved)
	require.Equal(t, uint64(enqueuers), o.leiosPersistDropped.Load())
}

// A rejected endorser block must not have been copied: the reservation and
// the count cap are both decided from the caller's own slices, so a drop
// costs no manifest copy and no transaction-body copies. With 64 transaction
// bodies per entry, cloning would allocate at least 65 times per rejected
// enqueue; admission-first allocates only the pending-map lookup key.
func TestLeiosPersistQueueDoesNotCopyRejectedEntry(t *testing.T) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, _ := leiosPersistTestEntry(t, 60, 64, 256)
	// Budget of zero rejects every entry, including this one, at the
	// oversize check -- the earliest possible admission decision.
	withLowerLeiosPersistQueueBudget(t, 0)

	allocs := testing.AllocsPerRun(64, func() {
		o.enqueueLeiosPersist(point, blockRaw, data)
	})

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(t, bytes)
	require.Zero(t, reserved)
	require.Less(
		t, allocs, 8.0,
		"a rejected endorser block must not be cloned before admission "+
			"(got %v allocations per rejected enqueue)", allocs,
	)
}

// Pop path: a drained job's reservation leaves the queue with it, so the
// budget is a steady-state limit rather than a one-shot allowance. Without
// the release on pop, the queue would refuse every write for the rest of the
// process lifetime once the budget had been reached even once.
func TestLeiosPersistQueueReleasesReservationOnPop(t *testing.T) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	firstPoint, firstRaw, firstData, firstSize := leiosPersistTestEntry(
		t, 43, 4, 512,
	)
	secondPoint, secondRaw, secondData, secondSize := leiosPersistTestEntry(
		t, 44, 4, 512,
	)
	withLowerLeiosPersistQueueBudget(t, firstSize)

	o.enqueueLeiosPersist(firstPoint, firstRaw, firstData)
	o.enqueueLeiosPersist(secondPoint, secondRaw, secondData)
	queued, _, _ := leiosPersistQueueState(o)
	require.Equal(t, 1, queued, "the second entry must not fit yet")

	o.drainLeiosPersist()

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(t, bytes, "a popped job must release its reservation")
	require.Zero(t, reserved)

	// The same endorser block that did not fit a moment ago now does.
	o.enqueueLeiosPersist(secondPoint, secondRaw, secondData)
	queued, bytes, reserved = leiosPersistQueueState(o)
	require.Equal(
		t, 1, queued,
		"capacity freed by the drain must be reusable",
	)
	require.Equal(t, secondSize, bytes)
	require.Zero(t, reserved)

	db := o.leiosDatabase()
	require.NotNil(t, db)
	_, manifest, err := db.GetLeiosEBManifest(firstPoint.Hash)
	require.NoError(t, err)
	require.Equal(t, []byte(firstRaw), manifest)
}

// Replace path: a complete job superseding a queued manifest-only job for the
// same endorser block takes over the queue slot and releases the incumbent's
// reservation, so the queue holds one reservation for one entry rather than
// accumulating both.
func TestLeiosPersistQueueReleasesReservationOnReplace(t *testing.T) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, completeSize := leiosPersistTestEntry(
		t, 45, 4, 512,
	)
	manifestSize := len(point.Hash) + len(blockRaw)
	withLowerLeiosPersistQueueBudget(t, manifestSize+completeSize)

	// The backfiller's manifest-only store, then the complete one.
	o.enqueueLeiosPersist(point, blockRaw, nil)
	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, 1, queued)
	require.Equal(t, manifestSize, bytes)
	require.Zero(t, reserved)

	o.enqueueLeiosPersist(point, blockRaw, data)

	queued, bytes, reserved = leiosPersistQueueState(o)
	require.Equal(t, 1, queued, "the two stores must coalesce to one job")
	require.Equal(
		t, completeSize, bytes,
		"the superseded manifest-only job must release its reservation",
	)
	require.Zero(t, reserved)
	job := o.leiosPersistPending[string(point.Hash)]
	require.NotNil(t, job)
	require.Equal(t, data.txsRaw, job.txsRaw, "the complete job must win")
	require.Zero(t, o.leiosPersistDropped.Load())

	o.drainLeiosPersist()
	_, bytes, reserved = leiosPersistQueueState(o)
	require.Zero(t, bytes)
	require.Zero(t, reserved)
}

// The reverse ordering -- a manifest-only store arriving behind an already
// queued complete job -- is refused before it reserves anything, so it
// neither displaces the complete job nor charges the budget.
func TestLeiosPersistQueueManifestBehindCompleteReservesNothing(t *testing.T) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, completeSize := leiosPersistTestEntry(
		t, 46, 4, 512,
	)
	withLowerLeiosPersistQueueBudget(t, completeSize)

	o.enqueueLeiosPersist(point, blockRaw, data)
	o.enqueueLeiosPersist(point, blockRaw, nil)

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, 1, queued)
	require.Equal(t, completeSize, bytes)
	require.Zero(t, reserved)
	job := o.leiosPersistPending[string(point.Hash)]
	require.NotNil(t, job)
	require.Equal(
		t, data.txsRaw, job.txsRaw,
		"a manifest-only store must not displace a complete job",
	)
	require.Zero(
		t, o.leiosPersistDropped.Load(),
		"routine coalescing is not a capacity drop",
	)
}

// Shutdown path: a reservation taken just before the writer was told to stop
// is released rather than installed. Installing it would strand the job (the
// shutdown drain may already have made its final map read) and leaking the
// reservation would leave the restarted queue short of capacity.
//
// reserveLeiosPersistBytes and installLeiosPersistJob are called directly, in
// the order enqueueLeiosPersist calls them, because the stop signal has to
// land in the window between them -- the window in which enqueueLeiosPersist
// is copying the payload -- and that window cannot be hit deterministically
// from outside.
func TestLeiosPersistQueueReleasesReservationWhenStopRacesInstall(
	t *testing.T,
) {
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, size := leiosPersistTestEntry(t, 47, 4, 512)
	withLowerLeiosPersistQueueBudget(t, size)

	key := string(point.Hash)
	admitted, dropReason := o.reserveLeiosPersistBytes(key, size, true)
	require.True(t, admitted)
	require.Empty(t, dropReason)
	_, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, size, bytes)
	require.Equal(t, 1, reserved)

	// Stop arrives while the payload would still be being copied.
	close(o.leiosPersistStop)

	installed := o.installLeiosPersistJob(key, &leiosPersistJob{
		slot:        point.Slot,
		hash:        point.Hash,
		manifestRaw: blockRaw,
		txsRaw:      data.txsRaw,
		size:        size,
	})
	require.False(t, installed, "a job must not be installed after stop")

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(
		t, bytes,
		"a job dropped at stop must release its reservation",
	)
	require.Zero(t, reserved)
}

// A live Restore/Truncate rebuilds the pending map, and the accounting is
// reset with it. Carrying the old map's byte total onto the new one would be a
// permanent reduction in queue capacity after every live lifecycle operation.
func TestLeiosPersistWriterRestartResetsQueueAccounting(t *testing.T) {
	o := newTestOuroborosWithLeiosDB(t)
	point, blockRaw, data, size := leiosPersistTestEntry(t, 48, 4, 512)
	withLowerLeiosPersistQueueBudget(t, size)

	o.enqueueLeiosPersist(point, blockRaw, data)
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())

	// The pause drained the job, so its reservation is already gone; the
	// restart must not carry any residue either way.
	nextPoint, nextRaw, nextData, nextSize := leiosPersistTestEntry(
		t, 49, 4, 512,
	)
	o.enqueueLeiosPersist(nextPoint, nextRaw, nextData)
	o.StopLeiosPersistWriter()

	_, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, bytes)
	require.Zero(t, reserved)
	require.Zero(
		t, o.leiosPersistDropped.Load(),
		"the restarted queue must have full capacity, not %d bytes of it",
		nextSize,
	)

	db := o.leiosDatabase()
	require.NotNil(t, db)
	_, manifest, err := db.GetLeiosEBManifest(nextPoint.Hash)
	require.NoError(t, err)
	require.Equal(t, []byte(nextRaw), manifest)
}
