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
	"context"
	"sync"
	"testing"
	"time"

	databaseTypes "github.com/blinklabs-io/dingo/database/types"
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
	// Recomputed here rather than read from leiosPersistJobSize, so the
	// expectation is independent of the code under test. The hash counts
	// twice: the queue retains it as the map key and again as job.hash.
	size := 2*len(point.Hash) + len(blockRaw)
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
// Not t.Parallel: withLowerLeiosPersistQueueBudget swaps the package-level
// leiosPersistMaxQueueBytes.
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

	job := o.leiosPersistPending[leiosBlockKey(point.Slot, point.Hash)]
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
	_, firstQueued := o.leiosPersistPending[leiosBlockKey(firstPoint.Slot, firstPoint.Hash)]
	require.True(t, firstQueued, "the admitted entry must be left in place")
	_, secondQueued := o.leiosPersistPending[leiosBlockKey(secondPoint.Slot, secondPoint.Hash)]
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
	manifest, err := db.GetLeiosEBManifest(firstPoint.Hash, firstPoint.Slot)
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
	// Hash counted twice, as in leiosPersistTestEntry.
	manifestSize := 2*len(point.Hash) + len(blockRaw)
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
	job := o.leiosPersistPending[leiosBlockKey(point.Slot, point.Hash)]
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
	job := o.leiosPersistPending[leiosBlockKey(point.Slot, point.Hash)]
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

	key := leiosBlockKey(point.Slot, point.Hash)
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
	manifest, err := db.GetLeiosEBManifest(nextPoint.Hash, nextPoint.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(nextRaw), manifest)
}

// A panic between the reservation and the payload copy (the allocation-failure
// window) must give the reservation back. A leaked one is permanent queue
// capacity lost to nothing.
func TestLeiosPersistEnqueueUnwindReleasesReservation(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, size := leiosPersistTestEntry(t, 70, 4, 512)
	o.leiosPersistAfterReserve = func() {
		_, bytes, reserved := leiosPersistQueueState(o)
		require.Equal(t, size, bytes, "reservation must precede the copy")
		require.Equal(t, 1, reserved)
		panic("simulated allocation failure")
	}

	require.Panics(t, func() {
		o.enqueueLeiosPersist(point, blockRaw, data)
	})

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(t, bytes, "an unwound enqueue must release its bytes")
	require.Zero(t, reserved, "an unwound enqueue must release its slot")
}

// A complete job that lands while a manifest-only job for the same hash is
// being copied wins, and the manifest-only job's reservation is released
// exactly once rather than replacing or double-charging the complete one.
func TestLeiosPersistManifestOnlyLosesRaceToCompleteJob(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	point, blockRaw, data, completeSize := leiosPersistTestEntry(
		t, 71, 4, 512,
	)
	manifestSize := leiosPersistJobSize(point.Hash, blockRaw, nil)
	key := leiosBlockKey(point.Slot, point.Hash)

	admitted, _ := o.reserveLeiosPersistBytes(key, manifestSize, false)
	require.True(t, admitted)

	// The complete job is admitted and installed inside that window.
	o.enqueueLeiosPersist(point, blockRaw, data)
	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, 1, queued)
	require.Equal(t, completeSize+manifestSize, bytes)
	require.Equal(t, 1, reserved)

	installed := o.installLeiosPersistJob(key, &leiosPersistJob{
		slot:        point.Slot,
		hash:        point.Hash,
		manifestRaw: blockRaw,
		size:        manifestSize,
	})
	require.False(t, installed)

	queued, bytes, reserved = leiosPersistQueueState(o)
	require.Equal(t, 1, queued)
	require.Equal(t, completeSize, bytes)
	require.Zero(t, reserved)
	require.NotNil(t, o.leiosPersistPending[key].txsRaw)
}

// Concurrent admission of distinct entries against a budget that holds exactly
// K of them admits exactly K, whatever the interleaving, and a drain returns
// the queue to zero.
// Not t.Parallel: withLowerLeiosPersistQueueBudget swaps the package-level
// leiosPersistMaxQueueBytes.
func TestLeiosPersistQueueConcurrentAdmissionHonorsByteBudget(t *testing.T) {
	const (
		enqueuers = 24
		fits      = 5
	)
	o := newTestOuroborosWithPausedLeiosPersistWriter(t)
	_, _, _, size := leiosPersistTestEntry(t, 200, 4, 512)
	withLowerLeiosPersistQueueBudget(t, fits*size)

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range enqueuers {
		point, blockRaw, data, s := leiosPersistTestEntry(t, 200+i, 4, 512)
		require.Equal(t, size, s, "entries must be equal-sized")
		wg.Go(func() {
			<-start
			o.enqueueLeiosPersist(point, blockRaw, data)
		})
	}
	close(start)
	wg.Wait()

	queued, bytes, reserved := leiosPersistQueueState(o)
	require.Equal(t, fits, queued)
	require.Equal(t, fits*size, bytes)
	require.Zero(t, reserved)
	require.Equal(t, uint64(enqueuers-fits), o.leiosPersistDropped.Load())

	o.drainLeiosPersist()
	queued, bytes, reserved = leiosPersistQueueState(o)
	require.Zero(t, queued)
	require.Zero(t, bytes)
	require.Zero(t, reserved)
}

// TestLeiosPersistAsyncCoalescesManifestThenComplete mirrors the backfiller's
// two-call pattern for one endorser block — a manifest-only store followed by a
// complete (manifest + txs) store — and verifies that after the async writer
// drains, the blob store holds the COMPLETE endorser block (manifest + all
// txs), i.e. the later complete write is not lost and the redundant manifest
// write is harmless. This exercises the asynchronous persistence path and the
// merged single-commit SetLeiosEB writer.
func TestLeiosPersistAsyncCoalescesManifestThenComplete(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 2)
	txsRaw := []cbor.RawMessage{
		mustCbor(t, "tx0"),
		mustCbor(t, "tx1"),
	}

	o := newTestOuroborosWithLeiosDB(t)

	// First the manifest only (no txs yet), as the backfiller's manifest fetch
	// does; then the complete block once its txs are fetched.
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			txsRaw,
			leiosStoreAuthoritative,
		),
	)

	// Drain the async writer so all queued persistence has committed.
	o.StopLeiosPersistWriter()

	db := o.leiosDatabase()
	require.NotNil(t, db)

	manifest, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), manifest)

	gotTxs, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, txsRaw, gotTxs)
}

func TestLeiosVerifiedEbSlotRestoresFromPersistedManifest(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 42, 1)
	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{mustCbor(t, "tx0")},
		leiosStoreAuthoritative,
	))
	o.StopLeiosPersistWriter()

	// Simulate a restart: the persisted manifest remains, while the process
	// watermark and in-memory cache are rebuilt from zero.
	o.leiosMaxVerifiedEbSlot.Store(0)
	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()
	o.restoreLeiosVerifiedEbSlot()

	require.Equal(t, point.Slot, o.MaxVerifiedEndorserBlockSlot())
}

// TestLeiosVerifiedEbSlotRestoredByNewOuroboros pins the call site rather
// than the helper. The test above calls restoreLeiosVerifiedEbSlot directly,
// so deleting the o.restoreLeiosVerifiedEbSlot() line from newOuroboros
// leaves ./ouroboros/ fully green and the restore ships inert -- the same
// class TestBuildDingoConfigWiresForgeTolerances was added for on the config
// knobs. Constructing a second Ouroboros over the same
// database and reading the exported watermark is what closes it.
func TestLeiosVerifiedEbSlotRestoredByNewOuroboros(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 4242, 1)
	first := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, first.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{mustCbor(t, "tx0")},
		leiosStoreAuthoritative,
	))
	first.StopLeiosPersistWriter()

	// A restart: a brand-new Ouroboros over the same database, with nothing
	// carried over in process and nothing called on it but the constructor.
	second := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LedgerState:             first.ledgerState,
		LeiosAnnouncementLedger: first.ledgerState,
	})
	t.Cleanup(second.StopLeiosPersistWriter)

	require.Equal(t, point.Slot, second.MaxVerifiedEndorserBlockSlot())
}

func TestLeiosPersistenceGCRetainsConfiguredSlotWindow(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	db := o.leiosDatabase()
	require.NotNil(t, db)
	txs := []cbor.RawMessage{mustCbor(t, "tx")}
	oldHash := make([]byte, 32)
	boundaryHash := make([]byte, 32)
	newHash := make([]byte, 32)
	oldHash[0], boundaryHash[0], newHash[0] = 1, 2, 3
	for _, record := range []struct {
		slot uint64
		hash []byte
	}{
		{slot: 10, hash: oldHash},
		{slot: 20, hash: boundaryHash},
		{slot: 30, hash: newHash},
	} {
		require.NoError(t, db.SetLeiosEB(
			record.slot,
			record.hash,
			[]byte("manifest"),
			txs,
		))
	}

	o.runLeiosPersistenceGC(context.Background(), nil)
	_, err := db.GetLeiosEBManifest(oldHash, 10)
	require.ErrorIs(t, err, databaseTypes.ErrBlobKeyNotFound)
	for _, record := range []struct {
		slot uint64
		hash []byte
	}{
		{slot: 20, hash: boundaryHash},
		{slot: 30, hash: newHash},
	} {
		_, err := db.GetLeiosEBManifest(record.hash, record.slot)
		require.NoError(t, err)
	}
}

func TestLeiosPersistenceGCPausesBeforeLiveDatabaseReplacement(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	o.config.LeiosPersistenceRetentionSlots = 10
	o.startLeiosPersistenceGC(0, false)
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())
	require.False(t, o.leiosPersistGCStarted.Load())
}

// TestLeiosPersistTwoOccurrencesOfSameHashPersistIndependently verifies that
// durable blob records distinguish occurrences by hash and slot, so
// when two live occurrences of the same content-addressed hash existed at
// different slots, the second persist silently overwrote the first --
// making it permanently unavailable for historical re-serving once its
// in-memory entry expired, even though both remained legitimately cached in
// memory. The blob store (SetLeiosEB/GetLeiosEBManifest/GetLeiosEBTxs) is now
// keyed by (slot, hash), so both occurrences persist and reload
// independently.
func TestLeiosPersistTwoOccurrencesOfSameHashPersistIndependently(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 30, 1)
	second := ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash}
	txs1 := []cbor.RawMessage{mustCbor(t, "tx0")}
	txs2 := []cbor.RawMessage{mustCbor(t, "tx1")}

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			txs1,
			leiosStoreAuthoritative,
		),
	)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			second,
			blockRaw,
			txs2,
			leiosStoreAuthoritative,
		),
	)

	o.StopLeiosPersistWriter()
	db := o.leiosDatabase()
	require.NotNil(t, db)

	manifest1, err := db.GetLeiosEBManifest(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), manifest1)
	gotTxs1, err := db.GetLeiosEBTxs(point.Hash, point.Slot)
	require.NoError(t, err)
	require.Equal(t, txs1, gotTxs1)

	manifest2, err := db.GetLeiosEBManifest(second.Hash, second.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), manifest2)
	gotTxs2, err := db.GetLeiosEBTxs(second.Hash, second.Slot)
	require.NoError(t, err)
	require.Equal(t, txs2, gotTxs2)
}

// TestLeiosPersistWriterStopIsSafeWithoutStart verifies StopLeiosPersistWriter
// is a no-op when no endorser block was ever fetched (the writer never started)
// and is safe to call more than once.
func TestLeiosPersistWriterStopIsSafeWithoutStart(t *testing.T) {
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	require.NotPanics(t, func() {
		o.StopLeiosPersistWriter()
		o.StopLeiosPersistWriter()
	})
}

func TestCloseReportsUnconfirmedLeiosPersistenceGCDrain(t *testing.T) {
	o := newTestOuroborosWithLeiosDB(t)

	originalTimeout := leiosPersistShutdownDrainTimeout
	leiosPersistShutdownDrainTimeout = 10 * time.Millisecond
	t.Cleanup(func() { leiosPersistShutdownDrainTimeout = originalTimeout })

	o.leiosPersistGCMu.Lock()
	o.leiosPersistGCStop = make(chan struct{})
	o.leiosPersistGCDone = make(chan struct{})
	o.leiosPersistGCMu.Unlock()
	o.leiosPersistGCStarted.Store(true)

	require.ErrorIs(t, o.Close(), ErrLeiosPersistDrainUnconfirmed)
	close(o.leiosPersistGCDone)
	require.NoError(t, o.Close())
}

func TestClosePreventsLeiosPersistenceFromStartingOrInstalling(t *testing.T) {
	t.Run("close before first enqueue", func(t *testing.T) {
		o := newTestOuroborosWithLeiosDB(t)
		o.config.LeiosPersistenceRetentionSlots = 100
		require.NoError(t, o.Close())

		point, blockRaw, data, _ := leiosPersistTestEntry(t, 0, 1, 4)
		o.enqueueLeiosPersist(point, blockRaw, data)

		require.False(t, o.leiosPersistStarted.Load())
		require.False(t, o.leiosPersistGCStarted.Load())
		require.Empty(t, o.leiosPersistPending)
	})

	t.Run("enqueue admitted before close", func(t *testing.T) {
		o := newTestOuroborosWithLeiosDB(t)
		point, blockRaw, data, _ := leiosPersistTestEntry(t, 1, 1, 4)
		reserved := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
		o.leiosPersistAfterReserve = func() {
			close(reserved)
			<-release
		}

		enqueueDone := make(chan struct{})
		go func() {
			o.enqueueLeiosPersist(point, blockRaw, data)
			close(enqueueDone)
		}()
		select {
		case <-reserved:
		case <-time.After(5 * time.Second):
			t.Fatal("enqueue did not reach its reserved-copy phase")
		}

		require.NoError(t, o.Close())
		releaseOnce.Do(func() { close(release) })
		select {
		case <-enqueueDone:
		case <-time.After(5 * time.Second):
			t.Fatal("in-flight enqueue did not finish after close")
		}

		o.leiosPersistMu.Lock()
		defer o.leiosPersistMu.Unlock()
		require.Empty(t, o.leiosPersistPending)
		require.Zero(t, o.leiosPersistBytes)
		require.Zero(t, o.leiosPersistReserved)
	})
}

// TestLeiosPersistStopDrainTimesOut verifies that the shutdown drain wait is
// bounded: if the writer's drain is stuck (e.g. the blob store hangs inside
// SetLeiosEB, so leiosPersistDone is never closed), stopLeiosPersistWriter
// returns after the drain timeout instead of blocking graceful shutdown
// forever, and still closes the stop channel so the writer can exit later.
func TestLeiosPersistStopDrainTimesOut(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
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
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)

	// Start the writer via a real enqueue, then drain and stop it.
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 1)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
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
	t.Parallel()

	o := newTestOuroborosWithLeiosDB(t)
	oldDB := o.leiosDatabase()
	require.NotNil(t, oldDB)

	point1, blockRaw1 := testLeiosEndorserBlockRawWithRefs(t, 20, 1)
	o.enqueueLeiosPersist(point1, blockRaw1, nil)

	// Pause immediately -- this must drain the just-queued job against
	// oldDB before returning, exactly like a live Restore/Truncate's
	// quiesce step pausing right before the database closes.
	require.NoError(t, o.PauseLeiosPersistWriterForLiveLifecycleOp())

	manifest1, err := oldDB.GetLeiosEBManifest(point1.Hash, point1.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw1), manifest1)

	// Simulate reinitializeAndResume reassigning LedgerState to a freshly
	// built database, the way node_lifecycle.go's reinit does.
	newOuroboros := newTestOuroborosWithLeiosDB(t)
	newDB := newOuroboros.leiosDatabase()
	require.NotNil(t, newDB)
	o.ledgerState = newOuroboros.ledgerState

	// A job enqueued after the pause must actually be accepted (not
	// silently dropped, unlike a plain post-Stop enqueue) and land in the
	// NEW database, proving the writer actually restarted rather than
	// staying permanently paused.
	point2, blockRaw2 := testLeiosEndorserBlockRawWithRefs(t, 21, 1)
	o.enqueueLeiosPersist(point2, blockRaw2, nil)
	o.StopLeiosPersistWriter()

	manifest2, err := newDB.GetLeiosEBManifest(point2.Hash, point2.Slot)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw2), manifest2)

	// And it must not have leaked into the old database.
	_, err = oldDB.GetLeiosEBManifest(point2.Hash, point2.Slot)
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
// Not t.Parallel: swaps the package-level leiosPersistShutdownDrainTimeout.
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
