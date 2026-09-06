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
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosPersistMaxPending bounds the coalescing pending-write map so a writer
// that falls behind under heavy catch-up load cannot grow memory without limit.
// When full, new distinct endorser blocks are dropped from persistence (logged)
// rather than blocking the fetch path — historical serving is best-effort and a
// dropped block can be re-fetched and re-persisted later. It does not affect
// UTxO correctness (that uses the ledger's own genesis-blob path).
const leiosPersistMaxPending = 4096

// leiosPersistMaxQueueBytes bounds the aggregate retained size (manifest plus
// every transaction body) of the pending-write map, counting both installed
// jobs and reservations whose payload copy is still in flight.
// leiosPersistMaxPending alone bounds only entry count: every job is built
// from an endorser-block cache entry that storeLeiosEndorserBlock has just
// admitted under leiosEndorserBlockCacheMaxEntryBytes (16 MiB), so
// leiosPersistMaxPending entries could retain 64 GiB. This bounds actual
// memory directly, at the same 256 MiB the endorser-block cache itself uses
// (leiosEndorserBlockCacheMaxBytes) -- the queue only ever holds a copy of
// what that cache already holds, so it has no reason to be allowed to grow
// larger than it. A job cannot be permanently unqueueable at this budget:
// the cache's own per-entry cap keeps every job at or below 16 MiB, far
// under the aggregate.
//
// Over-budget entries are dropped, not deferred, exactly like the
// leiosPersistMaxPending drop above: this queue feeds historical serving
// only, a dropped endorser block is re-fetched and re-persisted later, and
// deferring would mean blocking the leios-fetch hot path that this whole
// writer exists to keep clear.
//
// Declared as a var, not a const, solely so tests can lower it (save,
// override, t.Cleanup restore) without allocating hundreds of megabytes of
// test data. Production code only reads it.
var leiosPersistMaxQueueBytes = 256 << 20 // 256 MiB

// leiosPersistShutdownDrainTimeout bounds how long StopLeiosPersistWriter waits
// for the background writer to finish draining queued blob writes at shutdown.
// The drain calls Database.SetLeiosEB, which can block indefinitely on a stuck
// or slow blob store; persisting fetched endorser blocks for historical serving
// is best-effort, so that must never hang a graceful shutdown. After this
// timeout the wait is abandoned (a warning is logged, the writer goroutine is
// left to exit on its own once the store unblocks). The stop channel is always
// closed regardless, so the writer does exit; only the wait is bounded.
// Package-level var (not const) so tests can shrink it instead of running a
// real multi-second timeout.
var leiosPersistShutdownDrainTimeout = 5 * time.Second

// ErrLeiosPersistDrainUnconfirmed is returned by
// PauseLeiosPersistWriterForLiveLifecycleOp when its bounded wait gave up
// before confirming the writer goroutine actually exited. Unlike a normal
// permanent StopLeiosPersistWriter timeout (best-effort, the process is
// exiting anyway), a live restore/truncate's caller must not treat this as
// safe to proceed: the old writer may still be running drainLeiosPersist
// against the about-to-close database and the shared pending map.
var ErrLeiosPersistDrainUnconfirmed = errors.New(
	"leios persist writer drain not confirmed before timeout",
)

// leiosPersistJob is one endorser block queued for best-effort blob-store
// persistence. txsRaw is nil for a manifest-only job (incomplete EB).
//
// size is the byte reservation this job holds against
// leiosPersistMaxQueueBytes. It is recorded on the job, rather than
// recomputed from the payload when the job leaves the queue, so that the
// amount released is always exactly the amount reserved: the reservation is
// taken before the payload is copied (see enqueueLeiosPersist), so a
// recomputation would be a second, independent measurement of a different
// set of slices and any disagreement between the two would leak or
// double-release queue capacity.
type leiosPersistJob struct {
	slot        uint64
	hash        []byte
	manifestRaw []byte
	txsRaw      []cbor.RawMessage
	size        int
}

// leiosPersistJobSize is the retained size a job for this endorser block
// would hold: the hash key plus the manifest plus every transaction body.
// Measured from the caller's own slices so the admission decision in
// reserveLeiosPersistBytes costs no allocation and no copy.
func leiosPersistJobSize(
	hash []byte,
	manifestRaw []byte,
	txsRaw []cbor.RawMessage,
) int {
	// The hash is charged twice because the queue retains two copies: the
	// map key string and the cloned job.hash. Undercharging here would let
	// the queue hold more than the budget it reports.
	size := 2*len(hash) + len(manifestRaw)
	for _, raw := range txsRaw {
		size += len(raw)
	}
	return size
}

// enqueueLeiosPersist queues an endorser block for asynchronous blob-store
// persistence (historical serving) instead of writing it synchronously on the
// leios-fetch hot path. Jobs coalesce by (slot, hash): a complete job (carrying
// txs) supersedes a manifest-only one for the same occurrence, so the
// backfiller's manifest-only-then-complete pair collapses to a single write.
// Best-effort: a
// full queue drops the write; no error is surfaced to the caller.
//
// Admission runs before any copying. The queue's aggregate byte reservation
// and its entry-count cap are both decided from the caller's own slices in
// reserveLeiosPersistBytes, so an endorser block the queue is going to drop
// never costs the manifest-and-every-transaction-body copy that cloning it
// would -- previously a peer could make this path allocate a full copy of
// every endorser block it offered and have it thrown away immediately at the
// count cap, and 4096 accepted copies had no aggregate size limit at all.
func (o *Ouroboros) enqueueLeiosPersist(
	point ocommon.Point,
	blockRaw []byte,
	data *leiosEndorserBlockData,
) {
	if o.leiosDatabase() == nil {
		return
	}
	o.leiosPersistOnce.Do(o.startLeiosPersistWriter)
	// The caller's transaction slices, not a copy: they are only measured
	// here, and are cloned below if and only if the job is admitted.
	var txsRaw []cbor.RawMessage
	if data != nil && data.completeTxCache() && data.txCount > 0 {
		txsRaw = data.txsRaw
	}
	key := leiosBlockKey(point.Slot, point.Hash)
	size := leiosPersistJobSize(point.Hash, blockRaw, txsRaw)
	admitted, dropReason := o.reserveLeiosPersistBytes(
		key,
		size,
		len(txsRaw) > 0,
	)
	if !admitted {
		if dropReason != "" {
			// Logged outside leiosPersistMu: the drain holds that mutex to
			// pop each job, and a log sink that blocks must not stall it.
			o.logLeiosPersistDrop(point.Slot, dropReason)
		}
		return
	}
	// size bytes are now reserved. Every path out of this function must
	// either hand that reservation to an installed job or give it back --
	// a reservation that is neither is permanent queue capacity lost to
	// nothing, which looks exactly like the memory exhaustion the budget
	// exists to prevent. The deferred release covers the unwind path:
	// cloning a multi-megabyte endorser block is the one allocation-failure
	// point between the reservation and its hand-off.
	reservationHeld := true
	defer func() {
		if reservationHeld {
			o.releaseLeiosPersistReservation(size)
		}
	}()
	job := &leiosPersistJob{
		slot:        point.Slot,
		hash:        slices.Clone(point.Hash),
		manifestRaw: slices.Clone(blockRaw),
		txsRaw:      cloneRawMessages(txsRaw),
		size:        size,
	}
	// From here the reservation's fate belongs to installLeiosPersistJob,
	// which decides it in the same critical section as the map mutation and
	// performs the release itself on each of its own drop paths. The
	// deferred net must not also fire, or the release would be doubled.
	reservationHeld = false
	if !o.installLeiosPersistJob(key, job) {
		return
	}
	select {
	case o.leiosPersistSignal <- struct{}{}:
	default:
	}
}

// reserveLeiosPersistBytes decides whether a job of the given size is admitted
// and, when it is, reserves those bytes against leiosPersistMaxQueueBytes
// before the caller copies anything. It returns false having reserved nothing
// when the job must be dropped, along with a drop reason for the caller to log
// (empty for the routine manifest-only-behind-complete coalescing case, which
// is not a capacity drop).
//
// An in-flight reservation counts against both budgets: its bytes against
// leiosPersistMaxQueueBytes, and -- for a hash that is not already pending --
// one slot against leiosPersistMaxPending. Counting reservations toward the
// entry cap is what lets installLeiosPersistJob install unconditionally: N
// concurrent enqueues for N distinct hashes would otherwise all pass the count
// check against the same not-quite-full map and then all install, overshooting
// the cap by N-1.
func (o *Ouroboros) reserveLeiosPersistBytes(
	key string,
	size int,
	hasTxs bool,
) (bool, string) {
	o.leiosPersistMu.Lock()
	defer o.leiosPersistMu.Unlock()
	// Reject work once the writer is stopping. The shutdown drain runs only
	// after leiosPersistStop is closed and reads the pending map under this same
	// mutex; a job added after the drain has emptied the map would be stranded
	// forever (silently dropped) and would make shutdown report completion while
	// a freshly fetched endorser block was never persisted. Checking the stop
	// signal here, under the lock, closes that race: a job added while stop is
	// still open is committed before the drain's map read (which needs the lock)
	// can observe an empty map, and any job attempted after stop is closed is
	// rejected. Best-effort — a rejected block can be re-fetched and re-persisted
	// after restart.
	select {
	case <-o.leiosPersistStop:
		return false, ""
	default:
	}
	if existing := o.leiosPersistPending[key]; existing != nil {
		// Never let a manifest-only job overwrite one that already carries
		// txs — that would re-introduce the duplicate manifest write and lose
		// the tx bodies from the pending write.
		if existing.txsRaw != nil && !hasTxs {
			return false, ""
		}
	} else if len(o.leiosPersistPending)+
		o.leiosPersistReserved >= leiosPersistMaxPending {
		return false, "queue full"
	}
	// A job that could never fit even in an empty queue is reported
	// separately: it is a permanent property of that endorser block, not
	// transient pressure, and the two want different operator responses.
	// storeLeiosEndorserBlock's per-entry cap keeps this unreachable at the
	// production budgets; it is enforced here so lowering either bound
	// cannot turn it into a silent accounting hole.
	if size > leiosPersistMaxQueueBytes {
		return false, "endorser block larger than the whole queue budget"
	}
	// A replacement charges its full size rather than the delta against the
	// job it will supersede, so that exactly `size` is reserved here and
	// exactly `size` is released later on whichever path the reservation
	// takes. The incumbent's bytes are released by installLeiosPersistJob as
	// it leaves the map, so the two only overlap for the duration of the
	// copy. That transient double-charge costs at most one endorser block of
	// headroom (16 MiB against a 256 MiB budget), and a replacement rejected
	// for want of it leaves the already-queued manifest-only write in place
	// rather than losing the block entirely.
	if o.leiosPersistBytes+size > leiosPersistMaxQueueBytes {
		return false, "byte budget exhausted"
	}
	o.leiosPersistBytes += size
	o.leiosPersistReserved++
	return true, ""
}

// installLeiosPersistJob publishes a job whose payload copy is complete,
// handing the reservation reserveLeiosPersistBytes took over to the installed
// job. It always ends that reservation's in-flight phase, and it releases the
// reserved bytes itself on every path that does not install, so the caller
// owns the reservation only up to this call. Returns whether the job was
// installed, and therefore whether the writer needs a wakeup.
func (o *Ouroboros) installLeiosPersistJob(
	key string,
	job *leiosPersistJob,
) bool {
	o.leiosPersistMu.Lock()
	defer o.leiosPersistMu.Unlock()
	// The in-flight phase ends either way: the bytes are held by the
	// installed job from here on, or released below.
	o.leiosPersistReserved--
	select {
	case <-o.leiosPersistStop:
		// Stop was signalled while this job was being copied. Installing it
		// now would strand it exactly as a post-stop enqueue would: the
		// shutdown drain may already have made its final map read.
		o.leiosPersistBytes -= job.size
		return false
	default:
	}
	if existing := o.leiosPersistPending[key]; existing != nil {
		if existing.txsRaw != nil && job.txsRaw == nil {
			// A complete job for this hash landed while this manifest-only
			// one was being copied; keep the complete one, same as
			// reserveLeiosPersistBytes would have decided had it seen it.
			o.leiosPersistBytes -= job.size
			return false
		}
		// Replacement: the incumbent's reservation leaves the queue with it,
		// and this job keeps the reservation it arrived holding.
		o.leiosPersistBytes -= existing.size
	}
	o.leiosPersistPending[key] = job
	return true
}

// releaseLeiosPersistReservation gives back a reservation that never became an
// installed job -- the unwind path between reserveLeiosPersistBytes and
// installLeiosPersistJob.
func (o *Ouroboros) releaseLeiosPersistReservation(size int) {
	o.leiosPersistMu.Lock()
	defer o.leiosPersistMu.Unlock()
	o.leiosPersistReserved--
	o.leiosPersistBytes -= size
}

// logLeiosPersistDrop reports a dropped historical-serving write, rate-limited
// to one log line per 256 drops so sustained pressure cannot flood the log.
// Must be called without leiosPersistMu held.
func (o *Ouroboros) logLeiosPersistDrop(slot uint64, reason string) {
	if n := o.leiosPersistDropped.Add(1); n%256 == 1 {
		o.config.Logger.Warn(
			"dropping leios EB historical-serving write",
			"component", "network",
			"slot", slot,
			"reason", reason,
			"dropped_total", n,
		)
	}
}

// startLeiosPersistWriter initializes the writer state and launches the single
// background writer goroutine. Runs exactly once via leiosPersistOnce, before
// any enqueue proceeds past the Once, so the map and channels are safely
// published to concurrent enqueuers.
func (o *Ouroboros) startLeiosPersistWriter() {
	o.leiosPersistPending = make(map[string]*leiosPersistJob)
	// The byte and reservation counters describe the map being replaced, so
	// they are reset with it. This matters on the
	// PauseLeiosPersistWriterForLiveLifecycleOp restart path, where the old
	// map's accounting would otherwise be carried onto the new one as a
	// permanent reduction in queue capacity.
	o.leiosPersistBytes = 0
	o.leiosPersistReserved = 0
	o.leiosPersistSignal = make(chan struct{}, 1)
	o.leiosPersistStop = make(chan struct{})
	o.leiosPersistDone = make(chan struct{})
	o.leiosPersistStarted.Store(true)
	go o.leiosPersistLoop()
}

func (o *Ouroboros) leiosPersistLoop() {
	defer close(o.leiosPersistDone)
	for {
		select {
		case <-o.leiosPersistStop:
			// Drain remaining queued writes before exiting so a clean
			// shutdown still persists what was already fetched.
			o.drainLeiosPersist()
			return
		case <-o.leiosPersistSignal:
			o.drainLeiosPersist()
		}
	}
}

// drainLeiosPersist writes every currently-pending job. Order is irrelevant
// (each EB is independent), so it pops arbitrary map entries until empty.
func (o *Ouroboros) drainLeiosPersist() {
	db := o.leiosDatabase()
	for {
		o.leiosPersistMu.Lock()
		var key string
		var job *leiosPersistJob
		for k, j := range o.leiosPersistPending {
			key, job = k, j
			break
		}
		if job != nil {
			delete(o.leiosPersistPending, key)
			// A popped job's bytes leave the queue with it, which is what
			// makes room for the next enqueue. Without this the budget
			// would be a one-shot allowance for the process lifetime.
			o.leiosPersistBytes -= job.size
		}
		o.leiosPersistMu.Unlock()
		if job == nil {
			return
		}
		if db == nil {
			continue
		}
		if err := db.SetLeiosEB(job.slot, job.hash, job.manifestRaw, job.txsRaw); err != nil {
			o.config.Logger.Debug(
				"failed to persist leios EB to blob store",
				"component", "network",
				"slot", job.slot,
				"error", err,
			)
		}
	}
}

// StopLeiosPersistWriter stops the background persistence writer and waits, up
// to leiosPersistShutdownDrainTimeout, for it to drain queued writes and exit.
// Safe to call when the writer never started (no endorser block was ever
// fetched) and idempotent across multiple calls.
func (o *Ouroboros) StopLeiosPersistWriter() {
	o.stopLeiosPersistWriter(leiosPersistShutdownDrainTimeout)
}

// stopLeiosPersistWriter closes the stop channel and waits for the writer to
// drain and exit, giving up after drainTimeout so a stuck blob store cannot
// hang graceful shutdown. Split out from StopLeiosPersistWriter so tests can
// exercise the bounded wait without the production timeout. Returns whether
// the writer's exit was actually confirmed (false on timeout) so a caller
// that cannot tolerate an unconfirmed exit -- see
// PauseLeiosPersistWriterForLiveLifecycleOp -- can react instead of assuming
// a timeout means the writer is gone.
func (o *Ouroboros) stopLeiosPersistWriter(drainTimeout time.Duration) bool {
	if !o.leiosPersistStarted.Load() {
		return true
	}
	// Always close the stop channel so the writer observes the stop and exits,
	// even if we stop waiting for it below.
	o.leiosPersistStopOnce.Do(func() { close(o.leiosPersistStop) })
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case <-o.leiosPersistDone:
		return true
	case <-timer.C:
		// The drain is stuck (likely a slow/unavailable blob store). Abandon the
		// wait: historical-serving persistence is best-effort and the writer
		// goroutine will still exit once the store unblocks.
		o.config.Logger.Warn(
			"timed out waiting for leios EB persistence writer to drain; abandoning remaining historical-serving writes",
			"component",
			"network",
			"timeout",
			drainTimeout,
		)
		return false
	}
}

// PauseLeiosPersistWriterForLiveLifecycleOp stops the persistence writer
// (draining whatever is already queued against the current, about-to-close
// database) and resets its start-once guard, so a later enqueueLeiosPersist
// call -- once LedgerState has been reassigned to the reinitialized
// database after a live Restore/Truncate -- lazily relaunches a fresh
// writer against the new database, the same way the very first call ever
// does.
//
// Without this, the writer (once started) ran for the whole Ouroboros
// object's lifetime, since that object is retained (not rebuilt) across a
// live Restore/Truncate. A job already queued before quiesce began could
// still be mid-drain when closeStorageForLiveLifecycleOp closed the
// database out from under it, or -- worse -- could still be sitting
// untouched in the pending map when LedgerState was reassigned, so the
// eventual drain would silently write pre-operation data into the freshly
// restored/truncated store.
//
// Must be called late in the quiesce sequence, after inbound network
// traffic has actually stopped (connManager.Stop): enqueueLeiosPersist
// doesn't take leiosPersistMu before touching leiosPersistOnce, so a
// concurrent enqueue from still-live Leios fetch traffic could otherwise
// race this reset. This is not merely a documented call-order convention:
// node_lifecycle.go's quiesceForLiveLifecycleOp escalates a connManager.Stop
// failure to errStorageDrainUnconfirmed (the same as this method's own
// unconfirmed-drain error below), specifically because connManager.Stop
// returning an error means it could not confirm every connection/listener
// goroutine actually exited -- i.e. this method's precondition may not
// hold. That escalation makes the caller take the full-supervised-restart
// path (n.cancel()) instead of ever reaching reinitializeAndResume, so a
// straggling connection's Leios fetch can no longer race this reset in
// practice, not just "shouldn't" by convention.
//
// Returns ErrLeiosPersistDrainUnconfirmed, without resetting anything, if
// the drain wait timed out: the old writer goroutine may still be running
// drainLeiosPersist against the old database and the shared pending map,
// so resetting leiosPersistOnce here would let the very next enqueue start
// a second writer against a freshly reset map and channels while the old
// one is still reading and deleting from that same map (now repointed)
// under leiosPersistMu — silently stealing jobs meant for the new database
// and, worse, writing them into the old one via the stale captured db
// reference. The caller must not proceed to close storage or attempt
// reinitializeAndResume in that case; it must escalate to a supervised
// restart instead, the same as errStorageDrainUnconfirmed.
func (o *Ouroboros) PauseLeiosPersistWriterForLiveLifecycleOp() error {
	if !o.stopLeiosPersistWriter(leiosPersistShutdownDrainTimeout) {
		return ErrLeiosPersistDrainUnconfirmed
	}
	o.leiosPersistOnce = sync.Once{}
	o.leiosPersistStopOnce = sync.Once{}
	o.leiosPersistStarted.Store(false)
	return nil
}
