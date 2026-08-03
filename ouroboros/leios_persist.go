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
type leiosPersistJob struct {
	slot        uint64
	hash        []byte
	manifestRaw []byte
	txsRaw      []cbor.RawMessage
}

// enqueueLeiosPersist queues an endorser block for asynchronous blob-store
// persistence (historical serving) instead of writing it synchronously on the
// leios-fetch hot path. Jobs coalesce by hash: a complete job (carrying txs)
// supersedes a manifest-only one for the same hash, so the backfiller's
// manifest-only-then-complete pair collapses to a single write. Best-effort: a
// full queue drops the write; no error is surfaced to the caller.
func (o *Ouroboros) enqueueLeiosPersist(
	point ocommon.Point,
	blockRaw []byte,
	data *leiosEndorserBlockData,
) {
	if o.leiosDatabase() == nil {
		return
	}
	o.leiosPersistOnce.Do(o.startLeiosPersistWriter)
	job := &leiosPersistJob{
		slot:        point.Slot,
		hash:        slices.Clone(point.Hash),
		manifestRaw: slices.Clone(blockRaw),
	}
	if data != nil && data.completeTxCache() && data.txCount > 0 {
		job.txsRaw = cloneRawMessages(data.txsRaw)
	}
	key := string(job.hash)
	o.leiosPersistMu.Lock()
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
		o.leiosPersistMu.Unlock()
		return
	default:
	}
	if existing := o.leiosPersistPending[key]; existing != nil {
		// Never let a manifest-only job overwrite one that already carries
		// txs — that would re-introduce the duplicate manifest write and lose
		// the tx bodies from the pending write.
		if existing.txsRaw != nil && job.txsRaw == nil {
			o.leiosPersistMu.Unlock()
			return
		}
	} else if len(o.leiosPersistPending) >= leiosPersistMaxPending {
		o.leiosPersistMu.Unlock()
		if n := o.leiosPersistDropped.Add(1); n%256 == 1 {
			o.config.Logger.Warn(
				"leios EB persistence queue full; dropping historical-serving write",
				"component", "network",
				"slot", point.Slot,
				"dropped_total", n,
			)
		}
		return
	}
	o.leiosPersistPending[key] = job
	o.leiosPersistMu.Unlock()
	select {
	case o.leiosPersistSignal <- struct{}{}:
	default:
	}
}

// startLeiosPersistWriter initializes the writer state and launches the single
// background writer goroutine. Runs exactly once via leiosPersistOnce, before
// any enqueue proceeds past the Once, so the map and channels are safely
// published to concurrent enqueuers.
func (o *Ouroboros) startLeiosPersistWriter() {
	o.leiosPersistPending = make(map[string]*leiosPersistJob)
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
			"component", "network",
			"timeout", drainTimeout,
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
