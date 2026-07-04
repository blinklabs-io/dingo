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
	"slices"
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
const leiosPersistShutdownDrainTimeout = 5 * time.Second

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
// exercise the bounded wait without the production timeout.
func (o *Ouroboros) stopLeiosPersistWriter(drainTimeout time.Duration) {
	if !o.leiosPersistStarted.Load() {
		return
	}
	// Always close the stop channel so the writer observes the stop and exits,
	// even if we stop waiting for it below.
	o.leiosPersistStopOnce.Do(func() { close(o.leiosPersistStop) })
	timer := time.NewTimer(drainTimeout)
	defer timer.Stop()
	select {
	case <-o.leiosPersistDone:
	case <-timer.C:
		// The drain is stuck (likely a slow/unavailable blob store). Abandon the
		// wait: historical-serving persistence is best-effort and the writer
		// goroutine will still exit once the store unblocks.
		o.config.Logger.Warn(
			"timed out waiting for leios EB persistence writer to drain; abandoning remaining historical-serving writes",
			"component", "network",
			"timeout", drainTimeout,
		)
	}
}
