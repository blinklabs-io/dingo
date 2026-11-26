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
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/database/leiosdb"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// leiosPersistShutdownDrainTimeout bounds how long Close and
// PauseLeiosPersistWriterForLiveLifecycleOp wait for in-flight endorser-block
// writes and for the GC worker. A write calls Database.SetLeiosEB, which can
// block indefinitely on a stuck or slow store, and that must never hang a
// graceful shutdown or a live restore; the wait is abandoned and the caller
// told the drain is unconfirmed. Package-level var (not const) so tests can
// shrink it instead of running a real multi-second timeout.
var leiosPersistShutdownDrainTimeout = 5 * time.Second

// ErrLeiosPersistDrainUnconfirmed is returned when a bounded wait gives up
// before confirming that in-flight endorser-block writes finished and the GC
// worker exited. Callers must not close or replace storage while either may
// still use it.
var ErrLeiosPersistDrainUnconfirmed = errors.New(
	"leios persistence worker drain not confirmed before timeout",
)

// errLeiosPersistStopped refuses a write after Close or a live-lifecycle
// pause: the store it would reach is being, or has been, closed or replaced.
var errLeiosPersistStopped = errors.New("leios persistence is stopped")

// errLeiosPersistWrite marks a failure of the local LeiosDB rather than of
// the peer that supplied the content.
var errLeiosPersistWrite = errors.New("persist leios endorser block")

// persistLeiosEndorserBlock writes an endorser block to the LeiosDB before
// returning, so a caller that reports success, or offers the block onward,
// only does so once the block survives a restart. txsRaw is written only for
// a complete transaction set; a manifest-only entry writes the manifest.
// Writing the same occurrence twice is harmless: records are keyed by
// (slot, hash) and the content is verified against the manifest hash.
//
// It reports whether the content is now durable, for
// markLeiosEndorserBlockDurable. With no database it does nothing and reports
// false, so no entry is ever marked durable without one.
func (o *Ouroboros) persistLeiosEndorserBlock(
	point ocommon.Point,
	blockRaw []byte,
	data *leiosEndorserBlockData,
) (bool, error) {
	if o.leiosDatabase() == nil {
		return false, nil
	}
	if !o.beginLeiosPersistWrite() {
		return false, errLeiosPersistStopped
	}
	defer o.endLeiosPersistWrite()
	// Read again after registering: a pause waits for registered writes
	// before storage is replaced, so this is the store the write belongs to.
	db := o.leiosDatabase()
	if db == nil {
		return false, nil
	}
	var txsRaw []cbor.RawMessage
	if data != nil && data.completeTxCache() && data.txCount > 0 {
		txsRaw = data.txsRaw
	}
	o.leiosPersistStoreMu.Lock()
	// A pinned endorser block below the frontier is still persisted: it is
	// certified by an immutable ranking block, and replaying that block
	// needs the closure. The store itself refuses an unpinned write below
	// its collection watermark; the frontier check only avoids the round
	// trip while the watermark catches up.
	if point.Slot < o.leiosPersistPruneBeforeSlot {
		pinned, err := db.IsLeiosEBPinned(point.Slot, point.Hash)
		// A failed lookup persists: an extra record is collected later, a
		// missing certified one is not recoverable.
		if err == nil && !pinned {
			o.leiosPersistStoreMu.Unlock()
			return true, nil
		}
	}
	err := db.SetLeiosEB(point.Slot, point.Hash, blockRaw, txsRaw)
	o.leiosPersistStoreMu.Unlock()
	if errors.Is(err, leiosdb.ErrBehindImmutableTip) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf(
			"%w at slot %d: %w",
			errLeiosPersistWrite,
			point.Slot,
			err,
		)
	}
	// Without an immutable point the collection bound follows the newest
	// persisted endorser block, which only advances here. Ledger block events
	// alone would leave records above the last bound uncollected while the
	// chain is quiet.
	o.signalLeiosGC(event.Event{})
	return true, nil
}

// beginLeiosPersistWrite registers a write so that Close and a pause wait for
// it. The lifecycle lock is held only to register, never across the write, so
// a stuck store blocks no lifecycle operation past its bounded wait.
func (o *Ouroboros) beginLeiosPersistWrite() bool {
	o.leiosPersistLifecycleMu.RLock()
	defer o.leiosPersistLifecycleMu.RUnlock()
	o.leiosPersistMu.Lock()
	if o.leiosPersistClosed {
		o.leiosPersistMu.Unlock()
		return false
	}
	if o.leiosPersistInflight == 0 {
		o.leiosPersistIdle = make(chan struct{})
	}
	o.leiosPersistInflight++
	o.leiosPersistMu.Unlock()
	// Started here rather than at construction for an Ouroboros whose
	// database arrives later.
	o.startLeiosPersistenceGC(0, false)
	return true
}

func (o *Ouroboros) endLeiosPersistWrite() {
	o.leiosPersistMu.Lock()
	defer o.leiosPersistMu.Unlock()
	o.leiosPersistInflight--
	if o.leiosPersistInflight == 0 {
		close(o.leiosPersistIdle)
	}
}

// leiosPersistWritesIdle returns a channel that is closed once no write is in
// flight, or nil when none is.
func (o *Ouroboros) leiosPersistWritesIdle() <-chan struct{} {
	o.leiosPersistMu.Lock()
	defer o.leiosPersistMu.Unlock()
	if o.leiosPersistInflight == 0 {
		return nil
	}
	return o.leiosPersistIdle
}

// leiosEndorserBlockDurable reports whether data's current content was
// written to the LeiosDB.
func (o *Ouroboros) leiosEndorserBlockDurable(
	data *leiosEndorserBlockData,
) bool {
	return data != nil && data.durable
}

// leiosEndorserBlockNeedsWrite reports whether data holds slot-verified
// content that has a database to be written to and has not been. With no
// database nothing is ever durable, so it is excluded: there is nothing to
// write and nothing to wait for.
func (o *Ouroboros) leiosEndorserBlockNeedsWrite(
	data *leiosEndorserBlockData,
) bool {
	return data != nil && data.slotVerified && !data.durable &&
		o.leiosDatabase() != nil
}

// persistVerifiedLeiosEndorserBlock writes a slot-verified entry unless its
// content is already durable, and records the write on the cached entry so
// that re-offers of the same content do not rewrite it.
func (o *Ouroboros) persistVerifiedLeiosEndorserBlock(
	point ocommon.Point,
	blockRaw []byte,
	data *leiosEndorserBlockData,
) error {
	if !o.leiosEndorserBlockNeedsWrite(data) {
		return nil
	}
	written, err := o.persistLeiosEndorserBlock(point, blockRaw, data)
	if err != nil {
		return err
	}
	o.markLeiosEndorserBlockDurable(point, data, written)
	return nil
}

// markLeiosEndorserBlockDurable records that the cached entry for point holds
// content that was written. The entry may have been replaced since data was
// read; it is marked only while it still holds the content that was written.
// Readers hold cached pointers without leiosMu, so the entry is replaced with
// a marked copy rather than changed in place.
func (o *Ouroboros) markLeiosEndorserBlockDurable(
	point ocommon.Point,
	data *leiosEndorserBlockData,
	written bool,
) {
	if !written {
		return
	}
	key := leiosBlockKey(point.Slot, point.Hash)
	o.leiosMu.Lock()
	defer o.leiosMu.Unlock()
	current := o.leiosEndorserBlocks[key]
	if current == nil || current.durable ||
		!current.slotVerified || !current.hasManifest() ||
		len(current.txsRaw) != len(data.txsRaw) {
		return
	}
	marked := *current
	marked.durable = true
	for _, cacheKey := range current.cacheKeys {
		if o.leiosEndorserBlocks[cacheKey] == current {
			o.leiosEndorserBlocks[cacheKey] = &marked
		}
	}
}

// startLeiosPersistenceGC starts the optional pruning worker. initialMaxSlot is supplied after the startup
// watermark scan so enabling GC does not repeat that full scan immediately.
func (o *Ouroboros) startLeiosPersistenceGC(
	initialMaxSlot uint64,
	initialMaxKnown bool,
) {
	if o.leiosPersistenceGCDisabled() || o.leiosDatabase() == nil {
		return
	}
	o.leiosPersistGCOnce.Do(func() {
		stop := make(chan struct{})
		done := make(chan struct{})
		ctx, cancel := context.WithCancel(o.leiosValidationCtx)
		o.leiosPersistGCMu.Lock()
		o.leiosPersistMu.Lock()
		if o.leiosPersistClosed {
			o.leiosPersistMu.Unlock()
			o.leiosPersistGCMu.Unlock()
			cancel()
			return
		}
		o.leiosPersistGCStop = stop
		o.leiosPersistGCCancel = cancel
		o.leiosPersistGCDone = done
		o.leiosPersistGCStarted.Store(true)
		o.leiosPersistMu.Unlock()
		o.leiosPersistGCMu.Unlock()
		go o.leiosPersistGCLoop(
			ctx,
			stop,
			done,
			initialMaxSlot,
			initialMaxKnown,
		)
	})
}

// leiosPersistenceGCDisabled reports whether no retention rule can apply. A
// zero retention means the default, which prunes behind the immutable point and
// so needs a ledger to supply it.
func (o *Ouroboros) leiosPersistenceGCDisabled() bool {
	return o.config.LeiosPersistenceRetentionSlots == 0 &&
		o.leiosImmutableSlot == nil
}

func (o *Ouroboros) leiosPersistGCLoop(
	ctx context.Context,
	stop <-chan struct{},
	done chan<- struct{},
	initialMaxSlot uint64,
	initialMaxKnown bool,
) {
	defer close(done)
	var initialMax *uint64
	if initialMaxKnown {
		initialMax = &initialMaxSlot
	}
	o.runLeiosPersistenceGC(ctx, initialMax)

	// Collection follows the immutable tip, so it runs when the primary chain
	// changes rather than on a timer. A signal that arrives during a pass
	// leaves one wakeup pending, which is the next pass.
	signal := o.leiosGCSignal
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-signal:
			o.runLeiosPersistenceGC(ctx, nil)
		}
	}
}

// signalLeiosGC wakes the collection worker. It never blocks.
func (o *Ouroboros) signalLeiosGC(event.Event) {
	select {
	case o.leiosGCSignal <- struct{}{}:
	default:
	}
}

func (o *Ouroboros) runLeiosPersistenceGC(
	ctx context.Context,
	knownMaxSlot *uint64,
) {
	retention := o.config.LeiosPersistenceRetentionSlots
	if o.leiosPersistenceGCDisabled() || ctx.Err() != nil {
		return
	}
	db := o.leiosDatabase()
	if db == nil {
		return
	}
	store, err := db.LeiosDB()
	if err != nil {
		o.config.Logger.Warn("failed to open leios store for pruning", "error", err)
		return
	}
	// An uncertified endorser block at or after the immutable point can still
	// be certified, so no retention setting collects past it, and nothing is
	// collected while the ledger reports none or fails to read its chain:
	// ledger apply writes a certified endorser block's records before the
	// certifying ranking block commits, and a frontier past that block would
	// let a concurrent sweep delete them in between. With no ledger, or one
	// with no chain or security parameter to measure the point with, the
	// configured window applies, at least the stability window.
	var (
		immutableSlot  uint64
		immutableKnown bool
	)
	if o.leiosImmutableSlot != nil {
		var err error
		immutableSlot, immutableKnown, err = o.leiosImmutableSlot(ctx)
		switch {
		case err != nil:
			if !errors.Is(err, context.Canceled) {
				o.config.Logger.Warn(
					"failed to resolve immutable point for leios pruning",
					"error", err,
				)
			}
			// The default has no window to fall back on, and a ledger that
			// could not read its chain may be applying a certified endorser
			// block below the configured window's frontier.
			if retention == 0 ||
				!errors.Is(err, ledger.ErrImmutablePointUnconfigured) {
				return
			}
			immutableSlot, immutableKnown = 0, false
		case !immutableKnown:
			return
		}
	}
	// Certifications up to the immutable point are pinned before anything is
	// collected, so a certified endorser block leaves the volatile partition
	// by promotion, never by deletion. A failed walk collects nothing. When
	// the immutable point could not be resolved there is nothing new to pin,
	// but the pin watermark already stored still bounds the collection.
	var pinnedThrough *uint64
	if o.leiosPinAdvance != nil {
		if immutableKnown {
			if err := o.leiosPinAdvance(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					if o.leiosMetrics != nil {
						o.leiosMetrics.pinWalkFailures.Inc()
					}
					o.config.Logger.Error(
						"failed to pin certified leios endorser blocks; "+
							"collection is stalled until the pin walk succeeds",
						"error", err,
					)
				}
				return
			}
		}
		wm, err := store.Watermarks()
		if err != nil {
			o.config.Logger.Warn("failed to read leios watermarks", "error", err)
			return
		}
		switch {
		case wm.PinnedThroughSet:
			pinnedThrough = &wm.PinnedThrough.Slot
		case immutableKnown:
			// The chain has no block to pin through yet.
			return
		}
	}
	var candidateBeforeSlot uint64
	if retention == 0 {
		if !immutableKnown {
			return
		}
		candidateBeforeSlot = immutableSlot
	} else {
		var maxSlot uint64
		if knownMaxSlot != nil {
			maxSlot = *knownMaxSlot
		} else {
			maxSlot, _, err = store.MaxSlot()
			if err != nil {
				o.config.Logger.Warn(
					"failed to find the newest persisted leios endorser block for pruning",
					"error", err,
				)
				return
			}
		}
		if maxSlot > retention {
			candidateBeforeSlot = maxSlot - retention
		}
		if immutableKnown {
			candidateBeforeSlot = min(candidateBeforeSlot, immutableSlot)
		}
	}
	// A certification is pinned once its certifying ranking block is walked,
	// and that block is the child of the block that announced the endorser
	// block, so an endorser block announced strictly below the pin watermark
	// has been decided.
	if pinnedThrough != nil {
		candidateBeforeSlot = min(candidateBeforeSlot, *pinnedThrough)
	}
	if ctx.Err() != nil {
		return
	}
	// Writers hold this lock for SetLeiosEB. Publishing the cutoff first means
	// an in-flight write completes before the collection, while later writes
	// below it are rejected.
	o.leiosPersistStoreMu.Lock()
	// The bounds above are read from the store and the ledger on this pass, so
	// they replace the previous frontier even when lower: a pin rollback lowers
	// the pin watermark and demotes records, and collecting against the
	// earlier, higher frontier would delete them.
	o.leiosPersistPruneBeforeSlot = candidateBeforeSlot
	beforeSlot := o.leiosPersistPruneBeforeSlot
	o.leiosPersistStoreMu.Unlock()
	if beforeSlot == 0 || ctx.Err() != nil {
		return
	}
	deleted, err := store.GarbageCollect(ctx, beforeSlot)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			o.config.Logger.Warn(
				"failed to prune persisted leios endorser blocks",
				"before_slot", beforeSlot,
				"error", err,
			)
		}
		return
	}
	if deleted > 0 {
		o.config.Logger.Debug(
			"pruned persisted leios endorser blocks",
			"before_slot", beforeSlot,
			"deleted_keys", deleted,
		)
	}
}

func (o *Ouroboros) stopLeiosPersistenceWorkers(timeout time.Duration) bool {
	return o.stopLeiosPersistenceWorkersWithWait(
		timeout,
		waitForLeiosPersistWorker,
	)
}

// stopLeiosPersistenceWorkersWithWait waits for in-flight writes and stops the
// GC worker, giving each its own bounded wait. The caller holds
// leiosPersistLifecycleMu exclusively, so no new write registers meanwhile.
func (o *Ouroboros) stopLeiosPersistenceWorkersWithWait(
	timeout time.Duration,
	wait func(<-chan struct{}, time.Duration) bool,
) bool {
	gcDone, gcStarted := o.requestLeiosPersistenceGCStop()
	writesDone := true
	if idle := o.leiosPersistWritesIdle(); idle != nil {
		writesDone = wait(idle, timeout)
		if !writesDone {
			o.config.Logger.Warn(
				"timed out waiting for leios endorser-block writes to finish",
				"component", "network",
				"timeout", timeout,
			)
		}
	}
	gcStopped := true
	if gcStarted {
		gcStopped = wait(gcDone, timeout)
		if gcStopped {
			o.leiosPersistGCStarted.Store(false)
		} else {
			o.config.Logger.Warn(
				"timed out waiting for leios persistence GC to stop",
				"component", "network",
				"timeout", timeout,
			)
		}
	}
	return writesDone && gcStopped
}

func (o *Ouroboros) requestLeiosPersistenceGCStop() (<-chan struct{}, bool) {
	if !o.leiosPersistGCStarted.Load() {
		return nil, false
	}
	o.leiosPersistGCMu.Lock()
	stop := o.leiosPersistGCStop
	cancel := o.leiosPersistGCCancel
	done := o.leiosPersistGCDone
	o.leiosPersistGCStopOnce.Do(func() { close(stop) })
	o.leiosPersistGCMu.Unlock()
	// Cancel here rather than waiting for the worker to schedule a watcher.
	if cancel != nil {
		cancel()
	}
	return done, true
}

func waitForLeiosPersistWorker(
	done <-chan struct{},
	timeout time.Duration,
) bool {
	select {
	case <-done:
		return true
	default:
	}
	if timeout <= 0 {
		return false
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		// The worker can exit as the timer fires. Recheck its completion before
		// reporting an unconfirmed drain so a ready done channel always wins.
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

// PauseLeiosPersistWriterForLiveLifecycleOp refuses every later endorser-block
// write, waits for those already in flight against the current, about-to-close
// database, and stops the optional GC worker (canceling its current sweep).
//
// The refusal is permanent, like Close's. A live Restore/Truncate discards
// this Ouroboros and builds a replacement whose cache starts empty and reloads
// from the new database, so nothing on this instance may write once storage is
// being closed. Fetch workers and subscribed handlers still running here
// would otherwise register a write against the closing store.
//
// Call after inbound traffic stops. The lifecycle lock excludes new writes
// while this waits. If connection shutdown is unconfirmed, the caller must use
// a supervised restart rather than replacing storage while traffic may still
// access it.
//
// Returns ErrLeiosPersistDrainUnconfirmed if a write or the GC misses its
// deadline: a write may still be running against the old database, or the GC
// may still be scanning or deleting through its pinned blob store. The caller
// must not proceed to close or replace storage in that case; it must escalate
// to a supervised restart, the same as errStorageDrainUnconfirmed.
func (o *Ouroboros) PauseLeiosPersistWriterForLiveLifecycleOp() error {
	o.leiosPersistLifecycleMu.Lock()
	defer o.leiosPersistLifecycleMu.Unlock()

	o.leiosPersistMu.Lock()
	o.leiosPersistClosed = true
	o.leiosPersistMu.Unlock()
	if !o.stopLeiosPersistenceWorkers(leiosPersistShutdownDrainTimeout) {
		return ErrLeiosPersistDrainUnconfirmed
	}
	return nil
}
