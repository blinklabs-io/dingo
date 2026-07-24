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
)

// cancellableBarrier is a reader/writer barrier with the same read-side
// semantics as sync.RWMutex (many concurrent RLock holders; a waiting or
// held Lock blocks new RLock calls, so a writer cannot starve forever
// under continuous reader traffic), but whose exclusive side additionally
// supports a cancellable acquire (LockContext). Its zero value is a valid,
// unlocked barrier, matching sync.Mutex/sync.RWMutex's own zero-value
// contract — no explicit construction step required.
//
// A plain sync.RWMutex has no way to abandon a queued Lock() call: Go's
// runtime gives writers preference, so once a Lock() is queued, every
// subsequent RLock() blocks behind it regardless of whether the original
// caller waiting on that Lock() ever gives up. A caller that "cancels" by
// simply no longer waiting on the result (e.g. a select against ctx.Done()
// racing the blocked Lock() call in a background goroutine) therefore
// leaves a phantom writer in the queue that keeps blocking new readers
// until whatever reader it was waiting on eventually releases — exactly
// as long a stall as if the cancellation had never happened.
// cancellableBarrier's LockContext instead fully withdraws a cancelled
// waiter's claim on the writer slot, so giving up truly gives up: new
// readers are not blocked by an abandoned exclusive-acquire attempt.
type cancellableBarrier struct {
	mu            sync.Mutex
	readers       int
	writerWaiting bool
	// changed is closed and replaced every time a state transition might
	// unblock a waiter (RUnlock reaching zero readers, Unlock clearing
	// writerWaiting) — the standard "broadcast channel" pattern for a
	// condition variable that also needs to support a cancellable wait,
	// which sync.Cond cannot: Cond.Wait has no way to also select on
	// ctx.Done(). Lazily initialized (see changedLocked) so the zero
	// value of cancellableBarrier needs no explicit construction.
	changed chan struct{}
}

// changedLocked returns the current broadcast channel, initializing it on
// first use. Callers must hold mu.
func (b *cancellableBarrier) changedLocked() chan struct{} {
	if b.changed == nil {
		b.changed = make(chan struct{})
	}
	return b.changed
}

// notifyLocked wakes every current waiter. Callers must hold mu.
func (b *cancellableBarrier) notifyLocked() {
	if b.changed != nil {
		close(b.changed)
	}
	b.changed = make(chan struct{})
}

// RLock acquires the shared side: blocks while a writer is waiting for or
// holding exclusive access, otherwise proceeds immediately alongside any
// other concurrent reader.
func (b *cancellableBarrier) RLock() {
	b.mu.Lock()
	for b.writerWaiting {
		ch := b.changedLocked()
		b.mu.Unlock()
		<-ch
		b.mu.Lock()
	}
	b.readers++
	b.mu.Unlock()
}

// RUnlock releases the shared side.
func (b *cancellableBarrier) RUnlock() {
	b.mu.Lock()
	b.readers--
	if b.readers < 0 {
		b.mu.Unlock()
		panic("cancellableBarrier: RUnlock without matching RLock")
	}
	if b.readers == 0 {
		b.notifyLocked()
	}
	b.mu.Unlock()
}

// Lock acquires the exclusive side unconditionally, exactly like
// sync.RWMutex.Lock: waits its turn behind any other writer already
// waiting or holding, then blocks until every current reader releases,
// then blocks every new reader (and any other writer) until Unlock.
func (b *cancellableBarrier) Lock() {
	// context.Background() never cancels, so this is equivalent to an
	// uncancellable Lock — but still shares lockContext's single
	// implementation of the actual wait/claim logic below.
	_ = b.lockContext(context.Background())
}

// Unlock releases the exclusive side.
func (b *cancellableBarrier) Unlock() {
	b.mu.Lock()
	b.writerWaiting = false
	b.notifyLocked()
	b.mu.Unlock()
}

// LockContext is Lock, but ctx can abandon the wait: if ctx is cancelled
// before the exclusive side is acquired, this returns ctx.Err() having
// fully withdrawn its claim on the writer slot — unlike a sync.RWMutex-
// based implementation, no phantom queued writer is left behind to block
// new readers via writer preference (see the type's doc comment).
func (b *cancellableBarrier) LockContext(ctx context.Context) error {
	return b.lockContext(ctx)
}

func (b *cancellableBarrier) lockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.mu.Lock()
	for b.writerWaiting {
		ch := b.changedLocked()
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		b.mu.Lock()
	}
	b.writerWaiting = true
	for b.readers > 0 {
		ch := b.changedLocked()
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			b.mu.Lock()
			b.writerWaiting = false
			b.notifyLocked()
			b.mu.Unlock()
			return ctx.Err()
		}
		b.mu.Lock()
	}
	b.mu.Unlock()
	return nil
}
