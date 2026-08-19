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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// decodeCacheTestConnId returns a ConnectionId with valid net.Addr values
// (mirrors testConnId in blockfetch_test.go). The real chainsync/blockfetch
// handlers call ConnectionId.String() for logging, which dereferences
// LocalAddr/RemoteAddr directly and panics on the zero value -- a bare
// ConnectionId{} is not a safe stand-in for "no particular connection" here.
func decodeCacheTestConnId() ouroboros_conn.ConnectionId {
	return ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
}

// --- Part 1: pure decodeCache[T] unit tests -------------------------------
//
// These exercise the generic cache mechanism in isolation from any real CBOR
// decoding, so the correctness properties (hit/miss, collision safety,
// eviction, concurrency) are proven independently of what T happens to be.

// decodeCacheLen reports the current entry count. Test-only introspection
// into cache internals, kept out of the production file since nothing there
// needs it.
func decodeCacheLen[T any](c *decodeCache[T]) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// decodeCacheInsertForTest inserts an entry the same way getOrDecode does --
// into both entries and the order list -- so tests exercising eviction
// directly (bypassing getOrDecode) still populate the FIFO order eviction
// relies on. The caller must not hold c.mu.
func decodeCacheInsertForTest[T any](
	c *decodeCache[T],
	key decodeCacheKey,
	value T,
	insertedAt time.Time,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.order.PushBack(key)
	c.entries[key] = decodeCacheEntry[T]{
		value:      value,
		insertedAt: insertedAt,
	}
}

// countingDecoder returns a decode function that counts how many times it
// actually runs, and a pointer to the live count.
func countingDecoder(
	value int,
	err error,
	delay time.Duration,
) (func() (int, error), *atomic.Int64) {
	var calls atomic.Int64
	return func() (int, error) {
		calls.Add(1)
		if delay > 0 {
			time.Sleep(delay)
		}
		return value, err
	}, &calls
}

// TestDecodeCacheHitAvoidsRedecode checks the basic promise of the cache:
// decode something once, then ask for the same thing again, and confirm the
// second time reuses the answer instead of doing the work again.
func TestDecodeCacheHitAvoidsRedecode(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x01}
	decodeFn, calls := countingDecoder(42, nil, 0)

	value, err, decoded := c.getOrDecode(key, decodeFn)
	require.NoError(t, err)
	require.Equal(t, 42, value)
	require.True(t, decoded, "first call for a new key must be a real decode")
	require.EqualValues(t, 1, calls.Load())

	value, err, decoded = c.getOrDecode(key, decodeFn)
	require.NoError(t, err)
	require.Equal(t, 42, value)
	require.False(
		t,
		decoded,
		"second call for the same key must be a cache hit",
	)
	require.EqualValues(
		t, 1, calls.Load(),
		"decodeFn must not run again on a cache hit",
	)
}

// TestDecodeCacheDifferentKeysDecodeIndependently checks that two different
// pieces of data never get mixed up with each other: each one is decoded
// and remembered on its own, with no cross-contamination between them.
func TestDecodeCacheDifferentKeysDecodeIndependently(t *testing.T) {
	c := newDecodeCache[int]()
	keyA := decodeCacheKey{0xAA}
	keyB := decodeCacheKey{0xBB}
	decodeA, callsA := countingDecoder(1, nil, 0)
	decodeB, callsB := countingDecoder(2, nil, 0)

	valueA, errA, decodedA := c.getOrDecode(keyA, decodeA)
	valueB, errB, decodedB := c.getOrDecode(keyB, decodeB)

	require.NoError(t, errA)
	require.NoError(t, errB)
	require.Equal(t, 1, valueA)
	require.Equal(t, 2, valueB)
	require.True(t, decodedA)
	require.True(t, decodedB)
	require.EqualValues(
		t, 1, callsA.Load(),
		"a different key must never be satisfied from another key's entry",
	)
	require.EqualValues(t, 1, callsB.Load())
}

// TestDecodeCacheFailureIsCachedAndNotRetried checks what happens when
// something fails to decode (bad/broken data): a repeat of that same broken
// data must not be retried -- it should just remember "this one failed" and
// return the same answer again without redoing the work.
func TestDecodeCacheFailureIsCachedAndNotRetried(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x02}
	wantErr := errors.New("malformed input")
	decodeFn, calls := countingDecoder(0, wantErr, 0)

	_, err, decoded := c.getOrDecode(key, decodeFn)
	require.ErrorIs(t, err, wantErr)
	require.True(t, decoded)
	require.EqualValues(t, 1, calls.Load())

	// A second submission of the identical (bad) bytes must reuse the
	// cached failure, not re-attempt a decode that -- since decoding is a
	// pure function of its input -- can only ever fail the same way again.
	_, err, decoded = c.getOrDecode(key, decodeFn)
	require.ErrorIs(t, err, wantErr)
	require.False(t, decoded)
	require.EqualValues(t, 1, calls.Load())
}

// TestDecodeCachePrunesExpiredEntries checks that old entries actually get
// cleared out after enough time passes, so the cache doesn't just grow
// forever.
func TestDecodeCachePrunesExpiredEntries(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x03}
	decodeCacheInsertForTest(
		c, key, 7, time.Now().Add(-decodeCacheTTL-time.Second),
	)
	require.Equal(t, 1, decodeCacheLen(c))

	c.mu.Lock()
	c.evictLocked(time.Now())
	c.mu.Unlock()

	require.Zero(t, decodeCacheLen(c), "expired entry must be pruned")
}

// TestDecodeCacheExpiredEntryIsNotServedOnLookup is the regression test for
// a real gap found in code review: eviction was entirely insertion-
// triggered (evictLocked only ever ran from finishDecode, i.e. on a genuine
// miss completing), so a key that keeps getting hit -- with no other key
// ever decoded again in the meantime -- would never have its own age
// re-checked and could be served from cache long past decodeCacheTTL. This
// inserts an already-expired entry directly (bypassing getOrDecode, the
// same way TestDecodeCachePrunesExpiredEntries does) and confirms a lookup
// for that exact key detects the expiry itself, discards the stale entry,
// and falls through to a genuine fresh decode instead of returning the
// past-TTL cached value.
func TestDecodeCacheExpiredEntryIsNotServedOnLookup(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x09}
	decodeCacheInsertForTest(
		c, key, 111, time.Now().Add(-decodeCacheTTL-time.Second),
	)
	require.Equal(t, 1, decodeCacheLen(c))

	decodeFn, calls := countingDecoder(222, nil, 0)
	value, err, decoded := c.getOrDecode(key, decodeFn)
	require.NoError(t, err)
	require.True(
		t, decoded,
		"a lookup past the entry's TTL must be a genuine decode, not a hit",
	)
	require.Equal(
		t, 222, value,
		"the stale cached value must not be returned once past its TTL",
	)
	require.EqualValues(t, 1, calls.Load())

	// The fresh decode is now the current cached entry: a second lookup
	// immediately afterward must be a normal hit again.
	value, err, decoded = c.getOrDecode(key, decodeFn)
	require.NoError(t, err)
	require.False(t, decoded)
	require.Equal(t, 222, value)
	require.EqualValues(t, 1, calls.Load())
}

// TestDecodeCachePrunesBySizeWhenOverCapacity checks the other cleanup
// rule: if the cache gets too full, it removes the oldest entries to make
// room, instead of growing without limit.
func TestDecodeCachePrunesBySizeWhenOverCapacity(t *testing.T) {
	c := newDecodeCache[int]()
	now := time.Now()
	// Insert one more than the cap, all fresh (no TTL pruning triggers),
	// with strictly increasing insertedAt so eviction order is
	// deterministic: the single oldest entry must be the one dropped.
	for i := range decodeCacheMaxEntries + 1 {
		var key decodeCacheKey
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		decodeCacheInsertForTest(
			c, key, i, now.Add(time.Duration(i)*time.Millisecond),
		)
	}
	require.Equal(t, decodeCacheMaxEntries+1, decodeCacheLen(c))

	c.mu.Lock()
	c.evictLocked(
		now.Add(time.Duration(decodeCacheMaxEntries) * time.Millisecond),
	)
	c.mu.Unlock()

	require.Equal(
		t, decodeCacheMaxEntries, decodeCacheLen(c),
		"size-based pruning must trim down to exactly the cap",
	)
	var oldestKey decodeCacheKey
	oldestKey[0] = 0
	oldestKey[1] = 0
	_, stillPresent := c.entries[oldestKey]
	require.False(
		t,
		stillPresent,
		"the single oldest entry must be evicted first",
	)
}

// TestDecodeCacheConcurrentCallersShareOneDecode is the core correctness
// property: many goroutines submitting the identical key at the same time
// must trigger exactly one real decode, and every goroutine -- whether it
// ran the decode, hit the cache, or waited on the in-flight attempt -- must
// receive the identical, correct result. Run with -race.
func TestDecodeCacheConcurrentCallersShareOneDecode(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x04}
	const wantValue = 123
	const numCallers = 50
	// A small delay widens the race window so concurrent callers reliably
	// land on the in-flight-wait path rather than the already-cached path.
	decodeFn, calls := countingDecoder(wantValue, nil, 5*time.Millisecond)

	var wg sync.WaitGroup
	results := make([]int, numCallers)
	errs := make([]error, numCallers)
	for i := range numCallers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx], _ = c.getOrDecode(key, decodeFn)
		}(i)
	}
	wg.Wait()

	require.EqualValues(
		t, 1, calls.Load(),
		"concurrent callers for the same key must share exactly one decode",
	)
	for i := range numCallers {
		require.NoError(t, errs[i])
		require.Equal(
			t,
			wantValue,
			results[i],
			"caller %d got a wrong result",
			i,
		)
	}
}

// TestDecodeCacheConcurrentDifferentKeysDoNotSerialize proves the lock is
// never held across the decode call itself: two distinct keys whose decode
// functions each block until released must be able to be in flight at the
// same time, not forced one-after-another by a shared lock.
func TestDecodeCacheConcurrentDifferentKeysDoNotSerialize(t *testing.T) {
	c := newDecodeCache[int]()
	keyA := decodeCacheKey{0xA1}
	keyB := decodeCacheKey{0xB1}

	bothStarted := make(chan struct{})
	var startedOnce sync.Once
	var started atomic.Int32
	release := make(chan struct{})

	blockingDecoder := func(value int) func() (int, error) {
		return func() (int, error) {
			if started.Add(1) == 2 {
				startedOnce.Do(func() { close(bothStarted) })
			}
			<-release
			return value, nil
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, _ = c.getOrDecode(keyA, blockingDecoder(1))
	}()
	go func() {
		defer wg.Done()
		_, _, _ = c.getOrDecode(keyB, blockingDecoder(2))
	}()

	select {
	case <-bothStarted:
		// Both decodes are concurrently in flight, proving the cache lock
		// was released before either decode ran.
	case <-time.After(5 * time.Second):
		close(release)
		wg.Wait()
		t.Fatal(
			"decodes for different keys serialized instead of overlapping " +
				"-- the cache is holding its lock across the decode call",
		)
	}
	close(release)
	wg.Wait()
}

// TestDecodeCacheNoGoroutineLeakOnFailure covers the failure-fan-out case
// discussed for #489: when N callers are waiting on one in-flight decode and
// it fails, every waiter must be woken with that failure, not left hanging.
func TestDecodeCacheNoGoroutineLeakOnFailure(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x05}
	wantErr := errors.New("bad bytes")
	release := make(chan struct{})
	leaderStarted := make(chan struct{})
	var leaderOnce sync.Once

	const numWaiters = 4
	results := make([]error, numWaiters)
	var wg sync.WaitGroup
	wg.Add(numWaiters)
	for i := range numWaiters {
		go func(idx int) {
			defer wg.Done()
			_, err, _ := c.getOrDecode(key, func() (int, error) {
				leaderOnce.Do(func() { close(leaderStarted) })
				<-release
				return 0, wantErr
			})
			results[idx] = err
		}(i)
	}

	<-leaderStarted
	// Give the non-leader goroutines time to register as waiters before
	// the leader's decode is released.
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inFlight[key]) == numWaiters-1
	}, 2*time.Second, 5*time.Millisecond)
	close(release)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiter never woke up after the in-flight decode failed")
	}

	for i, err := range results {
		require.ErrorIsf(
			t, err, wantErr,
			"waiter %d did not receive the shared failure", i,
		)
	}
	require.Zero(
		t, len(c.inFlight),
		"in-flight bookkeeping must be cleared after the attempt resolves",
	)
}

// TestDecodeCachePanicDuringDecodeDoesNotStrandWaitersOrKey is the regression
// test for a real bug found by proactively auditing this file for remaining
// gaps: decodeFn panicking (CBOR decode on adversarial bytes can, in
// principle, panic instead of erroring) used to leave the key's in-flight
// claim held and any concurrent waiters parked forever, since nothing ever
// closed their channels or released the claim. This confirms the fix: the
// leader's own panic still propagates to its immediate caller (unchanged
// crash-or-recover behavior for that goroutine), but every concurrent
// waiter is woken with a normal error instead of hanging, the in-flight
// claim is released, and -- since the panic is now a cached failure like
// any other -- a later call for the identical bytes fails fast without
// invoking decodeFn (and therefore without panicking) again.
func TestDecodeCachePanicDuringDecodeDoesNotStrandWaitersOrKey(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x06}
	release := make(chan struct{})
	leaderStarted := make(chan struct{})
	var leaderOnce sync.Once

	const numWaiters = 3
	results := make([]error, numWaiters)
	var wg sync.WaitGroup
	wg.Add(numWaiters)
	leaderPanicked := make(chan struct{})
	for i := range numWaiters {
		go func(idx int) {
			defer wg.Done()
			defer func() {
				// Only the leader (the one goroutine that actually calls
				// decodeFn) observes the panic here; recovering it mimics
				// whatever, if anything, sits upstream of the cache in
				// production, and lets this test assert on the effect on
				// the OTHER waiters without crashing the test binary.
				if r := recover(); r != nil {
					close(leaderPanicked)
				}
			}()
			_, err, _ := c.getOrDecode(key, func() (int, error) {
				leaderOnce.Do(func() { close(leaderStarted) })
				<-release
				panic("simulated decode panic")
			})
			results[idx] = err
		}(i)
	}

	<-leaderStarted
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inFlight[key]) == numWaiters-1
	}, 2*time.Second, 5*time.Millisecond)
	close(release)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a waiter never woke up after the in-flight decode panicked")
	}
	<-leaderPanicked

	nonLeaderErrs := 0
	for _, err := range results {
		if err != nil {
			nonLeaderErrs++
			require.Contains(t, err.Error(), "decode panicked")
		}
	}
	require.Equal(
		t, numWaiters-1, nonLeaderErrs,
		"every non-leader waiter must be woken with the recorded panic error",
	)
	require.Zero(
		t,
		len(c.inFlight),
		"in-flight bookkeeping must be cleared after a panic, not left claimed forever",
	)

	// The panic is now a cached failure like any other: a later call for
	// the identical key must return it immediately without invoking
	// decodeFn (and therefore without panicking) again.
	_, err, decoded := c.getOrDecode(key, func() (int, error) {
		t.Fatal("decodeFn must not run again for an already-cached panic")
		return 0, nil
	})
	require.False(t, decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode panicked")
}

// TestDecodeCachePanicNilDuringDecodeDoesNotStrandWaitersOrKey is the
// regression test for a P1 bug found in code review: getOrDecode used to
// decide "did decodeFn panic" by testing recover()'s return value for
// nilness. recover() also returns nil when there is no panic at all, so a
// bare panic(nil) inside decodeFn was indistinguishable from normal
// completion -- the recovery branch would never run, this key's in-flight
// claim would never be released, and every current and future waiter for
// these exact bytes would block forever. This confirms a panic(nil)
// decodeFn is still detected, still finishes the entry and wakes waiters,
// and still re-raises to the caller, exactly like panicking with any other
// value.
func TestDecodeCachePanicNilDuringDecodeDoesNotStrandWaitersOrKey(
	t *testing.T,
) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x0E}

	func() {
		defer func() {
			// recover() here returns the runtime's substituted, non-nil
			// *runtime.PanicNilError on modern Go rather than literal nil,
			// but the fix does not depend on that: it is unconditional on
			// completed, not on recover()'s value, so this holds either way.
			_ = recover()
		}()
		_, _, _ = c.getOrDecode(key, func() (int, error) {
			panic(nil)
		})
		t.Fatal("getOrDecode must still panic for a panic(nil) decodeFn")
	}()

	require.Zero(
		t,
		len(c.inFlight),
		"the in-flight claim must be released even when decodeFn panics with nil",
	)

	_, err, decoded := c.getOrDecode(key, func() (int, error) {
		t.Fatal("decodeFn must not run again for an already-cached panic")
		return 0, nil
	})
	require.False(t, decoded)
	require.Error(
		t,
		err,
		"the panic(nil) must still be recorded as a cached failure",
	)
	require.Contains(t, err.Error(), "decode panicked")
}

// TestDecodeWithPanicSafeMetricsRecordsMissOnPanic is the regression test for
// a real bug found in code review: getOrDecode's own panic recovery
// re-raises after cleaning up the cache (see
// TestDecodeCachePanicDuringDecodeDoesNotStrandWaitersOrKey above), so it
// never returns normally to blockfetchClientBlockRaw/
// chainsyncClientRollForwardRaw -- their usual "record the outcome based on
// the returned decoded bool" line never runs, and a genuine decode attempt
// (a miss) that happened to panic went uncounted in the hit/miss metrics.
// This confirms decodeWithPanicSafeMetrics -- the helper both wrappers now
// call through -- invokes recordOutcome(true) exactly once before
// re-raising the panic, and that the panic still propagates to the caller
// unchanged.
func TestDecodeWithPanicSafeMetricsRecordsMissOnPanic(t *testing.T) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x08}
	var outcomes []bool

	func() {
		defer func() {
			r := recover()
			require.NotNil(t, r, "the panic must still propagate to the caller")
			require.Equal(t, "simulated decode panic", r)
		}()
		_, _ = decodeWithPanicSafeMetrics(
			c,
			key,
			func() (int, error) { panic("simulated decode panic") },
			func(isMiss bool) { outcomes = append(outcomes, isMiss) },
		)
		t.Fatal("decodeWithPanicSafeMetrics must not return normally on panic")
	}()

	require.Equal(
		t,
		[]bool{true},
		outcomes,
		"a genuine decode attempt that panicked must still be recorded as a miss",
	)

	// The panic is cached like any other failure (see
	// TestDecodeCachePanicDuringDecodeDoesNotStrandWaitersOrKey): a later
	// call for the same key must not invoke decodeFn again, but must still
	// be recorded as a miss (not a hit) -- see
	// TestDecodeWithPanicSafeMetricsNeverRecordsAFailureAsAHit.
	_, err := decodeWithPanicSafeMetrics(
		c,
		key,
		func() (int, error) {
			t.Fatal("decodeFn must not run again for an already-cached panic")
			return 0, nil
		},
		func(isMiss bool) { outcomes = append(outcomes, isMiss) },
	)
	require.Error(t, err)
	require.Equal(t, []bool{true, true}, outcomes)
}

// TestDecodeWithPanicSafeMetricsNeverRecordsAFailureAsAHit is the
// regression test for a real bug found in code review: getOrDecode's
// decoded flag is false for both a genuine cache hit AND a waiter woken by
// -- or a lookup hitting -- an already-failed outcome (corrupted/tampered
// bytes, or a panic another goroutine already recorded). Recording purely
// on decoded would inflate the hit counter with one entry per caller that
// ever touches a failed delivery, when no successful decode was ever
// reused. This confirms decodeWithPanicSafeMetrics reports every failed
// outcome as a miss regardless of which internal path produced it (a fresh
// failing decode, or a repeat hit on that same now-cached failure), and
// still reports a genuine successful hit as a hit.
func TestDecodeWithPanicSafeMetricsNeverRecordsAFailureAsAHit(t *testing.T) {
	c := newDecodeCache[int]()
	failKey := decodeCacheKey{0x0C}
	okKey := decodeCacheKey{0x0D}
	wantErr := errors.New("bad bytes")
	var outcomes []bool
	record := func(isMiss bool) { outcomes = append(outcomes, isMiss) }

	// Fresh failure: a genuine miss, correctly true regardless of the fix.
	_, err := decodeWithPanicSafeMetrics(
		c, failKey, func() (int, error) { return 0, wantErr }, record,
	)
	require.ErrorIs(t, err, wantErr)

	// Repeat of the identical bad bytes: served from the cached failure
	// (decoded=false internally), but must still be a miss, not a hit --
	// this is the specific case the fix addresses.
	_, err = decodeWithPanicSafeMetrics(
		c, failKey, func() (int, error) {
			t.Fatal("decodeFn must not run again for a cached failure")
			return 0, nil
		}, record,
	)
	require.ErrorIs(t, err, wantErr)
	require.Equal(
		t, []bool{true, true}, outcomes,
		"both the fresh failure and the repeat hit on it must count as misses",
	)

	// A genuine success, then a genuine hit on it: true then false, exactly
	// matching getOrDecode's own decoded contract -- the fix must not touch
	// this case.
	outcomes = nil
	_, err = decodeWithPanicSafeMetrics(
		c, okKey, func() (int, error) { return 7, nil }, record,
	)
	require.NoError(t, err)
	_, err = decodeWithPanicSafeMetrics(
		c, okKey, func() (int, error) {
			t.Fatal("decodeFn must not run again for a cache hit")
			return 0, nil
		}, record,
	)
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, outcomes)
}

// TestDecodeCacheWaiterGetsResultEvenIfItsEntryIsEvictedBeforeItWakes is the
// regression test for a real bug found by re-auditing this file for a
// narrow/stress gap: a waiter used to be woken by a closed signal channel
// and then re-read its answer from c.entries[key]. But an entry can be
// evicted by unrelated churn (many other keys being inserted) in the window
// between "the leader signals waiters" and "a descheduled waiter resumes and
// re-acquires the lock" -- so a waiter could silently observe a
// zero-value/nil "success" instead of the real decode outcome, with no
// error and no panic to reveal anything went wrong.
//
// An earlier version of this test constructed the in-flight map by hand and
// sent the result to the waiter's channel itself, never calling getOrDecode
// or finishDecode at all -- it could not have caught a regression to the
// old closed-signal-channel design, since the delivery it asserted on was
// the test's own code, not production code. This version drives both
// participants through the real getOrDecode (the leader via a real
// in-flight decode, the waiter by genuinely registering as a waiter on it)
// so finishDecode's real channel-send is what delivers the result; the map
// is only touched afterward, to simulate the "evicted by unrelated churn"
// condition itself, which is external to the delivery mechanism being
// tested.
func TestDecodeCacheWaiterGetsResultEvenIfItsEntryIsEvictedBeforeItWakes(
	t *testing.T,
) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x77}
	release := make(chan struct{})
	leaderStarted := make(chan struct{})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = c.getOrDecode(key, func() (int, error) {
			close(leaderStarted)
			<-release
			return 999, nil
		})
	}()
	<-leaderStarted

	waiterDone := make(chan struct{})
	var gotValue int
	var gotErr error
	var gotDecoded bool
	go func() {
		defer close(waiterDone)
		gotValue, gotErr, gotDecoded = c.getOrDecode(
			key,
			func() (int, error) {
				t.Error("the waiter must not run decodeFn itself")
				return 0, nil
			},
		)
	}()
	// Confirm the waiter has genuinely registered itself in c.inFlight[key]
	// -- the real waiter-registration code path in getOrDecode -- before
	// releasing the leader.
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inFlight[key]) == 1
	}, 2*time.Second, 5*time.Millisecond)

	close(release)
	select {
	case <-leaderDone:
	case <-time.After(2 * time.Second):
		t.Fatal("leader never returned")
	}
	// The leader's getOrDecode call has now returned, which only happens
	// after finishDecode has already inserted the entry AND sent the result
	// to the waiter's (buffered) channel. Deleting the entry here simulates
	// unrelated churn evicting it before the waiter goroutine gets around
	// to reading its already-delivered value -- it must have no effect on
	// what the waiter receives, since finishDecode delivers the result
	// directly rather than the waiter re-reading this map.
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()

	select {
	case <-waiterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter never returned")
	}
	require.Equal(
		t,
		999,
		gotValue,
		"waiter must get the real decode result even though its entry was evicted before it read anything",
	)
	require.NoError(t, gotErr)
	require.False(t, gotDecoded, "the waiter itself never ran decodeFn")
}

// TestDecodeCacheStressConcurrentChurnWithSharedKeys is a stress test
// combining the two properties every other test above exercises in
// isolation: many goroutines racing on a handful of SHARED keys (so
// concurrent waiter registration/wake-up is exercised) while simultaneously
// churning through unique keys fast enough to force continuous eviction
// well past decodeCacheMaxEntries. This is exactly the condition under
// which TestDecodeCacheWaiterGetsResultEvenIfItsEntryIsEvictedBeforeItWakes
// reproduces deterministically; running it under -race additionally checks
// for data races in the eviction/wake-up bookkeeping under real concurrent
// load. Every call's returned value is checked against the only value its
// key could ever have decoded to, so a silently wrong result (not just a
// hang or panic) would fail the test.
func TestDecodeCacheStressConcurrentChurnWithSharedKeys(t *testing.T) {
	c := newDecodeCache[int]()
	const numSharedKeys = 8
	sharedKeys := make([]decodeCacheKey, numSharedKeys)
	for i := range sharedKeys {
		sharedKeys[i] = decodeCacheKey{byte(i + 1)}
	}

	const numGoroutines = 64
	const itersPerGoroutine = 300
	var wg sync.WaitGroup
	type failure struct {
		goroutine, iter  int
		wantValue, value int
		err              error
	}
	failures := make(chan failure, numGoroutines*itersPerGoroutine)

	for g := range numGoroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range itersPerGoroutine {
				if i%2 == 0 {
					// Shared key: exercises concurrent waiter registration
					// -- many goroutines competing for the same in-flight
					// decode -- while eviction churn runs alongside it.
					idx := i % numSharedKeys
					want := (idx + 1) * 1000
					value, err, _ := c.getOrDecode(
						sharedKeys[idx],
						func() (int, error) { return want, nil },
					)
					if err != nil || value != want {
						failures <- failure{g, i, want, value, err}
					}
				} else {
					// Unique key: never reused anywhere else in this test,
					// so it forces a genuine miss every time and drives
					// sustained eviction once the cache is past capacity.
					var key decodeCacheKey
					key[0] = 0xF0
					key[1] = byte(g)
					key[2] = byte(i)
					key[3] = byte(i >> 8)
					want := g*100000 + i
					value, err, _ := c.getOrDecode(
						key,
						func() (int, error) { return want, nil },
					)
					if err != nil || value != want {
						failures <- failure{g, i, want, value, err}
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(failures)

	var bad []failure
	for f := range failures {
		bad = append(bad, f)
	}
	require.Empty(
		t, bad,
		"silently wrong decode results under concurrent churn: %+v", bad,
	)
	require.LessOrEqual(t, decodeCacheLen(c), decodeCacheMaxEntries)
}

// --- Part 2: real-decode-function integration tests ------------------------
//
// These run the actual decodeBlockfetchBlock/decodeChainsyncHeader functions
// (via a real *Ouroboros) against real Conway fixture bytes from
// ouroboros-mock, through the shared caches, proving the wiring -- not just
// the generic mechanism -- behaves correctly on genuine chain data.

func testOuroborosForDecodeCache(tb testing.TB) *Ouroboros {
	tb.Helper()
	return &Ouroboros{
		config:            OuroborosConfig{},
		blockDecodeCache:  newDecodeCache[gledger.Block](),
		headerDecodeCache: newDecodeCache[gledger.BlockHeader](),
	}
}

func conwayBlockFixtureBytes(t *testing.T) (blockType uint, raw []byte) {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Block_Conway",
	)
	require.NoError(t, err)
	blockType, err = fixture.LedgerBlockType()
	require.NoError(t, err)
	raw, err = fixture.LedgerBlockBytes()
	require.NoError(t, err)
	return blockType, raw
}

func conwayHeaderFixtureBytes(t *testing.T) (headerType uint, raw []byte) {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Header_Conway",
	)
	require.NoError(t, err)
	headerType, err = fixture.LedgerHeaderType()
	require.NoError(t, err)
	raw, err = fixture.LedgerHeaderBytes()
	require.NoError(t, err)
	return headerType, raw
}

func TestBlockDecodeCacheIntegrationRealConwayBlock(t *testing.T) {
	o := testOuroborosForDecodeCache(t)
	blockType, raw := conwayBlockFixtureBytes(t)
	key := hashDecodeInput(blockType, raw)
	var decodeCalls atomic.Int64

	decodeOnce := func() (gledger.Block, error) {
		decodeCalls.Add(1)
		return o.decodeBlockfetchBlock(blockType, raw)
	}

	block1, err, decoded1 := o.blockDecodeCache.getOrDecode(key, decodeOnce)
	require.NoError(t, err)
	require.True(t, decoded1)
	require.NotNil(t, block1)

	block2, err, decoded2 := o.blockDecodeCache.getOrDecode(key, decodeOnce)
	require.NoError(t, err)
	require.False(t, decoded2, "identical real block bytes must hit the cache")
	require.NotNil(t, block2)
	require.EqualValues(t, 1, decodeCalls.Load())
	require.Equal(
		t, block1.Hash(), block2.Hash(),
		"cached result must decode to the same block",
	)
}

func TestHeaderDecodeCacheIntegrationRealConwayHeader(t *testing.T) {
	o := testOuroborosForDecodeCache(t)
	headerType, raw := conwayHeaderFixtureBytes(t)
	key := hashDecodeInput(headerType, raw)
	var decodeCalls atomic.Int64

	decodeOnce := func() (gledger.BlockHeader, error) {
		decodeCalls.Add(1)
		return o.decodeChainsyncHeader(headerType, raw)
	}

	header1, err, decoded1 := o.headerDecodeCache.getOrDecode(key, decodeOnce)
	require.NoError(t, err)
	require.True(t, decoded1)
	require.NotNil(t, header1)

	header2, err, decoded2 := o.headerDecodeCache.getOrDecode(key, decodeOnce)
	require.NoError(t, err)
	require.False(t, decoded2, "identical real header bytes must hit the cache")
	require.NotNil(t, header2)
	require.EqualValues(t, 1, decodeCalls.Load())
	require.Equal(
		t, header1.Hash(), header2.Hash(),
		"cached result must decode to the same header",
	)
}

// TestBlockDecodeCacheCorruptedDeliveryDoesNotContaminateGoodEntry covers the
// "4 peers, one sends a corrupted copy" scenario discussed for #489: a
// tampered copy of a real block hashes to a different key than the genuine
// bytes, so it is decoded (and fails) completely independently, and can
// never poison the cache entry the honest bytes produce.
func TestBlockDecodeCacheCorruptedDeliveryDoesNotContaminateGoodEntry(
	t *testing.T,
) {
	o := testOuroborosForDecodeCache(t)
	blockType, goodRaw := conwayBlockFixtureBytes(t)
	badRaw := make([]byte, len(goodRaw))
	copy(badRaw, goodRaw)
	badRaw[len(badRaw)/2] ^= 0xFF // flip a byte in the middle of the CBOR

	goodKey := hashDecodeInput(blockType, goodRaw)
	badKey := hashDecodeInput(blockType, badRaw)
	require.NotEqual(
		t, goodKey, badKey,
		"corrupted bytes must hash to a different cache key than the original",
	)

	goodBlock, goodErr, _ := o.blockDecodeCache.getOrDecode(
		goodKey,
		func() (gledger.Block, error) {
			return o.decodeBlockfetchBlock(blockType, goodRaw)
		},
	)
	require.NoError(t, goodErr)
	require.NotNil(t, goodBlock)

	// The corrupted delivery decodes (and most likely fails) on its own,
	// independent entry -- it must not be able to overwrite or be confused
	// with the good entry already cached above.
	_, badErr, badDecoded := o.blockDecodeCache.getOrDecode(
		badKey,
		func() (gledger.Block, error) {
			return o.decodeBlockfetchBlock(blockType, badRaw)
		},
	)
	require.True(
		t,
		badDecoded,
		"the corrupted bytes must be a genuine cache miss",
	)

	// Whatever the outcome for the corrupted bytes, the good entry must be
	// completely unaffected.
	goodBlockAgain, goodErrAgain, decodedAgain := o.blockDecodeCache.getOrDecode(
		goodKey,
		func() (gledger.Block, error) {
			return o.decodeBlockfetchBlock(blockType, goodRaw)
		},
	)
	require.NoError(t, goodErrAgain)
	require.False(t, decodedAgain, "the good entry must still be cached")
	require.NotNil(t, goodBlockAgain)
	require.Equal(t, goodBlock.Hash(), goodBlockAgain.Hash())
	t.Logf("corrupted-bytes decode outcome: %v", badErr)
}

// TestHeaderDecodeCacheCorruptedDeliveryDoesNotContaminateGoodEntry is
// TestBlockDecodeCacheCorruptedDeliveryDoesNotContaminateGoodEntry's header
// counterpart. Headers go through a meaningfully different decode path
// (decodeChainsyncHeader has its own extra Byron-EBB fallback branch that
// decodeBlockfetchBlock does not), so this is checked independently rather
// than assumed to follow from the block version.
func TestHeaderDecodeCacheCorruptedDeliveryDoesNotContaminateGoodEntry(
	t *testing.T,
) {
	o := testOuroborosForDecodeCache(t)
	headerType, goodRaw := conwayHeaderFixtureBytes(t)
	badRaw := make([]byte, len(goodRaw))
	copy(badRaw, goodRaw)
	badRaw[len(badRaw)/2] ^= 0xFF

	goodKey := hashDecodeInput(headerType, goodRaw)
	badKey := hashDecodeInput(headerType, badRaw)
	require.NotEqual(t, goodKey, badKey)

	goodHeader, goodErr, _ := o.headerDecodeCache.getOrDecode(
		goodKey,
		func() (gledger.BlockHeader, error) {
			return o.decodeChainsyncHeader(headerType, goodRaw)
		},
	)
	require.NoError(t, goodErr)
	require.NotNil(t, goodHeader)

	_, badErr, badDecoded := o.headerDecodeCache.getOrDecode(
		badKey,
		func() (gledger.BlockHeader, error) {
			return o.decodeChainsyncHeader(headerType, badRaw)
		},
	)
	require.True(
		t,
		badDecoded,
		"the corrupted bytes must be a genuine cache miss",
	)

	goodHeaderAgain, goodErrAgain, decodedAgain := o.headerDecodeCache.getOrDecode(
		goodKey,
		func() (gledger.BlockHeader, error) {
			return o.decodeChainsyncHeader(headerType, goodRaw)
		},
	)
	require.NoError(t, goodErrAgain)
	require.False(t, decodedAgain, "the good entry must still be cached")
	require.NotNil(t, goodHeaderAgain)
	require.Equal(t, goodHeader.Hash(), goodHeaderAgain.Hash())
	t.Logf("corrupted header decode outcome: %v", badErr)
}

// --- Part 3: gap-closing tests ---------------------------------------------
//
// These cover the specific gaps identified when reviewing the suite: real
// production wiring (not just the cache mechanism in isolation), the
// Musashi/Leios decode branch, the defensive nil-guard, cache-key
// correctness, metrics, real-data concurrency, sustained-churn size bounds,
// and an empty-input edge case.

// TestBlockfetchClientBlockRawRoutesThroughSharedCache is the permanent
// regression test for the actual production wiring, not just the cache
// mechanism by itself. It calls the real blockfetchClientBlockRaw entry
// point -- what a peer connection's callback really invokes -- twice with
// identical real block bytes, and checks the cache's own hit/miss metrics
// (not timing, which would be flaky in CI) to confirm the second call was
// served from the cache. This is the test that would catch a future edit
// accidentally breaking the glue between blockfetchClientBlockRaw and the
// cache (wrong key, skipped call, wrong metric), which none of the other
// tests in this file can see since they all call the pieces separately.
func TestBlockfetchClientBlockRawRoutesThroughSharedCache(t *testing.T) {
	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})
	blockType, raw := conwayBlockFixtureBytes(t)
	ctx := blockfetch.CallbackContext{}

	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))
	require.NoError(t, o.blockfetchClientBlockRaw(ctx, blockType, raw))

	require.InDelta(
		t, 1, testutil.ToFloat64(o.decodeCacheMetrics.blockCacheMisses), 0,
		"the first delivery must be a real decode",
	)
	require.InDelta(
		t, 1, testutil.ToFloat64(o.decodeCacheMetrics.blockCacheHits), 0,
		"the second, identical delivery must be served from the cache",
	)
}

// TestChainsyncClientRollForwardRawRoutesThroughSharedCache is
// TestBlockfetchClientBlockRawRoutesThroughSharedCache's header/ChainSync
// counterpart, covering the real chainsyncClientRollForwardRaw entry point.
func TestChainsyncClientRollForwardRawRoutesThroughSharedCache(t *testing.T) {
	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})
	headerType, raw := conwayHeaderFixtureBytes(t)
	ctx := ochainsync.CallbackContext{ConnectionId: decodeCacheTestConnId()}
	tip := ochainsync.Tip{}

	require.NoError(
		t,
		o.chainsyncClientRollForwardRaw(ctx, headerType, raw, tip),
	)
	require.NoError(
		t,
		o.chainsyncClientRollForwardRaw(ctx, headerType, raw, tip),
	)

	require.InDelta(
		t, 1, testutil.ToFloat64(o.decodeCacheMetrics.headerCacheMisses), 0,
		"the first delivery must be a real decode",
	)
	require.InDelta(
		t, 1, testutil.ToFloat64(o.decodeCacheMetrics.headerCacheHits), 0,
		"the second, identical delivery must be served from the cache",
	)
}

// TestBlockfetchClientBlockRawRecordsRepeatedFailureAsMissesNotHits is the
// regression test, through the real production wrapper and real Prometheus
// counters, for a bug found in code review: a delivery that never decodes
// successfully -- fresh, or replayed from the cached failure, exactly like
// a second peer relaying the same tampered/corrupted bytes -- was recorded
// as a hit whenever getOrDecode's own decoded flag was false, which is true
// for every repeat lookup regardless of whether the cached outcome was a
// success or a failure. Two calls with identical corrupted bytes must both
// count as misses; neither ever represents a successful decode being
// reused.
func TestBlockfetchClientBlockRawRecordsRepeatedFailureAsMissesNotHits(
	t *testing.T,
) {
	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})
	blockType, raw := conwayBlockFixtureBytes(t)
	badRaw := make([]byte, len(raw))
	copy(badRaw, raw)
	badRaw[len(badRaw)/2] ^= 0xFF
	ctx := blockfetch.CallbackContext{}

	require.Error(t, o.blockfetchClientBlockRaw(ctx, blockType, badRaw))
	require.Error(t, o.blockfetchClientBlockRaw(ctx, blockType, badRaw))

	require.InDelta(
		t, 2, testutil.ToFloat64(o.decodeCacheMetrics.blockCacheMisses), 0,
		"both calls represent a failed decode and must count as misses",
	)
	require.Zero(
		t,
		testutil.ToFloat64(o.decodeCacheMetrics.blockCacheHits),
		"a failed decode -- fresh or replayed from the cached failure -- must never count as a hit",
	)
}

// TestDecodeCacheConcurrentWaitersOnFailureAreAllRecordedAsMisses proves the
// same fix for the concurrent-waiter case specifically: N callers racing on
// one in-flight decode that ultimately fails all get decoded=false from
// getOrDecode (none of them ran decodeFn themselves), but none of them ever
// represents a successful decode being reused either. Applying
// decoded||err!=nil -- the formula decodeWithPanicSafeMetrics now uses --
// to every participant (the leader included) must classify all of them as
// misses.
func TestDecodeCacheConcurrentWaitersOnFailureAreAllRecordedAsMisses(
	t *testing.T,
) {
	c := newDecodeCache[int]()
	key := decodeCacheKey{0x0A}
	wantErr := errors.New("bad bytes")
	release := make(chan struct{})
	leaderStarted := make(chan struct{})
	var leaderOnce sync.Once

	const numWaiters = 4
	type outcome struct {
		decoded bool
		err     error
	}
	results := make([]outcome, numWaiters)
	var wg sync.WaitGroup
	wg.Add(numWaiters)
	for i := range numWaiters {
		go func(idx int) {
			defer wg.Done()
			_, err, decoded := c.getOrDecode(key, func() (int, error) {
				leaderOnce.Do(func() { close(leaderStarted) })
				<-release
				return 0, wantErr
			})
			results[idx] = outcome{decoded, err}
		}(i)
	}

	<-leaderStarted
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.inFlight[key]) == numWaiters-1
	}, 2*time.Second, 5*time.Millisecond)
	close(release)
	wg.Wait()

	for i, r := range results {
		require.ErrorIs(t, r.err, wantErr, "caller %d", i)
		isMiss := r.decoded || r.err != nil
		require.True(
			t,
			isMiss,
			"caller %d: a failed delivery must never be recorded as a hit, whether it ran decodeFn itself or shared a failing in-flight decode",
			i,
		)
	}
}

// TestBlockDecodeCacheWorksWithMusashiLeiosDecodeBranch covers the one
// decode path this whole feature exists around: on the Musashi network,
// decodeBlockfetchBlock routes Conway blocks through
// models.DecodeConwayBlock instead of the strict gouroboros decoder --
// that special case is the entire reason dingo takes the raw callback
// instead of the decoded one (see decodeBlockfetchBlock's doc comment).
// Every other test in this file exercises the default (non-Musashi) path;
// this confirms the cache works correctly when that branch is the one
// actually running.
func TestBlockDecodeCacheWorksWithMusashiLeiosDecodeBranch(t *testing.T) {
	o := testOuroborosForDecodeCache(t)
	o.config.NetworkMagic = ouroboros.NetworkCardanoMusashi.NetworkMagic
	blockType, raw := conwayBlockFixtureBytes(t)
	require.EqualValues(
		t, gledger.BlockTypeConway, blockType,
		"fixture must be Conway to exercise the Musashi-specific decode branch",
	)

	key := hashDecodeInput(blockType, raw)
	decodeFn := func() (gledger.Block, error) {
		return o.decodeBlockfetchBlock(blockType, raw)
	}

	block1, err, decoded1 := o.blockDecodeCache.getOrDecode(key, decodeFn)
	require.NoError(t, err)
	require.True(t, decoded1)
	require.NotNil(t, block1)

	block2, err, decoded2 := o.blockDecodeCache.getOrDecode(key, decodeFn)
	require.NoError(t, err)
	require.False(
		t,
		decoded2,
		"identical bytes must hit the cache on the Musashi branch too",
	)
	require.NotNil(t, block2)
	require.Equal(t, block1.Hash(), block2.Hash())
}

// TestBlockfetchClientBlockRawRejectsNilBlockWithNoError exercises the
// defensive guard added after nilaway flagged that the generic cache cannot
// itself guarantee "a nil value only ever accompanies a non-nil error" --
// that contract is only a convention on decodeFn, not something the cache
// type enforces. This forces exactly that violated state directly into the
// cache (bypassing decodeFn, which in real use never produces it) and
// confirms blockfetchClientBlockRaw returns an error instead of silently
// passing a nil block down to the rest of the pipeline.
func TestBlockfetchClientBlockRawRejectsNilBlockWithNoError(t *testing.T) {
	o := testOuroborosForDecodeCache(t)
	blockType, raw := conwayBlockFixtureBytes(t)
	key := hashDecodeInput(blockType, raw)

	decodeCacheInsertForTest[gledger.Block](
		o.blockDecodeCache,
		key,
		nil,
		time.Now(),
	)

	ctx := blockfetch.CallbackContext{}
	err := o.blockfetchClientBlockRaw(ctx, blockType, raw)
	require.Error(
		t, err,
		"a nil block with no error must be rejected, not passed downstream",
	)
}

// TestChainsyncClientRollForwardRawRejectsNilHeaderWithNoError is
// TestBlockfetchClientBlockRawRejectsNilBlockWithNoError's header
// counterpart.
func TestChainsyncClientRollForwardRawRejectsNilHeaderWithNoError(
	t *testing.T,
) {
	o := testOuroborosForDecodeCache(t)
	headerType, raw := conwayHeaderFixtureBytes(t)
	key := hashDecodeInput(headerType, raw)

	decodeCacheInsertForTest[gledger.BlockHeader](
		o.headerDecodeCache,
		key,
		nil,
		time.Now(),
	)

	ctx := ochainsync.CallbackContext{}
	err := o.chainsyncClientRollForwardRaw(
		ctx,
		headerType,
		raw,
		ochainsync.Tip{},
	)
	require.Error(
		t, err,
		"a nil header with no error must be rejected, not passed downstream",
	)
}

// TestHashDecodeInputMixesInBlockType covers the claim in hashDecodeInput's
// doc comment directly: identical bytes under two different block types
// must never collide to the same cache key.
func TestHashDecodeInputMixesInBlockType(t *testing.T) {
	_, raw := conwayBlockFixtureBytes(t)
	keyA := hashDecodeInput(1, raw)
	keyB := hashDecodeInput(2, raw)
	require.NotEqual(
		t,
		keyA,
		keyB,
		"identical bytes under different block types must hash to different cache keys",
	)
}

// TestDecodeCacheOutcomeMetricsCountHitsAndMissesCorrectly confirms the
// Prometheus hit/miss counters actually reflect what happened, not just that
// they exist. Two calls for one key (a miss then a hit) and one call for a
// different key (a miss) should leave the counters at exactly 2 misses, 1
// hit -- if the decoded flag were ever recorded backwards, this would catch
// it, which nothing else in the suite checks.
func TestDecodeCacheOutcomeMetricsCountHitsAndMissesCorrectly(t *testing.T) {
	o := newOuroboros(OuroborosConfig{PromRegistry: prometheus.NewRegistry()})
	require.NotNil(t, o.decodeCacheMetrics)

	keyA := decodeCacheKey{0x10}
	keyB := decodeCacheKey{0x20}
	decodeFn := func() (gledger.Block, error) { return nil, nil }

	_, _, decodedA1 := o.blockDecodeCache.getOrDecode(keyA, decodeFn)
	o.recordBlockDecodeCacheOutcome(decodedA1)
	_, _, decodedA2 := o.blockDecodeCache.getOrDecode(keyA, decodeFn)
	o.recordBlockDecodeCacheOutcome(decodedA2)
	_, _, decodedB1 := o.blockDecodeCache.getOrDecode(keyB, decodeFn)
	o.recordBlockDecodeCacheOutcome(decodedB1)

	require.InDelta(
		t, 2, testutil.ToFloat64(o.decodeCacheMetrics.blockCacheMisses), 0,
		"keyA's first call and keyB's first call are both misses",
	)
	require.InDelta(
		t, 1, testutil.ToFloat64(o.decodeCacheMetrics.blockCacheHits), 0,
		"keyA's second call is a hit",
	)
}

// TestBlockDecodeCacheConcurrentCallersShareOneRealDecode is
// TestDecodeCacheConcurrentCallersShareOneDecode's real-data counterpart:
// the earlier test proves the generic mechanism is race-safe using a
// synthetic int payload; this repeats the same concurrent-race shape against
// the actual gouroboros decode function and a real Conway block, so the
// race guarantee is also demonstrated on genuine chain data end to end.
func TestBlockDecodeCacheConcurrentCallersShareOneRealDecode(t *testing.T) {
	o := testOuroborosForDecodeCache(t)
	blockType, raw := conwayBlockFixtureBytes(t)
	key := hashDecodeInput(blockType, raw)
	var decodeCalls atomic.Int64

	decodeFn := func() (gledger.Block, error) {
		decodeCalls.Add(1)
		return o.decodeBlockfetchBlock(blockType, raw)
	}

	const numCallers = 20
	var wg sync.WaitGroup
	results := make([]gledger.Block, numCallers)
	errs := make([]error, numCallers)
	for i := range numCallers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx], _ = o.blockDecodeCache.getOrDecode(
				key,
				decodeFn,
			)
		}(i)
	}
	wg.Wait()

	require.EqualValues(
		t, 1, decodeCalls.Load(),
		"many concurrent callers for the same real block must share one decode",
	)
	for i := range numCallers {
		require.NoError(t, errs[i])
		require.NotNil(t, results[i])
		require.Equal(t, results[0].Hash(), results[i].Hash())
	}
}

// TestDecodeCacheNeverExceedsCapDuringSustainedChurn is a soak-style check
// beyond TestDecodeCachePrunesBySizeWhenOverCapacity's single before/after
// snapshot: it inserts several times the cache's capacity worth of distinct
// keys and asserts the cap holds at every single step along the way, not
// just once at the end, guarding against an eviction bug that only shows up
// under sustained churn.
func TestDecodeCacheNeverExceedsCapDuringSustainedChurn(t *testing.T) {
	c := newDecodeCache[int]()
	decodeFn := func() (int, error) { return 1, nil }

	for i := range decodeCacheMaxEntries * 3 {
		var key decodeCacheKey
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = byte(i >> 16)
		_, _, _ = c.getOrDecode(key, decodeFn)
		require.LessOrEqual(
			t,
			decodeCacheLen(c),
			decodeCacheMaxEntries,
			"cache must never exceed its cap, at any point during churn, not just at the end",
		)
	}
}

// TestBlockDecodeCacheHandlesEmptyInputWithoutPanicking covers the edge case
// of a zero-length delivery: it must fail to decode like any other garbage
// input, not panic, and the failure must still be cached like any other
// result.
func TestBlockDecodeCacheHandlesEmptyInputWithoutPanicking(t *testing.T) {
	o := testOuroborosForDecodeCache(t)
	key := hashDecodeInput(0, nil)
	decodeFn := func() (gledger.Block, error) {
		return o.decodeBlockfetchBlock(0, nil)
	}

	_, err, decoded := o.blockDecodeCache.getOrDecode(key, decodeFn)
	require.True(t, decoded)
	require.Error(t, err, "empty input should fail to decode, not panic")

	_, err2, decoded2 := o.blockDecodeCache.getOrDecode(key, decodeFn)
	require.False(
		t,
		decoded2,
		"the cached failure must be reused, not panic again",
	)
	require.Error(t, err2)
}
