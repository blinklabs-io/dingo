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
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// decodeCacheMaxEntries and decodeCacheTTL bound the shared block/header
// decode caches the same way leiosEndorserBlockCacheMaxEntries/TTL bound the
// Leios EB cache: a two-stage prune (age first, then size) keeps memory
// bounded on a long-running node. Decoding is a pure function of (blockType,
// bytes), so unlike the Leios cache TTL is not a freshness concern here --
// it exists purely to bound memory, not to invalidate stale-but-still-valid
// answers.
const (
	decodeCacheMaxEntries = 1024
	decodeCacheTTL        = 2 * time.Minute
)

// decodeCacheKey identifies raw decode input by content hash (block type plus
// raw bytes), not by chain point. This is the key correctness property: two
// connections delivering byte-identical data always share one decode and one
// cache entry, while two connections delivering different bytes for what is
// nominally "the same block" (corruption, tampering, a buggy peer) hash to
// different keys and are decoded, cached, and reported on completely
// independently. A bad delivery from one peer can therefore never poison the
// answer another peer's good delivery produces. See dingo #489.
type decodeCacheKey [sha256.Size]byte

// hashDecodeInput computes the cache key for a decode input. blockType is
// mixed in ahead of the raw bytes so a (rare, currently theoretical) same-byte
// encoding under two different block types cannot collide.
func hashDecodeInput(blockType uint, raw []byte) decodeCacheKey {
	h := sha256.New()
	var typeBuf [8]byte
	binary.LittleEndian.PutUint64(typeBuf[:], uint64(blockType))
	h.Write(typeBuf[:])
	h.Write(raw)
	var key decodeCacheKey
	copy(key[:], h.Sum(nil))
	return key
}

// decodeCacheEntry holds the outcome of decoding one input. Exactly one of
// value/err is meaningful, matching the wrapped decode function's own
// (value, error) contract. Failed decodes are cached the same as successful
// ones: decoding is deterministic given fixed input bytes, so a cached
// failure is simply a correct remembered fact ("these bytes do not decode"),
// not a permanent poison -- it is bounded and evicted by the same TTL/size
// rules as every other entry, same as a success.
type decodeCacheEntry[T any] struct {
	value      T
	err        error
	insertedAt time.Time
}

// decodeCacheResult carries a decode outcome directly to a waiting caller
// through its wait channel, instead of the waiter re-reading c.entries after
// waking. An entry can be evicted by unrelated churn (many other keys being
// inserted) in the window between "the leader signals its waiters" and "a
// descheduled waiter resumes and re-acquires the lock" -- delivering the
// result inline means a waiter always receives the actual outcome it waited
// for, never a zero-value/"miss" masquerading as a successful empty decode
// because its entry happened to age out of the map in that window.
type decodeCacheResult[T any] struct {
	value T
	err   error
}

// decodeCache is a shared, bounded cache of decode outcomes keyed by content
// hash, with a waiter mechanism so concurrent callers submitting identical
// bytes at nearly the same instant share one decode instead of each doing the
// work independently. One instance covers one decode "kind" (blocks or
// headers); Ouroboros holds one of each. See dingo #489.
//
// Eviction is insertion-order FIFO via order, not a sort: entries are never
// mutated or re-inserted once written (a given key's decode outcome is
// immutable), so the oldest-inserted entry is always both the correct
// TTL-expiry candidate and the correct size-cap eviction candidate -- there
// is no "recently used" concept to track separately. An earlier version
// re-sorted the entire map by insertedAt on every insert once the cache was
// at capacity; benchmarking a sustained no-duplicates workload (every insert
// a genuine miss, the realistic case when peers are not delivering
// duplicates) showed that made decoding through the cache 7-80x slower than
// not caching at all. Keeping an explicit insertion-order list turns
// eviction into an O(1)-amortized pop from the front instead.
type decodeCache[T any] struct {
	mu       sync.Mutex
	entries  map[decodeCacheKey]decodeCacheEntry[T]
	order    *list.List // decodeCacheKey values, oldest-inserted at Front()
	inFlight map[decodeCacheKey][]chan decodeCacheResult[T]
}

func newDecodeCache[T any]() *decodeCache[T] {
	return &decodeCache[T]{
		entries:  make(map[decodeCacheKey]decodeCacheEntry[T]),
		order:    list.New(),
		inFlight: make(map[decodeCacheKey][]chan decodeCacheResult[T]),
	}
}

// getOrDecode returns the decoded value for key, running decodeFn at most
// once per key no matter how many goroutines call concurrently for the same
// key. The returned decoded flag is true only for the caller whose goroutine
// actually executed decodeFn (a genuine cache miss); every other caller --
// whether served from an already-populated entry or woken after waiting on
// this call's in-flight attempt -- gets decoded=false. Callers use that flag
// only for hit/miss metrics; the returned (value, err) is identical for every
// caller regardless of how it was obtained.
//
// decodeFn is never called while mu is held: the lock only ever guards map
// bookkeeping, never the decode itself, so concurrent decodes for different
// keys never serialize against each other.
//
// A decodeFn that panics does not strand this key: the panic is recovered
// long enough to record it as a normal cached failure and release every
// waiter, then re-raised so the calling goroutine's own crash-or-recover
// behavior is unchanged from before this cache existed. See finishDecode.
func (c *decodeCache[T]) getOrDecode(
	key decodeCacheKey,
	decodeFn func() (T, error),
) (value T, err error, decoded bool) {
	c.mu.Lock()
	if entry, ok := c.entries[key]; ok {
		c.mu.Unlock()
		return entry.value, entry.err, false
	}
	if waiters, claimed := c.inFlight[key]; claimed {
		// Buffered so finishDecode's send never blocks on a waiter that
		// hasn't reached the receive yet.
		ch := make(chan decodeCacheResult[T], 1)
		c.inFlight[key] = append(waiters, ch)
		c.mu.Unlock()
		result := <-ch
		return result.value, result.err, false
	}
	// Claim the in-flight slot (present-but-empty means "claimed, no
	// waiters yet") and release the lock before doing the actual work.
	c.inFlight[key] = []chan decodeCacheResult[T]{}
	c.mu.Unlock()

	// A panicking decodeFn (CBOR decode on adversarial bytes can, in
	// principle, panic instead of erroring -- gouroboros' own cbor.Value
	// decoder recovers only one specific panic class and re-panics for
	// every other one) must not leave this key's claim held and its
	// waiters parked forever. Recover, record the panic as a normal
	// (cacheable) decode failure via finishDecode -- exactly like any
	// other failure, so every current waiter is woken and any future
	// submission of the identical bytes fails fast instead of panicking
	// again -- then re-panic so this goroutine's own crash-or-recover
	// behavior is unchanged from before this cache existed.
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		var panicErr error
		if asErr, ok := r.(error); ok {
			panicErr = fmt.Errorf("decode panicked: %w", asErr)
		} else {
			panicErr = fmt.Errorf("decode panicked: %v", r)
		}
		c.finishDecode(key, value, panicErr)
		panic(r)
	}()

	value, err = decodeFn()
	c.finishDecode(key, value, err)
	return value, err, true
}

// decodeWithPanicSafeMetrics wraps getOrDecode so a panicking decodeFn still
// gets its hit/miss outcome recorded. getOrDecode's own panic recovery
// re-raises after cleaning up the cache (see its doc comment), so this call
// never returns normally on that path -- the caller's usual
// "record based on the returned decoded bool" pattern never runs, and a
// genuine decode attempt (a miss) that happened to panic would silently go
// uncounted. recordMiss is invoked from a recover here, before the panic is
// re-raised again, so it always sees exactly the same attempts a
// non-panicking decodeFn would have reported via decoded=true.
func decodeWithPanicSafeMetrics[T any](
	cache *decodeCache[T],
	key decodeCacheKey,
	decodeFn func() (T, error),
	recordMiss func(),
) (value T, err error, decoded bool) {
	defer func() {
		if r := recover(); r != nil {
			recordMiss()
			panic(r)
		}
	}()
	return cache.getOrDecode(key, decodeFn)
}

// finishDecode records key's decode outcome, releases its in-flight claim,
// and wakes every waiter with it. Shared by getOrDecode's normal-return path
// and its panic-recovery path so both leave the cache in the same
// consistent state -- a waiter never observes the difference between "the
// leader's decodeFn returned an error" and "the leader's decodeFn panicked".
func (c *decodeCache[T]) finishDecode(key decodeCacheKey, value T, err error) {
	c.mu.Lock()
	// now is captured under the lock, not before it: two concurrent
	// finishDecode calls (for two different keys) acquire the lock in some
	// order that need not match the order in which they would have called
	// time.Now() beforehand. order (the eviction FIFO) and insertedAt must
	// agree on which entry is older, or evictLocked's TTL loop can stop at
	// a front entry that looks fresh while a truly-older, later-positioned
	// entry never gets checked.
	now := time.Now()
	c.order.PushBack(key)
	c.entries[key] = decodeCacheEntry[T]{
		value:      value,
		err:        err,
		insertedAt: now,
	}
	waiters := c.inFlight[key]
	delete(c.inFlight, key)
	c.evictLocked(now)
	c.mu.Unlock()

	// Send the outcome directly rather than closing a signal channel and
	// letting each waiter re-read c.entries[key]: eviction (just run above,
	// and always possible again before a descheduled waiter resumes) can
	// remove this key from entries at any time after this point, and a
	// waiter reading a since-evicted key would silently observe a
	// zero-value/nil "success" instead of the real outcome.
	result := decodeCacheResult[T]{value: value, err: err}
	for _, ch := range waiters {
		ch <- result
	}
}

// evictLocked evicts entries older than decodeCacheTTL, then -- if still over
// decodeCacheMaxEntries -- pops additional oldest-inserted entries down to
// the cap. The caller must hold mu.
//
// Because order is a FIFO of insertion order and entries are immutable once
// written, the front of order is always both the oldest entry (the correct
// TTL-expiry candidate) and the correct next victim for size-cap eviction, so
// both stages are a simple pop-from-front loop -- O(1) amortized per
// eviction, no re-sorting the cache on every insert.
func (c *decodeCache[T]) evictLocked(now time.Time) {
	cutoff := now.Add(-decodeCacheTTL)
	for {
		front := c.order.Front()
		if front == nil {
			break
		}
		key := front.Value.(decodeCacheKey) //nolint:forcetypeassert
		entry, ok := c.entries[key]
		if !ok || !entry.insertedAt.Before(cutoff) {
			break
		}
		c.order.Remove(front)
		delete(c.entries, key)
	}
	for len(c.entries) > decodeCacheMaxEntries {
		front := c.order.Front()
		if front == nil {
			break
		}
		key := front.Value.(decodeCacheKey) //nolint:forcetypeassert
		c.order.Remove(front)
		delete(c.entries, key)
	}
}

// decodeCacheMetrics tracks hit/miss counts for the block and header decode
// caches, following the same promauto pattern as blockfetchMetrics.
type decodeCacheMetrics struct {
	blockCacheHits    prometheus.Counter
	blockCacheMisses  prometheus.Counter
	headerCacheHits   prometheus.Counter
	headerCacheMisses prometheus.Counter
}

func (o *Ouroboros) initDecodeCacheMetrics() {
	factory := promauto.With(o.config.PromRegistry)
	o.decodeCacheMetrics = &decodeCacheMetrics{
		blockCacheHits: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_decode_cache_block_hits_total",
			Help: "blocks served from the shared decode cache instead of being re-decoded",
		}),
		blockCacheMisses: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_decode_cache_block_misses_total",
			Help: "blocks actually decoded (cache miss)",
		}),
		headerCacheHits: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_decode_cache_header_hits_total",
			Help: "headers served from the shared decode cache instead of being re-decoded",
		}),
		headerCacheMisses: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_decode_cache_header_misses_total",
			Help: "headers actually decoded (cache miss)",
		}),
	}
}

// recordBlockDecodeCacheOutcome updates the block cache hit/miss counters if
// metrics are enabled. decoded matches decodeCache.getOrDecode's return value:
// true means this call actually decoded (miss), false means it reused a
// cache entry or an in-flight wait (hit).
func (o *Ouroboros) recordBlockDecodeCacheOutcome(decoded bool) {
	if o.decodeCacheMetrics == nil {
		return
	}
	if decoded {
		o.decodeCacheMetrics.blockCacheMisses.Inc()
	} else {
		o.decodeCacheMetrics.blockCacheHits.Inc()
	}
}

// recordHeaderDecodeCacheOutcome is recordBlockDecodeCacheOutcome's header
// counterpart.
func (o *Ouroboros) recordHeaderDecodeCacheOutcome(decoded bool) {
	if o.decodeCacheMetrics == nil {
		return
	}
	if decoded {
		o.decodeCacheMetrics.headerCacheMisses.Inc()
	} else {
		o.decodeCacheMetrics.headerCacheHits.Inc()
	}
}
