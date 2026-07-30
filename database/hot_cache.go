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
	"errors"
	"log/slog"
	"maps"
	"runtime"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// hotCacheData holds both entries and access counts together for atomic updates.
// This ensures entries and accessCnt are always consistent.
type hotCacheData struct {
	entries   map[string][]byte
	accessCnt map[string]uint64
}

// accessSampleRate controls how often access counts are updated on Get().
// A value of 4 means ~25% of Gets trigger an access count update.
// This trades LFU accuracy for performance by avoiding map copies on every read.
const accessSampleRate = 4

const (
	// defaultMaxCASAttempts bounds the work spent on one best-effort
	// copy-on-write update (Put, which folds in eviction, or access-count
	// tracking).
	defaultMaxCASAttempts = 16
	// casYieldThreshold is the retry count after which a CAS loop starts
	// sleeping a small, randomized amount instead of just yielding the
	// processor, so contending writers desynchronize instead of retrying
	// in lockstep.
	casYieldThreshold = 2
	// maxCASBackoff caps the randomized sleep between CAS retries.
	maxCASBackoff = 64 * time.Microsecond
	// abortLogInterval rate-limits the writer-aborted warning log so that
	// sustained contention produces bounded log/IO traffic instead of one
	// warning per dropped update. The writersAborted counter is unaffected
	// and remains authoritative regardless of how many log lines this
	// suppresses.
	abortLogInterval = time.Second
)

// HotCacheCASStats describes contention in HotCache's copy-on-write update
// path. Values are cumulative for the lifetime of the cache.
type HotCacheCASStats struct {
	// Attempts is the total number of CompareAndSwap attempts across Put
	// (which folds in eviction, see evictToFit) and access-count tracking.
	Attempts uint64
	// WritersAbortedAfterBudget is the number of copy-on-write updates that
	// exhausted their retry budget and were dropped as best-effort.
	WritersAbortedAfterBudget uint64
	// SuccessfulCommitsAfterBackoff is the number of updates that succeeded
	// only after backing off at least once, i.e. actual forward progress
	// under contention rather than just bounded termination.
	SuccessfulCommitsAfterBackoff uint64
	// SuccessfulCommitBackoffTime is the cumulative backoff duration spent
	// by updates counted in SuccessfulCommitsAfterBackoff.
	SuccessfulCommitBackoffTime time.Duration
}

// HotCache provides a lock-free cache for frequently accessed CBOR data.
// It uses copy-on-write semantics for thread-safe concurrent access without locks.
// Eviction follows a Least-Frequently-Used (LFU) policy with probabilistic counting.
type HotCache struct {
	data                atomic.Pointer[hotCacheData] // combined entries + access counts
	maxSize             int                          // max number of entries (0 = unlimited)
	maxBytes            int64                        // max memory in bytes (0 = unlimited)
	curBytes            atomic.Int64                 // current memory usage
	sampleCnt           atomic.Uint64                // counter for probabilistic access tracking
	casSequence         atomic.Uint64                // gives contending writers asymmetric jitter
	casAttempts         atomic.Uint64
	writersAborted      atomic.Uint64 // updates dropped after exhausting the budget
	commitsAfterBackoff atomic.Uint64
	commitBackoffNanos  atomic.Uint64
	maxCASAttempts      int

	// logger and cacheName are optional; set via SetLogger. A nil logger
	// (the default) disables the writer-aborted warning log entirely.
	logger    *slog.Logger
	cacheName string
	// lastAbortLogNanos rate-limits logWriterAborted to abortLogInterval.
	lastAbortLogNanos atomic.Int64
}

// SetLogger wires an optional logger into the cache for diagnostics: it logs
// a warning whenever a copy-on-write update is dropped after exhausting the
// CAS retry budget (see HotCacheCASStats.WritersAbortedAfterBudget). name
// identifies this cache instance in the log fields (e.g. "utxo", "tx"). A
// nil logger disables this logging, which is the default.
func (c *HotCache) SetLogger(logger *slog.Logger, name string) {
	c.logger = logger
	c.cacheName = name
}

// logWriterAborted logs the writer-aborted-after-budget event, if a logger
// has been configured, rate-limited to at most one line per
// abortLogInterval regardless of how many aborts occur in that window —
// under sustained contention this can otherwise emit thousands of warnings
// per second, trading CPU churn for log/IO churn. writersAborted itself is
// unaffected and always incremented by the caller, so it remains an
// authoritative count even while logging is suppressed. op identifies which
// CAS loop gave up (put or increment_access).
func (c *HotCache) logWriterAborted(op string) {
	if c.logger == nil {
		return
	}
	now := time.Now().UnixNano()
	last := c.lastAbortLogNanos.Load()
	if now-last < int64(abortLogInterval) {
		return
	}
	if !c.lastAbortLogNanos.CompareAndSwap(last, now) {
		// Another goroutine just logged for this window; skip to avoid a
		// burst of near-simultaneous duplicate lines.
		return
	}
	c.logger.Warn(
		"hot cache dropped a best-effort update after exhausting its CAS retry budget",
		"cache", c.cacheName,
		"op", op,
		"max_attempts", c.maxCASAttempts,
		"writers_aborted_total", c.writersAborted.Load(),
	)
}

// RegisterCASMetrics exposes this cache's copy-on-write contention counters
// (see HotCacheCASStats) on the given Prometheus registry, labeled by
// cacheName (e.g. "utxo", "tx"). If registry is nil, this is a no-op. This
// method is safe to call more than once with the same registry; a duplicate
// registration is ignored since the collectors read from this cache's own
// counters regardless of which registration call installed them.
func (c *HotCache) RegisterCASMetrics(
	registry prometheus.Registerer,
	cacheName string,
) error {
	if registry == nil {
		return nil
	}
	labels := prometheus.Labels{"cache": cacheName}
	collectors := []prometheus.Collector{
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "dingo_hot_cache_cas_attempts_total",
			Help:        "Total CompareAndSwap attempts across Put (including eviction) and access tracking",
			ConstLabels: labels,
		}, func() float64 { return float64(c.casAttempts.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "dingo_hot_cache_writers_aborted_after_budget_total",
			Help:        "Total copy-on-write updates dropped after exhausting the CAS retry budget",
			ConstLabels: labels,
		}, func() float64 { return float64(c.writersAborted.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "dingo_hot_cache_successful_commits_after_backoff_total",
			Help:        "Total copy-on-write updates that succeeded only after backing off at least once",
			ConstLabels: labels,
		}, func() float64 { return float64(c.commitsAfterBackoff.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "dingo_hot_cache_successful_commit_backoff_seconds_total",
			Help:        "Cumulative backoff time spent by updates that succeeded after backing off",
			ConstLabels: labels,
		}, func() float64 { return c.CASStats().SuccessfulCommitBackoffTime.Seconds() }),
	}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
				return err
			}
		}
	}
	return nil
}

// CASStats returns a snapshot of copy-on-write contention counters, suitable
// for diagnostics or metrics export.
func (c *HotCache) CASStats() HotCacheCASStats {
	return HotCacheCASStats{
		Attempts:                      c.casAttempts.Load(),
		WritersAbortedAfterBudget:     c.writersAborted.Load(),
		SuccessfulCommitsAfterBackoff: c.commitsAfterBackoff.Load(),
		// #nosec G115 -- backoff nanoseconds are bounded by maxCASBackoff per
		// attempt and maxCASAttempts attempts; won't approach int64 overflow.
		SuccessfulCommitBackoffTime: time.Duration(c.commitBackoffNanos.Load()),
	}
}

// recordSuccessfulCommit records a copy-on-write update that committed after
// a timed backoff. Keeping this accounting in one place ensures every CAS path
// uses the same metric semantics.
func (c *HotCache) recordSuccessfulCommit(backoffTime time.Duration) {
	if backoffTime <= 0 {
		return
	}
	c.commitsAfterBackoff.Add(1)
	c.commitBackoffNanos.Add(uint64(backoffTime))
}

// backoff waits between CAS retries: an immediate scheduler yield for the
// first couple of attempts, then a small randomized sleep whose window
// grows with the attempt count. casSequence lets concurrent callers land on
// different delays without a shared RNG or lock. Returns the duration
// slept, or 0 if only a yield occurred.
func (c *HotCache) backoff(attempt int) time.Duration {
	if attempt < casYieldThreshold {
		runtime.Gosched()
		return 0
	}
	shift := min(attempt-casYieldThreshold, 6)
	window := uint64(1) << shift
	sequence := c.casSequence.Add(0x9e3779b97f4a7c15)
	// #nosec G115 -- value is 1..window (window <= 64), far below int64 range.
	jitter := time.Duration(1+(sequence^(sequence>>30))%window) * time.Microsecond
	jitter = min(jitter, maxCASBackoff)
	time.Sleep(jitter)
	return jitter
}

// NewHotCache creates a new HotCache with the given size and memory limits.
// Set maxSize to 0 for unlimited entries (limited only by maxBytes).
// Set maxBytes to 0 for unlimited memory (limited only by maxSize).
func NewHotCache(maxSize int, maxBytes int64) *HotCache {
	cache := &HotCache{
		maxSize:        maxSize,
		maxBytes:       maxBytes,
		maxCASAttempts: defaultMaxCASAttempts,
	}

	// Initialize combined data structure
	data := &hotCacheData{
		entries:   make(map[string][]byte),
		accessCnt: make(map[string]uint64),
	}
	cache.data.Store(data)

	return cache
}

// Get retrieves a value from the cache by key.
// Returns the value and true if found, nil and false otherwise.
// This operation is lock-free and safe for concurrent use.
// Access counts are updated probabilistically (1 in accessSampleRate calls)
// to reduce overhead while maintaining approximate LFU behavior.
func (c *HotCache) Get(key []byte) ([]byte, bool) {
	data := c.data.Load()
	if data == nil {
		return nil, false
	}

	value, ok := data.entries[string(key)]
	if ok {
		// Probabilistic counting: only update access count 1 in accessSampleRate times
		// This avoids expensive map copies on every read while maintaining approximate LFU
		if c.sampleCnt.Add(1)%accessSampleRate == 0 {
			c.incrementAccess(key)
		}
		// Return a copy to prevent callers from mutating cached data
		return append([]byte(nil), value...), true
	}
	return nil, false
}

// Put adds or updates a value in the cache.
// If maxBytes > 0 and the entry size exceeds maxBytes/10, the entry is skipped.
// This operation uses copy-on-write semantics for thread safety. If the
// insert pushes the cache over its configured maxSize/maxBytes, eviction is
// folded into this same CAS attempt (see evictToFit) rather than run as a
// separate follow-up operation: a standalone eviction pass would have to win
// its own CAS race against concurrent access-count updates from Get, and
// could lose that race indefinitely under sustained read contention with no
// later Put to retry it, leaving the cache permanently over its limit.
func (c *HotCache) Put(key []byte, cbor []byte) {
	keyStr := string(key)
	entrySize := int64(len(key) + len(cbor))

	// Skip entries that are too large (> 10% of max memory)
	if c.maxBytes > 0 && entrySize > c.maxBytes/10 {
		return
	}

	// Copy-on-write: create new combined data with the update
	var backoffTime time.Duration
	for attempt := 0; attempt < c.maxCASAttempts; attempt++ {
		oldData := c.data.Load()

		newEntries := make(map[string][]byte, len(oldData.entries)+1)
		maps.Copy(newEntries, oldData.entries)

		// Track memory change
		var memDelta int64
		if oldValue, exists := newEntries[keyStr]; exists {
			memDelta = entrySize - int64(len(keyStr)+len(oldValue))
		} else {
			memDelta = entrySize
		}

		// Copy the value to prevent callers from mutating cached data
		cborCopy := make([]byte, len(cbor))
		copy(cborCopy, cbor)
		newEntries[keyStr] = cborCopy

		// Also update access count map
		newAccessCnt := make(map[string]uint64, len(oldData.accessCnt)+1)
		maps.Copy(newAccessCnt, oldData.accessCnt)
		newAccessCnt[keyStr]++ // increment on put

		// Trim to the configured limits as part of this same snapshot, using
		// an estimate of post-insert bytes derived from the running counter;
		// this mirrors how memDelta itself is computed relative to oldData
		// and needs no per-Put O(n) resummation.
		estimatedBytes := c.curBytes.Load() + memDelta
		trimmedEntries, trimmedAccessCnt, bytesEvicted := c.evictToFit(
			newEntries, newAccessCnt, estimatedBytes,
		)

		newData := &hotCacheData{
			entries:   trimmedEntries,
			accessCnt: trimmedAccessCnt,
		}

		// Try to atomically update both maps together
		c.casAttempts.Add(1)
		if c.data.CompareAndSwap(oldData, newData) {
			if c.maxBytes > 0 {
				c.curBytes.Add(memDelta - bytesEvicted)
			}
			c.recordSuccessfulCommit(backoffTime)
			return
		}
		// CAS failed. The bounded loop plus backoff below prevents this from
		// spinning forever or staying synchronized with other contenders.
		backoffTime += c.backoff(attempt)
	}

	// Cache population is strictly best-effort. Dropping the write keeps
	// contention out of the caller's critical path; a later miss recomputes it.
	c.writersAborted.Add(1)
	c.logWriterAborted("put")
}

// incrementAccess increments the access counter for a key.
// Uses copy-on-write semantics for thread safety.
// Only increments if the key still exists in entries to prevent orphan access counts.
func (c *HotCache) incrementAccess(key []byte) {
	keyStr := string(key)

	var backoffTime time.Duration
	for attempt := 0; attempt < c.maxCASAttempts; attempt++ {
		oldData := c.data.Load()
		if oldData == nil {
			return
		}

		// Check if key still exists in entries (may have been evicted)
		if _, exists := oldData.entries[keyStr]; !exists {
			return
		}

		// Copy and update access counts only - entries are unchanged so reuse them
		newAccessCnt := make(map[string]uint64, len(oldData.accessCnt))
		maps.Copy(newAccessCnt, oldData.accessCnt)
		newAccessCnt[keyStr]++

		newData := &hotCacheData{
			entries:   oldData.entries, // reuse immutable entries map
			accessCnt: newAccessCnt,
		}

		c.casAttempts.Add(1)
		if c.data.CompareAndSwap(oldData, newData) {
			c.recordSuccessfulCommit(backoffTime)
			return
		}
		// CAS failed. This is a probabilistic, best-effort access-count
		// update (see accessSampleRate), so dropping it under sustained
		// contention only costs a little LFU accuracy.
		backoffTime += c.backoff(attempt)
	}
	c.writersAborted.Add(1)
	c.logWriterAborted("increment_access")
}

// evictToFit trims entries (least-frequently-used first) so the result
// satisfies maxSize/maxBytes, given a pending insert already folded into
// entries/accessCnt and an estimate of the resulting byte usage. It returns
// the input maps unchanged (same references) if no trimming is needed, or
// new maps with the LFU entries removed, plus the bytes removed.
//
// This is called from within Put's own CAS-attempt loop rather than as a
// separate operation: eviction has no correctness value if it can be
// starved indefinitely by concurrent access-count updates from Get, since
// only Put ever grows the cache and there may be no later Put to retry a
// dropped eviction. Folding it into the same atomic snapshot as the insert
// makes limit enforcement a property of that single CAS instead of a
// separate, losable race.
func (c *HotCache) evictToFit(
	entries map[string][]byte,
	accessCnt map[string]uint64,
	estimatedBytes int64,
) (map[string][]byte, map[string]uint64, int64) {
	needEvictBySize := c.maxSize > 0 && len(entries) > c.maxSize
	needEvictByBytes := c.maxBytes > 0 && estimatedBytes > c.maxBytes
	if !needEvictBySize && !needEvictByBytes {
		return entries, accessCnt, 0
	}

	// Calculate target size (evict ~25%, but keep at least 1 entry)
	// Only computed when maxSize > 0; otherwise size-based eviction is disabled
	var targetSize int
	if c.maxSize > 0 {
		targetSize = max(1, c.maxSize*3/4)
	}

	var targetBytes int64
	if c.maxBytes > 0 {
		targetBytes = c.maxBytes * 3 / 4
	}

	// Build list of entries sorted by access count (ascending = least frequent first)
	type entry struct {
		key   string
		count uint64
		size  int64
	}

	entriesList := make([]entry, 0, len(entries))
	for k, v := range entries {
		entriesList = append(entriesList, entry{
			key:   k,
			count: accessCnt[k],
			size:  int64(len(k) + len(v)),
		})
	}

	// Sort by access count ascending (least frequently used first)
	sort.Slice(entriesList, func(i, j int) bool {
		return entriesList[i].count < entriesList[j].count
	})

	// Determine which entries to keep
	keysToRemove := make(map[string]bool)
	keptSize := 0
	var keptBytes int64

	for _, e := range slices.Backward(entriesList) {
		wouldExceedSize := c.maxSize > 0 && keptSize >= targetSize
		wouldExceedBytes := c.maxBytes > 0 && keptBytes+e.size > targetBytes

		if wouldExceedSize || wouldExceedBytes {
			keysToRemove[e.key] = true
		} else {
			keptSize++
			keptBytes += e.size
		}
	}

	if len(keysToRemove) == 0 {
		return entries, accessCnt, 0
	}

	// Create new maps without evicted entries
	newEntries := make(map[string][]byte, len(entries)-len(keysToRemove))
	newAccessCnt := make(map[string]uint64, len(entries)-len(keysToRemove))
	var bytesRemoved int64

	for k, v := range entries {
		if keysToRemove[k] {
			bytesRemoved += int64(len(k) + len(v))
			continue
		}
		newEntries[k] = v
		newAccessCnt[k] = accessCnt[k]
	}

	return newEntries, newAccessCnt, bytesRemoved
}
