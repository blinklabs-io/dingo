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
	"container/list"
	"errors"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type hotCacheEntry struct {
	value        []byte
	accessCnt    uint64
	orderElement *list.Element
}

type hotCacheShard struct {
	mu      sync.RWMutex
	entries map[string]hotCacheEntry
}

// accessSampleRate controls how often access counts are updated on Get().
// A value of 4 means ~25% of Gets trigger an access count update.
// This trades LFU accuracy for performance by avoiding map copies on every read.
const accessSampleRate = 4

// hotCacheShardCount spreads readers and sampled-access updates across
// independent locks. It is fixed so routine operation cost does not grow with
// configured cache cardinality.
const hotCacheShardCount = 64

// hotCacheEvictionSampleSize bounds the number of entries inspected by an
// overflowing Put. Eviction is approximate LFU: the least-used entries from
// this fixed candidate window are removed until the pending value fits. The
// fixed window keeps admission CPU and temporary memory independent of total
// cache cardinality.
const hotCacheEvictionSampleSize = 64

type hotCacheEvictionCandidate struct {
	key          string
	accessCnt    uint64
	size         int64
	order        int
	orderElement *list.Element
}

const (
	// defaultMaxCASAttempts bounds the work spent acquiring an update lock for
	// one best-effort Put or access-count update. The name is retained for
	// compatibility with the existing stats and metrics API.
	defaultMaxCASAttempts = 16
	// casYieldThreshold is the retry count after which an update loop starts
	// sleeping a small, randomized amount instead of just yielding the
	// processor, so contending writers desynchronize instead of retrying
	// in lockstep.
	casYieldThreshold = 2
	// maxCASBackoff caps the randomized sleep between update retries.
	maxCASBackoff = 64 * time.Microsecond
	// abortLogInterval rate-limits the writer-aborted warning log so that
	// sustained contention produces bounded log/IO traffic instead of one
	// warning per dropped update. The writersAborted counter is unaffected
	// and remains authoritative regardless of how many log lines this
	// suppresses.
	abortLogInterval = time.Second
)

// HotCacheCASStats describes contention in HotCache's update path. The type
// and field names predate the sharded implementation and remain stable for
// API and metrics compatibility. Values are cumulative for the cache's life.
type HotCacheCASStats struct {
	// Attempts is the total number of non-blocking lock attempts across Put
	// and access-count tracking.
	Attempts uint64
	// WritersAbortedAfterBudget is the number of best-effort updates that
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

// HotCache provides a sharded cache for frequently accessed CBOR data. Reads
// take a shard read lock, while writes are serialized only long enough to keep
// global size and byte accounting exact. Eviction follows an approximate
// Least-Frequently-Used (LFU) policy with probabilistic counting.
type HotCache struct {
	shards              [hotCacheShardCount]hotCacheShard
	updateMu            sync.Mutex
	evictionOrder       list.List
	size                int
	totalBytes          int64
	maxSize             int           // max number of entries (0 = unlimited)
	maxBytes            int64         // max memory in bytes (0 = unlimited)
	sampleCnt           atomic.Uint64 // counter for probabilistic access tracking
	casSequence         atomic.Uint64 // gives contending updates asymmetric jitter
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
// a warning whenever an update is dropped after exhausting the retry budget
// (see HotCacheCASStats.WritersAbortedAfterBudget). name
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
// update path gave up (put or increment_access).
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
		"hot cache dropped a best-effort update after exhausting its retry budget",
		"cache",
		c.cacheName,
		"op",
		op,
		"max_attempts",
		c.maxCASAttempts,
		"writers_aborted_total",
		c.writersAborted.Load(),
	)
}

// RegisterCASMetrics exposes this cache's update-contention counters (see
// HotCacheCASStats) on the given Prometheus registry, labeled by cacheName
// (e.g. "utxo", "tx"). The method and metric names retain their historical
// CAS terminology for compatibility. If registry is nil, this is a no-op.
// This method is safe to call more than once with the same registry.
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
			Help:        "Total non-blocking update attempts across Put and access tracking",
			ConstLabels: labels,
		}, func() float64 { return float64(c.casAttempts.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "dingo_hot_cache_writers_aborted_after_budget_total",
			Help:        "Total best-effort updates dropped after exhausting the retry budget",
			ConstLabels: labels,
		}, func() float64 { return float64(c.writersAborted.Load()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name:        "dingo_hot_cache_successful_commits_after_backoff_total",
			Help:        "Total updates that succeeded only after backing off at least once",
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

// CASStats returns a snapshot of update-contention counters, suitable for
// diagnostics or metrics export. Its name is retained for compatibility.
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

// recordSuccessfulCommit records an update that committed after
// a timed backoff. Keeping this accounting in one place ensures every update
// path uses the same metric semantics.
func (c *HotCache) recordSuccessfulCommit(backoffTime time.Duration) {
	if backoffTime <= 0 {
		return
	}
	c.commitsAfterBackoff.Add(1)
	c.commitBackoffNanos.Add(uint64(backoffTime))
}

// backoff waits between update retries: an immediate scheduler yield for the
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
	jitter := time.Duration(
		1+(sequence^(sequence>>30))%window,
	) * time.Microsecond
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

	for i := range cache.shards {
		cache.shards[i].entries = make(map[string]hotCacheEntry)
	}

	return cache
}

func hotCacheShardIndexBytes(key []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for _, value := range key {
		hash ^= uint64(value)
		hash *= prime64
	}
	return hash % hotCacheShardCount
}

func hotCacheShardIndexString(key string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	hash := uint64(offset64)
	for i := range len(key) {
		hash ^= uint64(key[i])
		hash *= prime64
	}
	return hash % hotCacheShardCount
}

// Get retrieves a value from the cache by key.
// Returns the value and true if found, nil and false otherwise.
// This operation is safe for concurrent use and locks only one shard.
// Access counts are updated probabilistically (1 in accessSampleRate calls)
// to reduce overhead while maintaining approximate LFU behavior.
func (c *HotCache) Get(key []byte) ([]byte, bool) {
	shard := &c.shards[hotCacheShardIndexBytes(key)]
	shard.mu.RLock()
	entry, ok := shard.entries[string(key)]
	if ok {
		// Copy while holding the read lock so a replacement cannot recycle the
		// backing array before the caller receives an isolated value.
		value := append([]byte(nil), entry.value...)
		shard.mu.RUnlock()
		// Probabilistic counting: only update access count 1 in accessSampleRate times
		// This keeps write-lock traffic low while maintaining approximate LFU.
		if c.sampleCnt.Add(1)%accessSampleRate == 0 {
			c.incrementAccess(key)
		}
		return value, true
	}
	shard.mu.RUnlock()
	return nil, false
}

// Put adds or updates a value in the cache.
// If maxBytes > 0 and the entry size exceeds maxBytes/10, the entry is skipped.
// This operation uses a bounded, non-blocking attempt to enter the serialized
// update path. If the insert pushes the cache over maxSize/maxBytes, eviction
// is completed before the update lock is released, so the cache cannot remain
// over its configured limits after Put returns.
func (c *HotCache) Put(key []byte, cbor []byte) {
	entrySize := int64(len(key) + len(cbor))

	// Skip entries that are too large (> 10% of max memory)
	if c.maxBytes > 0 && entrySize > c.maxBytes/10 {
		return
	}

	backoffTime, ok := c.beginUpdate()
	if !ok {
		c.writersAborted.Add(1)
		c.logWriterAborted("put")
		return
	}
	defer c.updateMu.Unlock()

	keyStr := string(key)
	shard := &c.shards[hotCacheShardIndexBytes(key)]

	shard.mu.RLock()
	oldEntry, exists := shard.entries[keyStr]
	shard.mu.RUnlock()

	projectedSize := c.size
	projectedBytes := c.totalBytes + entrySize
	if exists {
		projectedBytes -= int64(len(keyStr) + len(oldEntry.value))
	} else {
		projectedSize++
	}

	victims, fits := c.selectEvictionVictimsLocked(
		keyStr,
		projectedSize,
		projectedBytes,
	)
	if !fits {
		// Cache population is best-effort. If a bounded candidate window
		// cannot free enough room, preserve the current entries and advance
		// the window so later bounded attempts can inspect other candidates
		// rather than dropping every future admission behind the same prefix.
		c.rotateEvictionCandidatesLocked(victims)
		return
	}
	for _, victim := range victims {
		c.removeEntryLocked(victim.key)
	}

	// Copy only after admission succeeds so a dropped best-effort update does
	// not allocate a value it will never retain.
	cborCopy := append([]byte(nil), cbor...)
	shard.mu.Lock()
	oldEntry, exists = shard.entries[keyStr]
	accessCnt := uint64(1)
	var orderElement *list.Element
	if exists {
		accessCnt = oldEntry.accessCnt + 1
		orderElement = oldEntry.orderElement
		c.evictionOrder.MoveToBack(orderElement)
		c.totalBytes -= int64(len(keyStr) + len(oldEntry.value))
	} else {
		orderElement = c.evictionOrder.PushBack(keyStr)
		c.size++
	}
	shard.entries[keyStr] = hotCacheEntry{
		value:        cborCopy,
		accessCnt:    accessCnt,
		orderElement: orderElement,
	}
	c.totalBytes += entrySize
	shard.mu.Unlock()
	c.recordSuccessfulCommit(backoffTime)
}

// beginUpdate makes update admission best-effort and bounded while keeping
// the global size and byte counters serialized. The historical CAS counters
// now report these non-blocking lock attempts so existing dashboards retain
// their contention signal.
func (c *HotCache) beginUpdate() (time.Duration, bool) {
	var backoffTime time.Duration
	for attempt := 0; attempt < c.maxCASAttempts; attempt++ {
		c.casAttempts.Add(1)
		if c.updateMu.TryLock() {
			return backoffTime, true
		}
		backoffTime += c.backoff(attempt)
	}
	return backoffTime, false
}

// incrementAccess increments the access counter for a key.
// The update takes only the owning shard's lock and remains best-effort. It
// only increments if the key still exists to prevent orphan access counts.
func (c *HotCache) incrementAccess(key []byte) {
	keyStr := string(key)
	shard := &c.shards[hotCacheShardIndexBytes(key)]

	var backoffTime time.Duration
	for attempt := 0; attempt < c.maxCASAttempts; attempt++ {
		c.casAttempts.Add(1)
		if shard.mu.TryLock() {
			entry, exists := shard.entries[keyStr]
			if !exists {
				shard.mu.Unlock()
				return
			}
			entry.accessCnt++
			shard.entries[keyStr] = entry
			shard.mu.Unlock()
			c.recordSuccessfulCommit(backoffTime)
			return
		}
		backoffTime += c.backoff(attempt)
	}
	c.writersAborted.Add(1)
	c.logWriterAborted("increment_access")
}

// selectEvictionVictimsLocked requires updateMu. It inspects at most a fixed
// number of entries from the front of the insertion/replacement order and
// chooses the least frequently used candidates from that window. It returns
// false with the attempted candidates when the bounded sample cannot restore
// all configured limits. The caller may rotate that failed window without
// changing membership so later bounded attempts can make progress.
func (c *HotCache) selectEvictionVictimsLocked(
	excludeKey string,
	projectedSize int,
	projectedBytes int64,
) ([]hotCacheEvictionCandidate, bool) {
	entriesNeeded := 0
	if c.maxSize > 0 && projectedSize > c.maxSize {
		entriesNeeded = projectedSize - c.maxSize
	}
	bytesNeeded := int64(0)
	if c.maxBytes > 0 && projectedBytes > c.maxBytes {
		bytesNeeded = projectedBytes - c.maxBytes
	}
	if entriesNeeded == 0 && bytesNeeded == 0 {
		return nil, true
	}

	candidates := make(
		[]hotCacheEvictionCandidate,
		0,
		min(c.evictionOrder.Len(), hotCacheEvictionSampleSize),
	)
	order := 0
	for element := c.evictionOrder.Front(); element != nil && len(candidates) < hotCacheEvictionSampleSize; element = element.Next() {
		key := element.Value.(string)
		if key == excludeKey {
			continue
		}
		shard := &c.shards[hotCacheShardIndexString(key)]
		shard.mu.RLock()
		entry, exists := shard.entries[key]
		shard.mu.RUnlock()
		if !exists {
			continue
		}
		candidates = append(candidates, hotCacheEvictionCandidate{
			key:          key,
			accessCnt:    entry.accessCnt,
			size:         int64(len(key) + len(entry.value)),
			order:        order,
			orderElement: entry.orderElement,
		})
		order++
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].accessCnt != candidates[j].accessCnt {
			return candidates[i].accessCnt < candidates[j].accessCnt
		}
		if candidates[i].size != candidates[j].size {
			return candidates[i].size > candidates[j].size
		}
		return candidates[i].order < candidates[j].order
	})

	victims := make([]hotCacheEvictionCandidate, 0, len(candidates))
	var bytesFreed int64
	for _, candidate := range candidates {
		if len(victims) >= entriesNeeded && bytesFreed >= bytesNeeded {
			break
		}
		victims = append(victims, candidate)
		bytesFreed += candidate.size
	}
	if len(victims) < entriesNeeded || bytesFreed < bytesNeeded {
		return victims, false
	}
	return victims, true
}

// rotateEvictionCandidatesLocked requires updateMu. It moves a failed bounded
// sample to the back of the scan order without evicting it. The next admission
// can therefore inspect a different fixed-size window while every individual
// Put remains independent of total cache cardinality.
func (c *HotCache) rotateEvictionCandidatesLocked(
	candidates []hotCacheEvictionCandidate,
) {
	// selectEvictionVictimsLocked sorts by LFU preference. Restore the original
	// scan order before moving the window so its relative order stays stable.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].order < candidates[j].order
	})
	for _, candidate := range candidates {
		c.evictionOrder.MoveToBack(candidate.orderElement)
	}
}

// removeEntryLocked requires updateMu and removes one selected victim while
// keeping membership, byte accounting, and eviction order consistent.
func (c *HotCache) removeEntryLocked(key string) {
	shard := &c.shards[hotCacheShardIndexString(key)]
	shard.mu.Lock()
	entry, exists := shard.entries[key]
	if exists {
		delete(shard.entries, key)
		c.size--
		c.totalBytes -= int64(len(key) + len(entry.value))
		c.evictionOrder.Remove(entry.orderElement)
	}
	shard.mu.Unlock()
}
