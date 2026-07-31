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
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHotCacheGetPut(t *testing.T) {
	cache := NewHotCache(100, 0)

	// Test Put and Get
	key1 := []byte("key1")
	value1 := []byte("value1")

	cache.Put(key1, value1)

	got, ok := cache.Get(key1)
	require.True(t, ok, "expected key to be found")
	assert.Equal(t, value1, got, "expected value to match")

	// Test missing key
	got, ok = cache.Get([]byte("nonexistent"))
	assert.False(t, ok, "expected key not to be found")
	assert.Nil(t, got, "expected nil value for missing key")

	// Test overwrite
	value2 := []byte("value2")
	cache.Put(key1, value2)

	got, ok = cache.Get(key1)
	require.True(t, ok, "expected key to be found after overwrite")
	assert.Equal(t, value2, got, "expected updated value")

	// Test multiple keys
	for i := range 10 {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		cache.Put(key, value)
	}

	for i := range 10 {
		key := fmt.Appendf(nil, "key%d", i)
		expectedValue := fmt.Appendf(nil, "value%d", i)
		got, ok := cache.Get(key)
		require.True(t, ok, "expected key%d to be found", i)
		assert.Equal(t, expectedValue, got, "expected value%d to match", i)
	}
}

func TestHotCacheConcurrent(t *testing.T) {
	cache := NewHotCache(1000, 0)

	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Writers
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOperations {
				key := fmt.Appendf(nil, "key-%d-%d", id, j)
				value := fmt.Appendf(nil, "value-%d-%d", id, j)
				cache.Put(key, value)
			}
		}(i)
	}

	// Readers
	for i := range numGoroutines {
		go func(id int) {
			defer wg.Done()
			for j := range numOperations {
				key := fmt.Appendf(nil, "key-%d-%d", id, j)
				cache.Get(key)
			}
		}(i)
	}

	wg.Wait()

	// Verify some entries are still accessible
	for i := range 10 {
		key := fmt.Appendf(nil, "key-%d-%d", i, numOperations-1)
		got, ok := cache.Get(key)
		if ok {
			expected := fmt.Appendf(nil, "value-%d-%d", i, numOperations-1)
			assert.Equal(t, expected, got, "value mismatch for concurrent key")
		}
	}
}

func TestHotCacheEviction(t *testing.T) {
	maxSize := 10
	cache := NewHotCache(maxSize, 0)

	// Add entries up to maxSize
	for i := range maxSize {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		cache.Put(key, value)
	}

	// Access some keys to increase their frequency
	frequentKeys := [][]byte{
		[]byte("key0"),
		[]byte("key1"),
		[]byte("key2"),
	}
	for _, key := range frequentKeys {
		for range 10 {
			cache.Get(key)
		}
	}

	// Add more entries to trigger eviction
	for i := maxSize; i < maxSize+10; i++ {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		cache.Put(key, value)
	}

	// Frequently accessed keys should still be present
	for _, key := range frequentKeys {
		_, ok := cache.Get(key)
		assert.True(
			t,
			ok,
			"frequently accessed key %s should still be present",
			string(key),
		)
	}

	// Verify cache size is controlled (may be slightly over due to concurrent ops)
	data := cache.data.Load()
	assert.LessOrEqual(
		t,
		len(data.entries),
		maxSize+5,
		"cache should be close to maxSize after eviction",
	)
}

func TestHotCacheMemoryLimit(t *testing.T) {
	maxBytes := int64(1000)
	cache := NewHotCache(1000, maxBytes)

	// Add entries that together exceed maxBytes
	valueSize := 100
	for i := range 20 {
		key := fmt.Appendf(nil, "key%d", i)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i)
		}
		cache.Put(key, value)
	}

	// Memory usage should be controlled
	assert.LessOrEqual(
		t,
		cache.data.Load().totalBytes,
		maxBytes+int64(valueSize*3),
		"memory should be close to maxBytes after eviction",
	)

	// Test that oversized entries are rejected
	// Entry > maxBytes/10 should be skipped
	oversizedKey := []byte("oversized")
	oversizedValue := make([]byte, maxBytes/10+1)
	cache.Put(oversizedKey, oversizedValue)

	_, ok := cache.Get(oversizedKey)
	assert.False(t, ok, "oversized entry should not be stored")
}

func TestHotCacheLFUEviction(t *testing.T) {
	maxSize := 5
	cache := NewHotCache(maxSize, 0)

	// Add initial entries
	for i := range maxSize {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		cache.Put(key, value)
	}

	// Access key0 many times to make it frequently used
	for range 50 {
		cache.Get([]byte("key0"))
	}

	// Access key1 a moderate number of times
	for range 20 {
		cache.Get([]byte("key1"))
	}

	// key2, key3, key4 have access count of 1 from the Put operation only

	// Add new entries to force eviction
	for i := maxSize; i < maxSize+5; i++ {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		cache.Put(key, value)
	}

	// key0 (most frequent) should still be present
	_, ok := cache.Get([]byte("key0"))
	assert.True(t, ok, "most frequently accessed key should survive eviction")

	// key1 (moderately frequent) should likely still be present
	_, ok = cache.Get([]byte("key1"))
	assert.True(t, ok, "moderately accessed key should survive eviction")
}

// TestHotCacheConcurrentPutsCompleteUnderHighContention drives many more
// concurrent writers (and readers, to exercise incrementAccess's CAS path
// too) against a small, heavily-contended cache than TestHotCacheConcurrent
// and asserts every call returns within a bounded time. This validates the
// acceptance criteria that cache insertion cannot spin indefinitely under
// high contention, using the default retry budget.
func TestHotCacheConcurrentPutsCompleteUnderHighContention(t *testing.T) {
	cache := NewHotCache(50, 0)

	const numGoroutines = 200
	const numOperations = 200

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(numGoroutines * 2)
		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()
				for j := range numOperations {
					key := fmt.Appendf(nil, "key-%d", j%20)
					value := fmt.Appendf(nil, "value-%d-%d", id, j)
					cache.Put(key, value)
				}
			}(i)
		}
		// Readers exercise incrementAccess's CAS path (see accessSampleRate)
		// concurrently with the writers above, so stats.Attempts genuinely
		// reflects contention across both CAS paths it claims to cover.
		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()
				for j := range numOperations {
					key := fmt.Appendf(nil, "key-%d", j%20)
					cache.Get(key)
				}
			}(i)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Put calls did not complete under high contention within timeout")
	}

	stats := cache.CASStats()
	assert.Positive(t, stats.Attempts, "writers should attempt cache commits")
	// SuccessfulCommitsAfterBackoff/SuccessfulCommitBackoffTime only count
	// commits that needed a *sleeping* backoff (attempt >= casYieldThreshold);
	// a writer that wins after just one or two zero-duration yields
	// contributes to neither, so asserting either is positive is flaky on a
	// lightly-loaded or single-core runner. Attempts exceeding the total
	// number of Put calls is a threshold-independent proof that contention
	// forced at least one writer to retry rather than succeed on its first
	// CAS attempt every time.
	assert.Greater(
		t,
		stats.Attempts,
		uint64(numGoroutines*numOperations),
		"heavy contention should force at least one writer to retry, "+
			"not succeed on the first CAS attempt every time",
	)

	_, ok := cache.Get([]byte("key-0"))
	assert.True(t, ok, "contention should still leave successfully committed entries")
}

// TestHotCacheRetryBudgetFallback deterministically exercises the CAS
// retry-budget fallback by allowing one CAS attempt and hammering a shared
// cache with concurrent writers, then asserts the cache degrades gracefully
// (no panic, no hang) and reports writers that drop their best-effort update.
func TestHotCacheRetryBudgetFallback(t *testing.T) {
	cache := NewHotCache(50, 0)
	cache.maxCASAttempts = 1

	const numGoroutines = 100
	const numOperations = 100

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(id int) {
				defer wg.Done()
				for j := range numOperations {
					key := fmt.Appendf(nil, "key-%d", j%10)
					value := fmt.Appendf(nil, "value-%d-%d", id, j)
					cache.Put(key, value)
				}
			}(i)
		}
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Put calls did not complete within timeout under a tiny retry budget")
	}

	assert.Positive(
		t,
		cache.CASStats().WritersAbortedAfterBudget,
		"expected at least one writer to exhaust a one-attempt budget under contention",
	)

	// Cache must remain usable after the fallback triggers.
	cache.Put([]byte("after-fallback"), []byte("still-works"))
	got, ok := cache.Get([]byte("after-fallback"))
	assert.True(t, ok, "cache should still accept writes after a retry-budget fallback")
	assert.Equal(t, []byte("still-works"), got)
}

// TestHotCacheLogsWriterAbortedOnBudgetExhaustion deterministically forces
// the writer-aborted-after-budget path (by setting maxCASAttempts to 0, so
// the CAS loop body never runs) and asserts a configured logger reports it,
// with no reliance on real contention or goroutine timing.
func TestHotCacheLogsWriterAbortedOnBudgetExhaustion(t *testing.T) {
	cache := NewHotCache(10, 0)
	cache.maxCASAttempts = 0

	var buf bytes.Buffer
	cache.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)), "test-cache")

	cache.Put([]byte("key"), []byte("value"))

	logOutput := buf.String()
	assert.Contains(
		t,
		logOutput,
		"hot cache dropped a best-effort update after exhausting its CAS retry budget",
	)
	assert.Contains(t, logOutput, "cache=test-cache")
	assert.Contains(t, logOutput, "op=put")
	assert.Equal(t, uint64(1), cache.CASStats().WritersAbortedAfterBudget)
}

// TestHotCacheLogWriterAbortedIsRateLimited forces many deterministic
// aborts in a tight loop (via maxCASAttempts = 0, so every Put aborts) and
// asserts logging is rate-limited to at most one line, while the
// WritersAbortedAfterBudget counter still reflects every single abort. This
// guards against sustained contention turning CPU churn into log/IO churn.
func TestHotCacheLogWriterAbortedIsRateLimited(t *testing.T) {
	cache := NewHotCache(10, 0)
	cache.maxCASAttempts = 0

	var buf bytes.Buffer
	cache.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)), "test-cache")

	const attempts = 1000
	for i := range attempts {
		cache.Put(fmt.Appendf(nil, "key-%d", i), []byte("value"))
	}

	assert.Equal(
		t,
		uint64(attempts),
		cache.CASStats().WritersAbortedAfterBudget,
		"the counter must remain authoritative regardless of log rate limiting",
	)

	logLines := strings.Count(
		buf.String(),
		"hot cache dropped a best-effort update",
	)
	assert.Equal(
		t,
		1,
		logLines,
		"logging must be rate-limited to at most one line per abortLogInterval "+
			"even though every one of %d Put calls aborted",
		attempts,
	)
}

// TestHotCacheNilLoggerDoesNotPanic confirms that a cache with no logger
// configured (the default) silently drops the writer-aborted event instead
// of panicking on a nil logger dereference.
func TestHotCacheNilLoggerDoesNotPanic(t *testing.T) {
	cache := NewHotCache(10, 0)
	cache.maxCASAttempts = 0

	require.NotPanics(t, func() {
		cache.Put([]byte("key"), []byte("value"))
	})
	assert.Equal(t, uint64(1), cache.CASStats().WritersAbortedAfterBudget)
}

// TestHotCacheRegisterCASMetrics verifies that RegisterCASMetrics exposes
// live CAS contention counters on a Prometheus registry, that the counters
// reflect this cache's actual state, and that registering twice on the same
// registry is a safe no-op rather than an error or duplicate series.
func TestHotCacheRegisterCASMetrics(t *testing.T) {
	cache := NewHotCache(10, 0)
	registry := prometheus.NewRegistry()

	require.NoError(t, cache.RegisterCASMetrics(registry, "test"))
	require.NoError(t, cache.RegisterCASMetrics(registry, "test")) // reuse is a no-op

	cache.Put([]byte("key1"), []byte("value1"))
	cache.maxCASAttempts = 0
	cache.Put([]byte("key2"), []byte("value2")) // deterministically aborts

	families, err := registry.Gather()
	require.NoError(t, err)
	found := map[string]float64{}
	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			found[mf.GetName()] = m.GetCounter().GetValue()
		}
	}

	assert.Positive(t, found["dingo_hot_cache_cas_attempts_total"])
	assert.Equal(t, float64(1), found["dingo_hot_cache_writers_aborted_after_budget_total"])
	assert.Contains(t, found, "dingo_hot_cache_successful_commits_after_backoff_total")
	assert.Contains(t, found, "dingo_hot_cache_successful_commit_backoff_seconds_total")
}

func TestHotCacheEmptyCache(t *testing.T) {
	cache := NewHotCache(10, 0)

	// Get from empty cache
	got, ok := cache.Get([]byte("nonexistent"))
	assert.False(t, ok)
	assert.Nil(t, got)
}

func TestHotCacheNilKey(t *testing.T) {
	cache := NewHotCache(10, 0)

	// Put with nil key
	cache.Put(nil, []byte("value"))

	// Should be retrievable with empty key
	got, ok := cache.Get(nil)
	assert.True(t, ok)
	assert.Equal(t, []byte("value"), got)
}

func TestHotCacheZeroMaxSize(t *testing.T) {
	// Zero maxSize means unlimited by count
	cache := NewHotCache(0, 1000)

	for i := range 100 {
		key := fmt.Appendf(nil, "key%d", i)
		value := fmt.Appendf(nil, "value%d", i)
		cache.Put(key, value)
	}

	// All entries should be present (limited only by bytes)
	count := 0
	data := cache.data.Load()
	if data != nil {
		count = len(data.entries)
	}
	assert.Greater(t, count, 0, "cache should have entries")
}

func TestHotCacheSmallMaxSize(t *testing.T) {
	// Test that maxSize=1 works correctly (edge case for eviction)
	cache := NewHotCache(1, 0)

	// Add first entry
	cache.Put([]byte("key1"), []byte("value1"))
	val, ok := cache.Get([]byte("key1"))
	assert.True(t, ok, "first entry should be retrievable")
	assert.Equal(t, []byte("value1"), val)

	// Add second entry - should trigger eviction but keep at least 1
	cache.Put([]byte("key2"), []byte("value2"))

	// At least one entry should exist (either key1 or key2)
	data := cache.data.Load()
	assert.NotNil(t, data)
	assert.GreaterOrEqual(
		t,
		len(data.entries),
		1,
		"cache with maxSize=1 should keep at least 1 entry",
	)
	assert.LessOrEqual(
		t,
		len(data.entries),
		2,
		"cache with maxSize=1 should have at most 2 entries",
	)
}

// TestHotCachePutNeverPermanentlyExceedsMaxSizeUnderGetContention reproduces
// the reported regression: eviction run as a separate operation after Put
// could lose its own CAS race against concurrent access-count updates from
// Get, and since only Put retries eviction, a cache that never received
// another Put stayed oversized forever. Each round fills a small cache,
// races heavy Get-driven incrementAccess contention against a single
// overflow Put, and asserts the final state never exceeds maxSize. Because
// eviction is now folded into the overflowing Put's own CAS attempt (see
// HotCache.evictToFit), this must hold on every round, not just on average.
func TestHotCachePutNeverPermanentlyExceedsMaxSizeUnderGetContention(t *testing.T) {
	const maxSize = 10
	const rounds = 50
	const numReaders = 50
	const readsPerReader = 200

	for round := range rounds {
		cache := NewHotCache(maxSize, 0)
		keys := make([][]byte, maxSize)
		for i := range keys {
			keys[i] = fmt.Appendf(nil, "key-%d-%d", round, i)
			cache.Put(keys[i], []byte("value"))
		}

		var wg sync.WaitGroup
		wg.Add(numReaders + 1)

		// Concurrent readers hammer Get() on existing keys, forcing
		// incrementAccess CAS contention against the overflow Put below.
		for r := range numReaders {
			go func(id int) {
				defer wg.Done()
				for j := range readsPerReader {
					cache.Get(keys[(id+j)%maxSize])
				}
			}(r)
		}
		// One overflow Put racing the readers; this must evict.
		go func() {
			defer wg.Done()
			cache.Put(fmt.Appendf(nil, "key-%d-overflow", round), []byte("value"))
		}()

		wg.Wait()

		data := cache.data.Load()
		require.NotNil(t, data)
		assert.LessOrEqual(
			t,
			len(data.entries),
			maxSize,
			"round %d: cache must not permanently exceed maxSize after an "+
				"insert that pushed it over the limit",
			round,
		)
	}
}

// TestEvictToFitNoEvictionWhenWithinLimits verifies evictToFit is a no-op
// (returns the inputs and zero bytes removed) when neither maxSize nor
// maxBytes is exceeded.
func TestEvictToFitNoEvictionWhenWithinLimits(t *testing.T) {
	cache := NewHotCache(10, 100)
	entries := map[string][]byte{"a": []byte("1"), "b": []byte("2")}
	accessCnt := map[string]uint64{"a": 5, "b": 3}

	gotEntries, gotAccessCnt, bytesRemoved := cache.evictToFit(entries, accessCnt, 4)

	assert.Equal(t, entries, gotEntries)
	assert.Equal(t, accessCnt, gotAccessCnt)
	assert.Zero(t, bytesRemoved)
}

// TestEvictToFitRemovesLeastFrequentlyUsedFirstBySize verifies size-based
// eviction trims down to the target size and keeps the most frequently
// accessed entries, removing the least frequently accessed ones first.
func TestEvictToFitRemovesLeastFrequentlyUsedFirstBySize(t *testing.T) {
	cache := NewHotCache(4, 0)
	entries := map[string][]byte{
		"most":     []byte("v"),
		"more":     []byte("v"),
		"less":     []byte("v"),
		"least":    []byte("v"),
		"overflow": []byte("v"),
	}
	accessCnt := map[string]uint64{
		"most":     100,
		"more":     50,
		"less":     10,
		"least":    1,
		"overflow": 1,
	}

	gotEntries, gotAccessCnt, bytesRemoved := cache.evictToFit(entries, accessCnt, 0)

	// target size = max(1, 4*3/4) = 3
	assert.LessOrEqual(t, len(gotEntries), 3)
	assert.Contains(t, gotEntries, "most", "highest access count must survive eviction")
	assert.Contains(t, gotEntries, "more", "second highest access count must survive eviction")
	assert.NotContains(t, gotEntries, "least", "lowest access count should be evicted first")
	assert.Equal(t, len(gotEntries), len(gotAccessCnt))
	assert.Positive(t, bytesRemoved)
}

// TestEvictToFitByBytes verifies byte-based eviction trims down to the
// target byte budget, again preferring to keep the most frequently accessed
// entries.
func TestEvictToFitByBytes(t *testing.T) {
	cache := NewHotCache(0, 100) // maxBytes=100, maxSize unlimited
	entries := map[string][]byte{
		"a": make([]byte, 40),
		"b": make([]byte, 40),
		"c": make([]byte, 40),
	}
	accessCnt := map[string]uint64{"a": 10, "b": 5, "c": 1}

	var estimatedBytes int64
	for k, v := range entries {
		estimatedBytes += int64(len(k) + len(v))
	}

	gotEntries, _, bytesRemoved := cache.evictToFit(entries, accessCnt, estimatedBytes)

	// Each entry is 1 (key) + 40 (value) = 41 bytes; target = 100*3/4 = 75,
	// so only the highest-count entry ("a") fits.
	assert.Equal(t, int64(82), bytesRemoved)
	assert.Equal(t, map[string][]byte{"a": entries["a"]}, gotEntries)
}

// TestEvictToFitKeepsAtLeastOneEntryWhenMaxSizeIsOne is the size=1 edge
// case: eviction must never trim a non-empty cache down to zero entries.
func TestEvictToFitKeepsAtLeastOneEntryWhenMaxSizeIsOne(t *testing.T) {
	cache := NewHotCache(1, 0)
	entries := map[string][]byte{"a": []byte("1"), "b": []byte("2")}
	accessCnt := map[string]uint64{"a": 5, "b": 1}

	gotEntries, _, _ := cache.evictToFit(entries, accessCnt, 0)

	assert.GreaterOrEqual(t, len(gotEntries), 1)
	assert.LessOrEqual(t, len(gotEntries), 2)
}

// TestHotCacheTotalBytesStaysAccurateAcrossEvictions replaces an earlier
// version of this test that proved a real gap by directly desyncing a
// separate curBytes atomic (cache.curBytes.Store(0)) to simulate a delayed
// concurrent writer, then showing Put trusted the stale value and let the
// cache grow over maxBytes. That attack is no longer expressible at all:
// totalBytes now lives inside hotCacheData itself, so every read of it is
// tied to the exact entries snapshot it describes -- there is no separate
// counter left to desync. This test instead verifies the new invariant
// directly: after many sequential Puts (some of which force eviction),
// data.totalBytes must always equal the real, independently-computed sum of
// entry sizes, and must never exceed maxBytes.
func TestHotCacheTotalBytesStaysAccurateAcrossEvictions(t *testing.T) {
	const maxBytes = 1000             // per-entry cutoff = maxBytes/10 = 100
	cache := NewHotCache(0, maxBytes) // maxSize unlimited: purely byte-driven

	for i := range 30 {
		cache.Put(fmt.Appendf(nil, "key-%d", i), make([]byte, 90)) // 95 bytes each; forces repeated eviction
	}

	data := cache.data.Load()
	require.NotNil(t, data)
	var actualBytes int64
	for k, v := range data.entries {
		actualBytes += int64(len(k) + len(v))
	}

	assert.Equal(
		t, actualBytes, data.totalBytes,
		"totalBytes must always match the real sum of entries",
	)
	assert.LessOrEqual(t, data.totalBytes, int64(maxBytes))
}

// TestHotCachePutNeverPermanentlyExceedsMaxBytesUnderPutContention is the
// byte-limit counterpart to
// TestHotCachePutNeverPermanentlyExceedsMaxSizeUnderGetContention: many
// goroutines concurrently Put small entries against a small maxBytes limit,
// and the final real byte total (computed independently from data.entries,
// not trusted from any counter) must never permanently exceed maxBytes.
//
// This used to be a probabilistic, largely-decorative check: an earlier
// design tracked the byte total in a separately-updated atomic (curBytes),
// racy relative to the entries snapshot in a window only one or two
// instructions wide -- narrow enough that a direct probe with 300 concurrent
// writers over 50 rounds reproduced zero failures even on the buggy code
// (see TestHotCacheTotalBytesStaysAccurateAcrossEvictions for how that gap
// was proven instead). Now that the byte total lives inside hotCacheData
// itself, this test holds deterministically, every round, by construction.
func TestHotCachePutNeverPermanentlyExceedsMaxBytesUnderPutContention(t *testing.T) {
	const maxBytes = 2000 // per-entry cutoff = maxBytes/10 = 200
	const rounds = 20
	const numWriters = 200
	const valueSize = 90 // 90 + ~10-byte key ~= 100, under the per-entry cutoff

	for round := range rounds {
		cache := NewHotCache(0, maxBytes) // maxSize unlimited: purely byte-driven

		var wg sync.WaitGroup
		wg.Add(numWriters)
		for w := range numWriters {
			go func(id int) {
				defer wg.Done()
				key := fmt.Appendf(nil, "key-%d-%d", round, id)
				cache.Put(key, make([]byte, valueSize))
			}(w)
		}
		wg.Wait()

		data := cache.data.Load()
		require.NotNil(t, data)
		var actualBytes int64
		for k, v := range data.entries {
			actualBytes += int64(len(k) + len(v))
		}
		assert.LessOrEqual(
			t, actualBytes, int64(maxBytes),
			"round %d: cache holds %d bytes, over maxBytes=%d",
			round, actualBytes, maxBytes,
		)
	}
}
