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

type hotCacheSnapshot struct {
	entries    map[string][]byte
	totalBytes int64
}

func snapshotHotCache(cache *HotCache) *hotCacheSnapshot {
	cache.updateMu.Lock()
	defer cache.updateMu.Unlock()
	data := &hotCacheSnapshot{
		entries:    make(map[string][]byte, cache.size),
		totalBytes: cache.totalBytes,
	}
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.mu.RLock()
		for key, entry := range shard.entries {
			data.entries[key] = entry.value
		}
		shard.mu.RUnlock()
	}
	return data
}

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

func TestHotCacheMutationIsolation(t *testing.T) {
	cache := NewHotCache(10, 0)
	key := []byte("key")
	value := []byte("original")
	cache.Put(key, value)

	key[0] = 'X'
	value[0] = 'X'
	got, ok := cache.Get([]byte("key"))
	require.True(t, ok)
	require.Equal(t, []byte("original"), got)

	got[0] = 'X'
	gotAgain, ok := cache.Get([]byte("key"))
	require.True(t, ok)
	require.Equal(t, []byte("original"), gotAgain)
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

	// Verify cache size is controlled exactly after Put returns.
	data := snapshotHotCache(cache)
	assert.LessOrEqual(
		t,
		len(data.entries),
		maxSize,
		"cache should not exceed maxSize after eviction",
	)
}

func TestHotCacheMemoryLimit(t *testing.T) {
	maxBytes := int64(1000)
	cache := NewHotCache(1000, maxBytes)

	// Add entries that together exceed maxBytes
	valueSize := 90
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
		snapshotHotCache(cache).totalBytes,
		maxBytes,
		"memory should not exceed maxBytes after eviction",
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

// TestHotCacheOperationsHaveBoundedAllocations guards against cache
// cardinality leaking into replacement, unique-admission, and sampled-access
// paths. Each operation may allocate for its key/value and fixed eviction
// sample, but never for a copy of every retained entry.
func TestHotCacheOperationsHaveBoundedAllocations(t *testing.T) {
	const cardinality = 1000
	cache := NewHotCache(cardinality, 0)
	for i := range cardinality {
		cache.Put(
			fmt.Appendf(nil, "key-%04d", i),
			[]byte("cached-value"),
		)
	}

	key := []byte("key-0000")
	putAllocs := testing.AllocsPerRun(10, func() {
		cache.Put(key, []byte("replacement"))
	})
	assert.LessOrEqual(
		t,
		putAllocs,
		float64(8),
		"Put must not allocate in proportion to cache cardinality",
	)

	churnIndex := 0
	churnAllocs := testing.AllocsPerRun(10, func() {
		cache.Put(
			fmt.Appendf(nil, "churn-key-%04d", churnIndex),
			[]byte("replacement"),
		)
		churnIndex++
	})
	assert.LessOrEqual(
		t,
		churnAllocs,
		float64(12),
		"overflow admission must allocate only for a bounded eviction sample",
	)

	accessAllocs := testing.AllocsPerRun(10, func() {
		cache.incrementAccess(key)
	})
	assert.LessOrEqual(
		t,
		accessAllocs,
		float64(1),
		"sampled access tracking must not allocate in proportion to cache cardinality",
	)
}

func TestHotCacheOperationBytesDoNotScaleWithCardinality(t *testing.T) {
	const (
		smallCardinality = 128
		largeCardinality = 4096
	)

	for _, operation := range []struct {
		name  string
		churn bool
	}{
		{name: "replacement"},
		{name: "unique admission churn", churn: true},
	} {
		t.Run(operation.name, func(t *testing.T) {
			smallBytes := benchmarkHotCachePutBytes(
				smallCardinality,
				operation.churn,
			)
			largeBytes := benchmarkHotCachePutBytes(
				largeCardinality,
				operation.churn,
			)

			// Runtime and allocator bookkeeping can add small fixed noise. A
			// two-times-plus-1 KiB allowance is deliberately generous to that
			// noise, but still rejects copying a retained map whose allocation
			// grows linearly from 128 to 4096 entries.
			assert.LessOrEqual(
				t,
				largeBytes,
				smallBytes*2+1024,
				"bytes per operation must remain independent of retained cardinality: small=%d large=%d",
				smallBytes,
				largeBytes,
			)
		})
	}
}

func benchmarkHotCachePutBytes(cardinality int, churn bool) int64 {
	result := testing.Benchmark(func(b *testing.B) {
		cache, replacementKey := populatedHotCache(cardinality)
		value := []byte("replacement-value")
		nextKey := cardinality
		key := make([]byte, 0, 32)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			if churn {
				key = fmt.Appendf(key[:0], "churn-%08d", nextKey)
				nextKey++
			} else {
				key = replacementKey
			}
			cache.Put(key, value)
		}
	})
	return result.AllocedBytesPerOp()
}

// TestHotCacheConcurrentPutsCompleteUnderHighContention drives many more
// concurrent writers (and readers, to exercise incrementAccess's update path
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
		// Readers exercise incrementAccess's update path (see accessSampleRate)
		// concurrently with the writers above, so stats.Attempts genuinely
		// reflects contention across both update paths it claims to cover.
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
		t.Fatal(
			"Put calls did not complete under high contention within timeout",
		)
	}

	stats := cache.CASStats()
	assert.Positive(t, stats.Attempts, "writers should attempt cache commits")

	_, ok := cache.Get([]byte("key-0"))
	assert.True(
		t,
		ok,
		"contention should still leave successfully committed entries",
	)
}

// TestHotCacheRetryBudgetFallback deterministically holds the update lock
// while a one-attempt Put runs, then asserts the cache degrades gracefully
// and remains usable after dropping the best-effort update.
func TestHotCacheRetryBudgetFallback(t *testing.T) {
	cache := NewHotCache(50, 0)
	cache.maxCASAttempts = 1

	cache.updateMu.Lock()
	done := make(chan struct{})
	go func() {
		cache.Put([]byte("blocked"), []byte("dropped"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		cache.updateMu.Unlock()
		t.Fatal(
			"Put calls did not complete within timeout under a tiny retry budget",
		)
	}
	cache.updateMu.Unlock()

	assert.Equal(
		t,
		uint64(1),
		cache.CASStats().WritersAbortedAfterBudget,
		"the blocked writer should exhaust its one-attempt budget",
	)

	// Cache must remain usable after the fallback triggers.
	cache.Put([]byte("after-fallback"), []byte("still-works"))
	got, ok := cache.Get([]byte("after-fallback"))
	assert.True(
		t,
		ok,
		"cache should still accept writes after a retry-budget fallback",
	)
	assert.Equal(t, []byte("still-works"), got)
}

// TestHotCacheLogsWriterAbortedOnBudgetExhaustion deterministically forces
// the writer-aborted-after-budget path (by setting maxCASAttempts to 0, so
// the update loop body never runs) and asserts a configured logger reports it,
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
		"hot cache dropped a best-effort update after exhausting its retry budget",
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

// TestHotCacheRegisterCASMetrics verifies that the compatibility metrics
// expose live update contention on a Prometheus registry, reflect this
// cache's actual state, and safely tolerate duplicate registration.
func TestHotCacheRegisterCASMetrics(t *testing.T) {
	cache := NewHotCache(10, 0)
	registry := prometheus.NewRegistry()

	require.NoError(t, cache.RegisterCASMetrics(registry, "test"))
	require.NoError(
		t,
		cache.RegisterCASMetrics(registry, "test"),
	) // reuse is a no-op

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
	assert.Equal(
		t,
		float64(1),
		found["dingo_hot_cache_writers_aborted_after_budget_total"],
	)
	assert.Contains(
		t,
		found,
		"dingo_hot_cache_successful_commits_after_backoff_total",
	)
	assert.Contains(
		t,
		found,
		"dingo_hot_cache_successful_commit_backoff_seconds_total",
	)
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
	data := snapshotHotCache(cache)
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
	data := snapshotHotCache(cache)
	assert.NotNil(t, data)
	assert.GreaterOrEqual(
		t,
		len(data.entries),
		1,
		"cache with maxSize=1 should keep at least 1 entry",
	)
	assert.LessOrEqual(t, len(data.entries), 1)
}

// TestHotCachePutNeverPermanentlyExceedsMaxSizeUnderGetContention reproduces
// the reported regression: eviction run as a separate operation after Put
// could lose its own CAS race against concurrent access-count updates from
// Get, leaving a cache oversized when no later Put retried eviction. The
// sharded implementation keeps membership and limit enforcement under the
// same update lock, so this must hold on every round.
func TestHotCachePutNeverPermanentlyExceedsMaxSizeUnderGetContention(
	t *testing.T,
) {
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
		// incrementAccess contention against the overflow Put below.
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
			cache.Put(
				fmt.Appendf(nil, "key-%d-overflow", round),
				[]byte("value"),
			)
		}()

		wg.Wait()

		data := snapshotHotCache(cache)
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

func TestHotCacheCombinedLimitsUnderChurn(t *testing.T) {
	testCases := []struct {
		name      string
		maxSize   int
		maxBytes  int64
		valueSize int
	}{
		{
			name:      "entry count is tighter",
			maxSize:   5,
			maxBytes:  1000,
			valueSize: 80,
		},
		{
			name:      "byte count is tighter",
			maxSize:   50,
			maxBytes:  1000,
			valueSize: 85,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cache := NewHotCache(testCase.maxSize, testCase.maxBytes)
			for i := range 200 {
				cache.Put(
					fmt.Appendf(nil, "key-%03d", i),
					make([]byte, testCase.valueSize),
				)
			}

			data := snapshotHotCache(cache)
			var actualBytes int64
			for key, value := range data.entries {
				actualBytes += int64(len(key) + len(value))
			}
			require.Equal(t, actualBytes, data.totalBytes)
			assert.NotEmpty(t, data.entries)
			assert.LessOrEqual(t, len(data.entries), testCase.maxSize)
			assert.LessOrEqual(t, actualBytes, testCase.maxBytes)
		})
	}
}

func TestHotCacheBoundedAdmissionDropPreservesExistingEntries(t *testing.T) {
	const maxBytes = int64(1000)
	cache := NewHotCache(0, maxBytes)
	for i := range hotCacheEvictionSampleSize {
		cache.Put([]byte{byte(i)}, nil)
	}
	for i := range 10 {
		cache.Put([]byte{0xff, byte(i)}, make([]byte, 90))
	}
	before := snapshotHotCache(cache)
	require.Equal(t, int64(984), before.totalBytes)

	// The pending 100-byte entry needs 84 bytes of space, while the fixed
	// candidate window contains 64 one-byte entries. Admission must drop the
	// pending value without partially evicting that window.
	pendingKey := []byte{0xfe}
	cache.Put(pendingKey, make([]byte, 99))

	after := snapshotHotCache(cache)
	assert.Equal(t, before.totalBytes, after.totalBytes)
	assert.Equal(t, before.entries, after.entries)
	_, ok := cache.Get(pendingKey)
	assert.False(t, ok)

	// The failed bounded sample advances instead of permanently freezing
	// admission behind the same undersized candidates. A retry can inspect the
	// larger entries that follow and admit the pending value within the same
	// fixed work bound.
	cache.Put(pendingKey, make([]byte, 99))
	afterRetry := snapshotHotCache(cache)
	assert.LessOrEqual(t, afterRetry.totalBytes, maxBytes)
	value, ok := cache.Get(pendingKey)
	require.True(t, ok)
	assert.Len(t, value, 99)
}

// TestHotCacheTotalBytesStaysAccurateAcrossEvictions replaces an earlier
// version of this test that proved a real gap by directly desyncing a
// separate curBytes atomic (cache.curBytes.Store(0)) to simulate a delayed
// concurrent writer, then showing Put trusted the stale value and let the
// cache grow over maxBytes. That attack is no longer expressible at all:
// totalBytes is now serialized with membership updates, so there is no
// separately committed entries snapshot and byte counter to desynchronize.
// This test verifies the invariant directly after repeated evictions.
func TestHotCacheTotalBytesStaysAccurateAcrossEvictions(t *testing.T) {
	const maxBytes = 1000             // per-entry cutoff = maxBytes/10 = 100
	cache := NewHotCache(0, maxBytes) // maxSize unlimited: purely byte-driven

	for i := range 30 {
		cache.Put(
			fmt.Appendf(nil, "key-%d", i),
			make([]byte, 90),
		) // 95 bytes each; forces repeated eviction
	}

	data := snapshotHotCache(cache)
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
// was proven instead). The sharded implementation serializes membership and
// byte accounting, so this test holds deterministically every round.
func TestHotCachePutNeverPermanentlyExceedsMaxBytesUnderPutContention(
	t *testing.T,
) {
	const maxBytes = 2000 // per-entry cutoff = maxBytes/10 = 200
	const rounds = 20
	const numWriters = 200
	const valueSize = 90 // 90 + ~10-byte key ~= 100, under the per-entry cutoff

	for round := range rounds {
		cache := NewHotCache(
			0,
			maxBytes,
		) // maxSize unlimited: purely byte-driven

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

		data := snapshotHotCache(cache)
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
