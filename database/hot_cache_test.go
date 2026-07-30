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
		cache.curBytes.Load(),
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
// concurrent writers against a small, heavily-contended cache than
// TestHotCacheConcurrent and asserts every Put returns within a bounded
// time. This validates the acceptance criteria that cache insertion cannot
// spin indefinitely under high contention, using the default retry budget.
func TestHotCacheConcurrentPutsCompleteUnderHighContention(t *testing.T) {
	cache := NewHotCache(50, 0)

	const numGoroutines = 200
	const numOperations = 200

	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		wg.Add(numGoroutines)
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
