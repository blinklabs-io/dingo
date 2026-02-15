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

package ledger

import (
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/peergov"
	"github.com/stretchr/testify/require"
)

// newTestDB creates an in-memory database for testing and registers
// a cleanup function to close it when the test finishes.
func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// newTestAdapter creates a LedgerPeerProviderAdapter with the given
// parameters. It uses a minimal LedgerState and the provided database.
// The cacheTTL is set to the given value.
func newTestAdapter(
	t *testing.T,
	db *database.Database,
	eventBus *event.EventBus,
	cacheTTL time.Duration,
) *LedgerPeerProviderAdapter {
	t.Helper()
	ls := &LedgerState{db: db}
	adapter, err := NewLedgerPeerProvider(ls, db, eventBus)
	require.NoError(t, err)
	adapter.cacheTTL = cacheTTL
	return adapter
}

// sampleRelays returns a slice of pool relays for use in tests.
func sampleRelays() []peergov.PoolRelay {
	ipv4 := net.ParseIP("192.168.1.1").To4()
	ipv6 := net.ParseIP("::1")
	return []peergov.PoolRelay{
		{
			Hostname: "relay1.example.com",
			Port:     3001,
			IPv4:     &ipv4,
		},
		{
			Hostname: "relay2.example.com",
			Port:     6000,
			IPv6:     &ipv6,
		},
	}
}

// seedCache injects relay data directly into the adapter's cache,
// simulating a previous successful fetch from the database.
func seedCache(
	adapter *LedgerPeerProviderAdapter,
	relays []peergov.PoolRelay,
) {
	adapter.cacheMu.Lock()
	adapter.cachedRelays = relays
	adapter.cacheTime = time.Now()
	adapter.cacheMu.Unlock()
}

func TestLedgerPeerProviderNewErrors(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	t.Run("nil ledgerState", func(t *testing.T) {
		_, err := NewLedgerPeerProvider(nil, db, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ledgerState")
	})

	t.Run("nil db", func(t *testing.T) {
		_, err := NewLedgerPeerProvider(ls, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "db")
	})
}

func TestLedgerPeerProviderCacheHit(t *testing.T) {
	db := newTestDB(t)
	// Use a long TTL so the cache never expires during the test
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// First call should return cached data
	result1, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result1, len(relays))
	require.Equal(t, relays[0].Hostname, result1[0].Hostname)
	require.Equal(t, relays[1].Hostname, result1[1].Hostname)

	// Second call should also return cached data (same values)
	result2, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result2, len(relays))
	require.Equal(t, relays[0].Hostname, result2[0].Hostname)
	require.Equal(t, relays[1].Hostname, result2[1].Hostname)

	// Verify the cache is still populated
	adapter.cacheMu.RLock()
	require.NotNil(t, adapter.cachedRelays)
	adapter.cacheMu.RUnlock()
}

func TestLedgerPeerProviderCacheTTLExpiry(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Millisecond)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Verify cache is populated initially
	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result, len(relays))

	// Wait for TTL to expire using polling
	require.Eventually(t, func() bool {
		adapter.cacheMu.RLock()
		expired := time.Since(adapter.cacheTime) >= adapter.cacheTTL
		adapter.cacheMu.RUnlock()
		return expired
	}, 2*time.Second, 5*time.Millisecond, "cache TTL should expire")

	// After TTL expires, GetPoolRelays should re-fetch from DB.
	// The in-memory DB has no pool registrations, so it returns empty.
	result, err = adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Empty(t, result, "expected empty relays after TTL expiry since DB has no data")
}

func TestLedgerPeerProviderInvalidateCache(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Verify cache is populated
	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result, len(relays))

	// Invalidate the cache
	adapter.InvalidateCache()

	// Verify cache fields are cleared
	adapter.cacheMu.RLock()
	require.Nil(t, adapter.cachedRelays)
	require.True(t, adapter.cacheTime.IsZero())
	adapter.cacheMu.RUnlock()

	// Next call should re-fetch from DB (returns empty since no data)
	result, err = adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Empty(
		t,
		result,
		"expected empty relays after invalidation since DB has no data",
	)
}

func TestLedgerPeerProviderEventDrivenInvalidation(t *testing.T) {
	db := newTestDB(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	adapter := newTestAdapter(t, db, bus, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Verify cache is populated
	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result, len(relays))

	// Publish a PoolStateRestoredEvent
	bus.Publish(
		PoolStateRestoredEventType,
		event.NewEvent(
			PoolStateRestoredEventType,
			PoolStateRestoredEvent{Slot: 42},
		),
	)

	// The event handler runs asynchronously via SubscribeFunc, so
	// poll until the cache is cleared.
	require.Eventually(t, func() bool {
		adapter.cacheMu.RLock()
		defer adapter.cacheMu.RUnlock()
		return adapter.cachedRelays == nil
	}, 2*time.Second, 5*time.Millisecond,
		"cache should be invalidated after PoolStateRestoredEvent",
	)

	// After invalidation, fetching returns empty (no DB data)
	result, err = adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestLedgerPeerProviderDeepCopy(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Get the first copy
	result1, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result1, 2)

	// Mutate the returned slice: change hostname, port, and IP
	result1[0].Hostname = "mutated.example.com"
	result1[0].Port = 9999
	if result1[0].IPv4 != nil {
		(*result1[0].IPv4)[0] = 255
	}

	// Append to the slice to verify slice header independence
	result1 = append(result1, peergov.PoolRelay{
		Hostname: "extra.example.com",
		Port:     1234,
	})

	// Get a second copy from the cache
	result2, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result2, 2, "cached slice length should not be affected by append")

	// Verify the cached data is unaffected by mutations
	require.Equal(
		t,
		"relay1.example.com",
		result2[0].Hostname,
		"hostname should not be mutated",
	)
	require.Equal(
		t,
		uint(3001),
		result2[0].Port,
		"port should not be mutated",
	)
	require.NotNil(t, result2[0].IPv4)
	require.Equal(
		t,
		net.ParseIP("192.168.1.1").To4(),
		*result2[0].IPv4,
		"IPv4 address should not be mutated",
	)

	// Verify the second relay is also intact
	require.Equal(t, "relay2.example.com", result2[1].Hostname)
	require.NotNil(t, result2[1].IPv6)
	require.Equal(t, net.ParseIP("::1"), *result2[1].IPv6)
}

func TestLedgerPeerProviderDeepCopyIPv6(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	ipv6 := net.ParseIP("2001:db8::1")
	relays := []peergov.PoolRelay{
		{
			Hostname: "relay.example.com",
			Port:     3001,
			IPv6:     &ipv6,
		},
	}
	seedCache(adapter, relays)

	// Get a copy and mutate the IPv6 address
	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.NotNil(t, result[0].IPv6)
	(*result[0].IPv6)[0] = 0xFF

	// Get another copy and verify it is unaffected
	result2, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Equal(
		t,
		net.ParseIP("2001:db8::1"),
		*result2[0].IPv6,
		"IPv6 should not be mutated",
	)
}

func TestLedgerPeerProviderNilEventBus(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	// Constructing with nil eventBus should not panic
	adapter, err := NewLedgerPeerProvider(ls, db, nil)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// Basic operations should still work
	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Empty(t, result)

	// Invalidate should not panic
	adapter.InvalidateCache()
}

func TestLedgerPeerProviderCacheMissFetchesFromDB(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	// With no seeded cache and empty DB, GetPoolRelays should return empty
	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Empty(t, result)

	// The cache should now be populated (with empty data)
	adapter.cacheMu.RLock()
	require.NotNil(t, adapter.cachedRelays)
	require.False(t, adapter.cacheTime.IsZero())
	adapter.cacheMu.RUnlock()
}

func TestLedgerPeerProviderCurrentSlot(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	adapter, err := NewLedgerPeerProvider(ls, db, nil)
	require.NoError(t, err)

	// Default tip is zero
	require.Equal(t, uint64(0), adapter.CurrentSlot())
}

func TestLedgerPeerProviderInvalidateCacheIdempotent(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	// Multiple invalidations should not panic
	adapter.InvalidateCache()
	adapter.InvalidateCache()
	adapter.InvalidateCache()

	// Cache should remain cleared
	adapter.cacheMu.RLock()
	require.Nil(t, adapter.cachedRelays)
	require.True(t, adapter.cacheTime.IsZero())
	adapter.cacheMu.RUnlock()
}

func TestLedgerPeerProviderConcurrentAccess(t *testing.T) {
	db := newTestDB(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	adapter := newTestAdapter(t, db, bus, 10*time.Millisecond)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Run concurrent reads, invalidations, and event publishes.
	// The race detector will catch any data races.
	done := make(chan struct{})
	const goroutines = 10
	const iterations = 50

	for range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			for range iterations {
				// Mix of reads and invalidations
				_, _ = adapter.GetPoolRelays()
				adapter.InvalidateCache()
				seedCache(adapter, relays)
			}
		}()
	}

	// Also publish events concurrently
	for range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			for range iterations {
				bus.Publish(
					PoolStateRestoredEventType,
					event.NewEvent(
						PoolStateRestoredEventType,
						PoolStateRestoredEvent{Slot: 1},
					),
				)
			}
		}()
	}

	// Wait for all goroutines to finish
	for range goroutines * 2 {
		<-done
	}
}

func TestLedgerPeerProviderCacheNilIPFields(t *testing.T) {
	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	// Relay with no IP addresses (hostname-only relay)
	relays := []peergov.PoolRelay{
		{
			Hostname: "hostname-only.example.com",
			Port:     3001,
			IPv4:     nil,
			IPv6:     nil,
		},
	}
	seedCache(adapter, relays)

	result, err := adapter.GetPoolRelays()
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "hostname-only.example.com", result[0].Hostname)
	require.Equal(t, uint(3001), result[0].Port)
	require.Nil(t, result[0].IPv4)
	require.Nil(t, result[0].IPv6)
}

func TestLedgerPeerProviderDefaultTTL(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}

	adapter, err := NewLedgerPeerProvider(ls, db, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		defaultRelayCacheTTL,
		adapter.cacheTTL,
		"default TTL should be set by constructor",
	)
}

func TestCopyPoolRelaysEmpty(t *testing.T) {
	result := copyPoolRelays(nil)
	require.Empty(t, result)

	result = copyPoolRelays([]peergov.PoolRelay{})
	require.Empty(t, result)
	require.NotNil(t, result)
}

func TestCopyPoolRelaysFull(t *testing.T) {
	ipv4 := net.ParseIP("10.0.0.1").To4()
	ipv6 := net.ParseIP("fe80::1")
	original := []peergov.PoolRelay{
		{
			Hostname: "test.example.com",
			Port:     3001,
			IPv4:     &ipv4,
			IPv6:     &ipv6,
		},
	}

	result := copyPoolRelays(original)
	require.Len(t, result, 1)
	require.Equal(t, original[0].Hostname, result[0].Hostname)
	require.Equal(t, original[0].Port, result[0].Port)

	// Verify deep copy: different pointers, same values
	require.NotSame(t, original[0].IPv4, result[0].IPv4)
	require.NotSame(t, original[0].IPv6, result[0].IPv6)
	require.Equal(t, *original[0].IPv4, *result[0].IPv4)
	require.Equal(t, *original[0].IPv6, *result[0].IPv6)

	// Mutate the copy and verify original is unaffected
	(*result[0].IPv4)[0] = 0xFF
	require.Equal(t, byte(10), (*original[0].IPv4)[0])
}
