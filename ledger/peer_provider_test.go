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
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newTestDB creates an in-memory database for testing and registers
// a cleanup function to close it when the test finishes.
func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	return db
}

// newTestAdapter creates a PoolRelayProvider with the given
// parameters. It uses a minimal LedgerState and the provided database.
// The cacheTTL is set to the given value.
func newTestAdapter(
	t *testing.T,
	db *database.Database,
	eventBus *event.EventBus,
	cacheTTL time.Duration,
) *PoolRelayProvider {
	t.Helper()
	ls := &LedgerState{db: db}
	adapter, err := NewPoolRelayProvider(ls, db, eventBus)
	require.NoError(t, err)
	adapter.cacheTTL = cacheTTL
	return adapter
}

// sampleRelays returns a slice of pool relays for use in tests.
func sampleRelays() []PoolRelay {
	ipv4 := net.ParseIP("192.168.1.1").To4()
	ipv6 := net.ParseIP("::1")
	return []PoolRelay{
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
	adapter *PoolRelayProvider,
	relays []PoolRelay,
) {
	adapter.cacheMu.Lock()
	adapter.cachedRelays = relays
	adapter.cacheTime = time.Now()
	adapter.cacheMu.Unlock()
}

func TestPoolRelayProviderNewErrors(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	t.Run("nil ledgerState", func(t *testing.T) {
		_, err := NewPoolRelayProvider(nil, db, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ledgerState")
	})

	t.Run("nil db", func(t *testing.T) {
		_, err := NewPoolRelayProvider(ls, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "db")
	})
}

func TestPoolRelayProviderCacheHit(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	// Use a long TTL so the cache never expires during the test
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// First call should return cached data
	result1, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(t, result1, len(relays))
	require.Equal(t, relays[0].Hostname, result1[0].Hostname)
	require.Equal(t, relays[1].Hostname, result1[1].Hostname)

	// Second call should also return cached data (same values)
	result2, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(t, result2, len(relays))
	require.Equal(t, relays[0].Hostname, result2[0].Hostname)
	require.Equal(t, relays[1].Hostname, result2[1].Hostname)

	// Verify the cache is still populated
	adapter.cacheMu.RLock()
	require.NotNil(t, adapter.cachedRelays)
	adapter.cacheMu.RUnlock()
}

func TestPoolRelayProviderCacheTTLExpiry(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Millisecond)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Verify cache is populated initially
	result, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(t, result, len(relays))

	// Wait for TTL to expire using polling
	require.Eventually(t, func() bool {
		adapter.cacheMu.RLock()
		expired := time.Since(adapter.cacheTime) >= adapter.cacheTTL
		adapter.cacheMu.RUnlock()
		return expired
	}, testutil.AsyncWait, 5*time.Millisecond, "cache TTL should expire")

	// After TTL expires, GetPoolRelays should re-fetch from DB.
	// The in-memory DB has no pool registrations, so it returns empty.
	result, err = adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Empty(
		t,
		result,
		"expected empty relays after TTL expiry since DB has no data",
	)
}

func TestPoolRelayProviderInvalidateCache(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Verify cache is populated
	result, err := adapter.GetPoolRelays(context.Background())
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
	result, err = adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Empty(
		t,
		result,
		"expected empty relays after invalidation since DB has no data",
	)
}

func TestPoolRelayProviderEventDrivenInvalidation(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	adapter := newTestAdapter(t, db, bus, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Verify cache is populated
	result, err := adapter.GetPoolRelays(context.Background())
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
	}, testutil.AsyncWait, 5*time.Millisecond,
		"cache should be invalidated after PoolStateRestoredEvent",
	)

	// After invalidation, fetching returns empty (no DB data)
	result, err = adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)
}

func TestPoolRelayProviderDeepCopy(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	relays := sampleRelays()
	seedCache(adapter, relays)

	// Get the first copy
	result1, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(t, result1, 2)

	// Mutate the returned slice: change hostname, port, and IP
	result1[0].Hostname = "mutated.example.com"
	result1[0].Port = 9999
	if result1[0].IPv4 != nil {
		(*result1[0].IPv4)[0] = 255
	}

	// Append to the slice to verify slice header independence
	result1 = append(result1, PoolRelay{
		Hostname: "extra.example.com",
		Port:     1234,
	})

	// Get a second copy from the cache
	result2, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(
		t,
		result2,
		2,
		"cached slice length should not be affected by append",
	)

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

func TestPoolRelayProviderDeepCopyIPv6(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	ipv6 := net.ParseIP("2001:db8::1")
	relays := []PoolRelay{
		{
			Hostname: "relay.example.com",
			Port:     3001,
			IPv6:     &ipv6,
		},
	}
	seedCache(adapter, relays)

	// Get a copy and mutate the IPv6 address
	result, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.NotNil(t, result[0].IPv6)
	(*result[0].IPv6)[0] = 0xFF

	// Get another copy and verify it is unaffected
	result2, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Equal(
		t,
		net.ParseIP("2001:db8::1"),
		*result2[0].IPv6,
		"IPv6 should not be mutated",
	)
}

func TestPoolRelayProviderNilEventBus(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	// Constructing with nil eventBus should not panic
	adapter, err := NewPoolRelayProvider(ls, db, nil)
	require.NoError(t, err)
	require.NotNil(t, adapter)

	// Basic operations should still work
	result, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)

	// Invalidate should not panic
	adapter.InvalidateCache()
}

func TestPoolRelayProviderCacheMissFetchesFromDB(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	// With no seeded cache and empty DB, GetPoolRelays should return empty
	result, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Empty(t, result)

	// The cache should now be populated (with empty data)
	adapter.cacheMu.RLock()
	require.NotNil(t, adapter.cachedRelays)
	require.False(t, adapter.cacheTime.IsZero())
	adapter.cacheMu.RUnlock()
}

func TestPoolRelayProviderCurrentSlot(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	adapter, err := NewPoolRelayProvider(ls, db, nil)
	require.NoError(t, err)

	// Default tip is zero
	require.Equal(t, uint64(0), adapter.CurrentSlot())
}

func TestPoolRelayProviderInvalidateCacheIdempotent(t *testing.T) {
	t.Parallel()

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

func TestPoolRelayProviderConcurrentAccess(t *testing.T) {
	t.Parallel()

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
				_, _ = adapter.GetPoolRelays(context.Background())
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

func TestPoolRelayProviderCacheNilIPFields(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	adapter := newTestAdapter(t, db, nil, 10*time.Minute)

	// Relay with no IP addresses (hostname-only relay)
	relays := []PoolRelay{
		{
			Hostname: "hostname-only.example.com",
			Port:     3001,
			IPv4:     nil,
			IPv6:     nil,
		},
	}
	seedCache(adapter, relays)

	result, err := adapter.GetPoolRelays(context.Background())
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "hostname-only.example.com", result[0].Hostname)
	require.Equal(t, uint(3001), result[0].Port)
	require.Nil(t, result[0].IPv4)
	require.Nil(t, result[0].IPv6)
}

func TestPoolRelayProviderDefaultTTL(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := &LedgerState{db: db}

	adapter, err := NewPoolRelayProvider(ls, db, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		defaultRelayCacheTTL,
		adapter.cacheTTL,
		"default TTL should be set by constructor",
	)
}

func TestCopyPoolRelaysEmpty(t *testing.T) {
	t.Parallel()

	result := copyPoolRelays(nil)
	require.Empty(t, result)

	result = copyPoolRelays([]PoolRelay{})
	require.Empty(t, result)
	require.NotNil(t, result)
}

func TestCopyPoolRelaysFull(t *testing.T) {
	t.Parallel()

	ipv4 := net.ParseIP("10.0.0.1").To4()
	ipv6 := net.ParseIP("fe80::1")
	original := []PoolRelay{
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

// TestPoolRelayProviderCloseUnsubscribes pins that Close removes the
// cache-invalidation handler NewPoolRelayProvider registers. Without this, a
// live database restore/truncate -- which constructs a fresh
// PoolRelayProvider on every cycle (node_lifecycle.go) but previously had no
// way to unsubscribe the old one -- leaks one more permanently-active
// EventBus subscription per cycle.
func TestPoolRelayProviderCloseUnsubscribes(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	adapter := newTestAdapter(t, db, bus, defaultRelayCacheTTL)
	require.True(t, bus.HasSubscribers(PoolStateRestoredEventType))

	adapter.Close()
	require.False(t, bus.HasSubscribers(PoolStateRestoredEventType))

	// Safe to call more than once.
	require.NotPanics(t, adapter.Close)
}

// seedStakedPools registers two active pools: the first has a single-host
// relay and a MultiHostName relay (hostname, no port) and 700 lovelace
// delegated; the second has one relay and no delegation.
func seedStakedPools(
	t *testing.T,
	db *database.Database,
) (poolA, poolB []byte) {
	t.Helper()
	poolA = bytes.Repeat([]byte{0xa1}, 28)
	poolB = bytes.Repeat([]byte{0xb2}, 28)
	vrfA := bytes.Repeat([]byte{0xa3}, 32)
	vrfB := bytes.Repeat([]byte{0xb3}, 32)
	reward := bytes.Repeat([]byte{0xc4}, 28)
	ipA := net.ParseIP("44.0.0.1").To4()
	ipB := net.ParseIP("44.0.0.2").To4()
	for _, p := range []struct {
		key, vrf []byte
		relays   []models.PoolRegistrationRelay
	}{
		{poolA, vrfA, []models.PoolRegistrationRelay{
			{Ipv4: &ipA, Port: 3001},
			{Hostname: "multi.example.com"},
		}},
		{poolB, vrfB, []models.PoolRegistrationRelay{
			{Ipv4: &ipB, Port: 3002},
		}},
	} {
		require.NoError(t, db.ImportPool(
			t.Context(),
			nil,
			&models.Pool{
				PoolKeyHash: p.key, VrfKeyHash: p.vrf, RewardAccount: reward,
			},
			&models.PoolRegistration{
				PoolKeyHash: p.key, VrfKeyHash: p.vrf, RewardAccount: reward,
				AddedSlot: 10, Relays: p.relays,
			},
		))
	}
	require.NoError(t, db.SetEpoch(
		2, 0, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 25, Hash: []byte("tip")},
		BlockNumber: 1,
	}, nil))
	stakeKey := bytes.Repeat([]byte{0xd5}, 28)
	require.NoError(t, db.CreateAccount(t.Context(), nil, &models.Account{
		StakingKey: stakeKey, Pool: poolA, Active: true,
	}))
	require.NoError(t, db.CreateUtxo(t.Context(), nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0xe6}, 32),
		StakingKey: stakeKey, Amount: 700, AddedSlot: 15,
	}))
	return poolA, poolB
}

func relayByPort(t *testing.T, relays []PoolRelay, port uint) PoolRelay {
	t.Helper()
	for _, r := range relays {
		if r.Port == port && r.Hostname == "" {
			return r
		}
	}
	t.Fatalf("no relay with port %d in %v", port, relays)
	return PoolRelay{}
}

func TestPoolRelayProviderPopulatesStake(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	poolA, poolB := seedStakedPools(t, db)
	adapter := newTestAdapter(t, db, nil, time.Minute)

	relays, err := adapter.GetPoolRelays(t.Context())
	require.NoError(t, err)
	require.Len(t, relays, 3)

	require.Equal(t, uint64(700), relayByPort(t, relays, 3001).Stake)
	require.Equal(t, poolA, relayByPort(t, relays, 3001).PoolKeyHash)
	require.Zero(t, relayByPort(t, relays, 3002).Stake)
	require.Equal(t, poolB, relayByPort(t, relays, 3002).PoolKeyHash)
	foundMulti := false
	for _, r := range relays {
		if r.Hostname == "multi.example.com" {
			foundMulti = true
			// Both relays of the staked pool carry its stake.
			require.Equal(t, uint64(700), r.Stake)
			require.True(t, r.IsMultiHost)
			require.Zero(t, r.Port)
			continue
		}
		require.False(t, r.IsMultiHost)
	}
	require.True(t, foundMulti, "registered MultiHostName relay is missing")
	require.True(
		t,
		relayByPort(t, relays, 3002).StakeKnown,
		"successful zero-stake lookup must remain known",
	)
}

func TestPoolRelayProviderStakeLookupFailureIsNonFatal(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	seedStakedPools(t, db)
	adapter := newTestAdapter(t, db, nil, time.Minute)
	adapter.stakeByPools = func(context.Context, [][]byte) (map[string]uint64, error) {
		return nil, errors.New("stake store unavailable")
	}

	relays, err := adapter.GetPoolRelays(t.Context())
	require.NoError(t, err)
	require.Len(t, relays, 3)
	for _, r := range relays {
		require.Zero(t, r.Stake)
	}
}

func TestPoolRelayProviderStakeLookupGetsUniquePoolHashes(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	poolA, poolB := seedStakedPools(t, db)
	adapter := newTestAdapter(t, db, nil, time.Minute)
	var got [][]byte
	adapter.stakeByPools = func(_ context.Context, h [][]byte) (map[string]uint64, error) {
		got = h
		return nil, nil
	}
	_, err := adapter.GetPoolRelays(t.Context())
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{poolA, poolB}, got)
}

func TestCopyPoolRelaysCopiesStakeAndMultiHost(t *testing.T) {
	t.Parallel()
	original := []PoolRelay{
		{
			Hostname:    "multi.example.com",
			PoolKeyHash: []byte{0xaa},
			Stake:       99,
			IsMultiHost: true,
		},
	}
	result := copyPoolRelays(original)
	require.Equal(t, original, result)
	result[0].PoolKeyHash[0] = 0xbb
	require.Equal(t, byte(0xaa), original[0].PoolKeyHash[0])
}
