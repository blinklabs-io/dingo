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

package mempool

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// dynamicCapacityPool builds a pool whose configured capacity is large and
// whose capacity function reports capacity, which a test then moves.
func dynamicCapacityPool(
	t *testing.T,
	capacity *atomic.Int64,
	cleanupInterval time.Duration,
) (*Mempool, *prometheus.Registry) {
	t.Helper()
	registry := prometheus.NewRegistry()
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 1 << 30,
		CapacityFunc:    capacity.Load,
		CleanupInterval: cleanupInterval,
		PromRegistry:    registry,
	})
	require.NoError(t, err)
	require.NoError(t, pool.Start(context.Background()))
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })
	return pool.Mempool, registry
}

func TestMempoolCapacityFuncBoundsAdmission(t *testing.T) {
	t.Parallel()
	first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1)
	second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 1)
	txType := uint(conway.EraIdConway)
	var capacity atomic.Int64
	capacity.Store(retainedSize(t, first))
	pool, _ := dynamicCapacityPool(t, &capacity, 0)

	require.NoError(t, pool.AddTransaction(context.Background(), txType, first))
	var full *MempoolFullError
	require.ErrorAs(
		t,
		pool.AddTransaction(context.Background(), txType, second),
		&full,
		"the capacity in force, not the configured one, refuses the transaction",
	)
	require.Equal(t, capacity.Load(), full.Capacity)

	capacity.Store(2 * retainedSize(t, first))
	require.NoError(
		t,
		pool.AddTransaction(context.Background(), txType, second),
		"a capacity that grew admits what it refused",
	)
	require.Len(t, pool.Transactions(), 2)
}

func TestMempoolCapacityBytesReportsCapacityInForce(t *testing.T) {
	t.Parallel()
	var capacity atomic.Int64
	capacity.Store(4096)
	pool, registry := dynamicCapacityPool(t, &capacity, 0)

	require.Equal(t, int64(4096), pool.CapacityBytes())
	require.Equal(t, int64(4096), pool.MaxAdmissionHeadroomBytes())
	capacity.Store(1024)
	require.Equal(t, int64(1024), pool.CapacityBytes())
	require.Equal(t, int64(1024), pool.MaxAdmissionHeadroomBytes())
	require.Equal(t, int64(1024), pool.AdmissionHeadroomBytes())

	families, err := registry.Gather()
	require.NoError(t, err)
	var gauge float64 = -1
	for _, family := range families {
		if family.GetName() == "dingo_mempool_capacity_bytes" {
			gauge = family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	require.Equal(t, float64(1024), gauge, "the gauge follows the capacity")
}

func TestMempoolStartSettlesCapacityBeforeAnyTraffic(t *testing.T) {
	t.Parallel()
	var capacity atomic.Int64
	capacity.Store(4096)
	_, registry := dynamicCapacityPool(t, &capacity, 0)

	families, err := registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == "dingo_mempool_capacity_bytes" {
			require.Equal(
				t,
				float64(4096),
				family.GetMetric()[0].GetGauge().GetValue(),
			)
			return
		}
	}
	t.Fatal("capacity gauge is not registered")
}

func TestMempoolCapacityFuncKeepsLastCapacityWithoutAnAnswer(t *testing.T) {
	t.Parallel()
	var capacity atomic.Int64
	capacity.Store(4096)
	pool, _ := dynamicCapacityPool(t, &capacity, 0)
	require.Equal(t, int64(4096), pool.CapacityBytes())

	capacity.Store(0)
	require.Equal(t, int64(4096), pool.CapacityBytes())
	capacity.Store(-1)
	require.Equal(t, int64(4096), pool.CapacityBytes())
}

func TestMempoolWithoutCapacityFuncUsesConfiguredCapacity(t *testing.T) {
	t.Parallel()
	pool, err := NewFIFO(MempoolConfig{
		Validator:       newMockValidator(),
		MempoolCapacity: 2048,
		PromRegistry:    prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Stop(context.Background()) })

	require.Equal(t, int64(2048), pool.CapacityBytes())
	require.Equal(t, int64(2048), pool.MaxAdmissionHeadroomBytes())
}

// A capacity that shrinks below what the pool holds neither evicts nor
// corrupts the accounting: the pool refuses what does not fit and drains as
// blocks confirm its transactions.
func TestMempoolCapacityShrinkKeepsHeldTransactions(t *testing.T) {
	t.Parallel()
	first := withdrawalTxCbor(t, 0x01, withdrawalStakeKey, 1)
	second := withdrawalTxCbor(t, 0x02, withdrawalStakeKey, 1)
	third := withdrawalTxCbor(t, 0x03, withdrawalStakeKey, 1)
	txType := uint(conway.EraIdConway)
	one := retainedSize(t, first)
	var capacity atomic.Int64
	capacity.Store(2 * one)
	pool, _ := dynamicCapacityPool(t, &capacity, 0)
	require.NoError(t, pool.AddTransaction(context.Background(), txType, first))
	require.NoError(t, pool.AddTransaction(context.Background(), txType, second))

	capacity.Store(one)
	var full *MempoolFullError
	require.ErrorAs(
		t,
		pool.AddTransaction(context.Background(), txType, third),
		&full,
	)
	require.Len(t, pool.Transactions(), 2)
	require.Equal(t, 2*one, pool.SizeBytes())
	require.Zero(t, pool.AdmissionHeadroomBytes(), "over capacity has no headroom")
}

// Waiters on admission headroom are released when the capacity grows, which
// no removal announces.
func TestMempoolCapacityGrowthReleasesHeadroomWaiters(t *testing.T) {
	t.Parallel()
	var capacity atomic.Int64
	capacity.Store(1000)
	pool, _ := dynamicCapacityPool(t, &capacity, 5*time.Millisecond)
	addMockTransactionsOfSize(t, pool, 10, 100)

	result := make(chan bool, 1)
	go func() {
		result <- pool.waitForAdmissionHeadroom(500, make(chan struct{}))
	}()
	require.Never(
		t,
		func() bool { return len(result) > 0 },
		100*time.Millisecond,
		5*time.Millisecond,
		"a full pool leaves the waiter blocked",
	)

	capacity.Store(2000)
	select {
	case ok := <-result:
		require.True(t, ok)
	case <-time.After(10 * time.Second):
		t.Fatal("waiter was not released by the capacity growth")
	}
}

// The capacity function a node supplies as a provider dependency reaches the
// pool the provider builds, whichever backend is selected.
func TestMempoolProviderWiresCapacityFunc(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"default", "fifo", "dag"} {
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			host := plugin.NewHost()
			require.NoError(t, RegisterProvider(host))
			require.NoError(t, RegisterFIFOProvider(host))
			require.NoError(t, RegisterDAGProvider(host))

			service, err := plugin.Resolve[Service](
				context.Background(),
				host,
				plugin.CapabilityMempool,
				provider,
				map[string]any{"capacity": 1 << 20},
				ProviderDependencies{
					PromRegistry: prometheus.NewRegistry(),
					Validator:    newMockValidator(),
					CapacityFunc: func() int64 { return 4096 },
				},
			)
			require.NoError(t, err)
			require.Equal(t, int64(4096), service.CapacityBytes())
			require.NoError(t, host.Stop(context.Background()))
		})
	}
}
