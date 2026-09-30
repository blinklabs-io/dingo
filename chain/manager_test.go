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

package chain

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestRewindPrimaryChainToPointDoesNotDeadlockWithIterator(t *testing.T) {
	t.Parallel()

	cm, err := NewManager(nil, nil)
	if err != nil {
		t.Fatalf("NewManager: %s", err)
	}
	primaryChain := cm.PrimaryChain()
	primaryChain.persistent = true
	cm.securityParam = 10

	cm.mutex.Lock()
	iter := &ChainIterator{
		chain:          primaryChain,
		nextBlockIndex: initialBlockIndex,
		ctx:            context.Background(),
	}
	iterDone := make(chan error, 1)
	go func() {
		_, err := primaryChain.iterNext(iter, false)
		iterDone <- err
	}()
	testutil.WaitForCondition(t, func() bool {
		if primaryChain.mutex.TryLock() {
			primaryChain.mutex.Unlock()
			return false
		}
		return true
	}, time.Second, "iterator should hold primary chain lock")

	rewindStarted := make(chan struct{})
	rewindDone := make(chan error, 1)
	go func() {
		close(rewindStarted)
		rewindDone <- cm.RewindPrimaryChainToPoint(ocommon.NewPointOrigin())
	}()
	testutil.RequireReceive(
		t,
		rewindStarted,
		time.Second,
		"rewind goroutine should start",
	)
	runtime.Gosched()
	cm.mutex.Unlock()

	err = testutil.RequireReceive(
		t,
		iterDone,
		time.Second,
		"iterator should complete after manager lock is released",
	)
	if !errors.Is(err, ErrIteratorChainTip) {
		t.Fatalf("iterator error = %v, want %v", err, ErrIteratorChainTip)
	}
	err = testutil.RequireReceive(
		t,
		rewindDone,
		time.Second,
		"rewind should complete after iterator releases chain lock",
	)
	if err != nil {
		t.Fatalf("rewind: %s", err)
	}
}

func TestNewManagerReusesRegisteredMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	first, err := NewManager(nil, nil, registry)
	require.NoError(t, err)
	second, err := NewManager(nil, nil, registry)
	require.NoError(t, err)

	require.Same(t, first.rollbackPointNotOnChain, second.rollbackPointNotOnChain)
	require.Same(t, first.blockCache.cachedBlocks, second.blockCache.cachedBlocks)

	first.recordRollbackPointNotOnChain()
	second.blockCache.cachedBlocks.Set(2)
	metrics, err := registry.Gather()
	require.NoError(t, err)
	values := make(map[string]float64, len(metrics))
	for _, metric := range metrics {
		if len(metric.GetMetric()) == 1 {
			if counter := metric.GetMetric()[0].GetCounter(); counter != nil {
				values[metric.GetName()] = counter.GetValue()
			}
			if gauge := metric.GetMetric()[0].GetGauge(); gauge != nil {
				values[metric.GetName()] = gauge.GetValue()
			}
		}
	}
	require.Equal(t, float64(1), values[rollbackPointNotOnChainMetricName])
	require.Equal(t, float64(2), values[blockCacheMetricName])
}

func TestNewManagerRejectsIncompatibleRegisteredMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		register func(prometheus.Registerer) error
	}{
		{
			name: "block cache wrong collector type",
			register: func(registry prometheus.Registerer) error {
				return registry.Register(prometheus.NewCounter(prometheus.CounterOpts{
					Name: blockCacheMetricName,
					Help: "current number of cached blocks in the chain manager LRU cache",
				}))
			},
		},
		{
			name: "block cache mismatched descriptor",
			register: func(registry prometheus.Registerer) error {
				return registry.Register(prometheus.NewGauge(prometheus.GaugeOpts{
					Name: blockCacheMetricName,
					Help: "incompatible cached block metric",
				}))
			},
		},
		{
			name: "rollback point wrong collector type",
			register: func(registry prometheus.Registerer) error {
				return registry.Register(prometheus.NewGauge(prometheus.GaugeOpts{
					Name: rollbackPointNotOnChainMetricName,
					Help: "rollback targets rejected because the chain no longer holds the resolved block at its retained index",
				}))
			},
		},
		{
			name: "rollback point mismatched descriptor",
			register: func(registry prometheus.Registerer) error {
				return registry.Register(prometheus.NewCounter(prometheus.CounterOpts{
					Name: rollbackPointNotOnChainMetricName,
					Help: "incompatible rollback point metric",
				}))
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			registry := prometheus.NewRegistry()
			require.NoError(t, test.register(registry))
			manager, err := NewManager(nil, nil, registry)
			require.Error(t, err)
			require.Nil(t, manager)
		})
	}
}
