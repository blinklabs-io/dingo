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

package koiosparity

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewObserverWiresPromRegistryIntoMetrics proves NewObserver actually
// constructs and stores a live *metrics from ObserverConfig.PromRegistry --
// not just that newMetrics itself works (metrics_test.go covers that in
// isolation) -- and that emitResult, the single choke point every OnResult
// call goes through (processEpoch's and processAccountEpoch's success paths,
// and reportError's synthesized ERROR path), records into it.
func TestNewObserverWiresPromRegistryIntoMetrics(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	reg := prometheus.NewRegistry()
	o, err := NewObserver(ObserverConfig{
		Network:      "preview",
		CachePath:    t.TempDir() + "/cache.db",
		Source:       source,
		PromRegistry: reg,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(t.Context()) })
	require.NotNil(
		t,
		o.metrics,
		"NewObserver must construct a *metrics from a non-nil PromRegistry",
	)

	o.emitResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   42,
		Status:  StatusFail,
		Mismatches: []CheckMismatch{
			{Category: CategoryValueMismatch},
		},
	})

	assert.Equal(
		t,
		1.0,
		promtestutil.ToFloat64(
			o.metrics.epochResultTotal.WithLabelValues(
				ScopeAggregate,
				"fail",
			),
		),
		"emitResult must record into the Observer's own metrics",
	)
	assert.Equal(
		t,
		42.0,
		promtestutil.ToFloat64(
			o.metrics.lastFailEpoch.WithLabelValues(ScopeAggregate),
		),
	)
}

// TestNewObserverNilPromRegistryLeavesMetricsNil confirms an ObserverConfig
// with no PromRegistry set (the common case: most existing tests in this
// package, and the standalone koios-parity CLI) leaves o.metrics nil rather
// than registering anything, and that emitResult still works.
func TestNewObserverNilPromRegistryLeavesMetricsNil(t *testing.T) {
	t.Parallel()

	db := newTestDatabaseSourceDB(t)
	source, err := NewDatabaseSource(db)
	require.NoError(t, err)

	o, err := NewObserver(ObserverConfig{
		Network:   "preview",
		CachePath: t.TempDir() + "/cache.db",
		Source:    source,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = o.Stop(t.Context()) })
	require.Nil(t, o.metrics)

	// Must not panic.
	o.emitResult(&EpochCompareResult{
		Network: "preview",
		Epoch:   1,
		Status:  StatusPass,
	})
}
