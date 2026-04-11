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

package dingo

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageModeValid(t *testing.T) {
	tests := []struct {
		mode  StorageMode
		valid bool
	}{
		{StorageModeCore, true},
		{StorageModeAPI, true},
		{"", false},
		{"invalid", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.valid, tt.mode.Valid(), "mode=%q", tt.mode)
	}
}

func TestStorageModeIsAPI(t *testing.T) {
	assert.False(t, StorageModeCore.IsAPI())
	assert.True(t, StorageModeAPI.IsAPI())
}

func TestWithStorageMode(t *testing.T) {
	cfg := &Config{}

	// Default should be zero value (empty string)
	assert.Equal(t, StorageMode(""), cfg.storageMode)

	// Apply API mode
	WithStorageMode(StorageModeAPI)(cfg)
	assert.Equal(t, StorageModeAPI, cfg.storageMode)

	// Apply core mode
	WithStorageMode(StorageModeCore)(cfg)
	assert.Equal(t, StorageModeCore, cfg.storageMode)
}

func TestPeerGovernorOptionsIgnoreNonPositiveValues(t *testing.T) {
	cfg := &Config{}

	WithMinHotPeers(-1)(cfg)
	WithReconcileInterval(-1 * time.Minute)(cfg)
	WithInactivityTimeout(-5 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(-2)(cfg)
	WithMaxInboundConns(0)(cfg)

	assert.Zero(t, cfg.minHotPeers)
	assert.Zero(t, cfg.reconcileInterval)
	assert.Zero(t, cfg.inactivityTimeout)
	assert.Zero(t, cfg.maxConnectionsPerIP)
	assert.Zero(t, cfg.maxInboundConns)
}

func TestPeerGovernorOptionsApplyPositiveValues(t *testing.T) {
	cfg := &Config{}

	WithMinHotPeers(3)(cfg)
	WithReconcileInterval(30 * time.Second)(cfg)
	WithInactivityTimeout(2 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(4)(cfg)
	WithMaxInboundConns(25)(cfg)

	assert.Equal(t, 3, cfg.minHotPeers)
	assert.Equal(t, 30*time.Second, cfg.reconcileInterval)
	assert.Equal(t, 2*time.Minute, cfg.inactivityTimeout)
	assert.Equal(t, 4, cfg.maxConnectionsPerIP)
	assert.Equal(t, 25, cfg.maxInboundConns)
}

// TestUpdateRTSMetrics verifies the pure-function mapping from
// runtime.MemStats fields to the four cardano_node_metrics_RTS_* gauges.
// Specifically exercises the NumGC - NumForcedGC subtraction so a future
// typo that inverts the operands is caught immediately.
func TestUpdateRTSMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	m := &rtsMetrics{
		gcLiveBytes: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_live"},
		),
		gcHeapBytes: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_heap"},
		),
		gcMajorNum: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_major"},
		),
		gcMinorNum: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_minor"},
		),
	}
	stats := &runtime.MemStats{
		HeapAlloc:   1024,
		HeapSys:     4096,
		Sys:         8192,
		NumGC:       10,
		NumForcedGC: 3,
	}

	updateRTSMetrics(m, stats)

	require.Equal(t, float64(1024), promtestutil.ToFloat64(m.gcLiveBytes))
	require.Equal(t, float64(4096), promtestutil.ToFloat64(m.gcHeapBytes))
	require.Equal(t, float64(3), promtestutil.ToFloat64(m.gcMajorNum))
	// 10 total - 3 forced = 7 automatic
	require.Equal(t, float64(7), promtestutil.ToFloat64(m.gcMinorNum))
}

// TestRunRTSMetricsUpdater_Lifecycle verifies the background updater
// populates the gauges after its initial prime and exits cleanly when
// the context is cancelled.
func TestRunRTSMetricsUpdater_Lifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{promRegistry: reg}}
	n.registerRTSMetrics()
	require.NotNil(t, n.rtsMetrics, "registerRTSMetrics must populate n.rtsMetrics")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		n.runRTSMetricsUpdater(ctx, 5*time.Millisecond)
		close(done)
	}()

	// Wait for the initial prime (or first tick) to populate real values.
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(n.rtsMetrics.gcHeapBytes) > 0
	}, 2*time.Second, 10*time.Millisecond, "gcHeapBytes should be populated by the updater")

	cancel()
	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"updater should exit after ctx cancel",
	)
}
