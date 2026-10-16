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

package badger

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegisterBlobMetricsReusesSharedGCCollectors(t *testing.T) {
	registry := prometheus.NewRegistry()
	first := &BlobStoreBadger{promRegistry: registry}
	second := &BlobStoreBadger{promRegistry: registry}

	first.registerBlobMetrics()
	second.registerBlobMetrics()

	first.gcMetrics.attempts.Inc()
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(first.gcMetrics.attempts),
	)
	second.gcMetrics.attempts.Inc()
	require.Equal(t, float64(1), testutil.ToFloat64(second.gcMetrics.attempts))
}

func TestRegisterBlobMetricsAllowsLabelWrappedReuse(t *testing.T) {
	registry := prometheus.NewRegistry()
	firstRegisterer := prometheus.WrapRegistererWith(
		prometheus.Labels{"network": "preview"},
		registry,
	)
	first := &BlobStoreBadger{promRegistry: firstRegisterer}
	second := &BlobStoreBadger{promRegistry: firstRegisterer}

	first.registerBlobMetrics()
	require.NotPanics(t, second.registerBlobMetrics)
	second.gcMetrics.attempts.Inc()
	count, err := testutil.GatherAndCount(registry,
		"database_blob_gc_attempts_total")
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, float64(1), testutil.ToFloat64(second.gcMetrics.attempts))
}

func TestRegisterBlobMetricsRemovesClosedStoreSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	store := &BlobStoreBadger{promRegistry: registry}
	store.registerBlobMetrics()
	store.gcMetrics.attempts.Inc()
	count, err := testutil.GatherAndCount(registry,
		"database_blob_gc_attempts_total")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, store.Close())
	count, err = testutil.GatherAndCount(registry,
		"database_blob_gc_attempts_total")
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestValueLogGCMetricsReportReclaimedBytes(t *testing.T) {
	registry := prometheus.NewRegistry()
	store, err := New(
		WithDataDir(t.TempDir()),
		WithGc(false),
		WithPromRegistry(registry),
		WithValueThreshold(1),
		WithValueLogFileSize(1<<20),
		WithMemTableSize(1<<20),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	// The injected GC removes this file so the test covers byte accounting
	// without relying on Badger's rewrite selection.
	const reclaimedFileSize = 4 << 10
	reclaimedFile := filepath.Join(store.dataDir, "blob", "metrics-reclaimed.vlog")
	require.NoError(t, os.WriteFile(reclaimedFile, make([]byte, reclaimedFileSize), 0o600))
	var reclaimedFileRemoved bool
	store.runValueLogGC = func(float64) error {
		if reclaimedFileRemoved {
			return badgerdb.ErrNoRewrite
		}
		reclaimedFileRemoved = true
		return os.Remove(reclaimedFile)
	}

	ticks := make(chan time.Time, 1)
	stop := make(chan struct{})
	var stopOnce sync.Once
	// Cleanup runs before the store cleanup above, which waits for gcWg.
	t.Cleanup(func() { stopOnce.Do(func() { close(stop) }) })
	store.gcWg.Add(1)
	go store.blobGc(ticks, stop)
	var completedCycles float64
	gcDeadline := time.Now().Add(time.Minute)
	waitForNoRewrite := func(after float64) {
		remaining := time.Until(gcDeadline)
		if remaining <= 0 {
			t.Fatal("Badger GC did not finish within one minute")
		}
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(store.gcMetrics.noRewrite) > after
		}, remaining, 10*time.Millisecond)
	}
	ticks <- time.Time{}
	waitForNoRewrite(completedCycles)
	require.Equal(t, float64(1), testutil.ToFloat64(store.gcMetrics.successes))
	require.Positive(t, testutil.ToFloat64(store.gcMetrics.consecutive))
	require.Equal(
		t,
		float64(reclaimedFileSize),
		testutil.ToFloat64(store.gcMetrics.reclaimedBytes),
	)
	completedCycles = testutil.ToFloat64(store.gcMetrics.noRewrite)
	ticks <- time.Time{}
	waitForNoRewrite(completedCycles)
	stopOnce.Do(func() { close(stop) })
	store.gcWg.Wait()
	require.Zero(t, testutil.ToFloat64(store.gcMetrics.consecutive))
	require.Zero(t, testutil.ToFloat64(store.gcMetrics.reclaimedBytes))
	_, vlog, err := store.onDiskSize()
	require.NoError(t, err)
	require.Equal(
		t,
		float64(vlog),
		testutil.ToFloat64(store.gcMetrics.vlogBytes),
	)
}

func TestReclaimedBytesAccumulateWithinCycle(t *testing.T) {
	t.Parallel()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_reclaimed"})

	addReclaimedBytes(gauge, 300, 200, true)
	addReclaimedBytes(gauge, 200, 200, true)
	addReclaimedBytes(gauge, 200, 150, true)
	addReclaimedBytes(gauge, 150, 100, false)

	require.Equal(t, float64(150), testutil.ToFloat64(gauge))
}
