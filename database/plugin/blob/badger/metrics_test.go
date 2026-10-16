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
	"sync"
	"testing"
	"time"

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
	populateGCFixture(t, store)
	runValueLogGC := store.runValueLogGC
	var wantReclaimed float64
	store.runValueLogGC = func(ratio float64) error {
		beforeLSM, beforeVlog, err := store.onDiskSize()
		if err != nil {
			return err
		}
		gcErr := runValueLogGC(ratio)
		afterLSM, afterVlog, sizeErr := store.onDiskSize()
		if sizeErr != nil {
			return sizeErr
		}
		before := beforeLSM + beforeVlog
		after := afterLSM + afterVlog
		if gcErr == nil && before > after {
			wantReclaimed += float64(before - after)
		}
		return gcErr
	}

	ticks := make(chan time.Time, 1)
	stop := make(chan struct{})
	var stopOnce sync.Once
	// Registered after the store's Close cleanup so it runs first: Close waits
	// for this goroutine, and an assertion failure would otherwise hang it.
	t.Cleanup(func() { stopOnce.Do(func() { close(stop) }) })
	store.gcWg.Add(1)
	go store.blobGc(ticks, stop)
	// Badger samples a value-log file at random, so a cycle can end in
	// ErrNoRewrite without any rewrite. Tick until one cycle rewrites.
	for cycle := 1.0; testutil.ToFloat64(store.gcMetrics.successes) == 0; cycle++ {
		require.LessOrEqual(t, cycle, 20.0, "no GC cycle rewrote a value-log file")
		attemptsBefore := testutil.ToFloat64(store.gcMetrics.attempts)
		noRewriteBefore := testutil.ToFloat64(store.gcMetrics.noRewrite)
		errorsBefore := testutil.ToFloat64(store.gcMetrics.errors)
		ticks <- time.Time{}
		require.Eventually(t, func() bool {
			attempts := testutil.ToFloat64(store.gcMetrics.attempts)
			successes := testutil.ToFloat64(store.gcMetrics.successes)
			noRewrite := testutil.ToFloat64(store.gcMetrics.noRewrite)
			errors := testutil.ToFloat64(store.gcMetrics.errors)
			cycleComplete := attempts > attemptsBefore &&
				attempts == successes+noRewrite+errors
			return cycleComplete &&
				(successes > 0 || noRewrite > noRewriteBefore || errors > errorsBefore)
		}, time.Minute, 10*time.Millisecond)
	}
	require.Positive(t, testutil.ToFloat64(store.gcMetrics.consecutive))
	require.Equal(
		t,
		wantReclaimed,
		testutil.ToFloat64(store.gcMetrics.reclaimedBytes),
	)
	cycles := testutil.ToFloat64(store.gcMetrics.noRewrite)
	ticks <- time.Time{}
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(store.gcMetrics.noRewrite) > cycles
	}, time.Minute, 10*time.Millisecond)
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
