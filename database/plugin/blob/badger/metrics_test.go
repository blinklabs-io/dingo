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

	ticks := make(chan time.Time, 1)
	stop := make(chan struct{})
	store.gcWg.Add(1)
	go store.blobGc(ticks, stop)
	ticks <- time.Time{}
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(store.gcMetrics.noRewrite) > 0
	}, time.Minute, 10*time.Millisecond)
	close(stop)
	store.gcWg.Wait()

	require.Positive(t, testutil.ToFloat64(store.gcMetrics.successes))
	require.Positive(t, testutil.ToFloat64(store.gcMetrics.consecutive))
	require.Positive(t, testutil.ToFloat64(store.gcMetrics.reclaimedBytes))
	_, vlog, err := store.onDiskSize()
	require.NoError(t, err)
	require.Equal(
		t,
		float64(vlog),
		testutil.ToFloat64(store.gcMetrics.vlogBytes),
	)
}
