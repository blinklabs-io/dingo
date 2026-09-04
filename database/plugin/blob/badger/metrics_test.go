// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package badger

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type nonComparableRegisterer struct {
	prometheus.Registerer
	labels []string
}

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
	registerer := prometheus.WrapRegistererWith(
		prometheus.Labels{"network": "preview"},
		registry,
	)
	first := &BlobStoreBadger{promRegistry: registerer}
	second := &BlobStoreBadger{promRegistry: registerer}

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

func TestRegisterBlobMetricsAllowsNonComparableRegisterer(t *testing.T) {
	registry := prometheus.NewRegistry()
	registerer := nonComparableRegisterer{
		Registerer: registry,
		labels:     []string{"network"},
	}
	store := &BlobStoreBadger{promRegistry: registerer}
	require.NotPanics(t, store.registerBlobMetrics)
	second := &BlobStoreBadger{promRegistry: registerer}
	require.NotPanics(t, second.registerBlobMetrics)
	second.gcMetrics.attempts.Inc()
	count, err := testutil.GatherAndCount(registry,
		"database_blob_gc_attempts_total")
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, float64(1), testutil.ToFloat64(second.gcMetrics.attempts))
}
