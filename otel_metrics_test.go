// Copyright 2024 Blink Labs Software
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
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// Not t.Parallel: these tests set OTEL_METRICS_EXPORTER.

func TestMetricExportOffByDefault(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "")
	n := &Node{config: Config{promRegistry: prometheus.NewRegistry()}}
	require.NoError(t, n.setupMetricExport(context.Background()))
	require.Empty(t, n.shutdownFuncs)
}

func TestMetricExportPushesRegistry(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	reg := prometheus.NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dingo_test_probe_total",
		Help: "probe",
	})
	reg.MustRegister(counter)
	counter.Inc()

	reader, err := newMetricReader(context.Background(), reg)
	require.NoError(t, err)
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}
	require.True(
		t,
		strings.Contains(strings.Join(names, ","), "dingo_test_probe"),
		"collected %v",
		names,
	)
}

func TestMetricExportRequiresGatherer(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	n := &Node{config: Config{promRegistry: registererOnly{}}}
	require.Error(t, n.setupMetricExport(context.Background()))
}

type registererOnly struct{ prometheus.Registerer }

// The node wraps its registerer with a network label and then a rebuildable
// wrapper, neither of which is a Gatherer, so export must read the registry
// kept from before the wrap.
func TestMetricExportWorksThroughWrappedRegistry(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "console")
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{
		promRegistry: reg,
		cfg:          &internalconfig.Config{Network: "preview"},
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	n.configWrapPromRegistry()
	n.rebuildableMetrics = newRebuildableRegisterer(n.config.promRegistry)
	n.config.promRegistry = n.rebuildableMetrics
	_, isGatherer := n.config.promRegistry.(prometheus.Gatherer)
	require.False(t, isGatherer)

	require.NoError(t, n.setupMetricExport(context.Background()))
	require.Len(t, n.shutdownFuncs, 1)
	require.NoError(t, n.shutdownFuncs[0](context.Background()))
}

func TestInvalidMetricsExporterIsNotFatal(t *testing.T) {
	t.Setenv("OTEL_METRICS_EXPORTER", "bogus")
	var logs bytes.Buffer
	reg := prometheus.NewRegistry()
	n := &Node{
		config: Config{
			promRegistry: reg,
			logger:       slog.New(slog.NewTextHandler(&logs, nil)),
		},
		promGatherer: reg,
	}
	require.NoError(t, n.setupMetricExport(context.Background()))
	require.Empty(t, n.shutdownFuncs)
	require.Contains(t, logs.String(), "bogus")
}
