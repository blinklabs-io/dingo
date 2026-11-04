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
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Not t.Parallel: reads OTEL_SERVICE_NAME through t.Setenv.
func TestServiceNamePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name, configured, env, want string
	}{
		{"explicit beats env", "a", "b", "a"},
		{"env when unset", "", "b", "b"},
		{"default when neither", "", "", "dingo"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", tc.env)
			require.Equal(t, tc.want, effectiveServiceName(tc.configured))
		})
	}
}

// Not t.Parallel: reads OTEL_RESOURCE_ATTRIBUTES through t.Setenv.
func TestResourceHonorsOTELResourceAttributes(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "k8s.pod.name=p,service.name=env")
	t.Setenv("OTEL_SERVICE_NAME", "")
	n := &Node{config: Config{
		tracingServiceName: "relay-1",
		network:            "preview",
		networkMagic:       2,
	}}
	res, err := n.telemetryResource(context.Background())
	require.NoError(t, err)
	attrs := map[string]string{}
	for _, kv := range res.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	require.Equal(t, "p", attrs["k8s.pod.name"])
	require.Equal(t, "relay-1", attrs["service.name"])
	require.Equal(t, "preview", attrs["cardano.network.name"])
	require.Equal(t, "2", attrs["cardano.network.magic"])
	require.NotEmpty(t, attrs["service.version"])
}

// Not t.Parallel: reads OTEL_TRACES_EXPORTER through t.Setenv.
func TestAutoexportUsedWithoutEndpoint(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	n := &Node{config: Config{tracing: true}}
	exporter, err := n.traceExporter(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = exporter.Shutdown(context.Background()) })
	require.IsType(t, &stdouttrace.Exporter{}, exporter)
}

type blockingExporter struct{ release chan struct{} }

func (b blockingExporter) ExportSpans(
	ctx context.Context,
	_ []sdktrace.ReadOnlySpan,
) error {
	select {
	case <-b.release:
	case <-ctx.Done():
	}
	return nil
}

func (blockingExporter) Shutdown(context.Context) error { return nil }

// Not t.Parallel: installs the process-wide tracer provider and the block
// tracing configuration.
func TestBlockApplyUnaffectedByStalledExporter(t *testing.T) {
	exporter := blockingExporter{release: make(chan struct{})}
	n := &Node{config: Config{tracingServiceName: "dingo"}}
	res, err := n.telemetryResource(context.Background())
	require.NoError(t, err)
	provider := newTracerProvider(exporter, res, 1)
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	tracing.Configure(tracing.Config{SampleRatio: 1})
	tracing.SetNearTip(true)
	t.Cleanup(func() {
		close(exporter.release)
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(previous)
		tracing.Disable()
		tracing.SetNearTip(false)
	})

	hash := make([]byte, 32)
	start := time.Now()
	for range 10000 {
		_, _ = rand.Read(hash)
		_, span := tracing.StartBlock(
			context.Background(),
			tracing.StageApply,
			"block.apply",
			hash,
			nil,
		)
		span.End()
	}
	require.Less(t, time.Since(start), 2*time.Second)
}

// Not t.Parallel: reads OTEL_RESOURCE_ATTRIBUTES through t.Setenv.
func TestResourceToleratesMalformedOTELResourceAttributes(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "a=b,c")
	n := &Node{config: Config{
		tracingServiceName: "relay-1",
		logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	}}
	res, err := n.telemetryResource(context.Background())
	require.NoError(t, err)
	require.NotNil(t, res)
	attrs := map[string]string{}
	for _, kv := range res.Attributes() {
		attrs[string(kv.Key)] = kv.Value.Emit()
	}
	require.Equal(t, "relay-1", attrs["service.name"])
}

// Not t.Parallel: sets OTEL_TRACES_EXPORTER and the process-wide tracer
// provider.
func TestInvalidTracesExporterIsNotFatal(t *testing.T) {
	t.Setenv("OTEL_TRACES_EXPORTER", "bogus")
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
		tracing.Disable()
	})
	tracing.Disable()
	var logs bytes.Buffer
	n := &Node{config: Config{
		tracing: true,
		logger:  slog.New(slog.NewTextHandler(&logs, nil)),
	}}
	require.NoError(t, n.setupTracing(context.Background()))
	require.False(t, tracing.Enabled())
	require.Contains(t, logs.String(), "bogus")
}

// Not t.Parallel: replaces the process-wide tracer provider.
func TestStandaloneTracingExportsSpans(t *testing.T) {
	paths := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			paths <- r.URL.Path
			w.WriteHeader(http.StatusOK)
		},
	))
	t.Cleanup(server.Close)
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
	})

	shutdown, err := SetupStandaloneTracing(
		context.Background(),
		NewConfig(WithTracingEndpoint(server.URL)),
	)
	require.NoError(t, err)
	_, span := tracing.Start(context.Background(), "mithril.bootstrap")
	span.End()
	require.NoError(t, shutdown(context.Background()))

	select {
	case path := <-paths:
		require.Equal(t, "/v1/traces", path)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("standalone tracing exported nothing")
	}
}

// Not t.Parallel: inspects the process-wide tracer provider.
func TestStandaloneTracingOffIsNoop(t *testing.T) {
	previous := otel.GetTracerProvider()
	shutdown, err := SetupStandaloneTracing(context.Background(), NewConfig())
	require.NoError(t, err)
	require.NoError(t, shutdown(context.Background()))
	require.Same(t, previous, otel.GetTracerProvider())
}
