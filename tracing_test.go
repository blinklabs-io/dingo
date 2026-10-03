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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestWarnIfTracingMisconfigured covers the tracingStdout-without-tracing
// combination: setupTracing only runs when tracing is enabled, so stdout
// export alone exports nothing and must not fail silently.
func TestWarnIfTracingMisconfigured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		tracing       bool
		tracingStdout bool
		wantWarning   bool
	}{
		{name: "both disabled"},
		{name: "both enabled", tracing: true, tracingStdout: true},
		{name: "tracing only", tracing: true},
		{
			name:          "stdout without tracing",
			tracingStdout: true,
			wantWarning:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			n := &Node{
				config: Config{
					logger:        slog.New(slog.NewJSONHandler(&logs, nil)),
					tracing:       test.tracing,
					tracingStdout: test.tracingStdout,
				},
			}
			n.warnIfTracingMisconfigured()
			if !test.wantWarning {
				require.Empty(t, logs.String())
				return
			}
			require.Contains(t, logs.String(), `"level":"WARN"`)
			require.Contains(t, logs.String(), `"component":"tracing"`)
			require.Contains(
				t,
				logs.String(),
				"tracing stdout export is enabled but tracing is disabled",
			)
			// The operator must be told how to fix it, not just that it is wrong.
			require.Contains(t, logs.String(), "--tracing")
			require.Contains(t, logs.String(), "DINGO_TRACING_ENABLED=true")
		})
	}
}

func TestNewTracerProviderResourceAndSampler(t *testing.T) {
	t.Parallel()

	spanCount := func(t *testing.T, ratio float64) (int, tracetest.SpanStub) {
		t.Helper()
		exporter := tracetest.NewInMemoryExporter()
		provider := newTracerProvider(exporter, "dingo-test", ratio)
		for range 50 {
			_, span := provider.Tracer("dingo").Start(
				context.Background(),
				"probe",
			)
			span.End()
		}
		require.NoError(t, provider.ForceFlush(context.Background()))
		spans := exporter.GetSpans()
		require.NoError(t, provider.Shutdown(context.Background()))
		if len(spans) == 0 {
			return 0, tracetest.SpanStub{}
		}
		return len(spans), spans[0]
	}

	t.Run(
		"ratio one samples everything and sets the resource",
		func(t *testing.T) {
			t.Parallel()
			n, span := spanCount(t, 1)
			require.Equal(t, 50, n)
			attrs := map[string]string{}
			for _, kv := range span.Resource.Attributes() {
				attrs[string(kv.Key)] = kv.Value.Emit()
			}
			require.Equal(t, "dingo-test", attrs["service.name"])
			require.NotEmpty(t, attrs["service.version"])
		},
	)
	t.Run("ratio zero samples nothing", func(t *testing.T) {
		t.Parallel()
		n, _ := spanCount(t, 0)
		require.Zero(t, n)
	})
}

// Not t.Parallel: setupTracing replaces the process-wide OpenTelemetry
// tracer provider, propagator and error handler.
func TestSetupTracingExportsToConfiguredEndpoint(t *testing.T) {
	paths := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			paths <- r.URL.Path
			w.WriteHeader(http.StatusOK)
		},
	))
	t.Cleanup(server.Close)
	previous := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(previous) })

	n := &Node{config: Config{
		tracing:            true,
		tracingEndpoint:    server.URL,
		tracingServiceName: "dingo",
		tracingSampleRatio: 1,
	}}
	require.NoError(t, n.setupTracing(context.Background()))
	_, span := otel.Tracer("dingo").Start(context.Background(), "probe")
	span.End()
	for _, shutdown := range n.shutdownFuncs {
		require.NoError(t, shutdown(context.Background()))
	}

	select {
	case path := <-paths:
		require.Equal(t, "/v1/traces", path)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("no span export reached the configured endpoint")
	}
}

func TestOTLPTracesURL(t *testing.T) {
	t.Parallel()

	for endpoint, want := range map[string]string{
		"http://localhost:4318":                 "http://localhost:4318/v1/traces",
		"http://localhost:4318/":                "http://localhost:4318/v1/traces",
		"https://tempo.example/otlp/v1/traces":  "https://tempo.example/otlp/v1/traces",
		"http://collector.example:4318/custom/": "http://collector.example:4318/custom/",
	} {
		got, err := otlpTracesURL(endpoint)
		require.NoError(t, err)
		require.Equal(t, want, got, endpoint)
	}
	for _, endpoint := range []string{
		"http://bad host:4318",
		"localhost:4318",
		"collector.example:4318/v1/traces",
		"grpc://collector.example:4317",
		"http://",
	} {
		_, err := otlpTracesURL(endpoint)
		require.Error(t, err, endpoint)
	}
}

func TestTracingEndpointEnablesTracing(t *testing.T) {
	t.Parallel()

	const endpoint = "http://localhost:4318"
	unset := NewConfig()
	require.False(t, unset.Tracing())
	set := NewConfig(WithTracingEndpoint(endpoint))
	require.True(t, set.Tracing())
	setThenDisabled := NewConfig(
		WithTracingEndpoint(endpoint),
		WithTracing(false),
	)
	require.True(t, setThenDisabled.Tracing())
}
