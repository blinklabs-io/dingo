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

package testutil

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/internal/tracing"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// RecordSpans installs a recording tracer provider as the process-wide
// OpenTelemetry provider for the rest of the test and returns the recorder.
// The provider is a process global, so a test that calls this must stay
// sequential.
func RecordSpans(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	recorder := tracetest.NewSpanRecorder()
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(
		sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)),
	)
	t.Cleanup(func() { otel.SetTracerProvider(previous) })
	return recorder
}

// InstallUnsampledTracing installs an SDK tracer provider that samples
// nothing and enables block tracing at a zero ratio, restoring both when tb
// ends. Benchmarks use it to measure the cost tracing adds to a path whose
// spans are all dropped.
func InstallUnsampledTracing(tb testing.TB) {
	tb.Helper()
	previous := otel.GetTracerProvider()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.NeverSample()),
	)
	otel.SetTracerProvider(provider)
	wasEnabled := tracing.Enabled()
	wasNearTip := tracing.NearTip()
	prior := tracing.CurrentConfig()
	// Block tracing on with a zero ratio, near the tip: every stage takes
	// the sampling decision and drops the block.
	tracing.Configure(tracing.Config{SampleRatio: 0})
	tracing.SetNearTip(true)
	tb.Cleanup(func() {
		if wasEnabled {
			tracing.Configure(prior)
		} else {
			tracing.Disable()
		}
		tracing.SetNearTip(wasNearTip)
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	})
}

// ConfigureBlockTracing enables block tracing with cfg, marks the node near
// the tip so every stage is traced, and disables it again when t ends.
func ConfigureBlockTracing(t testing.TB, cfg tracing.Config) {
	t.Helper()
	tracing.Configure(cfg)
	tracing.SetNearTip(true)
	t.Cleanup(func() {
		tracing.Disable()
		tracing.SetNearTip(false)
	})
}
