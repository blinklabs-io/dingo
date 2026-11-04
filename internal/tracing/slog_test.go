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

package tracing

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func logOnce(
	t *testing.T,
	ctx context.Context,
	wrap func(*slog.Logger) *slog.Logger,
) map[string]any {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewJSONHandler(&buf, nil)))
	if wrap != nil {
		logger = wrap(logger)
	}
	logger.InfoContext(ctx, "hello")
	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	return rec
}

func spanContext(sampled bool) trace.SpanContext {
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5, 6},
		TraceFlags: flags,
	})
}

// recordingSpan wraps a span context so IsRecording reports true without
// installing a tracer provider.
type recordingSpan struct {
	trace.Span
	sc trace.SpanContext
}

func (r recordingSpan) IsRecording() bool              { return true }
func (r recordingSpan) SpanContext() trace.SpanContext { return r.sc }

func recordingContext() context.Context {
	sc := spanContext(true)
	return trace.ContextWithSpan(
		context.Background(),
		recordingSpan{
			Span: trace.SpanFromContext(context.Background()),
			sc:   sc,
		},
	)
}

func TestLogHandlerAddsIDsInsideSpan(t *testing.T) {
	t.Parallel()
	rec := logOnce(t, recordingContext(), nil)
	sc := spanContext(true)
	require.Equal(t, sc.TraceID().String(), rec["trace_id"])
	require.Equal(t, sc.SpanID().String(), rec["span_id"])
}

func TestLogHandlerUntouchedOutsideSpan(t *testing.T) {
	t.Parallel()
	rec := logOnce(t, context.Background(), nil)
	require.NotContains(t, rec, "trace_id")
	require.NotContains(t, rec, "span_id")
}

func TestLogHandlerUntouchedForNonRecordingSpan(t *testing.T) {
	t.Parallel()
	ctx := trace.ContextWithSpanContext(
		context.Background(),
		spanContext(false),
	)
	rec := logOnce(t, ctx, nil)
	require.NotContains(t, rec, "trace_id")
	require.NotContains(t, rec, "span_id")
}

func TestLogHandlerSurvivesWithAttrsAndGroup(t *testing.T) {
	t.Parallel()
	rec := logOnce(t, recordingContext(), func(l *slog.Logger) *slog.Logger {
		return l.With("component", "x").WithGroup("g")
	})
	require.Equal(t, "x", rec["component"])
	group, ok := rec["g"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, spanContext(true).TraceID().String(), group["trace_id"])
}
