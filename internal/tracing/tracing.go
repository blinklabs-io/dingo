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

// Package tracing starts spans on the process-wide OpenTelemetry tracer
// provider. With no provider installed the spans are no-ops.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Name is the instrumentation scope of every span the node starts.
const Name = "dingo"

// Start starts a span named name as a child of any span in ctx.
func Start(
	ctx context.Context,
	name string,
	attrs ...attribute.KeyValue,
) (context.Context, trace.Span) {
	//nolint:spancheck // the caller ends the returned span
	return otel.Tracer(Name).Start(
		ctx,
		name,
		trace.WithAttributes(attrs...),
	)
}

// End ends span, marking it failed when err is non-nil.
func End(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// Uint64 is attribute.Int64 for an unsigned value. Slots and epochs stay far
// below the int64 limit, so the conversion cannot overflow in practice.
func Uint64(key string, v uint64) attribute.KeyValue {
	return attribute.Int64(key, int64(v)) //nolint:gosec // see above
}
