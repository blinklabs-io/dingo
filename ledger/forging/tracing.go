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

package forging

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/gouroboros/ledger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// traceForgedBlock records a block.forge span in the forged block's trace,
// covering the forge attempt from start until the block was adopted. On a
// network of dingo nodes this is the first span of the block's trace. The
// returned function exports its child phases after publication; the root is
// visible immediately even if the observer blocks.
func traceForgedBlock(
	ctx context.Context,
	block ledger.Block,
	slot uint64,
	start, end time.Time,
) func() {
	_, span := tracing.StartHeader(ctx, tracing.StageForge, "block.forge", block,
		trace.WithTimestamp(start))
	if !span.IsRecording() {
		span.End(trace.WithTimestamp(end))
		return func() {}
	}
	span.SetAttributes(tracing.Uint64("forge.slot", slot))
	root := span.SpanContext()
	span.End(trace.WithTimestamp(end))
	timings, ok := ctx.Value(forgeTraceKey{}).(*forgeTrace)
	if !ok {
		return func() {}
	}
	return func() {
		parents := make([]trace.SpanContext, len(timings.phases))
		for i, phase := range timings.phases {
			if phase.end.IsZero() {
				continue
			}
			parent := root
			if phase.parent >= 0 {
				parent = parents[phase.parent]
			}
			_, child := otel.Tracer(tracing.Name).Start(
				trace.ContextWithSpanContext(ctx, parent), phase.name,
				trace.WithTimestamp(phase.start), trace.WithAttributes(phase.attrs...))
			parents[i] = child.SpanContext()
			child.End(trace.WithTimestamp(phase.end))
		}
	}
}

// The block hash is not known until construction finishes. Capture timestamps
// on the forging goroutine, then export them under the sampled block trace
// rather than starting an unrelated trace before selection.
type forgeTraceKey struct{}

type forgePhase struct {
	name       string
	start, end time.Time
	parent     int
	attrs      []attribute.KeyValue
}

type forgeTrace struct {
	phases []forgePhase
	active int
}

func withForgeTrace(
	ctx context.Context,
	start, leaderChecked time.Time,
) context.Context {
	if !tracing.Enabled() {
		return ctx
	}
	timings := &forgeTrace{phases: make([]forgePhase, 1, 16), active: -1}
	timings.phases[0] = forgePhase{
		name:  "block.forge.leader_check",
		start: start, end: leaderChecked, parent: -1,
	}
	return context.WithValue(ctx, forgeTraceKey{}, timings)
}

func beginForgePhase(
	ctx context.Context,
	name string,
) func(...attribute.KeyValue) {
	timings, ok := ctx.Value(forgeTraceKey{}).(*forgeTrace)
	if !ok {
		return func(...attribute.KeyValue) {}
	}
	parent := timings.active
	index := len(timings.phases)
	timings.phases = append(timings.phases, forgePhase{
		name: name, start: time.Now(), parent: parent,
	})
	timings.active = index
	return func(attrs ...attribute.KeyValue) {
		timings.phases[index].end = time.Now()
		timings.phases[index].attrs = attrs
		timings.active = parent
	}
}

// Publication enqueues diffusion after adoption. Keep it separate so the
// existing block.forge interval still ends at durable acceptance.
func beginForgePublication(ctx context.Context, block ledger.Block) func() {
	_, span := tracing.StartHeader(ctx, tracing.StageForge,
		"block.forge.publish", block)
	return func() { span.End() }
}
