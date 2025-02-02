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
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
)

func (n *Node) setupTracing() error {
	// Set up propagator.
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	// Set up trace provider.
	var traceExporter trace.SpanExporter
	var err error
	if n.config.tracingStdout {
		traceExporter, err = stdouttrace.New(
			stdouttrace.WithPrettyPrint(),
		)
	} else {
		// TODO: make options configurable (#387)
		traceExporter, err = otlptracehttp.New(context.TODO())
	}
	if err != nil {
		return err
	}
	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
	)
	n.shutdownFuncs = append(n.shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)
	otel.SetErrorHandler(
		otel.ErrorHandlerFunc(
			func(err error) {
				slog.Error(err.Error())
			},
		),
	)

	return nil
}
