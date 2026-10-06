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
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/blinklabs-io/dingo/internal/version"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// warnIfTracingMisconfigured logs a warning when stdout span export is
// requested while tracing itself is disabled. setupTracing is only called
// when tracing is enabled (see Node.Run), so tracingStdout on its own never
// creates an exporter and would otherwise be silently ignored.
func (n *Node) warnIfTracingMisconfigured() {
	if n.config.tracing || !n.config.tracingStdout {
		return
	}
	n.config.logger.Warn(
		"tracing stdout export is enabled but tracing is disabled, so no spans"+
			" will be exported; enable tracing (yaml tracing: true, --tracing,"+
			" or DINGO_TRACING_ENABLED=true) or turn off the stdout option",
		"component", "tracing",
	)
}

func (n *Node) setupTracing(ctx context.Context) error {
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
		var opts []otlptracehttp.Option
		if n.config.tracingEndpoint != "" {
			endpoint, endpointErr := otlpTracesURL(n.config.tracingEndpoint)
			if endpointErr != nil {
				return endpointErr
			}
			opts = append(opts, otlptracehttp.WithEndpointURL(endpoint))
		}
		traceExporter, err = otlptracehttp.New(ctx, opts...)
	}
	if err != nil {
		return err
	}
	tracerProvider := newTracerProvider(
		traceExporter,
		n.config.tracingServiceName,
		n.config.tracingSampleRatio,
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

func newTracerProvider(
	exporter trace.SpanExporter,
	serviceName string,
	sampleRatio float64,
) *trace.TracerProvider {
	serviceVersion := version.Version
	if serviceVersion == "" {
		serviceVersion = "devel"
	}
	return trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewSchemaless(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		)),
		// Honor the caller's sampling decision so one trace is never
		// sampled at some hops and dropped at others.
		trace.WithSampler(
			trace.ParentBased(trace.TraceIDRatioBased(sampleRatio)),
		),
	)
}

// otlpTracesURL returns the OTLP traces URL for endpoint. WithEndpointURL
// uses the path verbatim, so a bare collector address such as
// http://localhost:4318 gets the standard /v1/traces path, as the
// OTEL_EXPORTER_OTLP_ENDPOINT variable would.
func otlpTracesURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid tracing endpoint: %w", err)
	}
	// url.Parse reads a bare host:port as a scheme and an opaque part, which
	// the exporter would accept and then send nowhere.
	// Schemes are case-insensitive, and the exporter only accepts them in
	// lower case.
	u.Scheme = strings.ToLower(u.Scheme)
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf(
			"invalid tracing endpoint %q: want an http or https URL",
			endpoint,
		)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/traces"
	}
	return u.String(), nil
}
