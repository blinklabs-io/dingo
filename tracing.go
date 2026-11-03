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
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/tracing"
	"github.com/blinklabs-io/dingo/internal/version"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
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

	pools := make([][28]byte, 0, len(n.config.tracingAlwaysSamplePools))
	for _, p := range n.config.tracingAlwaysSamplePools {
		pool, err := internalconfig.ParsePoolKeyHash(p)
		if err != nil {
			return fmt.Errorf("tracing always-sample pools: %w", err)
		}
		pools = append(pools, pool)
	}
	traceExporter, err := n.traceExporter(ctx)
	if errors.Is(err, errAutoexportFailed) {
		// Only the standard OTEL_* variables chose this exporter; a typo
		// there must not stop the node, so tracing stays off instead.
		n.config.logger.Warn(
			"tracing disabled: cannot build the span exporter the "+
				"OTEL_* environment selects",
			"component", "tracing",
			"OTEL_TRACES_EXPORTER", os.Getenv("OTEL_TRACES_EXPORTER"),
			"error", err,
		)
		return nil
	}
	if err != nil {
		return err
	}
	res, err := n.telemetryResource(ctx)
	if err != nil {
		return err
	}
	tracerProvider := newTracerProvider(
		traceExporter,
		res,
		n.config.tracingSampleRatio,
	)
	n.shutdownFuncs = append(n.shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)
	logger := n.config.logger
	otel.SetErrorHandler(
		otel.ErrorHandlerFunc(
			func(err error) {
				logger.Error(
					"opentelemetry export failed",
					"component", "tracing",
					"error", err,
				)
			},
		),
	)
	tracing.Configure(tracing.Config{
		SampleRatio:       n.config.tracingSampleRatio,
		AlwaysSamplePools: pools,
	})

	return nil
}

// traceExporter selects the span exporter. Explicit dingo settings win:
// stdout, then the configured endpoint. Otherwise the standard
// OTEL_TRACES_EXPORTER and OTEL_EXPORTER_OTLP_* variables decide, with OTLP
// over HTTP as their default.
func (n *Node) traceExporter(ctx context.Context) (trace.SpanExporter, error) {
	if n.config.tracingStdout {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}
	if n.config.tracingEndpoint != "" {
		endpoint, err := otlpTracesURL(n.config.tracingEndpoint)
		if err != nil {
			return nil, err
		}
		return otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	}
	exporter, err := autoexport.NewSpanExporter(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errAutoexportFailed, err)
	}
	return exporter, nil
}

// errAutoexportFailed marks an exporter error that came from the standard
// OTEL_* environment rather than from an explicit dingo setting.
var errAutoexportFailed = errors.New("OTEL_* exporter selection failed")

// effectiveServiceName returns the configured service name, else
// OTEL_SERVICE_NAME, else "dingo".
func effectiveServiceName(configured string) string {
	if configured != "" {
		return configured
	}
	if env := os.Getenv("OTEL_SERVICE_NAME"); env != "" {
		return env
	}
	return "dingo"
}

// telemetryResource describes this node for every exported signal. A
// malformed OTEL_RESOURCE_ATTRIBUTES only loses the attributes it could not
// parse: it is reported, never fatal.
func (n *Node) telemetryResource(
	ctx context.Context,
) (*resource.Resource, error) {
	res, err := NewTelemetryResource(
		ctx,
		n.config.tracingServiceName,
		n.config.network,
		n.config.networkMagic,
	)
	if errors.Is(err, resource.ErrPartialResource) {
		n.config.logger.Warn(
			"ignoring unparsable OTEL_RESOURCE_ATTRIBUTES entries",
			"component", "tracing",
			"error", err,
		)
		return res, nil
	}
	return res, err
}

// NewTelemetryResource returns the OpenTelemetry resource dingo attaches to
// exported traces, logs and metrics. When OTEL_RESOURCE_ATTRIBUTES cannot be
// fully parsed it returns the resource it could build together with an error
// wrapping resource.ErrPartialResource. An empty serviceName falls back to
// OTEL_SERVICE_NAME, then "dingo"; an empty network or zero magic is
// omitted. OTEL_RESOURCE_ATTRIBUTES is honored, but these attributes win
// where the two name the same key.
func NewTelemetryResource(
	ctx context.Context,
	serviceName string,
	network string,
	networkMagic uint32,
) (*resource.Resource, error) {
	serviceVersion := version.Version
	if serviceVersion == "" {
		serviceVersion = "devel"
	}
	attrs := []attribute.KeyValue{
		semconv.ServiceName(effectiveServiceName(serviceName)),
		semconv.ServiceVersion(serviceVersion),
	}
	if network != "" {
		attrs = append(attrs, attribute.String("cardano.network.name", network))
	}
	if networkMagic != 0 {
		attrs = append(
			attrs,
			attribute.Int64("cardano.network.magic", int64(networkMagic)),
		)
	}
	return resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithAttributes(attrs...),
	)
}

func newTracerProvider(
	exporter trace.SpanExporter,
	res *resource.Resource,
	sampleRatio float64,
) *trace.TracerProvider {
	return trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(res),
		// Honor the caller's sampling decision so one trace is never
		// sampled at some hops and dropped at others. Block traces rely on
		// this: their synthetic parent carries the block's decision.
		trace.WithSampler(
			trace.ParentBased(trace.TraceIDRatioBased(sampleRatio)),
		),
	)
}

// otlpTracesURL returns the OTLP traces URL for endpoint. WithEndpointURL
// uses the path verbatim, so a bare collector address such as
// http://localhost:4318 gets the standard /v1/traces path, as the
// OTEL_EXPORTER_OTLP_ENDPOINT variable would.
//
// Errors never quote endpoint, and the result carries no userinfo, query or
// fragment: the exporter sends only scheme, host and path, so dropping them
// changes nothing on the wire and keeps credentials out of every later
// exporter error.
func otlpTracesURL(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		// Even the cause of a *url.Error quotes input: a '/', '?' or '#'
		// in a password ends the authority early, and the password is then
		// reported as an invalid port.
		return "", errors.New("invalid tracing endpoint: malformed URL")
	}
	// url.Parse reads a bare host:port as a scheme and an opaque part, which
	// the exporter would accept and then send nowhere.
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", errors.New(
			"invalid tracing endpoint: want an http or https URL",
		)
	}
	u.User = nil
	u.RawQuery, u.ForceQuery = "", false
	u.Fragment, u.RawFragment = "", ""
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/traces"
	}
	return u.String(), nil
}

// SetupStandaloneTracing installs tracing from cfg for work that runs before
// or without a Node, such as a Mithril bootstrap ahead of serving. It
// returns the function that flushes and removes it again; when cfg does not
// enable tracing, both are no-ops.
func SetupStandaloneTracing(
	ctx context.Context,
	cfg Config,
) (func(context.Context) error, error) {
	if !cfg.tracing {
		return func(context.Context) error { return nil }, nil
	}
	if cfg.logger == nil {
		cfg.logger = slog.New(slog.DiscardHandler)
	}
	n := &Node{config: cfg}
	if err := n.setupTracing(ctx); err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		tracing.Disable()
		var errs error
		for _, shutdown := range n.shutdownFuncs {
			errs = errors.Join(errs, shutdown(ctx))
		}
		return errs
	}, nil
}
