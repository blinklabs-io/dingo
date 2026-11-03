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
	"os"

	"github.com/prometheus/client_golang/prometheus"
	promBridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/contrib/exporters/autoexport"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// setupMetricExport pushes the node's Prometheus registry over OTLP when
// OTEL_METRICS_EXPORTER asks for it. The /metrics endpoint is unaffected.
func (n *Node) setupMetricExport(ctx context.Context) error {
	if v := os.Getenv("OTEL_METRICS_EXPORTER"); v == "" || v == "none" {
		return nil
	}
	gatherer, ok := n.config.promRegistry.(prometheus.Gatherer)
	if !ok {
		return errors.New(
			"OTEL_METRICS_EXPORTER is set but the metrics registry " +
				"cannot be gathered",
		)
	}
	reader, err := newMetricReader(ctx, gatherer)
	if err != nil {
		// The exporter comes only from the OTEL_* environment; a typo there
		// must not stop the node, and /metrics is unaffected either way.
		n.config.logger.Warn(
			"metric export disabled: cannot build the reader "+
				"OTEL_METRICS_EXPORTER selects",
			"component", "telemetry",
			"OTEL_METRICS_EXPORTER", os.Getenv("OTEL_METRICS_EXPORTER"),
			"error", err,
		)
		return nil
	}
	res, err := n.telemetryResource(ctx)
	if err != nil {
		return err
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	n.shutdownFuncs = append(n.shutdownFuncs, provider.Shutdown)
	return nil
}

// newMetricReader builds the reader OTEL_METRICS_EXPORTER selects, fed by
// the Prometheus bridge over gatherer so the pushed metrics carry the same
// names as /metrics.
func newMetricReader(
	ctx context.Context,
	gatherer prometheus.Gatherer,
) (sdkmetric.Reader, error) {
	autoexport.WithFallbackMetricProducer(
		func(context.Context) (sdkmetric.Producer, error) {
			return promBridge.NewMetricProducer(
				promBridge.WithGatherer(gatherer),
			), nil
		},
	)
	return autoexport.NewMetricReader(ctx)
}
