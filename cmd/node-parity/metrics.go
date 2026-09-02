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

package main

import (
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// parityMetrics holds this process's Prometheus counters. Metric names and
// label sets are deliberately small and closed: never a pool ID, tx hash,
// or TxIn label, which would make cardinality unbounded by the size of the
// chain being watched.
type parityMetrics struct {
	// checksTotal counts completed cycles (matched or diverged); skipped
	// cycles are counted separately by checksSkippedTotal rather than
	// folded in here as a false "matched".
	checksTotal prometheus.Counter
	// checksSkippedTotal's "reason" is closed to the two sandwichOK failure
	// modes: nodeparity.SkipTipMismatch and nodeparity.SkipTipAdvanced.
	checksSkippedTotal *prometheus.CounterVec
	// divergenceTotal's "field" is closed to the three fields
	// nodeparity.Diff reports: protocol_params, stake_distribution, utxo.
	divergenceTotal *prometheus.CounterVec
}

// newParityMetrics registers this process's counters under a registry
// wrapped with a "network" const label, matching the real dingo node's own
// configWrapPromRegistry (root config.go): every metric this tool emits
// carries the network it was run against, the same way dingo's own
// cardano_node_metrics_* do, rather than requiring an operator's scrape
// config to attach one after the fact. Registers into the process-wide
// default registerer, which serveMetrics's promhttp.Handler() serves.
func newParityMetrics(network string) *parityMetrics {
	return newParityMetricsIn(network, prometheus.DefaultRegisterer)
}

// newParityMetricsIn is newParityMetrics with the underlying registerer
// injectable, so tests can register into a throwaway
// prometheus.NewRegistry() instead of the process-wide default -- which
// only allows one registration per metric name per process, so a second
// production call (or a second test) would otherwise panic on a duplicate
// registration.
func newParityMetricsIn(
	network string, base prometheus.Registerer,
) *parityMetrics {
	registry := prometheus.WrapRegistererWith(
		prometheus.Labels{"network": network},
		base,
	)
	factory := promauto.With(registry)
	return &parityMetrics{
		checksTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "node_parity_checks_total",
			Help: "Completed node-parity check cycles (matched or diverged; excludes skipped cycles).",
		}),
		checksSkippedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "node_parity_checks_skipped_total",
			Help: "Check cycles discarded because the two nodes did not hold a stable common tip, by reason.",
		}, []string{"reason"}),
		divergenceTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "node_parity_divergence_total",
			Help: "Ledger-state divergences found between dingo and cardano-node, by field.",
		}, []string{"field"}),
	}
}

// recordSkip increments checksSkippedTotal for a discarded cycle.
func (m *parityMetrics) recordSkip(reason string) {
	m.checksSkippedTotal.WithLabelValues(reason).Inc()
}

// recordCheck increments checksTotal and, for each field the diff actually
// found a divergence in, divergenceTotal.
func (m *parityMetrics) recordCheck(diff nodeparity.Diff) {
	m.checksTotal.Inc()
	if diff.ProtocolParamsDiff != "" {
		m.divergenceTotal.WithLabelValues("protocol_params").Inc()
	}
	if len(diff.StakeDistribution) > 0 {
		m.divergenceTotal.WithLabelValues("stake_distribution").Inc()
	}
	if len(diff.UTxO) > 0 {
		m.divergenceTotal.WithLabelValues("utxo").Inc()
	}
}

// serveMetrics starts a Prometheus /metrics HTTP server on addr in the
// background and returns it so the caller can Shutdown it on exit. A
// dedicated mux (rather than http.DefaultServeMux) keeps this from ever
// exposing anything but /metrics, matching internal/node/node.go's own
// metrics-listener convention. It serves the process-wide default gatherer
// (promhttp.Handler()), which sees newParityMetrics's counters regardless of
// the network-label wrapping used to register them.
func serveMetrics(addr string, logger *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 60 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		logger.Info("serving prometheus metrics", "addr", addr)
		if err := srv.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "err", err)
		}
	}()
	return srv
}
