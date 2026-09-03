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
	"fmt"
	"log/slog"
	"net"
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
	// checkErrorsTotal counts Check calls that failed outright (a dial or
	// query error), as opposed to a completed cycle that found a
	// divergence or a discarded (skipped) cycle. Counted separately so
	// NodeParityNotChecking's "is the tool doing anything at all" signal
	// stays true even when every cycle is failing -- a persistently
	// misconfigured address (wrong port, node down) makes checksTotal and
	// checksSkippedTotal both stay at zero forever, which looks
	// indistinguishable from the tool itself being stuck unless something
	// else confirms it is actually attempting and failing.
	checkErrorsTotal prometheus.Counter
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
	checksSkippedTotal := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "node_parity_checks_skipped_total",
		Help: "Check cycles discarded because the two nodes did not hold a stable common tip, by reason.",
	}, []string{"reason"})
	divergenceTotal := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "node_parity_divergence_total",
		Help: "Ledger-state divergences found between dingo and cardano-node, by field.",
	}, []string{"field"})
	// A CounterVec exposes no series at all for a label value until
	// something increments it. NodeParityNotChecking's alert expression
	// sums rate(checks_skipped_total) into rate(checks_total): if no skip
	// has ever happened, that side of the sum is missing data rather than
	// zero, and a binary PromQL operator between vectors drops any series
	// missing from either side -- so the whole expression would evaluate to
	// no data instead of a genuine 0, and the alert could never fire even
	// while the tool is dead. Pre-materializing every reason (and, for
	// dashboard consistency, every divergence field) at construction time
	// gives them a real 0 sample from process start, the same way the bare
	// checksTotal Counter already behaves.
	for _, reason := range []string{nodeparity.SkipTipMismatch, nodeparity.SkipTipAdvanced} {
		checksSkippedTotal.WithLabelValues(reason)
	}
	for _, field := range []string{"protocol_params", "stake_distribution", "utxo"} {
		divergenceTotal.WithLabelValues(field)
	}
	return &parityMetrics{
		checksTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "node_parity_checks_total",
			Help: "Completed node-parity check cycles (matched or diverged; excludes skipped cycles).",
		}),
		checksSkippedTotal: checksSkippedTotal,
		divergenceTotal:    divergenceTotal,
		checkErrorsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "node_parity_check_errors_total",
			Help: "Check calls that failed outright (a dial or query error), as opposed to a completed or skipped cycle.",
		}),
	}
}

// recordSkip increments checksSkippedTotal for a discarded cycle.
func (m *parityMetrics) recordSkip(reason string) {
	m.checksSkippedTotal.WithLabelValues(reason).Inc()
}

// recordCheckError increments checkErrorsTotal for a Check call that failed
// outright (a dial or query error).
func (m *parityMetrics) recordCheckError() {
	m.checkErrorsTotal.Inc()
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

// serveMetrics binds addr and starts a Prometheus /metrics HTTP server on it
// in the background, returning the server so the caller can Shutdown it on
// exit. The bind itself (net.Listen) happens synchronously so a bad address
// or an already-occupied port is returned to the caller immediately, rather
// than only ever appearing as a background log line while watchRun carries
// on as if monitoring were up. A dedicated mux (rather than
// http.DefaultServeMux) keeps this from ever exposing anything but
// /metrics, matching internal/node/node.go's own metrics-listener
// convention. It serves the process-wide default gatherer
// (promhttp.Handler()), which sees newParityMetrics's counters regardless of
// the network-label wrapping used to register them.
func serveMetrics(addr string, logger *slog.Logger) (*http.Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen %s: %w", addr, err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		// The actual bound address, not the possibly-":0"/wildcard addr
		// argument: a caller (or a test) reading srv.Addr back after this
		// returns needs the real port the OS assigned, not what was asked
		// for.
		Addr:              listener.Addr().String(),
		Handler:           mux,
		ReadHeaderTimeout: 60 * time.Second,
		// ReadTimeout bounds the whole request, not just headers:
		// ReadHeaderTimeout alone still lets a client complete the headers
		// and then drip the body indefinitely, holding the connection open.
		// /metrics is a GET with no body, but net/http does not reject an
		// unbounded body on its own.
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	go func() {
		logger.Info("serving prometheus metrics", "addr", srv.Addr)
		if err := srv.Serve(listener); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server error", "err", err)
		}
	}()
	return srv, nil
}
