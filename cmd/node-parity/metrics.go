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
// ReferenceCardanoNode and ReferenceKoios are divergenceTotal's "reference"
// label values: which oracle Dingo's ledger state was compared against.
// check/watch compare against a live cardano-node; from-genesis compares
// against Koios, because cardano-node cannot fill the reference role for a
// from-genesis replay (see from_genesis.go's command doc).
const (
	ReferenceCardanoNode = "cardano_node"
	ReferenceKoios       = "koios"
)

type parityMetrics struct {
	// checksTotal counts completed cycles (matched or diverged); skipped
	// cycles are counted separately by checksSkippedTotal rather than
	// folded in here as a false "matched".
	checksTotal prometheus.Counter
	// checksSkippedTotal's "reason" is closed to tipsAgree's failure mode:
	// nodeparity.SkipTipMismatch.
	checksSkippedTotal *prometheus.CounterVec
	// divergenceTotal's "field" is closed to the three fields
	// nodeparity.Diff reports: protocol_params, stake_distribution, utxo.
	// Its "reference" says which oracle Dingo was compared against --
	// cardano_node for check/watch, koios for from-genesis. Without it a
	// responder reading NodeParityDivergence cannot tell which side to go
	// look at, and the two references fail in very different ways: a
	// cardano-node disagreement is a consensus question, a Koios one can
	// equally be that oracle's own data.
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

	// incrementalBlocksTotal counts blocks validated by --mode=incremental's
	// per-block delta check (matched or diverged); incrementalMismatchTotal
	// is the subset that diverged. Separate from checksTotal/divergenceTotal
	// above, which count whole-ledger-state (full-mode or checkpoint)
	// cycles -- folding the two together would make a dashboard built for
	// one mode's cadence misread the other's.
	incrementalBlocksTotal   prometheus.Counter
	incrementalMismatchTotal prometheus.Counter
	// fullCheckTriggersTotal's "reason" is closed to nodeparity's
	// FullCheckReason constants (startup, interval, epoch_transition,
	// rollback, mismatch) -- always incremental mode's own full checkpoints,
	// never full mode's per-block-triggered checks (those are checksTotal).
	fullCheckTriggersTotal *prometheus.CounterVec
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
		Help: "Ledger-state divergences found between dingo and its reference oracle, by field and reference.",
	}, []string{"field", "reference"})
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
	for _, reason := range []string{nodeparity.SkipTipMismatch} {
		checksSkippedTotal.WithLabelValues(reason)
	}
	for _, field := range []string{"protocol_params", "stake_distribution", "utxo"} {
		for _, ref := range []string{ReferenceCardanoNode, ReferenceKoios} {
			divergenceTotal.WithLabelValues(field, ref)
		}
	}
	fullCheckTriggersTotal := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "node_parity_full_check_triggers_total",
		Help: "Incremental mode's full checkpoint comparisons, by trigger reason.",
	}, []string{"reason"})
	for _, reason := range []nodeparity.FullCheckReason{
		nodeparity.FullCheckStartup,
		nodeparity.FullCheckInterval,
		nodeparity.FullCheckEpochTransition,
		nodeparity.FullCheckRollback,
		nodeparity.FullCheckMismatch,
	} {
		fullCheckTriggersTotal.WithLabelValues(string(reason))
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
		incrementalBlocksTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "node_parity_incremental_blocks_total",
			Help: "Blocks validated by incremental mode's per-block UTxO delta check (matched or diverged).",
		}),
		incrementalMismatchTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "node_parity_incremental_mismatch_total",
			Help: "Incremental mode blocks whose per-block delta check found a divergence.",
		}),
		fullCheckTriggersTotal: fullCheckTriggersTotal,
	}
}

// recordSkip increments checksSkippedTotal for a discarded cycle.
func (m *parityMetrics) recordSkip(reason string) {
	m.checksSkippedTotal.WithLabelValues(reason).Inc()
}

// fromGenesisFields are the three checks a from-genesis epoch runs, and the
// only values epochChecksIncompleteTotal's "field" label takes. Same three
// names divergenceTotal's "field" uses, so one dashboard can put "diverged"
// and "could not run" side by side for the same check.
var fromGenesisFields = []string{
	"protocol_params", "stake_distribution", "utxo",
}

// fromGenesisMetrics is from-genesis's own counter set, deliberately NOT
// parityMetrics. The two subcommands measure different things at different
// cadences, and sharing one set made both wrong:
//
//   - checks_total counts check/watch cycles, which fire per block. A
//     from-genesis epoch takes minutes to hours, growing with the UTxO set
//     it has to reconstruct, so folding epochs into that counter made a
//     healthy replay look stalled to NodeParityNotChecking, whose window is
//     sized for block cadence.
//   - checks_skipped_total's "reason" is closed to the tip-sandwich failure
//     mode, and a discarded cycle is a whole cycle. from-genesis has no tip
//     sandwich and fails per field, so putting field names in that label
//     both broke the label's contract and counted one degraded epoch as up
//     to three skipped "cycles".
//
// Registering a separate set also keeps a from-genesis process from
// exposing a permanently-zero checks_total, which NodeParityNotChecking
// would read as a dead tool.
type fromGenesisMetrics struct {
	// epochsTotal counts epochs where at least one of the three checks
	// reached a trustworthy verdict. An epoch whose every check was
	// untrusted verified nothing, so counting it here would inflate the
	// denominator an operator reads as "epochs actually validated".
	epochsTotal prometheus.Counter
	// epochChecksIncompleteTotal counts checks that could not be trusted,
	// by field -- most often Koios being unreachable or rate-limited.
	// Distinct from a divergence: "the reference was unavailable" and
	// "Dingo answered the wrong value" are very different pages.
	epochChecksIncompleteTotal *prometheus.CounterVec
	// divergenceTotal is the same series check/watch use, tagged
	// reference=koios. Shared on purpose: a divergence is a divergence
	// whichever mode found it, and the alert rules key on this name.
	divergenceTotal *prometheus.CounterVec
}

// newFromGenesisMetrics registers from-genesis's counters under a registry
// wrapped with a "network" const label, exactly as newParityMetrics does for
// check/watch.
func newFromGenesisMetrics(network string) *fromGenesisMetrics {
	return newFromGenesisMetricsIn(network, prometheus.DefaultRegisterer)
}

// newFromGenesisMetricsIn is newFromGenesisMetrics with the registerer
// injectable, for the same reason newParityMetricsIn has one: the
// process-wide default allows a metric name to be registered only once.
func newFromGenesisMetricsIn(
	network string, base prometheus.Registerer,
) *fromGenesisMetrics {
	registry := prometheus.WrapRegistererWith(
		prometheus.Labels{"network": network},
		base,
	)
	factory := promauto.With(registry)
	incomplete := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "node_parity_epoch_checks_incomplete_total",
		Help: "from-genesis epoch checks that could not be trusted (most often the Koios reference being unavailable), by field.",
	}, []string{"field"})
	divergenceTotal := factory.NewCounterVec(prometheus.CounterOpts{
		Name: "node_parity_divergence_total",
		Help: "Ledger-state divergences found between dingo and its reference oracle, by field and reference.",
	}, []string{"field", "reference"})
	// Pre-materialize, for the reason newParityMetricsIn documents at
	// length: a CounterVec exposes no series until something increments
	// it, and an alert expression combining two vectors drops any series
	// missing from either side, so an alert on a never-yet-incremented
	// label could never fire.
	for _, field := range fromGenesisFields {
		incomplete.WithLabelValues(field)
		divergenceTotal.WithLabelValues(field, ReferenceKoios)
	}
	return &fromGenesisMetrics{
		epochsTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "node_parity_epochs_total",
			Help: "from-genesis epochs where at least one check reached a trustworthy verdict.",
		}),
		epochChecksIncompleteTotal: incomplete,
		divergenceTotal:            divergenceTotal,
	}
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
		m.divergenceTotal.WithLabelValues("protocol_params", ReferenceCardanoNode).Inc()
	}
	if len(diff.StakeDistribution) > 0 {
		m.divergenceTotal.WithLabelValues("stake_distribution", ReferenceCardanoNode).Inc()
	}
	if len(diff.UTxO) > 0 {
		m.divergenceTotal.WithLabelValues("utxo", ReferenceCardanoNode).Inc()
	}
}

// recordFullCheckTrigger increments fullCheckTriggersTotal for the reason
// incremental mode ran one of its full checkpoint comparisons.
func (m *parityMetrics) recordFullCheckTrigger(reason string) {
	m.fullCheckTriggersTotal.WithLabelValues(reason).Inc()
}

// recordIncrementalBlock increments incrementalBlocksTotal and, if diff
// found a divergence, incrementalMismatchTotal and the same
// divergenceTotal{field} series full-mode checks use -- a mismatch is a
// mismatch regardless of which mode found it, so a dashboard built around
// divergenceTotal alone still sees it.
func (m *parityMetrics) recordIncrementalBlock(diff nodeparity.Diff) {
	m.incrementalBlocksTotal.Inc()
	if diff.Empty() {
		return
	}
	m.incrementalMismatchTotal.Inc()
	if diff.ProtocolParamsDiff != "" {
		m.divergenceTotal.WithLabelValues("protocol_params", ReferenceCardanoNode).Inc()
	}
	if len(diff.StakeDistribution) > 0 {
		m.divergenceTotal.WithLabelValues("stake_distribution", ReferenceCardanoNode).Inc()
	}
	if len(diff.UTxO) > 0 {
		m.divergenceTotal.WithLabelValues("utxo", ReferenceCardanoNode).Inc()
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
