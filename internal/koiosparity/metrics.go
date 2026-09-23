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

package koiosparity

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics exports the koios-parity observer's per-epoch results as
// Prometheus series (dingo #4681): before this, a FAIL/ERROR result was only
// visible by grepping the node's log or querying check_epoch_status/
// check_mismatches directly. Mirrors ledger's stateMetrics pattern: a plain
// struct of collectors, an init/constructor that registers them with
// promauto, and a receiver whose methods tolerate a nil *metrics so a caller
// that never wires a registry (unit tests, the standalone koios-parity CLI,
// an ObserverConfig with no PromRegistry) never nil-panics.
type metrics struct {
	// epochResultTotal counts one per completed CheckEpoch call (both the
	// success path and reportError's synthetic ERROR), by status
	// (pass/fail/error, lowercased to match Prometheus label-value
	// convention).
	epochResultTotal *prometheus.CounterVec
	// mismatchTotal counts one per mismatch row in a result's Mismatches, by
	// category and severity -- the same check_mismatches.category breakdown
	// the cache already persists, graphable without querying it directly.
	mismatchTotal *prometheus.CounterVec
	// lastCheckedEpoch is set to the epoch number on every completed check
	// (pass, fail, or error), so "is this still checking new epochs" is
	// answerable from Grafana alone.
	lastCheckedEpoch *prometheus.GaugeVec
	// epochMismatchCount is set to len(result.Mismatches) for the most
	// recently checked epoch, mirroring check_epoch_status.mismatch_count.
	epochMismatchCount *prometheus.GaugeVec
	// lastFailEpoch/lastErrorEpoch are set to the epoch number the last time
	// that epoch's status was FAIL/ERROR respectively. Deliberately never
	// reset on a later PASS: these are "when did this last happen"
	// indicators, not the current epoch's verdict, so a stale mismatch that
	// has not been diagnosed yet must not disappear from the dashboard
	// merely because a later epoch happened to pass.
	lastFailEpoch  *prometheus.GaugeVec
	lastErrorEpoch *prometheus.GaugeVec
}

// newMetrics constructs and registers a metrics value against promRegistry.
// promRegistry may be nil -- the standalone koios-parity CLI and any test
// that builds an Observer/ObserverConfig without setting PromRegistry -- in
// which case newMetrics returns nil rather than registering anything, and
// every recordResult call against that nil value is a safe no-op (see
// recordResult).
func newMetrics(promRegistry prometheus.Registerer) *metrics {
	if promRegistry == nil {
		return nil
	}
	factory := promauto.With(promRegistry)
	m := &metrics{}
	m.epochResultTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_koiosparity_epoch_result_total",
			Help: "koios-parity epoch validation results, by network and status (pass/fail/error)",
		},
		[]string{"network", "status"},
	)
	m.mismatchTotal = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_koiosparity_mismatch_total",
			Help: "koios-parity mismatch rows recorded, by network, category and severity (fail/error/informational -- see severityOf)",
		},
		[]string{"network", "category", "severity"},
	)
	m.lastCheckedEpoch = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dingo_koiosparity_last_checked_epoch",
			Help: "most recent epoch the koios-parity observer has completed a check for, by network",
		},
		[]string{"network"},
	)
	m.epochMismatchCount = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dingo_koiosparity_epoch_mismatch_count",
			Help: "mismatch row count for the most recently checked epoch, by network (mirrors check_epoch_status.mismatch_count)",
		},
		[]string{"network"},
	)
	m.lastFailEpoch = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dingo_koiosparity_last_fail_epoch",
			Help: "epoch number of the last koios-parity result with status FAIL, by network; does not reset on a later PASS",
		},
		[]string{"network"},
	)
	m.lastErrorEpoch = factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dingo_koiosparity_last_error_epoch",
			Help: "epoch number of the last koios-parity result with status ERROR, by network; does not reset on a later PASS",
		},
		[]string{"network"},
	)
	return m
}

// recordResult updates every metric above from one completed epoch result.
// The sole place observer.go calls into this package's metrics -- see
// Observer.emitResult, the single choke point every OnResult call already
// goes through (processEpoch's success path, processAccountEpoch's success
// path, and reportError's synthesized ERROR path), so this covers all three
// without scattering individual metric updates across observer.go.
//
// Safe to call on a nil *metrics (PromRegistry was never configured) and
// with a nil result.
func (m *metrics) recordResult(result *EpochCompareResult) {
	if m == nil || result == nil {
		return
	}
	status := strings.ToLower(result.Status)
	if m.epochResultTotal != nil {
		m.epochResultTotal.WithLabelValues(result.Network, status).Inc()
	}
	if m.lastCheckedEpoch != nil {
		m.lastCheckedEpoch.WithLabelValues(result.Network).
			Set(float64(result.Epoch))
	}
	if m.epochMismatchCount != nil {
		m.epochMismatchCount.WithLabelValues(result.Network).
			Set(float64(len(result.Mismatches)))
	}
	if m.mismatchTotal != nil {
		for _, mm := range result.Mismatches {
			m.mismatchTotal.WithLabelValues(
				result.Network,
				mm.Category,
				severityLabel(severityOf(mm.Category)),
			).Inc()
		}
	}
	switch result.Status {
	case StatusFail:
		if m.lastFailEpoch != nil {
			m.lastFailEpoch.WithLabelValues(result.Network).
				Set(float64(result.Epoch))
		}
	case StatusError:
		if m.lastErrorEpoch != nil {
			m.lastErrorEpoch.WithLabelValues(result.Network).
				Set(float64(result.Epoch))
		}
	}
}
