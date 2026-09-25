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
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics exports the koios-parity observer's per-epoch results as Prometheus
// series, so a FAIL/ERROR result is visible without grepping the node's log
// or querying check_epoch_status/check_mismatches directly. Every method
// tolerates a nil *metrics, which is what newMetrics returns when no
// registry is configured.
//
// Every series carries a queue label (ScopeAggregate or ScopeAccount). The
// aggregate and account queues each emit a result for the same epoch, and the
// account queue can fall arbitrarily far behind, so a gauge shared between
// them would let a lagging account-queue PASS overwrite the aggregate queue's
// latest FAIL. An account-queue result re-runs the aggregate phase too, so its
// Status and Mismatches cover both phases, exactly as CheckedScopes says.
type metrics struct {
	epochResultTotal   *prometheus.CounterVec
	mismatchTotal      *prometheus.CounterVec
	lastCheckedEpoch   *prometheus.GaugeVec
	epochMismatchCount *prometheus.GaugeVec
	// lastFailEpoch/lastErrorEpoch are never reset by a later PASS: an
	// undiagnosed mismatch must not disappear from a dashboard because a
	// later epoch happened to pass.
	lastFailEpoch  *prometheus.GaugeVec
	lastErrorEpoch *prometheus.GaugeVec
}

// newMetrics registers the observer's collectors with promRegistry, or returns
// nil when promRegistry is nil. startKoiosParityObserver runs again against
// the same registry on a live restore/truncate, so a collector that is
// already registered is reused rather than treated as an error.
func newMetrics(promRegistry prometheus.Registerer) *metrics {
	if promRegistry == nil {
		return nil
	}
	return &metrics{
		epochResultTotal: registerCollector(
			promRegistry,
			prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: "dingo_koiosparity_epoch_result_total",
					Help: "koios-parity epoch validation results, by network, queue and status (pass/fail/error)",
				},
				[]string{"network", "queue", "status"},
			),
		),
		mismatchTotal: registerCollector(promRegistry, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_koiosparity_mismatch_total",
				Help: "koios-parity mismatch rows recorded, by network, queue, category and severity (fail/error/informational)",
			},
			[]string{"network", "queue", "category", "severity"},
		)),
		lastCheckedEpoch: registerCollector(
			promRegistry,
			prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "dingo_koiosparity_last_checked_epoch",
					Help: "most recent epoch the koios-parity observer has completed a check for, by network and queue",
				},
				[]string{"network", "queue"},
			),
		),
		epochMismatchCount: registerCollector(
			promRegistry,
			prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: "dingo_koiosparity_epoch_mismatch_count",
					Help: "mismatch row count for the most recently checked epoch, by network and queue (mirrors check_epoch_status.mismatch_count)",
				},
				[]string{"network", "queue"},
			),
		),
		lastFailEpoch: registerCollector(promRegistry, prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_koiosparity_last_fail_epoch",
				Help: "epoch number of the last koios-parity result with status FAIL, by network and queue; does not reset on a later PASS",
			},
			[]string{"network", "queue"},
		)),
		lastErrorEpoch: registerCollector(promRegistry, prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "dingo_koiosparity_last_error_epoch",
				Help: "epoch number of the last koios-parity result with status ERROR, by network and queue; does not reset on a later PASS",
			},
			[]string{"network", "queue"},
		)),
	}
}

// registerCollector registers c, or returns the collector already registered
// under the same descriptor. Any other registration error means c's
// descriptor conflicts with an unrelated collector, which is a programming
// error, so it panics as promauto would.
func registerCollector[C prometheus.Collector](
	promRegistry prometheus.Registerer,
	c C,
) C {
	err := promRegistry.Register(c)
	if err == nil {
		return c
	}
	if are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); ok {
		if existing, ok := are.ExistingCollector.(C); ok {
			return existing
		}
	}
	panic(err)
}

// resultQueue names the observer queue that produced result: only the account
// queue's results cover ScopeAccount.
func resultQueue(result *EpochCompareResult) string {
	if result.CoversScope(ScopeAccount) {
		return ScopeAccount
	}
	return ScopeAggregate
}

// recordResult updates every series from one completed result. It is called
// only from Observer.emitResult, which every result from both queues and from
// reportError passes through. Safe on a nil receiver and a nil result.
func (m *metrics) recordResult(result *EpochCompareResult) {
	if m == nil || result == nil {
		return
	}
	queue := resultQueue(result)
	m.epochResultTotal.WithLabelValues(
		result.Network,
		queue,
		strings.ToLower(result.Status),
	).Inc()
	m.lastCheckedEpoch.WithLabelValues(result.Network, queue).
		Set(float64(result.Epoch))
	m.epochMismatchCount.WithLabelValues(result.Network, queue).
		Set(float64(len(result.Mismatches)))
	for _, mm := range result.Mismatches {
		m.mismatchTotal.WithLabelValues(
			result.Network,
			queue,
			mm.Category,
			severityLabel(severityOf(mm.Category)),
		).Inc()
	}
	switch result.Status {
	case StatusFail:
		m.lastFailEpoch.WithLabelValues(result.Network, queue).
			Set(float64(result.Epoch))
	case StatusError:
		m.lastErrorEpoch.WithLabelValues(result.Network, queue).
			Set(float64(result.Epoch))
	}
}
