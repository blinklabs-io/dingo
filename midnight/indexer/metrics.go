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

package indexer

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// indexerMetrics holds the Prometheus instruments for the Midnight indexer's
// block-scan path.
type indexerMetrics struct {
	blocksIndexed prometheus.Counter
	eventsTotal   *prometheus.CounterVec
}

// newIndexerMetrics registers the indexer's counters against reg. reg may be
// nil (promauto skips registration but still returns usable instruments), or
// it may be node.go's rebuildableRegisterer wrapper -- a live restore/
// truncate reconstructs the indexer via New, and node_lifecycle.go's
// unregisterAll() runs first, so the re-registration here never collides
// with the previous instance's collectors (see metrics_registerer.go).
func newIndexerMetrics(reg prometheus.Registerer) *indexerMetrics {
	factory := promauto.With(reg)
	return &indexerMetrics{
		blocksIndexed: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_midnight_blocks_indexed_total",
			Help: "total blocks processed by the Midnight indexer",
		}),
		eventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_midnight_events_total",
			Help: "total Midnight events written, by type (create, spend, registration, deregistration)",
		}, []string{"type"}),
	}
}

// recordBlockEvents applies one committed block's tallies: one blocksIndexed
// increment, plus each entry in counts added to eventsTotal by type. Called
// only after processBlock's transaction commits, so a block that fails and
// rolls back never contributes to either metric -- counts is a plain map
// accumulated during scanning specifically so nothing touches Prometheus
// until the block's rows are actually durable.
func (m *indexerMetrics) recordBlockEvents(counts map[string]int) {
	m.blocksIndexed.Inc()
	for eventType, n := range counts {
		if n <= 0 {
			continue
		}
		m.eventsTotal.WithLabelValues(eventType).Add(float64(n))
	}
}
