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
//
// Both counters are cumulative processing counts, not a live view of what
// midnight_* rows currently exist in the database, and are never
// decremented -- see recordBlockEvents and rollbackBlock.
func newIndexerMetrics(reg prometheus.Registerer) *indexerMetrics {
	factory := promauto.With(reg)
	return &indexerMetrics{
		blocksIndexed: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_midnight_blocks_indexed_total",
			Help: "cumulative blocks committed by the Midnight indexer; not decremented when a later chain reorg rolls one back (see rollbackBlock)",
		}),
		eventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_midnight_events_total",
			Help: "cumulative Midnight events committed, by type (create, spend, registration, deregistration); not decremented on chain-reorg rollback, and may include idempotent-replay recounts after a crash restart (see processTx/processOutput)",
		}, []string{"type"}),
	}
}

// recordBlockEvents applies one committed block's tallies: one blocksIndexed
// increment, plus each entry in counts added to eventsTotal by type. Called
// only after processBlock's transaction commits, so a block that fails and
// rolls back *within processBlock itself* (a write later in the same block
// failed) never contributes to either metric -- counts is a plain map
// accumulated during scanning specifically so nothing touches Prometheus
// until the block's rows are actually durable.
//
// This is a different rollback than rollbackBlock's chain-reorg undo, which
// runs after this function already ran for the block being undone. Both
// counters are standard monotonic Prometheus _total counters (their value
// is meant to feed rate()/increase(), which assume a counter only goes up --
// an occasional decrease reads as a process restart, not real data), so a
// chain reorg that deletes previously-committed midnight_* rows via
// rollbackBlock intentionally does not decrement either counter here. They
// answer "how much has the indexer ever committed," not "how many midnight_*
// rows exist right now" -- query the tables directly for the latter.
func (m *indexerMetrics) recordBlockEvents(counts map[string]int) {
	m.blocksIndexed.Inc()
	for eventType, n := range counts {
		if n <= 0 {
			continue
		}
		m.eventsTotal.WithLabelValues(eventType).Add(float64(n))
	}
}
