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
	"errors"
	"time"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics reports how far the IndexerDB trails the ledger.
type Metrics struct {
	tipSlot    prometheus.Gauge
	lagBlocks  prometheus.Gauge
	lagSeconds prometheus.Gauge
	// noAncestor counts rollbacks the writer refused because no checkpoint
	// was left on the canonical chain; each one stops the writer.
	noAncestor prometheus.Counter
	// repeated is the number of consecutive writer passes that failed with
	// the same error.
	repeated prometheus.Gauge
	slotTime func(uint64) (time.Time, error)
}

// NewMetrics registers the indexer gauges on reg, which may be nil. slotTime
// converts a slot to wall-clock time for the lag in seconds; without it the
// lag in seconds is the lag in slots, one second each.
func NewMetrics(
	reg prometheus.Registerer,
	slotTime func(uint64) (time.Time, error),
) (*Metrics, error) {
	m := &Metrics{
		slotTime: slotTime,
		tipSlot: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_indexer_tip_slot",
			Help: "Slot of the last block the IndexerDB has indexed.",
		}),
		lagBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_indexer_lag_blocks",
			Help: "Blocks the IndexerDB trails the ledger tip by.",
		}),
		lagSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_indexer_lag_seconds",
			Help: "Seconds of chain time the IndexerDB trails the ledger tip by.",
		}),
		repeated: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_indexer_repeated_failures",
			Help: "Consecutive writer passes that failed with the same error.",
		}),
		noAncestor: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "dingo_indexer_no_rollback_ancestor_total",
			Help: "Rollbacks refused because no checkpoint remained on the canonical chain.",
		}),
	}
	if reg == nil {
		return m, nil
	}
	for _, g := range []*prometheus.Gauge{
		&m.tipSlot, &m.lagBlocks, &m.lagSeconds, &m.repeated,
	} {
		if err := reg.Register(*g); err != nil {
			are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
			if !ok {
				return nil, err
			}
			*g = are.ExistingCollector.(prometheus.Gauge)
		}
	}
	if err := reg.Register(m.noAncestor); err != nil {
		are, ok := errors.AsType[prometheus.AlreadyRegisteredError](err)
		if !ok {
			return nil, err
		}
		m.noAncestor = are.ExistingCollector.(prometheus.Counter)
	}
	return m, nil
}

// RepeatedFailures records how many consecutive writer passes failed with the
// same error.
func (m *Metrics) RepeatedFailures(count int) {
	m.repeated.Set(float64(count))
}

// NoRollbackAncestor records a rollback refused for lack of an ancestor.
func (m *Metrics) NoRollbackAncestor() {
	m.noAncestor.Inc()
}

// Observe records the tips a writer pass ended on.
func (m *Metrics) Observe(tip indexerdb.Tip, ledgerTip ochainsync.Tip) {
	m.tipSlot.Set(float64(tip.Slot))
	var blocks uint64
	if ledgerTip.BlockNumber > tip.BlockNumber {
		blocks = ledgerTip.BlockNumber - tip.BlockNumber
	}
	m.lagBlocks.Set(float64(blocks))
	m.lagSeconds.Set(m.lagSecondsBetween(tip.Slot, ledgerTip.Point.Slot))
}

func (m *Metrics) lagSecondsBetween(indexed, ledger uint64) float64 {
	if ledger <= indexed {
		return 0
	}
	if m.slotTime != nil {
		from, errFrom := m.slotTime(indexed)
		to, errTo := m.slotTime(ledger)
		if errFrom == nil && errTo == nil {
			return to.Sub(from).Seconds()
		}
	}
	return float64(ledger - indexed)
}
