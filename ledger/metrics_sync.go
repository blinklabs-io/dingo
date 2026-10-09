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

package ledger

import (
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The sync-progress metrics answer "is the node fetching, and if not, what is
// the scheduler waiting on". Scheduler state is exported from atomic mirrors
// written next to each mutation rather than read at scrape time: the
// underlying fields are guarded by chainsyncBlockfetchMutex (and, for the
// ready channel, a second mutex that not every writer takes), and a scrape
// that waited on those locks would go silent in exactly the stall it exists
// to diagnose.

// setBatchInFlight records whether a blockfetch batch is in flight, mirroring
// chainsyncBlockfetchReadyChan != nil.
func (m *stateMetrics) setBatchInFlight(inFlight bool) {
	if m == nil {
		return
	}
	m.blockfetchBatchInFlight.Store(inFlight)
}

// setContinuationPending mirrors blockfetchContinuationPending.
func (m *stateMetrics) setContinuationPending(pending bool) {
	if m == nil {
		return
	}
	m.blockfetchContinuationPending.Store(pending)
}

// setBlockfetchSelected mirrors whether selectedBlockfetchConnId names a
// connection.
func (m *stateMetrics) setBlockfetchSelected(selected bool) {
	if m == nil {
		return
	}
	m.blockfetchConnSelected.Store(selected)
}

// markBlockAdded records that a block was just added to the chain. It is one
// atomic store, so it is safe on the per-block path.
func (m *stateMetrics) markBlockAdded() {
	if m == nil {
		return
	}
	m.lastBlockAddedUnixNano.Store(time.Now().UnixNano())
}

// incHeaderQueueFull counts one header rejected because the chain's header
// queue was full.
func (m *stateMetrics) incHeaderQueueFull() {
	if m == nil || m.headerQueueFull == nil {
		return
	}
	m.headerQueueFull.Inc()
}

// setSelectedBlockfetchConnId is the only writer of selectedBlockfetchConnId,
// so the exported has-selection gauge cannot drift from the field. The caller
// holds the same lock it held for the plain assignment.
func (ls *LedgerState) setSelectedBlockfetchConnId(
	connId ouroboros.ConnectionId,
) {
	ls.selectedBlockfetchConnId = connId
	ls.metrics.setBlockfetchSelected(connIdKey(connId) != "")
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// registerSyncGauges registers the header-queue and blockfetch-scheduler
// gauges. The header-queue gauges read the chain at scrape time: the length
// takes only the chain's lock and the capacity only the chain manager's, and
// neither is held across the other or across any ledger lock. None of the
// series is labelled by connection or peer.
func (ls *LedgerState) registerSyncGauges(registerer prometheus.Registerer) {
	factory := promauto.With(registerer)
	m := &ls.metrics
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_chain_header_queue_length",
			Help: "headers queued on the primary chain awaiting their block bodies",
		},
		func() float64 {
			if ls.chain == nil {
				return 0
			}
			return float64(ls.chain.HeaderCount())
		},
	)
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_chain_header_queue_capacity",
			Help: "maximum headers the primary chain's header queue accepts before rejecting with a queue-full error",
		},
		func() float64 {
			if ls.chain == nil {
				return 0
			}
			return float64(ls.chain.MaxQueuedHeaders())
		},
	)
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_last_block_added_timestamp_seconds",
			Help: "unix time a fetched block was last added to the chain, or 0 when none has been since start",
		},
		func() float64 {
			nanos := m.lastBlockAddedUnixNano.Load()
			if nanos == 0 {
				return 0
			}
			return float64(nanos) / float64(time.Second)
		},
	)
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_blockfetch_batch_in_flight",
			Help: "1 while a blockfetch batch is in flight, else 0",
		},
		func() float64 { return boolGauge(m.blockfetchBatchInFlight.Load()) },
	)
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_blockfetch_continuation_pending",
			Help: "1 while a blockfetch continuation is scheduled but has not yet started, else 0",
		},
		func() float64 {
			return boolGauge(m.blockfetchContinuationPending.Load())
		},
	)
	factory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_blockfetch_connection_selected",
			Help: "1 while the blockfetch scheduler has a connection selected for the next batch, else 0",
		},
		func() float64 { return boolGauge(m.blockfetchConnSelected.Load()) },
	)
}
