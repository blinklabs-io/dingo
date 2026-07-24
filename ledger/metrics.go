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

package ledger

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type stateMetrics struct {
	blockNum            prometheus.Gauge
	density             prometheus.Gauge
	epochNum            prometheus.Gauge
	slotInEpoch         prometheus.Gauge
	slotNum             prometheus.Gauge
	forks               prometheus.Gauge
	slotClockFallbacks  prometheus.Counter
	blocksForgedTotal   prometheus.Counter
	blockForgingLatency prometheus.Histogram
	forgingEnabled      prometheus.Gauge
	nodeStartTime       prometheus.Gauge
	tipGapSlots         prometheus.Gauge
	shelleyStartTime    prometheus.Gauge
	epochLengthSlots    prometheus.Gauge
	shadowGateDecisions *prometheus.CounterVec
	// Incremented when a stored governance proposal's CBOR fails to
	// decode during the mid-epoch ratifiability check, so the failures
	// surface as a metric instead of just log volume.
	governanceProposalDecodeFailures prometheus.Counter
	// Incremented when a peer repeatedly asks us to roll back to a point
	// we cannot cross to (local chain diverged), so a stuck node surfaces
	// as a metric instead of only a WARN loop. See issue #2728.
	unrecoverableRollbacks prometheus.Counter
	// Incremented when at-tip validation recovery detects a non-converging,
	// descending series of distinct failures and holds at the ledger tip
	// instead of rewinding the primary chain ever deeper. A rising value
	// means local ledger validation is diverging from the network (e.g. a
	// false-positive validation rejection), not a peer/fork problem. See
	// issue #2939.
	atTipRecoveryNonConverging prometheus.Counter
}

func (m *stateMetrics) init(promRegistry prometheus.Registerer) {
	promautoFactory := promauto.With(promRegistry)
	m.blockNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_blockNum_int",
		Help: "current block number",
	})
	m.density = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_density_real",
		Help: "chain density",
	})
	m.epochNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_epoch_int",
		Help: "current epoch number",
	})
	m.slotInEpoch = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_slotInEpoch_int",
		Help: "current relative slot number in epoch",
	})
	m.slotNum = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_slotNum_int",
		Help: "current slot number",
	})
	m.forks = promautoFactory.NewGauge(prometheus.GaugeOpts{
		Name: "cardano_node_metrics_forks_int",
		Help: "number of forks seen",
	})
	m.slotClockFallbacks = promautoFactory.NewCounter(prometheus.CounterOpts{
		Name: "dingo_ledger_slot_clock_fallback_total",
		Help: "number of ledger slot clock fallbacks to the current tip",
	})
	m.blocksForgedTotal = promautoFactory.NewCounter(prometheus.CounterOpts{
		Name: "cardano_node_metrics_blocksForgedNum_int",
		Help: "total number of blocks forged by this node",
	})
	m.blockForgingLatency = promautoFactory.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "dingo_metrics_blockForgingLatency_seconds",
			Help:    "latency of block forging from slot start to block completion",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // 1ms to ~16s
		},
	)
	m.forgingEnabled = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_forging_enabled",
			Help: "whether block forging is enabled (0 or 1)",
		},
	)
	m.nodeStartTime = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_nodeStartTime_int",
			Help: "unix timestamp when the node started",
		},
	)
	m.tipGapSlots = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_tip_gap_slots",
			Help: "slots between wall-clock slot and chain tip",
		},
	)
	m.shelleyStartTime = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_shelley_start_time",
			Help: "Shelley genesis start as unix timestamp",
		},
	)
	m.epochLengthSlots = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_epoch_length_slots",
			Help: "slots per epoch for the current network",
		},
	)
	// Shadow blockfetch gate decisions, labelled by the path taken:
	//   path="dispatched"        — primary slow, shadow sent
	//   path="skipped_fast"      — primary under cutoff, shadow suppressed
	//   path="skipped_no_sample" — primary has no EWMA yet (cold connection)
	// And cutoff="median" (population-based) or cutoff="fallback"
	// (fixed shadowBlockfetchPrimarySlowThreshold). The fallback ratio
	// over total decisions is the "is the median path firing" signal.
	m.shadowGateDecisions = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_blockfetch_shadow_gate_decisions_total",
			Help: "shadow blockfetch gate decisions, by path and cutoff source",
		},
		[]string{"path", "cutoff"},
	)
	m.governanceProposalDecodeFailures = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_governance_proposal_decode_failures_total",
			Help: "stored governance proposals whose CBOR failed to decode during ratifiability checks",
		},
	)
	m.unrecoverableRollbacks = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_chainsync_unrecoverable_rollback_total",
			Help: "times a peer repeatedly requested a rollback we cannot cross to (local chain diverged, operator intervention required)",
		},
	)
	m.atTipRecoveryNonConverging = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_attip_recovery_nonconverging_total",
			Help: "times at-tip validation recovery held at the ledger tip instead of rewinding the primary chain deeper, because a descending series of distinct failures indicated local validation divergence (operator intervention required)",
		},
	)
}
