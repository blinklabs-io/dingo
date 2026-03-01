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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package forging

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// forgingMetrics holds Prometheus metrics for block forging and KES
// lifecycle monitoring. Metric names match cardano-node where a
// direct equivalent exists, enabling reuse of existing SPO
// dashboards.
type forgingMetrics struct {
	// KES lifecycle (match cardano-node names)
	currentKESPeriod    prometheus.Gauge
	remainingKESPeriods prometheus.Gauge
	opCertStartKES      prometheus.Gauge
	opCertExpiryKES     prometheus.Gauge

	// Forge outcomes (match cardano-node Forge_* names)
	forgeAboutToLead  prometheus.Counter
	forgeNodeIsLeader prometheus.Counter
	forgeNotLeader    prometheus.Counter
	forgeForged       prometheus.Counter
	forgeAdopted      prometheus.Counter
	forgeCouldNot     prometheus.Counter

	// Dingo-specific (no cardano-node equivalent)
	slotBattlesTotal prometheus.Counter
	blockSizeBytes   prometheus.Histogram
	blockTxCount     prometheus.Histogram
	forgeSyncSkip    prometheus.Counter
	slotClockErrors  prometheus.Counter
	tipGapSlots      prometheus.Gauge
}

// initForgingMetrics initializes all forging metrics using the
// provided Prometheus registerer.
func initForgingMetrics(
	reg prometheus.Registerer,
) *forgingMetrics {
	factory := promauto.With(reg)
	m := &forgingMetrics{}

	// KES lifecycle — names match cardano-node
	m.currentKESPeriod = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_currentKESPeriod_int",
			Help: "current KES period",
		},
	)
	m.remainingKESPeriods = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_remainingKESPeriods_int",
			Help: "KES periods remaining before OpCert expires",
		},
	)
	m.opCertStartKES = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_operationalCertificateStartKESPeriod_int",
			Help: "KES period when the operational certificate was issued",
		},
	)
	m.opCertExpiryKES = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "cardano_node_metrics_operationalCertificateExpiryKESPeriod_int",
			Help: "KES period when the operational certificate expires",
		},
	)

	// Forge outcomes — names match cardano-node Forge_* counters
	m.forgeAboutToLead = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_Forge_about_to_lead_int",
			Help: "slots where this node checked leadership",
		},
	)
	m.forgeNodeIsLeader = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_Forge_node_is_leader_int",
			Help: "slots where this node was the slot leader",
		},
	)
	m.forgeNotLeader = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_Forge_node_not_leader_int",
			Help: "slots where this node was not the slot leader",
		},
	)
	m.forgeForged = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_Forge_forged_int",
			Help: "blocks successfully forged",
		},
	)
	m.forgeAdopted = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_Forge_adopted_int",
			Help: "forged blocks adopted onto the chain",
		},
	)
	m.forgeCouldNot = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_Forge_could_not_forge_int",
			Help: "slots where forging failed (syncing, build error, etc)",
		},
	)

	// Dingo-specific metrics
	m.slotBattlesTotal = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "cardano_node_metrics_slotBattlesTotal_int",
			Help: "slot battles detected (competing blocks at same slot)",
		},
	)
	m.blockSizeBytes = factory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "cardano_node_metrics_forgedBlockSize_bytes",
			Help: "size of forged block bodies in bytes",
			Buckets: prometheus.ExponentialBuckets(
				256, 2, 14,
			), // 256B to ~2MB
		},
	)
	m.blockTxCount = factory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "cardano_node_metrics_forgedBlockTxCount_int",
			Help: "number of transactions in forged blocks",
			Buckets: prometheus.LinearBuckets(
				0, 10, 20,
			), // 0, 10, 20, ..., 190
		},
	)
	m.forgeSyncSkip = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_forge_sync_skip_total",
			Help: "forging attempts skipped because upstream tip is ahead",
		},
	)
	m.slotClockErrors = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_forge_slot_clock_errors_total",
			Help: "errors reading slot clock for forging",
		},
	)
	m.tipGapSlots = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_forge_tip_gap_slots",
			Help: "latest observed tip gap in slots",
		},
	)

	return m
}
