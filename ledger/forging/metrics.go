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
	// Leader checks refused because this node's own views of its chain did not
	// agree, or because the newest block it holds was stale. Five reasons,
	// each from a different pair of inputs:
	//
	//   - "slot_gap": the ledger-applied tip trails this node's header
	//     frontier by more than ForgeHeaderFrontierToleranceSlots. Inputs:
	//     applied tip slot, frontier slot.
	//   - "frontier_hash_diverged": frontier and applied tip sit at the SAME
	//     slot but name different blocks -- an equal-slot fork the ledger has
	//     not applied. Inputs: applied tip hash, frontier hash.
	//   - "frontier_behind_applied": the frontier is at a LOWER slot than the
	//     applied tip, so the parent the builder would use is one the ledger
	//     has already built past. Inputs: applied tip slot, frontier slot.
	//   - "eb_manifest_ahead": the headers alone looked fine, and only a
	//     corroborated Leios endorser block pushed the gap over the
	//     tolerance -- proof a ranking block exists at a slot whose header
	//     this node has not admitted. Inputs: applied tip slot, frontier
	//     slot, highest corroborated endorser-block slot.
	//   - "applied_tip_stale": the newest block this node holds by ANY
	//     evidence is too old, with the local tips in agreement. Two
	//     independent sources: trailing the corroborated upstream target (or,
	//     when no sync target is published, the admitted-header frontier) by
	//     more than ForgeUpstreamStalenessSlots, or -- only when an operator
	//     sets ForgeAppliedTipStalenessSlots -- trailing the current
	//     wall-clock slot by more than that bound. This is the reason that
	//     fires when header admission and ledger application stall TOGETHER,
	//     which is exactly when every gap above reads 0.
	//
	// Counted only on slots this node was actually elected to forge, so the
	// value is lost blocks rather than leader checks. Any increment except
	// "applied_tip_stale" means the ledger pipeline, not the network, was the
	// thing behind; "applied_tip_stale" means this node was behind the
	// network as a whole. See ARCHITECTURE.md, "Block Production".
	forgeStaleTipSkip *prometheus.CounterVec
	// Pre-materialized children for the reason label values, so the leader
	// check does not resolve a label on every skip and neither series is
	// absent from a dashboard before the first skip.
	forgeStaleTipSkipSlotGap        prometheus.Counter
	forgeStaleTipSkipHashDiverged   prometheus.Counter
	forgeStaleTipSkipFrontierBehind prometheus.Counter
	forgeStaleTipSkipAppliedStale   prometheus.Counter
	forgeStaleTipSkipEbAhead        prometheus.Counter
	slotClockErrors                 prometheus.Counter
	tipGapSlots                     prometheus.Gauge

	// Slots refused by the persisted last-forged-slot fence. Any
	// increment means the node was asked to forge a slot it had
	// already committed to, which points at a slot-clock regression
	// or a rolled-back database rather than normal operation.
	forgeFenceBlocked prometheus.Counter

	// Self-validation before adoption (optional, nil when disabled)
	forgeValidationDuration prometheus.Histogram
	forgeValidationFailed   prometheus.Counter

	// Panics recovered from pluggable forging callbacks (leader
	// selection, validation, publication), by phase.
	forgePanicRecovered *prometheus.CounterVec

	// Leios EB forging outcomes
	leiosEbForged  prometheus.Counter
	leiosEbSkipped *prometheus.CounterVec
	leiosEbFailed  prometheus.Counter
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
			Name: "dingo_metrics_slotBattlesTotal_int",
			Help: "slot battles detected (competing blocks at same slot)",
		},
	)
	m.forgeFenceBlocked = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_metrics_forgeFenceBlocked_int",
			Help: "forge attempts refused by the last-forged-slot fence",
		},
	)
	m.blockSizeBytes = factory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "dingo_metrics_forgedBlockSize_bytes",
			Help: "size of forged block bodies in bytes",
			Buckets: prometheus.ExponentialBuckets(
				256, 2, 14,
			), // 256B to ~2MB
		},
	)
	m.blockTxCount = factory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "dingo_metrics_forgedBlockTxCount_int",
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
	m.forgeStaleTipSkip = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_forge_stale_tip_skip_total",
			Help: "forging attempts skipped because this node's ledger-applied tip, its own header frontier and its corroborated endorser-block evidence did not describe the same chain position, or because the newest block it holds was stale; by reason (slot_gap, frontier_hash_diverged, frontier_behind_applied, eb_manifest_ahead, applied_tip_stale)",
		},
		[]string{"reason"},
	)
	m.forgeStaleTipSkipSlotGap = m.forgeStaleTipSkip.WithLabelValues(
		forgeStaleTipReasonSlotGap,
	)
	m.forgeStaleTipSkipHashDiverged = m.forgeStaleTipSkip.WithLabelValues(
		forgeStaleTipReasonHashDiverged,
	)
	m.forgeStaleTipSkipFrontierBehind = m.forgeStaleTipSkip.WithLabelValues(
		forgeStaleTipReasonFrontierBehind,
	)
	m.forgeStaleTipSkipAppliedStale = m.forgeStaleTipSkip.WithLabelValues(
		forgeStaleTipReasonAppliedStale,
	)
	m.forgeStaleTipSkipEbAhead = m.forgeStaleTipSkip.WithLabelValues(
		forgeStaleTipReasonEbManifestAhead,
	)
	m.tipGapSlots = factory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_forge_tip_gap_slots",
			Help: "ledger-apply backlog in slots at the last leader check (header frontier minus ledger-applied tip)",
		},
	)
	m.forgeValidationDuration = factory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "dingo_forge_validation_duration_seconds",
			Help: "wall-clock time spent in forged-block self-validation (header crypto, body hash, per-tx); only recorded when validation is enabled",
			Buckets: prometheus.ExponentialBuckets(
				0.001, 2, 12,
			), // 1ms to ~4s
		},
	)
	m.forgeValidationFailed = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_forge_validation_failed_total",
			Help: "forged blocks dropped because self-validation failed",
		},
	)
	m.forgePanicRecovered = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_forge_panic_recovered_total",
			Help: "panics recovered from pluggable forging callbacks, by phase",
		},
		[]string{"phase"},
	)
	m.leiosEbForged = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_metrics_leios_forge_eb_forged_total",
			Help: "endorser blocks successfully forged and broadcast",
		},
	)
	m.leiosEbSkipped = factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_metrics_leios_forge_eb_skipped_total",
			Help: "endorser block production attempts skipped, by reason",
		},
		[]string{"reason"},
	)
	m.leiosEbFailed = factory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_metrics_leios_forge_eb_failed_total",
			Help: "endorser block production attempts that returned an error",
		},
	)

	return m
}
