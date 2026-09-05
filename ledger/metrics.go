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
	"time"

	"github.com/blinklabs-io/gouroboros/pipeline"
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
	// Wall-clock time the ledger apply path spent waiting for a referenced
	// Leios endorser block, by outcome ("arrived", "timeout" or "cancelled"). This wait is
	// taken ahead of the batch's DB transaction on the single ledger pipeline,
	// so it is time every block queued behind the batch also spends waiting.
	// Only references ledger application actually reads are waited on (see
	// leiosApplyReadsOwnAnnouncement); the rest are prefetched in the
	// background and never observed here.
	leiosEbWaitSeconds *prometheus.HistogramVec
	// Pre-materialized observers for the outcome label values, so the apply
	// path does not resolve a label on every wait.
	leiosEbWaitArrived   prometheus.Observer
	leiosEbWaitTimedOut  prometheus.Observer
	leiosEbWaitCancelled prometheus.Observer
	// Waits that ran to the full diffusion window without the endorser block
	// arriving. A rising value against a flat leios_eb_wait_seconds "arrived"
	// count means the wait is buying nothing and is pure apply latency.
	// Cancellations are deliberately excluded: they say nothing about
	// endorser-block availability.
	leiosEbWaitTimeouts prometheus.Counter
	// Incremented when a stored governance proposal's CBOR fails to
	// decode during the mid-epoch ratifiability check, so the failures
	// surface as a metric instead of just log volume.
	governanceProposalDecodeFailures prometheus.Counter
	// Incremented when a peer repeatedly asks us to roll back to a point
	// we cannot cross to (local chain diverged), so a stuck node surfaces
	// as a metric instead of only a WARN loop. See issue #2728.
	unrecoverableRollbacks prometheus.Counter
	// Incremented when a chainsync peer asks for a rollback we refuse, but
	// its own advertised tip is a strict ancestor of ours on our primary
	// chain: the peer is behind, not forked, and is kept attached instead
	// of being rejected and denied. A rising value with a flat local tip
	// means our upstreams are lagging us, not that anything diverged.
	chainsyncBehindPeers prometheus.Counter
	// Incremented when at-tip validation recovery detects a non-converging,
	// descending series of distinct failures and holds at the ledger tip
	// instead of rewinding the primary chain ever deeper. A rising value
	// means local ledger validation is diverging from the network (e.g. a
	// false-positive validation rejection), not a peer/fork problem. See
	// issue #2939.
	atTipRecoveryNonConverging prometheus.Counter
	// Incremented when unresolved-producer replay recovery repeatedly fails
	// to move the applied ledger tip forward and holds at that tip instead of
	// pruning another security-parameter window. See issue #3005.
	replayRecoveryNonConverging prometheus.Counter
	// Incremented when the cross-fork continuation audit finds a freshly
	// fetched body spending an input whose producing transaction is not on
	// the local applied chain. A rising value means a peer is feeding the
	// node a continuation from a fork it never applied. See issue #3005.
	continuationInputUnresolved prometheus.Counter
	// Observed for every Praos leader-eligibility decision on an inbound
	// header: (threshold - leaderValue) / threshold. Positive is eligible,
	// and the magnitude is the headroom. dingo derives its leadership stake
	// by independent reimplementation, so its relative stake error is never
	// provably zero, and a threshold comparison turns an error of eps into a
	// flipped decision with probability about eps per block. Recording every
	// decision rather than only the failures is what makes that eps
	// measurable in the field: a stake error clusters decisions near zero,
	// while a derivation bug produces margins that are not marginal.
	leaderThresholdMargin prometheus.Histogram
	// Incremented when a header is rejected because its VRF leader value did
	// not clear the stake-derived threshold. Read alongside the margin
	// histogram: rejections whose margin sits just under zero indicate a
	// stake discrepancy rather than a genuinely ineligible producer.
	leaderThresholdRejections prometheus.Counter
	// Set to the number of consecutive ledger-pipeline restarts that have
	// made no tip progress, and to 1 while that count is past the point
	// where the pipeline is treated as stuck. A deterministic failure (a
	// rejected canonical block, say) repeats forever, so without this a
	// wedged node is visible only as a repeating WARN. See issue #3165.
	pipelineNoProgressRestarts prometheus.Gauge
	pipelineStuck              prometheus.Gauge
	// Set to 1 once the ledger pipeline has stopped retrying altogether.
	// Unlike pipelineStuck this is terminal: the pipeline goroutine has
	// returned and nothing will clear it short of a restart, so it is the
	// signal to alert on for a node that has permanently stopped following
	// the chain. See issue #3261.
	pipelineHalted prometheus.Gauge
	// Incremented when validation recovery declares a failure unrepairable
	// because every rewind target it may legally reach lies inside the
	// Mithril protected window. Each increment is one node that can no
	// longer follow the chain without being re-bootstrapped.
	mithrilTrustWindowUnrepairable prometheus.Counter
	// Incremented for each epoch-boundary reward round that could not be
	// applied because one of its inputs was absent. Every increment leaves
	// reward balances -- and the leadership stake derived from them --
	// permanently short by that epoch's rewards, which is what makes a node
	// reject canonical blocks near the eligibility threshold. A nonzero
	// value on a Mithril-bootstrapped node explains a stake shortfall; a
	// rising value on any node is a live divergence from the network.
	skippedStakeRewardRounds prometheus.Counter
	// Snapshot of gouroboros/pipeline.PipelineMetrics.Stats() for the
	// block-processing pipeline (issue #1894), refreshed after every batch
	// decodeReadChainBatch submits to it. These are gauges rather than
	// counters because the pipeline itself owns the cumulative totals
	// (they can only be Set from a periodic snapshot, not incremented
	// in-place from here); nil when BlockPipelineEnabled is off.
	blockPipelineBlocksDecoded    prometheus.Gauge
	blockPipelineBlocksValidated  prometheus.Gauge
	blockPipelineDecodeErrors     prometheus.Gauge
	blockPipelineValidationErrors prometheus.Gauge
	blockPipelineQueueDepth       prometheus.Gauge
	// blockPipelineExpectedEta0Errors/blockPipelineDeferredEpochCacheErrors/
	// blockPipelineUnexpectedErrors count errors drained from
	// blockPipeline.Errors() by drainBlockPipelineErrors (issue #1894
	// deadlock fix): the eta0 counter tracks errBlockPipelineEta0Unavailable
	// (no cached Praos nonce yet -- normal on every from-genesis sync, since
	// it is how Byron-era slots always fail this lookup, but the same
	// rollover-not-complete condition is not verified to be Byron-specific
	// here); the deferred counter tracks errHeaderVerificationDeferred (the
	// pipeline's epoch cache has not yet caught up with a block already
	// committed to ls.chain -- a transient race, self-healing once the
	// cache advances, per this section's own doc comment); the unexpected
	// counter tracks everything else reaching errorsChan (decode errors,
	// non-Byron validation failures, apply-stage invariant violations),
	// which should stay at 0 in healthy operation. Unlike the *Errors gauges
	// above (owned by the pipeline's own snapshot), these are counters
	// incremented directly as each error is drained.
	blockPipelineExpectedEta0Errors       prometheus.Counter
	blockPipelineDeferredEpochCacheErrors prometheus.Counter
	blockPipelineUnexpectedErrors         prometheus.Counter
}

// The accessors below tolerate an uninitialised stateMetrics. A LedgerState
// built directly -- as the ledger's own unit tests do -- never calls init, so
// its metric fields are nil; instrumenting a path that those tests exercise
// must not turn a metric into a nil dereference.

func (m *stateMetrics) observeLeaderThresholdMargin(margin float64) {
	if m == nil || m.leaderThresholdMargin == nil {
		return
	}
	m.leaderThresholdMargin.Observe(margin)
}

// Outcome label values for dingo_metrics_leios_eb_wait_seconds.
//
//   - arrived:   the endorser block became available during the wait.
//   - timeout:   the diffusion window elapsed without it. This is the outcome
//     that means the wait cost apply latency and bought nothing.
//   - cancelled: the wait ended because the block-processing context was
//     cancelled (node shutdown, or the pass being aborted and restarted).
//     Nothing was learned about the endorser block's availability, so this is
//     kept out of the timeout counter: folding it in would inflate the
//     timeout rate exactly when a node is shutting down or restarting its
//     pipeline, which is when the metric is most likely to be read.
const (
	leiosEbWaitOutcomeArrived   = "arrived"
	leiosEbWaitOutcomeTimeout   = "timeout"
	leiosEbWaitOutcomeCancelled = "cancelled"
)

// observeLeiosEbWait records one apply-path endorser-block wait under the
// given outcome. Recording the duration under every outcome (rather than only
// timeouts) is what makes the metric answer the question that matters: whether
// the wait is delivering endorser blocks or just costing apply latency before
// proceeding without one.
func (m *stateMetrics) observeLeiosEbWait(d time.Duration, outcome string) {
	if m == nil {
		return
	}
	var obs prometheus.Observer
	switch outcome {
	case leiosEbWaitOutcomeArrived:
		obs = m.leiosEbWaitArrived
	case leiosEbWaitOutcomeTimeout:
		obs = m.leiosEbWaitTimedOut
		if m.leiosEbWaitTimeouts != nil {
			m.leiosEbWaitTimeouts.Inc()
		}
	case leiosEbWaitOutcomeCancelled:
		obs = m.leiosEbWaitCancelled
	}
	if obs != nil {
		obs.Observe(d.Seconds())
	}
}

func (m *stateMetrics) incLeaderThresholdRejections() {
	if m == nil || m.leaderThresholdRejections == nil {
		return
	}
	m.leaderThresholdRejections.Inc()
}

func (m *stateMetrics) incSkippedStakeRewardRounds() {
	if m == nil || m.skippedStakeRewardRounds == nil {
		return
	}
	m.skippedStakeRewardRounds.Inc()
}

// incBlockPipelineExpectedEta0Error records a block-processing pipeline
// validate-stage error drained from errorsChan that was classified as the
// expected Byron-era (no Praos epoch nonce) case. See
// errBlockPipelineEta0Unavailable.
func (m *stateMetrics) incBlockPipelineExpectedEta0Error() {
	if m == nil || m.blockPipelineExpectedEta0Errors == nil {
		return
	}
	m.blockPipelineExpectedEta0Errors.Inc()
}

// incBlockPipelineDeferredEpochCacheError records a block-processing
// pipeline validate-stage error drained from errorsChan that was classified
// as the pipeline's epoch cache not yet covering an already-committed
// block's slot (errHeaderVerificationDeferred) -- a transient condition
// that resolves once the epoch cache catches up, not a validation problem.
func (m *stateMetrics) incBlockPipelineDeferredEpochCacheError() {
	if m == nil || m.blockPipelineDeferredEpochCacheErrors == nil {
		return
	}
	m.blockPipelineDeferredEpochCacheErrors.Inc()
}

// incBlockPipelineUnexpectedError records a block-processing pipeline error
// drained from errorsChan that was not one of the expected/transient cases
// above (e.g. a decode error, a non-Byron validation failure, or an
// apply-stage invariant violation).
func (m *stateMetrics) incBlockPipelineUnexpectedError() {
	if m == nil || m.blockPipelineUnexpectedErrors == nil {
		return
	}
	m.blockPipelineUnexpectedErrors.Inc()
}

// setPipelineHalted records that the ledger pipeline has stopped retrying.
// Terminal by design: nothing clears it, because nothing restarts the pipeline.
func (m *stateMetrics) setPipelineHalted() {
	if m == nil || m.pipelineHalted == nil {
		return
	}
	m.pipelineHalted.Set(1)
}

// incMithrilTrustWindowUnrepairable records a validation failure that no legal
// rewind can repair because the trust anchor blocks every deeper target.
func (m *stateMetrics) incMithrilTrustWindowUnrepairable() {
	if m == nil || m.mithrilTrustWindowUnrepairable == nil {
		return
	}
	m.mithrilTrustWindowUnrepairable.Inc()
}

func (m *stateMetrics) setPipelineNoProgress(restarts int, stuck bool) {
	if m == nil {
		return
	}
	if m.pipelineNoProgressRestarts != nil {
		m.pipelineNoProgressRestarts.Set(float64(restarts))
	}
	if m.pipelineStuck != nil {
		stuckValue := float64(0)
		if stuck {
			stuckValue = 1
		}
		m.pipelineStuck.Set(stuckValue)
	}
}

// updateBlockPipelineStats refreshes the block-processing pipeline gauges
// from a PipelineStats snapshot. Safe to call on an uninitialised
// stateMetrics (tests that build a LedgerState without calling init).
func (m *stateMetrics) updateBlockPipelineStats(stats pipeline.PipelineStats) {
	if m == nil {
		return
	}
	if m.blockPipelineBlocksDecoded != nil {
		m.blockPipelineBlocksDecoded.Set(float64(stats.BlocksDecoded))
	}
	if m.blockPipelineBlocksValidated != nil {
		m.blockPipelineBlocksValidated.Set(float64(stats.BlocksValidated))
	}
	if m.blockPipelineDecodeErrors != nil {
		m.blockPipelineDecodeErrors.Set(float64(stats.DecodeErrors))
	}
	if m.blockPipelineValidationErrors != nil {
		m.blockPipelineValidationErrors.Set(float64(stats.ValidationErrors))
	}
	if m.blockPipelineQueueDepth != nil {
		m.blockPipelineQueueDepth.Set(float64(stats.CurrentQueueDepth))
	}
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
	m.leiosEbWaitSeconds = promautoFactory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "dingo_metrics_leios_eb_wait_seconds",
			Help: "wall-clock time the ledger apply path spent waiting for a referenced Leios endorser block, by outcome",
			// 5ms to ~82s: the wait is bounded by the certify-by deadline
			// converted to wall clock, so the useful range spans sub-slot
			// arrivals up to several stacked protocol windows.
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 15),
		},
		[]string{"outcome"},
	)
	m.leiosEbWaitArrived = m.leiosEbWaitSeconds.WithLabelValues(
		leiosEbWaitOutcomeArrived,
	)
	m.leiosEbWaitTimedOut = m.leiosEbWaitSeconds.WithLabelValues(
		leiosEbWaitOutcomeTimeout,
	)
	m.leiosEbWaitCancelled = m.leiosEbWaitSeconds.WithLabelValues(
		leiosEbWaitOutcomeCancelled,
	)
	m.leiosEbWaitTimeouts = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_metrics_leios_eb_wait_timeouts_total",
			Help: "ledger apply-path waits for a referenced Leios endorser block that ran to the full diffusion window without it arriving",
		},
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
	m.chainsyncBehindPeers = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_chainsync_behind_peers_total",
			Help: "times a chainsync peer asked for a rollback past the security parameter while its own tip was a strict ancestor of ours (peer behind on our chain, kept attached rather than denied)",
		},
	)
	m.atTipRecoveryNonConverging = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_attip_recovery_nonconverging_total",
			Help: "times at-tip validation recovery held at the ledger tip instead of rewinding the primary chain deeper, because a descending series of distinct failures indicated local validation divergence (operator intervention required)",
		},
	)
	m.replayRecoveryNonConverging = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_replay_recovery_nonconverging_total",
			Help: "times unresolved-producer replay recovery held at the applied ledger tip because repeated recovery attempts made no forward progress",
		},
	)
	m.continuationInputUnresolved = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_continuation_input_unresolved_total",
			Help: "inputs in freshly fetched continuation blocks whose producing transaction is not on the local applied chain (cross-fork splice indicator)",
		},
	)
	m.leaderThresholdMargin = promautoFactory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "dingo_ledger_leader_threshold_margin",
			Help: "(threshold - VRF leader value) / threshold for every Praos leader-eligibility decision on an inbound header; positive is eligible, and values clustered near zero mean the local stake distribution is close enough to the threshold for a small stake error to flip the decision",
			// Resolution is concentrated around zero: everything this
			// metric exists to detect lives within a fraction of a percent
			// of the boundary, and the bulk far from it carries no signal.
			Buckets: []float64{
				-1, -0.1, -0.01, -0.001, -0.0001, -0.00001,
				0,
				0.00001, 0.0001, 0.001, 0.01, 0.1, 1,
			},
		},
	)
	m.leaderThresholdRejections = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_leader_threshold_rejections_total",
			Help: "headers rejected because the producer's VRF leader value did not clear the stake-derived threshold",
		},
	)
	m.pipelineNoProgressRestarts = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_pipeline_no_progress_restarts",
			Help: "consecutive ledger-pipeline restarts that made no tip progress; resets to zero as soon as the tip advances",
		},
	)
	m.skippedStakeRewardRounds = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_skipped_stake_reward_rounds_total",
			Help: "epoch-boundary reward rounds skipped for want of their inputs; each one leaves reward balances and the leadership stake distribution permanently short by that epoch's rewards, which makes the node reject canonical blocks near the leader-eligibility threshold",
		},
	)
	m.pipelineStuck = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_pipeline_stuck",
			Help: "1 while the ledger pipeline has restarted without tip progress often enough to be treated as stuck on a deterministic failure (operator intervention required), 0 otherwise",
		},
	)
	m.pipelineHalted = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_pipeline_halted",
			Help: "1 once the ledger pipeline has stopped retrying on an unrepairable validation failure; terminal, so the node is no longer following the chain and requires operator intervention",
		},
	)
	m.mithrilTrustWindowUnrepairable = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_mithril_trust_window_unrepairable_total",
			Help: "validation failures declared unrepairable because every legal rewind target lay inside the Mithril protected window; the state the failing block needs predates the anchor and cannot be re-derived without crossing it",
		},
	)
	m.blockPipelineBlocksDecoded = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_block_pipeline_blocks_decoded",
			Help: "cumulative blocks successfully decoded by the block-processing pipeline (issue #1894); 0 unless blockPipelineEnabled is set",
		},
	)
	m.blockPipelineBlocksValidated = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_block_pipeline_blocks_validated",
			Help: "cumulative blocks that passed the block-processing pipeline's VRF/KES validate stage; 0 unless blockPipelineValidateEnabled is set",
		},
	)
	m.blockPipelineDecodeErrors = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_block_pipeline_decode_errors",
			Help: "cumulative block-processing pipeline decode failures",
		},
	)
	m.blockPipelineValidationErrors = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_block_pipeline_validation_errors",
			Help: "cumulative block-processing pipeline VRF/KES validation failures, including expected Byron-era non-validation",
		},
	)
	m.blockPipelineQueueDepth = promautoFactory.NewGauge(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_block_pipeline_queue_depth",
			Help: "current number of blocks buffered inside the block-processing pipeline's inter-stage channels",
		},
	)
	m.blockPipelineExpectedEta0Errors = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_pipeline_expected_eta0_errors_total",
			Help: "block-processing pipeline validate-stage errors drained " +
				"from errorsChan because a cached epoch has no Praos nonce",
		},
	)
	m.blockPipelineDeferredEpochCacheErrors = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_pipeline_deferred_epoch_cache_errors_total",
			Help: "block-processing pipeline validate-stage errors drained from errorsChan classified as a transient epoch-cache lag behind an already-committed block; expected to resolve once the epoch cache catches up",
		},
	)
	m.blockPipelineUnexpectedErrors = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_pipeline_unexpected_errors_total",
			Help: "block-processing pipeline errors drained from errorsChan that are not one of the expected/transient cases above; a nonzero value indicates a decode, validation, or apply-stage problem worth investigating",
		},
	)
}
