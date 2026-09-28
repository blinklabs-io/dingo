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
	"math"
	"sync"
	"sync/atomic"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type stateMetrics struct {
	blockfetchEventMu     sync.Mutex
	blockfetchEventStarts map[uint64]time.Time
	blockfetchEventNextID uint64
	blockNum              prometheus.Gauge
	density               prometheus.Gauge
	epochNum              prometheus.Gauge
	slotInEpoch           prometheus.Gauge
	slotNum               prometheus.Gauge
	forks                 prometheus.Gauge
	slotClockFallbacks    prometheus.Counter
	blocksForgedTotal     prometheus.Counter
	blockForgingLatency   prometheus.Histogram
	forgingEnabled        prometheus.Gauge
	nodeStartTime         prometheus.Gauge
	tipGapSlots           prometheus.Gauge
	shelleyStartTime      prometheus.Gauge
	epochLengthSlots      prometheus.Gauge
	shadowGateDecisions   *prometheus.CounterVec
	// Wall-clock time the ledger apply path spent waiting for a referenced
	// Leios endorser block, by outcome ("arrived", "timeout", "cancelled" or
	// "unavailable"). It covers both waits the apply path can take: the
	// diffusion window, and the CIP grace phase that waits out an in-flight
	// by-point fetch. This wait is taken ahead of the batch's DB transaction
	// on the single ledger pipeline, so it is time every block queued behind
	// the batch also spends waiting.
	// Only references ledger application actually reads are waited on (see
	// leiosApplyReadsOwnAnnouncement); the rest are prefetched in the
	// background and never observed here.
	leiosEbWaitSeconds *prometheus.HistogramVec
	// Pre-materialized observers for the outcome label values, so the apply
	// path does not resolve a label on every wait.
	leiosEbWaitArrived     prometheus.Observer
	leiosEbWaitTimedOut    prometheus.Observer
	leiosEbWaitCancelled   prometheus.Observer
	leiosEbWaitUnavailable prometheus.Observer
	// Waits that ran to a full bound without the endorser block arriving --
	// the diffusion window, or the CIP grace phase's hard bound. A rising
	// value against a flat leios_eb_wait_seconds "arrived" count means the
	// wait is buying nothing and is pure apply latency.
	//
	// Two outcomes are deliberately excluded because neither is a bound
	// expiring: "cancelled" says nothing about endorser-block availability,
	// and "unavailable" means the fetch COMPLETED without caching, which is
	// routine on a CIP node and would swamp the counter.
	leiosEbWaitTimeouts prometheus.Counter
	// Per-stage wall-clock time spent processing one block through the
	// ledger's slice of the pipeline: header_verify (VRF/KES/signature
	// checks run when a fetched block arrives), validate (one
	// transaction's era ledger-rule validation, including Plutus
	// evaluation, inside ledgerProcessBlock), and apply (writing a
	// flushed LedgerDeltaBatch's UTXO and transaction rows to the
	// metadata store), and epoch_rollover (the epoch-boundary transaction
	// in ledgerProcessBlocksFromSource: era transitions, reward
	// application and the governance tally, during which no block is
	// applied). See dingo_blockfetch_stage_duration_seconds in the
	// ouroboros package for the wire-decode stage, which runs before a
	// block reaches the ledger at all. Together the two metrics answer
	// "where does per-block processing time go", which no existing
	// histogram covers end to end.
	blockStageDuration *prometheus.HistogramVec
	// Pre-materialized observers for the stage label values, so the hot
	// path does not resolve a label on every block or transaction.
	blockStageHeaderVerify  prometheus.Observer
	blockStageValidate      prometheus.Observer
	blockStageApply         prometheus.Observer
	blockStageEpochRollover prometheus.Observer
	// Per-stage maximum wall-clock duration ever observed since process
	// start, each holding math.Float64bits of a seconds value and advanced
	// by updateMaxDuration's compare-and-swap loop at the same
	// observeBlockStage call site that records blockStageDuration, so the
	// two can never drift out of sync with each other.
	// (blockStageDuration's doc comment above covers what each stage means.)
	//
	// These atomics are the metric's only state. They are exported as
	// dingo_ledger_block_stage_max_duration_seconds by GaugeFunc collectors
	// that read them at scrape time -- see registerBlockStageMaxDuration --
	// rather than by Gauges that observeBlockStage pushes into, so there is
	// no second copy of the value that can fall behind this one.
	//
	// A histogram quantile is only an estimate bounded by its bucket edges,
	// however wide they are; this is the exact worst-ever-seen value, so an
	// epoch-boundary stall like blinklabs-io/dingo#4364 (block application
	// blocked for 25s to over 300s) shows up exactly on the epoch_rollover
	// stage instead of "somewhere past the last bucket boundary".
	//
	// Deliberately monotonic non-resetting ("worst ever seen"), not a
	// windowed maximum: a windowed maximum needs a periodic-reset custom
	// Collector, real complexity for a question -- "did we ever see
	// something this bad" -- that a simple non-resetting maximum already
	// answers.
	blockStageHeaderVerifyMax  atomic.Uint64
	blockStageValidateMax      atomic.Uint64
	blockStageApplyMax         atomic.Uint64
	blockStageEpochRolloverMax atomic.Uint64
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
	// Incremented every time a fetched block fails to extend the chain tip
	// (chain.BlockNotFitChainTipError). A handful from one connection during
	// a brief rollback/reorg race is normal; a sustained high rate from one
	// connection is the signal noteNonExtendingBlockRejection uses to
	// recycle it (see nonExtendingBlockFloodRecycles) -- this counter makes
	// that pattern visible before it crosses the recycle threshold, and
	// across all connections even when none individually crosses it. See
	// issue #4272.
	nonExtendingBlockRejections prometheus.Counter
	// Incremented each time noteNonExtendingBlockRejection actually recycles
	// a connection for flooding non-extending blocks (as opposed to every
	// individual rejection, counted above). See issue #4272.
	nonExtendingBlockFloodRecycles prometheus.Counter
	// Incremented when at-tip validation recovery detects a non-converging,
	// descending series of distinct failures and holds at the ledger tip
	// instead of rewinding the primary chain ever deeper. A rising value
	// means local ledger validation is diverging from the network (e.g. a
	// false-positive validation rejection), not a peer/fork problem. See
	// issue #2939.
	atTipRecoveryNonConverging prometheus.Counter
	// Incremented when an at-tip recovery rewind target falls below the
	// consumed-UTxO prune floor and is clamped to the ledger tip. The sweep
	// hard-deletes spent rows, and rollback restores them with an UPDATE, so
	// a rewind past the floor cannot rebuild the live UTxO set it implies. A
	// rising value means recovery is asking for rewinds deeper than local
	// history can support. See issue #3766.
	atTipRecoveryPruneFloorClamped prometheus.Counter
	// Incremented when unresolved-producer replay recovery repeatedly fails
	// to move the applied ledger tip forward and holds at that tip instead of
	// pruning another security-parameter window. See issue #3005.
	replayRecoveryNonConverging prometheus.Counter
	// Incremented when the cross-fork continuation audit finds a freshly
	// fetched body spending an input whose producing transaction is not on
	// the local applied chain. A rising value means a peer is feeding the
	// node a continuation from a fork it never applied. See issue #3005.
	continuationInputUnresolved prometheus.Counter
	// Incremented when the primary-chain/ledger divergence reconciler
	// cannot resolve one of the ledger's own applied block_nonce points to
	// build its ledger.tx undo events, typically because chain selection
	// already replaced that block and, after a process restart, the
	// manager's block cache no longer retains it either. The reconciler
	// still rolls the ledger back correctly; only the undo notification for
	// that block is missing, so a rising value means ledger.tx subscribers
	// may be carrying stale derived state for an abandoned branch. See
	// issue #3516.
	reconciliationUndoUnresolved prometheus.Counter
	// Incremented by the block-number count the reconciler's undo-block
	// resolution expects but has no block_nonce row for at all -- not
	// merely unresolvable (reconciliationUndoUnresolved), but entirely
	// absent from the query, the shape of a Byron-era applied block: Byron's
	// BFT/PoA consensus writes no VRF nonce, so it is invisible to a
	// block_nonce-keyed search. A rising value means an applied block's
	// ledger.tx undo event could not even be attempted for lack of a
	// durable per-block record, not merely because the content was no
	// longer reachable. See issue #3778.
	reconciliationUndoMissingRecord prometheus.Counter
	// Cross-fork continuation audit outcomes. clean, missing_producer and
	// inconclusive_eb_pending count one audited input each; disarmed_cap
	// counts one audit window; skipped_budget counts one audited body whose
	// endorser-block budget was exhausted; ref_unresolvable counts one
	// endorser-block reference abandoned for good. See the
	// continuationAuditResult* constants for the label values and
	// countContinuationAuditOutcome for the helper that records them.
	continuationAuditOutcomes *prometheus.CounterVec
	// Pre-materialized children of continuationAuditOutcomes, so every
	// verdict is exported from process start (an absent series and a zero
	// series read very differently when the question is "is this node
	// reporting missing producers") and so the audit pays no label lookup
	// per input while holding chainsyncBlockfetchMutex.
	continuationAuditClean                 prometheus.Counter
	continuationAuditMissingProducer       prometheus.Counter
	continuationAuditInconclusiveEbPending prometheus.Counter
	continuationAuditDisarmedCap           prometheus.Counter
	continuationAuditSkippedBudget         prometheus.Counter
	continuationAuditRefUnresolvable       prometheus.Counter
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
	// Incremented each time GetStakeDistribution or GetPoolDistr2 omits a
	// pool that holds stake at the queried snapshot but has no resolvable
	// registration/VRF key hash on record. This is a deliberate fallback
	// (see poolStakeDistribution's own comment), not a failure, so it
	// does not abort the query -- but a sustained nonzero value means a
	// real cross-node comparison tool would see dingo's reply as short by
	// that many pools, the exact condition blinklabs-io/dingo#4152 found
	// via cmd/node-parity against a real cardano-node without any other
	// visible symptom. Making this a metric rather than only the existing
	// WARN log lets that be caught by an alert instead of requiring a
	// manual diff to notice again.
	poolStakeDistributionOmittedPools prometheus.Counter
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
	// which should stay at 0 in healthy operation; the apply-pending-limit
	// counter tracks pipeline.ErrPendingLimitExceeded (the apply stage's
	// out-of-order buffer grew past MaxPendingBlocks because one stage
	// worker fell behind its siblings -- a load signal, not a block
	// failure: the item stays buffered and is applied in sequence); the
	// shutdown counter tracks context.Canceled/context.DeadlineExceeded
	// (BlockPipeline.Stop cancels the pipeline context before draining, so
	// a stage worker mid-item at shutdown can report the cancellation
	// instead of its item's outcome). Unlike the *Errors gauges above
	// (owned by the pipeline's own snapshot), these are counters
	// incremented directly as each error is drained.
	blockPipelineExpectedEta0Errors       prometheus.Counter
	blockPipelineDeferredEpochCacheErrors prometheus.Counter
	blockPipelineApplyPendingLimitErrors  prometheus.Counter
	blockPipelineShutdownErrors           prometheus.Counter
	blockPipelineUnexpectedErrors         prometheus.Counter
	// Per-block composition metrics (issue #4367), all labelled by era
	// (block.Era().Name, e.g. "Babbage", "Conway"). Recorded once per
	// applied block, right where blocksProcessed is incremented in
	// ledgerProcessBlocksFromSource, so a spike in
	// dingo_database_sql_operations_total /
	// dingo_database_sql_query_duration_seconds can be correlated against
	// what kind of block content produced it -- more transactions, a run
	// of script-heavy blocks, a certificate-heavy block, or simply a
	// different era's block format -- instead of only the aggregate load.
	blocksTotal            *prometheus.CounterVec
	blockTransactionsTotal *prometheus.CounterVec
	// blocksWithScriptsTotal counts one per block that carries at least one
	// Plutus V1/V2/V3 script or redeemer in any transaction, a coarser
	// (block-level, boolean) proxy than redeemersTotal for how often
	// script-bearing blocks occur.
	blocksWithScriptsTotal *prometheus.CounterVec
	// redeemersTotal sums per-block redeemer counts: a finer-grained proxy
	// for script-validation volume than blocksWithScriptsTotal, since a
	// block can carry many redeemers across its transactions.
	redeemersTotal *prometheus.CounterVec
	// utxoCreatedTotal/utxoConsumedTotal follow Produced()/Consumed()
	// semantics, not raw Outputs()/Inputs(): a phase-2-failed transaction
	// creates only its collateral return (at index len(Outputs())) and
	// consumes only its collateral, while Outputs()/Inputs() would report
	// entries that never took effect. See the same distinction in
	// ledgerProcessBlock's intraBlockUtxos population (state.go).
	utxoCreatedTotal  *prometheus.CounterVec
	utxoConsumedTotal *prometheus.CounterVec
	certificatesTotal *prometheus.CounterVec
	// commitBatchBlocks observes len(nextBatch) each time
	// ledgerReadChainIterator submits a gathered batch of blocks downstream
	// for a single DB transaction (batchSize caps it at 50). Added
	// alongside the dingo#4464 premature-flush fix so a future run can
	// confirm the batch-size distribution actually shifted upward, rather
	// than relying on re-measuring physical disk I/O.
	commitBatchBlocks prometheus.Histogram
}

// The accessors below tolerate an uninitialised stateMetrics. A LedgerState
// built directly -- as the ledger's own unit tests do -- never calls init, so
// its metric fields are nil; instrumenting a path that those tests exercise
// must not turn a metric into a nil dereference.

// observeCommitBatchBlocks records one gathered batch's block count; see
// commitBatchBlocks. Passes that gather nothing are not observed at all --
// see the call site in ledgerReadChainIterator.
func (m *stateMetrics) observeCommitBatchBlocks(blocks int) {
	if m == nil || m.commitBatchBlocks == nil {
		return
	}
	m.commitBatchBlocks.Observe(float64(blocks))
}

func (m *stateMetrics) observeLeaderThresholdMargin(margin float64) {
	if m == nil || m.leaderThresholdMargin == nil {
		return
	}
	m.leaderThresholdMargin.Observe(margin)
}

// Outcome label values for dingo_metrics_leios_eb_wait_seconds.
//
//   - arrived:   the endorser block became available during the wait.
//   - timeout:   a bound elapsed without it -- the diffusion window, or the
//     CIP grace phase's hard bound. This is the outcome that means the wait
//     cost apply latency and bought nothing.
//   - unavailable: the CIP grace phase's by-point fetch COMPLETED without
//     caching, because no peer holds the endorser block. Nothing timed out,
//     and this is the common ending on a CIP node, so it is deliberately not
//     folded into timeout: doing so would inflate the timeout rate and its
//     counter on routine operation.
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
	// leiosEbWaitOutcomeUnavailable is the CIP grace phase's routine ending:
	// the by-point fetch COMPLETED without caching, because no peer holds the
	// endorser block. Nothing timed out and nothing was cancelled, so folding
	// it into either would overstate both -- and it is the most common ending
	// on a CIP node, so it would overstate them badly.
	leiosEbWaitOutcomeUnavailable = "unavailable"
)

// Stage labels for blockStageDuration. See its field doc comment for what
// each stage covers.
const (
	blockStageHeaderVerify  = "header_verify"
	blockStageValidate      = "validate"
	blockStageApply         = "apply"
	blockStageEpochRollover = "epoch_rollover"
)

// observeBlockStage records one sample of wall-clock time spent in the named
// per-block processing stage, into both blockStageDuration (the histogram)
// and blockStageMaxDuration (the exact running maximum for that stage) --
// the same call site feeds both, so they cannot drift out of sync with each
// other. Safe to call before init (or when metrics are disabled), matching
// the other observe helpers in this file.
func (m *stateMetrics) observeBlockStage(stage string, d time.Duration) {
	if m == nil {
		return
	}
	var obs prometheus.Observer
	var record *atomic.Uint64
	switch stage {
	case blockStageHeaderVerify:
		obs = m.blockStageHeaderVerify
		record = &m.blockStageHeaderVerifyMax
	case blockStageValidate:
		obs = m.blockStageValidate
		record = &m.blockStageValidateMax
	case blockStageApply:
		obs = m.blockStageApply
		record = &m.blockStageApplyMax
	case blockStageEpochRollover:
		obs = m.blockStageEpochRollover
		record = &m.blockStageEpochRolloverMax
	}
	if obs == nil {
		// Unknown stage, or metrics were never initialised: neither field
		// was resolved, so there is nothing to update on either metric.
		// Gating the running maximum on the histogram observer too is what
		// keeps the two reporting the same set of samples.
		return
	}
	seconds := d.Seconds()
	obs.Observe(seconds)
	updateMaxDuration(record, seconds)
}

func (m *stateMetrics) beginBlockfetchEvent() uint64 {
	if m == nil {
		return 0
	}
	m.blockfetchEventMu.Lock()
	if m.blockfetchEventStarts == nil {
		m.blockfetchEventStarts = make(map[uint64]time.Time)
	}
	m.blockfetchEventNextID++
	id := m.blockfetchEventNextID
	m.blockfetchEventStarts[id] = time.Now()
	m.blockfetchEventMu.Unlock()
	return id
}

func (m *stateMetrics) endBlockfetchEvent(id uint64) {
	if m == nil || id == 0 {
		return
	}
	m.blockfetchEventMu.Lock()
	delete(m.blockfetchEventStarts, id)
	m.blockfetchEventMu.Unlock()
}

func (m *stateMetrics) blockfetchEventInProgressSeconds() float64 {
	m.blockfetchEventMu.Lock()
	defer m.blockfetchEventMu.Unlock()
	if len(m.blockfetchEventStarts) == 0 {
		return 0
	}
	var oldest time.Time
	for _, started := range m.blockfetchEventStarts {
		if oldest.IsZero() || started.Before(oldest) {
			oldest = started
		}
	}
	return time.Since(oldest).Seconds()
}

// updateMaxDuration performs a lock-free "keep the maximum ever observed"
// update: it compares observed against the current value of record (an
// atomic.Uint64 holding math.Float64bits of the running maximum) and
// compare-and-swaps it in only when observed is strictly larger. On the hot
// per-block path this costs one atomic load and, in the common case where
// observed does not beat the record, no write at all -- no lock, no
// allocation.
//
// record is the metric's only state; the exported gauge reads it at scrape
// time (see registerBlockStageMaxDuration). That is what makes the exported
// value exactly equal to the record at all times, and it is the reason this
// does not also push the new value into a Gauge. Pushing would introduce a
// second copy that can fall behind and stay behind: two goroutines can each
// win a CAS (say 5s then 10s) and land their Set calls in the other order,
// leaving the gauge reading 5 while the record holds 10. Nothing recovers
// that until an observation beats 10 -- an observation of 7 exits at the
// comparison below without touching the gauge -- so on a metric whose whole
// purpose is the worst case, the exported value could understate the record
// for the life of the process.
func updateMaxDuration(record *atomic.Uint64, observed float64) {
	for {
		old := record.Load()
		if observed <= math.Float64frombits(old) {
			return
		}
		if record.CompareAndSwap(old, math.Float64bits(observed)) {
			return
		}
	}
}

// blockStageMaxDurationHelp documents
// dingo_ledger_block_stage_max_duration_seconds. Shared by all three
// per-stage collectors, which must agree on it: the Prometheus registry
// rejects two collectors that export the same metric name with different
// help text.
const blockStageMaxDurationHelp = "maximum wall-clock duration ever observed for each ledger-owned per-block processing stage (see dingo_ledger_block_stage_duration_seconds), since process start; monotonically non-decreasing, and exact rather than bucket-bounded"

// registerBlockStageMaxDuration exports each stage's running maximum as one
// series of dingo_ledger_block_stage_max_duration_seconds.
//
// One GaugeFunc per stage carrying the stage as a constant label, rather
// than a single GaugeVec: a GaugeVec's members hold their own copy of the
// value and must be Set, which is a second place the number lives and can
// go stale (see updateMaxDuration). A GaugeFunc reads the atomic at scrape
// time, so the exported value is the record by construction. This is the
// same pull-based pattern the sqlstore and sqlite metadata plugins already
// use for values they do not own a copy of.
func (m *stateMetrics) registerBlockStageMaxDuration(
	factory promauto.Factory,
) {
	for stage, record := range map[string]*atomic.Uint64{
		blockStageHeaderVerify:  &m.blockStageHeaderVerifyMax,
		blockStageValidate:      &m.blockStageValidateMax,
		blockStageApply:         &m.blockStageApplyMax,
		blockStageEpochRollover: &m.blockStageEpochRolloverMax,
	} {
		factory.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "dingo_ledger_block_stage_max_duration_seconds",
				Help:        blockStageMaxDurationHelp,
				ConstLabels: prometheus.Labels{"stage": stage},
			},
			func() float64 {
				return math.Float64frombits(record.Load())
			},
		)
	}
}

// blockComposition summarizes the shape of one applied block: era,
// transaction count, Plutus script/redeemer presence, UTxO churn, and
// certificate count. computeBlockComposition derives it once per block so
// observeBlockComposition never has to walk the block's transactions itself
// (issue #4367).
type blockComposition struct {
	era          string
	transactions int
	hasScripts   bool
	redeemers    int
	utxoCreated  int
	utxoConsumed int
	certificates int
}

// computeBlockComposition derives a blockComposition from one applied block.
// UTxO churn follows Produced()/Consumed() semantics, not raw
// Outputs()/Inputs() -- see the utxoCreatedTotal/utxoConsumedTotal field doc
// comment for why. A block counts as carrying scripts when any
// transaction's witness set has a Plutus V1/V2/V3 script or at least one
// redeemer: a redeemer alone still means phase-2 script evaluation ran, even
// when the script itself is supplied by a reference input rather than the
// witness set (mirroring txHasRedeemers in ledger/eras/validation.go).
func computeBlockComposition(block lcommon.Block) blockComposition {
	c := blockComposition{era: block.Era().Name}
	txs := block.Transactions()
	c.transactions = len(txs)
	for _, tx := range txs {
		// This runs inside the block-apply DB transaction for every applied
		// block, so the created-UTxO count is taken without building the
		// UTxOs. Produced() allocates an lcommon.Utxo per output and
		// round-trips the transaction hash through hex to construct each
		// one's input reference; only its length is wanted here, and every
		// era defines Produced() for a valid transaction as exactly one UTxO
		// per output. The phase-2-failed rule differs by era (Alonzo produces
		// nothing, Babbage onward at most a collateral return), so that case
		// is still read from Produced() rather than restated here.
		if tx.IsValid() {
			c.utxoCreated += len(tx.Outputs())
		} else {
			c.utxoCreated += len(tx.Produced())
		}
		c.utxoConsumed += len(tx.Consumed())
		c.certificates += len(tx.Certificates())
		witnesses := tx.Witnesses()
		if witnesses == nil {
			continue
		}
		if len(witnesses.PlutusV1Scripts()) > 0 ||
			len(witnesses.PlutusV2Scripts()) > 0 ||
			len(witnesses.PlutusV3Scripts()) > 0 {
			c.hasScripts = true
		}
		redeemers := witnesses.Redeemers()
		if redeemers == nil {
			continue
		}
		for range redeemers.Iter() {
			c.redeemers++
			c.hasScripts = true
		}
	}
	return c
}

// observeBlockComposition records one applied block's composition under its
// era label. Safe to call on an uninitialised stateMetrics (metrics
// disabled), matching the other observe helpers in this file.
func (m *stateMetrics) observeBlockComposition(c blockComposition) {
	if m == nil {
		return
	}
	if m.blocksTotal != nil {
		m.blocksTotal.WithLabelValues(c.era).Inc()
	}
	if m.blockTransactionsTotal != nil {
		m.blockTransactionsTotal.WithLabelValues(c.era).
			Add(float64(c.transactions))
	}
	if m.blocksWithScriptsTotal != nil && c.hasScripts {
		m.blocksWithScriptsTotal.WithLabelValues(c.era).Inc()
	}
	if m.redeemersTotal != nil {
		m.redeemersTotal.WithLabelValues(c.era).Add(float64(c.redeemers))
	}
	if m.utxoCreatedTotal != nil {
		m.utxoCreatedTotal.WithLabelValues(c.era).
			Add(float64(c.utxoCreated))
	}
	if m.utxoConsumedTotal != nil {
		m.utxoConsumedTotal.WithLabelValues(c.era).
			Add(float64(c.utxoConsumed))
	}
	if m.certificatesTotal != nil {
		m.certificatesTotal.WithLabelValues(c.era).
			Add(float64(c.certificates))
	}
}

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
	case leiosEbWaitOutcomeUnavailable:
		obs = m.leiosEbWaitUnavailable
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

func (m *stateMetrics) incPoolStakeDistributionOmittedPool() {
	if m == nil || m.poolStakeDistributionOmittedPools == nil {
		return
	}
	m.poolStakeDistributionOmittedPools.Inc()
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

// incBlockPipelineApplyPendingLimitError records a block-processing pipeline
// apply-stage backpressure signal drained from errorsChan
// (pipeline.ErrPendingLimitExceeded): more out-of-order blocks were buffered
// waiting for an earlier sequence number than MaxPendingBlocks allows. The
// item is still buffered and still applied in sequence, so this is a
// throughput/scheduling signal rather than a decode, validation, or apply
// failure.
func (m *stateMetrics) incBlockPipelineApplyPendingLimitError() {
	if m == nil || m.blockPipelineApplyPendingLimitErrors == nil {
		return
	}
	m.blockPipelineApplyPendingLimitErrors.Inc()
}

// incBlockPipelineShutdownError records a block-processing pipeline stage
// worker reporting its own context cancellation
// (context.Canceled/context.DeadlineExceeded) rather than an item outcome.
// BlockPipeline.Stop cancels the pipeline context before it drains the
// stages, so any worker mid-item when shutdown starts can report this; it
// says the node is stopping, not that a block failed.
func (m *stateMetrics) incBlockPipelineShutdownError() {
	if m == nil || m.blockPipelineShutdownErrors == nil {
		return
	}
	m.blockPipelineShutdownErrors.Inc()
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
	promautoFactory.NewGaugeFunc(
		prometheus.GaugeOpts{
			Name: "dingo_ledger_blockfetch_event_in_progress_seconds",
			Help: "wall-clock time since the oldest active blockfetch event handler began, or zero when no blockfetch event is being handled",
		},
		m.blockfetchEventInProgressSeconds,
	)
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
			Help: "wall-clock time the ledger apply path spent waiting for a referenced Leios endorser block, across both the diffusion window and the CIP in-flight-fetch grace phase, by outcome (arrived, timeout, cancelled, unavailable)",
			// 5ms to ~164s. The lower end covers sub-slot arrivals; the
			// upper end must clear the LONGEST wait this histogram now
			// records, the CIP grace phase's leiosTipFetchHardBound
			// (leiosBackfillMaxWait, 120s). At 15 buckets the top edge was
			// ~82s, so every wedged-fetch wait -- the most anomalous ones,
			// and the reason the grace phase is instrumented at all --
			// collapsed into +Inf with no resolution.
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 16),
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
	m.leiosEbWaitUnavailable = m.leiosEbWaitSeconds.WithLabelValues(
		leiosEbWaitOutcomeUnavailable,
	)
	m.leiosEbWaitTimeouts = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_metrics_leios_eb_wait_timeouts_total",
			Help: "ledger apply-path waits for a referenced Leios endorser block that ran to a full bound without it arriving: the diffusion window, or the CIP in-flight-fetch grace phase hard bound",
		},
	)
	m.blockStageDuration = promautoFactory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "dingo_ledger_block_stage_duration_seconds",
			Help: "wall-clock time spent in each ledger-owned stage of per-block processing, by stage: header_verify (VRF/KES/signature checks on blockfetch arrival), validate (one transaction's era ledger-rule validation, including Plutus evaluation), apply (writing a flushed delta batch's UTXO and transaction rows to the metadata store), epoch_rollover (the epoch-boundary transaction, including reward application and the governance tally, during which no block is applied)",
			// 100us to ~419s. Most observations are sub-100ms (a single
			// cheap signature check, one transaction's validation, one
			// delta-batch flush), which is why resolution stays fine down
			// there. The upper end has to resolve epoch_rollover:
			// blinklabs-io/dingo#4364 measured block application blocked
			// for 25s to 318s across preview boundaries, all of which the
			// old ~3.3s ceiling put in +Inf.
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 23),
		},
		[]string{"stage"},
	)
	m.blockStageHeaderVerify = m.blockStageDuration.WithLabelValues(
		blockStageHeaderVerify,
	)
	m.blockStageValidate = m.blockStageDuration.WithLabelValues(
		blockStageValidate,
	)
	m.blockStageApply = m.blockStageDuration.WithLabelValues(
		blockStageApply,
	)
	m.blockStageEpochRollover = m.blockStageDuration.WithLabelValues(
		blockStageEpochRollover,
	)
	m.registerBlockStageMaxDuration(promautoFactory)
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
	m.nonExtendingBlockRejections = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_chainsync_non_extending_blocks_total",
			Help: "fetched blocks that failed to extend the chain tip (chain.BlockNotFitChainTipError). A handful is normal during a brief rollback/reorg race; a sustained high rate from one connection triggers recycling it, see dingo_chainsync_non_extending_block_flood_recycles_total",
		},
	)
	m.nonExtendingBlockFloodRecycles = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_chainsync_non_extending_block_flood_recycles_total",
			Help: "connections recycled for repeatedly serving blocks that do not extend the chain tip within a bounded window (issue #4272)",
		},
	)
	m.atTipRecoveryNonConverging = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_attip_recovery_nonconverging_total",
			Help: "times at-tip validation recovery held at the ledger tip instead of rewinding the primary chain deeper, because a descending series of distinct failures indicated local validation divergence (operator intervention required)",
		},
	)
	m.atTipRecoveryPruneFloorClamped = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_attip_recovery_prune_floor_clamped_total",
			Help: "times an at-tip validation recovery rewind target below the consumed-UTxO prune floor was clamped to the ledger tip, because UTxOs consumed above that floor were hard-deleted and cannot be restored by a rewind",
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
	m.reconciliationUndoUnresolved = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_reconciliation_undo_unresolved_total",
			Help: "applied blocks the primary-chain/ledger divergence reconciler could not resolve to build ledger.tx undo events (block already replaced by chain selection and no longer cached, typically after a restart)",
		},
	)
	m.reconciliationUndoMissingRecord = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_reconciliation_undo_missing_record_total",
			Help: "applied blocks in a reconciliation undo range with no block_nonce row at all, not merely unresolvable content -- the shape of a Byron-era applied block (issue #3778)",
		},
	)
	// Cross-fork continuation audit verdicts, labelled by result:
	//   result="clean"                   — the input resolved to a producer
	//   result="missing_producer"        — no producer found; reported at
	//        Error, and the splice indicator above is incremented too
	//   result="inconclusive_eb_pending" — the window's producer set is
	//        knowingly incomplete because a certified Leios endorser block
	//        had not been fetched when the audit ran, so an unresolved input
	//        cannot be distinguished from one the audit simply cannot see
	//   result="disarmed_cap"            — one per window, when the producer
	//        set reached continuationAuditMaxProducedTxs and the window was
	//        disarmed rather than report from a truncated set
	//   result="skipped_budget"          — one per audited body that had more
	//        endorser blocks queued than continuationAuditMaxEndorserBlocksPerBlock
	//        allows it to resolve; the rest stay queued for a later body
	//   result="ref_unresolvable"        — an endorser-block reference given up
	//        on for good, because resolving it failed for a reason retrying
	//        cannot fix; that hole in the producer set never closes
	// A node whose inconclusive count dominates is telling the operator the
	// audit is not covering it, which is the honest reading of an
	// endorser-block backlog.
	m.continuationAuditOutcomes = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_continuation_audit_outcomes_total",
			Help: "cross-fork continuation audit verdicts by result; clean/missing_producer/inconclusive_eb_pending count audited inputs, skipped_budget counts audited blocks, ref_unresolvable counts abandoned endorser-block references, disarmed_cap counts audit windows",
		},
		[]string{"result"},
	)
	m.continuationAuditClean = m.continuationAuditOutcomes.WithLabelValues(
		continuationAuditResultClean,
	)
	m.continuationAuditMissingProducer = m.continuationAuditOutcomes.WithLabelValues(
		continuationAuditResultMissingProducer,
	)
	m.continuationAuditInconclusiveEbPending = m.continuationAuditOutcomes.WithLabelValues(
		continuationAuditResultInconclusiveEbPending,
	)
	m.continuationAuditDisarmedCap = m.continuationAuditOutcomes.WithLabelValues(
		continuationAuditResultDisarmedCap,
	)
	m.continuationAuditSkippedBudget = m.continuationAuditOutcomes.WithLabelValues(
		continuationAuditResultSkippedBudget,
	)
	m.continuationAuditRefUnresolvable = m.continuationAuditOutcomes.WithLabelValues(
		continuationAuditResultRefUnresolvable,
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
	m.poolStakeDistributionOmittedPools = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_pool_stake_distribution_omitted_pools_total",
			Help: "pools omitted from GetStakeDistribution/GetPoolDistr2 because they held snapshot stake but had no resolvable registration/VRF key hash on record",
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
			Help: "cumulative blocks that passed the block-processing pipeline's VRF/KES/OpCert validate stage; 0 unless blockPipelineValidateEnabled is set",
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
			Help: "cumulative block-processing pipeline VRF/KES/OpCert validation failures, including expected Byron-era non-validation",
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
	m.blockPipelineApplyPendingLimitErrors = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_pipeline_apply_pending_limit_errors_total",
			Help: "block-processing pipeline apply-stage backpressure signals drained from errorsChan because the out-of-order pending buffer exceeded MaxPendingBlocks; the block is still buffered and applied in sequence, so this reports stage-worker scheduling lag rather than a block failure",
		},
	)
	m.blockPipelineShutdownErrors = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_pipeline_shutdown_errors_total",
			Help: "block-processing pipeline stage-worker context cancellations drained from errorsChan while the pipeline was stopping; expected on any shutdown with blocks still in flight and not a block failure",
		},
	)
	m.blockPipelineUnexpectedErrors = promautoFactory.NewCounter(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_pipeline_unexpected_errors_total",
			Help: "block-processing pipeline errors drained from errorsChan that are not one of the expected/transient cases above; a nonzero value indicates a decode, validation, or apply-stage problem worth investigating",
		},
	)
	m.blocksTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_blocks_total",
			Help: "blocks processed by the ledger apply path, by era; a block reprocessed after an apply retry is counted again, so this tracks processing rate rather than an exact durably-applied count",
		},
		[]string{"era"},
	)
	m.blockTransactionsTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_block_transactions_total",
			Help: "transactions in applied blocks, by era",
		},
		[]string{"era"},
	)
	m.blocksWithScriptsTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_blocks_with_scripts_total",
			Help: "applied blocks, by era, that carry at least one Plutus V1/V2/V3 script or redeemer in any transaction",
		},
		[]string{"era"},
	)
	m.redeemersTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_redeemers_total",
			Help: "redeemers in applied blocks, by era; a finer-grained proxy for Plutus script-validation volume than dingo_ledger_blocks_with_scripts_total",
		},
		[]string{"era"},
	)
	m.utxoCreatedTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_utxo_created_total",
			Help: "UTxOs created by applied blocks, by era; a phase-2-failed transaction contributes only its collateral return, not its ordinary outputs",
		},
		[]string{"era"},
	)
	m.utxoConsumedTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_utxo_consumed_total",
			Help: "UTxOs consumed by applied blocks, by era; a phase-2-failed transaction contributes only its collateral inputs, not its ordinary inputs",
		},
		[]string{"era"},
	)
	m.certificatesTotal = promautoFactory.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dingo_ledger_certificates_total",
			Help: "certificates in applied blocks, by era",
		},
		[]string{"era"},
	)
	m.commitBatchBlocks = promautoFactory.NewHistogram(
		prometheus.HistogramOpts{
			Name: "dingo_ledger_commit_batch_blocks",
			Help: "blocks gathered into one batch by the chain-read loop before it is submitted for a single DB transaction; batchSize (50) is the cap. A distribution clustered well below the cap during bulk sync means batches are flushing prematurely (see dingo#4464).",
			// Explicit buckets rather than exponential/linear: batchSize
			// caps this at 50, and the values worth distinguishing are
			// small (the premature-flush symptom) versus close to the cap
			// (healthy batching), not a smooth curve between them.
			Buckets: []float64{
				1, 2, 3, 5, 8, 10, 15, 20, 25, 30, 35, 40, 45, 50,
			},
		},
	)
}
