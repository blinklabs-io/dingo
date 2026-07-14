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

package leios

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
)

// observeFutureToleranceSlots bounds how far ahead of the current (or tip)
// slot an endorser block observation is admitted into the pipeline. Honest
// EBs are produced in the present and diffused, so the only legitimate
// future skew is clock drift and diffusion delay; beyond this an
// observation is a peer attempting to pre-seed future slots, which would
// make MayProduceEndorserBlock deny the legitimate local producer when
// those slots open. Defined as the vote manager's slotWindowFutureTolerance
// so the two Leios components admit endorser blocks over the same future
// window and cannot drift apart. Far-past slots are bounded separately by
// InstanceTTLSlots.
const observeFutureToleranceSlots = slotWindowFutureTolerance

// Stage identifies where an endorser block sits in the CIP-0164 Linear
// Leios pipeline. The pipeline is three logical stages -- produce/diffuse,
// vote/certify, and ranking-block inclusion -- expanded here into the
// finer phases the manager tracks. Stages are derived purely from the
// distance between an EB's produce slot and the current slot (plus whether
// a certificate has been observed); see stageFor.
type Stage int

const (
	// StageProduce is the window in which the slot leader may forge the
	// endorser block for its slot.
	StageProduce Stage = iota
	// StageDiffuse is the propagation grace period after production,
	// during which the EB is fetched and cached across peers.
	StageDiffuse
	// StageVote is the committee voting window. Actual vote acceptance is
	// enforced by VoteManager.slotWindowCheck; this stage is the
	// pipeline's view of the same window.
	StageVote
	// StageCertify is the window in which collected votes must reach
	// quorum and a certificate must be built.
	StageCertify
	// StageEligible means a certificate has been observed and the EB is
	// eligible for inclusion in a ranking block.
	StageEligible
	// StageExpired means the EB is past its useful lifetime: it either
	// missed the certification deadline or its inclusion window closed.
	StageExpired
)

// stageCount is the number of distinct stages. It is not a stage itself; it
// sizes per-stage aggregations such as the stage gauge.
const stageCount = int(StageExpired) + 1

// String returns a human-readable stage name.
func (s Stage) String() string {
	switch s {
	case StageProduce:
		return "produce"
	case StageDiffuse:
		return "diffuse"
	case StageVote:
		return "vote"
	case StageCertify:
		return "certify"
	case StageEligible:
		return "eligible"
	case StageExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// PipelineTiming holds the per-phase timing windows for the Leios
// pipeline, all expressed in slots relative to an endorser block's produce
// slot. CIP-0164 has not finalized these parameters, so these are
// provisional dingo-local values (mirroring the provisional vote window in
// VoteManager). They are deliberately kept in one struct, off-chain, so
// they can be revised as the spec firms up without a ledger schema change.
//
// The window offsets are cumulative and must be monotonically
// non-decreasing (validated by Validate): an EB at offset off (= current
// slot - produce slot) is in the produce phase while off <
// ProduceWindowSlots, the diffuse phase while off < DiffuseWindowSlots, and
// so on.
type PipelineTiming struct {
	// ProduceWindowSlots bounds how long after its produce slot an EB may
	// still be forged.
	ProduceWindowSlots uint64
	// DiffuseWindowSlots is the cumulative offset by which diffusion is
	// expected to complete.
	DiffuseWindowSlots uint64
	// VoteWindowSlots is the cumulative offset by which voting closes. It is
	// the single source for VoteManager's vote-acceptance past bound
	// (relative to the EB produce slot), so the two components admit votes
	// over the same window; stageFor surfaces the same boundary as
	// StageVote.
	VoteWindowSlots uint64
	// CertifyByDeadlineSlots is the cumulative offset by which a
	// certificate must be observed; past it an uncertified EB expires.
	CertifyByDeadlineSlots uint64
	// RbInclusionWindowSlots is the cumulative offset past which a
	// certified EB is no longer eligible for ranking-block inclusion.
	RbInclusionWindowSlots uint64
	// InstanceTTLSlots is the cumulative offset at which a pipeline
	// instance is dropped entirely.
	InstanceTTLSlots uint64
}

// DefaultPipelineTiming returns the provisional default timing. The values
// assume the Musashi testnet's 1s slots (config/cardano/musashi) and are sized
// to be comfortably larger than diffusion and voting latency while keeping
// the in-memory instance set small.
func DefaultPipelineTiming() PipelineTiming {
	return PipelineTiming{
		ProduceWindowSlots:     1,
		DiffuseWindowSlots:     5,
		VoteWindowSlots:        10,
		CertifyByDeadlineSlots: 20,
		RbInclusionWindowSlots: 40,
		InstanceTTLSlots:       100,
	}
}

// Validate checks that the timing windows are positive and monotonically
// non-decreasing, the invariant stageFor relies on.
func (t PipelineTiming) Validate() error {
	steps := []struct {
		name string
		val  uint64
	}{
		{"ProduceWindowSlots", t.ProduceWindowSlots},
		{"DiffuseWindowSlots", t.DiffuseWindowSlots},
		{"VoteWindowSlots", t.VoteWindowSlots},
		{"CertifyByDeadlineSlots", t.CertifyByDeadlineSlots},
		{"RbInclusionWindowSlots", t.RbInclusionWindowSlots},
		{"InstanceTTLSlots", t.InstanceTTLSlots},
	}
	var prev uint64
	for i, s := range steps {
		if s.val == 0 {
			return fmt.Errorf("leios pipeline timing: %s must be > 0", s.name)
		}
		if i > 0 && s.val < prev {
			return fmt.Errorf(
				"leios pipeline timing: %s (%d) must be >= the previous window (%d)",
				s.name,
				s.val,
				prev,
			)
		}
		prev = s.val
	}
	return nil
}

// stageFor maps an endorser block to its pipeline stage from the distance
// between its produce slot and the current slot, plus whether a
// certificate has been observed. It is a pure function -- the single source
// of truth for slot->stage mapping -- so it can be table-tested
// exhaustively. Equivocation is handled by callers, not here.
func stageFor(
	produceSlot, now uint64,
	t PipelineTiming,
	certified bool,
) Stage {
	if now < produceSlot {
		// The produce window has not opened yet.
		return StageProduce
	}
	off := now - produceSlot
	if off >= t.InstanceTTLSlots {
		return StageExpired
	}
	if certified {
		// Once certified, an EB is eligible until its inclusion window
		// closes, regardless of which earlier phase it certified in.
		if off < t.RbInclusionWindowSlots {
			return StageEligible
		}
		return StageExpired
	}
	switch {
	case off < t.ProduceWindowSlots:
		return StageProduce
	case off < t.DiffuseWindowSlots:
		return StageDiffuse
	case off < t.VoteWindowSlots:
		return StageVote
	case off < t.CertifyByDeadlineSlots:
		return StageCertify
	default:
		// Past the certification deadline without a certificate.
		return StageExpired
	}
}

// ProduceDecision is the answer to "may an endorser block be produced for
// this slot right now?", returned by MayProduceEndorserBlock. It is the
// stable seam consumed by the forge-loop integration (issue #1862); this
// package does not itself forge EBs.
type ProduceDecision struct {
	// Allowed reports whether production is permitted at the current slot.
	Allowed bool
	// Slot is the produce slot the decision is about.
	Slot uint64
	// WindowEnd is the exclusive slot at which the produce window closes.
	WindowEnd uint64
	// Reason describes why production was disallowed (empty when allowed).
	Reason string
}

// EligibleEb is a certified endorser block eligible for ranking-block
// inclusion, returned by EligibleCertifiedEbs. The actual RB embedding is
// out of scope for issue #1861 (no ranking-block CDDL exists yet); this
// type is the interface a future RB builder consumes.
type EligibleEb struct {
	SlotNo            uint64
	EndorserBlockHash lcommon.Blake2b256
	Certificate       *lcommon.LeiosEbCertificate
	Epoch             uint64
	AnnouncingRbHash  lcommon.Blake2b256
}

// ebState tracks one endorser block as it moves through the pipeline.
type ebState struct {
	hash      lcommon.Blake2b256
	slot      uint64
	epoch     uint64
	certified bool
	// certificates is keyed by the vote signing context. The same EB may be
	// announced by more than one ranking block; those aggregate signatures
	// are not interchangeable.
	certificates map[lcommon.Blake2b256]*lcommon.LeiosEbCertificate
	embedded     bool
	// equivocated is set when another distinct EB was observed for the
	// same slot. Equivocated EBs are never offered for RB inclusion.
	equivocated bool
}

// pipelineInstance groups the endorser blocks observed for a single produce
// slot. More than one EB in an instance is slot-level equivocation.
type pipelineInstance struct {
	produceSlot uint64
	epoch       uint64
	ebs         map[lcommon.Blake2b256]*ebState
	// equivocated records that this slot has already been counted as
	// equivocating, so the metric and warn log fire once per slot rather
	// than once per additional EB.
	equivocated bool
}

// PipelineManagerConfig configures a PipelineManager.
type PipelineManagerConfig struct {
	Logger        *slog.Logger
	EventBus      *event.EventBus
	SlotProvider  SlotProvider
	EpochProvider EpochProvider
	Timing        PipelineTiming
	PromRegistry  prometheus.Registerer
}

// PipelineManager orchestrates the CIP-0164 Linear Leios pipeline: it tracks
// endorser blocks through produce/diffuse/vote/certify/inclusion phases
// under provisional timing windows, detects slot-level EB equivocation,
// flushes in-flight state at epoch boundaries and rollbacks, and exposes
// the producer-facing and inclusion-facing seams the forge loop will use.
//
// It is intentionally decoupled from VoteManager: both observe the same
// endorser blocks independently, and the only channel between them is the
// EbQuorumEvent the VoteManager publishes when verified votes reach quorum.
// The pipeline never re-tallies votes and never rebuilds certificates.
//
// All state is in-memory and slot-driven: window decisions are made by
// querying SlotProvider.CurrentOrTipSlot rather than a slot clock (the
// SlotClock is private to LedgerState), mirroring how VoteManager advances.
type PipelineManager struct {
	logger        *slog.Logger
	eventBus      *event.EventBus
	slotProvider  SlotProvider
	epochProvider EpochProvider
	timing        PipelineTiming
	metrics       *pipelineMetrics

	mu       sync.Mutex
	running  bool
	stopping bool
	cancel   context.CancelFunc
	loopWg   sync.WaitGroup
	subs     []managerSubscription

	// instances are keyed by produce slot; byHash indexes every tracked
	// ebState by hash for O(1) quorum and embed lookups.
	instances map[uint64]*pipelineInstance
	byHash    map[lcommon.Blake2b256]*ebState
}

// NewPipelineManager creates a pipeline manager.
func NewPipelineManager(cfg PipelineManagerConfig) (*PipelineManager, error) {
	if cfg.EventBus == nil {
		return nil, errors.New("leios pipeline manager: nil event bus")
	}
	if cfg.SlotProvider == nil {
		return nil, errors.New("leios pipeline manager: nil slot provider")
	}
	if cfg.EpochProvider == nil {
		return nil, errors.New("leios pipeline manager: nil epoch provider")
	}
	if err := cfg.Timing.Validate(); err != nil {
		return nil, err
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	m := &PipelineManager{
		logger:        logger.With("component", "leios-pipeline"),
		eventBus:      cfg.EventBus,
		slotProvider:  cfg.SlotProvider,
		epochProvider: cfg.EpochProvider,
		timing:        cfg.Timing,
		instances:     make(map[uint64]*pipelineInstance),
		byHash:        make(map[lcommon.Blake2b256]*ebState),
	}
	if cfg.PromRegistry != nil {
		m.metrics = initPipelineManagerMetrics(cfg.PromRegistry)
	}
	return m, nil
}

// Start subscribes to EB quorum, epoch transition, and chain update events
// and begins processing them.
func (m *PipelineManager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		return nil
	}
	if m.stopping {
		return errors.New(
			"leios pipeline manager: Stop in progress, cannot Start",
		)
	}
	if ctx == nil {
		return errors.New("leios pipeline manager: nil context")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"leios pipeline manager: parent context already done: %w",
			err,
		)
	}
	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.running = true

	quorumSubId, quorumCh := m.eventBus.Subscribe(EbQuorumEventType)
	epochSubId, epochCh := m.eventBus.Subscribe(event.EpochTransitionEventType)
	chainSubId, chainCh := m.eventBus.Subscribe(chain.ChainUpdateEventType)
	m.subs = []managerSubscription{
		{eventType: EbQuorumEventType, id: quorumSubId},
		{eventType: event.EpochTransitionEventType, id: epochSubId},
		{eventType: chain.ChainUpdateEventType, id: chainSubId},
	}
	m.loopWg.Go(func() {
		m.eventLoop(childCtx, quorumCh, epochCh, chainCh)
		// If the loop exits because the parent context was cancelled
		// (not via Stop), reset running and unsubscribe so Start can be
		// called again without leaking a stale subscriber.
		m.mu.Lock()
		if !m.stopping {
			m.stopLocked()
		}
		m.mu.Unlock()
	})
	m.logger.Info("leios pipeline manager started")
	return nil
}

// stopLocked tears down running state. Callers must hold m.mu.
func (m *PipelineManager) stopLocked() {
	if !m.running {
		return
	}
	m.running = false
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	for _, sub := range m.subs {
		m.eventBus.Unsubscribe(sub.eventType, sub.id)
	}
	m.subs = nil
}

// Stop stops the pipeline manager.
func (m *PipelineManager) Stop() error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	m.stopLocked()
	m.mu.Unlock()

	// Wait outside the lock so the event loop's cleanup path can
	// re-acquire it.
	m.loopWg.Wait()

	m.mu.Lock()
	m.stopping = false
	m.mu.Unlock()
	m.logger.Info("leios pipeline manager stopped")
	return nil
}

// eventLoop dispatches subscribed events until the context is cancelled or
// the subscription channels close.
func (m *PipelineManager) eventLoop(
	ctx context.Context,
	quorumCh <-chan event.Event,
	epochCh <-chan event.Event,
	chainCh <-chan event.Event,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-quorumCh:
			if !ok {
				return
			}
			if data, ok := evt.Data.(EbQuorumEvent); ok {
				m.handleEbQuorum(data)
			}
		case evt, ok := <-epochCh:
			if !ok {
				return
			}
			if data, ok := evt.Data.(event.EpochTransitionEvent); ok {
				m.handleEpochTransition(data)
			}
		case evt, ok := <-chainCh:
			if !ok {
				return
			}
			if data, ok := evt.Data.(chain.ChainRollbackEvent); ok {
				m.handleRollback(data)
			}
		}
	}
}

// ObserveEndorserBlock registers an endorser block observed for a slot
// (received and cached by the ouroboros component) into the pipeline. A
// second distinct EB hash for the same slot is recorded as equivocation.
// Repeated observations of the same (slot, hash) are idempotent.
func (m *PipelineManager) ObserveEndorserBlock(
	slot uint64,
	ebHash lcommon.Blake2b256,
) {
	cur := m.slotProvider.CurrentOrTipSlot()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredLocked(cur)

	// Reject observations outside the acceptance window before recording
	// anything. A far-future slot would otherwise let a peer pre-seed an
	// instance and deny the local producer (see MayProduceEndorserBlock);
	// a far-past slot can never become eligible. The slot is peer-supplied
	// here, unlike the quorum path, which is fed by the locally windowed
	// vote manager.
	if !m.withinObservationWindow(slot, cur) {
		m.logger.Debug(
			"ignoring out-of-window endorser block observation",
			"slot", slot,
			"current_slot", cur,
		)
		return
	}

	inst := m.instances[slot]
	if inst == nil {
		inst = &pipelineInstance{
			produceSlot: slot,
			epoch:       m.epochForSlot(slot),
			ebs:         make(map[lcommon.Blake2b256]*ebState),
		}
		m.instances[slot] = inst
	}
	if _, exists := inst.ebs[ebHash]; exists {
		return
	}
	eb := &ebState{hash: ebHash, slot: slot, epoch: inst.epoch}
	inst.ebs[ebHash] = eb
	m.byHash[ebHash] = eb
	if m.metrics != nil {
		m.metrics.ebObservedTotal.Inc()
	}
	m.markEquivocationLocked(inst)
	m.updateGaugesLocked(cur)
}

// handleEbQuorum marks an endorser block certified when the VoteManager
// reports its verified votes reached quorum, capturing the built
// certificate verbatim. If the certified EB's body was never observed (only
// its votes), it is tracked here so eligibility reflects every certified
// block.
func (m *PipelineManager) handleEbQuorum(evt EbQuorumEvent) {
	cur := m.slotProvider.CurrentOrTipSlot()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredLocked(cur)

	eb := m.byHash[evt.EndorserBlockHash]
	if eb == nil {
		inst := m.instances[evt.SlotNo]
		if inst == nil {
			inst = &pipelineInstance{
				produceSlot: evt.SlotNo,
				epoch:       evt.Epoch,
				ebs:         make(map[lcommon.Blake2b256]*ebState),
			}
			m.instances[evt.SlotNo] = inst
		}
		eb = &ebState{
			hash:  evt.EndorserBlockHash,
			slot:  evt.SlotNo,
			epoch: inst.epoch,
		}
		inst.ebs[evt.EndorserBlockHash] = eb
		m.byHash[evt.EndorserBlockHash] = eb
		m.markEquivocationLocked(inst)
	}
	if _, exists := eb.certificates[evt.AnnouncingRbHash]; exists {
		return
	}
	// A certificate at or past the certification deadline is too late: the
	// EB has already expired in stageFor's uncertified branch, so honoring it
	// would resurrect a block the rest of the pipeline treats as dead. Keep
	// it tracked (it still counts toward equivocation and the stage gauge)
	// but never mark it certified, so EligibleCertifiedEbs and StageOf agree
	// it is expired.
	if cur >= evt.SlotNo && cur-evt.SlotNo >= m.timing.CertifyByDeadlineSlots {
		if m.metrics != nil {
			m.metrics.certsRejectedTotal.WithLabelValues("late").Inc()
		}
		m.logger.Warn(
			"discarding leios certificate past certification deadline",
			"slot", evt.SlotNo,
			"eb_hash", evt.EndorserBlockHash.String(),
			"offset_slots", cur-evt.SlotNo,
			"deadline_slots", m.timing.CertifyByDeadlineSlots,
		)
		m.updateGaugesLocked(cur)
		return
	}
	firstCertification := !eb.certified
	eb.certified = true
	if eb.certificates == nil {
		eb.certificates = make(
			map[lcommon.Blake2b256]*lcommon.LeiosEbCertificate,
		)
	}
	eb.certificates[evt.AnnouncingRbHash] = evt.Certificate
	if m.metrics != nil && firstCertification {
		m.metrics.ebCertifiedTotal.Inc()
	}
	m.logger.Info(
		"leios endorser block certified",
		"slot", evt.SlotNo,
		"eb_hash", evt.EndorserBlockHash.String(),
		"eligible", !eb.equivocated,
	)
	m.updateGaugesLocked(cur)
}

// MayProduceEndorserBlock reports whether an endorser block may be forged
// for the given slot at the current slot. It is the producer-facing seam
// for the forge loop (#1862); this package does not forge.
func (m *PipelineManager) MayProduceEndorserBlock(
	slot uint64,
) (ProduceDecision, error) {
	cur := m.slotProvider.CurrentOrTipSlot()
	windowEnd := slot + m.timing.ProduceWindowSlots
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredLocked(cur)

	decision := ProduceDecision{Slot: slot, WindowEnd: windowEnd}
	if inst, ok := m.instances[slot]; ok && len(inst.ebs) > 0 {
		decision.Reason = "endorser block already observed for slot"
		return decision, nil
	}
	if cur < slot {
		decision.Reason = "produce window not yet open"
		return decision, nil
	}
	if cur >= windowEnd {
		decision.Reason = "produce window closed"
		return decision, nil
	}
	decision.Allowed = true
	return decision, nil
}

// EligibleCertifiedEbs returns the certified, non-equivocated, not-yet-
// embedded endorser blocks currently within their ranking-block inclusion
// window, sorted by slot. The actual embedding is the caller's
// responsibility (RB CDDL work is out of scope here).
func (m *PipelineManager) EligibleCertifiedEbs() []EligibleEb {
	cur := m.slotProvider.CurrentOrTipSlot()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneExpiredLocked(cur)

	var out []EligibleEb
	for _, inst := range m.instances {
		for _, eb := range inst.ebs {
			if !eb.certified || eb.equivocated || eb.embedded {
				continue
			}
			if stageFor(
				inst.produceSlot,
				cur,
				m.timing,
				true,
			) != StageEligible {
				continue
			}
			for rbHash, cert := range eb.certificates {
				out = append(out, EligibleEb{
					SlotNo:            inst.produceSlot,
					EndorserBlockHash: eb.hash,
					Certificate:       cert,
					Epoch:             inst.epoch,
					AnnouncingRbHash:  rbHash,
				})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SlotNo < out[j].SlotNo
	})
	return out
}

// MarkEmbedded records that a certified endorser block was included in a
// ranking block, so it is not offered for inclusion again.
func (m *PipelineManager) MarkEmbedded(ebHash lcommon.Blake2b256) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if eb, ok := m.byHash[ebHash]; ok {
		eb.embedded = true
	}
}

// StageOf reports the pipeline stage of a tracked endorser block and whether
// it is tracked. It is the read-only introspection seam for the producer and
// inclusion paths; it never mutates pipeline state (no pruning), so it is
// safe to call at high frequency. The stage is derived from the current slot
// and the EB's observed certified flag via stageFor.
func (m *PipelineManager) StageOf(
	slot uint64,
	ebHash lcommon.Blake2b256,
) (Stage, bool) {
	cur := m.slotProvider.CurrentOrTipSlot()
	m.mu.Lock()
	defer m.mu.Unlock()
	eb, ok := m.byHash[ebHash]
	if !ok || eb.slot != slot {
		return StageProduce, false
	}
	return stageFor(slot, cur, m.timing, eb.certified), true
}

// markEquivocationLocked flags every EB in an instance as equivocated once
// the instance holds more than one distinct EB hash. Without an EB producer
// identity (the CIP-0164 endorser block carries none yet) we cannot pick a
// winner, so all are excluded from inclusion. The metric and warn log fire
// once per equivocating slot, not once per additional EB. Callers must hold
// m.mu.
func (m *PipelineManager) markEquivocationLocked(inst *pipelineInstance) {
	if len(inst.ebs) <= 1 {
		return
	}
	for _, eb := range inst.ebs {
		eb.equivocated = true
	}
	if inst.equivocated {
		// This slot was already counted as equivocating.
		return
	}
	inst.equivocated = true
	if m.metrics != nil {
		m.metrics.ebEquivocationTotal.Inc()
	}
	m.logger.Warn(
		"leios endorser block equivocation detected",
		"slot", inst.produceSlot,
		"eb_count", len(inst.ebs),
	)
}

// pruneExpiredLocked drops pipeline instances whose produce slot is more
// than InstanceTTLSlots behind the current slot. Callers must hold m.mu.
func (m *PipelineManager) pruneExpiredLocked(cur uint64) {
	for slot, inst := range m.instances {
		if cur < inst.produceSlot {
			continue
		}
		if cur-inst.produceSlot < m.timing.InstanceTTLSlots {
			continue
		}
		for h := range inst.ebs {
			delete(m.byHash, h)
		}
		delete(m.instances, slot)
	}
	m.updateGaugesLocked(cur)
}

// handleEpochTransition flushes pipeline instances older than the previous
// epoch. The previous epoch is retained so in-flight blocks near the
// boundary survive. Committee/stake-snapshot rotation needs no work here:
// the pipeline consumes already-built certificates via EbQuorumEvent and
// inherits VoteManager's active snapshot selection transparently.
func (m *PipelineManager) handleEpochTransition(
	evt event.EpochTransitionEvent,
) {
	var keepFrom uint64
	if evt.NewEpoch >= 1 {
		keepFrom = evt.NewEpoch - 1
	}
	cur := m.slotProvider.CurrentOrTipSlot()
	m.mu.Lock()
	defer m.mu.Unlock()
	for slot, inst := range m.instances {
		if inst.epoch < keepFrom {
			for h := range inst.ebs {
				delete(m.byHash, h)
			}
			delete(m.instances, slot)
		}
	}
	m.updateGaugesLocked(cur)
	m.logger.Debug(
		"flushed leios pipeline state at epoch transition",
		"new_epoch", evt.NewEpoch,
		"retained_instances", len(m.instances),
	)
}

// handleRollback drops pipeline instances whose produce slot is past the
// rollback point, so a re-produced endorser block on the replacement chain
// is not mistaken for equivocation.
func (m *PipelineManager) handleRollback(evt chain.ChainRollbackEvent) {
	cur := m.slotProvider.CurrentOrTipSlot()
	m.mu.Lock()
	defer m.mu.Unlock()
	for slot, inst := range m.instances {
		if inst.produceSlot > evt.Point.Slot {
			for h := range inst.ebs {
				delete(m.byHash, h)
			}
			delete(m.instances, slot)
		}
	}
	m.updateGaugesLocked(cur)
	m.logger.Debug(
		"flushed leios pipeline state after rollback",
		"rollback_slot", evt.Point.Slot,
		"retained_instances", len(m.instances),
	)
}

// withinObservationWindow reports whether an endorser block observed for
// slot should be admitted, given the current (or tip) slot. Future slots
// are bounded by a small clock-skew/diffusion tolerance so a peer cannot
// pre-seed arbitrary future slots; far-past slots already older than the
// instance TTL are rejected since they can never become eligible.
func (m *PipelineManager) withinObservationWindow(slot, cur uint64) bool {
	if slot > cur {
		return slot-cur <= observeFutureToleranceSlots
	}
	return cur-slot < m.timing.InstanceTTLSlots
}

// epochForSlot resolves a slot's epoch, falling back to the current epoch
// when the slot cannot be resolved (e.g. far-future projection error). The
// epoch is used only for boundary flushing, so a best-effort value is safe.
func (m *PipelineManager) epochForSlot(slot uint64) uint64 {
	epoch, err := m.epochProvider.EpochForSlot(slot)
	if err != nil {
		m.logger.Debug(
			"could not resolve epoch for slot; using current epoch",
			"slot", slot,
			"error", err,
		)
		return m.epochProvider.CurrentEpoch()
	}
	return epoch
}

// updateGaugesLocked refreshes the instance-count and per-stage gauges.
// Iterating instances and calling stageFor with each EB's actual certified
// flag here is what exercises stageFor's uncertified branch in production;
// nothing else calls stageFor with certified=false. Every stage label is
// rewritten on each refresh (including zeros), so a stage that empties is
// set to 0 rather than left stale. Callers must hold m.mu.
func (m *PipelineManager) updateGaugesLocked(cur uint64) {
	if m.metrics == nil {
		return
	}
	m.metrics.instancesCount.Set(float64(len(m.instances)))
	var counts [stageCount]int
	for _, inst := range m.instances {
		for _, eb := range inst.ebs {
			counts[stageFor(inst.produceSlot, cur, m.timing, eb.certified)]++
		}
	}
	for s := range stageCount {
		m.metrics.stagesCount.WithLabelValues(Stage(s).String()).
			Set(float64(counts[s]))
	}
}
