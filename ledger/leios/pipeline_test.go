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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pipelineFixture wires a PipelineManager against the in-package fake
// providers (reused from manager_test.go). The fake epoch provider maps a
// slot to slot/testSlotsPerEpoch (testSlotsPerEpoch == 100).
type pipelineFixture struct {
	mgr      *PipelineManager
	eventBus *event.EventBus
	slot     *fakeSlotProvider
	epoch    *fakeEpochProvider
}

func newPipelineFixture(
	t *testing.T,
	timing PipelineTiming,
) *pipelineFixture {
	t.Helper()
	eb := event.NewEventBus(nil, nil)
	sp := &fakeSlotProvider{slot: 0}
	ep := &fakeEpochProvider{currentEpoch: 0}
	mgr, err := NewPipelineManager(PipelineManagerConfig{
		EventBus:      eb,
		SlotProvider:  sp,
		EpochProvider: ep,
		Timing:        timing,
	})
	require.NoError(t, err)
	return &pipelineFixture{mgr: mgr, eventBus: eb, slot: sp, epoch: ep}
}

func ebHashFor(s string) lcommon.Blake2b256 {
	return lcommon.NewBlake2b256([]byte(s))
}

func TestStageFor(t *testing.T) {
	// Default timing: produce<1, diffuse<5, vote<10, certify<20,
	// inclusion<40, ttl<100 (cumulative offsets from the produce slot).
	tm := DefaultPipelineTiming()
	const ps = 1000 // produce slot
	tests := []struct {
		name      string
		now       uint64
		certified bool
		want      Stage
	}{
		{"before produce slot", ps - 3, false, StageProduce},
		{"produce window", ps + 0, false, StageProduce},
		{"diffuse window low", ps + 1, false, StageDiffuse},
		{"diffuse window high", ps + 4, false, StageDiffuse},
		{"vote window low", ps + 5, false, StageVote},
		{"vote window high", ps + 9, false, StageVote},
		{"certify window low", ps + 10, false, StageCertify},
		{"certify window high", ps + 19, false, StageCertify},
		{"past certify deadline uncertified", ps + 20, false, StageExpired},
		{"far past uncertified", ps + 99, false, StageExpired},
		{"certified early is eligible", ps + 0, true, StageEligible},
		{"certified mid inclusion", ps + 39, true, StageEligible},
		{"certified past inclusion window", ps + 40, true, StageExpired},
		{"certified past ttl", ps + 100, true, StageExpired},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := stageFor(ps, tc.now, tm, tc.certified)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestPipelineTimingValidate(t *testing.T) {
	require.NoError(t, DefaultPipelineTiming().Validate())

	zero := DefaultPipelineTiming()
	zero.VoteWindowSlots = 0
	require.Error(t, zero.Validate())

	nonMonotonic := DefaultPipelineTiming()
	// certify deadline earlier than the vote window violates ordering
	nonMonotonic.CertifyByDeadlineSlots = 1
	require.Error(t, nonMonotonic.Validate())
}

func TestNewPipelineManagerValidation(t *testing.T) {
	good := PipelineManagerConfig{
		EventBus:      event.NewEventBus(nil, nil),
		SlotProvider:  &fakeSlotProvider{},
		EpochProvider: &fakeEpochProvider{},
		Timing:        DefaultPipelineTiming(),
	}
	_, err := NewPipelineManager(good)
	require.NoError(t, err)

	noBus := good
	noBus.EventBus = nil
	_, err = NewPipelineManager(noBus)
	require.Error(t, err)

	badTiming := good
	badTiming.Timing = PipelineTiming{}
	_, err = NewPipelineManager(badTiming)
	require.Error(t, err)
}

func TestMayProduceEndorserBlock(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	const slot = 500

	// Current slot equals the produce slot: allowed.
	f.slot.slot = slot
	d, err := f.mgr.MayProduceEndorserBlock(slot)
	require.NoError(t, err)
	assert.True(t, d.Allowed)
	assert.Equal(t, uint64(slot+1), d.WindowEnd)

	// Before the produce slot: window not yet open.
	f.slot.slot = slot - 1
	d, err = f.mgr.MayProduceEndorserBlock(slot)
	require.NoError(t, err)
	assert.False(t, d.Allowed)
	assert.Contains(t, d.Reason, "not yet open")

	// After the produce window: closed.
	f.slot.slot = slot + 1
	d, err = f.mgr.MayProduceEndorserBlock(slot)
	require.NoError(t, err)
	assert.False(t, d.Allowed)
	assert.Contains(t, d.Reason, "closed")

	// An EB already observed for the slot blocks production.
	f.slot.slot = slot
	f.mgr.ObserveEndorserBlock(slot, ebHashFor("eb"))
	d, err = f.mgr.MayProduceEndorserBlock(slot)
	require.NoError(t, err)
	assert.False(t, d.Allowed)
	assert.Contains(t, d.Reason, "already observed")
}

func TestObserveEndorserBlockRejectsFutureSlots(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	f.slot.slot = 1000

	// A far-future slot must be ignored so a peer cannot pre-seed it.
	future := uint64(1000 + observeFutureToleranceSlots + 5)
	f.mgr.ObserveEndorserBlock(future, ebHashFor("future-eb"))
	f.mgr.mu.Lock()
	_, exists := f.mgr.instances[future]
	f.mgr.mu.Unlock()
	assert.False(t, exists, "far-future observations must be ignored")

	// The legitimate producer can still produce for that slot when it opens.
	f.slot.slot = future
	d, err := f.mgr.MayProduceEndorserBlock(future)
	require.NoError(t, err)
	assert.True(
		t,
		d.Allowed,
		"a pre-seeded future slot must not block production",
	)

	// A near-future observation within tolerance (clock skew) is accepted.
	f.slot.slot = 2000
	near := uint64(2000 + 1)
	f.mgr.ObserveEndorserBlock(near, ebHashFor("near-eb"))
	f.mgr.mu.Lock()
	_, nearExists := f.mgr.instances[near]
	f.mgr.mu.Unlock()
	assert.True(
		t,
		nearExists,
		"near-future observations within tolerance are accepted",
	)
}

func TestObserveAndCertifySingleEb(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	const slot = 300
	hash := ebHashFor("eb-a")
	cert := &lcommon.LeiosEbCertificate{SlotNo: slot, EndorserBlockHash: hash}

	f.slot.slot = slot
	f.mgr.ObserveEndorserBlock(slot, hash)
	// Idempotent re-observation does not create a duplicate or equivocation.
	f.mgr.ObserveEndorserBlock(slot, hash)

	// Not yet certified -> not eligible.
	assert.Empty(t, f.mgr.EligibleCertifiedEbs())

	f.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hash,
		Epoch:             0,
		Certificate:       cert,
	})

	// Within the inclusion window -> eligible, certificate captured verbatim.
	f.slot.slot = slot + 5
	eligible := f.mgr.EligibleCertifiedEbs()
	require.Len(t, eligible, 1)
	assert.Equal(t, uint64(slot), eligible[0].SlotNo)
	assert.Same(t, cert, eligible[0].Certificate)

	// Past the inclusion window -> no longer eligible.
	f.slot.slot = slot + DefaultPipelineTiming().RbInclusionWindowSlots
	assert.Empty(t, f.mgr.EligibleCertifiedEbs())
}

func TestMarkEmbeddedExcludesEb(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	const slot = 300
	hash := ebHashFor("eb-a")
	f.slot.slot = slot
	f.mgr.ObserveEndorserBlock(slot, hash)
	f.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hash,
		Certificate:       &lcommon.LeiosEbCertificate{},
	})
	f.slot.slot = slot + 5
	require.Len(t, f.mgr.EligibleCertifiedEbs(), 1)

	f.mgr.MarkEmbedded(hash)
	assert.Empty(t, f.mgr.EligibleCertifiedEbs())
}

func TestEbEquivocationExcludesFromEligibility(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	const slot = 300
	hashA := ebHashFor("eb-a")
	hashB := ebHashFor("eb-b")
	cert := &lcommon.LeiosEbCertificate{}

	f.slot.slot = slot
	f.mgr.ObserveEndorserBlock(slot, hashA)
	f.mgr.ObserveEndorserBlock(slot, hashB) // distinct EB, same slot

	// Both are certified, but both were flagged equivocated.
	f.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hashA,
		Certificate:       cert,
	})
	f.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hashB,
		Certificate:       cert,
	})

	f.slot.slot = slot + 5
	assert.Empty(
		t,
		f.mgr.EligibleCertifiedEbs(),
		"equivocated endorser blocks must never be eligible for inclusion",
	)
}

func TestEbEquivocationMetricCountsOncePerSlot(t *testing.T) {
	reg := prometheus.NewRegistry()
	mgr, err := NewPipelineManager(PipelineManagerConfig{
		EventBus:      event.NewEventBus(nil, nil),
		SlotProvider:  &fakeSlotProvider{slot: 300},
		EpochProvider: &fakeEpochProvider{},
		Timing:        DefaultPipelineTiming(),
		PromRegistry:  reg,
	})
	require.NoError(t, err)

	const slot = 300
	// Three distinct EBs for one slot is still a single equivocating slot.
	mgr.ObserveEndorserBlock(slot, ebHashFor("eb-a"))
	mgr.ObserveEndorserBlock(slot, ebHashFor("eb-b"))
	mgr.ObserveEndorserBlock(slot, ebHashFor("eb-c"))

	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(mgr.metrics.ebEquivocationTotal),
		"equivocation metric must count slots, not additional EBs",
	)
}

func TestEbQuorumPastCertifyDeadlineRejected(t *testing.T) {
	tm := DefaultPipelineTiming()
	const slot = 300
	hash := ebHashFor("eb-late")

	// A certificate arriving one slot before the deadline is accepted.
	inTime := newPipelineFixture(t, tm)
	inTime.slot.slot = slot
	inTime.mgr.ObserveEndorserBlock(slot, hash)
	inTime.slot.slot = slot + tm.CertifyByDeadlineSlots - 1
	inTime.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hash,
		Certificate:       &lcommon.LeiosEbCertificate{},
	})
	require.Len(t, inTime.mgr.EligibleCertifiedEbs(), 1)

	// A certificate arriving at the deadline offset is too late: the EB
	// stays tracked but is never certified, so it can never become eligible.
	late := newPipelineFixture(t, tm)
	late.slot.slot = slot
	late.mgr.ObserveEndorserBlock(slot, hash)
	late.slot.slot = slot + tm.CertifyByDeadlineSlots
	late.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hash,
		Certificate:       &lcommon.LeiosEbCertificate{},
	})
	assert.Empty(t, late.mgr.EligibleCertifiedEbs())
	late.mgr.mu.Lock()
	eb := late.mgr.byHash[hash]
	late.mgr.mu.Unlock()
	require.NotNil(t, eb, "a late-certified EB stays tracked")
	assert.False(t, eb.certified, "a late certificate must not certify the EB")
}

func TestLateCertMetricCounted(t *testing.T) {
	reg := prometheus.NewRegistry()
	tm := DefaultPipelineTiming()
	mgr, err := NewPipelineManager(PipelineManagerConfig{
		EventBus:      event.NewEventBus(nil, nil),
		SlotProvider:  &fakeSlotProvider{slot: 300 + tm.CertifyByDeadlineSlots},
		EpochProvider: &fakeEpochProvider{},
		Timing:        tm,
		PromRegistry:  reg,
	})
	require.NoError(t, err)

	mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            300,
		EndorserBlockHash: ebHashFor("eb-late"),
		Certificate:       &lcommon.LeiosEbCertificate{},
	})
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(mgr.metrics.certsRejectedTotal.WithLabelValues("late")),
	)
}

func TestStageGaugesReflectUncertifiedStages(t *testing.T) {
	reg := prometheus.NewRegistry()
	mgr, err := NewPipelineManager(PipelineManagerConfig{
		EventBus:      event.NewEventBus(nil, nil),
		SlotProvider:  &fakeSlotProvider{slot: 1000},
		EpochProvider: &fakeEpochProvider{},
		Timing:        DefaultPipelineTiming(),
		PromRegistry:  reg,
	})
	require.NoError(t, err)
	sp := mgr.slotProvider.(*fakeSlotProvider)

	// Observed at the current slot -> offset 0 -> produce stage.
	mgr.ObserveEndorserBlock(1000, ebHashFor("eb-p"))
	stageGauge := func(s Stage) float64 {
		return promtestutil.ToFloat64(
			mgr.metrics.stagesCount.WithLabelValues(s.String()),
		)
	}
	assert.Equal(t, float64(1), stageGauge(StageProduce))
	assert.Equal(t, float64(0), stageGauge(StageVote))

	// Advance into the vote band (offset 6) and refresh via a query path:
	// this drives stageFor's uncertified branch in production.
	sp.slot = 1006
	_ = mgr.EligibleCertifiedEbs()
	assert.Equal(t, float64(1), stageGauge(StageVote))
	assert.Equal(t, float64(0), stageGauge(StageProduce))
}

func TestStageOf(t *testing.T) {
	tm := DefaultPipelineTiming()
	f := newPipelineFixture(t, tm)
	const slot = 500
	hash := ebHashFor("eb")
	f.slot.slot = slot
	f.mgr.ObserveEndorserBlock(slot, hash)

	st, ok := f.mgr.StageOf(slot, hash)
	require.True(t, ok)
	assert.Equal(t, StageProduce, st)

	// Past the certify deadline without a certificate -> expired.
	f.slot.slot = slot + tm.CertifyByDeadlineSlots
	st, ok = f.mgr.StageOf(slot, hash)
	require.True(t, ok)
	assert.Equal(t, StageExpired, st)

	// An unknown hash is not tracked.
	_, ok = f.mgr.StageOf(slot, ebHashFor("missing"))
	assert.False(t, ok)
}

func TestEbQuorumForUnobservedEb(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	const slot = 200
	hash := ebHashFor("eb-unseen")
	cert := &lcommon.LeiosEbCertificate{}

	// A quorum arrives for an EB whose body was never observed (only its
	// votes). It is still tracked so eligibility reflects every certified
	// block.
	f.mgr.handleEbQuorum(EbQuorumEvent{
		SlotNo:            slot,
		EndorserBlockHash: hash,
		Epoch:             0,
		Certificate:       cert,
	})
	f.slot.slot = slot
	eligible := f.mgr.EligibleCertifiedEbs()
	require.Len(t, eligible, 1)
	assert.Equal(t, uint64(slot), eligible[0].SlotNo)
}

func TestEpochTransitionFlush(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	// slot 50 -> epoch 0, slot 120 -> epoch 1 (fake epoch provider).
	f.slot.slot = 50
	f.mgr.ObserveEndorserBlock(50, ebHashFor("eb-old"))
	f.slot.slot = 120
	f.mgr.ObserveEndorserBlock(120, ebHashFor("eb-new"))

	f.mgr.handleEpochTransition(event.EpochTransitionEvent{
		PreviousEpoch: 1,
		NewEpoch:      2,
	})

	// keepFrom = NewEpoch-1 = 1: the epoch-0 instance is dropped, epoch-1
	// retained.
	f.mgr.mu.Lock()
	_, hasOld := f.mgr.instances[50]
	_, hasNew := f.mgr.instances[120]
	f.mgr.mu.Unlock()
	assert.False(t, hasOld)
	assert.True(t, hasNew)
}

func TestRollbackFlush(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	f.slot.slot = 80
	f.mgr.ObserveEndorserBlock(50, ebHashFor("eb-kept"))
	f.mgr.ObserveEndorserBlock(80, ebHashFor("eb-dropped"))

	f.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: 60},
	})

	f.mgr.mu.Lock()
	_, hasKept := f.mgr.instances[50]
	_, hasDropped := f.mgr.instances[80]
	f.mgr.mu.Unlock()
	assert.True(t, hasKept, "instances at or before the rollback point remain")
	assert.False(t, hasDropped, "instances past the rollback point are dropped")
}

func TestPruneExpiredInstances(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	f.slot.slot = 10
	f.mgr.ObserveEndorserBlock(10, ebHashFor("eb"))

	f.mgr.mu.Lock()
	require.Len(t, f.mgr.instances, 1)
	f.mgr.mu.Unlock()

	// Advance past the instance TTL; a query triggers lazy pruning.
	f.slot.slot = 10 + DefaultPipelineTiming().InstanceTTLSlots
	_ = f.mgr.EligibleCertifiedEbs()

	f.mgr.mu.Lock()
	assert.Empty(t, f.mgr.instances)
	assert.Empty(t, f.mgr.byHash)
	f.mgr.mu.Unlock()
}

func TestPipelineLifecycleAndEventDispatch(t *testing.T) {
	f := newPipelineFixture(t, DefaultPipelineTiming())
	require.NoError(t, f.mgr.Start(context.Background()))
	// Start is idempotent.
	require.NoError(t, f.mgr.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, f.mgr.Stop()) })

	const slot = 400
	hash := ebHashFor("eb-live")
	f.slot.slot = slot
	f.mgr.ObserveEndorserBlock(slot, hash)

	// A quorum event published on the bus must drive the EB to eligibility
	// through the running event loop.
	f.eventBus.Publish(
		EbQuorumEventType,
		event.NewEvent(EbQuorumEventType, EbQuorumEvent{
			SlotNo:            slot,
			EndorserBlockHash: hash,
			Epoch:             0,
			Certificate:       &lcommon.LeiosEbCertificate{},
		}),
	)

	testutil.WaitForCondition(t, func() bool {
		return len(f.mgr.EligibleCertifiedEbs()) == 1
	}, 2*time.Second, "eb becomes eligible after quorum event is dispatched")
}
