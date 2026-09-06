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
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Timings taken from a block producer measured on a 1s-slot Leios network:
// the endorser block is in hand a median of one slot after the announcing
// ranking block's slot, but that ranking block only finishes applying a
// median of 32 slots later, because applying it is what waits on fetching
// the endorser block. The vote window is 10 slots wide.
const (
	headerArmingRbSlot        = 577
	headerArmingEbAcquiredAt  = headerArmingRbSlot + 1
	headerArmingRbAppliedAt   = headerArmingRbSlot + 32
	headerArmingVoteWindow    = 10
	headerArmingSeatedVoterId = 3
)

// syncBuffer is a bytes.Buffer safe for the vote manager's event loop
// goroutine to write log records into while the test reads them.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newHeaderArmingFixture builds a fixture whose local pool is seated on the
// committee with a loaded signing key, with the vote window and wall-clock
// slot under the test's control.
func newHeaderArmingFixture(
	t *testing.T,
	slots *fakeSlotProvider,
	extra ...func(*managerFixture, *VoteManagerConfig),
) *managerFixture {
	t.Helper()
	opts := []func(*managerFixture, *VoteManagerConfig){
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = slots
			cfg.VoteWindowSlots = headerArmingVoteWindow
		},
	}
	opts = append(opts, extra...)
	fixture := newManagerFixture(t, opts...)
	member := fixture.members[headerArmingSeatedVoterId]
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], member.PoolKeyHash)
	key := fixture.keys[headerArmingSeatedVoterId]
	require.NotNil(t, key)
	require.NoError(t, fixture.mgr.EnableVoting(poolKeyHash, key))
	return fixture
}

// publishHeaderAnnouncement delivers the announcement the way chainsync
// roll-forward does: from the ranking block's header, before the block body
// has been fetched or applied.
func publishHeaderAnnouncement(
	fixture *managerFixture,
	slot uint64,
	rbHash, ebHash lcommon.Blake2b256,
	seq uint64,
) {
	fixture.eventBus.Publish(
		chain.ChainHeaderEventType,
		event.NewEvent(
			chain.ChainHeaderEventType,
			chain.ChainHeaderAnnouncementEvent{
				Slot:   slot,
				RbHash: rbHash,
				EbHash: ebHash,
				EbSize: 1024,
				Seq:    seq,
			},
		),
	)
}

// TestVoteManagerVotesFromHeaderArrivalBeforeRankingBlockApplies is the
// regression test for a seated committee member that never emitted a vote.
// The announcement was armed only when the announcing ranking block applied,
// which for an EB-announcing block is a median of 32 slots after its own
// slot -- outside the 10-slot vote window it is measured against. The
// announcement is in the header and available from chainsync roll-forward
// long before that.
func TestVoteManagerVotesFromHeaderArrivalBeforeRankingBlockApplies(
	t *testing.T,
) {
	slots := &fakeSlotProvider{slot: headerArmingRbSlot}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))

	// The ranking block's header arrives from chainsync roll-forward at its
	// own slot. Its body has not been fetched, let alone applied.
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)

	// The announced endorser block is acquired one slot later.
	slots.setSlot(headerArmingEbAcquiredAt)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)

	emittedEvent := testutil.RequireReceive(
		t,
		emittedCh,
		2*time.Second,
		"vote emitted while the vote window is still open",
	)
	emitted, ok := emittedEvent.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash)
	assert.Equal(
		t,
		uint64(headerArmingSeatedVoterId),
		emitted.Vote.VoterId,
	)
	require.NoError(t, VerifyVoteSignature(
		fixture.keys[headerArmingSeatedVoterId].PublicKey(),
		PrototypeVoteMessageBytes(rbHash),
		emitted.Vote.VoteSignature,
	))

	// The announcing ranking block finally applies 32 slots later. By then
	// the vote window is long closed; the vote must already exist.
	slots.setSlot(headerArmingRbAppliedAt)
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"the post-apply backstop must not emit a second vote",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: headerArmingSeatedVoterId,
		}}),
		1,
		"exactly one vote for the announcement",
	)
}

// TestVoteManagerHeaderAndApplyArmingDoNotDoubleVote pins the idempotency of
// arming the same announcement twice. Both observations happen while the vote
// window is open, so only the per-ranking-block dedup can prevent the second
// vote.
func TestVoteManagerHeaderAndApplyArmingDoNotDoubleVote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	testutil.RequireReceive(
		t,
		emittedCh,
		2*time.Second,
		"vote emitted from the header path",
	)

	// The apply path observes the same announcement, still inside the vote
	// window, and both an EB re-acquisition and a repeated header
	// observation land on top of it.
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 2)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no duplicate vote for an announcement already voted on",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: headerArmingSeatedVoterId,
		}}),
		1,
		"exactly one vote for the announcement",
	)
}

// TestVoteManagerRolledBackHeaderAnnouncementDoesNotVote covers the risk
// header arming introduces: the announcing ranking block is not applied and
// may never be. If it is rolled away before the endorser block is acquired,
// no vote may be emitted for it.
func TestVoteManagerRolledBackHeaderAnnouncementDoesNotVote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))

	// Arm from the header. ObserveAnnouncement is the entrypoint the header
	// event handler calls; using it directly keeps the rollback ordering
	// below deterministic.
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)

	// A peer vote above the rollback point gives the test an observable
	// signal for the rollback having been processed.
	require.NoError(t, fixture.mgr.HandleVote(
		"conn-a",
		fixture.makeVote(t, 1, headerArmingRbSlot, ebHash),
	))
	fixture.eventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainRollbackEvent{
				Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
			},
		),
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo: headerArmingRbSlot, VoterId: 1,
		}})) == 0
	}, 2*time.Second, "rollback pruned state above the rollback point")

	// The endorser block arrives after the rollback. The announcement it
	// would have satisfied is gone, so nothing is emitted.
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no vote for an announcement rolled off our chain",
	)
	assert.Empty(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo:  headerArmingRbSlot,
			VoterId: headerArmingSeatedVoterId,
		}}),
	)
}

// TestVoteManagerSlotWindowDeclineIsCountedAndWarned covers the second half
// of the reported failure: the decline was logged at Debug and incremented no
// metric, so a permanently non-voting producer looked green on every health
// signal an operator checks.
func TestVoteManagerSlotWindowDeclineIsCountedAndWarned(t *testing.T) {
	reg := prometheus.NewRegistry()
	logBuf := &syncBuffer{}
	slots := &fakeSlotProvider{slot: 1000}
	fixture := newHeaderArmingFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = reg
			cfg.Logger = slog.New(slog.NewJSONHandler(
				logBuf,
				&slog.HandlerOptions{Level: slog.LevelWarn},
			))
		},
	)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	// An announcement whose ranking block slot is far behind the wall clock,
	// exactly what the apply-driven path used to hand the emitter.
	staleSlot := uint64(1000 - headerArmingVoteWindow - 100)
	fixture.mgr.HandleEndorserBlock(staleSlot, ebHash)
	fixture.mgr.ObserveAnnouncement(staleSlot, rbHash, ebHash)

	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo:  staleSlot,
		VoterId: headerArmingSeatedVoterId,
	}}))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
				voteNotEmittedSlotWindow,
			),
		),
		"slot-window decline is counted",
	)
	assert.Contains(
		t,
		logBuf.String(),
		"outside vote window",
		"a seated node holding a key warns rather than staying silent",
	)
	assert.True(
		t,
		strings.Contains(logBuf.String(), `"level":"WARN"`),
		"the decline is logged at warn level, got: %s",
		logBuf.String(),
	)
}

// TestVoteManagerVotesNotEmittedCountsMissingKey pins a second reason label so
// the counter is usable to tell "not configured" apart from "too late".
func TestVoteManagerVotesNotEmittedCountsMissingKey(t *testing.T) {
	reg := prometheus.NewRegistry()
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newManagerFixture(
		t,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.SlotProvider = slots
			cfg.VoteWindowSlots = headerArmingVoteWindow
			cfg.PromRegistry = reg
		},
	)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, rbHash, ebHash)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			fixture.mgr.metrics.votesNotEmittedTotal.WithLabelValues(
				voteNotEmittedNoKey,
			),
		),
	)
}

// publishHeaderInvalidation delivers the counterpart signal: queued headers
// above point left the chain without becoming blocks.
func publishHeaderInvalidation(
	fixture *managerFixture,
	slot uint64,
	reason string,
	seq uint64,
) {
	publishHeaderInvalidationNaming(fixture, slot, reason, seq, nil)
}

// publishHeaderInvalidationNaming is publishHeaderInvalidation for a discard
// the point cannot describe, where the chain grew past the dropped headers and
// they are named individually instead.
func publishHeaderInvalidationNaming(
	fixture *managerFixture,
	slot uint64,
	reason string,
	seq uint64,
	rbHashes []lcommon.Blake2b256,
) {
	fixture.eventBus.Publish(
		chain.ChainHeaderEventType,
		event.NewEvent(
			chain.ChainHeaderEventType,
			chain.ChainHeaderInvalidationEvent{
				Point:    ocommon.Point{Slot: slot},
				RbHashes: rbHashes,
				Reason:   reason,
				Seq:      seq,
			},
		),
	)
}

// TestVoteManagerInvalidatedHeaderAnnouncementDoesNotVote is the ordering
// regression test. The chain rolls back and then re-queues the peer's fork
// headers, so an announcement and the invalidation that voids it are produced
// back to back. They ride one event type precisely so the manager cannot
// observe them out of order: here the announcement is armed first and the
// invalidation follows, and the endorser block arriving afterwards must not
// produce a vote for a ranking block that is no longer on our chain.
func TestVoteManagerInvalidatedHeaderAnnouncementDoesNotVote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("orphaned-rb"))

	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 1)
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		2,
	)
	// Waiting on the invalidation's sequence is sound where waiting on an
	// announcement's is not: handleChainHeaderInvalidation advances the
	// watermark in the same critical section as its pruning, and both
	// handlers run on the one event-loop goroutine, so the announcement
	// ahead of it has already been applied in full.
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 2
	}, 2*time.Second, "invalidation applied")

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no vote for an announcement the chain invalidated",
	)
	assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}}))
}

// TestVoteManagerLateRollbackDoesNotDropRearmedAnnouncement is the other half
// of the same hazard. chain.update and the header stream are delivered on
// independent channels, so the ChainRollbackEvent for a fork resolution can
// arrive after the header stream has already replayed the winning fork's
// headers. Pruning announcements on that late rollback would delete the
// replacement chain's announcement and put the node back to not voting.
func TestVoteManagerLateRollbackDoesNotDropRearmedAnnouncement(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("replacement-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("replacement-rb"))

	// Chain-mutation order: roll back to S-1, then admit the replacement
	// chain's announcing header at S.
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		7,
	)
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 8)
	waitForAnnouncement(t, fixture, rbHash)

	// The matching rollback finally arrives on chain.update, carrying the
	// sequence number of the mutation the header stream already moved past.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   7,
	})

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	emitted := testutil.RequireReceive(
		t,
		emittedCh,
		2*time.Second,
		"vote for the replacement chain's announcement",
	)
	vote, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, vote.Vote.AnnouncingRbHash)
}

// TestVoteManagerUnsequencedRollbackStillPrunes keeps the pre-existing
// contract for a rollback that did not come from the chain's sequencer: with
// no sequence number there is nothing to supersede it, so it prunes as before.
func TestVoteManagerUnsequencedRollbackStillPrunes(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("orphaned-rb"))
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 4)
	waitForAnnouncement(t, fixture, rbHash)

	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
	})
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"an unsequenced rollback still prunes announcements",
	)
}

// TestVoteManagerHeaderStreamRecoversFromClosedChannel covers the header
// stream closing under the event loop. It is ordering-critical and the only
// thing that arms a vote inside the window, so losing it silently would put
// the node back to never voting. The loop must keep serving chain events and
// re-arm header delivery instead of exiting.
func TestVoteManagerHeaderStreamRecoversFromClosedChannel(t *testing.T) {
	reg := prometheus.NewRegistry()
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(
		t,
		slots,
		func(_ *managerFixture, cfg *VoteManagerConfig) {
			cfg.PromRegistry = reg
		},
	)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	// Close the manager's header subscription out from under the loop,
	// exactly as a bus-side detach would.
	fixture.mgr.mu.Lock()
	var headerSubId event.EventSubscriberId
	for _, sub := range fixture.mgr.subs {
		if sub.eventType == chain.ChainHeaderEventType {
			headerSubId = sub.id
		}
	}
	fixture.mgr.mu.Unlock()
	require.NotZero(t, headerSubId)
	fixture.eventBus.Unsubscribe(chain.ChainHeaderEventType, headerSubId)

	testutil.WaitForCondition(t, func() bool {
		return promtestutil.ToFloat64(
			fixture.mgr.metrics.headerStreamResubscribeTotal,
		) == 1
	}, 2*time.Second, "header stream resubscribed")

	// The replacement subscription arms announcements again.
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 3)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	emitted := testutil.RequireReceive(
		t,
		emittedCh,
		2*time.Second,
		"vote emitted after the header stream was recovered",
	)
	vote, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, vote.Vote.AnnouncingRbHash)
}

// TestVoteManagerNotEmittedReasonsMaterialized pins that every reason label
// exists from startup, so rate()/increase() have a series to work with on a
// node that has never emitted a vote -- which is the node this counter is for.
func TestVoteManagerNotEmittedReasonsMaterialized(t *testing.T) {
	reg := prometheus.NewRegistry()
	newManagerFixture(t, func(_ *managerFixture, cfg *VoteManagerConfig) {
		cfg.PromRegistry = reg
	})
	families, err := reg.Gather()
	require.NoError(t, err)
	var labels []string
	for _, family := range families {
		if family.GetName() != "dingo_metrics_leios_votes_not_emitted_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "reason" {
					labels = append(labels, pair.GetValue())
				}
			}
		}
	}
	assert.ElementsMatch(t, voteNotEmittedReasons, labels)
}

// waitForAnnouncement blocks until the announcement record itself is in the
// manager's map.
//
// Waiting on lastHeaderStreamSeq is not equivalent and must not be used for
// this: handleChainHeaderAnnouncement advances that watermark in its own
// critical section and only then calls observeAnnouncement, which resolves the
// epoch and re-acquires mu before inserting the record. A test that proceeds
// on the watermark can therefore read the map before the record exists.
func waitForAnnouncement(
	t *testing.T,
	fixture *managerFixture,
	rbHash lcommon.Blake2b256,
) {
	t.Helper()
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		_, ok := fixture.mgr.announcements[rbHash]
		return ok
	}, 2*time.Second, "announcement record present")
}

// armAndVote drives one announcement from header arrival to an emitted local
// vote and returns the emitted event, so the tests below start from a node
// that has genuinely voted rather than from hand-placed state.
func armAndVote(
	t *testing.T,
	fixture *managerFixture,
	emittedCh <-chan event.Event,
	slot uint64,
	rbHash, ebHash lcommon.Blake2b256,
	seq uint64,
) VoteEmittedEvent {
	t.Helper()
	publishHeaderAnnouncement(fixture, slot, rbHash, ebHash, seq)
	waitForAnnouncement(t, fixture, rbHash)
	fixture.mgr.HandleEndorserBlock(slot, ebHash)
	emitted := testutil.RequireReceive(
		t, emittedCh, 2*time.Second, "local vote emitted",
	)
	vote, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, rbHash, vote.Vote.AnnouncingRbHash)
	return vote
}

// TestVoteManagerLateRollbackKeepsReplacementVoteAndTally is the second half of
// the cross-stream ordering hazard. Protecting only the announcement is not
// enough: the vote, its tally, its dedup record and the acquired endorser
// block are all keyed by slot, and the replacement chain occupies the same
// slot, so a late rollback would erase a vote this node had already emitted
// correctly -- and, because the announcement survives and is marked voted,
// never re-emit it.
func TestVoteManagerLateRollbackKeepsReplacementVoteAndTally(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("replacement-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("replacement-rb"))

	// Chain-mutation order: roll back to S-1 (seq 7), then admit the
	// replacement chain's announcing header at S (seq 8), which this node
	// votes on.
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		7,
	)
	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, rbHash, ebHash, 8,
	)
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The matching rollback finally arrives on chain.update, carrying the
	// sequence number of the mutation the header stream already moved past.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   7,
	})

	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}),
		1,
		"the replacement chain's vote survives a superseded rollback",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, rbHash)
	assert.Contains(t, fixture.mgr.voteRecords, voteId)
	assert.Contains(t, fixture.mgr.acquiredEbs, ebHash)
	var tallied bool
	for key := range fixture.mgr.tallies {
		if key.announcingRbHash == rbHash {
			tallied = true
		}
	}
	assert.True(t, tallied, "the replacement chain's tally survives")
}

// TestVoteManagerLateRollbackStillPrunesAbandonedChainState is the guard's
// other side: state belonging to the chain the rollback abandons must still be
// removed, even while the replacement chain's state at the same slot is
// protected.
func TestVoteManagerLateRollbackStillPrunesAbandonedChainState(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)

	abandonedRb := lcommon.NewBlake2b256([]byte("abandoned-rb"))
	abandonedEb := lcommon.NewBlake2b256([]byte("abandoned-eb"))
	replacementRb := lcommon.NewBlake2b256([]byte("replacement-rb"))
	replacementEb := lcommon.NewBlake2b256([]byte("replacement-eb"))

	// Armed before the rollback (seq 3) and after it (seq 9), both above
	// the rollback point.
	publishHeaderAnnouncement(
		fixture, headerArmingRbSlot, abandonedRb, abandonedEb, 3,
	)
	publishHeaderAnnouncement(
		fixture, headerArmingRbSlot, replacementRb, replacementEb, 9,
	)
	waitForAnnouncement(t, fixture, abandonedRb)
	waitForAnnouncement(t, fixture, replacementRb)

	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   7,
	})

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(
		t,
		fixture.mgr.announcements,
		abandonedRb,
		"an announcement armed before the rollback is still pruned",
	)
	assert.Contains(
		t,
		fixture.mgr.announcements,
		replacementRb,
		"an announcement armed after the rollback is protected",
	)
}

// TestVoteManagerInvalidationDropsDerivedVoteAndAllowsRevote covers a header
// cleared *after* its endorser block arrived and the vote was emitted --
// blockfetch startup failing on an admitted announcing header, for instance.
// Leaving the vote, tally and dedup record behind would keep the (slot, voter)
// vote id occupied, so the replacement chain's vote at the same slot would be
// read as equivocation and dropped.
func TestVoteManagerInvalidationDropsDerivedVoteAndAllowsRevote(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	orphanRb := lcommon.NewBlake2b256([]byte("orphan-rb"))
	orphanEb := lcommon.NewBlake2b256([]byte("orphan-eb"))
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, orphanRb, orphanEb, 1,
	)
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The queue holding that header is discarded.
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationQueueCleared,
		2,
	)
	testutil.WaitForCondition(t, func() bool {
		return len(
			fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}),
		) == 0
	}, 2*time.Second, "the vote derived from the cleared header is dropped")

	fixture.mgr.mu.Lock()
	assert.NotContains(t, fixture.mgr.announcements, orphanRb)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, orphanRb)
	assert.NotContains(t, fixture.mgr.voteRecords, voteId)
	assert.NotContains(t, fixture.mgr.acquiredEbs, orphanEb)
	assert.Empty(t, fixture.mgr.tallies)
	fixture.mgr.mu.Unlock()

	// The vote id is free again, so the replacement chain's announcing
	// block at the same slot is voted on rather than being read as
	// equivocation.
	replacementRb := lcommon.NewBlake2b256([]byte("replacement-rb"))
	replacementEb := lcommon.NewBlake2b256([]byte("replacement-eb"))
	revote := armAndVote(
		t,
		fixture,
		emittedCh,
		headerArmingRbSlot,
		replacementRb,
		replacementEb,
		3,
	)
	assert.Equal(t, replacementRb, revote.Vote.AnnouncingRbHash)
	assert.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)
}

// TestVoteManagerInvalidationKeepsUnrelatedAnnouncementState pins that the
// derived-state cleanup is keyed by announcing ranking block, not swept by
// slot: an announcement the invalidation does not cover keeps its own vote,
// tally, dedup record and acquired endorser block.
func TestVoteManagerInvalidationKeepsUnrelatedAnnouncementState(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	// Below the invalidation point: survives.
	keptRb := lcommon.NewBlake2b256([]byte("kept-rb"))
	keptEb := lcommon.NewBlake2b256([]byte("kept-eb"))
	keptSlot := uint64(headerArmingRbSlot - 5)
	armAndVote(t, fixture, emittedCh, keptSlot, keptRb, keptEb, 1)
	keptVoteId := lcommon.LeiosVoteId{
		SlotNo:  keptSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	// Above the invalidation point: dropped, along with everything derived
	// from it.
	orphanRb := lcommon.NewBlake2b256([]byte("orphan-rb"))
	orphanEb := lcommon.NewBlake2b256([]byte("orphan-eb"))
	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, orphanRb, orphanEb, 2,
	)
	orphanVoteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{
		keptVoteId, orphanVoteId,
	}), 2)

	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		3,
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{orphanVoteId},
		)) == 0
	}, 2*time.Second, "the invalidated announcement's vote is dropped")

	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{keptVoteId}),
		1,
		"an announcement the invalidation does not cover keeps its vote",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, keptRb)
	assert.Contains(t, fixture.mgr.voteRecords, keptVoteId)
	assert.Contains(t, fixture.mgr.acquiredEbs, keptEb)
	assert.NotContains(t, fixture.mgr.announcements, orphanRb)
	assert.NotContains(t, fixture.mgr.voteRecords, orphanVoteId)
	assert.NotContains(t, fixture.mgr.acquiredEbs, orphanEb)
	var keptTally, orphanTally bool
	for key := range fixture.mgr.tallies {
		switch key.announcingRbHash {
		case keptRb:
			keptTally = true
		case orphanRb:
			orphanTally = true
		}
	}
	assert.True(t, keptTally, "the surviving announcement keeps its tally")
	assert.False(t, orphanTally, "the invalidated announcement's tally is gone")
}

// TestVoteManagerRollbackDeliveredBeforeHeaderStreamIsSafe answers the
// remaining two-channel case directly. chain.update and chain.header are still
// separate subscriptions, so the event loop's select can process a rollback
// before header events that the chain produced earlier. That inversion is now
// harmless rather than prevented: the rollback's own invalidation rides the
// header stream behind the announcement it voids, so the authoritative removal
// still happens in chain-mutation order, and the rollback's slot sweep is
// sequence-guarded so it cannot delete anything the header stream armed later.
func TestVoteManagerRollbackDeliveredBeforeHeaderStreamIsSafe(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	ebHash := lcommon.NewBlake2b256([]byte("orphan-eb"))
	rbHash := lcommon.NewBlake2b256([]byte("orphan-rb"))

	// Chain-mutation order is: announcing header admitted (seq 3), then
	// rolled back (seq 4). The rollback wins the race to the manager and is
	// applied before either header event.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: headerArmingRbSlot - 1},
		Seq:   4,
	})

	// The header stream then delivers both events, still in order.
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash, 3)
	publishHeaderInvalidation(
		fixture,
		headerArmingRbSlot-1,
		chain.HeaderInvalidationRollback,
		4,
	)
	// The invalidation's sequence is the safe watermark to wait on: it is
	// advanced in the same critical section as the pruning, behind the
	// announcement it voids on the one event-loop goroutine.
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 4
	}, 2*time.Second, "header stream drained")

	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, ebHash)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		500*time.Millisecond,
		"no vote for a header the rollback removed, whatever order the two streams arrived in",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, rbHash)
}

// TestVoteManagerLocalBlockInvalidationDropsNamedAnnouncement covers the
// discard a locally forged block causes. The chain grows rather than shrinks,
// so the discarded peer header can sit at or below the new tip and the point
// alone cannot name it -- the invalidation names it explicitly. Without this,
// a producer that admits an announcing header and then forges on the same
// parent keeps that announcement armed and votes for a ranking block that is
// not on its chain, leaving the vote id occupied by an abandoned fork.
func TestVoteManagerLocalBlockInvalidationDropsNamedAnnouncement(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}
	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, peerRb, peerEb, 1,
	)
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The locally forged block lands at a HIGHER slot than the discarded
	// peer header, so a point-based rule would keep the announcement.
	publishHeaderInvalidationNaming(
		fixture,
		headerArmingRbSlot+10,
		chain.HeaderInvalidationLocalBlock,
		2,
		[]lcommon.Blake2b256{peerRb},
	)
	testutil.WaitForCondition(t, func() bool {
		return len(
			fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}),
		) == 0
	}, 2*time.Second, "the discarded header's vote is dropped")

	fixture.mgr.mu.Lock()
	assert.NotContains(t, fixture.mgr.announcements, peerRb)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, peerRb)
	assert.NotContains(t, fixture.mgr.voteRecords, voteId)
	assert.NotContains(t, fixture.mgr.acquiredEbs, peerEb)
	assert.Empty(t, fixture.mgr.tallies)
	fixture.mgr.mu.Unlock()

	// The vote id is free, so the block that actually won the slot can be
	// voted on.
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))
	revote := armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, localRb, localEb, 3,
	)
	assert.Equal(t, localRb, revote.Vote.AnnouncingRbHash)
}

// TestVoteManagerLocalBlockInvalidationKeepsUnnamedAnnouncements pins that a
// named-header invalidation drops only what it names. The forged block's own
// announcement, and any other header still on the chain, must survive -- the
// producer must not invalidate its own vote.
func TestVoteManagerLocalBlockInvalidationKeepsUnnamedAnnouncements(
	t *testing.T,
) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	discardedRb := lcommon.NewBlake2b256([]byte("discarded-rb"))
	discardedEb := lcommon.NewBlake2b256([]byte("discarded-eb"))
	keptRb := lcommon.NewBlake2b256([]byte("kept-rb"))
	keptEb := lcommon.NewBlake2b256([]byte("kept-eb"))
	keptSlot := uint64(headerArmingRbSlot - 5)

	armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, discardedRb, discardedEb, 1,
	)
	armAndVote(t, fixture, emittedCh, keptSlot, keptRb, keptEb, 2)
	keptVoteId := lcommon.LeiosVoteId{
		SlotNo:  keptSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	// Point is above both announcements, so only the naming saves the one
	// that is still on the chain.
	publishHeaderInvalidationNaming(
		fixture,
		headerArmingRbSlot+10,
		chain.HeaderInvalidationLocalBlock,
		3,
		[]lcommon.Blake2b256{discardedRb},
	)
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		_, still := fixture.mgr.announcements[discardedRb]
		return !still
	}, 2*time.Second, "named announcement dropped")

	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{keptVoteId}),
		1,
		"an announcement the invalidation does not name keeps its vote",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, keptRb)
	assert.Contains(t, fixture.mgr.voteRecords, keptVoteId)
	assert.Contains(t, fixture.mgr.acquiredEbs, keptEb)
}

// TestVoteManagerSameSlotLocalForgeStillVotes is the regression test for the
// competing same-slot forge. The node votes for a peer's announcing header at
// slot S, occupying the (slot, voter) vote id; it then forges its own
// announcing block at the same slot, which discards that peer header.
//
// Arming the forged block's announcement before the invalidation frees the id
// gets its vote rejected as a duplicate, and nothing retries it -- the producer
// silently misses its own vote. The chain therefore announces a forged block on
// the ordered header stream immediately behind the invalidation, so the id is
// always free by the time the announcement is armed. This test delivers the
// two in that order and requires the local vote.
func TestVoteManagerSameSlotLocalForgeStillVotes(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))
	voteId := lcommon.LeiosVoteId{
		SlotNo:  headerArmingRbSlot,
		VoterId: headerArmingSeatedVoterId,
	}

	// The peer's header wins first and takes the vote id.
	peerVote := armAndVote(
		t, fixture, emittedCh, headerArmingRbSlot, peerRb, peerEb, 1,
	)
	assert.Equal(t, peerRb, peerVote.Vote.AnnouncingRbHash)
	require.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId}), 1)

	// The local block's endorser block is already in hand when it is forged.
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, localEb)

	// The apply-driven backstop arms the forged block first, while the id is
	// still held by the peer vote. The attempt is rejected, but it must not
	// mark the announcement as voted.
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, localRb, localEb)
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"the vote id is still held by the peer vote",
	)

	// Chain-mutation order on the header stream: the invalidation naming the
	// discarded peer header, then the forged block's own announcement.
	publishHeaderInvalidationNaming(
		fixture,
		headerArmingRbSlot,
		chain.HeaderInvalidationLocalBlock,
		2,
		[]lcommon.Blake2b256{peerRb},
	)
	publishHeaderAnnouncement(
		fixture, headerArmingRbSlot, localRb, localEb, 3,
	)

	emitted := testutil.RequireReceive(
		t,
		emittedCh,
		2*time.Second,
		"the forged block's own vote is emitted once the id is freed",
	)
	local, ok := emitted.Data.(VoteEmittedEvent)
	require.True(t, ok)
	assert.Equal(t, localRb, local.Vote.AnnouncingRbHash)

	// Exactly one vote, and it is the local one.
	testutil.RequireNoReceive(
		t,
		emittedCh,
		300*time.Millisecond,
		"exactly one vote for the slot",
	)
	raws := fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{voteId})
	require.Len(t, raws, 1)
	var stored lcommon.LeiosVote
	_, err := cbor.Decode(raws[0], &stored)
	require.NoError(t, err)
	assert.Equal(t, localEb, stored.EndorserBlockHash)

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, peerRb)
	assert.NotContains(t, fixture.mgr.acquiredEbs, peerEb)
	assert.Contains(t, fixture.mgr.announcements, localRb)
	assert.Contains(t, fixture.mgr.votedAnnouncements, localRb)
	for key := range fixture.mgr.tallies {
		assert.NotEqual(
			t,
			peerRb,
			key.announcingRbHash,
			"the discarded peer announcement's tally is gone",
		)
	}
}

// TestVoteManagerRejectedEmissionDoesNotMarkAnnouncementVoted pins the
// invariant the retry above depends on: an emission attempt refused by the
// vote store must leave the announcement eligible, or freeing the vote id
// later would have nothing to retry.
func TestVoteManagerRejectedEmissionDoesNotMarkAnnouncementVoted(t *testing.T) {
	slots := &fakeSlotProvider{slot: headerArmingEbAcquiredAt}
	fixture := newHeaderArmingFixture(t, slots)
	subId, emittedCh := fixture.eventBus.Subscribe(VoteEmittedEventType)
	defer fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)

	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))

	armAndVote(t, fixture, emittedCh, headerArmingRbSlot, peerRb, peerEb, 1)
	fixture.mgr.HandleEndorserBlock(headerArmingRbSlot, localEb)
	fixture.mgr.ObserveAnnouncement(headerArmingRbSlot, localRb, localEb)

	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, localRb)
	assert.NotContains(
		t,
		fixture.mgr.votedAnnouncements,
		localRb,
		"a refused emission must stay retryable",
	)
}
