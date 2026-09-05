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
	fixture.eventBus.Publish(
		chain.ChainHeaderEventType,
		event.NewEvent(
			chain.ChainHeaderEventType,
			chain.ChainHeaderInvalidationEvent{
				Point:  ocommon.Point{Slot: slot},
				Reason: reason,
				Seq:    seq,
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
	// Both events are on one channel, so waiting for the invalidation to be
	// applied also proves the announcement ahead of it was.
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
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 8
	}, 2*time.Second, "replacement announcement armed")

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
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 4
	}, 2*time.Second, "announcement armed")

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
