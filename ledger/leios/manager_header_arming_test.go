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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
) {
	fixture.eventBus.Publish(
		chain.ChainHeaderAnnouncementEventType,
		event.NewEvent(
			chain.ChainHeaderAnnouncementEventType,
			chain.ChainHeaderAnnouncementEvent{
				Slot:   slot,
				RbHash: rbHash,
				EbHash: ebHash,
				EbSize: 1024,
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
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash)

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
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash)
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
	publishHeaderAnnouncement(fixture, headerArmingRbSlot, rbHash, ebHash)
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
