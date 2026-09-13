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

// Ordering, stated plainly.
//
// A Leios announcement is provisional. It is armed from a ranking-block header,
// before that block has been fetched or applied, because the vote window is
// measured from the header's own slot and applying an EB-announcing block waits
// on fetching the very endorser block it announces. So between arming a vote and
// casting it, the announcing block can leave our chain. Three rules keep that
// safe, and this file walks each of them end to end.
//
//  1. Announcements and the invalidations that void them share ONE sequenced
//     stream. chain.ChainHeaderEventType carries both, and the chain enqueues
//     them on its own sequencer under the chain lock, so publication order is
//     chain-mutation order and a single subscriber cannot observe them
//     inverted. An announcement seen after an invalidation was genuinely
//     re-admitted; one seen before it is genuinely gone.
//
//  2. Rollbacks on chain.update are sequence-guarded. That topic is delivered
//     on a different channel with no ordering against the header stream, so a
//     rollback can arrive after the header stream has already applied the
//     matching invalidation AND re-armed the replacement chain -- which is
//     what fork resolution does every time: roll back, then re-queue the
//     peer's fork headers. Every announcement carries the chain-mutation
//     sequence that armed it, and a rollback prunes only what was armed no
//     later than itself. An unsequenced rollback supersedes nothing and prunes
//     as it always did.
//
//  3. There is no longer a cross-topic arming path. A locally forged block
//     never passes through header admission, so its own announcement used to
//     be armed only from chain.update -- a race it could not win against the
//     invalidation for the peer header it displaced. The chain now announces a
//     forged block on the same sequenced stream, immediately behind that
//     invalidation, so every announcement in the system arrives on one ordered
//     stream and the apply-driven path is a pure backstop.
//
// The scenarios below are (a) announcement then rollback, (b) rollback then a
// late header, (c) a local forge displacing a queued header, and (d) the header
// queue being discarded on a stalled peer. Each asserts the vote outcome, which
// is the only thing an operator actually cares about.

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	demoSlot    = headerArmingRbSlot
	demoVoterId = headerArmingSeatedVoterId
)

// newOrderingDemo builds one seated committee member with a loaded key, its
// wall clock parked one slot after the announcing block (so the vote window is
// open throughout), and a channel of the votes it emits.
func newOrderingDemo(t *testing.T) (*managerFixture, <-chan any) {
	t.Helper()
	fixture := newHeaderArmingFixture(
		t,
		&fakeSlotProvider{slot: headerArmingEbAcquiredAt},
	)
	subId, ch := fixture.eventBus.Subscribe(VoteEmittedEventType)
	t.Cleanup(func() {
		fixture.eventBus.Unsubscribe(VoteEmittedEventType, subId)
	})
	out := make(chan any, 16)
	go func() {
		for evt := range ch {
			out <- evt.Data
		}
	}()
	return fixture, out
}

func demoVoteId() lcommon.LeiosVoteId {
	return lcommon.LeiosVoteId{SlotNo: demoSlot, VoterId: demoVoterId}
}

// requireVoteFor drains one emitted vote and requires it to name rbHash.
func requireVoteFor(
	t *testing.T,
	votes <-chan any,
	rbHash lcommon.Blake2b256,
	msg string,
) {
	t.Helper()
	data := testutil.RequireReceive(t, votes, 2*time.Second, msg)
	emitted, ok := data.(VoteEmittedEvent)
	require.True(t, ok, "got %T", data)
	assert.Equal(t, rbHash, emitted.Vote.AnnouncingRbHash, msg)
}

// TestOrderingDemoAnnouncementThenRollback: (a) the ordinary case. The header
// arrives, the chain then rolls it away, and the endorser block turns up
// afterwards. Rule 1 puts the invalidation behind the announcement on the one
// stream, so by the time the endorser block could trigger a vote the
// announcement is gone.
func TestOrderingDemoAnnouncementThenRollback(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	rb := lcommon.NewBlake2b256([]byte("rolled-away-rb"))
	eb := lcommon.NewBlake2b256([]byte("rolled-away-eb"))

	// Chain mutation 1: the announcing header is admitted.
	publishHeaderAnnouncement(fixture, demoSlot, rb, eb, 1)
	// Chain mutation 2: it is rolled away again.
	publishHeaderInvalidation(
		fixture, demoSlot-1, chain.HeaderInvalidationRollback, 2,
	)
	testutil.WaitForCondition(t, func() bool {
		fixture.mgr.mu.Lock()
		defer fixture.mgr.mu.Unlock()
		return fixture.mgr.lastHeaderStreamSeq >= 2
	}, 2*time.Second, "both header events applied, in order")

	// The endorser block finally arrives. There is nothing left to vote for.
	fixture.mgr.HandleEndorserBlock(demoSlot, eb)
	testutil.RequireNoReceive(
		t, votes, 500*time.Millisecond,
		"no vote for a ranking block that left our chain",
	)
	assert.Empty(t, fixture.mgr.VotesByIds(
		[]lcommon.LeiosVoteId{demoVoteId()},
	))
}

// TestOrderingDemoRollbackThenLateHeader: (b) the guard that makes the second
// topic safe. Fork resolution rolls back and then re-queues the winning fork's
// headers, so the replacement chain is armed BEFORE the rollback's own
// chain.update is delivered. Rule 2 stops that late rollback from deleting the
// replacement chain's announcement and the vote already cast for it.
func TestOrderingDemoRollbackThenLateHeader(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	rb := lcommon.NewBlake2b256([]byte("replacement-rb"))
	eb := lcommon.NewBlake2b256([]byte("replacement-eb"))

	// Chain mutation 7: roll back. Chain mutation 8: admit the replacement
	// chain's announcing header. Both on the one ordered stream.
	publishHeaderInvalidation(
		fixture, demoSlot-1, chain.HeaderInvalidationRollback, 7,
	)
	publishHeaderAnnouncement(fixture, demoSlot, rb, eb, 8)
	waitForAnnouncement(t, fixture, rb)

	fixture.mgr.HandleEndorserBlock(demoSlot, eb)
	requireVoteFor(t, votes, rb, "the replacement chain is voted for")

	// Only now does mutation 7's rollback reach the other topic. It must not
	// undo anything mutation 8 established.
	fixture.mgr.handleRollback(chain.ChainRollbackEvent{
		Point: ocommon.Point{Slot: demoSlot - 1},
		Seq:   7,
	})
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{demoVoteId()}),
		1,
		"a superseded rollback keeps the replacement chain's vote",
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.Contains(t, fixture.mgr.announcements, rb)
}

// TestOrderingDemoLocalForgeDisplacingQueuedHeader: (c) the case that used to
// lose the producer its own vote. The node votes for a peer's header at slot S,
// then forges its own announcing block at S, which discards that peer header.
// The peer vote holds the (slot, voter) vote id, so the forged block can only
// vote once the invalidation has freed it -- which rule 3 guarantees, by
// putting the forged block's announcement behind the invalidation on the same
// stream instead of on chain.update.
func TestOrderingDemoLocalForgeDisplacingQueuedHeader(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	peerRb := lcommon.NewBlake2b256([]byte("peer-rb"))
	peerEb := lcommon.NewBlake2b256([]byte("peer-eb"))
	localRb := lcommon.NewBlake2b256([]byte("local-rb"))
	localEb := lcommon.NewBlake2b256([]byte("local-eb"))

	// The peer's header wins first and takes the vote id.
	publishHeaderAnnouncement(fixture, demoSlot, peerRb, peerEb, 1)
	waitForAnnouncement(t, fixture, peerRb)
	fixture.mgr.HandleEndorserBlock(demoSlot, peerEb)
	requireVoteFor(t, votes, peerRb, "the peer header is voted for first")

	// We forge at the same slot with our own endorser block in hand. The
	// apply-driven backstop arms it early, while the id is still taken; the
	// attempt is refused and must stay retryable.
	fixture.mgr.HandleEndorserBlock(demoSlot, localEb)
	fixture.mgr.ObserveAnnouncement(demoSlot, localRb, localEb)
	testutil.RequireNoReceive(
		t, votes, 300*time.Millisecond,
		"the vote id is still held by the peer vote",
	)

	// Chain mutation order: the peer header is discarded, then the forged
	// block announces itself.
	publishHeaderInvalidationNaming(
		fixture,
		demoSlot,
		chain.HeaderInvalidationLocalBlock,
		2,
		[]lcommon.Blake2b256{peerRb},
	)
	publishHeaderAnnouncement(fixture, demoSlot, localRb, localEb, 3)

	requireVoteFor(t, votes, localRb, "the forged block gets its own vote")
	testutil.RequireNoReceive(
		t, votes, 300*time.Millisecond, "exactly one vote for the slot",
	)
	assert.Len(
		t,
		fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{demoVoteId()}),
		1,
	)
	fixture.mgr.mu.Lock()
	defer fixture.mgr.mu.Unlock()
	assert.NotContains(t, fixture.mgr.announcements, peerRb)
	assert.Contains(t, fixture.mgr.announcements, localRb)
}

// TestOrderingDemoHeaderQueueDiscardedOnStall: (d) the peer-stall path. A
// header is admitted and voted for, then the connection dies or blockfetch
// times out and the whole header queue is discarded. No block was ever added,
// so no rollback is published -- the invalidation on the header stream is the
// only thing that voids the announcement, and it must take the vote, tally and
// dedup record with it so the slot can be voted on again.
func TestOrderingDemoHeaderQueueDiscardedOnStall(t *testing.T) {
	fixture, votes := newOrderingDemo(t)
	stalledRb := lcommon.NewBlake2b256([]byte("stalled-rb"))
	stalledEb := lcommon.NewBlake2b256([]byte("stalled-eb"))

	publishHeaderAnnouncement(fixture, demoSlot, stalledRb, stalledEb, 1)
	waitForAnnouncement(t, fixture, stalledRb)
	fixture.mgr.HandleEndorserBlock(demoSlot, stalledEb)
	requireVoteFor(t, votes, stalledRb, "the admitted header is voted for")

	// The peer stalls; the header queue is discarded back to the block tip.
	publishHeaderInvalidation(
		fixture, demoSlot-1, chain.HeaderInvalidationQueueCleared, 2,
	)
	testutil.WaitForCondition(t, func() bool {
		return len(fixture.mgr.VotesByIds(
			[]lcommon.LeiosVoteId{demoVoteId()},
		)) == 0
	}, 2*time.Second, "the discarded header's vote is dropped")

	fixture.mgr.mu.Lock()
	assert.NotContains(t, fixture.mgr.announcements, stalledRb)
	assert.NotContains(t, fixture.mgr.votedAnnouncements, stalledRb)
	assert.NotContains(t, fixture.mgr.voteRecords, demoVoteId())
	fixture.mgr.mu.Unlock()

	// The vote id is free, so whichever header wins the slot next is voted
	// for rather than dropped as a duplicate.
	nextRb := lcommon.NewBlake2b256([]byte("next-rb"))
	nextEb := lcommon.NewBlake2b256([]byte("next-eb"))
	publishHeaderAnnouncement(fixture, demoSlot, nextRb, nextEb, 3)
	waitForAnnouncement(t, fixture, nextRb)
	fixture.mgr.HandleEndorserBlock(demoSlot, nextEb)
	requireVoteFor(t, votes, nextRb, "the slot can be voted on again")
}
