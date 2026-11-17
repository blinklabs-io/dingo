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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// announcementReadiness validates the armed announcement rbHash's endorser
// block and returns its CIP-164 readiness verdict. Call it before voting is
// enabled, so validating the endorser block does not itself emit a vote.
func announcementReadiness(
	t *testing.T,
	fixture *managerFixture,
	rbHash lcommon.Blake2b256,
) string {
	t.Helper()
	fixture.mgr.mu.Lock()
	record, ok := fixture.mgr.announcements[rbHash]
	fixture.mgr.mu.Unlock()
	require.True(t, ok, "announcement must be armed")
	fixture.mgr.HandleValidatedEndorserBlock(record.slot, rbHash, record.ebHash)
	fixture.mgr.mu.Lock()
	record = fixture.mgr.announcements[rbHash]
	fixture.mgr.mu.Unlock()
	_, _, reason, err := fixture.mgr.localVoteReadiness(rbHash, record)
	require.NoError(t, err)
	return reason
}

func armFromChainHeader(
	fixture *managerFixture,
	rbHash, ebHash lcommon.Blake2b256,
	seq uint64,
	arrivedAt time.Time,
) {
	fixture.mgr.handleChainHeaderAnnouncement(
		chain.ChainHeaderAnnouncementEvent{
			Slot:      600,
			RbHash:    rbHash,
			EbHash:    ebHash,
			Issuer:    lcommon.PoolKeyHash{0x01},
			Seq:       seq,
			ArrivedAt: arrivedAt,
		},
	)
}

// TestVoteManagerTimesLHdrFromLeiosNotifyArrival covers blinklabs-io/dingo#6462:
// a ranking-block header that reached this node on LeiosNotify within L_hdr,
// and was admitted by chainsync only after L_hdr, must not be declined as a
// late header. The fixture's slot 600 begins at base, L_hdr is 1 s and the
// vote period 4 s, so the vote window is base+3s to base+7s.
func TestVoteManagerTimesLHdrFromLeiosNotifyArrival(t *testing.T) {
	t.Parallel()
	base := time.Unix(1_800_000_000, 0)
	onTime := base.Add(200 * time.Millisecond)
	late := base.Add(2500 * time.Millisecond)

	t.Run("votes when LeiosNotify beat L_hdr and chainsync did not", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		rbHash := lcommon.NewBlake2b256([]byte("notify-first-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("notify-first-eb"))

		*now = onTime
		fixture.mgr.ObserveAnnouncementArrival(600, rbHash, ebHash, onTime)
		*now = late
		armFromChainHeader(fixture, rbHash, ebHash, 1, late)
		require.Empty(t, announcementReadiness(t, fixture, rbHash))

		enableFixtureVoting(t, fixture)
		*now = base.Add(3 * time.Second)
		fixture.mgr.HandleValidatedEndorserBlock(600, rbHash, ebHash)
		assert.Len(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo: 600, VoterId: headerArmingSeatedVoterId,
		}}), 1)
	})

	t.Run("an earlier arrival lowers an already armed announcement", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		rbHash := lcommon.NewBlake2b256([]byte("chain-first-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("chain-first-eb"))

		*now = late
		armFromChainHeader(fixture, rbHash, ebHash, 1, late)
		require.Equal(
			t, voteNotEmittedLateHeader,
			announcementReadiness(t, fixture, rbHash),
		)
		fixture.mgr.ObserveAnnouncementArrival(600, rbHash, ebHash, onTime)
		assert.Empty(t, announcementReadiness(t, fixture, rbHash))
	})

	t.Run("a late LeiosNotify arrival is still a late header", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		rbHash := lcommon.NewBlake2b256([]byte("both-late-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("both-late-eb"))

		*now = late
		fixture.mgr.ObserveAnnouncementArrival(
			600, rbHash, ebHash, base.Add(time.Second+time.Nanosecond),
		)
		armFromChainHeader(fixture, rbHash, ebHash, 1, late)
		assert.Equal(
			t, voteNotEmittedLateHeader,
			announcementReadiness(t, fixture, rbHash),
		)
	})

	t.Run("an arrival alone never arms a vote", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		enableFixtureVoting(t, fixture)
		rbHash := lcommon.NewBlake2b256([]byte("unarmed-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("unarmed-eb"))

		*now = onTime
		fixture.mgr.ObserveAnnouncementArrival(600, rbHash, ebHash, onTime)
		*now = base.Add(3 * time.Second)
		fixture.mgr.HandleValidatedEndorserBlock(600, rbHash, ebHash)

		fixture.mgr.mu.Lock()
		_, armed := fixture.mgr.announcements[rbHash]
		latest := fixture.mgr.latestRbHash
		fixture.mgr.mu.Unlock()
		assert.False(t, armed, "an arrival stamp must not arm an announcement")
		assert.NotEqual(t, rbHash, latest, "an arrival stamp must not move the latest RB")
		assert.Empty(t, fixture.mgr.VotesByIds([]lcommon.LeiosVoteId{{
			SlotNo: 600, VoterId: headerArmingSeatedVoterId,
		}}))
	})

	t.Run("an arrival for another endorser block is ignored", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		rbHash := lcommon.NewBlake2b256([]byte("mismatch-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("mismatch-eb"))

		*now = onTime
		fixture.mgr.ObserveAnnouncementArrival(
			600, rbHash, lcommon.NewBlake2b256([]byte("other-eb")), onTime,
		)
		*now = late
		armFromChainHeader(fixture, rbHash, ebHash, 1, late)
		assert.Equal(
			t, voteNotEmittedLateHeader,
			announcementReadiness(t, fixture, rbHash),
		)
	})

	// The stamp is consumed when the chain arms the announcement, so a
	// header invalidation drops the arrival time with the announcement, as
	// it does for one armed from chainsync alone: a re-admission is timed
	// from its own admission.
	t.Run("a header invalidation drops the consumed arrival", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		rbHash := lcommon.NewBlake2b256([]byte("invalidated-rb"))
		ebHash := lcommon.NewBlake2b256([]byte("invalidated-eb"))

		*now = onTime
		fixture.mgr.ObserveAnnouncementArrival(600, rbHash, ebHash, onTime)
		armFromChainHeader(fixture, rbHash, ebHash, 1, onTime.Add(time.Millisecond))
		fixture.mgr.handleChainHeaderInvalidation(
			chain.ChainHeaderInvalidationEvent{
				Point:    ocommon.Point{Slot: 599},
				RbHashes: []lcommon.Blake2b256{rbHash},
				Reason:   chain.HeaderInvalidationRollback,
				Seq:      2,
			},
		)
		*now = late
		armFromChainHeader(fixture, rbHash, ebHash, 3, late)
		assert.Equal(
			t, voteNotEmittedLateHeader,
			announcementReadiness(t, fixture, rbHash),
		)
	})

	t.Run("stamps expire and are bounded", func(t *testing.T) {
		fixture, _, now := newCIP164TimingFixture(t, base, 603)
		*now = onTime
		for i := range announcementArrivalMaxEntries + 1 {
			fixture.mgr.ObserveAnnouncementArrival(
				600,
				lcommon.NewBlake2b256([]byte{byte(i), byte(i >> 8)}),
				lcommon.NewBlake2b256([]byte("bounded-eb")),
				onTime,
			)
		}
		fixture.mgr.mu.Lock()
		assert.Len(
			t, fixture.mgr.announcementArrivals, announcementArrivalMaxEntries,
		)
		fixture.mgr.mu.Unlock()

		*now = onTime.Add(fixture.mgr.voteTTL + time.Second)
		fixture.mgr.ObserveAnnouncementArrival(
			600,
			lcommon.NewBlake2b256([]byte("after-ttl-rb")),
			lcommon.NewBlake2b256([]byte("after-ttl-eb")),
			*now,
		)
		fixture.mgr.mu.Lock()
		assert.Len(t, fixture.mgr.announcementArrivals, 1)
		fixture.mgr.mu.Unlock()
	})
}
