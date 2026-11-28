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
	"errors"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// The votes of an announcement are the verified stake of the voters counted
// for its announcing ranking block, against the committee and threshold of
// the epoch it was announced in. An announcement nobody voted for has none.
func TestVoteManagerAnnouncementVotes(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	const slot = 500
	rbHash := lcommon.NewBlake2b256([]byte("announcing-rb"))
	otherRbHash := lcommon.NewBlake2b256([]byte("other-announcing-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	fixture.mgr.ObserveAnnouncement(slot, rbHash, ebHash)
	fixture.mgr.ObserveAnnouncement(slot+1, otherRbHash, ebHash)

	votes, err := fixture.mgr.AnnouncementVotes(slot, rbHash, ebHash)
	require.NoError(t, err)
	require.Zero(t, votes.VerifiedStake)
	require.Equal(t, uint64(550), votes.TotalActiveStake)
	require.Zero(t, big.NewRat(7, 10).Cmp(votes.Threshold))

	// Voters 0, 1 and 2 hold stakes 100, 90 and 80.
	for _, voter := range []uint64{0, 1, 2} {
		require.NoError(t, fixture.mgr.HandlePrototypeVote(
			"conn-a",
			fixture.makePrototypeVote(t, voter, rbHash),
		))
	}
	require.NoError(t, fixture.mgr.HandlePrototypeVote(
		"conn-a",
		fixture.makePrototypeVote(t, 9, otherRbHash),
	))

	votes, err = fixture.mgr.AnnouncementVotes(slot, rbHash, ebHash)
	require.NoError(t, err)
	require.Equal(t, uint64(270), votes.VerifiedStake)
	require.Equal(t, uint64(550), votes.TotalActiveStake)

	other, err := fixture.mgr.AnnouncementVotes(slot+1, otherRbHash, ebHash)
	require.NoError(t, err)
	require.Equal(t, uint64(10), other.VerifiedStake,
		"a vote counts for its own announcing block only")

	// Votes for another endorser block under the same announcing block do
	// not count for this one.
	elsewhere, err := fixture.mgr.AnnouncementVotes(
		slot, rbHash, lcommon.NewBlake2b256([]byte("another-eb")),
	)
	require.NoError(t, err)
	require.Zero(t, elsewhere.VerifiedStake)
}

// Without a committee the outcome of an announcement is unknown, which is
// not the same as nobody having voted.
func TestVoteManagerAnnouncementVotesUnknownWithoutCommittee(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	fixture.params.mu.Lock()
	fixture.params.err = errors.New("no parameters")
	fixture.params.mu.Unlock()

	_, err := fixture.mgr.AnnouncementVotes(
		500,
		lcommon.NewBlake2b256([]byte("rb")),
		lcommon.NewBlake2b256([]byte("eb")),
	)
	require.Error(t, err)
}

// Votes of the legacy scheme, which sign the slot and endorser block and carry
// no announcing block, count for the announcement of that endorser block.
func TestVoteManagerAnnouncementVotesCountsLegacyVotes(t *testing.T) {
	t.Parallel()
	fixture := newManagerFixture(t)
	const slot = 500
	rbHash := lcommon.NewBlake2b256([]byte("legacy-announcing-rb"))
	ebHash := lcommon.NewBlake2b256([]byte("legacy-announced-eb"))
	fixture.mgr.ObserveAnnouncement(slot, rbHash, ebHash)

	for _, voter := range []uint64{0, 1} {
		require.NoError(t, fixture.mgr.HandleLegacyVote(
			"conn-a",
			fixture.makeVote(t, voter, slot, ebHash),
		))
	}
	votes, err := fixture.mgr.AnnouncementVotes(slot, rbHash, ebHash)
	require.NoError(t, err)
	require.Equal(t, uint64(190), votes.VerifiedStake)
}
