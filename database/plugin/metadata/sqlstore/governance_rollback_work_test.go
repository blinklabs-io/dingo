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

package sqlstore

import (
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestGovernanceVoteRollbackDeleteWithoutHistoryIndex(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(
		`CREATE TABLE governance_vote (id INTEGER PRIMARY KEY, added_slot INTEGER);
CREATE TABLE governance_vote_history (vote_id INTEGER);`,
	)
	require.NoError(t, err)
	rows, err := db.Query(
		"EXPLAIN QUERY PLAN "+governanceVoteRollbackDeleteQuery,
		150,
	)
	require.NoError(t, err)
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.NotContains(
		t,
		strings.ToUpper(strings.Join(plan, "\n")),
		"CORRELATED",
		"history membership must not rescan history for every vote without an index",
	)

	for _, history := range []string{"", "(NULL)", "(1)", "(1),(1),(NULL)", "(2),(3),(NULL)"} {
		_, err = db.Exec(
			`DELETE FROM governance_vote; DELETE FROM governance_vote_history;
INSERT INTO governance_vote VALUES (1,100),(2,150),(3,200),(4,100);`,
		)
		require.NoError(t, err)
		if history != "" {
			_, err = db.Exec(
				"INSERT INTO governance_vote_history VALUES " + history,
			)
			require.NoError(t, err)
		}
		var expected string
		require.NoError(
			t,
			db.QueryRow(`SELECT COALESCE(group_concat(id), '') FROM (
SELECT id FROM governance_vote WHERE NOT (added_slot > 150 OR NOT EXISTS (
SELECT 1 FROM governance_vote_history h WHERE h.vote_id=governance_vote.id)) ORDER BY id)`).Scan(&expected),
		)
		_, err = db.Exec(governanceVoteRollbackDeleteQuery, 150)
		require.NoError(t, err)
		var actual string
		require.NoError(
			t,
			db.QueryRow(`SELECT COALESCE(group_concat(id), '') FROM (
SELECT id FROM governance_vote ORDER BY id)`).Scan(&actual),
		)
		require.Equal(t, expected, actual, "history=%s", history)
	}
}

func TestGovernanceVoteRollbackUpdatesOnlyAffectedAndLegacyVotes(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	proposal := seedGovernanceVoteProposal(t, store)
	voteIDs := make([]uint, 4)
	for i := range voteIDs {
		slot := uint64(100)
		vote := &models.GovernanceVote{
			ProposalID: proposal, VoterType: models.VoterTypeDRep,
			VoterCredential: credentialHash(
				byte(0x80 + i),
			), Vote: models.VoteYes,
			AddedSlot: slot, VoteUpdatedSlot: &slot,
			AnchorURL: "https://first.example", AnchorHash: []byte{1},
		}
		require.NoError(t, store.SetGovernanceVote(vote, nil))
		voteIDs[i] = vote.ID
		if i >= 2 {
			slot = 200
			vote.VoteUpdatedSlot = &slot
			if i == 2 {
				vote.Vote = models.VoteNo
				vote.AnchorURL = "https://second.example"
				vote.AnchorHash = []byte{2}
			}
			// The last vote repeats its value without a cast location:
			// its current timestamp moves but its history does not grow.
			require.NoError(t, store.SetGovernanceVote(vote, nil))
		}
	}
	_, err := db.Exec(
		`UPDATE governance_vote SET vote_updated_slot=NULL WHERE id=?`,
		voteIDs[1],
	)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE rollback_vote_updates (vote_id INTEGER);
CREATE TRIGGER record_rollback_vote_update AFTER UPDATE ON governance_vote BEGIN
INSERT INTO rollback_vote_updates VALUES (NEW.id); END;`)
	require.NoError(t, err)
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(150, nil))
	for i, id := range voteIDs {
		var count int
		require.NoError(
			t,
			db.QueryRow(`SELECT COUNT(*) FROM rollback_vote_updates WHERE vote_id=?`, id).
				Scan(&count),
		)
		expected := 1
		if i == 0 {
			expected = 0
		}
		require.Equal(t, expected, count, "vote %d", i)
		var value int
		var slot uint64
		var url string
		var hash []byte
		require.NoError(
			t,
			db.QueryRow(`SELECT vote,vote_updated_slot,anchor_url,anchor_hash FROM governance_vote WHERE id=?`, id).
				Scan(&value, &slot, &url, &hash),
		)
		require.Equal(t, int(models.VoteYes), value)
		require.Equal(t, uint64(100), slot)
		require.Equal(t, "https://first.example", url)
		require.Equal(t, []byte{1}, hash)
		var history int
		require.NoError(
			t,
			db.QueryRow(`SELECT COUNT(*) FROM governance_vote_history WHERE vote_id=?`, id).
				Scan(&history),
		)
		require.Equal(t, 1, history, fmt.Sprintf("vote %d", i))
	}
	_, err = db.Exec(`DELETE FROM rollback_vote_updates`)
	require.NoError(t, err)
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(150, nil))
	var updates int
	require.NoError(
		t,
		db.QueryRow(`SELECT COUNT(*) FROM rollback_vote_updates`).
			Scan(&updates),
	)
	require.Zero(
		t,
		updates,
		"repeating rollback must not rewrite restored votes",
	)
}

func TestGovernanceVoteRollbackNormalizesOlderLocatedReplay(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	proposal := seedGovernanceVoteProposal(t, store)
	for _, step := range []struct {
		slot  uint64
		value uint8
		tx    byte
	}{
		{100, models.VoteYes, 1}, {200, models.VoteNo, 2}, {100, models.VoteYes, 1},
	} {
		vote := &models.GovernanceVote{
			ProposalID: proposal,
			VoterType:  models.VoterTypeDRep,
			VoterCredential: credentialHash(
				0xa0,
			),
			Vote:            step.value,
			AddedSlot:       step.slot,
			VoteUpdatedSlot: &step.slot,
			CastTxHash:      credentialHash(step.tx),
			AnchorURL: fmt.Sprintf(
				"https://cast-%d.example",
				step.tx,
			),
			AnchorHash: []byte{step.tx},
		}
		require.NoError(t, store.SetGovernanceVote(vote, nil))
	}
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))
	votes, err := store.GetGovernanceVotes(proposal, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)
	require.Equal(t, uint64(200), *votes[0].VoteUpdatedSlot)
	require.Equal(t, "https://cast-2.example", votes[0].AnchorURL)
	require.Equal(t, []byte{2}, votes[0].AnchorHash)
}

func TestGovernanceVoteRollbackRestoresSameSlotOlderCast(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	proposal := seedGovernanceVoteProposal(t, store)
	for _, step := range []struct {
		value uint8
		tx    byte
	}{{models.VoteYes, 1}, {models.VoteNo, 2}, {models.VoteYes, 1}} {
		slot := uint64(100)
		vote := &models.GovernanceVote{
			ProposalID: proposal,
			VoterType:  models.VoterTypeDRep,
			VoterCredential: credentialHash(
				0xa1,
			),
			Vote:            step.value,
			AddedSlot:       slot,
			VoteUpdatedSlot: &slot,
			CastTxHash:      credentialHash(step.tx),
			AnchorURL: fmt.Sprintf(
				"https://cast-%d.example",
				step.tx,
			),
			AnchorHash: []byte{step.tx},
		}
		require.NoError(t, store.SetGovernanceVote(vote, nil))
	}
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))
	votes, err := store.GetGovernanceVotes(proposal, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)
	require.Equal(t, "https://cast-2.example", votes[0].AnchorURL)
	require.Equal(t, []byte{2}, votes[0].AnchorHash)
}

func TestGovernanceVoteRollbackNormalizesUnlocatedRepeat(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	proposal := seedGovernanceVoteProposal(t, store)
	for _, slot := range []uint64{100, 200} {
		vote := &models.GovernanceVote{
			ProposalID: proposal,
			VoterType:  models.VoterTypeDRep,
			VoterCredential: credentialHash(
				0xa2,
			),
			Vote:            models.VoteYes,
			AddedSlot:       slot,
			VoteUpdatedSlot: &slot,
		}
		require.NoError(t, store.SetGovernanceVote(vote, nil))
	}
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))
	votes, err := store.GetGovernanceVotes(proposal, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint64(100), *votes[0].VoteUpdatedSlot)
}

func TestGovernanceVoteRollbackDistinguishesNullAndEmptyAnchors(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	proposal := seedGovernanceVoteProposal(t, store)
	for i, tc := range []struct {
		historyURL, currentURL   any
		historyHash, currentHash any
	}{
		{nil, "", nil, nil}, {"", nil, nil, nil}, {"", "", nil, []byte{}}, {"", "", []byte{}, nil},
	} {
		slot := uint64(100)
		vote := &models.GovernanceVote{
			ProposalID: proposal,
			VoterType:  models.VoterTypeDRep,
			VoterCredential: credentialHash(
				byte(0xb0 + i),
			),
			Vote:            models.VoteYes,
			AddedSlot:       slot,
			VoteUpdatedSlot: &slot,
		}
		require.NoError(t, store.SetGovernanceVote(vote, nil))
		_, err := db.Exec(
			`UPDATE governance_vote_history SET anchor_url=?,anchor_hash=? WHERE vote_id=?`,
			tc.historyURL,
			tc.historyHash,
			vote.ID,
		)
		require.NoError(t, err)
		_, err = db.Exec(
			`UPDATE governance_vote SET anchor_url=?,anchor_hash=? WHERE id=?`,
			tc.currentURL,
			tc.currentHash,
			vote.ID,
		)
		require.NoError(t, err)
	}
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))
	var differences int
	require.NoError(
		t,
		db.QueryRow(`SELECT COUNT(*) FROM governance_vote v JOIN governance_vote_history h ON h.vote_id=v.id
WHERE v.anchor_url IS NOT h.anchor_url OR v.anchor_hash IS NOT h.anchor_hash OR v.vote IS NOT h.vote OR v.vote_updated_slot IS NOT h.transition_slot`).
			Scan(&differences),
	)
	require.Zero(t, differences)
}

func TestGovernanceVoteRollbackNormalizesAnchorHashStorageClass(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	proposal := seedGovernanceVoteProposal(t, store)
	slot := uint64(100)
	vote := &models.GovernanceVote{
		ProposalID: proposal,
		VoterType:  models.VoterTypeDRep,
		VoterCredential: credentialHash(
			0xc0,
		),
		Vote:            models.VoteYes,
		AddedSlot:       slot,
		VoteUpdatedSlot: &slot,
	}
	require.NoError(t, store.SetGovernanceVote(vote, nil))
	_, err := db.Exec(
		`UPDATE governance_vote SET anchor_hash=1 WHERE id=?`,
		vote.ID,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`UPDATE governance_vote_history SET anchor_hash=1.0 WHERE vote_id=?`,
		vote.ID,
	)
	require.NoError(t, err)
	var currentType, historyType string
	require.NoError(
		t,
		db.QueryRow(`SELECT typeof(v.anchor_hash),typeof(h.anchor_hash)
FROM governance_vote v JOIN governance_vote_history h ON h.vote_id=v.id WHERE v.id=?`, vote.ID).Scan(&currentType, &historyType),
	)
	require.Equal(t, "integer", currentType)
	require.Equal(t, "real", historyType)
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))
	require.NoError(
		t,
		db.QueryRow(`SELECT typeof(anchor_hash) FROM governance_vote WHERE id=?`, vote.ID).
			Scan(&currentType),
	)
	require.Equal(
		t,
		historyType,
		currentType,
		"rollback must copy the stored history value, including its BLOB storage class",
	)
}

func TestGovernanceVoteRollbackNormalizesAnchorHashFloatBits(t *testing.T) {
	t.Parallel()
	store, db := newManagementTestStoreWithRawDB(t)
	proposal := seedGovernanceVoteProposal(t, store)
	slot := uint64(100)
	vote := &models.GovernanceVote{
		ProposalID: proposal,
		VoterType:  models.VoterTypeDRep,
		VoterCredential: credentialHash(
			0xc1,
		),
		Vote:            models.VoteYes,
		AddedSlot:       slot,
		VoteUpdatedSlot: &slot,
	}
	require.NoError(t, store.SetGovernanceVote(vote, nil))
	_, err := db.Exec(
		`UPDATE governance_vote SET anchor_hash=? WHERE id=?`,
		math.Copysign(0, -1),
		vote.ID,
	)
	require.NoError(t, err)
	_, err = db.Exec(
		`UPDATE governance_vote_history SET anchor_hash=0.0 WHERE vote_id=?`,
		vote.ID,
	)
	require.NoError(t, err)
	var current, history float64
	var currentType, historyType string
	require.NoError(
		t,
		db.QueryRow(`SELECT v.anchor_hash,h.anchor_hash,typeof(v.anchor_hash),typeof(h.anchor_hash)
FROM governance_vote v JOIN governance_vote_history h ON h.vote_id=v.id WHERE v.id=?`, vote.ID).
			Scan(&current, &history, &currentType, &historyType),
	)
	require.Equal(t, "real", currentType)
	require.Equal(t, "real", historyType)
	require.NotEqual(t, math.Float64bits(history), math.Float64bits(current))
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))
	require.NoError(
		t,
		db.QueryRow(`SELECT anchor_hash FROM governance_vote WHERE id=?`, vote.ID).
			Scan(&current),
	)
	require.Equal(
		t,
		math.Float64bits(history),
		math.Float64bits(current),
		"rollback must preserve the history float bits for numeric BLOB values",
	)
}
