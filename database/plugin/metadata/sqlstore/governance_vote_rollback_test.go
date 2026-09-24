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
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// newManagementTestStoreWithRawDB is newManagementTestStore plus the
// underlying *sql.DB, for tests that need to write rows migrations would
// have produced (e.g. a v22 backfill row) directly, bypassing the store's
// own write path.
func newManagementTestStoreWithRawDB(t *testing.T) (*Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:sqlstore_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store, db
}

func seedGovernanceVoteProposal(t *testing.T, store *Store) uint {
	t.Helper()
	proposal := &models.GovernanceProposal{
		TxHash:        credentialHash(0x01),
		ActionIndex:   0,
		ActionType:    0,
		ProposedEpoch: 1,
		ExpiresEpoch:  10,
		Deposit:       1_000_000,
		AddedSlot:     1,
	}
	require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	return proposal.ID
}

// TestGovernanceVoteRollbackRestoresReplacedVote covers dingo#4463: replacing
// a vote and then rolling back to a slot between the two casts must restore
// the vote that was actually current at that slot, not delete it outright.
func TestGovernanceVoteRollbackRestoresReplacedVote(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	proposalID := seedGovernanceVoteProposal(t, store)

	voterCredential := credentialHash(0x42)
	initialSlot := uint64(100)
	replacedSlot := uint64(200)
	rollbackSlot := uint64(150)

	vote := &models.GovernanceVote{
		ProposalID:         proposalID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    voterCredential,
		Vote:               models.VoteYes,
		AddedSlot:          initialSlot,
		VoteUpdatedSlot:    &initialSlot,
	}
	require.NoError(t, store.SetGovernanceVote(vote, nil))

	replacement := &models.GovernanceVote{
		ProposalID:         proposalID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    voterCredential,
		Vote:               models.VoteNo,
		AddedSlot:          initialSlot,
		VoteUpdatedSlot:    &replacedSlot,
	}
	require.NoError(t, store.SetGovernanceVote(replacement, nil))

	votes, err := store.GetGovernanceVotes(proposalID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)

	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(rollbackSlot, nil))

	votes, err = store.GetGovernanceVotes(proposalID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(
		t,
		uint8(models.VoteYes),
		votes[0].Vote,
		"rollback to a slot before the replacement must restore the original vote",
	)
	require.NotNil(t, votes[0].VoteUpdatedSlot)
	require.Equal(t, initialSlot, *votes[0].VoteUpdatedSlot)
}

// TestGovernanceVoteRollbackDropsVoteCastAfterRollbackPoint covers the
// existing behavior for a vote that did not exist at all before the rollback
// point: it must still be deleted outright, not restored.
func TestGovernanceVoteRollbackDropsVoteCastAfterRollbackPoint(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	proposalID := seedGovernanceVoteProposal(t, store)

	voterCredential := credentialHash(0x43)
	castSlot := uint64(200)
	rollbackSlot := uint64(150)

	vote := &models.GovernanceVote{
		ProposalID:         proposalID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 0,
		VoterCredential:    voterCredential,
		Vote:               models.VoteYes,
		AddedSlot:          castSlot,
		VoteUpdatedSlot:    &castSlot,
	}
	require.NoError(t, store.SetGovernanceVote(vote, nil))

	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(rollbackSlot, nil))

	votes, err := store.GetGovernanceVotes(proposalID, nil)
	require.NoError(t, err)
	require.Empty(t, votes)
}

// TestGovernanceVoteRollbackRestoresAcrossMultipleReplacements covers a vote
// replaced more than once, rolling back to a slot before the most recent
// replacement but after an earlier one.
func TestGovernanceVoteRollbackRestoresAcrossMultipleReplacements(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	proposalID := seedGovernanceVoteProposal(t, store)

	voterCredential := credentialHash(0x44)
	slot1 := uint64(100)
	slot2 := uint64(200)
	slot3 := uint64(300)
	rollbackSlot := uint64(250)

	for _, step := range []struct {
		vote uint8
		slot uint64
	}{
		{models.VoteYes, slot1},
		{models.VoteNo, slot2},
		{models.VoteAbstain, slot3},
	} {
		v := &models.GovernanceVote{
			ProposalID:         proposalID,
			VoterType:          models.VoterTypeDRep,
			VoterCredentialTag: 0,
			VoterCredential:    voterCredential,
			Vote:               step.vote,
			AddedSlot:          slot1,
			VoteUpdatedSlot:    &step.slot,
		}
		require.NoError(t, store.SetGovernanceVote(v, nil))
	}

	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(rollbackSlot, nil))

	votes, err := store.GetGovernanceVotes(proposalID, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)
	require.NotNil(t, votes[0].VoteUpdatedSlot)
	require.Equal(t, slot2, *votes[0].VoteUpdatedSlot)
}

// TestGovernanceVoteRollbackDeletesVoteWithNoSurvivingHistory covers a vote
// that predates the v22 upgrade: the migration backfill writes only one
// history row, for the value current at the time the migration ran, not for
// any earlier replacement. If that single row's transition_slot falls after
// the rollback target, the delete step removes it and leaves the vote with
// no surviving history at all. The restore step must fall back to deleting
// that vote (matching the pre-fix, pre-history behavior) instead of writing
// a NULL into the non-nullable vote/anchor columns.
func TestGovernanceVoteRollbackDeletesVoteWithNoSurvivingHistory(t *testing.T) {
	t.Parallel()
	store, rawDB := newManagementTestStoreWithRawDB(t)
	proposalID := seedGovernanceVoteProposal(t, store)

	voterCredential := credentialHash(0x45)
	addedSlot := uint64(100)
	preUpgradeReplacementSlot := uint64(200)
	rollbackSlot := uint64(150)

	_, err := rawDB.Exec(`
INSERT INTO governance_vote (
    proposal_id, voter_type, voter_credential_tag, voter_credential, vote,
    added_slot, vote_updated_slot
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		proposalID,
		models.VoterTypeDRep,
		0,
		voterCredential,
		models.VoteNo,
		addedSlot,
		preUpgradeReplacementSlot,
	)
	require.NoError(t, err)
	// Mirrors the v22 backfill: exactly one history row, at the slot the
	// vote's value was current when the migration ran, not at addedSlot.
	_, err = rawDB.Exec(`
INSERT INTO governance_vote_history (
    vote_id, transition_slot, vote
)
SELECT id, ?, vote FROM governance_vote
WHERE proposal_id = ? AND voter_credential = ?`,
		preUpgradeReplacementSlot,
		proposalID,
		voterCredential,
	)
	require.NoError(t, err)

	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(rollbackSlot, nil))

	votes, err := store.GetGovernanceVotes(proposalID, nil)
	require.NoError(t, err)
	require.Empty(
		t,
		votes,
		"a vote with no surviving history must be dropped, not left with a NULL vote",
	)
}
