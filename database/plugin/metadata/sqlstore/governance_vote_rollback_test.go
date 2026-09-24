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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

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
