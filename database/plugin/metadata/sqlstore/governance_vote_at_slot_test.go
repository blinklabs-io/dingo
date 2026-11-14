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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestGetGovernanceVotesAtSlot_OmitsVoteDeletedBeforeRecast covers a DRep
// that votes, deregisters (soft-deleting its votes), then registers and
// votes again on the same proposal. The recast clears deleted_slot on the
// row, so a read at a slot inside the deregistered window must still omit
// the vote.
func TestGetGovernanceVotesAtSlot_OmitsVoteDeletedBeforeRecast(
	t *testing.T,
) {
	t.Parallel()

	store := newMigratedTestStore(t)
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0xF3}, 32),
		ActionType:    6,
		ExpiresEpoch:  10,
		AnchorHash:    bytes.Repeat([]byte{0xF4}, 32),
		ReturnAddress: append([]byte{0xe0}, bytes.Repeat([]byte{0xF5}, 28)...),
		GovActionCbor: []byte{0x81, 0x06},
		AddedSlot:     100,
	}
	require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	credential := bytes.Repeat([]byte{0xF6}, 28)
	cast := func(v uint8, slot uint64) {
		t.Helper()
		var updated *uint64
		added := uint64(150)
		if slot != added {
			updated = &slot
		}
		require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:      proposal.ID,
			VoterType:       models.VoterTypeDRep,
			VoterCredential: credential,
			Vote:            v,
			AddedSlot:       added,
			VoteUpdatedSlot: updated,
		}, nil))
	}

	cast(models.VoteNo, 150)
	deleted, err := store.DeleteGovernanceVotesForDrep(
		0,
		credential,
		1,
		200,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	cast(models.VoteYes, 300)

	votes, err := store.GetGovernanceVotesAtSlot(proposal.ID, 175, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1, "vote live before deregistration")
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)

	votes, err = store.GetGovernanceVotesAtSlot(proposal.ID, 250, nil)
	require.NoError(t, err)
	require.Empty(t, votes, "vote was deleted at slot 200 and not yet recast")

	votes, err = store.GetGovernanceVotesAtSlot(proposal.ID, 350, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1, "vote recast at slot 300")
	require.Equal(t, uint8(models.VoteYes), votes[0].Vote)
}

// TestDeleteGovernanceVotesAfterSlot_RestoresDeletionAcrossRecast covers a
// rollback that lands between a DRep's vote deletion and its recast: the
// recast is undone, and the vote is deleted again because the recast had
// cleared deleted_slot on the live row.
func TestDeleteGovernanceVotesAfterSlot_RestoresDeletionAcrossRecast(
	t *testing.T,
) {
	t.Parallel()

	store := newMigratedTestStore(t)
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0xF7}, 32),
		ActionType:    6,
		ExpiresEpoch:  10,
		AnchorHash:    bytes.Repeat([]byte{0xF8}, 32),
		ReturnAddress: append([]byte{0xe0}, bytes.Repeat([]byte{0xF9}, 28)...),
		GovActionCbor: []byte{0x81, 0x06},
		AddedSlot:     100,
	}
	require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	credential := bytes.Repeat([]byte{0xFA}, 28)
	recast := uint64(300)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeDRep,
		VoterCredential: credential, Vote: models.VoteNo, AddedSlot: 150,
	}, nil))
	_, err := store.DeleteGovernanceVotesForDrep(0, credential, 1, 200, nil)
	require.NoError(t, err)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeDRep,
		VoterCredential: credential, Vote: models.VoteYes, AddedSlot: 150,
		VoteUpdatedSlot: &recast,
	}, nil))
	live, err := store.GetGovernanceVotes(proposal.ID, nil)
	require.NoError(t, err)
	require.Len(t, live, 1)

	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(250, nil))

	live, err = store.GetGovernanceVotes(proposal.ID, nil)
	require.NoError(t, err)
	require.Empty(t, live, "vote deleted at slot 200 stays deleted")
	votes, err := store.GetGovernanceVotesAtSlot(proposal.ID, 175, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)
	votes, err = store.GetGovernanceVotesAtSlot(proposal.ID, 250, nil)
	require.NoError(t, err)
	require.Empty(t, votes)

	// Casting again after the rollback revives the vote.
	again := uint64(260)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeDRep,
		VoterCredential: credential, Vote: models.VoteNo, AddedSlot: 150,
		VoteUpdatedSlot: &again,
	}, nil))
	votes, err = store.GetGovernanceVotesAtSlot(proposal.ID, 270, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
}

// TestGetGovernanceVotesPage_DeletionBelongsToDeletedCast covers the cast
// listing after a delete and recast: the journaled deletion is not listed as
// a cast, the cast it deleted reports its deleted slot, and the recast does
// not.
func TestGetGovernanceVotesPage_DeletionBelongsToDeletedCast(t *testing.T) {
	t.Parallel()

	store := newMigratedTestStore(t)
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0xFB}, 32),
		ActionType:    6,
		ExpiresEpoch:  10,
		AnchorHash:    bytes.Repeat([]byte{0xFC}, 32),
		ReturnAddress: append([]byte{0xe0}, bytes.Repeat([]byte{0xFD}, 28)...),
		GovActionCbor: []byte{0x81, 0x06},
		AddedSlot:     100,
	}
	require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	credential := bytes.Repeat([]byte{0xFE}, 28)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeDRep,
		VoterCredential: credential, Vote: models.VoteNo, AddedSlot: 150,
	}, nil))
	_, err := store.DeleteGovernanceVotesForDrep(0, credential, 1, 200, nil)
	require.NoError(t, err)
	recast := uint64(300)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID: proposal.ID, VoterType: models.VoterTypeDRep,
		VoterCredential: credential, Vote: models.VoteYes, AddedSlot: 150,
		VoteUpdatedSlot: &recast,
	}, nil))

	rows, total, err := store.GetGovernanceVotesPage(
		models.GovernanceVoteFilter{ProposalID: &proposal.ID},
		10, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Equal(t, 2, total)
	require.Len(t, rows, 2)
	require.Equal(t, uint8(models.VoteNo), rows[0].Vote.Vote)
	require.NotNil(t, rows[0].Vote.DeletedSlot)
	require.Equal(t, uint64(200), *rows[0].Vote.DeletedSlot)
	require.False(t, rows[0].Counted)
	require.Equal(t, uint8(models.VoteYes), rows[1].Vote.Vote)
	require.Nil(t, rows[1].Vote.DeletedSlot)
	require.True(t, rows[1].Counted)
}
