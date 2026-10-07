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

// TestRestoreDrepStateAtSlot_RepairedDrepWithoutRegistration covers a DRep
// row the vote path recreated without a registration (InsertDrepIfAbsent):
// rolling back one of its later votes restores its expiry instead of failing
// the whole rollback for want of a registration.
func TestRestoreDrepStateAtSlot_RepairedDrepWithoutRegistration(
	t *testing.T,
) {
	t.Parallel()

	store := newMigratedTestStore(t)
	credential := bytes.Repeat([]byte{0xE1}, 28)
	require.NoError(t, store.InsertDrepIfAbsent(
		0, credential, 100, "", nil, true, nil,
	))
	require.NoError(t, store.UpdateDRepActivity(0, credential, 2, 20, 300, nil))
	require.NoError(t, store.UpdateDRepActivity(0, credential, 4, 20, 500, nil))

	require.NoError(t, store.RestoreDrepStateAtSlot(400, nil))
	drep, err := store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(22), drep.ExpiryEpoch)
	require.Equal(t, uint64(2), drep.LastActivityEpoch)
}

// TestRestoreDrepStateAtSlot_KeepsExpiryWithoutEarlierHistory covers a DRep
// whose only expiry history is after the rollback slot, as the v36 seed row
// leaves a DRep active shortly before an upgrade. Its certificate state is not
// being rewound, so it keeps the expiry it has rather than being reset to 0,
// which exempts a DRep from expiry.
func TestRestoreDrepStateAtSlot_KeepsExpiryWithoutEarlierHistory(
	t *testing.T,
) {
	t.Parallel()

	store := newMigratedTestStore(t)
	credential := bytes.Repeat([]byte{0xE2}, 28)
	require.NoError(t, store.ImportDrep(
		&models.Drep{Credential: credential, AddedSlot: 100, Active: true},
		&models.RegistrationDrep{DrepCredential: credential, AddedSlot: 100},
		nil,
	))
	_, err := store.writeDB.Exec(
		"DELETE FROM drep_expiry_history WHERE credential = ?",
		credential,
	)
	require.NoError(t, err)
	require.NoError(t, store.UpdateDRepActivity(0, credential, 3, 20, 300, nil))

	require.NoError(t, store.RestoreDrepStateAtSlot(200, nil))
	drep, err := store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(23), drep.ExpiryEpoch)
}

// TestGetGovernanceVotesAtSlot_OmitsVoteReplacedBeforeJournal covers a vote
// replaced before the v22 vote journal: its only history row is the
// replacement, so its value at an earlier slot is unknown and the vote is
// left out rather than reported with the later value.
func TestGetGovernanceVotesAtSlot_OmitsVoteReplacedBeforeJournal(
	t *testing.T,
) {
	t.Parallel()

	store := newMigratedTestStore(t)
	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0xE3}, 32),
		ActionType:    6,
		ExpiresEpoch:  10,
		AnchorHash:    bytes.Repeat([]byte{0xE4}, 32),
		ReturnAddress: append([]byte{0xe0}, bytes.Repeat([]byte{0xE5}, 28)...),
		GovActionCbor: []byte{0x81, 0x06},
		AddedSlot:     100,
	}
	require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	updated := uint64(200)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: bytes.Repeat([]byte{0xE6}, 28),
		Vote:            models.VoteNo,
		AddedSlot:       150,
		VoteUpdatedSlot: &updated,
	}, nil))
	_, err := store.writeDB.Exec(
		"DELETE FROM governance_vote_history WHERE transition_slot < 200",
	)
	require.NoError(t, err)

	votes, err := store.GetGovernanceVotesAtSlot(proposal.ID, 170, nil)
	require.NoError(t, err)
	require.Empty(t, votes)
	votes, err = store.GetGovernanceVotesAtSlot(proposal.ID, 250, nil)
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, uint8(models.VoteNo), votes[0].Vote)
}
