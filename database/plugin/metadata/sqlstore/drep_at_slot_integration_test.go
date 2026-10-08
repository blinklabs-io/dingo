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

//go:build dingo_db_integration

package sqlstore

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func TestPostgresDrepAndGovernanceAtSlot(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	testDrepAndGovernanceAtSlot(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
	)
}

func TestMySQLDrepAndGovernanceAtSlot(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	testDrepAndGovernanceAtSlot(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
	)
}

// testDrepAndGovernanceAtSlot runs the drep_expiry_history writes and the
// at-slot DRep and governance reads on a backend's own SQL dialect.
func testDrepAndGovernanceAtSlot(t *testing.T, store *Store) {
	t.Helper()
	credential := bytes.Repeat([]byte{0xD9}, 28)
	deposit := types.Uint64(500_000_000)
	require.NoError(t, store.ImportDrep(
		&models.Drep{
			Credential:        credential,
			AddedSlot:         100,
			LastActivityEpoch: 1,
			ExpiryEpoch:       21,
			Active:            true,
		},
		&models.RegistrationDrep{
			DrepCredential: credential,
			AddedSlot:      100,
			DepositAmount:  deposit,
		},
		nil,
	))
	// The second write at the same slot takes the conflict path.
	require.NoError(t, store.UpdateDRepActivity(0, credential, 3, 20, 300, nil))
	require.NoError(t, store.UpdateDRepActivity(0, credential, 4, 20, 300, nil))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: bytes.Repeat([]byte{0x5D}, 28),
		Drep:       credential,
		DrepType:   models.DrepTypeAddrKeyHash,
		AddedSlot:  150,
		Active:     true,
	}))

	expiryAt := func(slot uint64) uint64 {
		t.Helper()
		dreps, err := store.GetDrepsAtSlot(nil, slot, nil)
		require.NoError(t, err)
		require.Len(t, dreps, 1)
		return dreps[0].ExpiryEpoch
	}
	require.Equal(t, uint64(21), expiryAt(200))
	require.Equal(t, uint64(24), expiryAt(400))
	deposits, err := store.GetDrepRegistrationDepositsAtSlot(nil, 200, nil)
	require.NoError(t, err)
	require.Equal(
		t,
		uint64(deposit),
		deposits[models.DrepDepositKey(0, credential)],
	)
	delegators, err := store.GetDRepDelegatorsAtSlot(nil, 200, nil)
	require.NoError(t, err)
	require.Len(
		t,
		delegators[models.NewStakeCredentialRef(0, credential).MapKey()],
		1,
	)

	proposal := &models.GovernanceProposal{
		TxHash:        bytes.Repeat([]byte{0x7A}, 32),
		ActionType:    6,
		ExpiresEpoch:  10,
		AnchorHash:    bytes.Repeat([]byte{0x7B}, 32),
		ReturnAddress: append([]byte{0xe0}, bytes.Repeat([]byte{0x7C}, 28)...),
		GovActionCbor: []byte{0x81, 0x06},
		AddedSlot:     100,
	}
	require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	vote := &models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeDRep,
		VoterCredential: credential,
		Vote:            models.VoteYes,
		AddedSlot:       150,
	}
	require.NoError(t, store.SetGovernanceVote(vote, nil))
	updated := uint64(250)
	vote.Vote = models.VoteNo
	vote.VoteUpdatedSlot = &updated
	require.NoError(t, store.SetGovernanceVote(vote, nil))
	proposals, err := store.GetGovernanceProposalSetAtSlot(50, nil)
	require.NoError(t, err)
	require.Empty(t, proposals)
	proposals, err = store.GetGovernanceProposalSetAtSlot(200, nil)
	require.NoError(t, err)
	require.Len(t, proposals, 1)
	for _, tc := range []struct {
		slot uint64
		want uint8
	}{
		{200, models.VoteYes},
		{300, models.VoteNo},
	} {
		votes, err := store.GetGovernanceVotesAtSlot(proposal.ID, tc.slot, nil)
		require.NoError(t, err)
		require.Len(t, votes, 1)
		require.Equal(t, tc.want, votes[0].Vote)
	}

	require.NoError(t, store.RestoreDrepStateAtSlot(200, nil))
	drep, err := store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(21), drep.ExpiryEpoch)
}
