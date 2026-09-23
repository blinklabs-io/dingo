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
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func TestDrepUpdateRequiresActiveRegistration(t *testing.T) {
	t.Parallel()

	store := newMigratedSQLiteStore(t)
	credential := common.NewBlake2b224(bytes.Repeat([]byte{0x5a}, 28))
	model := &models.Drep{
		CredentialTag: 0,
		Credential:    credential[:],
		AddedSlot:     10,
		Active:        false,
	}
	require.NoError(t, store.CreateDrep(nil, model))
	db, ctx, err := store.dbFromTxn(nil)
	require.NoError(t, err)
	cert := &common.UpdateDrepCertificate{
		DrepCredential: common.Credential{
			CredType:   0,
			Credential: credential,
		},
	}

	_, err = applyDrepUpdateCertificate(ctx, db, cert, 0, 11)
	require.ErrorIs(t, err, models.ErrDrepNotFound,
		"an inactive historical row must not authorize a DRep update")

	require.NoError(t, setDrepCertificateState(
		ctx,
		db,
		0,
		credential[:],
		12,
		"",
		nil,
		true,
		false,
	))
	_, err = applyDrepUpdateCertificate(ctx, db, cert, 0, 13)
	require.NoError(t, err,
		"a new registration must make subsequent updates valid")
	updated, err := store.GetDrepByCredential(0, credential[:], true, nil)
	require.NoError(t, err)
	require.True(t, updated.Active)
	require.Equal(t, uint64(13), updated.AddedSlot)

	activeCredential := common.NewBlake2b224(bytes.Repeat([]byte{0x5b}, 28))
	active := &models.Drep{
		CredentialTag: 1,
		Credential:    activeCredential[:],
		AddedSlot:     20,
		Active:        true,
	}
	require.NoError(t, store.CreateDrep(nil, active))
	validUpdate := &common.UpdateDrepCertificate{
		DrepCredential: common.Credential{
			CredType:   1,
			Credential: activeCredential,
		},
	}
	_, err = applyDrepUpdateCertificate(ctx, db, validUpdate, 0, 21)
	require.NoError(t, err)
	updated, err = store.GetDrepByCredential(1, activeCredential[:], true, nil)
	require.NoError(t, err)
	require.True(t, updated.Active)
}

func TestDormantDRepExpiryBumpIsIdempotentAndRollbackable(t *testing.T) {
	t.Parallel()

	store := newMigratedSQLiteStore(t)
	credential := bytes.Repeat([]byte{0x5c}, 28)
	require.NoError(t, store.CreateDrep(nil, &models.Drep{
		CredentialTag:     0,
		Credential:        credential,
		ExpiryEpoch:       20,
		LastActivityEpoch: 4,
		Active:            true,
	}))

	updated, err := store.BumpDormantDRepExpiries(30, nil)
	require.NoError(t, err)
	require.Equal(t, 1, updated)
	updated, err = store.BumpDormantDRepExpiries(30, nil)
	require.NoError(t, err)
	require.Zero(t, updated, "replaying a boundary must not extend expiry twice")

	require.NoError(t, store.UpdateDRepActivity(0, credential, 40, 25, 5, nil))
	drep, err := store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), drep.ExpiryEpoch)
	require.Equal(t, uint64(25), drep.LastActivityEpoch)

	db, ctx, err := store.dbFromTxn(nil)
	require.NoError(t, err)
	require.NoError(t, store.restoreDrepExpiryHistory(db, ctx, 39))
	drep, err = store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(21), drep.ExpiryEpoch)
	require.Equal(t, uint64(4), drep.LastActivityEpoch)

	require.NoError(t, store.restoreDrepExpiryHistory(db, ctx, 29))
	drep, err = store.GetDrepByCredential(0, credential, true, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(20), drep.ExpiryEpoch)
	require.Equal(t, uint64(4), drep.LastActivityEpoch)
}

func TestDrepDeregistrationEffectsPreserveTaggedStateAndRollback(t *testing.T) {
	t.Parallel()

	store := newMigratedSQLiteStore(t)
	credential := bytes.Repeat([]byte{0x61}, 28)
	accounts := []*models.Account{
		{
			StakingKey:    bytes.Repeat([]byte{0x62}, 28),
			CredentialTag: 0,
			Drep:          credential,
			DrepType:      models.DrepTypeAddrKeyHash,
			AddedSlot:     10,
			CreatedSlot:   10,
			Active:        true,
		},
		{
			StakingKey:    bytes.Repeat([]byte{0x63}, 28),
			CredentialTag: 0,
			Drep:          credential,
			DrepType:      models.DrepTypeAddrKeyHash,
			AddedSlot:     11,
			CreatedSlot:   11,
			Active:        true,
		},
		{
			StakingKey:    bytes.Repeat([]byte{0x64}, 28),
			CredentialTag: 0,
			Drep:          credential,
			DrepType:      models.DrepTypeScriptHash,
			AddedSlot:     12,
			CreatedSlot:   12,
			Active:        true,
		},
		{
			StakingKey:    bytes.Repeat([]byte{0x65}, 28),
			CredentialTag: 0,
			DrepType:      models.DrepTypeAlwaysAbstain,
			AddedSlot:     13,
			CreatedSlot:   13,
			Active:        true,
		},
	}
	for _, account := range accounts {
		require.NoError(t, store.ImportAccount(account, nil))
	}
	proposals := []*models.GovernanceProposal{
		{TxHash: bytes.Repeat([]byte{0x66}, 32), ExpiresEpoch: 10, AddedSlot: 5},
		{TxHash: bytes.Repeat([]byte{0x67}, 32), ExpiresEpoch: 10, AddedSlot: 6},
	}
	for _, proposal := range proposals {
		require.NoError(t, store.SetGovernanceProposal(proposal, nil))
	}
	for index, proposal := range proposals {
		updatedSlot := uint64(20 + index)
		require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
			ProposalID:         proposal.ID,
			VoterType:          models.VoterTypeDRep,
			VoterCredentialTag: 0,
			VoterCredential:    credential,
			Vote:               1,
			AddedSlot:          updatedSlot,
			VoteUpdatedSlot:    &updatedSlot,
		}, nil))
	}
	updatedSlot := uint64(22)
	require.NoError(t, store.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:         proposals[0].ID,
		VoterType:          models.VoterTypeDRep,
		VoterCredentialTag: 1,
		VoterCredential:    credential,
		Vote:               1,
		AddedSlot:          updatedSlot,
		VoteUpdatedSlot:    &updatedSlot,
	}, nil))

	cleared, err := store.ClearDRepDelegationForCredential(0, credential, 30, nil)
	require.NoError(t, err)
	require.Equal(t, 2, cleared)
	deleted, err := store.DeleteGovernanceVotesForDrep(0, credential, 30, nil)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	for index, account := range accounts {
		got, err := store.GetAccountByCredential(
			account.CredentialTag,
			account.StakingKey,
			true,
			nil,
		)
		require.NoError(t, err)
		if index < 2 {
			require.Nil(t, got.Drep)
			require.Equal(t, uint64(30), got.AddedSlot)
		} else {
			require.Equal(t, account.Drep, got.Drep)
			require.Equal(t, account.DrepType, got.DrepType)
		}
	}
	keyVotes, err := store.GetGovernanceVotes(proposals[0].ID, nil)
	require.NoError(t, err)
	require.Len(t, keyVotes, 1)
	require.Equal(t, uint8(1), keyVotes[0].VoterCredentialTag)
	otherVotes, err := store.GetGovernanceVotes(proposals[1].ID, nil)
	require.NoError(t, err)
	require.Empty(t, otherVotes)

	require.NoError(t, store.RestoreAccountStateAtSlot(29, nil))
	require.NoError(t, store.DeleteGovernanceVotesAfterSlot(29, nil))
	for index, account := range accounts[:2] {
		got, err := store.GetAccountByCredential(0, account.StakingKey, true, nil)
		require.NoError(t, err)
		require.Equal(t, credential, got.Drep, "account %d", index)
	}
	keyVotes, err = store.GetGovernanceVotes(proposals[0].ID, nil)
	require.NoError(t, err)
	require.Len(t, keyVotes, 2)
}
