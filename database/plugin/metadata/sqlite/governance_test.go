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

package sqlite

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestStore(t *testing.T) *MetadataStoreSqlite {
	t.Helper()
	store, err := New("", nil, nil)
	require.NoError(t, err)
	require.NoError(t, store.Start())
	require.NoError(t, store.DB().AutoMigrate(models.MigrateModels...))
	t.Cleanup(func() {
		store.Close() //nolint:errcheck
	})
	return store
}

func TestGetConstitution(t *testing.T) {
	store := setupTestStore(t)

	// Initially no constitution
	constitution, err := store.GetConstitution(nil)
	require.NoError(t, err)
	assert.Nil(t, constitution)

	// Add a constitution
	err = store.SetConstitution(&models.Constitution{
		AnchorUrl:  "https://example.com/constitution.json",
		AnchorHash: []byte("hash123456789012345678901234567"),
		PolicyHash: []byte("policy12345678901234567890"),
		AddedSlot:  1000,
	}, nil)
	require.NoError(t, err)

	// Retrieve it
	constitution, err = store.GetConstitution(nil)
	require.NoError(t, err)
	require.NotNil(t, constitution)
	assert.Equal(
		t,
		"https://example.com/constitution.json",
		constitution.AnchorUrl,
	)
	assert.Equal(
		t,
		[]byte("hash123456789012345678901234567"),
		constitution.AnchorHash,
	)
	assert.Equal(
		t,
		[]byte("policy12345678901234567890"),
		constitution.PolicyHash,
	)
	assert.Equal(t, uint64(1000), constitution.AddedSlot)
}

func TestGetConstitutionDeletedSlot(t *testing.T) {
	store := setupTestStore(t)

	// Add a constitution
	err := store.SetConstitution(&models.Constitution{
		AnchorUrl:  "https://example.com/constitution.json",
		AnchorHash: []byte("hash123456789012345678901234567"),
		AddedSlot:  1000,
	}, nil)
	require.NoError(t, err)

	// Soft-delete it
	deletedSlot := uint64(2000)
	result := store.DB().Model(&models.Constitution{}).
		Where("added_slot = ?", 1000).
		Update("deleted_slot", deletedSlot)
	require.NoError(t, result.Error)

	// Should return nil for soft-deleted constitutions
	constitution, err := store.GetConstitution(nil)
	require.NoError(t, err)
	assert.Nil(t, constitution)
}

func TestGetCommitteeMember(t *testing.T) {
	store := setupTestStore(t)

	coldKey := []byte("cold_credential_12345678901234567890123456")
	hotKey := []byte("hot_credential_123456789012345678901234567")

	// Initially not found
	member, err := store.GetCommitteeMember(coldKey, nil)
	require.NoError(t, err)
	assert.Nil(t, member)

	// Add a committee member
	result := store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: coldKey,
		HostCredential: hotKey,
		CertificateID:  1,
		AddedSlot:      1000,
	})
	require.NoError(t, result.Error)

	// Retrieve it
	member, err = store.GetCommitteeMember(coldKey, nil)
	require.NoError(t, err)
	require.NotNil(t, member)
	assert.Equal(t, coldKey, member.ColdCredential)
	assert.Equal(t, hotKey, member.HostCredential)
}

func TestGetActiveCommitteeMembers(t *testing.T) {
	store := setupTestStore(t)

	coldKey1 := []byte("cold_credential_1234567890123456789012345a")
	hotKey1 := []byte("hot_credential_12345678901234567890123456a")
	coldKey2 := []byte("cold_credential_1234567890123456789012345b")
	hotKey2 := []byte("hot_credential_12345678901234567890123456b")

	// Add two committee members
	result := store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: coldKey1,
		HostCredential: hotKey1,
		CertificateID:  1,
		AddedSlot:      1000,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: coldKey2,
		HostCredential: hotKey2,
		CertificateID:  2,
		AddedSlot:      1000,
	})
	require.NoError(t, result.Error)

	// Both should be active
	members, err := store.GetActiveCommitteeMembers(nil)
	require.NoError(t, err)
	assert.Len(t, members, 2)
}

func TestIsCommitteeMemberResigned(t *testing.T) {
	store := setupTestStore(t)

	coldKey := []byte("cold_credential_12345678901234567890123456")
	hotKey := []byte("hot_credential_123456789012345678901234567")

	// Not found => not resigned
	resigned, err := store.IsCommitteeMemberResigned(coldKey, nil)
	require.NoError(t, err)
	assert.False(t, resigned)

	// Add a committee member
	result := store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: coldKey,
		HostCredential: hotKey,
		CertificateID:  1,
		AddedSlot:      1000,
	})
	require.NoError(t, result.Error)

	// Not resigned yet
	resigned, err = store.IsCommitteeMemberResigned(coldKey, nil)
	require.NoError(t, err)
	assert.False(t, resigned)

	// Add a resignation
	result = store.DB().Create(&models.ResignCommitteeCold{
		ColdCredential: coldKey,
		CertificateID:  2,
		AddedSlot:      2000,
	})
	require.NoError(t, result.Error)

	// Now resigned
	resigned, err = store.IsCommitteeMemberResigned(coldKey, nil)
	require.NoError(t, err)
	assert.True(t, resigned)
}

func TestGetActiveDreps(t *testing.T) {
	store := setupTestStore(t)

	// Initially empty
	dreps, err := store.GetActiveDreps(nil)
	require.NoError(t, err)
	assert.Empty(t, dreps)

	// Add an active DRep
	err = store.SetDrep(
		[]byte("drep_credential_1234567890123456789012345a"),
		1000,
		"https://drep1.example.com",
		[]byte("anchor_hash_1234567890123456789012"),
		true,
		nil,
	)
	require.NoError(t, err)

	// Add another active DRep
	err = store.SetDrep(
		[]byte("drep_credential_1234567890123456789012345b"),
		2000,
		"https://drep2.example.com",
		[]byte("anchor_hash_2234567890123456789012"),
		true,
		nil,
	)
	require.NoError(t, err)

	// Both should be returned
	dreps, err = store.GetActiveDreps(nil)
	require.NoError(t, err)
	assert.Len(t, dreps, 2)
	for _, drep := range dreps {
		assert.True(t, drep.Active)
	}

	// Manually deactivate one DRep via direct DB update
	// (SetDrep upsert has GORM zero-value limitations with bool false)
	result := store.DB().Model(&models.Drep{}).
		Where("credential = ?", []byte("drep_credential_1234567890123456789012345b")).
		Update("active", false)
	require.NoError(t, result.Error)

	// Now only one active DRep should be returned
	dreps, err = store.GetActiveDreps(nil)
	require.NoError(t, err)
	assert.Len(t, dreps, 1)
	assert.Equal(
		t,
		[]byte("drep_credential_1234567890123456789012345a"),
		dreps[0].Credential,
	)
	assert.True(t, dreps[0].Active)
	assert.Equal(t, "https://drep1.example.com", dreps[0].AnchorUrl)
}

func TestGetDrep(t *testing.T) {
	store := setupTestStore(t)

	cred := []byte("drep_credential_1234567890123456789012345a")

	// Not found
	drep, err := store.GetDrep(cred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, drep)

	// Add an active DRep
	err = store.SetDrep(
		cred,
		1000,
		"https://drep.example.com",
		[]byte("anchor_hash_1234567890123456789012"),
		true,
		nil,
	)
	require.NoError(t, err)

	// Found when active
	drep, err = store.GetDrep(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.Equal(t, cred, drep.Credential)
	assert.Equal(t, "https://drep.example.com", drep.AnchorUrl)
	assert.True(t, drep.Active)

	// Deactivate via direct DB update
	// (SetDrep upsert has GORM zero-value limitations with bool false)
	result := store.DB().Model(&models.Drep{}).
		Where("credential = ?", cred).
		Update("active", false)
	require.NoError(t, result.Error)

	// Not found when excluding inactive
	drep, err = store.GetDrep(cred, false, nil)
	require.NoError(t, err)
	assert.Nil(t, drep)

	// Found when including inactive
	drep, err = store.GetDrep(cred, true, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.False(t, drep.Active)
}

func TestGetGovernanceProposal(t *testing.T) {
	store := setupTestStore(t)

	txHash := []byte("tx_hash_1234567890123456789012345678901234")
	actionIndex := uint32(0)

	// Not found
	proposal, err := store.GetGovernanceProposal(
		txHash,
		actionIndex,
		nil,
	)
	require.NoError(t, err)
	assert.Nil(t, proposal)

	// Add a proposal
	err = store.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        txHash,
		ActionIndex:   actionIndex,
		ActionType:    uint8(6), // Info action
		ProposedEpoch: 100,
		ExpiresEpoch:  200,
		AnchorUrl:     "https://proposal.example.com",
		AnchorHash:    []byte("anchor_hash_1234567890123456789012"),
		Deposit:       500000000,
		ReturnAddress: []byte("return_addr_1234567890123456789"),
		AddedSlot:     5000,
	}, nil)
	require.NoError(t, err)

	// Found
	proposal, err = store.GetGovernanceProposal(
		txHash,
		actionIndex,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, proposal)
	assert.Equal(t, txHash, proposal.TxHash)
	assert.Equal(t, actionIndex, proposal.ActionIndex)
	assert.Equal(t, uint8(6), proposal.ActionType)
	assert.Equal(t, uint64(100), proposal.ProposedEpoch)
}

func TestGetActiveGovernanceProposals(t *testing.T) {
	store := setupTestStore(t)

	// Add an active proposal (expires in epoch 200)
	err := store.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        []byte("tx_hash_1234567890123456789012345678901234"),
		ActionIndex:   0,
		ActionType:    uint8(6),
		ProposedEpoch: 100,
		ExpiresEpoch:  200,
		AnchorUrl:     "https://active.example.com",
		AnchorHash:    []byte("anchor_hash_1234567890123456789012"),
		Deposit:       500000000,
		ReturnAddress: []byte("return_addr_1234567890123456789"),
		AddedSlot:     5000,
	}, nil)
	require.NoError(t, err)

	// Add an expired proposal (expires in epoch 50)
	err = store.SetGovernanceProposal(&models.GovernanceProposal{
		TxHash:        []byte("tx_hash_2234567890123456789012345678901234"),
		ActionIndex:   0,
		ActionType:    uint8(6),
		ProposedEpoch: 30,
		ExpiresEpoch:  50,
		AnchorUrl:     "https://expired.example.com",
		AnchorHash:    []byte("anchor_hash_2234567890123456789012"),
		Deposit:       500000000,
		ReturnAddress: []byte("return_addr_2234567890123456789"),
		AddedSlot:     3000,
	}, nil)
	require.NoError(t, err)

	// Query at epoch 100 - only active proposal should be returned
	proposals, err := store.GetActiveGovernanceProposals(100, nil)
	require.NoError(t, err)
	assert.Len(t, proposals, 1)
	assert.Equal(t, "https://active.example.com", proposals[0].AnchorUrl)

	// Query at epoch 30 - both should be returned
	proposals, err = store.GetActiveGovernanceProposals(30, nil)
	require.NoError(t, err)
	assert.Len(t, proposals, 2)
}

func TestUpdateDRepActivity(t *testing.T) {
	store := setupTestStore(t)

	cred := []byte("drep_credential_1234567890123456789012345a")

	// Create a DRep
	err := store.SetDrep(cred, 1000, "https://drep.example.com",
		[]byte("anchor_hash_1234567890123456789012"), true, nil)
	require.NoError(t, err)

	// Verify initial state (no activity epoch set)
	drep, err := store.GetDrep(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.Equal(t, uint64(0), drep.LastActivityEpoch)
	assert.Equal(t, uint64(0), drep.ExpiryEpoch)

	// Update activity at epoch 100 with inactivity period of 20
	err = store.UpdateDRepActivity(cred, 100, 20, nil)
	require.NoError(t, err)

	// Verify activity was updated
	drep, err = store.GetDrep(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.Equal(t, uint64(100), drep.LastActivityEpoch)
	assert.Equal(t, uint64(120), drep.ExpiryEpoch)

	// Update activity again at epoch 110
	err = store.UpdateDRepActivity(cred, 110, 20, nil)
	require.NoError(t, err)

	drep, err = store.GetDrep(cred, false, nil)
	require.NoError(t, err)
	require.NotNil(t, drep)
	assert.Equal(t, uint64(110), drep.LastActivityEpoch)
	assert.Equal(t, uint64(130), drep.ExpiryEpoch)

	// Updating a non-existent DRep should return an error
	err = store.UpdateDRepActivity(
		[]byte("nonexistent_credential_1234567890123456"),
		200,
		20,
		nil,
	)
	require.ErrorIs(t, err, models.ErrDrepActivityNotUpdated)
}

func TestGetExpiredDReps(t *testing.T) {
	store := setupTestStore(t)

	credA := []byte("drep_credential_1234567890123456789012345a")
	credB := []byte("drep_credential_1234567890123456789012345b")
	credC := []byte("drep_credential_1234567890123456789012345c")

	// Create three DReps
	err := store.SetDrep(credA, 1000, "", nil, true, nil)
	require.NoError(t, err)
	err = store.SetDrep(credB, 1000, "", nil, true, nil)
	require.NoError(t, err)
	err = store.SetDrep(credC, 1000, "", nil, true, nil)
	require.NoError(t, err)

	// Set activity: A expires at epoch 120, B at 130, C at 150
	err = store.UpdateDRepActivity(credA, 100, 20, nil)
	require.NoError(t, err)
	err = store.UpdateDRepActivity(credB, 110, 20, nil)
	require.NoError(t, err)
	err = store.UpdateDRepActivity(credC, 130, 20, nil)
	require.NoError(t, err)

	// At epoch 119: no DReps expired
	expired, err := store.GetExpiredDReps(119, nil)
	require.NoError(t, err)
	assert.Empty(t, expired)

	// At epoch 120: DRep A expired
	expired, err = store.GetExpiredDReps(120, nil)
	require.NoError(t, err)
	assert.Len(t, expired, 1)
	assert.Equal(t, credA, expired[0].Credential)

	// At epoch 130: DReps A and B expired
	expired, err = store.GetExpiredDReps(130, nil)
	require.NoError(t, err)
	assert.Len(t, expired, 2)

	// At epoch 200: all three expired
	expired, err = store.GetExpiredDReps(200, nil)
	require.NoError(t, err)
	assert.Len(t, expired, 3)
}

func TestGetDRepVotingPower(t *testing.T) {
	store := setupTestStore(t)

	drepCred := []byte("drep_credential_1234567890123456789012345a")

	// Create a DRep
	err := store.SetDrep(drepCred, 1000, "", nil, true, nil)
	require.NoError(t, err)

	// No delegators = zero voting power
	power, err := store.GetDRepVotingPower(drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), power)

	// Create accounts delegated to this DRep
	stakingKey1 := []byte("staking_key_12345678901234567890")
	stakingKey2 := []byte("staking_key_22345678901234567890")

	result := store.DB().Create(&models.Account{
		StakingKey: stakingKey1,
		Drep:       drepCred,
		Active:     true,
		AddedSlot:  1000,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.Account{
		StakingKey: stakingKey2,
		Drep:       drepCred,
		Active:     true,
		AddedSlot:  1000,
	})
	require.NoError(t, result.Error)

	// Create UTxOs for these staking keys
	result = store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_id_12345678901234567890123456789012"),
		StakingKey: stakingKey1,
		Amount:     5000000,
		OutputIdx:  0,
		AddedSlot:  1000,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_id_22345678901234567890123456789012"),
		StakingKey: stakingKey1,
		Amount:     3000000,
		OutputIdx:  0,
		AddedSlot:  1000,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.Utxo{
		TxId:       []byte("tx_id_32345678901234567890123456789012"),
		StakingKey: stakingKey2,
		Amount:     2000000,
		OutputIdx:  0,
		AddedSlot:  1000,
	})
	require.NoError(t, result.Error)

	// Add a deleted UTxO that should not count
	result = store.DB().Create(&models.Utxo{
		TxId:        []byte("tx_id_42345678901234567890123456789012"),
		StakingKey:  stakingKey2,
		Amount:      9000000,
		OutputIdx:   0,
		AddedSlot:   1000,
		DeletedSlot: 2000,
	})
	require.NoError(t, result.Error)

	// Voting power should be 5M + 3M + 2M = 10M
	power, err = store.GetDRepVotingPower(drepCred, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(10000000), power)
}

func TestGetCommitteeActiveCount(t *testing.T) {
	store := setupTestStore(t)

	// Initially zero
	count, err := store.GetCommitteeActiveCount(nil)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Create transaction records (parent for certificates via FK constraint).
	result := store.DB().Create(&models.Transaction{
		ID:   100,
		Hash: []byte("committee_tx_hash_1234567890123456"),
		Slot: 1000,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.Transaction{
		ID:   200,
		Hash: []byte("committee_tx_hash_2234567890123456"),
		Slot: 2000,
	})
	require.NoError(t, result.Error)

	// Create certificate records that the committee queries join against.
	// Each certificate needs a unique (TransactionID, CertIndex) pair.
	result = store.DB().Create(&models.Certificate{
		ID:            1,
		TransactionID: 100,
		CertIndex:     0,
		Slot:          1000,
		CertType:      14, // AuthCommitteeHot cert type
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.Certificate{
		ID:            2,
		TransactionID: 100,
		CertIndex:     1,
		Slot:          1000,
		CertType:      14,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.Certificate{
		ID:            3,
		TransactionID: 200,
		CertIndex:     0,
		Slot:          2000,
		CertType:      15, // ResignCommitteeCold cert type
	})
	require.NoError(t, result.Error)

	// Add two committee members
	result = store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: []byte("cold_credential_1234567890123456789012345a"),
		HostCredential: []byte("hot_credential_12345678901234567890123456a"),
		CertificateID:  1,
		AddedSlot:      1000,
	})
	require.NoError(t, result.Error)

	result = store.DB().Create(&models.AuthCommitteeHot{
		ColdCredential: []byte("cold_credential_1234567890123456789012345b"),
		HostCredential: []byte("hot_credential_12345678901234567890123456b"),
		CertificateID:  2,
		AddedSlot:      1000,
	})
	require.NoError(t, result.Error)

	count, err = store.GetCommitteeActiveCount(nil)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Resign one member
	result = store.DB().Create(&models.ResignCommitteeCold{
		ColdCredential: []byte("cold_credential_1234567890123456789012345a"),
		CertificateID:  3,
		AddedSlot:      2000,
	})
	require.NoError(t, result.Error)

	count, err = store.GetCommitteeActiveCount(nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// NOTE: IsCommitteeThresholdMet is tested in ledger/view_test.go.
// It is not duplicated here to avoid maintaining parallel
// implementations. The sqlite package cannot import ledger
// (circular dependency), so threshold logic tests live in
// the ledger package.
