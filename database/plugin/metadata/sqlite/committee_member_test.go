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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

func TestSetCommitteeMembers(t *testing.T) {
	store := setupTestStore(t)

	members := []*models.CommitteeMember{
		{
			ColdCredHash: []byte("cred_hash_1_2345678901234567"),
			ExpiresEpoch: 300,
			AddedSlot:    5000,
		},
		{
			ColdCredHash: []byte("cred_hash_2_2345678901234567"),
			ExpiresEpoch: 350,
			AddedSlot:    5000,
		},
	}

	err := store.SetCommitteeMembers(members, nil)
	require.NoError(t, err)

	// Verify they were persisted
	result, err := store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestSetCommitteeMembersEmpty(t *testing.T) {
	store := setupTestStore(t)

	// Empty slice should be a no-op
	err := store.SetCommitteeMembers(
		[]*models.CommitteeMember{}, nil,
	)
	require.NoError(t, err)

	result, err := store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	assert.Empty(t, result)
}

func TestSetCommitteeMembersUpsert(t *testing.T) {
	store := setupTestStore(t)

	cred := []byte("cred_hash_1_2345678901234567")

	// Insert initial member
	err := store.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredHash: cred,
			ExpiresEpoch: 300,
			AddedSlot:    5000,
		},
	}, nil)
	require.NoError(t, err)

	// Upsert with new expiry epoch
	err = store.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredHash: cred,
			ExpiresEpoch: 400,
			AddedSlot:    6000,
		},
	}, nil)
	require.NoError(t, err)

	// Should still be one member with updated expiry
	result, err := store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, cred, result[0].ColdCredHash)
	assert.Equal(t, uint64(400), result[0].ExpiresEpoch)
	assert.Equal(t, uint64(6000), result[0].AddedSlot)
}

func TestGetCommitteeMembersExcludesDeleted(t *testing.T) {
	store := setupTestStore(t)

	cred1 := []byte("cred_hash_1_2345678901234567")
	cred2 := []byte("cred_hash_2_2345678901234567")

	err := store.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredHash: cred1,
			ExpiresEpoch: 300,
			AddedSlot:    5000,
		},
		{
			ColdCredHash: cred2,
			ExpiresEpoch: 350,
			AddedSlot:    5000,
		},
	}, nil)
	require.NoError(t, err)

	// Soft-delete one member
	deletedSlot := uint64(6000)
	result := store.DB().Model(&models.CommitteeMember{}).
		Where("cold_cred_hash = ?", cred1).
		Update("deleted_slot", deletedSlot)
	require.NoError(t, result.Error)

	// Only non-deleted member should be returned
	members, err := store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, cred2, members[0].ColdCredHash)
}

func TestDeleteCommitteeMembersAfterSlot(t *testing.T) {
	store := setupTestStore(t)

	// Add members at different slots
	err := store.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredHash: []byte("cred_hash_1_2345678901234567"),
			ExpiresEpoch: 300,
			AddedSlot:    5000,
		},
		{
			ColdCredHash: []byte("cred_hash_2_2345678901234567"),
			ExpiresEpoch: 350,
			AddedSlot:    7000,
		},
	}, nil)
	require.NoError(t, err)

	// Rollback to slot 6000 — should delete the member at slot 7000
	err = store.DeleteCommitteeMembersAfterSlot(6000, nil)
	require.NoError(t, err)

	members, err := store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(
		t,
		[]byte("cred_hash_1_2345678901234567"),
		members[0].ColdCredHash,
	)
}

func TestDeleteCommitteeMembersAfterSlotClearsDeletedSlot(t *testing.T) {
	store := setupTestStore(t)

	cred := []byte("cred_hash_1_2345678901234567")

	// Add a member at slot 5000
	err := store.SetCommitteeMembers([]*models.CommitteeMember{
		{
			ColdCredHash: cred,
			ExpiresEpoch: 300,
			AddedSlot:    5000,
		},
	}, nil)
	require.NoError(t, err)

	// Soft-delete it at slot 7000
	result := store.DB().Model(&models.CommitteeMember{}).
		Where("cold_cred_hash = ?", cred).
		Update("deleted_slot", uint64(7000))
	require.NoError(t, result.Error)

	// Verify it's excluded from active members
	members, err := store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	assert.Empty(t, members)

	// Rollback to slot 6000 — should clear deleted_slot since 7000 > 6000
	err = store.DeleteCommitteeMembersAfterSlot(6000, nil)
	require.NoError(t, err)

	// Now it should be active again
	members, err = store.GetCommitteeMembers(nil)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, cred, members[0].ColdCredHash)
	assert.Nil(t, members[0].DeletedSlot)
}
