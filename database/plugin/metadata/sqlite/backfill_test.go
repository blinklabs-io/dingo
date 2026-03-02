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
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBackfillCheckpoint_NotFound(t *testing.T) {
	store := setupTestDB(t)

	cp, err := store.GetBackfillCheckpoint("metadata", nil)
	require.NoError(t, err)
	assert.Nil(t, cp, "non-existent checkpoint should return nil")
}

func TestSetBackfillCheckpoint_Create(t *testing.T) {
	store := setupTestDB(t)

	now := time.Now().Truncate(time.Second)
	cp := &models.BackfillCheckpoint{
		Phase:      "metadata",
		LastSlot:   1000,
		TotalSlots: 50000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}

	err := store.SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	got, err := store.GetBackfillCheckpoint("metadata", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "metadata", got.Phase)
	assert.Equal(t, uint64(1000), got.LastSlot)
	assert.Equal(t, uint64(50000), got.TotalSlots)
	assert.False(t, got.Completed)
}

func TestSetBackfillCheckpoint_Upsert(t *testing.T) {
	store := setupTestDB(t)

	now := time.Now().Truncate(time.Second)
	cp := &models.BackfillCheckpoint{
		Phase:      "metadata",
		LastSlot:   1000,
		TotalSlots: 50000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	err := store.SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Update the checkpoint
	cp.LastSlot = 25000
	cp.UpdatedAt = now.Add(10 * time.Second)
	cp.Completed = true
	err = store.SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	got, err := store.GetBackfillCheckpoint("metadata", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, uint64(25000), got.LastSlot)
	assert.True(t, got.Completed)
}

func TestSetBackfillCheckpoint_CompletedFlag(t *testing.T) {
	store := setupTestDB(t)

	now := time.Now().Truncate(time.Second)
	cp := &models.BackfillCheckpoint{
		Phase:      "metadata",
		LastSlot:   50000,
		TotalSlots: 50000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  true,
	}
	err := store.SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	got, err := store.GetBackfillCheckpoint("metadata", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.True(t, got.Completed)
}

func TestSetBackfillCheckpoint_WithTransaction(t *testing.T) {
	store := setupTestDB(t)

	now := time.Now().Truncate(time.Second)
	cp := &models.BackfillCheckpoint{
		Phase:      "metadata",
		LastSlot:   5000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}

	txn := store.Transaction()
	err := store.SetBackfillCheckpoint(cp, txn)
	require.NoError(t, err)
	err = txn.Commit()
	require.NoError(t, err)

	got, err := store.GetBackfillCheckpoint("metadata", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, uint64(5000), got.LastSlot)
}
