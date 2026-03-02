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

package node

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB creates an in-memory database for tests.
func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	db, err := database.New(&database.Config{
		DataDir: "", // in-memory
		Logger:  logger,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close() //nolint:errcheck
	})
	return db
}

func TestNeedsBackfill_NoCheckpoint(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// No checkpoint exists => NeedsBackfill should return false
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.False(t, needed)
}

func TestNeedsBackfill_IncompleteCheckpoint(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Create an incomplete checkpoint
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      backfillPhase,
		LastSlot:   5000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Incomplete checkpoint => NeedsBackfill should return true
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.True(t, needed)
}

func TestNeedsBackfill_CompletedCheckpoint(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Create a completed checkpoint
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      backfillPhase,
		LastSlot:   100000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  true,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Completed checkpoint => NeedsBackfill should return false
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.False(t, needed)
}

func TestRun_EmptyBlobStore(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// No blocks in blob store => Run should return nil immediately
	err := bf.Run(context.Background())
	require.NoError(t, err)

	// No checkpoint should have been created
	needed, needsErr := bf.NeedsBackfill()
	require.NoError(t, needsErr)
	assert.False(t, needed)
}

func TestRun_AlreadyCompleted(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Pre-create a completed checkpoint
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      backfillPhase,
		LastSlot:   100000,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  true,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	// Run should return immediately without error
	err = bf.Run(context.Background())
	require.NoError(t, err)
}

func TestRun_CancelledContext_EmptyBlobStore(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Create an incomplete checkpoint so Run will attempt work
	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      backfillPhase,
		LastSlot:   0,
		TotalSlots: 100000,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	err := db.Metadata().SetBackfillCheckpoint(cp, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With an empty blob store (tipSlot=0) Run returns nil early
	// before reaching the iteration loop, even with a cancelled
	// context.
	err = bf.Run(ctx)
	require.NoError(t, err)
}

func TestRun_CancelledContext_WithBlocks(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Insert a block so tipSlot > 0 and Run reaches the
	// iteration loop.
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = byte(i)
	}
	err := db.BlockCreate(models.Block{
		Slot: 100,
		Hash: hash,
		Cbor: []byte{0x82, 0x01},
		Type: 1,
	}, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// With blocks present, Run enters the iteration loop and
	// detects the cancelled context, returning an error.
	err = bf.Run(ctx)
	require.Error(t, err, "Run should return error on cancelled context with blocks")
	assert.ErrorIs(t, err, context.Canceled)
}
