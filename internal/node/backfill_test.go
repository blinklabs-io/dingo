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
	"bytes"
	"context"
	"io"
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

	// No checkpoint and no blocks => nothing to backfill
	needed, err := bf.NeedsBackfill()
	require.NoError(t, err)
	assert.False(t, needed)
}

func TestNeedsBackfill_NoCheckpointWithBlocks(t *testing.T) {
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())

	// Blocks present but no checkpoint => backfill is NOT needed.
	// Backfill is driven by the checkpoint alone. Normal-sync API
	// nodes have blocks but never run backfill, so the absence of
	// a checkpoint must mean "don't backfill".
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
		Phase:      BackfillPhase,
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
		Phase:      BackfillPhase,
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
		Phase:      BackfillPhase,
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
		Phase:      BackfillPhase,
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

func TestRun_IncompleteCheckpointAtZeroStartsAtSlotZero(t *testing.T) {
	db := newTestDB(t)

	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   0,
		TotalSlots: 1,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))

	for _, slot := range []uint64{0, 1} {
		hash := make([]byte, 32)
		hash[0] = byte(slot + 1)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot: slot,
			Hash: hash,
			Cbor: []byte{0x82, 0x01},
			Type: 1,
		}, nil))
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	bf := NewBackfill(db, nil, logger)

	require.NoError(t, bf.Run(context.Background()))
	got, err := db.Metadata().GetBackfillCheckpoint(BackfillPhase, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(1), got.LastSlot)
}

// TestRun_IncompleteCheckpointAtZeroVisitsSlotZero proves the iterator
// actually starts at slot 0 (rather than LastSlot+1 = 1). Asserting only
// the final cp.LastSlot is too weak: it's left at 1 in both the correct
// resume-from-zero case and the buggy skip-slot-zero case. We verify
// directly by feeding intentionally-unparseable CBOR for both blocks and
// observing the per-block "skipping unparseable block" log line for
// slot 0 — that log fires from inside the iteration loop and so only
// emits when the iterator visits that slot.
func TestRun_IncompleteCheckpointAtZeroVisitsSlotZero(t *testing.T) {
	db := newTestDB(t)

	now := time.Now()
	cp := &models.BackfillCheckpoint{
		Phase:      BackfillPhase,
		LastSlot:   0,
		TotalSlots: 1,
		StartedAt:  now,
		UpdatedAt:  now,
		Completed:  false,
	}
	require.NoError(t, db.Metadata().SetBackfillCheckpoint(cp, nil))

	for _, slot := range []uint64{0, 1} {
		hash := make([]byte, 32)
		hash[0] = byte(slot + 1)
		require.NoError(t, db.BlockCreate(models.Block{
			Slot: slot,
			Hash: hash,
			Cbor: []byte{0x82, 0x01},
			Type: 1,
		}, nil))
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	bf := NewBackfill(db, nil, logger)

	require.NoError(t, bf.Run(context.Background()))

	// One unparseable-block warning per visited slot proves the
	// iterator's slot boundary directly. Both must appear; missing
	// slot 0 means resume started at 1 and silently skipped the
	// first block.
	output := logs.String()
	assert.Contains(t, output, `"msg":"skipping unparseable block"`)
	assert.Contains(t, output, `"slot":0`,
		"iterator must visit slot 0 when resuming from a checkpoint with LastSlot=0")
	assert.Contains(t, output, `"slot":1`,
		"iterator must also visit slot 1")
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
