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

package lifecycle_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

// cancelAfterNErrChecks is a context.Context whose Err() returns nil for
// the first n calls, then context.Canceled for every call after that —
// used to simulate a cancellation landing partway through a single batch,
// deterministically, without racing a real timer against real deletes.
type cancelAfterNErrChecks struct {
	context.Context
	n     int64
	count atomic.Int64
}

func (c *cancelAfterNErrChecks) Err() error {
	if c.count.Add(1) > c.n {
		return context.Canceled
	}
	return nil
}

func newTestDB(t *testing.T) *database.Database {
	t.Helper()
	tmpDir := t.TempDir()
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		config.DefaultBlobPlugin,
		"data-dir",
		tmpDir,
	))
	require.NoError(t, plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		config.DefaultMetadataPlugin,
		"data-dir",
		tmpDir,
	))
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func testBlock(id uint64, hashByte byte) models.Block {
	return models.Block{
		ID:     id,
		Slot:   id * 10,
		Hash:   bytes.Repeat([]byte{hashByte}, 32),
		Cbor:   []byte{0x80},
		Number: id,
		Type:   1,
	}
}

// TestDeleteBlocksAfterRemovesOnlyBlocksAboveThreshold verifies that
// blocks at or below afterID survive and every block above it is deleted.
func TestDeleteBlocksAfterRemovesOnlyBlocksAboveThreshold(t *testing.T) {
	db := newTestDB(t)

	for id := uint64(1); id <= 5; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 2, 5, 0,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), blocksDeleted)

	for id := uint64(1); id <= 2; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(t, err, "block %d should survive truncation", id)
	}
	for id := uint64(3); id <= 5; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIsf(
			t, err, models.ErrBlockNotFound,
			"block %d should have been deleted", id,
		)
	}
}

// TestDeleteBlocksAfterNoopWhenTipAtOrBelowThreshold verifies that a
// threshold at or above the current tip deletes nothing.
func TestDeleteBlocksAfterNoopWhenTipAtOrBelowThreshold(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))

	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 5, 1, 0,
	)
	require.NoError(t, err)
	require.Zero(t, blocksDeleted)

	_, err = db.BlockByIndex(1, nil)
	require.NoError(t, err)
}

// TestDeleteBlocksAfterRespectsSmallBatchSize verifies that a batch size
// forcing multiple transactions still deletes exactly the same blocks.
func TestDeleteBlocksAfterRespectsSmallBatchSize(t *testing.T) {
	db := newTestDB(t)
	for id := uint64(1); id <= 10; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	// batchSize=1 forces multiple transactions; the end result must be the
	// same as a single large batch.
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(
		context.Background(), db, 3, 10, 1,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(7), blocksDeleted)

	for id := uint64(1); id <= 3; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoError(t, err)
	}
	for id := uint64(4); id <= 10; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
	}
}

// TestDeleteBlocksAfterNoticesCancellationMidBatch guards against
// comment-18's original bug: ctx was only checked once per batch (before
// entering that batch's transaction), so with the default 10,000-block
// batch size, a cancellation landing partway through a single large batch
// used to sit unnoticed until the entire batch finished deleting —
// potentially a long delay for a disaster-recovery truncate an operator
// just asked to cancel. This uses a single batch (batchSize larger than
// the whole block range) and a context that reports "not yet cancelled"
// for exactly the one check made before the batch starts, then
// "cancelled" for every check after that: with only a once-per-batch
// check, that single pre-batch check passes and the whole batch (all
// blocks) completes with no error; with a per-block check, the very
// first block inside the batch observes the cancellation, so the whole
// batch's transaction rolls back and no blocks are deleted at all.
func TestDeleteBlocksAfterNoticesCancellationMidBatch(t *testing.T) {
	db := newTestDB(t)
	const numBlocks = 20
	for id := uint64(1); id <= numBlocks; id++ {
		require.NoError(t, db.BlockCreate(testBlock(id, byte(id)), nil))
	}

	// batchSize=0 defaults to DefaultBlockDeleteBatchSize (10,000), well
	// above numBlocks, so DeleteBlocksAfter processes every block in a
	// single batch/transaction.
	ctx := &cancelAfterNErrChecks{Context: context.Background(), n: 1}
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(ctx, db, 0, numBlocks, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(
		t, blocksDeleted,
		"a rolled-back batch must not be counted as deleted",
	)

	for id := uint64(1); id <= numBlocks; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoErrorf(
			t, err,
			"block %d must survive: the whole batch's transaction must "+
				"roll back once cancellation is noticed mid-batch", id,
		)
	}
}

// TestDeleteBlocksAfterCanceledContext verifies that a pre-cancelled
// context is caught before any batch runs, returning context.Canceled.
func TestDeleteBlocksAfterCanceledContext(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.BlockCreate(testBlock(1, 0x01), nil))
	require.NoError(t, db.BlockCreate(testBlock(2, 0x02), nil))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	blocksDeleted, err := lifecycle.DeleteBlocksAfter(ctx, db, 0, 2, 0)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, blocksDeleted)
}
