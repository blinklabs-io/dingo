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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/stretchr/testify/require"
)

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

	require.NoError(t, lifecycle.DeleteBlocksAfter(
		context.Background(), db, 2, 5, 0,
	))

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

	require.NoError(t, lifecycle.DeleteBlocksAfter(
		context.Background(), db, 5, 1, 0,
	))

	_, err := db.BlockByIndex(1, nil)
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
	require.NoError(t, lifecycle.DeleteBlocksAfter(
		context.Background(), db, 3, 10, 1,
	))

	for id := uint64(1); id <= 3; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.NoError(t, err)
	}
	for id := uint64(4); id <= 10; id++ {
		_, err := db.BlockByIndex(id, nil)
		require.ErrorIs(t, err, models.ErrBlockNotFound)
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
	err := lifecycle.DeleteBlocksAfter(ctx, db, 0, 2, 0)
	require.ErrorIs(t, err, context.Canceled)
}
