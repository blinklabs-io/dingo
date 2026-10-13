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

package database

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// overBudgetRetainedBlocks exceeds the entry budget of one badger transaction
// on the test blob store (see testBadgerMaxBatchCount), the way a synced
// network's retained history exceeds production's ~209,715.
const overBudgetRetainedBlocks = overBudgetBlobDeletes

func overBudgetBlockHash(i int) []byte {
	hash := make([]byte, 32)
	//nolint:gosec // loop counter, always in range
	binary.BigEndian.PutUint32(hash[:4], uint32(i))
	return hash
}

// seedLegacyRetainedBlocks writes count block blobs straight into the blob
// store, in bounded transactions and without touching the inventory, and
// then removes the inventory record: the on-disk shape of a database written
// before the inventory existed.
func seedLegacyRetainedBlocks(t *testing.T, db *Database, count int) {
	t.Helper()
	const batch = 2_000
	for start := 0; start < count; start += batch {
		end := min(start+batch, count)
		txn := NewBlobOnlyTxn(db, true)
		store := txn.BlobStore()
		require.NotNil(t, store)
		for i := start; i < end; i++ {
			require.NoError(t, store.SetBlock(
				txn.Blob(),
				//nolint:gosec // loop counter, always in range
				uint64(i+1),
				overBudgetBlockHash(i),
				[]byte{0x80},
				//nolint:gosec // loop counter, always in range
				uint64(i+1),
				1,
				//nolint:gosec // loop counter, always in range
				uint64(i+1),
				nil,
			))
		}
		require.NoError(t, txn.Commit())
	}
	txn := NewBlobOnlyTxn(db, true)
	require.NoError(t, txn.BlobStore().Delete(txn.Blob(), blockInventoryKey))
	require.NoError(t, txn.Commit())
}

// TestBlockInventoryInitializesOverBudgetLegacyStore opens a legacy database
// that retains more blocks than one badger transaction can index. The
// inventory must still be built, so the database opens.
func TestBlockInventoryInitializesOverBudgetLegacyStore(t *testing.T) {
	t.Parallel()
	db := newOnDiskTestDB(t)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	seedLegacyRetainedBlocks(t, db, overBudgetRetainedBlocks)

	require.NoError(t, db.EnsureBlockInventory())

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(overBudgetRetainedBlocks), count)
	require.Equal(t, uint64(1), oldest)

	// The index behind the record is complete: removing the oldest block
	// finds its successor through the index.
	txn := db.BlockBlobTxn()
	t.Cleanup(txn.Release)
	require.NoError(t, db.tombstoneBlockTxn(txn, 1, overBudgetBlockHash(0)))
	require.NoError(t, txn.Commit())
	count, oldest, err = db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(overBudgetRetainedBlocks-1), count)
	require.Equal(t, uint64(2), oldest)
}

// TestBlockInventoryClearsOverBudgetStaleIndex starts from index entries left
// behind without an inventory record, more than one badger transaction can
// delete. Initialization must clear them and count only retained blocks.
func TestBlockInventoryClearsOverBudgetStaleIndex(t *testing.T) {
	t.Parallel()
	db := newOnDiskTestDB(t)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	const batch = 2_000
	for start := 0; start < overBudgetRetainedBlocks; start += batch {
		end := min(start+batch, overBudgetRetainedBlocks)
		txn := NewBlobOnlyTxn(db, true)
		for i := start; i < end; i++ {
			//nolint:gosec // loop counter, always in range
			key, err := blockInventoryIndexKey(uint64(i+1), overBudgetBlockHash(i))
			require.NoError(t, err)
			require.NoError(t, txn.BlobStore().Set(txn.Blob(), key, nil))
		}
		require.NoError(t, txn.Commit())
	}
	insertTestBlock(t, db, 500_000, randomHash(t), []byte{0x80})
	txn := NewBlobOnlyTxn(db, true)
	require.NoError(t, txn.BlobStore().Delete(txn.Blob(), blockInventoryKey))
	require.NoError(t, txn.Commit())

	require.NoError(t, db.EnsureBlockInventory())

	count, oldest, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)
	require.Equal(t, uint64(500_000), oldest)
}
