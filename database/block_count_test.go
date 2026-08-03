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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCountBlocksAndOldestSlot_EmptyDatabase verifies that a database
// with no blocks reports a zero count and a zero oldest slot.
func TestCountBlocksAndOldestSlot_EmptyDatabase(t *testing.T) {
	db := newTestDB(t)

	count, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Zero(t, count)
	require.Zero(t, oldestSlot)
}

// TestCountBlocksAndOldestSlot_CountsAndFindsOldest verifies that blocks
// inserted out of slot order still report the correct count and oldest slot.
func TestCountBlocksAndOldestSlot_CountsAndFindsOldest(t *testing.T) {
	db := newTestDB(t)

	// Inserted out of slot order on purpose: the oldest slot must be
	// found by content, not by insertion order.
	insertTestBlock(t, db, 300, randomHash(t), []byte{0x80})
	insertTestBlock(t, db, 100, randomHash(t), []byte{0x80})
	insertTestBlock(t, db, 200, randomHash(t), []byte{0x80})

	count, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), count)
	require.Equal(t, uint64(100), oldestSlot)
}

// TestCountBlocksAndOldestSlot_ExcludesTombstonedBlocks verifies that a
// history-expiry-pruned block (its bp key kept alive with a tombstone
// marker so bi/bh lookups still resolve — see TombstoneBlock) is excluded
// from both the count and the oldest-slot search: its content isn't
// actually retained, so counting it would overstate how much history is
// available and understate how far back retained history actually goes.
func TestCountBlocksAndOldestSlot_ExcludesTombstonedBlocks(t *testing.T) {
	db := newTestDB(t)

	oldestHash := randomHash(t)
	insertTestBlock(t, db, 100, oldestHash, []byte{0x80})
	insertTestBlock(t, db, 200, randomHash(t), []byte{0x80})

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *Txn) error {
		return db.Blob().TombstoneBlock(txn.Blob(), 100, oldestHash)
	}))

	count, oldestSlot, err := db.CountBlocksAndOldestSlot(nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count, "the tombstoned block must not be counted")
	require.Equal(
		t, uint64(200), oldestSlot,
		"the tombstoned block's slot must not be reported as the oldest",
	)
}
