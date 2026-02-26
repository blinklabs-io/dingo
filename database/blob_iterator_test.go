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
	"crypto/rand"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDB creates an in-memory Database instance for testing.
func newTestDB(t *testing.T) *Database {
	t.Helper()
	config := &Config{
		DataDir: "", // In-memory
	}
	db, err := New(config)
	require.NoError(t, err, "failed to create test database")
	t.Cleanup(func() {
		require.NoError(t, db.Close(), "failed to close test database")
	})
	return db
}

// randomHash generates a random 32-byte hash for testing.
func randomHash(t *testing.T) []byte {
	t.Helper()
	hash := make([]byte, 32)
	_, err := rand.Read(hash)
	require.NoError(t, err)
	return hash
}

// insertTestBlock inserts a block into the blob store for testing.
func insertTestBlock(
	t *testing.T,
	db *Database,
	slot uint64,
	hash []byte,
	cbor []byte,
) {
	t.Helper()
	block := models.Block{
		Slot: slot,
		Hash: hash,
		Cbor: cbor,
		Type: 1,
	}
	err := db.BlockCreate(block, nil)
	require.NoError(t, err, "failed to insert test block at slot %d", slot)
}

func TestBlobBlockIterator_EmptyDatabase(t *testing.T) {
	db := newTestDB(t)

	iter := db.BlocksFromSlot(0)
	defer iter.Close()

	result, err := iter.NextRaw()
	require.NoError(t, err)
	assert.Nil(t, result, "result should be nil for empty database")
}

// collectIterSlots drains a BlobBlockIterator via NextRaw and returns
// all yielded slots. Each result's Cbor is also checked for non-nil.
func collectIterSlots(
	t *testing.T, iter *BlobBlockIterator,
) []uint64 {
	t.Helper()
	var slots []uint64
	for {
		result, err := iter.NextRaw()
		require.NoError(t, err)
		if result == nil {
			break
		}
		assert.NotNil(
			t, result.Cbor,
			"cbor should not be nil for slot %d", result.Slot,
		)
		slots = append(slots, result.Slot)
	}
	return slots
}

func TestBlobBlockIterator_SlotRanges(t *testing.T) {
	seedSlots := []uint64{10, 20, 30, 40, 50}

	tests := []struct {
		name     string
		start    uint64
		end      *uint64 // nil = BlocksFromSlot, non-nil = BlocksInRange
		expected []uint64
	}{
		{
			name:     "BlocksFromSlot/all",
			start:    0,
			expected: []uint64{10, 20, 30, 40, 50},
		},
		{
			name:     "BlocksFromSlot/mid_range",
			start:    25,
			expected: []uint64{30, 40, 50},
		},
		{
			name:     "BlocksInRange/inclusive",
			start:    20,
			end:      ptr(uint64(40)),
			expected: []uint64{20, 30, 40},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			for _, slot := range seedSlots {
				insertTestBlock(
					t, db, slot, randomHash(t),
					[]byte{0x82, 0x01},
				)
			}

			var iter *BlobBlockIterator
			if tc.end != nil {
				iter = db.BlocksInRange(tc.start, *tc.end)
			} else {
				iter = db.BlocksFromSlot(tc.start)
			}
			defer iter.Close()

			collected := collectIterSlots(t, iter)
			assert.Equal(t, tc.expected, collected)
		})
	}
}

// ptr returns a pointer to the given value.
func ptr[T any](v T) *T { return &v }

func TestBlobBlockIterator_Progress(t *testing.T) {
	db := newTestDB(t)

	// Insert blocks
	slots := []uint64{100, 200, 300}
	for _, slot := range slots {
		insertTestBlock(t, db, slot, randomHash(t), []byte{0x82, 0x01})
	}

	// Test with endSlot
	iter := db.BlocksInRange(0, 500)
	defer iter.Close()

	// Before any iteration, current should be 0
	current, end := iter.Progress()
	assert.Equal(t, uint64(0), current, "initial current slot should be 0")
	assert.Equal(t, uint64(500), end, "end slot should match constructor")

	// After iterating one block
	result, err := iter.NextRaw()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(100), result.Slot)

	current, end = iter.Progress()
	assert.Equal(t, uint64(100), current, "current should track last yielded slot")
	assert.Equal(t, uint64(500), end, "end should remain unchanged")
}

func TestBlobBlockIterator_ProgressNoEndSlot(t *testing.T) {
	db := newTestDB(t)

	iter := db.BlocksFromSlot(0)
	defer iter.Close()

	_, end := iter.Progress()
	assert.Equal(t, uint64(0), end, "end should be 0 when not specified")
}

func TestBlobBlockIterator_CloseMultipleTimes(t *testing.T) {
	db := newTestDB(t)

	iter := db.BlocksFromSlot(0)

	// Close multiple times — should not panic
	iter.Close()
	iter.Close()
	iter.Close()

	// NextRaw after close should return nil
	result, err := iter.NextRaw()
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBlobBlockIterator_CloseWhileIterating(t *testing.T) {
	db := newTestDB(t)

	// Insert blocks
	for _, slot := range []uint64{10, 20, 30, 40, 50} {
		insertTestBlock(t, db, slot, randomHash(t), []byte{0x82, 0x01})
	}

	iter := db.BlocksFromSlot(0)

	// Read one block
	result, err := iter.NextRaw()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(10), result.Slot)

	// Close mid-iteration
	iter.Close()

	// Subsequent reads should return nil
	result, err = iter.NextRaw()
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBlobBlockIterator_CborContent(t *testing.T) {
	db := newTestDB(t)

	expectedCbor := []byte{0x83, 0x01, 0x02, 0x03}
	hash := randomHash(t)
	insertTestBlock(t, db, 42, hash, expectedCbor)

	iter := db.BlocksFromSlot(0)
	defer iter.Close()

	result, err := iter.NextRaw()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(42), result.Slot)
	assert.Equal(t, hash, result.Hash)
	assert.Equal(t, expectedCbor, result.Cbor)
}

func TestBlobBlockIterator_EmptyRange(t *testing.T) {
	db := newTestDB(t)

	// Insert blocks outside the requested range
	insertTestBlock(t, db, 10, randomHash(t), []byte{0x82, 0x01})
	insertTestBlock(t, db, 50, randomHash(t), []byte{0x82, 0x01})

	// Request range with no blocks
	iter := db.BlocksInRange(20, 40)
	defer iter.Close()

	result, err := iter.NextRaw()
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBlobBlockIterator_SingleBlock(t *testing.T) {
	db := newTestDB(t)

	hash := randomHash(t)
	insertTestBlock(t, db, 100, hash, []byte{0x82, 0x01})

	iter := db.BlocksFromSlot(100)
	defer iter.Close()

	// First call should return the block
	result, err := iter.NextRaw()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(100), result.Slot)
	assert.Equal(t, hash, result.Hash)
	assert.NotNil(t, result.Cbor)

	// Second call should indicate end
	result, err = iter.NextRaw()
	require.NoError(t, err)
	assert.Nil(t, result)
}

func TestBlobBlockIterator_MultiBatchResume(t *testing.T) {
	db := newTestDB(t)

	// Insert more blocks than blobIteratorBatchSize (1000) to
	// exercise the resume-key carry-over between batches.
	const numBlocks = 1050
	expectedSlots := make([]uint64, numBlocks)
	for i := range numBlocks {
		slot := uint64(i + 1)
		expectedSlots[i] = slot
		insertTestBlock(
			t, db, slot, randomHash(t), []byte{0x82, byte(i % 256)},
		)
	}

	iter := db.BlocksFromSlot(0)
	defer iter.Close()

	var collected []uint64
	for {
		result, err := iter.NextRaw()
		require.NoError(t, err)
		if result == nil {
			break
		}
		collected = append(collected, result.Slot)
	}

	require.Len(
		t, collected, numBlocks,
		"should iterate all blocks across multiple batches",
	)
	assert.Equal(
		t, expectedSlots, collected,
		"blocks should be in ascending slot order across batches",
	)
}

func TestBlobBlockIterator_MaxEndSlot(t *testing.T) {
	db := newTestDB(t)

	hash := randomHash(t)
	insertTestBlock(t, db, 100, hash, []byte{0x82, 0x01})

	// Using max uint64 as endSlot must not overflow
	iter := db.BlocksInRange(0, ^uint64(0))
	defer iter.Close()

	result, err := iter.NextRaw()
	require.NoError(t, err)
	require.NotNil(t, result, "block should be found with max endSlot")
	assert.Equal(t, uint64(100), result.Slot)

	result, err = iter.NextRaw()
	require.NoError(t, err)
	assert.Nil(t, result)
}
