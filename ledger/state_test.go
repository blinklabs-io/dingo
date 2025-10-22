// Copyright 2025 Blink Labs Software
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

package ledger

import (
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestGetBlock_Success tests that GetBlock returns a valid block when found
func TestGetBlock_Success(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create test block
	testBlock := models.Block{
		Hash:     []byte{0x01, 0x02, 0x03, 0x04},
		Slot:     12345,
		Number:   100,
		PrevHash: []byte{0x00, 0x00, 0x00, 0x00},
		Cbor:     []byte{0x82, 0x00, 0x81, 0x00}, // minimal valid CBOR
		Type:     5,                               // Byron
	}

	// Add block to chain
	primaryChain := cm.PrimaryChain()
	if err := primaryChain.AddBlock(testBlock, nil); err != nil {
		t.Fatalf("failed to add test block: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: primaryChain,
		db:    db,
	}

	// Test GetBlock with valid point
	point := ocommon.NewPoint(testBlock.Slot, testBlock.Hash)
	result, err := ls.GetBlock(point)
	if err != nil {
		t.Fatalf("GetBlock returned unexpected error: %v", err)
	}

	// Verify returned block matches
	if result.Slot != testBlock.Slot {
		t.Errorf("Expected slot %d, got %d", testBlock.Slot, result.Slot)
	}
	if string(result.Hash) != string(testBlock.Hash) {
		t.Errorf("Expected hash %x, got %x", testBlock.Hash, result.Hash)
	}
	if result.Number != testBlock.Number {
		t.Errorf("Expected block number %d, got %d", testBlock.Number, result.Number)
	}
}

// TestGetBlock_NotFound tests that GetBlock returns an error when block not found
func TestGetBlock_NotFound(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		db:    db,
	}

	// Test GetBlock with non-existent point
	point := ocommon.NewPoint(99999, []byte{0xff, 0xff, 0xff, 0xff})
	result, err := ls.GetBlock(point)
	if err == nil {
		t.Fatal("Expected error for non-existent block, got nil")
	}

	// Verify zero value is returned on error
	if result.Slot != 0 {
		t.Errorf("Expected zero slot on error, got %d", result.Slot)
	}
	if len(result.Hash) != 0 {
		t.Errorf("Expected empty hash on error, got %x", result.Hash)
	}
}

// TestGetBlock_ReturnsValueNotPointer tests that GetBlock returns value type, not pointer
func TestGetBlock_ReturnsValueNotPointer(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create test block
	testBlock := models.Block{
		Hash:     []byte{0xaa, 0xbb, 0xcc, 0xdd},
		Slot:     54321,
		Number:   200,
		PrevHash: []byte{0x00, 0x00, 0x00, 0x00},
		Cbor:     []byte{0x82, 0x00, 0x81, 0x00},
		Type:     5,
	}

	// Add block to chain
	primaryChain := cm.PrimaryChain()
	if err := primaryChain.AddBlock(testBlock, nil); err != nil {
		t.Fatalf("failed to add test block: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: primaryChain,
		db:    db,
	}

	// Get block twice
	point := ocommon.NewPoint(testBlock.Slot, testBlock.Hash)
	result1, err1 := ls.GetBlock(point)
	if err1 != nil {
		t.Fatalf("First GetBlock call failed: %v", err1)
	}

	result2, err2 := ls.GetBlock(point)
	if err2 != nil {
		t.Fatalf("Second GetBlock call failed: %v", err2)
	}

	// Modify first result
	result1.Number = 999

	// Verify second result is unaffected (proves value semantics)
	if result2.Number != testBlock.Number {
		t.Errorf("Expected result2.Number to be %d (unmodified), got %d", testBlock.Number, result2.Number)
	}
}

// TestGetBlock_EmptyHash tests GetBlock with empty hash
func TestGetBlock_EmptyHash(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		db:    db,
	}

	// Test GetBlock with empty hash
	point := ocommon.NewPoint(12345, []byte{})
	_, err = ls.GetBlock(point)
	if err == nil {
		t.Error("Expected error for empty hash, got nil")
	}
}

// TestGetBlock_ZeroSlot tests GetBlock with slot 0 (genesis)
func TestGetBlock_ZeroSlot(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create genesis block
	genesisBlock := models.Block{
		Hash:     []byte{0x00, 0x11, 0x22, 0x33},
		Slot:     0,
		Number:   0,
		PrevHash: []byte{0x00, 0x00, 0x00, 0x00},
		Cbor:     []byte{0x82, 0x00, 0x81, 0x00},
		Type:     5,
	}

	// Add block to chain
	primaryChain := cm.PrimaryChain()
	if err := primaryChain.AddBlock(genesisBlock, nil); err != nil {
		t.Fatalf("failed to add genesis block: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: primaryChain,
		db:    db,
	}

	// Test GetBlock with slot 0
	point := ocommon.NewPoint(0, genesisBlock.Hash)
	result, err := ls.GetBlock(point)
	if err != nil {
		t.Fatalf("GetBlock failed for genesis block: %v", err)
	}

	if result.Slot != 0 {
		t.Errorf("Expected slot 0 for genesis block, got %d", result.Slot)
	}
}

// TestGetBlock_MultipleBlocks tests retrieving multiple different blocks
func TestGetBlock_MultipleBlocks(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	primaryChain := cm.PrimaryChain()

	// Create multiple test blocks
	testBlocks := []models.Block{
		{
			Hash:     []byte{0x01, 0x00, 0x00, 0x00},
			Slot:     100,
			Number:   1,
			PrevHash: []byte{0x00, 0x00, 0x00, 0x00},
			Cbor:     []byte{0x82, 0x00, 0x81, 0x00},
			Type:     5,
		},
		{
			Hash:     []byte{0x02, 0x00, 0x00, 0x00},
			Slot:     200,
			Number:   2,
			PrevHash: []byte{0x01, 0x00, 0x00, 0x00},
			Cbor:     []byte{0x82, 0x00, 0x81, 0x00},
			Type:     5,
		},
		{
			Hash:     []byte{0x03, 0x00, 0x00, 0x00},
			Slot:     300,
			Number:   3,
			PrevHash: []byte{0x02, 0x00, 0x00, 0x00},
			Cbor:     []byte{0x82, 0x00, 0x81, 0x00},
			Type:     5,
		},
	}

	// Add blocks to chain
	for _, block := range testBlocks {
		if err := primaryChain.AddBlock(block, nil); err != nil {
			t.Fatalf("failed to add test block: %v", err)
		}
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: primaryChain,
		db:    db,
	}

	// Test retrieving each block
	for i, expectedBlock := range testBlocks {
		point := ocommon.NewPoint(expectedBlock.Slot, expectedBlock.Hash)
		result, err := ls.GetBlock(point)
		if err != nil {
			t.Errorf("GetBlock failed for block %d: %v", i, err)
			continue
		}

		if result.Slot != expectedBlock.Slot {
			t.Errorf("Block %d: expected slot %d, got %d", i, expectedBlock.Slot, result.Slot)
		}
		if string(result.Hash) != string(expectedBlock.Hash) {
			t.Errorf("Block %d: expected hash %x, got %x", i, expectedBlock.Hash, result.Hash)
		}
		if result.Number != expectedBlock.Number {
			t.Errorf("Block %d: expected number %d, got %d", i, expectedBlock.Number, result.Number)
		}
	}
}

// TestGetBlock_ErrorPropagation tests that errors from chain are properly propagated
func TestGetBlock_ErrorPropagation(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		db:    db,
	}

	// Test with various invalid points
	testCases := []struct {
		name string
		point ocommon.Point
	}{
		{
			name: "non-existent slot",
			point: ocommon.NewPoint(999999999, []byte{0xde, 0xad, 0xbe, 0xef}),
		},
		{
			name: "wrong hash for slot",
			point: ocommon.NewPoint(100, []byte{0x00, 0x00, 0x00, 0x00}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := ls.GetBlock(tc.point)
			if err == nil {
				t.Error("Expected error, got nil")
			}
			// Verify zero value is returned
			if result.ID != 0 || len(result.Hash) != 0 {
				t.Error("Expected zero value on error")
			}
		})
	}
}

// TestGetBlock_ConsistentReturns tests that repeated calls return consistent results
func TestGetBlock_ConsistentReturns(t *testing.T) {
	// Setup test database
	db, err := database.New(&database.Config{
		BlobCacheSize: 1 << 20,
		DataDir:       "",
	})
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	defer db.Close()

	// Create chain manager
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Create test block
	testBlock := models.Block{
		Hash:     []byte{0xde, 0xad, 0xbe, 0xef},
		Slot:     777,
		Number:   77,
		PrevHash: []byte{0x00, 0x00, 0x00, 0x00},
		Cbor:     []byte{0x82, 0x00, 0x81, 0x00},
		Type:     5,
	}

	primaryChain := cm.PrimaryChain()
	if err := primaryChain.AddBlock(testBlock, nil); err != nil {
		t.Fatalf("failed to add test block: %v", err)
	}

	// Create LedgerState
	ls := &LedgerState{
		chain: primaryChain,
		db:    db,
	}

	point := ocommon.NewPoint(testBlock.Slot, testBlock.Hash)

	// Call GetBlock multiple times
	for i := 0; i < 5; i++ {
		result, err := ls.GetBlock(point)
		if err != nil {
			t.Fatalf("GetBlock call %d failed: %v", i, err)
		}

		// Verify consistency
		if result.Slot != testBlock.Slot {
			t.Errorf("Call %d: expected slot %d, got %d", i, testBlock.Slot, result.Slot)
		}
		if string(result.Hash) != string(testBlock.Hash) {
			t.Errorf("Call %d: expected hash %x, got %x", i, testBlock.Hash, result.Hash)
		}
		if result.Number != testBlock.Number {
			t.Errorf("Call %d: expected number %d, got %d", i, testBlock.Number, result.Number)
		}
	}
}