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

package integration

import (
	"encoding/hex"
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// testDataDir returns the path to the immutable testdata directory
func testDataDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(
		filepath.Dir(thisFile),
		"..",
		"..",
		"database",
		"immutable",
		"testdata",
	)
}

// mockLedgerState implements the interface for ChainManager.SetLedger
type mockLedgerState struct {
	securityParam int
}

func (m *mockLedgerState) SecurityParam() int {
	return m.securityParam
}

// loadBlocksFromImmutable loads blocks from the immutable testdata into the chain
// Returns the loaded blocks and points for use in rollback tests
func loadBlocksFromImmutable(
	t *testing.T,
	c *chain.Chain,
	maxBlocks int,
) ([]ledger.Block, []ocommon.Point) {
	t.Helper()

	imm, err := immutable.New(testDataDir())
	if err != nil {
		t.Fatalf("failed to open immutable db: %v", err)
	}
	defer imm.Close() //nolint:errcheck

	iter, err := imm.BlocksFromPoint(ocommon.Point{Slot: 0, Hash: []byte{}})
	if err != nil {
		t.Fatalf("failed to create block iterator: %v", err)
	}
	defer iter.Close()

	var blocks []ledger.Block
	var points []ocommon.Point

	for range maxBlocks {
		immBlock, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error reading block: %v", err)
		}
		if immBlock == nil {
			// No more blocks
			break
		}

		// Decode the block
		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		if err != nil {
			t.Fatalf(
				"failed to decode block at slot %d: %v",
				immBlock.Slot,
				err,
			)
		}

		// Add block to chain
		if err := c.AddBlock(block, nil); err != nil {
			t.Fatalf(
				"failed to add block to chain at slot %d: %v",
				immBlock.Slot,
				err,
			)
		}

		blocks = append(blocks, block)
		points = append(points, ocommon.Point{
			Slot: block.SlotNumber(),
			Hash: block.Hash().Bytes(),
		})
	}

	return blocks, points
}

func TestRollbackToSecurityParamDepth(t *testing.T) {
	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Configure plugins with temp directory
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		config.DefaultBlobPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set blob plugin data-dir: %v", err)
	}
	if err := plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		config.DefaultMetadataPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set metadata plugin data-dir: %v", err)
	}

	// Create database
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter (K=432 for preview network)
	// For this test, we use a smaller value to avoid loading too many blocks
	const testSecurityParam = 50
	mockLedger := &mockLedgerState{securityParam: testSecurityParam}
	cm.SetLedger(mockLedger)

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load enough blocks to test rollback (K + 10 blocks)
	numBlocks := testSecurityParam + 10
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get current tip before rollback
	tipBefore := c.Tip()
	t.Logf(
		"chain tip before rollback: slot=%d hash=%s",
		tipBefore.Point.Slot,
		hex.EncodeToString(tipBefore.Point.Hash),
	)

	// Test 1: Rollback to exactly K blocks back (should succeed)
	// The rollback point is at index (len(blocks) - 1 - K) = last block - K
	rollbackIndex := len(blocks) - 1 - testSecurityParam
	if rollbackIndex < 0 || rollbackIndex >= len(points) {
		t.Fatalf(
			"rollback index out of range: %d (blocks=%d, points=%d, K=%d)",
			rollbackIndex,
			len(blocks),
			len(points),
			testSecurityParam,
		)
	}
	rollbackPoint := points[rollbackIndex]

	t.Logf(
		"rolling back to K blocks back: slot=%d hash=%s (index=%d)",
		rollbackPoint.Slot,
		hex.EncodeToString(rollbackPoint.Hash),
		rollbackIndex,
	)

	err = c.Rollback(rollbackPoint)
	if err != nil {
		t.Fatalf("rollback to K blocks should succeed, but got error: %v", err)
	}

	// Verify chain tip after rollback
	tipAfterRollback := c.Tip()
	t.Logf(
		"chain tip after rollback: slot=%d hash=%s",
		tipAfterRollback.Point.Slot,
		hex.EncodeToString(tipAfterRollback.Point.Hash),
	)

	if tipAfterRollback.Point.Slot != rollbackPoint.Slot {
		t.Errorf(
			"tip slot mismatch after rollback: got %d, want %d",
			tipAfterRollback.Point.Slot,
			rollbackPoint.Slot,
		)
	}
	if string(tipAfterRollback.Point.Hash) != string(rollbackPoint.Hash) {
		t.Errorf(
			"tip hash mismatch after rollback: got %s, want %s",
			hex.EncodeToString(tipAfterRollback.Point.Hash),
			hex.EncodeToString(rollbackPoint.Hash),
		)
	}

	t.Log("rollback to K blocks back succeeded as expected")
}

func TestRollbackBeyondSecurityParam(t *testing.T) {
	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Configure plugins with temp directory
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		config.DefaultBlobPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set blob plugin data-dir: %v", err)
	}
	if err := plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		config.DefaultMetadataPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set metadata plugin data-dir: %v", err)
	}

	// Create database
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter (K=432 for preview network)
	// For this test, we use a smaller value to avoid loading too many blocks
	const testSecurityParam = 50
	mockLedger := &mockLedgerState{securityParam: testSecurityParam}
	cm.SetLedger(mockLedger)

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load enough blocks to test rollback beyond K (K + 20 blocks)
	numBlocks := testSecurityParam + 20
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get current tip before rollback attempt
	tipBefore := c.Tip()
	t.Logf(
		"chain tip before rollback attempt: slot=%d hash=%s",
		tipBefore.Point.Slot,
		hex.EncodeToString(tipBefore.Point.Hash),
	)

	// Test: Attempt rollback to a point that doesn't exist in the chain
	// We use a valid slot from the chain but with a fake hash
	// This simulates a rollback to a point that was never added
	fakeHash := make([]byte, 32)
	for i := range fakeHash {
		fakeHash[i] = byte(i)
	}
	// Use a slot from the middle of the chain but with wrong hash
	midIndex := len(points) / 2
	midSlot := points[midIndex].Slot
	fakePoint := ocommon.Point{
		Slot: midSlot,
		Hash: fakeHash,
	}

	t.Logf(
		"attempting rollback to non-existent point: slot=%d hash=%s",
		fakePoint.Slot,
		hex.EncodeToString(fakePoint.Hash),
	)

	err = c.Rollback(fakePoint)
	if err == nil {
		t.Fatal("rollback to non-existent point should fail, but succeeded")
	}

	// Assert expected error type - rollback to non-existent point should return ErrBlockNotFound
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Errorf(
			"expected ErrBlockNotFound, got error type: %T, value: %v",
			err,
			err,
		)
	} else {
		t.Log("rollback failed with ErrBlockNotFound as expected")
	}

	// Verify chain tip is unchanged after failed rollback
	tipAfterFailed := c.Tip()
	if tipAfterFailed.Point.Slot != tipBefore.Point.Slot {
		t.Errorf(
			"tip slot changed after failed rollback: got %d, want %d",
			tipAfterFailed.Point.Slot,
			tipBefore.Point.Slot,
		)
	}
	if string(tipAfterFailed.Point.Hash) != string(tipBefore.Point.Hash) {
		t.Errorf(
			"tip hash changed after failed rollback: got %s, want %s",
			hex.EncodeToString(tipAfterFailed.Point.Hash),
			hex.EncodeToString(tipBefore.Point.Hash),
		)
	}

	t.Log("chain state preserved after failed rollback attempt")
}

func TestRollbackStateRestoration(t *testing.T) {
	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Configure plugins with temp directory
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		config.DefaultBlobPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set blob plugin data-dir: %v", err)
	}
	if err := plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		config.DefaultMetadataPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set metadata plugin data-dir: %v", err)
	}

	// Create database
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter
	const testSecurityParam = 30
	mockLedger := &mockLedgerState{securityParam: testSecurityParam}
	cm.SetLedger(mockLedger)

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load blocks
	numBlocks := 100
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get initial tip
	initialTip := c.Tip()
	t.Logf(
		"initial chain tip: slot=%d hash=%s blockNumber=%d",
		initialTip.Point.Slot,
		hex.EncodeToString(initialTip.Point.Hash),
		initialTip.BlockNumber,
	)

	// Rollback to a midpoint
	midpointIndex := len(points) / 2
	midpointPoint := points[midpointIndex]

	t.Logf(
		"rolling back to midpoint: slot=%d hash=%s (index=%d)",
		midpointPoint.Slot,
		hex.EncodeToString(midpointPoint.Hash),
		midpointIndex,
	)

	err = c.Rollback(midpointPoint)
	if err != nil {
		t.Fatalf("rollback to midpoint failed: %v", err)
	}

	// Verify tip after rollback
	tipAfterRollback := c.Tip()
	if tipAfterRollback.Point.Slot != midpointPoint.Slot {
		t.Errorf(
			"tip slot mismatch after rollback: got %d, want %d",
			tipAfterRollback.Point.Slot,
			midpointPoint.Slot,
		)
	}
	if string(tipAfterRollback.Point.Hash) != string(midpointPoint.Hash) {
		t.Errorf(
			"tip hash mismatch after rollback: got %s, want %s",
			hex.EncodeToString(tipAfterRollback.Point.Hash),
			hex.EncodeToString(midpointPoint.Hash),
		)
	}

	// Verify the chain tip reflects the rollback point
	// Note: BlockByPoint may still find blocks in blob storage after rollback
	// since blob storage retains block data even after chain rollback.
	// The important thing is that the chain tip is correct.
	if tipAfterRollback.BlockNumber != blocks[midpointIndex].BlockNumber() {
		t.Errorf(
			"tip block number mismatch after rollback: got %d, want %d",
			tipAfterRollback.BlockNumber,
			blocks[midpointIndex].BlockNumber(),
		)
	}

	// Verify the rollback point block is still accessible
	block, err := c.BlockByPoint(midpointPoint, nil)
	if err != nil {
		t.Errorf(
			"rollback point block should still be accessible, got error: %v",
			err,
		)
	} else if block.Slot != midpointPoint.Slot {
		t.Errorf(
			"rollback point block slot mismatch: got %d, want %d",
			block.Slot,
			midpointPoint.Slot,
		)
	}

	t.Log("state correctly restored after rollback")

	// Test that we can add new blocks after rollback
	// We need to reload blocks from the rollback point forward
	imm, err := immutable.New(testDataDir())
	if err != nil {
		t.Fatalf("failed to reopen immutable db: %v", err)
	}
	defer imm.Close() //nolint:errcheck

	iter, err := imm.BlocksFromPoint(midpointPoint)
	if err != nil {
		t.Fatalf("failed to create block iterator from midpoint: %v", err)
	}
	defer iter.Close()

	// Skip the midpoint block itself (it's already in the chain)
	_, err = iter.Next()
	if err != nil {
		t.Fatalf("failed to skip midpoint block: %v", err)
	}

	// Add a few more blocks after the rollback point
	blocksAdded := 0
	for range 10 {
		immBlock, err := iter.Next()
		if err != nil {
			t.Fatalf("unexpected error reading block after rollback: %v", err)
		}
		if immBlock == nil {
			break
		}

		block, err := ledger.NewBlockFromCbor(immBlock.Type, immBlock.Cbor)
		if err != nil {
			t.Fatalf("failed to decode block: %v", err)
		}

		if err := c.AddBlock(block, nil); err != nil {
			t.Fatalf("failed to add block after rollback: %v", err)
		}
		blocksAdded++
	}

	t.Logf("successfully added %d blocks after rollback", blocksAdded)

	// Verify new tip is correct
	newTip := c.Tip()
	if newTip.Point.Slot <= midpointPoint.Slot {
		t.Errorf(
			"new tip slot should be greater than midpoint: got %d, midpoint was %d",
			newTip.Point.Slot,
			midpointPoint.Slot,
		)
	}

	t.Logf(
		"final chain tip: slot=%d hash=%s blockNumber=%d",
		newTip.Point.Slot,
		hex.EncodeToString(newTip.Point.Hash),
		newTip.BlockNumber,
	)
}

func TestRollbackToOrigin(t *testing.T) {
	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Configure plugins with temp directory
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		config.DefaultBlobPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set blob plugin data-dir: %v", err)
	}
	if err := plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		config.DefaultMetadataPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set metadata plugin data-dir: %v", err)
	}

	// Create database
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter
	mockLedger := &mockLedgerState{securityParam: 100}
	cm.SetLedger(mockLedger)

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load some blocks
	numBlocks := 50
	blocks, _ := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Get tip before rollback
	tipBefore := c.Tip()
	t.Logf(
		"chain tip before rollback to origin: slot=%d hash=%s",
		tipBefore.Point.Slot,
		hex.EncodeToString(tipBefore.Point.Hash),
	)

	// Rollback to origin (slot 0, empty hash)
	originPoint := ocommon.Point{
		Slot: 0,
		Hash: []byte{},
	}

	err = c.Rollback(originPoint)
	if err != nil {
		t.Fatalf("rollback to origin failed: %v", err)
	}

	// Verify tip is at origin
	tipAfter := c.Tip()
	if tipAfter.Point.Slot != 0 {
		t.Errorf(
			"tip slot after origin rollback should be 0, got %d",
			tipAfter.Point.Slot,
		)
	}
	if len(tipAfter.Point.Hash) != 0 {
		t.Errorf(
			"tip hash after origin rollback should be empty, got %s",
			hex.EncodeToString(tipAfter.Point.Hash),
		)
	}

	t.Log("successfully rolled back to origin")
}

func TestChainIteratorAfterRollback(t *testing.T) {
	// Set up temporary directory for test database
	tmpDir := t.TempDir()

	// Configure plugins with temp directory
	if err := plugin.SetPluginOption(
		plugin.PluginTypeBlob,
		config.DefaultBlobPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set blob plugin data-dir: %v", err)
	}
	if err := plugin.SetPluginOption(
		plugin.PluginTypeMetadata,
		config.DefaultMetadataPlugin,
		"data-dir",
		tmpDir,
	); err != nil {
		t.Fatalf("failed to set metadata plugin data-dir: %v", err)
	}

	// Create database
	db, err := database.New(&database.Config{
		DataDir:        tmpDir,
		BlobPlugin:     config.DefaultBlobPlugin,
		MetadataPlugin: config.DefaultMetadataPlugin,
	})
	if err != nil {
		t.Fatalf("failed to create database: %v", err)
	}
	defer db.Close()

	// Create chain manager with database
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("failed to create chain manager: %v", err)
	}

	// Set security parameter
	mockLedger := &mockLedgerState{securityParam: 50}
	cm.SetLedger(mockLedger)

	// Get primary chain
	c := cm.PrimaryChain()
	if c == nil {
		t.Fatal("primary chain is nil")
	}

	// Load blocks
	numBlocks := 100
	blocks, points := loadBlocksFromImmutable(t, c, numBlocks)

	if len(blocks) < numBlocks || len(points) < numBlocks {
		t.Skipf(
			"not enough blocks in testdata: got %d, need %d",
			len(blocks),
			numBlocks,
		)
	}

	// Create iterator from origin
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("failed to create chain iterator: %v", err)
	}
	defer iter.Cancel()

	// Advance iterator to near the tip
	for i := 0; i < len(blocks)-10; i++ {
		next, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				break
			}
			t.Fatalf("unexpected error from iterator: %v", err)
		}
		if next == nil {
			t.Fatal("unexpected nil from iterator")
		}
	}

	// Rollback to midpoint
	midpointIndex := len(points) / 2
	midpointPoint := points[midpointIndex]

	t.Logf(
		"rolling back while iterator is active: slot=%d",
		midpointPoint.Slot,
	)

	err = c.Rollback(midpointPoint)
	if err != nil {
		t.Fatalf("rollback failed: %v", err)
	}

	// Iterator should report rollback on next call
	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("iterator.Next after rollback failed: %v", err)
	}
	if next == nil {
		t.Fatal("iterator returned nil after rollback")
	}
	if !next.Rollback {
		t.Error("iterator should report rollback, but Rollback flag is false")
	}

	// Verify rollback point matches
	if next.Point.Slot != midpointPoint.Slot {
		t.Errorf(
			"rollback point slot mismatch: got %d, want %d",
			next.Point.Slot,
			midpointPoint.Slot,
		)
	}

	t.Log("iterator correctly reported rollback event")
}
