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

package chain_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// spliceForkHashPrefix keeps competing fork blocks distinct from the shared
// testBlocks fixtures while staying 32 bytes wide.
const spliceForkHashPrefix = "00004744abababababababababababababababababababababababababab"

func mockBlockPoint(b *MockBlock) ocommon.Point {
	return ocommon.Point{
		Slot: b.SlotNumber(),
		Hash: b.Hash().Bytes(),
	}
}

// buildAbandonedForkChain sets up the exact state that precedes the #3005
// cross-fork splice:
//
//	index 1: testBlocks[0]        (shared ancestor)
//	index 2: testBlocks[1]        (shared ancestor, rollback target)
//	index 3: forkB[0]             (fork B, currently on the chain)
//	index 4: forkB[1]             (fork B tip)
//
// while testBlocks[2] and testBlocks[3] (fork A) were rolled back off the chain
// and therefore only survive in the manager's retained block cache, still
// carrying their old indices 3 and 4.
//
// It returns the chain plus fork A's first rolled-back block.
func buildAbandonedForkChain(
	t *testing.T,
	db *database.Database,
) (*chain.Chain, *MockBlock, []*MockBlock) {
	t.Helper()
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	// Fork A: the shared ancestors plus two blocks we later abandon.
	for _, testBlock := range testBlocks[:4] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding fork A block: %s", err)
		}
	}
	// Abandon fork A back to the shared ancestor at index 2.
	sharedAncestor := testBlocks[1]
	if err := c.Rollback(mockBlockPoint(sharedAncestor)); err != nil {
		t.Fatalf("unexpected error rolling back to shared ancestor: %s", err)
	}
	// Fork B replaces indices 3 and 4 with different blocks.
	forkB := []*MockBlock{
		{
			MockBlockNumber: 3,
			MockSlot:        41,
			MockHash:        spliceForkHashPrefix + "0003",
			MockPrevHash:    sharedAncestor.MockHash,
		},
		{
			MockBlockNumber: 4,
			MockSlot:        61,
			MockHash:        spliceForkHashPrefix + "0004",
			MockPrevHash:    spliceForkHashPrefix + "0003",
		},
	}
	for _, forkBlock := range forkB {
		if err := c.AddBlock(forkBlock, nil); err != nil {
			t.Fatalf("unexpected error adding fork B block: %s", err)
		}
	}
	// Precondition for the wedge: fork A's abandoned block is still
	// resolvable by point out of the retained block cache, and it still
	// reports the index that fork B now occupies.
	abandoned := testBlocks[2]
	cachedBlock, err := c.BlockByPoint(mockBlockPoint(abandoned), nil)
	if err != nil {
		t.Fatalf(
			"expected abandoned fork A block to stay resolvable by point: %s",
			err,
		)
	}
	if cachedBlock.ID != 3 {
		t.Fatalf(
			"expected retained fork A block to keep index 3, got %d",
			cachedBlock.ID,
		)
	}
	return c, abandoned, forkB
}

// assertChainPrevHashContiguous walks the persisted chain from origin and
// verifies every block's PrevHash matches the hash of the block stored at the
// preceding index. A cross-fork splice shows up here as a block whose parent is
// not the block that physically precedes it, which is precisely the shape that
// leaves a spender on the chain whose producing block was never applied.
func assertChainPrevHashContiguous(t *testing.T, c *chain.Chain) {
	t.Helper()
	iter, err := c.FromPoint(ocommon.NewPointOrigin(), false)
	if err != nil {
		t.Fatalf("unexpected error creating chain iterator: %s", err)
	}
	defer iter.Cancel()
	var prevHash []byte
	var prevSlot uint64
	for {
		next, err := iter.Next(false)
		if errors.Is(err, chain.ErrIteratorChainTip) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error iterating chain: %s", err)
		}
		if next == nil {
			t.Fatal("unexpected nil iterator result")
		}
		blk := next.Block
		if prevHash != nil && !bytes.Equal(blk.PrevHash, prevHash) {
			t.Fatalf(
				"cross-fork splice: block at index %d (slot %d) has prev hash %s "+
					"but the preceding chain block (slot %d) has hash %s",
				blk.ID,
				blk.Slot,
				hex.EncodeToString(blk.PrevHash),
				prevSlot,
				hex.EncodeToString(prevHash),
			)
		}
		prevHash = blk.Hash
		prevSlot = blk.Slot
	}
}

// TestRollbackRejectsPointNotOnChain covers the root cause of issue #3005.
//
// Chain.rollbackLocked resolves the rollback point through
// ChainManager.blockByPoint, which answers from the retained block cache before
// the database. A block this chain already rolled back therefore still resolves
// and still reports its old block index, an index another fork now occupies.
// Rolling back to it truncates to that stale index and moves currentTip to a
// point the chain does not hold, so the next block is appended above a block
// that is not its parent: a cross-fork splice.
func TestRollbackRejectsPointNotOnChain(t *testing.T) {
	db := newTestDB(t)
	c, abandoned, forkB := buildAbandonedForkChain(t, db)
	forkBTip := mockBlockPoint(forkB[len(forkB)-1])

	err := c.Rollback(mockBlockPoint(abandoned))
	if err == nil {
		t.Fatal(
			"expected Rollback to reject a point this chain no longer holds",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
	tip := c.Tip()
	if tip.Point.Slot != forkBTip.Slot ||
		!bytes.Equal(tip.Point.Hash, forkBTip.Hash) {
		t.Fatalf(
			"rejected rollback must leave the tip untouched: got %d/%s, want %d/%s",
			tip.Point.Slot,
			hex.EncodeToString(tip.Point.Hash),
			forkBTip.Slot,
			hex.EncodeToString(forkBTip.Hash),
		)
	}
	assertChainPrevHashContiguous(t, c)
}

// TestValidateRollbackRejectsPointNotOnChain covers the pre-check the chainsync
// rollback-loop detector uses to decide whether a repeated peer rollback is
// "crossable". A point resolvable only through the retained cache must not be
// reported as crossable, otherwise the detector keeps re-applying the very
// rollback that splices the chain.
func TestValidateRollbackRejectsPointNotOnChain(t *testing.T) {
	db := newTestDB(t)
	c, abandoned, _ := buildAbandonedForkChain(t, db)

	err := c.ValidateRollback(mockBlockPoint(abandoned))
	if err == nil {
		t.Fatal(
			"expected ValidateRollback to reject a point this chain no longer holds",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
}

// TestRollbackToRetainedPointDoesNotSpliceChain drives the full defect: after
// the bad rollback the chain accepts a continuation built on the abandoned fork
// while the block physically stored at the preceding index belongs to the other
// fork. Iterating the chain then yields a block whose parent is absent, which is
// what leaves the ledger unable to resolve the producer of that block's inputs.
func TestRollbackToRetainedPointDoesNotSpliceChain(t *testing.T) {
	db := newTestDB(t)
	c, abandoned, _ := buildAbandonedForkChain(t, db)

	// A peer serving fork A rolls us back to its own block, then feeds the
	// next block on fork A.
	if err := c.Rollback(mockBlockPoint(abandoned)); err == nil {
		// Only the unfixed code reaches here; keep going so the assertion
		// below reports the splice rather than a bare "expected error".
		continuation := testBlocks[3]
		if addErr := c.AddBlock(continuation, nil); addErr != nil {
			t.Fatalf(
				"unexpected error adding fork A continuation: %s",
				addErr,
			)
		}
	}
	assertChainPrevHashContiguous(t, c)
}

// TestRollbackRejectsPointAheadOfTip covers the other shape of the same defect.
//
// After a rollback the abandoned blocks keep their original, higher block
// indexes in the retained cache, so a peer can hand back a point that resolves
// above the current tip. Obeying it set tipBlockIndex above the last block the
// chain actually stores and moved currentTip to a block absent from the chain,
// leaving a hole: the next block was written past the gap, chain iteration
// stopped short of it, and prev-hash contiguity was enforced against a phantom
// tip. Like the stale-index shape, the chain must refuse rather than adopt a
// tip it does not hold.
//
// It must be refused as "point not found", never as an over-K rollback: issue
// #3035 was a node permanently denying every peer because this case was
// misclassified as exceeding the security parameter. Not-on-chain re-intersects
// and recovers; over-K does not.
func TestRollbackRejectsPointAheadOfTip(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 2)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	// Roll back the last two blocks (index 6 -> 4). The removed blocks stay in
	// the manager's cache carrying indexes 5 and 6.
	if err := c.Rollback(mockBlockPoint(testBlocks[len(testBlocks)-3])); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	tipBefore := c.Tip()
	aheadPoint := mockBlockPoint(testBlocks[len(testBlocks)-1])

	for _, tc := range []struct {
		name string
		call func() error
	}{
		{
			name: "ValidateRollback",
			call: func() error { return c.ValidateRollback(aheadPoint) },
		},
		{
			name: "Rollback",
			call: func() error { return c.Rollback(aheadPoint) },
		},
	} {
		err := tc.call()
		if err == nil {
			t.Fatalf(
				"%s: expected rejection of a point ahead of the chain tip",
				tc.name,
			)
		}
		if errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
			t.Fatalf(
				"%s: a point ahead of the tip must not be reported as "+
					"exceeding security param K (issue #3035): %s",
				tc.name,
				err,
			)
		}
		if !errors.Is(err, chain.ErrRollbackPointNotOnChain) {
			t.Fatalf(
				"%s: expected ErrRollbackPointNotOnChain, got: %s",
				tc.name,
				err,
			)
		}
		if !errors.Is(err, models.ErrBlockNotFound) {
			t.Fatalf(
				"%s: rejection must wrap ErrBlockNotFound so callers "+
					"re-intersect, got: %s",
				tc.name,
				err,
			)
		}
	}

	tipAfter := c.Tip()
	if tipAfter.Point.Slot != tipBefore.Point.Slot ||
		!bytes.Equal(tipAfter.Point.Hash, tipBefore.Point.Hash) {
		t.Fatalf(
			"rejected rollback must leave the tip untouched: got %d/%s, want %d/%s",
			tipAfter.Point.Slot,
			hex.EncodeToString(tipAfter.Point.Hash),
			tipBefore.Point.Slot,
			hex.EncodeToString(tipBefore.Point.Hash),
		)
	}
	assertChainPrevHashContiguous(t, c)
}

// TestRollbackStillAcceptsPointsOnChain guards against the fix over-rejecting:
// ordinary rollbacks to blocks the chain still holds must keep working, and a
// rollback to origin must remain possible.
func TestRollbackStillAcceptsPointsOnChain(t *testing.T) {
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	target := mockBlockPoint(testBlocks[2])
	if err := c.ValidateRollback(target); err != nil {
		t.Fatalf("unexpected error validating on-chain rollback: %s", err)
	}
	if err := c.Rollback(target); err != nil {
		t.Fatalf("unexpected error rolling back to on-chain point: %s", err)
	}
	tip := c.Tip()
	if tip.Point.Slot != target.Slot ||
		!bytes.Equal(tip.Point.Hash, target.Hash) {
		t.Fatalf(
			"expected tip at rollback point %d, got %d",
			target.Slot,
			tip.Point.Slot,
		)
	}
	assertChainPrevHashContiguous(t, c)
	if err := c.Rollback(ocommon.NewPointOrigin()); err != nil {
		t.Fatalf("unexpected error rolling back to origin: %s", err)
	}
	if c.Tip().Point.Slot != 0 {
		t.Fatalf("expected origin tip, got slot %d", c.Tip().Point.Slot)
	}
}
