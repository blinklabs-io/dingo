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
	"errors"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
)

// blockPoint builds the chain point for a mock test block.
func blockPoint(b *MockBlock) ocommon.Point {
	return ocommon.Point{
		Slot: b.SlotNumber(),
		Hash: b.Hash().Bytes(),
	}
}

// TestFromPointRejectsRolledBackPointPersistent verifies that a forward
// iterator cannot be created from a point that this chain rolled back.
//
// Rolling back removes the block row but retains the block in the manager's
// LRU cache so non-primary chains can still reconcile against it, which keeps
// the block resolvable by point long after it left the chain. An iterator
// built from such a point is positioned at an index the chain no longer has,
// so it can never yield the block the caller asked for. Blockfetch turns that
// into a StartBatch/BatchDone pair carrying no blocks, which the requesting
// peer cannot distinguish from a served range, so it re-requests the same
// single-block range forever instead of trying another peer.
func TestFromPointRejectsRolledBackPointPersistent(t *testing.T) {
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
	rolledBackPoint := blockPoint(testBlocks[len(testBlocks)-1])
	survivingPoint := blockPoint(testBlocks[len(testBlocks)-2])
	if err := c.Rollback(survivingPoint); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	// Precondition for the wedge: the rolled-back block is still
	// resolvable by point out of the retained block cache.
	if _, err := c.BlockByPoint(rolledBackPoint, nil); err != nil {
		t.Fatalf(
			"expected rolled-back block to stay resolvable by point: %s",
			err,
		)
	}
	iter, err := c.FromPoint(rolledBackPoint, true)
	if err == nil {
		iter.Cancel()
		t.Fatal(
			"expected FromPoint to reject a start point this chain rolled back",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
	// A reverse iterator shares the same start-point resolution and must
	// reject the rolled-back point too.
	revIter, err := c.FromPointReverse(rolledBackPoint, true)
	if err == nil {
		revIter.Cancel()
		t.Fatal(
			"expected FromPointReverse to reject a rolled-back start point",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound from reverse, got: %s", err)
	}
	// The surviving tip must remain iterable: the guard rejects points the
	// chain dropped, not points it still holds.
	tipIter, err := c.FromPoint(survivingPoint, true)
	if err != nil {
		t.Fatalf("unexpected error iterating from surviving tip: %s", err)
	}
	defer tipIter.Cancel()
	next, err := tipIter.Next(false)
	if err != nil {
		t.Fatalf("unexpected error reading surviving tip block: %s", err)
	}
	if next == nil || next.Point.Slot != survivingPoint.Slot {
		t.Fatalf(
			"expected surviving tip block at slot %d, got %+v",
			survivingPoint.Slot,
			next,
		)
	}
}

// TestFromPointRejectsRolledBackPointInMemory covers the same guard on a
// non-persistent chain, where rolled-back blocks stay in the manager's LRU
// cache instead of a database.
func TestFromPointRejectsRolledBackPointInMemory(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	rolledBackPoint := blockPoint(testBlocks[len(testBlocks)-1])
	survivingPoint := blockPoint(testBlocks[len(testBlocks)-2])
	if err := c.Rollback(survivingPoint); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	iter, err := c.FromPoint(rolledBackPoint, true)
	if err == nil {
		iter.Cancel()
		t.Fatal(
			"expected FromPoint to reject a start point this chain rolled back",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
}

// TestFromPointAcceptsCommonPointOnInMemoryFork verifies that a fork iterator
// can start at its in-memory primary chain intersection. The common prefix is
// not stored in the fork's blocks slice, and an in-memory manager has no
// database index for blockByIndex to query directly.
func TestFromPointAcceptsCommonPointOnInMemoryFork(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	primary := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := primary.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding primary block: %s", err)
		}
	}

	commonPoint := blockPoint(testBlocks[1])
	fork, err := cm.NewChain(commonPoint)
	if err != nil {
		t.Fatalf("unexpected error creating fork: %s", err)
	}

	iter, err := fork.FromPoint(commonPoint, true)
	if err != nil {
		t.Fatalf("unexpected error creating iterator at common point: %s", err)
	}
	defer iter.Cancel()
	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected error reading common point: %s", err)
	}
	if next == nil || next.Rollback || next.Point.Slot != commonPoint.Slot ||
		!bytes.Equal(next.Point.Hash, commonPoint.Hash) {
		t.Fatalf("expected common point, got %+v", next)
	}
	reverse, err := fork.FromPointReverse(commonPoint, true)
	if err != nil {
		t.Fatalf("unexpected error creating reverse iterator at common point: %s", err)
	}
	defer reverse.Cancel()
	reverseNext, err := reverse.Next(false)
	if err != nil {
		t.Fatalf("unexpected error reading common point in reverse: %s", err)
	}
	if reverseNext == nil || reverseNext.Rollback ||
		reverseNext.Point.Slot != commonPoint.Slot ||
		!bytes.Equal(reverseNext.Point.Hash, commonPoint.Hash) {
		t.Fatalf("expected common point in reverse, got %+v", reverseNext)
	}

	// The same point remains in the cache after the primary rollback, but it
	// is no longer part of the primary chain and must still be rejected.
	if err := primary.Rollback(blockPoint(testBlocks[0])); err != nil {
		t.Fatalf("unexpected primary rollback error: %s", err)
	}
	if _, err := fork.FromPoint(commonPoint, true); !errors.Is(
		err,
		models.ErrBlockNotFound,
	) {
		t.Fatalf("expected rolled-back common point to be rejected, got: %v", err)
	}
}
