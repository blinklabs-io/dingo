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

// slotZeroHashPoint is a rollback point carrying a real 32-byte hash at slot 0.
// Slot 0 is a legitimate slot -- it is where a Byron-era genesis-adjacent point
// sits -- so a point is not "empty" merely because its slot is zero. Only a
// point with neither a slot nor a hash is.
func slotZeroHashPoint() ocommon.Point {
	hash, err := hex.DecodeString(
		"00004744abababababababababababababababababababababababab5107",
	)
	if err != nil {
		panic(err)
	}
	// Pad to the 32 bytes a block hash occupies.
	full := make([]byte, 32)
	copy(full, hash)
	return ocommon.Point{Slot: 0, Hash: full}
}

// TestValidateRollbackChecksMembershipOfSlotZeroHashPoint covers the gate on
// the rollback-point lookup. Both ValidateRollback and rollbackLocked resolved
// the point only when point.Slot > 0, so a point at slot 0 carrying a hash the
// chain does not hold skipped rollbackPointBlock entirely and was reported as
// valid.
func TestValidateRollbackChecksMembershipOfSlotZeroHashPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)

	err := c.ValidateRollback(slotZeroHashPoint())
	if err == nil {
		t.Fatal(
			"expected ValidateRollback to reject a slot-0 point whose hash " +
				"this chain does not hold; a hash-bearing point must have its " +
				"membership checked regardless of slot",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}
}

// TestRollbackChecksMembershipOfSlotZeroHashPoint drives the same gap through
// the mutating path. Without the fix the lookup is skipped, rollbackBlockIndex
// stays 0, the fork depth is computed against index zero, and the rollback
// deletes the chain tail while setting currentTip to a point whose block the
// chain does not retain.
func TestRollbackChecksMembershipOfSlotZeroHashPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)
	tipBefore := c.Tip()

	err := c.Rollback(slotZeroHashPoint())
	if err == nil {
		t.Fatal(
			"expected Rollback to reject a slot-0 point whose hash this " +
				"chain does not hold",
		)
	}
	if !errors.Is(err, models.ErrBlockNotFound) {
		t.Fatalf("expected ErrBlockNotFound, got: %s", err)
	}

	tip := c.Tip()
	if tip.Point.Slot != tipBefore.Point.Slot ||
		!bytes.Equal(tip.Point.Hash, tipBefore.Point.Hash) {
		t.Fatalf(
			"a rejected rollback must leave the tip untouched: got %d/%s, "+
				"want %d/%s",
			tip.Point.Slot,
			hex.EncodeToString(tip.Point.Hash),
			tipBefore.Point.Slot,
			hex.EncodeToString(tipBefore.Point.Hash),
		)
	}
	if tip.BlockNumber != tipBefore.BlockNumber {
		t.Fatalf(
			"a rejected rollback must not move the block number: got %d, want %d",
			tip.BlockNumber,
			tipBefore.BlockNumber,
		)
	}
}

// TestRollbackStillAcceptsGenuineEmptyPoint pins the case the slot > 0 gate was
// there to serve: a point with neither slot nor hash names no block, so it must
// still skip the lookup rather than fail it.
func TestRollbackStillAcceptsGenuineEmptyPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)

	if err := c.ValidateRollback(ocommon.Point{}); err != nil {
		t.Fatalf(
			"an empty point names no block and must not require a lookup: %s",
			err,
		)
	}
}

func buildSlotZeroTestChain(t *testing.T, db *database.Database) *chain.Chain {
	t.Helper()
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:4] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block: %s", err)
		}
	}
	return c
}

// TestIteratorResumesAfterSlotZeroRollbackPoint covers the member of this class
// that the fix above makes reachable. testBlocks[0] sits at slot 0 with a real
// hash, so once a hash-bearing slot-0 point resolves properly it can reach the
// iterator's pending-rollback branch -- which gated the same way and would
// otherwise treat it as "rolling back to origin" and replay the chain from
// genesis instead of resuming after the rolled-back-to block.
func TestIteratorResumesAfterSlotZeroRollbackPoint(t *testing.T) {
	db := newTestDB(t)
	c := buildSlotZeroTestChain(t, db)

	genesisPoint := mockBlockPoint(testBlocks[0])
	if genesisPoint.Slot != 0 {
		t.Fatalf("fixture precondition: expected slot 0, got %d", genesisPoint.Slot)
	}
	if len(genesisPoint.Hash) == 0 {
		t.Fatal("fixture precondition: expected a non-empty hash at slot 0")
	}

	iter, err := c.FromPoint(genesisPoint, true)
	if err != nil {
		t.Fatalf("unexpected error creating iterator: %s", err)
	}
	// Advance the iterator past the rollback target so the rollback is queued
	// against it rather than landing where it already sits.
	for range 3 {
		if _, err := iter.Next(false); err != nil {
			t.Fatalf("unexpected error advancing iterator: %s", err)
		}
	}

	if err := c.Rollback(genesisPoint); err != nil {
		t.Fatalf("rollback to a real slot-0 block must succeed: %s", err)
	}

	res, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected iterator error: %s", err)
	}
	if res == nil {
		t.Fatal("expected a rollback result from the iterator")
	}
	if !res.Rollback {
		t.Fatal("expected the pending rollback to be delivered first")
	}
	if res.Point.Slot != genesisPoint.Slot ||
		!bytes.Equal(res.Point.Hash, genesisPoint.Hash) {
		t.Fatalf(
			"rollback point mismatch: got %d/%s, want %d/%s",
			res.Point.Slot,
			hex.EncodeToString(res.Point.Hash),
			genesisPoint.Slot,
			hex.EncodeToString(genesisPoint.Hash),
		)
	}

	// The rollback truncated the chain to the target, so resuming correctly
	// means the iterator sits *after* it. Add a fresh block and require that
	// the iterator delivers that one -- not testBlocks[0] replayed, which is
	// what resetting to the initial block index would produce.
	nextBlock := &MockBlock{
		MockBlockNumber: 2,
		MockSlot:        21,
		MockHash:        spliceForkHashPrefix + "0021",
		MockPrevHash:    testBlocks[0].MockHash,
	}
	if err := c.AddBlock(nextBlock, nil); err != nil {
		t.Fatalf("unexpected error adding block after rollback: %s", err)
	}

	next, err := iter.Next(false)
	if err != nil {
		t.Fatalf("unexpected iterator error: %s", err)
	}
	if next == nil {
		t.Fatal("expected the iterator to deliver the block after the rollback")
	}
	if next.Block.Slot == genesisPoint.Slot {
		t.Fatal(
			"iterator replayed the slot-0 rollback target instead of resuming " +
				"after it: the pending-rollback branch treated a hash-bearing " +
				"slot-0 point as a rollback to origin",
		)
	}
	if next.Block.Slot != nextBlock.MockSlot {
		t.Fatalf(
			"iterator must resume after the slot-0 rollback point: got slot "+
				"%d, want %d",
			next.Block.Slot,
			nextBlock.MockSlot,
		)
	}
}
