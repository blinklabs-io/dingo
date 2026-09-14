// Copyright 2026 Blink Labs Software

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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// headerRestoreChain builds a persistent primary chain holding the first three
// test blocks and queues headers for the remaining three on top of that tip.
func headerRestoreChain(t *testing.T) (*database.Database, *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for i, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("AddBlock(%d): %v", i, err)
		}
	}
	for i, testBlock := range testBlocks[3:] {
		if err := c.AddBlockHeader(testBlock); err != nil {
			t.Fatalf("AddBlockHeader(%d): %v", i+3, err)
		}
	}
	return db, c
}

func headerPoint(b *MockBlock) ocommon.Point {
	return ocommon.Point{Slot: b.SlotNumber(), Hash: b.Hash().Bytes()}
}

// TestQueuedHeaderRollbackIsNotUndoneByACallerTransaction pins that a
// queued-header rollback survives a caller-supplied transaction rolling back
// underneath it.
//
// rollbackLocked answers a rollback whose point is a queued header without
// waiting for caller transactions, because it removes no persistent block. It
// does trim the queued-header list, and that list is exactly what
// finishCallerTxnAdd restores when a caller transaction concludes without
// committing. Nothing counted the trim as a chain mutation, so the restore
// guard that refuses to write over a moved chain did not see it, and the
// pre-add queue went back over the trim: a header the rollback had already
// published a HeaderInvalidationRollback for was re-queued.
func TestQueuedHeaderRollbackIsNotUndoneByACallerTransaction(t *testing.T) {
	t.Parallel()

	db, c := headerRestoreChain(t)
	// The queue is [3 4 5] over a tip of testBlocks[2].
	if got := c.HeaderCount(); got != 3 {
		t.Fatalf("queued %d headers, want 3", got)
	}
	// The add matches the first queued header, so it consumes it and leaves
	// [4 5]. Its store write stays inside txn until txn concludes.
	txn := db.BlobTxn(true)
	defer txn.Release()
	if err := c.AddBlock(testBlocks[3], txn); err != nil {
		t.Fatalf("AddBlock on caller transaction: %v", err)
	}
	if got := c.HeaderCount(); got != 2 {
		t.Fatalf("after the add the queue holds %d headers, want 2", got)
	}

	// Roll back to the first remaining queued header. That discards the
	// header for testBlocks[5] and removes no block.
	done := make(chan error, 1)
	go func() { done <- c.Rollback(headerPoint(testBlocks[4])) }()

	// The rollback may answer straight away or wait for the caller
	// transaction; both are permitted, and the assertions below hold either
	// way. What is not permitted is trimming the queue and then having the
	// trim written back over.
	var rollbackErr error
	answered := false
	select {
	case rollbackErr = <-done:
		answered = true
	case <-time.After(2 * time.Second):
	}

	if err := txn.Rollback(); err != nil {
		t.Fatalf("roll back the caller transaction: %v", err)
	}

	if !answered {
		select {
		case rollbackErr = <-done:
		case <-time.After(60 * time.Second):
			t.Fatal("queued-header rollback did not finish")
		}
	}
	if rollbackErr != nil {
		t.Fatalf("queued-header rollback: %v", rollbackErr)
	}

	// The header for testBlocks[3] returns, because the block that consumed
	// it was never committed. The header for testBlocks[5] must not: the
	// rollback discarded it and published its invalidation.
	if got := c.HeaderCount(); got != 2 {
		t.Fatalf(
			"queue holds %d headers after the rollback, want 2; a third is the discarded header re-queued",
			got,
		)
	}
	start, end := c.HeaderRange(10)
	if start.Slot != testBlocks[3].SlotNumber() {
		t.Fatalf(
			"queue starts at slot %d, want %d",
			start.Slot,
			testBlocks[3].SlotNumber(),
		)
	}
	if end.Slot != testBlocks[4].SlotNumber() {
		t.Fatalf(
			"queue ends at slot %d, want %d: the discarded header was resurrected",
			end.Slot,
			testBlocks[4].SlotNumber(),
		)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("unexpected tip after rollback: %+v", tip)
	}
}

// TestRejectedFirstCallerAddLeavesNoBarrierHold pins that a caller transaction
// whose only add was rejected does not hold the pending-add barrier. The hold
// exists so a removal path waits for a chain add whose store write it cannot
// see; an add rejected before it mutated anything leaves no such write, so a
// rollback that waits pendingAddDrainTimeout for it and then aborts is waiting
// for nothing.
func TestRejectedFirstCallerAddLeavesNoBarrierHold(t *testing.T) {
	t.Parallel()

	db, c := callerTxnChain(t)
	// Queue the header the chain expects next, then offer a different block.
	if err := c.AddBlockHeader(testBlocks[4]); err != nil {
		t.Fatalf("AddBlockHeader: %v", err)
	}
	txn := db.BlobTxn(true)
	defer txn.Release()
	if err := c.AddBlock(testBlocks[5], txn); err == nil {
		t.Fatal(
			"expected the add to be rejected for not matching the queued header",
		)
	}

	done := make(chan error, 1)
	go func() { done <- c.Rollback(rollbackPoint()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rollback after a rejected caller add: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal(
			"rollback waited on a caller transaction that carries no chain add",
		)
	}
}
