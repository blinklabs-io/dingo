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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// callerTxnChain builds a persistent primary chain holding the first four test
// blocks, each written and committed by the chain itself, and returns it with
// its database.
func callerTxnChain(t *testing.T) (*database.Database, *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	// Larger than the chain this builds, so no rollback below is refused for
	// exceeding K and every one reaches its removal loop.
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for i, testBlock := range testBlocks[:4] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("AddBlock(%d): %v", i, err)
		}
	}
	return db, c
}

// addOnCallerTxn adds testBlocks[4] through txn, leaving block index 5 on the
// in-memory chain and out of the store until txn concludes.
func addOnCallerTxn(
	t *testing.T,
	db *database.Database,
	c *chain.Chain,
) *database.Txn {
	t.Helper()
	txn := db.BlobTxn(true)
	if err := c.AddBlock(testBlocks[4], txn); err != nil {
		_ = txn.Rollback()
		t.Fatalf("AddBlock on caller transaction: %v", err)
	}
	if tip := c.Tip(); tip.Point.Slot != testBlocks[4].SlotNumber() {
		_ = txn.Rollback()
		t.Fatalf("in-memory tip did not advance: %+v", tip)
	}
	if _, err := db.BlockByIndex(5, nil); !errors.Is(
		err,
		models.ErrBlockNotFound,
	) {
		_ = txn.Rollback()
		t.Fatalf(
			"expected block index 5 to be invisible outside the caller transaction, got %v",
			err,
		)
	}
	return txn
}

// rollbackPoint is the point of testBlocks[2], block index 3: a rollback there
// removes indices 5 and 4, so its removal loop starts at the index the caller
// transaction still holds.
func rollbackPoint() ocommon.Point {
	return ocommon.Point{
		Slot: testBlocks[2].SlotNumber(),
		Hash: testBlocks[2].Hash().Bytes(),
	}
}

// TestRollbackWaitsForUncommittedCallerTransaction pins that a rollback never
// resolves a block index whose store write is still sitting in an uncommitted
// caller-supplied transaction.
//
// addBlockLocked writes the block through whichever transaction it is given and
// then advances c.tipBlockIndex under c.mutex. With a caller-supplied
// transaction the chain neither performs nor observes the commit, so between
// the tip advancing and the caller committing there is an index the in-memory
// chain legitimately holds and the store cannot serve:
// ChainManager.removeBlockByIndex opens its own transaction, and no transaction
// sees another's uncommitted writes. rollbackLocked's removal loop starts at
// c.tipBlockIndex, so without the barrier it fails its very first iteration
// with "remove block at index 5: block not found". Chain.batchCommitMutex
// closes this window for the batch transactions the chain owns and left it open
// here (issue #4005).
func TestRollbackWaitsForUncommittedCallerTransaction(t *testing.T) {
	db, c := callerTxnChain(t)
	txn := addOnCallerTxn(t, db, c)

	done := make(chan error, 1)
	go func() { done <- c.Rollback(rollbackPoint()) }()

	// The rollback must still be waiting: reaching its removal loop now is
	// exactly the defect, and it reports it as a not-found index rather than
	// by blocking.
	select {
	case err := <-done:
		t.Fatalf(
			"rollback resolved indices of an uncommitted caller transaction instead of waiting: %v",
			err,
		)
	case <-time.After(500 * time.Millisecond):
	}

	if err := txn.Commit(); err != nil {
		t.Fatalf("commit caller transaction: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("rollback after the caller transaction committed: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal(
			"rollback did not resume after the caller transaction committed",
		)
	}

	if tip := c.Tip(); tip.Point.Slot != testBlocks[2].SlotNumber() {
		t.Fatalf("unexpected tip after rollback: %+v", tip)
	}
	for _, idx := range []uint64{4, 5} {
		if _, err := db.BlockByIndex(idx, nil); !errors.Is(
			err,
			models.ErrBlockNotFound,
		) {
			t.Fatalf("rollback left block index %d in the store: %v", idx, err)
		}
	}
}

// TestRollbackResumesWhenCallerTransactionRollsBack pins the failure mode of
// the release hook: a transaction that rolls back must free the barrier just as
// a commit does. Releasing from AfterCommit, which fires only on a durable
// commit, would strand the record and leave every later rollback waiting out
// the drain timeout.
func TestRollbackResumesWhenCallerTransactionRollsBack(t *testing.T) {
	db, c := callerTxnChain(t)
	txn := addOnCallerTxn(t, db, c)
	if err := txn.Rollback(); err != nil {
		t.Fatalf("rollback caller transaction: %v", err)
	}

	// The rollback reports the index the abandoned transaction never wrote;
	// what is pinned here is that it gets there at all. The drain timeout is
	// 30s, so a bound well inside it separates "released by the transaction
	// ending" from "waited the barrier out".
	done := make(chan error, 1)
	go func() { done <- c.Rollback(rollbackPoint()) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal(
			"rollback did not resume after the caller transaction rolled back: " +
				"the barrier is released only on commit",
		)
	}
}
