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

package chain

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
)

func newBarrierTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("NewDatabase: %v", err)
	}
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	return db
}

// TestCallerTxnHoldOutlivesTheAdd pins the two boundaries the record has to
// sit between: it must not end when the add returns, because the caller has
// not committed yet, and it must end when the transaction concludes, whether
// that is a commit or a rollback.
func TestCallerTxnHoldOutlivesTheAdd(t *testing.T) {
	for _, tc := range []struct {
		name   string
		finish func(*database.Txn) error
	}{
		{"commit", func(txn *database.Txn) error { return txn.Commit() }},
		{"rollback", func(txn *database.Txn) error { return txn.Rollback() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newBarrierTestDB(t)
			c := &Chain{persistent: true}
			txn := db.BlobTxn(true)
			endAdd := c.beginCallerTxnAdd(txn)
			if got := c.pendingAdds.heldCount(); got != 1 {
				t.Fatalf(
					"add on a caller transaction recorded %d holds, want 1",
					got,
				)
			}
			endAdd()
			if got := c.pendingAdds.heldCount(); got != 1 {
				t.Fatalf(
					"hold ended with the add rather than with the transaction: %d holds",
					got,
				)
			}
			if err := tc.finish(txn); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got := c.pendingAdds.heldCount(); got != 0 {
				t.Fatalf("%s left %d holds outstanding", tc.name, got)
			}
		})
	}
}

// TestCallerTxnHoldSkipped pins the two cases that must record nothing: a nil
// transaction, whose store write the chain commits before the tip advances, and
// a non-persistent chain, which writes to the manager's block cache rather than
// the store and so has no commit to lag behind.
func TestCallerTxnHoldSkipped(t *testing.T) {
	db := newBarrierTestDB(t)
	persistent := &Chain{persistent: true}
	persistent.beginCallerTxnAdd(nil)()
	if got := persistent.pendingAdds.heldCount(); got != 0 {
		t.Fatalf("nil transaction recorded %d holds", got)
	}
	ephemeral := &Chain{}
	txn := db.BlobTxn(true)
	defer txn.Release()
	ephemeral.beginCallerTxnAdd(txn)()
	if got := ephemeral.pendingAdds.heldCount(); got != 0 {
		t.Fatalf("non-persistent chain recorded %d holds", got)
	}
}

// TestRepeatAddOnHeldTxnSkipsTheExclusion pins that a second add on a
// transaction already recorded neither records again nor queues behind the
// removal path's write hold. Queueing it there would pair a rollback waiting on
// a transaction with the goroutine that owns that transaction waiting on the
// rollback, and neither would move until the drain expired.
func TestRepeatAddOnHeldTxnSkipsTheExclusion(t *testing.T) {
	db := newBarrierTestDB(t)
	c := &Chain{persistent: true}
	txn := db.BlobTxn(true)
	defer txn.Release()
	c.beginCallerTxnAdd(txn)()

	c.batchCommitMutex.Lock()
	defer c.batchCommitMutex.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.beginCallerTxnAdd(txn)()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"repeat add on an already-recorded transaction queued behind the removal path",
		)
	}
	if got := c.pendingAdds.heldCount(); got != 1 {
		t.Fatalf("repeat add recorded %d holds, want 1", got)
	}
}

// TestAwaitPendingCallerAddsIsBounded pins that the wait is a safety valve
// rather than a synchronisation point. A caller that abandons its transaction,
// or that rolls the chain back from inside one it has not finished, must leave
// that one removal exposed to the window the barrier closes rather than
// blocking it -- and every chain mutation queued behind it -- for the life of
// the process.
func TestAwaitPendingCallerAddsIsBounded(t *testing.T) {
	db := newBarrierTestDB(t)
	c := &Chain{persistent: true}
	txn := db.BlobTxn(true)
	defer txn.Release()
	c.beginCallerTxnAdd(txn)()
	if got := c.pendingAdds.heldCount(); got != 1 {
		t.Fatalf("recorded %d holds, want 1", got)
	}
	outstanding, drained := c.pendingAdds.awaitDrained(10 * time.Millisecond)
	if drained {
		t.Fatal("wait reported a drain while a transaction was still open")
	}
	if outstanding != 1 {
		t.Fatalf(
			"expiry reported %d outstanding transactions, want 1",
			outstanding,
		)
	}
}
