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

package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTransactionAlreadyCanceledContextFailsImmediately guards the simplest
// case: a ctx canceled before Transaction/ReadTransaction is even called
// must fail the begin outright rather than opening a transaction nothing
// can ever commit. Mirrors migrations/runner_test.go's
// TestProcessLockerCancellation shape: an already-canceled context is a
// deterministic guarantee, not a timing race.
func TestTransactionAlreadyCanceledContextFailsImmediately(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	writeErr := store.Transaction(ctx).Commit()
	require.ErrorIs(t, writeErr, context.Canceled)

	readErr := store.ReadTransaction(ctx).Commit()
	require.ErrorIs(t, readErr, context.Canceled)
}

// TestTransactionContextDeadlineAbortsBlockedBegin guards the core promise
// of threading a caller's ctx into Transaction: a caller waiting for a
// connection (here, SQLite's single-writer pool held by another
// transaction) is unblocked by its own deadline instead of waiting out
// whoever is holding the connection. Adapts
// TestSQLiteBulkModeKeepsPlannerAndWritersAvailable's blocking shape
// (store_test.go), swapping the blocking cause's resolution from "the
// holder commits" to "the waiter's own deadline fires".
func TestTransactionContextDeadlineAbortsBlockedBegin(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	store.writeDB.SetMaxOpenConns(1)

	holder := store.Transaction(t.Context())
	t.Cleanup(func() { _ = holder.Rollback() })

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	waiter := store.Transaction(ctx)
	err := waiter.Commit()
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(
		t, elapsed, 2*time.Second,
		"the waiter must return on its own deadline, not wait for the holder",
	)

	// The holder is unaffected by the waiter's unrelated deadline.
	require.NoError(t, holder.Commit())
}

// TestTransactionContextCancellationRollsBackWrites guards "preserve
// transaction rollback on cancellation": a write issued through a
// Transaction(ctx) must not survive once ctx is canceled mid-transaction,
// and the connection it held must be released back to the pool rather than
// leaked.
//
// This intentionally does not pin writeDB to a single connection: doing so
// with SQLite's mode=memory&cache=shared DSN interacts badly with
// database/sql discarding (rather than idling) a connection whose in-flight
// statement failed from ctx cancellation -- a brief window with zero live
// connections destroys the shared in-memory database out from under the
// test, which is a SQLite test-fixture artifact, not the behavior under
// test. Connection release is instead asserted directly against pool
// stats.
func TestTransactionContextCancellationRollsBackWrites(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	// database/sql discards (rather than idles) a pooled connection whose
	// in-flight statement failed from ctx cancellation, and reopens a fresh
	// one lazily on next use. For SQLite's mode=memory&cache=shared DSN, a
	// window with zero live connections destroys the shared in-memory
	// database along with it. Hold one extra, otherwise-unused connection
	// open for the test's duration so the schema survives that window.
	keepAlive, err := store.writeDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = keepAlive.Close() })

	ctx, cancel := context.WithCancel(t.Context())
	txn := store.Transaction(ctx)
	require.NoError(t, store.SetCommitTimestamp(42, txn))

	cancel()

	// database/sql rolls back a Tx once the ctx supplied to BeginTx is
	// canceled, per BeginTx's documented contract -- but that happens on an
	// internal watcher goroutine, not synchronously with cancel(), so poll
	// rather than assert immediately. (In practice this also fails on the
	// first attempt regardless of that goroutine's timing: dbFromTxn hands
	// this statement the transaction's own now-canceled ctx directly.)
	require.Eventually(t, func() bool {
		return store.SetCommitTimestamp(43, txn) != nil
	}, 2*time.Second, 5*time.Millisecond,
		"transaction must stop accepting writes once its ctx is canceled")

	require.Error(t, txn.Commit())

	// The connection the aborted transaction held must come back to the
	// pool rather than being leaked: only the keepAlive connection above
	// should remain checked out.
	require.Eventually(t, func() bool {
		return store.writeDB.Stats().InUse <= 1
	}, 2*time.Second, 5*time.Millisecond,
		"canceled transaction's connection must be released back to the pool")

	// Neither the successful first write nor anything else from the
	// canceled transaction may be durably visible.
	persisted, err := store.GetCommitTimestamp()
	require.NoError(t, err)
	require.Zero(t, persisted)
}
