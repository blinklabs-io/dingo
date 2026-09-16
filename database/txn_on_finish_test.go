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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOnFinishFiresOnEveryTerminalPath pins the property that separates
// OnFinish from AfterCommit: a hold taken for a transaction's lifetime is
// released whichever way that transaction ends. AfterCommit fires only on a
// durable commit, so releasing from it strands the hold for the life of the
// process on every rollback.
func TestOnFinishFiresOnEveryTerminalPath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		readWrite bool
		finish    func(*Txn)
	}{
		{"commit", true, func(txn *Txn) { require.NoError(t, txn.Commit()) }},
		{"rollback", true, func(txn *Txn) { require.NoError(t, txn.Rollback()) }},
		{"release", true, func(txn *Txn) { txn.Release() }},
		{
			// Commit on a read-only transaction rolls back rather than
			// committing, and still has to report the transaction over.
			"read-only commit",
			false,
			func(txn *Txn) { require.NoError(t, txn.Commit()) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			txn := db.BlobTxn(tc.readWrite)
			var calls int
			var mu sync.Mutex
			txn.OnFinish(func() {
				mu.Lock()
				defer mu.Unlock()
				calls++
			})
			mu.Lock()
			require.Equal(
				t,
				0,
				calls,
				"callback fired before the transaction ended",
			)
			mu.Unlock()
			tc.finish(txn)
			mu.Lock()
			require.Equal(t, 1, calls, "callback did not fire exactly once")
			mu.Unlock()
			// A repeat terminal call is a no-op on the transaction and must
			// not re-fire the callback.
			require.NoError(t, txn.Rollback())
			mu.Lock()
			require.Equal(
				t,
				1,
				calls,
				"callback re-fired on a finished transaction",
			)
			mu.Unlock()
		})
	}
}

// TestOnFinishAfterFinishRunsImmediately pins that an acquire-then-register
// sequence cannot lose its release to a transaction that concluded in between.
func TestOnFinishAfterFinishRunsImmediately(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	require.NoError(t, txn.Commit())
	fired := false
	txn.OnFinish(func() { fired = true })
	require.True(
		t,
		fired,
		"registration on a finished transaction dropped the callback",
	)
}

// TestOnFinishRunsInRegistrationOrderAndContainsPanics pins that one caller's
// panicking callback cannot strand another caller's hold.
func TestOnFinishRunsInRegistrationOrderAndContainsPanics(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	var order []string
	txn.OnFinish(func() { order = append(order, "first") })
	txn.OnFinish(func() { panic("callback boom") })
	txn.OnFinish(func() { order = append(order, "third") })
	require.NotPanics(t, func() { require.NoError(t, txn.Rollback()) })
	require.Equal(t, []string{"first", "third"}, order)
}

// TestOnFinishCallbackMayTakeLocks pins that callbacks run without the
// transaction lock held, which is what lets them release a lock of their own.
func TestOnFinishCallbackMayTakeLocks(t *testing.T) {
	db := newTestDB(t)
	txn := db.BlobTxn(true)
	var held sync.Mutex
	held.Lock()
	txn.OnFinish(func() {
		// Re-entering the transaction from a callback would deadlock if the
		// dispatch still held txn.lock.
		txn.OnFinish(func() { held.Unlock() })
	})
	require.NoError(t, txn.Commit())
	require.True(t, held.TryLock(), "nested callback never ran")
}
