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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

type blockingAfterCommitTxn struct {
	commitStarted chan struct{}
	allowCommit   chan struct{}
}

func (t *blockingAfterCommitTxn) Commit() error {
	close(t.commitStarted)
	<-t.allowCommit
	return nil
}

func (*blockingAfterCommitTxn) Rollback() error {
	return nil
}

// TestTxnAfterCommitRunsOnCommit verifies after-commit callbacks fire, once and
// in registration order, only after a read-write transaction commits.
func TestTxnAfterCommitRunsOnCommit(t *testing.T) {
	db := openTestDB(t)

	txn := db.Transaction(true)
	var order []int
	txn.AfterCommit(func() { order = append(order, 1) })
	txn.AfterCommit(func() { order = append(order, 2) })
	// A nil callback is ignored rather than panicking at commit time.
	txn.AfterCommit(nil)

	require.Empty(t, order, "callbacks must not fire before commit")
	require.NoError(t, txn.Commit())
	require.Equal(t, []int{1, 2}, order,
		"callbacks must run once, in registration order, after commit")
}

// TestTxnAfterCommitSkippedOnRollback verifies a rolled-back transaction never
// fires its after-commit callbacks, so callers may register side effects that
// must reflect committed state only.
func TestTxnAfterCommitSkippedOnRollback(t *testing.T) {
	db := openTestDB(t)

	txn := db.Transaction(true)
	fired := false
	txn.AfterCommit(func() { fired = true })

	require.NoError(t, txn.Rollback())
	require.False(t, fired, "rollback must not fire after-commit callbacks")
}

// TestTxnAfterCommitSkippedOnReadOnly verifies committing a read-only
// transaction (which only releases resources) does not fire callbacks, since no
// durable commit occurred.
func TestTxnAfterCommitSkippedOnReadOnly(t *testing.T) {
	db := openTestDB(t)

	txn := db.Transaction(false)
	fired := false
	txn.AfterCommit(func() { fired = true })

	require.NoError(t, txn.Commit())
	require.False(t, fired,
		"read-only commit must not fire after-commit callbacks")
}

func TestTxnAfterCommitConcurrentWithCommitIsNotLost(t *testing.T) {
	backend := &blockingAfterCommitTxn{
		commitStarted: make(chan struct{}),
		allowCommit:   make(chan struct{}),
	}
	txn := &Txn{
		metadataTxn: backend,
		readWrite:   true,
	}
	commitDone := make(chan error, 1)
	go func() {
		commitDone <- txn.Commit()
	}()
	testutil.RequireReceive(
		t,
		backend.commitStarted,
		time.Second,
		"backend commit started",
	)

	callbackFired := make(chan struct{})
	registrationStarted := make(chan struct{})
	registrationDone := make(chan struct{})
	go func() {
		close(registrationStarted)
		txn.AfterCommit(func() { close(callbackFired) })
		close(registrationDone)
	}()

	testutil.RequireReceive(
		t,
		registrationStarted,
		time.Second,
		"concurrent callback registration started",
	)
	close(backend.allowCommit)
	require.NoError(t, testutil.RequireReceive(
		t,
		commitDone,
		time.Second,
		"transaction commit",
	))
	testutil.RequireReceive(
		t,
		registrationDone,
		time.Second,
		"concurrent callback registration",
	)
	testutil.RequireReceive(
		t,
		callbackFired,
		time.Second,
		"concurrently registered callback",
	)
}

func TestTxnAfterCommitRegisteredAfterCommitRuns(t *testing.T) {
	db := openTestDB(t)
	txn := db.Transaction(true)
	require.NoError(t, txn.Commit())

	fired := false
	txn.AfterCommit(func() { fired = true })
	require.True(t, fired,
		"registration after a successful commit must dispatch immediately")
}

func TestTxnAfterCommitCallbackCanRegisterCallback(t *testing.T) {
	db := openTestDB(t)
	txn := db.Transaction(true)
	var order []int
	txn.AfterCommit(func() {
		order = append(order, 1)
		txn.AfterCommit(func() { order = append(order, 2) })
	})

	require.NoError(t, txn.Commit())
	require.Equal(t, []int{1, 2}, order)
}
