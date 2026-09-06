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
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

type panicCommitTxn struct {
	rollbackCount int
}

func (*panicCommitTxn) Commit() error {
	panic("commit panic")
}

func (t *panicCommitTxn) Rollback() error {
	t.rollbackCount++
	return nil
}

// TestTxnDoCommitPanicReleasesLockAndBarrier proves that a panic raised by an
// underlying store's Commit does not strand Txn.lock or the shared commit
// barrier. Txn.Do must finish its recovery rollback and re-panic; lifecycle
// code must then be able to pause commits, and a writer queued behind that
// pause must open promptly after resume.
func TestTxnDoCommitPanicReleasesLockAndBarrier(t *testing.T) {
	db := &Database{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	backend := &panicCommitTxn{}
	txn := &Txn{
		db:          db,
		metadataTxn: backend,
		readWrite:   true,
	}
	acquireCommitBarrier(txn, true)

	panicRecovered := make(chan any, 1)
	go func() {
		defer func() { panicRecovered <- recover() }()
		_ = txn.Do(func(*Txn) error { return nil })
	}()

	require.Equal(t, "commit panic", testutil.RequireReceive(
		t,
		panicRecovered,
		time.Second,
		"commit panic must propagate after rollback",
	))
	require.Equal(t, 1, backend.rollbackCount,
		"Txn.Do must roll back the underlying store before re-panicking")

	paused := make(chan func(), 1)
	go func() { paused <- db.PauseCommits() }()
	resume := testutil.RequireReceive(
		t,
		paused,
		time.Second,
		"PauseCommits must acquire after panic cleanup",
	)
	var resumeOnce sync.Once
	safeResume := func() { resumeOnce.Do(resume) }

	writerOpened := make(chan struct{})
	writerRelease := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		next := &Txn{db: db, readWrite: true}
		acquireCommitBarrier(next, true)
		close(writerOpened)
		<-writerRelease
		_ = next.Rollback()
	}()
	var writerReleaseOnce sync.Once
	safeReleaseWriter := func() {
		writerReleaseOnce.Do(func() { close(writerRelease) })
	}
	t.Cleanup(func() {
		safeResume()
		safeReleaseWriter()
		select {
		case <-writerDone:
		case <-time.After(time.Second):
			t.Errorf("timeout cleaning up queued writer")
		}
	})
	testutil.RequireNoReceive(
		t,
		writerOpened,
		100*time.Millisecond,
		"the pause must still exclude a new writer",
	)
	safeResume()
	testutil.RequireReceive(
		t,
		writerOpened,
		time.Second,
		"the next writer must open after resume",
	)
	safeReleaseWriter()
	testutil.RequireReceive(
		t,
		writerDone,
		time.Second,
		"the next writer must roll back during cleanup",
	)
}
