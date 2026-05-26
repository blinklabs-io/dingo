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

package ledger

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// Regression for #2107: replayBufferedHeadersAsync must be a no-op once
// Close has been observed, so its goroutine can never reach DB reads
// after the database is closed.
func TestReplayBufferedHeadersAsyncSkippedAfterClose(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	// Simulate that Close has begun (closed=true) before the replay
	// is scheduled. The async helper must not spawn a worker.
	ls.closed.Store(true)

	ls.replayBufferedHeadersAsync(fixture.connId)

	done := make(chan struct{})
	go func() {
		ls.replayWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"replayWG did not complete: a worker was spawned despite ls.closed=true",
	)
}

// Regression for #2107: Close must drain in-flight replay goroutines
// before returning, so callers (Node.shutdown phase 3) can safely close
// the database without racing the replay's DB reads.
func TestCloseWaitsForInFlightReplay(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	// Hold the chainsync mutex so the goroutine spawned by
	// replayBufferedHeadersAsync blocks before touching the DB.
	ls.chainsyncMutex.Lock()
	chainsyncMutexLocked := true
	defer func() {
		if chainsyncMutexLocked {
			ls.chainsyncMutex.Unlock()
		}
	}()
	ls.replayBufferedHeadersAsync(fixture.connId)

	// (The goroutine ran replayWG.Add(1) synchronously before launching,
	// so the wait group counter is non-zero from this point.)
	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- ls.Close()
	}()

	// Wait until Close has set closed=true and is therefore committed
	// to draining the replay worker before it can return.
	testutil.WaitForCondition(
		t,
		ls.closed.Load,
		2*time.Second,
		"Close did not set ls.closed within 2s",
	)

	// With closed=true and the worker blocked on chainsyncMutex, Close
	// must not return: doing so would close the DB while the worker is
	// still alive (the original #2107 panic).
	testutil.RequireNoReceive(
		t,
		closeReturned,
		50*time.Millisecond,
		"Close returned before replay drained",
	)

	// Releasing the mutex lets the worker observe ls.closed=true and exit
	// without issuing any DB reads; Close can then finish draining.
	ls.chainsyncMutex.Unlock()
	chainsyncMutexLocked = false

	err := testutil.RequireReceive(
		t,
		closeReturned,
		15*time.Second,
		"Close did not return after replay worker exited",
	)
	if err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}
