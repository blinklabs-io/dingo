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
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("replayWG did not complete: a worker was spawned despite ls.closed=true")
	}
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
	ls.replayBufferedHeadersAsync(fixture.connId)

	// Confirm a worker really did start and is now waiting on the lock.
	// (The goroutine ran replayWG.Add(1) synchronously before launching.)
	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- ls.Close()
	}()

	// Close must not return while the worker is still in flight.
	select {
	case err := <-closeReturned:
		ls.chainsyncMutex.Unlock()
		t.Fatalf("Close returned (err=%v) before replay drained", err)
	case <-time.After(200 * time.Millisecond):
	}

	// Releasing the mutex lets the worker observe ls.closed=true and exit
	// without issuing any DB reads.
	ls.chainsyncMutex.Unlock()

	select {
	case err := <-closeReturned:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Close did not return after replay worker exited")
	}
}
