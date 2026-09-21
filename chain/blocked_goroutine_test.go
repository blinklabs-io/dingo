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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// semaphoreFrame is the runtime frame a goroutine blocked on a sync.Mutex or
// sync.RWMutex sits in. Matching the frame rather than the goroutine's wait
// reason keeps this independent of how the runtime spells that reason, which
// has changed between Go releases.
const semaphoreFrame = "sync.runtime_Semacquire"

// waitUntilParkedIn blocks until some goroutine is parked on a lock taken
// inside symbol.
//
// The tests below have to release a chain lock only once a second goroutine is
// already queued on it, because what they exercise is the window that opens
// when that lock is handed over. Being queued on a lock is not observable
// through the chain's own API and a wall-clock delay would leave the round
// silently unexercised whenever the goroutine was slower than the delay, so
// this reads the condition off the runtime's own goroutine dump.
func waitUntilParkedIn(t *testing.T, symbol string) {
	t.Helper()
	testutil.WaitForConditionWithInterval(
		t,
		func() bool {
			return testutil.GoroutineParkedIn(semaphoreFrame, symbol)
		},
		5*time.Second,
		time.Millisecond,
		"no goroutine parked on a lock in "+symbol,
	)
}

// waitUntilGoroutineIn waits until a goroutine has reached symbol, including
// waits on channels rather than only sync locks. It goes through
// testutil.GoroutineParkedIn for the same reason waitUntilParkedIn does: a
// fixed buffer handed to runtime.Stack truncates the dump silently, and a
// goroutine past the cut reads as absent.
func waitUntilGoroutineIn(t *testing.T, symbol string) {
	t.Helper()
	testutil.WaitForConditionWithInterval(
		t,
		func() bool {
			return testutil.GoroutineParkedIn(symbol)
		},
		5*time.Second,
		time.Millisecond,
		"no goroutine reached "+symbol,
	)
}
