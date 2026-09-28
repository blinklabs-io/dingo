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

// Package testutil provides common test helper utilities for the Dingo project.
// It replaces ad-hoc time.Sleep patterns with deterministic synchronization
// helpers that make tests faster and more reliable.
package testutil

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// AsyncWait is the deadline a test should give a wait on asynchronous
// progress -- a goroutine the code under test started, a background
// manager reacting to an event, a commit draining its after-commit
// callbacks -- when the test has no way to observe that work other than
// polling for its result.
//
// It is a failure deadline, not a delay. WaitForCondition and
// RequireReceive return the moment the condition holds, so a larger value
// costs a passing run nothing; it only changes how long a genuinely stuck
// test takes to report, and the per-package -timeout still backstops that.
//
// The value is deliberately far above the time the work takes on an idle
// machine, because that time is not what the deadline has to cover. `go
// test` defaults -parallel to GOMAXPROCS, so on a large host dozens of
// race-instrumented tests run at once, each with its own SQLite database,
// and a wait that completes in milliseconds standalone takes tens of
// seconds under that load. Slow single-core runners produce the same
// effect for the opposite reason.
//
// Use one value rather than a per-site guess: a spread of 1s/2s/5s
// deadlines across a package encodes no information about the work being
// waited on, and the short end of the spread is where the flakes are.
const AsyncWait = 2 * time.Minute

// WaitForCondition polls the given condition function until it returns true
// or the timeout expires. This replaces the common pattern of
// time.Sleep followed by an assertion check.
func WaitForCondition(
	t *testing.T,
	condition func() bool,
	timeout time.Duration,
	msg string,
) {
	t.Helper()
	require.Eventually(
		t,
		condition,
		timeout,
		10*time.Millisecond,
		msg,
	)
}

// WaitForConditionWithInterval is like WaitForCondition but allows specifying
// a custom polling interval.
func WaitForConditionWithInterval(
	t *testing.T,
	condition func() bool,
	timeout time.Duration,
	interval time.Duration,
	msg string,
) {
	t.Helper()
	require.Eventually(
		t,
		condition,
		timeout,
		interval,
		msg,
	)
}

// RequireReceive waits for a value on the given channel or fails the test
// if the timeout expires. This replaces the common pattern of
// time.Sleep followed by reading a channel.
func RequireReceive[T any](
	t *testing.T,
	ch <-chan T,
	timeout time.Duration,
	msg string,
) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for channel receive: %s", msg)
		var zero T
		return zero // unreachable
	}
}

// RequireNoReceive verifies that no value is received on the given channel
// within the specified duration. This replaces the pattern of
// time.Sleep followed by a non-blocking channel read to confirm
// that nothing was sent.
func RequireNoReceive[T any](
	t *testing.T,
	ch <-chan T,
	duration time.Duration,
	msg string,
) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf(
			"unexpected value received on channel: %v: %s",
			v,
			msg,
		)
	case <-time.After(duration):
		// Expected: nothing received
	}
}
