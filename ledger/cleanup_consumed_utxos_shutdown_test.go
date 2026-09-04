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
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// shrinkCleanupConsumedUtxosInterval makes the periodic cleanup timer fire on
// a test timescale. The package var is restored on cleanup, so these tests
// must not run in parallel with each other.
func shrinkCleanupConsumedUtxosInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := cleanupConsumedUtxosInterval
	cleanupConsumedUtxosInterval = d
	t.Cleanup(func() { cleanupConsumedUtxosInterval = prev })
}

// newCleanupTimerFireSignal returns a hook that reports each timer fire on the
// returned channel. The send is non-blocking so an unread fire never stalls
// the timer callback itself.
func newCleanupTimerFireSignal() (func(), <-chan struct{}) {
	fires := make(chan struct{}, 64)
	return func() {
		select {
		case fires <- struct{}{}:
		default:
		}
	}, fires
}

// drainCleanupTimerFires discards every fire already buffered. Close has
// stopped and drained the timer by the time this is called, so all of them
// happened before it returned; only a fire arriving afterwards is a defect.
// Draining fully -- rather than discarding a single fire -- is what keeps the
// absence assertion from depending on how many intervals elapsed while the
// test was between receives.
func drainCleanupTimerFires(fires <-chan struct{}) {
	for {
		select {
		case <-fires:
		default:
			return
		}
	}
}

// TestCleanupConsumedUtxos_TimerStopsOnClose covers the first half of issue
// #3439: the cleanup timer callback re-arms itself via
// scheduleCleanupConsumedUtxos, so a Close that does not stop it leaves a
// self-perpetuating timer running against a database its owner closes
// immediately after Close returns (LedgerState does not own the database --
// see the note at the end of Close).
func TestCleanupConsumedUtxos_TimerStopsOnClose(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	ls.scheduleCleanupConsumedUtxos()
	// Two fires prove the callback re-armed itself at least once, so the
	// absence check below is measuring a stopped timer rather than one that
	// simply never started.
	testutil.RequireReceive(
		t, fires, 5*time.Second,
		"cleanup timer must fire while the ledger state is open",
	)
	testutil.RequireReceive(
		t, fires, 5*time.Second,
		"cleanup timer must re-arm itself while the ledger state is open",
	)

	require.NoError(t, ls.Close())

	drainCleanupTimerFires(fires)

	// 200ms is ~40 shrunken intervals. The assertion is that a stopped timer
	// fires zero times, not that a running one fires within a deadline, so
	// runner load cannot turn this into a flake.
	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"cleanup timer must not fire after Close returns",
	)
}

// TestCleanupConsumedUtxos_CloseWaitsForActiveCallback covers the drain half
// of the acceptance criteria. Stopping a time.Timer does not wait for an
// AfterFunc callback that has already started, so Close must join the
// in-flight run; otherwise it returns while cleanup is still issuing database
// work, and the owner closes the database out from under it.
func TestCleanupConsumedUtxos_CloseWaitsForActiveCallback(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	// The run hook, not the timer-fired hook: this must block inside the
	// region that has already registered with the drain, which is what Close
	// is required to wait for.
	ls.cleanupConsumedUtxosRunHook = func() {
		once.Do(func() {
			close(entered)
			<-release
		})
	}

	ls.scheduleCleanupConsumedUtxos()
	testutil.RequireReceive(
		t, entered, 5*time.Second,
		"cleanup timer callback must start before Close is called",
	)

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- ls.Close() }()

	testutil.RequireNoReceive(
		t, closeReturned, 200*time.Millisecond,
		"Close must not return while a cleanup callback is in flight",
	)

	close(release)
	err := testutil.RequireReceive(
		t, closeReturned, 10*time.Second,
		"Close must return once the in-flight cleanup callback finishes",
	)
	require.NoError(t, err)
}

// TestCleanupConsumedUtxos_NoDatabaseWorkAfterClose covers the second
// acceptance criterion for the path that has no timer at all: the epoch
// transition fires cleanup as a bare `go ls.cleanupConsumedUtxos()`
// (state.go), which can lose the race with shutdown. Stopping the timer alone
// does not constrain that goroutine.
//
// TestCleanupConsumedUtxos_CoreModePrunes is the positive control: the same
// seeded row and tip are deleted by the same call on an open ledger state, so
// a passing result here cannot come from cleanup being inert.
func TestCleanupConsumedUtxos_NoDatabaseWorkAfterClose(t *testing.T) {
	db := newTestDBForCleanup(t, types.StorageModeCore)
	txId := bytes.Repeat([]byte{0xC5}, 32)
	const (
		addedSlot   uint64 = 1_000
		deletedSlot uint64 = 5_000
		tipSlot     uint64 = 100_000 // > 50_000 default stability window
	)
	seedSpentUtxoForCleanup(t, db, txId, 0, addedSlot, deletedSlot)

	ls := newLedgerStateForCleanup(db, tipSlot)
	require.NoError(t, ls.Close())

	ls.cleanupConsumedUtxos()

	post, err := db.Metadata().GetUtxoIncludingSpent(txId, 0, nil)
	require.NoError(t, err)
	assert.NotNil(
		t, post,
		"cleanup must not begin database work after Close returns",
	)
}

// TestCleanupConsumedUtxos_RepeatedCloseIsSafe covers the repeated-close half
// of the third acceptance criterion. A drain built on sync.WaitGroup is easy
// to get wrong on the second call, and Close is genuinely called twice on the
// live restore/truncate path.
func TestCleanupConsumedUtxos_RepeatedCloseIsSafe(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	ls.scheduleCleanupConsumedUtxos()
	testutil.RequireReceive(
		t, fires, 5*time.Second,
		"cleanup timer must fire while the ledger state is open",
	)

	require.NoError(t, ls.Close())
	require.NoError(t, ls.Close(), "repeated Close must remain a no-op")

	drainCleanupTimerFires(fires)
	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"cleanup timer must stay stopped across repeated Close calls",
	)
}

// TestCleanupConsumedUtxos_ScheduleAfterCloseDoesNotArm covers the re-arm
// window directly: the timer callback calls scheduleCleanupConsumedUtxos
// after running cleanup, so a callback that was already in flight when Close
// stopped the timer would otherwise install a fresh one behind Close's back.
func TestCleanupConsumedUtxos_ScheduleAfterCloseDoesNotArm(t *testing.T) {
	shrinkCleanupConsumedUtxosInterval(t, 5*time.Millisecond)
	db := newTestDBForCleanup(t, types.StorageModeCore)
	ls := newLedgerStateForCleanup(db, 100_000)

	hook, fires := newCleanupTimerFireSignal()
	ls.cleanupConsumedUtxosTimerFiredHook = hook

	require.NoError(t, ls.Close())
	ls.scheduleCleanupConsumedUtxos()

	testutil.RequireNoReceive(
		t, fires, 200*time.Millisecond,
		"scheduling cleanup after Close must not arm a timer",
	)
}
