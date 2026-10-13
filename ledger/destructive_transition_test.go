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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

func TestRollbackWaitsForDestructiveTransitionBarrier(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	finish := fixture.ls.db.BeginDestructiveTransition()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(finish) }
	var wg sync.WaitGroup
	joinBeforeFixtureClose(t, &wg, release)

	rollbackDone := make(chan error, 1)
	wg.Go(func() {
		rollbackDone <- fixture.ls.rollbackChainAndStateDeferred(
			t.Context(),
			fixture.ancestorTip.Point,
			nil,
		)
	})

	// Both bounds here are deliberately generous rather than tuned. What this
	// test asserts is ordering -- parked before release, complete after -- and
	// every operation it waits on takes milliseconds when it works at all. A
	// tight bound adds only a timing failure mode under a race-instrumented
	// full-package run, where these tests share a machine and the package
	// takes minutes.
	const barrierWait = 30 * time.Second

	testutil.WaitForCondition(t, func() bool {
		return testutil.GoroutineParkedIn(
			"github.com/blinklabs-io/dingo/database.(*cancellableBarrier).lockContext",
			"github.com/blinklabs-io/dingo/ledger.(*LedgerState).rollbackChainAndStateDeferred",
		)
	}, barrierWait, "rollback must be parked on the destructive transition barrier")
	// The barrier, not scheduling luck, is what is holding the rollback: it
	// has entered rollbackChainAndStateDeferred and has not returned.
	testutil.RequireNoReceive(
		t,
		rollbackDone,
		100*time.Millisecond,
		"rollback completing before the destructive transition finished",
	)
	require.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())

	release()
	require.NoError(t, testutil.RequireReceive(
		t,
		rollbackDone,
		barrierWait,
		"rollback after destructive transition",
	))
}

// joinBeforeFixtureClose registers a cleanup that runs release and then waits
// for wg. Cleanups run last-registered first, so calling this after
// newChainsyncRollbackFixture makes it run before the fixture closes the
// ledger: a test that fails mid-wait must not leave a rollback or
// reconciliation running into LedgerState.Close.
func joinBeforeFixtureClose(t *testing.T, wg *sync.WaitGroup, release func()) {
	t.Helper()
	t.Cleanup(func() {
		release()
		joined := make(chan struct{})
		go func() {
			wg.Wait()
			close(joined)
		}()
		select {
		case <-joined:
		case <-time.After(testutil.AsyncWait):
			t.Error("rollback or reconciliation still running at cleanup")
		}
	})
}

func TestReconciliationTakesPruneLockBeforeDestructiveBarrier(t *testing.T) {
	// Not t.Parallel: both reconciliation tests inspect process-wide goroutine
	// dumps for identical frames and could satisfy each other's waits.
	fixture := newChainsyncRollbackFixture(t)
	// Hold the prune lock so reconciliation pauses before taking the
	// destructive barrier. Another destructive transition must still be able
	// to enter while reconciliation is waiting for the prune lock.
	fixture.ls.consumedUtxoPruneMutex.Lock()
	var releasePruneOnce sync.Once
	releasePrune := func() {
		releasePruneOnce.Do(fixture.ls.consumedUtxoPruneMutex.Unlock)
	}
	var wg sync.WaitGroup
	joinBeforeFixtureClose(t, &wg, releasePrune)

	reconcileDone := make(chan error, 1)
	wg.Go(func() {
		reconcileDone <- fixture.ls.reconcilePrimaryChainTipWithLedgerTip(t.Context())
	})
	testutil.WaitForCondition(
		t,
		func() bool {
			return testutil.GoroutineParkedIn(
				"sync.(*Mutex).Lock",
				"(*LedgerState).withConsumedUtxoPruneBoundary",
			)
		},
		testutil.AsyncWait,
		"reconciliation waiting for the prune lock",
	)

	barrierEntered := make(chan error, 1)
	wg.Go(func() {
		finish, err := fixture.ls.db.BeginDestructiveTransitionContext(
			t.Context(),
		)
		if err == nil {
			finish()
		}
		barrierEntered <- err
	})
	require.NoError(t, testutil.RequireReceive(
		t,
		barrierEntered,
		testutil.AsyncWait,
		"reconciliation must not hold the destructive barrier while waiting for the prune lock",
	))

	releasePrune()
	require.NoError(t, testutil.RequireReceive(
		t,
		reconcileDone,
		testutil.AsyncWait,
		"reconciliation after the prune lock is released",
	))
}

func TestReconciliationWaitsForDestructiveTransitionBarrier(t *testing.T) {
	// Not t.Parallel: both reconciliation tests inspect process-wide goroutine
	// dumps for identical frames and could satisfy each other's waits.
	fixture := newChainsyncRollbackFixture(t)
	require.NoError(
		t,
		fixture.ls.chain.Rollback(t.Context(), fixture.ancestorTip.Point),
	)
	finish := fixture.ls.db.BeginDestructiveTransition()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(finish) }
	var wg sync.WaitGroup
	joinBeforeFixtureClose(t, &wg, release)

	reconcileDone := make(chan error, 1)
	wg.Go(func() {
		reconcileDone <- fixture.ls.reconcilePrimaryChainTipWithLedgerTip(t.Context())
	})
	testutil.WaitForCondition(
		t,
		func() bool {
			return testutil.GoroutineParkedIn(
				"database.(*cancellableBarrier).lockContext",
				"(*LedgerState).withDestructiveDatabaseTransition",
			)
		},
		testutil.AsyncWait,
		"reconciliation waiting for the destructive transition barrier",
	)
	testutil.RequireNoReceive(
		t,
		reconcileDone,
		100*time.Millisecond,
		"reconciliation finishing before the destructive transition",
	)
	require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	release()
	require.NoError(t, testutil.RequireReceive(
		t,
		reconcileDone,
		testutil.AsyncWait,
		"reconciliation after the destructive transition finishes",
	))
	require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
}
