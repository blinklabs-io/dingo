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

package bark

import (
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// newTestBark builds a Bark wired to db, with no listening server (Start is
// never called), for exercising Acquire/PauseDB/ResumeDB directly.
func newTestBark(t *testing.T, db *database.Database) *Bark {
	t.Helper()
	b, err := NewBark(BarkConfig{DB: db, Port: 1})
	require.NoError(t, err)
	return b
}

// TestAcquireReturnsCurrentDB verifies the ordinary, uncontended path: a
// database was set at construction time, so Acquire hands it back with a
// working release func.
func TestAcquireReturnsCurrentDB(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	got, release, err := b.Acquire()
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Same(t, db, got)
	release()
}

// TestAcquireFailsFastWhilePaused guards against comment-34's original bug:
// a live Restore/Truncate closes the old *database.Database out from under
// any in-flight Bark request that's still holding a stale pointer to it —
// anywhere from a confusing internal error (sqlite) to an outright panic
// (Badger). PauseDB must make new Acquire calls fail immediately with
// ErrDBUnavailable, rather than either blocking (which would stall a
// request for the whole restore/truncate duration) or handing out a
// pointer that's about to be closed.
func TestAcquireFailsFastWhilePaused(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	paused := make(chan struct{})
	go func() {
		b.PauseDB()
		close(paused)
	}()
	testutil.RequireReceive(t, paused, time.Second, "PauseDB should complete "+
		"immediately when there is no in-flight Acquire holding the gate")

	_, release, err := b.Acquire()
	require.ErrorIs(t, err, ErrDBUnavailable)
	require.Nil(t, release)
}

// TestPauseDBWaitsForInFlightAcquire verifies PauseDB doesn't return — and
// therefore a caller closing the database it guards doesn't proceed — until
// every Acquire holder in flight when it was called has released.
func TestPauseDBWaitsForInFlightAcquire(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	_, release, err := b.Acquire()
	require.NoError(t, err)

	pauseDone := make(chan struct{})
	go func() {
		b.PauseDB()
		close(pauseDone)
	}()

	testutil.RequireNoReceive(t, pauseDone, 150*time.Millisecond,
		"PauseDB must wait for the in-flight Acquire to release before "+
			"closing the pause gate")

	release()
	testutil.RequireReceive(t, pauseDone, time.Second, "PauseDB should "+
		"complete promptly once the in-flight Acquire releases")
}

// TestResumeDBPublishesNewDBAndUnpauses verifies ResumeDB both republishes
// a (possibly different) *database.Database for future Acquire calls and
// releases the pause PauseDB put in place.
func TestResumeDBPublishesNewDBAndUnpauses(t *testing.T) {
	oldDB := newTestDB(t)
	newDB := newTestDB(t)
	b := newTestBark(t, oldDB)

	b.PauseDB()
	_, _, err := b.Acquire()
	require.ErrorIs(t, err, ErrDBUnavailable)

	b.ResumeDB(newDB)

	got, release, err := b.Acquire()
	require.NoError(t, err)
	require.Same(t, newDB, got)
	release()
}

// TestAddrClearsAfterStop guards against comment-46's original bug: Stop
// (and the ctx-cancellation-triggered auto-shutdown goroutine Start
// starts) reset b.server to nil but left b.listenerAddr pointing at the
// now-closed listener's address, so Addr() kept returning that stale,
// no-longer-valid address after the server had actually stopped — instead
// of "", the same as before Start was ever called. A caller polling Addr()
// to tell "is bark actually listening right now" (or reusing it after a
// stop/restart cycle) could be misled into believing a dead address was
// still live.
func TestAddrClearsAfterStop(t *testing.T) {
	db := newTestDB(t)
	b, err := NewBark(BarkConfig{DB: db, Host: "127.0.0.1", Port: freeTCPPort(t)})
	require.NoError(t, err)
	require.Empty(t, b.Addr(), "Addr must be empty before Start is ever called")

	require.NoError(t, b.Start(context.Background()))
	require.NotEmpty(t, b.Addr(), "Addr must be populated once Start has bound the listener")

	require.NoError(t, b.Stop(context.Background()))
	require.Empty(t, b.Addr(), "Addr must be cleared, not stale, once the server has stopped")
}

// TestAddrClearsWhenStartContextIsCancelled is TestAddrClearsAfterStop's
// counterpart for the OTHER shutdown path: Start's own ctx being
// cancelled directly (not a separate call to Stop) triggers the same
// auto-shutdown goroutine, which must clear listenerAddr the same way.
func TestAddrClearsWhenStartContextIsCancelled(t *testing.T) {
	db := newTestDB(t)
	b, err := NewBark(BarkConfig{DB: db, Host: "127.0.0.1", Port: freeTCPPort(t)})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, b.Start(ctx))
	require.NotEmpty(t, b.Addr())

	cancel()
	testutil.WaitForCondition(t, func() bool {
		return b.Addr() == ""
	}, 5*time.Second, "Addr must clear once Start's ctx cancellation finishes shutting the server down")
}
