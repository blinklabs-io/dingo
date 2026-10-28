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

package ouroboros

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// addObserver tracks another observer on the fixture's node.
func (f promotedObserverFixture) addObserver(
	t *testing.T,
	local string,
) ouroboros.ConnectionId {
	t.Helper()
	conn := newTestConnId(local, "10.0.0.31:3001")
	require.True(
		t,
		f.o.chainsyncState.TryAddObservedClientConnIdWithDirection(conn, false),
	)
	return conn
}

func (f promotedObserverFixture) rollForwardOn(
	t *testing.T,
	conn ouroboros.ConnectionId,
	block uint64,
	hashByte byte,
) {
	t.Helper()
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		newTestBlockHeader(block, block, hashByte),
		peerTipAt(900),
	))
}

// While the ledger lags, every observer streams above the applied tip, so
// each one a freed ingress slot promotes is above it. Re-syncing each of them
// closes one connection per promotion and the next observer takes the slot
// again: a cascade through every connected peer. Only one promoted observer
// is re-synced per window; the others stay observers.
func TestChainsyncPromotedObserversAboveLedgerAreNotResyncedInACascade(
	t *testing.T,
) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	second := f.addObserver(t, "127.0.0.1:6031")

	f.rollForwardOn(t, f.conn, 500, 0xb1)
	first := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the first promoted observer above the ledger must be re-synced",
	)
	data, ok := first.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonPromotedObserverAhead,
		data.Reason,
	)

	f.rollForwardOn(t, second, 501, 0xb2)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		300*time.Millisecond,
		"a second promoted observer above the ledger must not be re-synced in the same window",
	)
	observabilityOnly, tracked := f.o.chainsyncState.ClientObservabilityOnly(
		second,
	)
	require.True(t, tracked)
	require.True(
		t,
		observabilityOnly,
		"an observer whose promotion was not re-synced must not keep an ingress slot",
	)
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		200*time.Millisecond,
		"the header of an observer left unpromoted must not be recorded",
	)
}

// The window only spaces re-syncs. Once it has elapsed, the next promoted
// observer above the ledger is re-synced from the local tip as before, so an
// observer cannot keep an ingress slot at a cursor above the ledger.
func TestChainsyncPromotedObserverAboveLedgerIsResyncedOnceWindowElapses(
	t *testing.T,
) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	second := f.addObserver(t, "127.0.0.1:6032")

	f.rollForwardOn(t, f.conn, 500, 0xc1)
	testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the first promoted observer above the ledger must be re-synced",
	)
	f.o.promotedAheadRecycleMu.Lock()
	f.o.promotedAheadRecycleAt = time.Now().Add(
		-chainsyncPromotedAheadRecycleCooldown,
	)
	f.o.promotedAheadRecycleMu.Unlock()

	f.rollForwardOn(t, second, 501, 0xc2)
	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"a promoted observer above the ledger must be re-synced once the window elapses",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, second, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonPromotedObserverAhead,
		data.Reason,
	)
}

// An observer left unpromoted by the window that then continues the local
// chain is promoted: the window never blocks an observer that needs no
// fresh intersection.
func TestChainsyncPromotedObserverAtLedgerTipIsPromotedInsideWindow(
	t *testing.T,
) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	second := f.addObserver(t, "127.0.0.1:6033")

	f.rollForwardOn(t, f.conn, 500, 0xd1)
	testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the first promoted observer above the ledger must be re-synced",
	)

	f.rollForwardOn(t, second, 11, 0xd2)
	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"an observer continuing the local chain must be promoted and recorded",
	)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"an observer at the ledger tip needs no fresh intersection",
	)
	observabilityOnly, tracked := f.o.chainsyncState.ClientObservabilityOnly(
		second,
	)
	require.True(t, tracked)
	require.False(t, observabilityOnly)
}

// The window is taken before the callback's capacity wait. When that wait
// fails no re-sync is scheduled, so the window must not stay held: the next
// promoted observer above the ledger is still re-synced.
func TestChainsyncPromotedAheadWindowIsReleasedWithoutAResync(t *testing.T) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	second := f.addObserver(t, "127.0.0.1:6034")
	f.o.chainsyncHeaderBackpressure = func(context.Context, int) error {
		return errors.New("capacity wait failed")
	}
	require.Error(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		newTestBlockHeader(500, 500, 0xe1),
		peerTipAt(900),
	))
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"a failed capacity wait schedules no re-sync",
	)

	f.o.chainsyncHeaderBackpressure = nil
	f.rollForwardOn(t, second, 501, 0xe2)
	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the window must be free for the next promoted observer above the ledger",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, second, data.ConnectionId)
}

// When every ingress slot is taken the observer cannot be promoted, so no
// re-sync is due and the window it claimed must be returned: once a slot
// frees, the next promoted observer above the ledger is re-synced.
func TestChainsyncPromotedAheadWindowIsReleasedWhenPromotionFails(
	t *testing.T,
) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	second := f.addObserver(t, "127.0.0.1:6035")
	holders := make([]ouroboros.ConnectionId, 0, 3)
	for i, port := range []string{"6036", "6037", "6038"} {
		holder := newTestConnId("127.0.0.1:"+port, "10.0.0.32:3001")
		require.True(
			t,
			f.o.chainsyncState.AddClientConnId(holder),
			"holder %d must take an ingress slot",
			i,
		)
		holders = append(holders, holder)
	}

	f.rollForwardOn(t, f.conn, 500, 0xf1)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"an observer that cannot be promoted needs no re-sync",
	)
	observabilityOnly, tracked := f.o.chainsyncState.ClientObservabilityOnly(
		f.conn,
	)
	require.True(t, tracked)
	require.True(t, observabilityOnly)

	f.o.chainsyncState.RemoveClientConnId(holders[0])
	f.rollForwardOn(t, second, 501, 0xf2)
	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"a failed promotion must not hold the window",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, second, data.ConnectionId)
}

// A re-sync that could not be scheduled returns the window as well.
func TestChainsyncPromotedAheadWindowIsReleasedWhenResyncIsNotScheduled(
	t *testing.T,
) {
	t.Parallel()

	f := newPromotedObserverFixture(t, true)
	second := f.addObserver(t, "127.0.0.1:6039")
	schedule := f.o.chainsyncScheduleAt
	f.o.chainsyncScheduleAt = nil
	f.rollForwardOn(t, f.conn, 500, 0xa5)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"nothing can be scheduled without a scheduler",
	)

	f.o.chainsyncScheduleAt = schedule
	f.rollForwardOn(t, second, 501, 0xa6)
	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"a re-sync that was not scheduled must not hold the window",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, second, data.ConnectionId)
}
