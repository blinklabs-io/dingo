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
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// With no best peer, many honest peers can trail the local tip by more than k
// at once. Recycling all of them in one gap leaves no honest peer to select,
// so only one behind-local-tip recycle may be requested per cooldown window
// and the peer whose cursor is at the local tip is never recycled.
func TestMassBehindLocalTipRecycleKeepsSomeHonestPeers(t *testing.T) {
	t.Parallel()

	const (
		tipBlock     = uint64(10000)
		k            = uint64(432)
		honestPeers  = 13
		cursorBehind = uint64(500)
	)
	f := newForecastHorizonFixture(t, "10.0.0.1:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(tipBlock, k)
	f.setBestPeer(false)

	state := f.o.chainsyncState
	// The default client limit is below the peer count under test.
	addClient := func(remote string) ouroboros.ConnectionId {
		t.Helper()
		conn := newTestConnId("127.0.0.1:6020", remote)
		require.True(
			t,
			state.TryAddClientConnIdWithDirection(conn, honestPeers+2, true),
		)
		return conn
	}
	honestConns := make([]ouroboros.ConnectionId, 0, honestPeers)
	for i := range honestPeers {
		honestConns = append(
			honestConns,
			addClient(fmt.Sprintf("10.1.0.%d:3001", i+1)),
		)
	}
	forkConn := addClient("10.2.0.1:3001")

	deliver := func(
		conn ouroboros.ConnectionId,
		block uint64,
		tip ochainsync.Tip,
		b byte,
	) {
		t.Helper()
		require.NoError(t, f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			newTestBlockHeader(block, block, b),
			tip,
		))
	}
	honestTip := peerTipAt(tipBlock + 50000)
	for i, conn := range honestConns {
		deliver(conn, tipBlock-cursorBehind, honestTip, byte(0x10+i))
	}
	deliver(forkConn, tipBlock, peerTipAt(tipBlock+1), 0xf0)

	// The first recycle is the one the policy permits; wait for it, then
	// require that nothing else follows.
	first := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"a far-behind honest peer must still be recycled once",
	)
	data, ok := first.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, event.ChainsyncResyncReasonBehindLocalTip, data.Reason)
	require.NotEqual(t, forkConn, data.ConnectionId,
		"the peer whose cursor is at the local tip is not recycled")
	require.Contains(t, honestConns, data.ConnectionId)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		300*time.Millisecond,
		"other honest peers must not be recycled in the same window",
	)
}

// A connection whose re-intersection is already scheduled or fired is being
// recycled anyway. Its in-flight far-behind headers must not take the
// cross-connection window, or another stale peer waits a further cooldown
// with nothing recycled. Once the window lapses a second peer is recycled.
func TestBehindLocalTipRecycleWindowSkipsConnectionAlreadyResyncing(
	t *testing.T,
) {
	t.Parallel()

	const (
		tipBlock = uint64(10000)
		k        = uint64(432)
	)
	f := newForecastHorizonFixture(t, "10.0.0.1:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(tipBlock, k)
	f.setBestPeer(false)
	other := newTestConnId("127.0.0.1:6020", "10.1.0.1:3001")
	require.True(t, f.o.chainsyncState.AddClientConnId(other))

	deliver := func(conn ouroboros.ConnectionId, b byte) {
		t.Helper()
		block := tipBlock - 500
		require.NoError(t, f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			newTestBlockHeader(block, block, b),
			peerTipAt(tipBlock+50000),
		))
	}
	requireResyncOf := func(conn ouroboros.ConnectionId, msg string) {
		t.Helper()
		evt := testutil.RequireReceive(t, resyncCh, testutil.AsyncWait, msg)
		data, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		require.Equal(t, event.ChainsyncResyncReasonBehindLocalTip, data.Reason)
		require.Equal(t, conn, data.ConnectionId, msg)
	}

	deliver(f.conn, 0x01)
	requireResyncOf(f.conn, "the first far-behind peer is recycled")

	// The connection has not closed yet, and the window has lapsed.
	f.o.behindTipRecycleMu.Lock()
	f.o.behindTipRecycleAt = time.Now().Add(
		-chainsyncBehindTipRecycleCooldown - time.Second,
	)
	f.o.behindTipRecycleMu.Unlock()
	deliver(f.conn, 0x02)
	deliver(other, 0x03)
	requireResyncOf(other, "a second stale peer is recycled after the window")
}

// A far-behind header on a connection already gone from the connection
// manager schedules no re-intersection. It must not take the window either,
// or a live stale peer waits a full cooldown with nothing recycled.
func TestBehindLocalTipRecycleWindowNotSpentWhenNothingScheduled(
	t *testing.T,
) {
	t.Parallel()

	const (
		tipBlock = uint64(10000)
		k        = uint64(432)
	)
	f := newForecastHorizonFixture(t, "10.0.0.1:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(tipBlock, k)
	f.setBestPeer(false)

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: f.o.eventBus,
			Logger:   logger,
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})
	liveConn := newChainsyncResyncTestConnection(t, "10.1.0.1:3001")
	require.True(t, connManager.AddConnection(
		liveConn,
		false,
		"10.1.0.1:3001",
	))
	f.o.connManager = connManager
	live := liveConn.Id()
	require.True(t, f.o.chainsyncState.AddClientConnId(live))

	deliver := func(conn ouroboros.ConnectionId, b byte) {
		t.Helper()
		block := tipBlock - 500
		require.NoError(t, f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			newTestBlockHeader(block, block, b),
			peerTipAt(tipBlock+50000),
		))
	}

	// f.conn was never registered with the connection manager.
	deliver(f.conn, 0x01)
	deliver(live, 0x02)
	evt := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"the live stale peer is recycled in the same window",
	)
	data, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, event.ChainsyncResyncReasonBehindLocalTip, data.Reason)
	require.Equal(t, live, data.ConnectionId)
}

// A future-header recovery timer with a later onset does not recycle the
// connection yet, and every header until it fires is withheld. A far-behind
// header must replace it with an immediate re-intersection rather than leave
// the cursor stale until the later onset.
func TestBehindLocalTipRecycleReplacesLaterFutureHeaderTimer(t *testing.T) {
	t.Parallel()

	const (
		tipBlock = uint64(10000)
		k        = uint64(432)
	)
	f := newForecastHorizonFixture(t, "10.0.0.1:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(tipBlock, k)
	f.setBestPeer(false)

	f.o.scheduleFutureHeaderResync(f.conn, time.Now().Add(time.Hour))
	require.True(t, f.o.futureHeaderResyncPending(f.conn))

	block := tipBlock - 500
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		newTestBlockHeader(block, block, 0x01),
		peerTipAt(tipBlock+50000),
	))
	evt := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"the far-behind peer is recycled without waiting for the later timer",
	)
	data, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, event.ChainsyncResyncReasonBehindLocalTip, data.Reason)
	require.Equal(t, f.conn, data.ConnectionId)
}
