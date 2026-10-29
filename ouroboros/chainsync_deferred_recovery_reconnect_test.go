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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// addSession tracks another connection to remote, as peer governance opens
// after closing the previous one.
func (f deferredHeaderRecoveryFixture) addSession(
	t *testing.T,
	local string,
	remote string,
) ouroboros.ConnectionId {
	t.Helper()
	conn := newTestConnId(local, remote)
	require.True(t, f.state.AddClientConnId(conn))
	return conn
}

// deferHeaderOn delivers a header that the fixture defers on conn.
func (f deferredHeaderRecoveryFixture) deferHeaderOn(
	t *testing.T,
	conn ouroboros.ConnectionId,
	hash byte,
	prevHash byte,
	slot uint64,
	blockNumber uint64,
) ochainsync.Tip {
	t.Helper()
	header := nonByronTestHeader{&testBlockHeader{
		hash:        gledger.Blake2b256{hash},
		prevHash:    gledger.Blake2b256{prevHash},
		slotNumber:  slot,
		blockNumber: blockNumber,
	}}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, header.Hash().Bytes()),
		BlockNumber: blockNumber,
	}
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	))
	require.Contains(t, f.o.deferredHeaderRecovery, conn)
	return tip
}

// closeSession drops the state a closed connection leaves behind.
func (f deferredHeaderRecoveryFixture) closeSession(
	conn ouroboros.ConnectionId,
) {
	f.o.clearDeferredHeaderRecovery(conn)
	f.o.cancelFutureHeaderResync(conn)
}

// A peer whose headers stay deferred is re-intersected once. Its reconnected
// session delivers the same headers, defers them again, and the next applied
// block would re-intersect it again: a close and reconnect for every block
// the lagging ledger applies, none of which can change the verdict.
func TestDeferredHeaderRecoveryDoesNotRecycleReconnectedPeerEveryBlock(
	t *testing.T,
) {
	t.Parallel()

	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6041")
	f.deferHeader(t, 0xef, 0x01, 200, 1)
	f.applyElsewhere(ocommon.NewPoint(300, []byte("applied-1")))
	first := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the first deferral must re-intersect the peer once",
	)
	data, ok := first.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		data.Reason,
	)
	f.closeSession(f.conn)

	reconnected := f.addSession(t, "127.0.0.1:6042", "1.1.1.6:3001")
	f.deferHeaderOn(t, reconnected, 0xef, 0x01, 200, 1)
	f.applyElsewhere(ocommon.NewPoint(301, []byte("applied-2")))

	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		300*time.Millisecond,
		"a reconnected peer whose headers defer again must not be re-intersected on the next block",
	)
	require.Contains(
		t,
		f.o.deferredHeaderRecovery,
		reconnected,
		"the withheld header stays parked so a later block can retry",
	)
}

// The window is per remote address and per recovery: a different peer that
// defers is re-intersected at once, as is the same peer once the window has
// elapsed and a later block arrives.
func TestDeferredHeaderRecoveryRecyclesOtherPeersAndRetriesAfterWindow(
	t *testing.T,
) {
	t.Parallel()

	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6051")
	f.deferHeader(t, 0xef, 0x01, 200, 1)
	f.applyElsewhere(ocommon.NewPoint(300, []byte("applied-1")))
	testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the first deferral must re-intersect the peer once",
	)
	f.closeSession(f.conn)

	other := f.addSession(t, "127.0.0.1:6052", "1.1.1.7:3001")
	f.deferHeaderOn(t, other, 0xef, 0x01, 200, 1)
	f.applyElsewhere(ocommon.NewPoint(301, []byte("applied-2")))
	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"a different peer's deferral must not wait on another peer's window",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, other, data.ConnectionId)
	f.closeSession(other)

	reconnected := f.addSession(t, "127.0.0.1:6053", "1.1.1.6:3001")
	f.deferHeaderOn(t, reconnected, 0xef, 0x01, 200, 1)
	f.applyElsewhere(ocommon.NewPoint(302, []byte("applied-3")))
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"the first peer is still inside its window",
	)

	f.o.deferredRecoveryRecycleMu.Lock()
	f.o.deferredRecoveryRecycleAt[reconnected.RemoteAddr.String()] = time.Now().
		Add(-chainsyncDeferredRecoveryRepeatCooldown)
	f.o.deferredRecoveryRecycleMu.Unlock()
	f.applyElsewhere(ocommon.NewPoint(303, []byte("applied-4")))
	resync = testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the parked deferral must be retried once the window has elapsed",
	)
	data, ok = resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, reconnected, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		data.Reason,
	)
}

// A recovery that scheduled nothing returns its window: the peer's retry on
// the next block is not refused for a re-intersection that never happened.
func TestDeferredHeaderRecoveryWindowIsReleasedWhenNothingIsScheduled(
	t *testing.T,
) {
	t.Parallel()

	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6061")
	schedule := f.o.chainsyncScheduleAt
	f.o.chainsyncScheduleAt = nil
	f.deferHeader(t, 0xef, 0x01, 200, 1)
	f.applyElsewhere(ocommon.NewPoint(300, []byte("applied-1")))
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"nothing can be scheduled without a scheduler",
	)
	require.Contains(
		t,
		f.o.deferredHeaderRecovery,
		f.conn,
		"an unscheduled recovery stays parked",
	)

	f.o.chainsyncScheduleAt = schedule
	f.applyElsewhere(ocommon.NewPoint(301, []byte("applied-2")))
	testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the window of an unscheduled recovery must not hold the retry",
	)
}

func (f deferredHeaderFixture) rollForwardOn(
	t *testing.T,
	conn ouroboros.ConnectionId,
	header gledger.BlockHeader,
) {
	t.Helper()
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	))
}

// With nothing queued the idle path re-intersects a withheld peer at once.
// That is also bounded per remote address: a reconnected session that defers
// the same header is not re-intersected again inside the window, and a window
// whose re-intersection was never scheduled is returned.
func TestDeferredHeaderIdleRecoveryIsLimitedPerRemoteAddress(t *testing.T) {
	t.Parallel()

	var localTip, forkParent, forkHash gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0] = 0x01, 0x03, 0x04
	f := newDeferredHeaderFixture(t, localTip)
	fork := nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	schedule := f.o.chainsyncScheduleAt
	f.o.chainsyncScheduleAt = nil

	// Nothing is scheduled, so the window is returned.
	f.rollForward(t, fork)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"nothing can be scheduled without a scheduler",
	)
	f.o.clearDeferredHeaderRecovery(f.conn)

	f.o.chainsyncScheduleAt = schedule
	second := newTestConnId("127.0.0.1:6071", "1.1.1.5:3001")
	require.True(t, f.state.AddClientConnId(second))
	f.rollForwardOn(t, second, fork)
	first := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the window of an unscheduled recovery must not hold the idle retry",
	)
	data, ok := first.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, second, data.ConnectionId)
	f.o.clearDeferredHeaderRecovery(second)
	f.o.cancelFutureHeaderResync(second)

	third := newTestConnId("127.0.0.1:6072", "1.1.1.5:3001")
	require.True(t, f.state.AddClientConnId(third))
	f.rollForwardOn(t, third, fork)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		300*time.Millisecond,
		"a reconnected peer that defers again must not be re-intersected inside the window",
	)
}
