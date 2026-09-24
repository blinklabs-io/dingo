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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests pin recoverBlockfetchRestartFailureLocked, the recovery path
// for a failed restartQueuedBlockfetchAfterForkLocked call on the "fork
// extends from current tip" branch. Live Preview-testnet reports showed a
// rapid string of chain-selection reversals recycling the very connection
// this path was about to dispatch a blockfetch request against, logged as
// "failed to start blockfetch after fork extension" immediately followed by
// "failed to lookup connection ID" -- and, before this fix, the queued
// fork-extension headers were then left with nothing ever scheduled to fetch
// their bodies, a silent pipeline stall (continuous reselection, zero
// block-apply progress). Without this recovery function, the caller
// (handleEventChainsyncBlockHeaderWithPending) only logged the failure.

func newForkExtensionRestartFixture(
	t *testing.T,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("fork-ext-hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	require.Equal(t, 1, testChain.HeaderCount())
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return ls, testChain
}

// TestRecoverBlockfetchRestartFailureRetriesLiveActiveConnection asserts the
// cheap path: when chain selection currently has a different, live
// connection active, the recovery retries the blockfetch restart against it
// instead of dropping the queue and requesting a re-sync.
func TestRecoverBlockfetchRestartFailureRetriesLiveActiveConnection(t *testing.T) {
	t.Parallel()

	ls, testChain := newForkExtensionRestartFixture(t)
	failedConn := testChainsyncConnId(6000, 3001)
	activeConn := testChainsyncConnId(6000, 3002)

	var requestedConn ouroboros.ConnectionId
	requestCount := 0
	ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &activeConn
	}
	ls.config.ConnectionLiveFunc = func(connId ouroboros.ConnectionId) bool {
		return connId == activeConn
	}
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		requestedConn = connId
		return 0, nil
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			resyncCh <- e
		},
	)
	t.Cleanup(func() { bus.Unsubscribe(event.ChainsyncResyncEventType, subId) })

	var pending pendingPublishes
	ls.chainsyncBlockfetchMutex.Lock()
	ls.recoverBlockfetchRestartFailureLocked(
		failedConn,
		assertAnError,
		&pending,
	)
	ls.chainsyncBlockfetchMutex.Unlock()
	pending.flush()

	assert.Equal(t, 1, requestCount,
		"must retry the blockfetch request exactly once, against the live active connection")
	assert.Equal(t, activeConn, requestedConn)
	assert.Equal(t, activeConn, ls.activeBlockfetchConnId)
	assert.Equal(t, activeConn, ls.selectedBlockfetchConnId)
	assert.Equal(t, 1, testChain.HeaderCount(),
		"queued fork-extension headers must survive a successful retry")

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"a successful retry on a live connection must not request a re-sync",
	)
}

// TestRecoverBlockfetchRestartFailureFallsBackToResyncWithoutLiveConnection
// asserts the safety-net path: when chain selection has no live alternative
// (e.g. GetActiveConnectionFunc is unset, or the corresponding
// mode has no configured active-connection callback), the recovery drops the
// stranded header queue and requests a chainsync re-sync rather than leaving
// the pipeline stalled with queued headers nothing will ever fetch.
func TestRecoverBlockfetchRestartFailureFallsBackToResyncWithoutLiveConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, testChain := newForkExtensionRestartFixture(t)
	failedConn := testChainsyncConnId(6000, 3001)
	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			resyncCh <- e
		},
	)
	t.Cleanup(func() { bus.Unsubscribe(event.ChainsyncResyncEventType, subId) })

	var pending pendingPublishes
	ls.chainsyncBlockfetchMutex.Lock()
	ls.recoverBlockfetchRestartFailureLocked(
		failedConn,
		assertAnError,
		&pending,
	)
	ls.chainsyncBlockfetchMutex.Unlock()
	pending.flush()

	assert.Equal(t, 0, requestCount,
		"no live alternative connection exists, so no retry request is made")
	assert.Equal(t, 0, testChain.HeaderCount(),
		"the stranded queue must be dropped rather than left with nothing to fetch it")

	resyncEvt := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"a chainsync re-sync must be requested",
	)
	assert.Equal(t, failedConn, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonForkExtensionRestartFailed,
		resyncEvt.Reason,
	)
}

// assertAnError is a stand-in restartQueuedBlockfetchAfterForkLocked error:
// the recovery function's behavior depends only on failedConn and the
// current wiring, never on the error's content beyond logging it.
var assertAnError = &testRestartError{}

type testRestartError struct{}

func (e *testRestartError) Error() string {
	return "synthetic restartQueuedBlockfetchAfterForkLocked failure"
}
