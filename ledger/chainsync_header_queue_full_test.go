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
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fillHeaderQueue queues headers onto the fixture's chain up to the queue's
// capacity and returns the last queued header's point and block number.
func fillHeaderQueue(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
) (ocommon.Point, uint64) {
	t.Helper()
	prevHash := fixture.currentTip.Point.Hash
	prevBlockNumber := fixture.currentTip.BlockNumber
	slot := fixture.currentTip.Point.Slot
	for i := range fixture.ls.chain.MaxQueuedHeaders() {
		slot++
		prevBlockNumber++
		h := mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("queue-full-%d", i)),
			),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: prevBlockNumber,
			slot:        slot,
		}
		require.NoError(
			t,
			fixture.ls.chain.AddBlockHeader(context.Background(), h),
		)
		prevHash = h.Hash().Bytes()
	}
	return ocommon.NewPoint(slot, prevHash), prevBlockNumber
}

// A full header queue with nothing fetching it rejects every later header
// before the handler can decide to start a fetch, so the node stops extending
// its chain while headers keep arriving. The header handler must start the
// fetch itself when it rejects a header for capacity.
func TestHeaderQueueFullWithIdleBlockfetchStartsFetch(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	queueTip, queueBlockNumber := fillHeaderQueue(t, fixture)
	require.Equal(
		t,
		fixture.ls.chain.MaxQueuedHeaders(),
		fixture.ls.chain.HeaderCount(),
	)
	require.Nil(t, fixture.ls.chainsyncBlockfetchReadyChan)

	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}

	connId := testChainsyncConnId(6301, 3001)
	next := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("queue-full-next")),
		prevHash:    lcommon.NewBlake2b256(queueTip.Hash),
		blockNumber: queueBlockNumber + 1,
		slot:        queueTip.Slot + 1,
	}
	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(next.slot, next.hash.Bytes()),
		BlockHeader:  next,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				next.slot+100,
				testHashBytes("peer-tip"),
			),
			BlockNumber: next.blockNumber + 100,
		},
	}
	err := fixture.ls.handleEventChainsyncBlockHeaderWithPending(evt, nil)
	require.ErrorIs(t, err, chain.ErrHeaderQueueFull)

	assert.Positive(
		t,
		requestCount,
		"a header rejected for queue capacity with no batch in flight must "+
			"start blockfetch for the queued headers; nothing else can, so "+
			"the queue never drains",
	)
	assert.NotNil(t, fixture.ls.chainsyncBlockfetchReadyChan)
}

// A header rejected for capacity while a batch is draining the queue must not
// disturb that batch.
func TestHeaderQueueFullWithBlockfetchInFlightDoesNotRestart(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	queueTip, queueBlockNumber := fillHeaderQueue(t, fixture)
	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}
	inFlight := make(chan struct{})
	fixture.ls.chainsyncBlockfetchReadyChan = inFlight

	next := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("queue-full-next-b")),
		prevHash:    lcommon.NewBlake2b256(queueTip.Hash),
		blockNumber: queueBlockNumber + 1,
		slot:        queueTip.Slot + 1,
	}
	evt := ChainsyncEvent{
		ConnectionId: testChainsyncConnId(6302, 3001),
		Point:        ocommon.NewPoint(next.slot, next.hash.Bytes()),
		BlockHeader:  next,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				next.slot+100,
				testHashBytes("peer-tip-b"),
			),
			BlockNumber: next.blockNumber + 100,
		},
	}
	err := fixture.ls.handleEventChainsyncBlockHeaderWithPending(evt, nil)
	require.ErrorIs(t, err, chain.ErrHeaderQueueFull)
	assert.Zero(t, requestCount)
	assert.Equal(t, inFlight, fixture.ls.chainsyncBlockfetchReadyChan)
}

// A blockfetch timeout whose retry fails with no alternate connection clears
// the batch but keeps the queued headers. With the queue at capacity, that is
// the idle full queue: every later header is rejected for capacity, so the
// next header must be the one that starts the fetch again.
func TestHeaderQueueFullAfterTimeoutRetryFailureResumesFetch(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	queueTip, queueBlockNumber := fillHeaderQueue(t, fixture)
	connId := testChainsyncConnId(6303, 3001)
	requestErr := errors.New("connection unavailable")
	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, requestErr
	}
	fixture.ls.activeBlockfetchConnId = connId
	fixture.ls.selectedBlockfetchConnId = connId
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})

	handleBlockfetchTimeoutForTest(fixture.ls, connId, nil)

	require.Positive(t, requestCount, "the timeout must attempt a retry")
	require.Nil(
		t,
		fixture.ls.chainsyncBlockfetchReadyChan,
		"a failed retry leaves no batch in flight",
	)
	require.False(t, fixture.ls.blockfetchContinuationPending)
	require.Equal(
		t,
		fixture.ls.chain.MaxQueuedHeaders(),
		fixture.ls.chain.HeaderCount(),
		"a failed retry with no alternate keeps the full queue",
	)

	requestErr = nil
	requestCount = 0
	next := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("queue-full-next-c")),
		prevHash:    lcommon.NewBlake2b256(queueTip.Hash),
		blockNumber: queueBlockNumber + 1,
		slot:        queueTip.Slot + 1,
	}
	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(next.slot, next.hash.Bytes()),
		BlockHeader:  next,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				next.slot+100,
				testHashBytes("peer-tip-c"),
			),
			BlockNumber: next.blockNumber + 100,
		},
	}
	err := fixture.ls.handleEventChainsyncBlockHeaderWithPending(evt, nil)
	require.ErrorIs(t, err, chain.ErrHeaderQueueFull)
	assert.Positive(
		t,
		requestCount,
		"the first header rejected after the failed retry must restart "+
			"blockfetch for the full queue",
	)
	assert.NotNil(t, fixture.ls.chainsyncBlockfetchReadyChan)
}

// When the fetch the capacity rejection starts cannot be dispatched, the full
// queue is dropped and a re-sync requested under a reason that names this
// path, not the fork-resolution path that shares the recovery.
func TestHeaderQueueFullRestartFailureRequestsResync(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	queueTip, queueBlockNumber := fillHeaderQueue(t, fixture)
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		return 0, errors.New("simulated blockfetch request failure")
	}
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	fixture.ls.config.EventBus = bus
	resyncSubID, resyncCh := bus.SubscribeWithBuffer(
		event.ChainsyncResyncEventType,
		4,
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, resyncSubID)
	})

	connId := testChainsyncConnId(6304, 3001)
	next := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("queue-full-next-d")),
		prevHash:    lcommon.NewBlake2b256(queueTip.Hash),
		blockNumber: queueBlockNumber + 1,
		slot:        queueTip.Slot + 1,
	}
	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(next.slot, next.hash.Bytes()),
		BlockHeader:  next,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				next.slot+100,
				testHashBytes("peer-tip-d"),
			),
			BlockNumber: next.blockNumber + 100,
		},
	}
	// nil pending publishes immediately, so the subscription observes the
	// re-sync request synchronously.
	err := fixture.ls.handleEventChainsyncBlockHeaderWithPending(evt, nil)
	require.ErrorIs(t, err, chain.ErrHeaderQueueFull)
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	resyncEvt := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait,
		"a failed restart for a full queue must request a re-sync",
	)
	data, ok := resyncEvt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok, "unexpected event payload %T", resyncEvt.Data)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonHeaderQueueFullRestartFailed,
		data.Reason,
	)
}
