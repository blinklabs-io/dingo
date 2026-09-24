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
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// requestIdTestLedger builds a deep catch-up ledger whose
// BlockfetchRequestRangeFunc numbers requests from 1 per call, the way
// gouroboros numbers RequestRange calls per connection, and signals each
// dispatch on the returned channel.
func requestIdTestLedger(
	t *testing.T,
	connId ouroboros.ConnectionId,
) (*LedgerState, <-chan uint64, []lcommon.Blake2b256) {
	t.Helper()
	headerCount := BlockfetchBatchSize + 50
	testChain, hashes := buildDeepCatchupChain(t, headerCount)
	dispatched := make(chan uint64, 16)
	var mu sync.Mutex
	var nextId uint64
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				require.Equal(t, connId, gotConnId)
				mu.Lock()
				nextId++
				id := nextId
				mu.Unlock()
				dispatched <- id
				return id, nil
			},
		},
	}
	// Mithril-covered so delivered blocks skip header crypto verification;
	// these synthetic blocks carry no VRF/KES material.
	ls.mithrilLedgerSlot = uint64(headerCount)
	ls.publishSnapshotsLocked()
	return ls, dispatched, hashes
}

func receiveDispatches(
	t *testing.T,
	dispatched <-chan uint64,
	want ...uint64,
) {
	t.Helper()
	for _, id := range want {
		got := testutil.RequireReceive(
			t,
			dispatched,
			testutil.AsyncWait,
			"expected blockfetch dispatch did not happen",
		)
		require.Equal(t, id, got)
	}
}

func blockfetchReservationsLocked(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
) int {
	return len(ls.blockfetchRequestsInFlight[connIdKey(connId)])
}

// TestBlockfetchRefusedPromotionKeepsQueuedRequestReserved reproduces the
// refused-promotion race: the active batch's BatchDone releases its own
// reservation, promotion refuses the stale pre-queued request, and the
// cleanup that follows must not release the pre-queued request's reservation
// in its place. That request is still outstanding with the peer, so a
// same-connection redispatch must wait for its terminal event, and every
// later terminal event must release exactly its own request.
func TestBlockfetchRefusedPromotionKeepsQueuedRequestReserved(
	t *testing.T,
) {
	t.Parallel()

	connId := testChainsyncConnId(6320, 3001)
	ls, dispatched, _ := requestIdTestLedger(t, connId)

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchLocked(connId, nil))
	require.NotNil(t, ls.nextBlockfetchRequest)
	require.Equal(t, 2, blockfetchReservationsLocked(ls, connId))
	ls.chainsyncBlockfetchMutex.Unlock()
	receiveDispatches(t, dispatched, 1, 2)

	// A rollback after request 2 was pre-queued makes it stale, so promotion
	// refuses it once request 1 completes having applied a block.
	ls.blockfetchRollbackGeneration.Add(1)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.batchBlocksApplied = 1
	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
		RequestId:    1,
	}, nil))
	require.Nil(t, ls.nextBlockfetchRequest)
	// Checked before releasing the mutex, so the continuation worker cannot
	// have dispatched yet: request 2 is outstanding and must stay reserved.
	assert.Equal(
		t,
		1,
		blockfetchReservationsLocked(ls, connId),
		"refused promotion released the still-outstanding pre-queued "+
			"request's reservation",
	)
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.RequireNoReceive(
		t,
		dispatched,
		50*time.Millisecond,
		"redispatched on the connection while pre-queued request 2 was "+
			"still outstanding",
	)

	// Request 2's own terminal event drains it and unblocks the redispatch.
	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
		RequestId:    2,
	}, nil))
	ls.chainsyncBlockfetchMutex.Unlock()
	receiveDispatches(t, dispatched, 3, 4)
	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()

	ls.chainsyncBlockfetchMutex.Lock()
	assert.Equal(
		t,
		2,
		blockfetchReservationsLocked(ls, connId),
		"requests 3 and 4 are outstanding and must each hold a reservation",
	)
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestBlockfetchLateTerminalEventReleasesOnlyItsOwnRequest covers the
// force-release side: a timeout teardown releases the active and pre-queued
// requests' reservations while both are still outstanding with the peer and
// redispatches on the same connection. Their late terminal events must not
// release the replacement requests' reservations, and the timed-out active
// request's terminal event must not open the discard latch armed for the
// pre-queued request, whose blocks would then be accepted as the
// replacement batch's.
func TestBlockfetchLateTerminalEventReleasesOnlyItsOwnRequest(
	t *testing.T,
) {
	t.Parallel()

	connId := testChainsyncConnId(6321, 3001)
	ls, dispatched, hashes := requestIdTestLedger(t, connId)

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchLocked(connId, nil))
	ls.chainsyncBlockfetchMutex.Unlock()
	receiveDispatches(t, dispatched, 1, 2)

	handleBlockfetchTimeoutForTest(ls, connId, nil)
	receiveDispatches(t, dispatched, 3, 4)

	ls.chainsyncBlockfetchMutex.Lock()
	require.Equal(t, 2, blockfetchReservationsLocked(ls, connId))
	replacement := append(
		[]chan struct{}(nil),
		ls.blockfetchRequestsInFlight[connIdKey(connId)]...,
	)
	for _, id := range []uint64{1, 2} {
		require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
			ConnectionId: connId,
			BatchDone:    true,
			RequestId:    id,
		}, nil))
		assert.Equal(
			t,
			2,
			blockfetchReservationsLocked(ls, connId),
			"late terminal event for force-released request %d released "+
				"a replacement request's reservation",
			id,
		)
		if id != 1 {
			continue
		}
		// Request 2 is still streaming: its block must be dropped, not
		// buffered into the replacement batch.
		require.NoError(t, ls.handleEventBlockfetchBlockDeferred(
			BlockfetchEvent{
				ConnectionId: connId,
				RequestId:    2,
				Point:        ocommon.NewPoint(1, hashes[0].Bytes()),
				Block: &blockfetchTestBlock{
					hash:        hashes[0],
					prevHash:    lcommon.NewBlake2b256(nil),
					slot:        1,
					blockNumber: 1,
				},
			},
			nil,
		))
		assert.Empty(
			t,
			ls.pendingBlockfetchEvents,
			"a block from the abandoned pre-queued request was accepted "+
				"after the timed-out request's terminal event",
		)
	}
	for i, done := range replacement {
		select {
		case <-done:
			t.Errorf("replacement request %d was released early", i+3)
		default:
		}
	}
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestHandleConnectionClosedReleasesEveryPipelinedRequest pins the close
// path to its contract: a connection close is the terminal barrier for every
// request on it. A connection that no longer owns the active batch can still
// hold more than one reservation (a released-but-outstanding pre-queued
// request and the replacement dispatched after it), and the close must not
// leave any of them for a later same-connection dispatch to wait on.
func TestHandleConnectionClosedReleasesEveryPipelinedRequest(t *testing.T) {
	t.Parallel()

	connId := testChainsyncConnId(6322, 3001)
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.chainsyncBlockfetchMutex.Lock()
	reserved := []chan struct{}{
		ls.beginBlockfetchRequestLocked(connId),
		ls.beginBlockfetchRequestLocked(connId),
	}
	ls.bindBlockfetchRequestIdLocked(connId, reserved[0], 1)
	ls.bindBlockfetchRequestIdLocked(connId, reserved[1], 2)
	ls.chainsyncBlockfetchMutex.Unlock()

	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: connId},
	))

	ls.chainsyncBlockfetchMutex.Lock()
	assert.NotContains(t, ls.blockfetchRequestsInFlight, connIdKey(connId))
	ls.chainsyncBlockfetchMutex.Unlock()
	for i, done := range reserved {
		select {
		case <-done:
		default:
			t.Errorf("connection close left request %d reserved", i+1)
		}
	}
}
