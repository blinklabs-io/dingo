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

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The peer a batch is fetched from comes from the selection policy, asked
// about the last block of the range that is actually queued: a peer that
// announced the range start may have forked away before its end.
func TestSelectInitialBlockfetchConnAsksPolicyForQueuedRangeEnd(
	t *testing.T,
) {
	t.Parallel()

	testChain, hashes := buildDeepCatchupChain(t, 3)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	header := testChainsyncConnId(6000, 3001)
	better := testChainsyncConnId(6000, 3002)
	var gotOrigin ouroboros.ConnectionId
	var gotPoint ocommon.Point
	ls.config.SelectBlockfetchPeerFunc = func(
		origin ouroboros.ConnectionId,
		point ocommon.Point,
	) ouroboros.ConnectionId {
		gotOrigin, gotPoint = origin, point
		return better
	}

	selected := ls.selectInitialBlockfetchConn(header)

	assert.Equal(t, better, selected)
	assert.Equal(t, header, gotOrigin)
	assert.Equal(t, ocommon.NewPoint(3, hashes[2].Bytes()), gotPoint)
}

// In deep catch-up a promoted request normally tops the pipeline up on its own
// connection. When the policy prefers another peer for the next window, the
// pipeline is left empty so the promoted batch drains on its peer and the
// continuation after it hands the queue over.
func TestTryPromoteQueuedBlockfetchLeavesNextWindowToPreferredPeer(
	t *testing.T,
) {
	t.Parallel()

	const remainingHeaders = 80
	const promotedClaim = 50
	testChain, hashes := buildDeepCatchupChain(t, remainingHeaders)
	incumbent := testChainsyncConnId(6302, 3001)
	better := testChainsyncConnId(6302, 3002)
	var requests []ouroboros.ConnectionId
	var asked []ocommon.Point
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       incumbent,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requests = append(requests, connId)
				return uint64(len(requests)), nil
			},
			SelectBlockfetchPeerFunc: func(
				origin ouroboros.ConnectionId,
				point ocommon.Point,
			) ouroboros.ConnectionId {
				asked = append(asked, point)
				return better
			},
		},
	}
	ls.publishSnapshotsLocked()
	ls.nextBlockfetchRequest = &queuedBlockfetchRequest{
		connId:      incumbent,
		headerStart: ocommon.NewPoint(1, hashes[0].Bytes()),
		headerEnd: ocommon.NewPoint(
			promotedClaim,
			hashes[promotedClaim-1].Bytes(),
		),
		headerCount:  promotedClaim,
		dispatchedAt: time.Now(),
	}

	ls.chainsyncBlockfetchMutex.Lock()
	promoted := ls.tryPromoteQueuedBlockfetchLocked()
	next := ls.nextBlockfetchRequest
	ls.chainsyncBlockfetchMutex.Unlock()

	require.True(t, promoted)
	assert.Equal(t, incumbent, ls.activeBlockfetchConnId)
	assert.Empty(t, requests, "no top-up may be queued on the incumbent")
	assert.Nil(t, next)
	require.Len(t, asked, 1)
	assert.Equal(
		t,
		ocommon.NewPoint(remainingHeaders, hashes[remainingHeaders-1].Bytes()),
		asked[0],
		"the policy is asked about the window after the promoted claim",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

func TestSelectInitialBlockfetchConnWithoutPolicyKeepsHeaderPeer(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newForkExtensionRestartFixture(t)
	header := testChainsyncConnId(6000, 3001)

	assert.Equal(t, header, ls.selectInitialBlockfetchConn(header))
}

// The next batch of a drained queue is fetched from the peer the policy now
// prefers, and that peer stays selected for the batches after it.
func TestBatchDoneContinuationFetchesFromSelectedPeer(t *testing.T) {
	t.Parallel()

	ls, _ := newForkExtensionRestartFixture(t)
	incumbent := testChainsyncConnId(6000, 3001)
	better := testChainsyncConnId(6000, 3002)
	ls.activeBlockfetchConnId = incumbent
	ls.selectedBlockfetchConnId = incumbent
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.batchBlocksReceived = 1
	ls.batchBlocksApplied = 1
	ls.config.SelectBlockfetchPeerFunc = func(
		origin ouroboros.ConnectionId,
		start ocommon.Point,
	) ouroboros.ConnectionId {
		return better
	}
	var requested []ouroboros.ConnectionId
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requested = append(requested, connId)
		return 0, nil
	}

	require.NoError(t, handleEventBlockfetchBatchDoneForTest(
		ls,
		BlockfetchEvent{ConnectionId: incumbent, BatchDone: true},
		nil,
	))

	require.Equal(t, 1, len(requested))
	assert.Equal(t, better, requested[0])
	assert.Equal(t, better, ls.selectedBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

type throughputSample struct {
	connId  ouroboros.ConnectionId
	bytes   uint64
	elapsed time.Duration
}

// newThroughputFixture returns a ledger whose active batch is on conn, with
// every slot Mithril-covered so blocks are accepted without header crypto, and
// a recorder for the throughput samples.
func newThroughputFixture(
	t *testing.T,
) (*LedgerState, ouroboros.ConnectionId, *[]throughputSample) {
	t.Helper()
	ls, _ := newForkExtensionRestartFixture(t)
	conn := testChainsyncConnId(6000, 3001)
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = conn
	ls.selectedBlockfetchConnId = conn
	ls.mithrilLedgerSlot = 100
	ls.publishSnapshotsLocked()
	samples := &[]throughputSample{}
	ls.config.RecordBlockfetchThroughputFunc = func(
		connId ouroboros.ConnectionId,
		bytes uint64,
		elapsed time.Duration,
	) {
		*samples = append(*samples, throughputSample{connId, bytes, elapsed})
	}
	return ls, conn, samples
}

func deliverBlocks(
	t *testing.T,
	ls *LedgerState,
	conn ouroboros.ConnectionId,
	sizes ...int,
) {
	t.Helper()
	for i, size := range sizes {
		slot := uint64(i + 1)
		require.NoError(t, handleEventBlockfetchBlockDeferred(
			ls,
			BlockfetchEvent{
				ConnectionId: conn,
				Block:        &mockBabbageBlock{slot: slot},
				RawBlock:     make([]byte, size),
				Point: ocommon.Point{
					Slot: slot,
					Hash: []byte{byte(slot)},
				},
			},
			nil,
		))
	}
}

func TestRecordBatchDeliverySkipsMixedPeerSamples(t *testing.T) {
	t.Parallel()
	ls, conn, samples := newThroughputFixture(t)
	otherConn := testChainsyncConnId(6001, 3001)

	ls.noteBatchBlockArrivalLocked(conn, 100)
	ls.noteBatchBlockArrivalLocked(otherConn, 200)
	ls.batchFirstBlockAt = time.Now().Add(-time.Second)
	ls.batchLastBlockAt = time.Now()
	ls.recordBatchDeliveryLocked(otherConn)

	assert.Empty(t, *samples)
}

// A batch's rate is the bytes that followed its first block over the time
// they took, so the first block's own latency is not counted twice.
func TestBatchDoneRecordsDeliveryThroughput(t *testing.T) {
	t.Parallel()

	ls, conn, samples := newThroughputFixture(t)
	deliverBlocks(t, ls, conn, 100, 200, 300)
	// Model the blocks having been committed by a mid-batch flush: the mock
	// blocks cannot be applied to a chain, and what the batch does with
	// them afterwards is beside the point here.
	ls.pendingBlockfetchEvents = nil

	_ = handleEventBlockfetchBatchDoneForTest(
		ls,
		BlockfetchEvent{ConnectionId: conn, BatchDone: true},
		nil,
	)

	require.Equal(t, 1, len(*samples))
	assert.Equal(t, conn, (*samples)[0].connId)
	assert.Equal(t, uint64(500), (*samples)[0].bytes)
	assert.GreaterOrEqual(t, (*samples)[0].elapsed, time.Duration(0))
}

func TestBatchDoneWithOneBlockRecordsNoThroughput(t *testing.T) {
	t.Parallel()

	ls, conn, samples := newThroughputFixture(t)
	deliverBlocks(t, ls, conn, 100)
	ls.pendingBlockfetchEvents = nil

	_ = handleEventBlockfetchBatchDoneForTest(
		ls,
		BlockfetchEvent{ConnectionId: conn, BatchDone: true},
		nil,
	)

	assert.Equal(t, 0, len(*samples), "one block carries no delivery rate")
}
