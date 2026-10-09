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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
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
			Point:       ocommon.NewPoint(next.slot+100, testHashBytes("peer-tip")),
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
			Point:       ocommon.NewPoint(next.slot+100, testHashBytes("peer-tip-b")),
			BlockNumber: next.blockNumber + 100,
		},
	}
	err := fixture.ls.handleEventChainsyncBlockHeaderWithPending(evt, nil)
	require.ErrorIs(t, err, chain.ErrHeaderQueueFull)
	assert.Zero(t, requestCount)
	assert.Equal(t, inFlight, fixture.ls.chainsyncBlockfetchReadyChan)
}
