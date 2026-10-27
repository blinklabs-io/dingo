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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// holdNextBlock is the block that extends the fixture's current tip.
func holdNextBlock(f *blockfetchRollbackFixture) *blockfetchTestBlock {
	return &blockfetchTestBlock{
		hash:        f.forkAHash,
		prevHash:    lcommon.NewBlake2b256(f.currentTip.Point.Hash),
		slot:        f.currentTip.Point.Slot + 10,
		blockNumber: f.currentTip.BlockNumber + 1,
	}
}

func holdHeaderEvent(
	f *blockfetchRollbackFixture,
	b *blockfetchTestBlock,
) event.Event {
	point := ocommon.NewPoint(b.slot, b.hash.Bytes())
	return event.NewEvent(ChainsyncEventType, ChainsyncEvent{
		ConnectionId: f.connId,
		Point:        point,
		Type:         1,
		BlockHeader: mockHeader{
			hash:        b.hash,
			prevHash:    b.prevHash,
			blockNumber: b.blockNumber,
			slot:        b.slot,
		},
		Tip: ochainsync.Tip{Point: point, BlockNumber: b.blockNumber},
	})
}

func holdBlockEvent(
	f *blockfetchRollbackFixture,
	b *blockfetchTestBlock,
) event.Event {
	return event.NewEvent(BlockfetchEventType, BlockfetchEvent{
		ConnectionId: f.connId,
		Point:        ocommon.NewPoint(b.slot, b.hash.Bytes()),
		Type:         1,
		Block:        b,
	})
}

// A hold taken while a batch is in flight discards the queued headers and the
// batch, so a trusted batch applied meanwhile meets no queued header; events
// delivered while held, and the rest of the abandoned batch, store nothing.
func TestHoldNetworkIngestDropsQueuedEvents(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	next := holdNextBlock(f)

	release := f.ls.HoldNetworkIngest()
	defer release()
	assert.True(t, f.ls.NetworkIngestHeld())
	assert.Zero(t, f.ls.chain.HeaderCount(), "the hold clears queued headers")

	f.ls.handleEventChainsync(holdHeaderEvent(f, next))
	f.ls.handleEventChainsync(event.NewEvent(ChainsyncEventType, ChainsyncEvent{
		ConnectionId: f.connId,
		Rollback:     true,
		Point:        f.ancestorTip.Point,
	}))
	f.ls.handleEventBlockfetch(holdBlockEvent(f, next))
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
	))

	assert.Zero(t, f.ls.chain.HeaderCount(), "a held header must not queue")
	assert.Empty(t, f.ls.pendingBlockfetchEvents)
	assert.Equal(
		t,
		f.currentTip.Point,
		f.ls.chain.Tip().Point,
		"neither a held block nor a held rollback may move the chain",
	)
	assert.Len(t, f.requests, 1, "a held node must issue no blockfetch")
}

// Holds nest: ingest resumes when the last release runs, a release is safe to
// call twice, and only the last one publishes the resync.
func TestHoldNetworkIngestNests(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRollbackFixture(t)
	outer := f.ls.HoldNetworkIngest()
	inner := f.ls.HoldNetworkIngest()
	inner()
	inner()
	assert.True(t, f.ls.NetworkIngestHeld(), "the outer hold is still taken")
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		50*time.Millisecond,
		"an inner release must not resync",
	)
	outer()
	assert.False(t, f.ls.NetworkIngestHeld())
	testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the last release resyncs",
	)
	outer()
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		50*time.Millisecond,
		"a repeated release must not resync again",
	)
}

// After release every client re-intersects from the current chain tip, and
// the blocks after it are fetched and applied to the chain as usual.
func TestHoldReleaseResyncs(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRollbackFixture(t)
	next := holdNextBlock(f)

	release := f.ls.HoldNetworkIngest()
	f.ls.handleEventChainsync(holdHeaderEvent(f, next))
	require.Zero(t, f.ls.chain.HeaderCount())
	release()

	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		testutil.AsyncWait,
		"the release must request a resync",
	)
	assert.Equal(t, event.ChainsyncResyncReasonNetworkIngestReleased, resync.Reason)
	assert.Equal(
		t,
		f.ls.chain.Tip().Point,
		resync.Point,
		"clients re-intersect from the current chain tip",
	)
	assert.Equal(t, event.ChainsyncResyncEvent{}.ConnectionId, resync.ConnectionId,
		"every client re-intersects, not one connection")

	f.ls.handleEventChainsync(holdHeaderEvent(f, next))
	require.Equal(t, 1, f.ls.chain.HeaderCount(), "headers queue again")
	if len(f.requests) == 0 {
		require.NoError(t, startQueuedBlockfetchForTest(f.ls, f.connId, nil))
	}
	require.Len(t, f.requests, 1, "the block after the tip is fetched")
	f.ls.handleEventBlockfetch(holdBlockEvent(f, next))
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
	))
	assert.Equal(
		t,
		ocommon.NewPoint(next.slot, next.hash.Bytes()),
		f.ls.chain.Tip().Point,
		"the fetched block extends the chain",
	)
}

// A blockfetch start racing the hold can leave a batch current after the
// discard. Its blocks are still dropped while held.
func TestHoldNetworkIngestDropsRacingBatchBlocks(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRollbackFixture(t)
	next := holdNextBlock(f)
	release := f.ls.HoldNetworkIngest()
	defer release()

	f.ls.chainsyncBlockfetchMutex.Lock()
	f.ls.activeBlockfetchConnId = f.connId
	f.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	f.ls.chainsyncBlockfetchMutex.Unlock()
	f.ls.handleEventBlockfetch(holdBlockEvent(f, next))
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
	))

	assert.Empty(t, f.ls.pendingBlockfetchEvents)
	assert.Equal(t, f.currentTip.Point, f.ls.chain.Tip().Point,
		"a held node must store no block")
}

// Chain selection keeps running while held. A switch to a peer ahead of the
// chain with nothing queued would otherwise request a fresh cursor for it.
func TestHoldNetworkIngestIgnoresChainSwitch(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRollbackFixture(t)
	release := f.ls.HoldNetworkIngest()
	defer release()

	farTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100_000, []byte("far-tip")),
		BlockNumber: 10_000,
	}
	f.ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: testChainsyncConnId(6400, 3001),
			NewConnectionId:      f.connId,
			NewTip:               farTip,
			NewObservedTip:       farTip,
			NewObservedTipSet:    true,
		},
	))
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		100*time.Millisecond,
		"a held node must not request a fresh cursor on a chain switch",
	)
}

// After the release the closing resync is asynchronous, so a connection from
// the hold can still deliver from a cursor that ran ahead of, or sits behind,
// the externally applied tip. Until it closes its messages must not queue a
// header or roll the chain back to resolve a fork.
func TestHoldReleaseDropsHeldConnections(t *testing.T) {
	t.Parallel()
	f := newBlockfetchRollbackFixture(t)
	tracker := chainsync.NewState(nil, nil)
	require.True(t, tracker.AddClientConnId(f.connId))
	f.ls.config.NetworkIngestHoldTracker = tracker
	next := holdNextBlock(f)

	release := f.ls.HoldNetworkIngest()
	release()
	require.False(t, f.ls.NetworkIngestHeld())

	f.ls.handleEventChainsync(holdHeaderEvent(f, next))
	f.ls.handleEventChainsync(event.NewEvent(ChainsyncEventType, ChainsyncEvent{
		ConnectionId: f.connId,
		Rollback:     true,
		Point:        f.ancestorTip.Point,
	}))
	assert.Zero(t, f.ls.chain.HeaderCount(),
		"a connection from the hold must not queue a header")
	assert.Equal(t, f.currentTip.Point, f.ls.chain.Tip().Point,
		"a connection from the hold must not roll the chain back")

	// Once that connection has gone, its replacement is followed.
	tracker.RemoveClientConnId(f.connId)
	f.ls.handleEventChainsync(holdHeaderEvent(f, next))
	assert.Equal(t, 1, f.ls.chain.HeaderCount())
}
