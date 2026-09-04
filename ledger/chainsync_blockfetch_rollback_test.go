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
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

// syncSafeBuffer is a mutex-guarded log sink. The blockfetch continuation runs
// on its own worker goroutine, which logs concurrently with the test goroutine
// reading those logs back.
type syncSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncSafeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncSafeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// blockfetchTestBlock is a block body whose identity fields the test controls,
// so a body from an abandoned fork can be delivered through the real
// blockfetch subscriber. Only the fields the chain-insertion path reads are
// implemented; the rest of gledger.Block is embedded and never called.
type blockfetchTestBlock struct {
	gledger.Block
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	slot        uint64
	blockNumber uint64
}

func (b *blockfetchTestBlock) Hash() lcommon.Blake2b256 { return b.hash }

func (b *blockfetchTestBlock) PrevHash() lcommon.Blake2b256 { return b.prevHash }
func (b *blockfetchTestBlock) SlotNumber() uint64           { return b.slot }

func (b *blockfetchTestBlock) BlockNumber() uint64 { return b.blockNumber }

func (b *blockfetchTestBlock) Era() lcommon.Era { return babbage.EraBabbage }
func (b *blockfetchTestBlock) Type() int        { return 1 }

func (b *blockfetchTestBlock) Cbor() []byte { return []byte{0x80} }

// blockfetchRollbackFixture is the chainsync rollback fixture plus the
// blockfetch wiring the continuation path needs: a request recorder, a log
// sink, and a resync subscriber.
type blockfetchRollbackFixture struct {
	*chainsyncRollbackFixture
	requests  []ocommon.Point
	logBuf    *syncSafeBuffer
	resyncCh  chan event.ChainsyncResyncEvent
	forkAHash lcommon.Blake2b256
	forkBHash lcommon.Blake2b256
}

func newBlockfetchRollbackFixture(t *testing.T) *blockfetchRollbackFixture {
	t.Helper()
	base := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	base.ls.config.EventBus = bus

	f := &blockfetchRollbackFixture{
		chainsyncRollbackFixture: base,
		logBuf:                   &syncSafeBuffer{},
		resyncCh:                 make(chan event.ChainsyncResyncEvent, 8),
		forkAHash: lcommon.NewBlake2b256(
			testHashBytes("blockfetch-fork-a-header"),
		),
		forkBHash: lcommon.NewBlake2b256(
			testHashBytes("blockfetch-fork-b-header"),
		),
	}
	base.ls.config.Logger = slog.New(
		slog.NewJSONHandler(f.logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}),
	)
	base.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		start ocommon.Point,
		_ ocommon.Point,
	) error {
		f.requests = append(f.requests, start)
		return nil
	}
	bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if resync, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				f.resyncCh <- resync
			}
		},
	)
	return f
}

// queueForkAHeaderAndStartBatch queues a header that continues the fixture's
// current tip and starts a real blockfetch batch for it.
func (f *blockfetchRollbackFixture) queueForkAHeaderAndStartBatch(
	t *testing.T,
) {
	t.Helper()
	require.NoError(t, f.ls.chain.AddBlockHeader(mockHeader{
		hash:        f.forkAHash,
		prevHash:    lcommon.NewBlake2b256(f.currentTip.Point.Hash),
		blockNumber: f.currentTip.BlockNumber + 1,
		slot:        f.currentTip.Point.Slot + 10,
	}))
	require.Equal(t, 1, f.ls.chain.HeaderCount())
	require.NoError(t, startQueuedBlockfetchForTest(f.ls, f.connId, nil))
	require.Len(t, f.requests, 1)
}

// deliverForkABody delivers the body for the fork-A header through the real
// blockfetch event subscriber, where it is buffered pending a commit batch.
func (f *blockfetchRollbackFixture) deliverForkABody(t *testing.T) {
	t.Helper()
	point := ocommon.NewPoint(
		f.currentTip.Point.Slot+10,
		f.forkAHash.Bytes(),
	)
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: f.connId,
			Point:        point,
			Type:         1,
			Block: &blockfetchTestBlock{
				hash:        f.forkAHash,
				prevHash:    lcommon.NewBlake2b256(f.currentTip.Point.Hash),
				slot:        point.Slot,
				blockNumber: f.currentTip.BlockNumber + 1,
			},
		},
	))
	require.Len(
		t,
		f.ls.pendingBlockfetchEvents,
		1,
		"the body must be buffered, not committed, for the abandoned "+
			"batch to still hold it when the fork is resolved",
	)
}

// rollbackToAncestorAndQueueForkB reproduces what fork resolution does once it
// has picked the peer's chain: roll the primary chain back to the common
// ancestor, then queue the winning peer's header path from there.
func (f *blockfetchRollbackFixture) rollbackToAncestorAndQueueForkB(
	t *testing.T,
) {
	t.Helper()
	f.ls.chainsyncMutex.Lock()
	var pending pendingPublishes
	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Rollback:     true,
			Point:        f.ancestorTip.Point,
		},
		&pending,
	)
	f.ls.chainsyncMutex.Unlock()
	pending.flush()
	require.NoError(t, err)
	require.Equal(t, f.ancestorTip.Point, f.ls.chain.Tip().Point)
	require.Equal(
		t,
		0,
		f.ls.chain.HeaderCount(),
		"the rollback discards the header queue the abandoned batch was "+
			"fetching",
	)

	require.NoError(t, f.ls.chain.AddBlockHeader(mockHeader{
		hash:        f.forkBHash,
		prevHash:    lcommon.NewBlake2b256(f.ancestorTip.Point.Hash),
		blockNumber: f.ancestorTip.BlockNumber + 1,
		slot:        f.ancestorTip.Point.Slot + 5,
	}))
	require.Equal(t, 1, f.ls.chain.HeaderCount())
}

// TestForkRestartKeepsReplacementHeadersWhenAbandonedBatchArrives is issue
// #3771. Fork resolution rolls the chain back to the common ancestor, queues
// the winning peer's header path from there, and only then restarts
// blockfetch. That restart flushes whatever the abandoned batch had already
// buffered, so the first body from the losing fork reached chain insertion
// against the replacement header queue, was rejected as not matching it, and
// cleared it -- leaving nothing queued, nothing fetching, and the remaining
// bodies logging "does not fit on current chain tip" on their way out.
//
// A body fetched for a chain the node has since rolled back must be discarded
// instead, so the replacement queue survives and the restarted batch fetches
// it.
func TestForkRestartKeepsReplacementHeadersWhenAbandonedBatchArrives(
	t *testing.T,
) {
	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	f.deliverForkABody(t)
	f.rollbackToAncestorAndQueueForkB(t)

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(f.ls, f.connId, nil),
	)

	assert.Equal(
		t,
		1,
		f.ls.chain.HeaderCount(),
		"the replacement header queue must survive the abandoned batch",
	)
	require.Len(
		t,
		f.requests,
		2,
		"the fork restart must issue a request for the replacement queue",
	)
	assert.Equal(
		t,
		ocommon.NewPoint(
			f.ancestorTip.Point.Slot+5,
			f.forkBHash.Bytes(),
		),
		f.requests[1],
		"the restarted batch must fetch from the new continuation point",
	)
	assert.NotContains(
		t,
		f.logBuf.String(),
		"ignoring blockfetch block",
		"a body for a superseded chain must be discarded before it "+
			"reaches chain insertion",
	)
}

// The bounded-recovery half of #3771: a batch that delivered bodies but
// extended nothing, while headers stayed queued, must feed the same
// same-range failure streak a NoBlocks reply feeds, so the range is dropped
// and a fresh intersect requested rather than being re-requested forever.
func TestBatchDoneTreatsDiscardedBatchAsUnobtainedRange(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)

	for attempt := 1; attempt <= blockfetchMaxSameRangeFailures; attempt++ {
		if attempt > 1 {
			require.NoError(
				t,
				startQueuedBlockfetchForTest(f.ls, f.connId, nil),
			)
		} else {
			f.queueForkAHeaderAndStartBatch(t)
			f.deliverForkABody(t)
			f.rollbackToAncestorAndQueueForkB(t)
		}
		require.Positive(t, f.ls.chain.HeaderCount())
		var pending pendingPublishes
		require.NoError(t, handleEventBlockfetchBatchDoneForTest(
			f.ls,
			BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
			&pending,
		))
		pending.flush()
	}

	assert.Equal(
		t,
		0,
		f.ls.chain.HeaderCount(),
		"a range that never extends the chain must stop being requested",
	)
	// The rollback publishes its own "local ledger rollback" resync, so scan
	// for the one the range-failure streak is responsible for.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case resync := <-f.resyncCh:
			if resync.Reason !=
				event.ChainsyncResyncReasonBlockfetchRangeUnavailable {
				continue
			}
			assert.Equal(t, f.connId, resync.ConnectionId)
			return
		case <-deadline:
			t.Fatal(
				"expected a chainsync resync for the queued range that " +
					"repeatedly extended nothing",
			)
		}
	}
}

// The negative case for the discard: the discard is keyed on the chain having
// rolled back under the batch, so a body that genuinely does not fit an
// unchanged tip must still be rejected by chain insertion rather than quietly
// dropped. Accepting such a body to keep the pipeline moving would be far
// worse than the stall.
func TestNonFittingBodyStillRejectedWhenTipDidNotMove(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)
	tipBefore := f.ls.chain.Tip()

	// No queued header and no rollback: the body's own prev hash is what is
	// compared against the tip, and it names a block we do not have.
	point := ocommon.NewPoint(
		tipBefore.Point.Slot+10,
		testHashBytes("orphan-body"),
	)
	f.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	f.ls.activeBlockfetchConnId = f.connId
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: f.connId,
			Point:        point,
			Type:         1,
			Block: &blockfetchTestBlock{
				hash: lcommon.NewBlake2b256(point.Hash),
				prevHash: lcommon.NewBlake2b256(
					testHashBytes("unknown-parent"),
				),
				slot:        point.Slot,
				blockNumber: tipBefore.BlockNumber + 1,
			},
		},
	))
	require.Len(t, f.ls.pendingBlockfetchEvents, 1)

	f.ls.chainsyncBlockfetchMutex.Lock()
	err := f.ls.flushPendingBlockfetchBlocksDeferred(nil)
	f.ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	assert.Equal(
		t,
		tipBefore.Point,
		f.ls.chain.Tip().Point,
		"a body that does not fit the tip must not be added",
	)
	logs := f.logBuf.String()
	assert.True(
		t,
		strings.Contains(logs, "does not fit on current chain tip"),
		"the body must be rejected by chain insertion, not discarded: %s",
		logs,
	)
	assert.NotContains(
		t,
		logs,
		"discarding blockfetch blocks for superseded chain",
		"nothing rolled back, so the discard path must not run",
	)
}

// A body that is delivered and then discarded has not obtained the range it
// was fetched for, so it must not clear that range's failure record. Clearing
// on arrival kept the streak at zero for exactly the batches the bound exists
// to catch: every round recorded one failure and the next round's delivery
// erased it.
func TestDiscardedBodyDoesNotCountAsRangeProgress(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	forkAPoint := ocommon.NewPoint(
		f.currentTip.Point.Slot+10,
		f.forkAHash.Bytes(),
	)

	// A batch that ends without obtaining the queued range records the
	// failure against that range's start point, and starts a fresh batch for
	// the same range.
	var pending pendingPublishes
	require.NoError(t, handleEventBlockfetchBatchDoneForTest(
		f.ls,
		BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
		&pending,
	))
	pending.flush()
	require.True(
		t,
		f.ls.blockfetchRangeFailure.matches(forkAPoint),
		"the unobtained range must be tracked before this test can prove "+
			"anything about clearing it",
	)
	require.Len(t, f.requests, 2)

	// Supersede the batch that is now in flight, then let the peer deliver
	// the tracked range's own start block into it.
	f.ls.chainsyncMutex.Lock()
	var rollbackPending pendingPublishes
	rollbackErr := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Rollback:     true,
			Point:        f.ancestorTip.Point,
		},
		&rollbackPending,
	)
	f.ls.chainsyncMutex.Unlock()
	rollbackPending.flush()
	require.NoError(t, rollbackErr)
	f.deliverForkABody(t)

	f.ls.chainsyncBlockfetchMutex.Lock()
	err := f.ls.flushPendingBlockfetchBlocksDeferred(nil)
	f.ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	assert.True(
		t,
		f.ls.blockfetchRangeFailure.matches(forkAPoint),
		"a discarded body must leave the range's failure record intact",
	)
}
