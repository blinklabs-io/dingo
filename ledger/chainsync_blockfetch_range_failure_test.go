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
	"errors"
	"fmt"
	"io"
	"log/slog"
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

// errBlockfetchNoBlocks is the error gouroboros' blockfetch client returns
// from GetBlockRange when the served peer answers MsgNoBlocks, which is what
// a peer that rolled the requested block back now replies (the range server
// rejects a start point the chain no longer holds). It reaches the ledger
// synchronously through BlockfetchRequestRangeFunc, so it never produces a
// BatchDone event.
var errBlockfetchNoBlocks = errors.New(
	"request block range: block(s) not found",
)

// The production helpers named *Locked require their caller to own the
// blockfetch mutex. Keep direct unit-test calls honest now that the helper
// briefly releases that mutex around the external request callback.
func startQueuedBlockfetchForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	return ls.startQueuedBlockfetchLocked(connId, pending)
}

func startQueuedBlockfetchWithWaitSignalForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
	waitStarted chan<- struct{},
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	return ls.startQueuedBlockfetchLockedWithWaitSignal(
		connId,
		pending,
		waitStarted,
	)
}

func restartQueuedBlockfetchAfterForkForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	return ls.restartQueuedBlockfetchAfterForkLocked(connId, pending)
}

func handleEventBlockfetchBatchDoneForTest(
	ls *LedgerState,
	e BlockfetchEvent,
	pending *pendingPublishes,
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.handleEventBlockfetchBatchDone(e, pending)
	ls.chainsyncBlockfetchMutex.Unlock()
	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()
	return err
}

func handleBlockfetchTimeoutForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	ls.handleBlockfetchTimeoutLocked(connId, pending)
}

// TestStartQueuedBlockfetchReleasesMutexAroundRequest models the receive-side
// half of the production deadlock. The real blockfetch callback publishes a
// ledger.blockfetch event, whose subscriber needs chainsyncBlockfetchMutex;
// the request callback must therefore be able to run while that mutex is
// available even when the caller started the batch under the lock.
func TestStartQueuedBlockfetchReleasesMutexAroundRequest(t *testing.T) {
	ls, _, _ := newNoBlocksLedgerState(t, "hdr-lock-cycle")
	defer ls.config.EventBus.Stop()
	ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) error {
		acquired := make(chan struct{})
		go func() {
			ls.chainsyncBlockfetchMutex.Lock()
			close(acquired)
			ls.chainsyncBlockfetchMutex.Unlock()
		}()
		select {
		case <-acquired:
			return nil
		case <-time.After(time.Second):
			return errors.New("blockfetch request ran while blockfetch mutex was held")
		}
	}

	connId := testChainsyncConnId(6111, 3001)
	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.startQueuedBlockfetchLocked(connId, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestStartQueuedBlockfetchDrainsPriorRequestBeforeConnectionReuse verifies
// that a late shadow request cannot share its connection with a later batch.
// Blockfetch events carry only a connection ID, so reusing the connection
// before the prior callback returns would make the old BatchDone ambiguous.
func TestStartQueuedBlockfetchDrainsPriorRequestBeforeConnectionReuse(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("reuse-header")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	connId := testChainsyncConnId(6113, 3001)
	requestStarted := make(chan struct{})
	waitStarted := make(chan struct{})
	requestDone := make(chan struct{})
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) error {
				close(requestStarted)
				return nil
			},
		},
		blockfetchRequestsInFlight: map[string]chan struct{}{
			connIdKey(connId): requestDone,
		},
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- startQueuedBlockfetchWithWaitSignalForTest(
			ls,
			connId,
			nil,
			waitStarted,
		)
	}()
	select {
	case <-requestStarted:
		t.Fatal("reused connection before prior blockfetch request drained")
	case <-waitStarted:
	}
	testutil.RequireNoReceive(
		t,
		requestStarted,
		50*time.Millisecond,
		"blockfetch request started before prior request drained",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	delete(ls.blockfetchRequestsInFlight, connIdKey(connId))
	close(requestDone)
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.RequireReceive(
		t,
		requestStarted,
		time.Second,
		"blockfetch request did not start after prior request drained",
	)
	require.NoError(t, <-startDone)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestBlockfetchBatchDoneDoesNotBlockSubscriberOnContinuation verifies that
// the blockfetch EventBus subscriber does not synchronously enter the next
// GetBlockRange call. GetBlockRange waits for the next BatchDone, so doing so
// from this subscriber deadlocks once the subscriber buffer fills with the
// next batch's blocks.
func TestBlockfetchBatchDoneDoesNotBlockSubscriberOnContinuation(t *testing.T) {
	testChain := &chain.Chain{}
	for blockNumber := uint64(1); blockNumber <= 2; blockNumber++ {
		prevHash := lcommon.NewBlake2b256(nil)
		if blockNumber > 1 {
			prevHash = lcommon.NewBlake2b256([]byte{byte(blockNumber - 1)})
		}
		require.NoError(t, testChain.AddBlockHeader(mockHeader{
			hash:        lcommon.NewBlake2b256([]byte{byte(blockNumber)}),
			prevHash:    prevHash,
			blockNumber: blockNumber,
			slot:        blockNumber,
		}))
	}
	connId := testChainsyncConnId(6112, 3001)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		selectedBlockfetchConnId:     connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		batchBlocksReceived:          1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) error {
				close(requestStarted)
				<-releaseRequest
				return nil
			},
		},
	}

	handlerDone := make(chan struct{})
	go func() {
		ls.handleEventBlockfetch(event.NewEvent(
			BlockfetchEventType,
			BlockfetchEvent{ConnectionId: connId, BatchDone: true},
		))
		close(handlerDone)
	}()
	testutil.RequireReceive(
		t,
		handlerDone,
		time.Second,
		"blockfetch subscriber remained blocked in continuation request",
	)
	testutil.RequireReceive(t, requestStarted, time.Second, "continuation request did not start")

	close(releaseRequest)
	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// newNoBlocksLedgerState builds a LedgerState with one queued header whose
// range every peer refuses with a NoBlocks error, and returns it alongside the
// request counter and a channel of published resync events.
func newNoBlocksLedgerState(
	t *testing.T,
	headerLabel string,
) (*LedgerState, *int, chan event.ChainsyncResyncEvent) {
	t.Helper()
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte(headerLabel)),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	require.Equal(t, 1, testChain.HeaderCount())

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	resyncChan := make(chan event.ChainsyncResyncEvent, 8)
	eventBus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if resync, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				resyncChan <- resync
			}
		},
	)

	requestCount := 0
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger:   logger,
			EventBus: eventBus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) error {
				requestCount++
				return errBlockfetchNoBlocks
			},
		},
	}
	ls.publishSnapshotsLocked()
	return ls, &requestCount, resyncChan
}

// TestStartQueuedBlockfetchDropsHeadersAfterRepeatedNoBlocks pins the recovery
// on the path a NoBlocks response actually takes. GetBlockRange resolves the
// NoBlocks reply into an error returned synchronously from
// BlockfetchRequestRangeFunc, so the request never reaches BatchDone and the
// empty-batch accounting in handleEventBlockfetchBatchDone never sees it. The
// header queue must still be dropped, because a latched header blocks local
// forging for as long as it is queued.
func TestStartQueuedBlockfetchDropsHeadersAfterRepeatedNoBlocks(t *testing.T) {
	ls, requestCount, resyncChan := newNoBlocksLedgerState(t, "hdr-no-blocks")
	connId := testChainsyncConnId(6102, 3001)

	const attempts = 25
	require.Greater(
		t,
		attempts,
		blockfetchMaxSameRangeFailures,
		"attempt count must exceed the bound for this test to prove it",
	)
	for range attempts {
		// Callers treat this error as advisory (several only log it), so
		// the recovery cannot depend on any caller acting on it.
		_ = startQueuedBlockfetchForTest(ls, connId, nil)
	}

	assert.LessOrEqual(
		t,
		*requestCount,
		blockfetchMaxSameRangeFailures,
		"a range no peer will serve must stop being requested",
	)
	assert.Equal(
		t,
		0,
		ls.chain.HeaderCount(),
		"the unfetchable queued header must be dropped so locally "+
			"forged blocks are no longer rejected",
	)
	resync := testutil.RequireReceive(
		t,
		resyncChan,
		2*time.Second,
		"chainsync resync after repeated NoBlocks responses",
	)
	assert.Equal(t, connId, resync.ConnectionId)
}

// TestStartQueuedBlockfetchTransientErrorsDoNotAccumulate verifies that
// request failures which do not establish NoBlocks leave the range-failure
// record untouched. A reconnecting peer can return these errors repeatedly
// for the same queued range while the range remains servable.
func TestStartQueuedBlockfetchTransientErrorsDoNotAccumulate(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		unconfigured bool
	}{
		{
			name: "transport reset",
			err:  errors.New("connection reset by peer"),
		},
		{
			name: "protocol shutdown",
			err:  errors.New("protocol is shutting down"),
		},
		{
			name: "send queue failure",
			err:  errors.New("failed to enqueue blockfetch request"),
		},
		{
			name:         "wiring error",
			unconfigured: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ls, _, resyncChan := newNoBlocksLedgerState(
				t,
				"hdr-transient-"+test.name,
			)
			if test.unconfigured {
				ls.config.BlockfetchRequestRangeFunc = nil
			} else {
				requestErr := test.err
				ls.config.BlockfetchRequestRangeFunc = func(
					ouroboros.ConnectionId,
					ocommon.Point,
					ocommon.Point,
				) error {
					return requestErr
				}
			}

			connId := testChainsyncConnId(6110, 3001)
			for range blockfetchMaxSameRangeFailures * 3 {
				err := startQueuedBlockfetchForTest(ls, connId, nil)
				require.Error(t, err)
			}

			assert.Equal(t, 1, ls.chain.HeaderCount())
			assert.Equal(
				t,
				0,
				ls.blockfetchRangeFailure.count,
				"transient request errors must not advance the unavailable count",
			)
			testutil.RequireNoReceive(
				t,
				resyncChan,
				100*time.Millisecond,
				"transient request errors must not trigger resync",
			)
		})
	}
}

// TestRestartQueuedBlockfetchAfterForkDropsHeadersOnRepeatedNoBlocks covers
// the specific caller observed wedging on DevNet. After a slot battle,
// tryResolveFork rolls back to the common ancestor, re-queues the winning
// peer's headers and calls restartQueuedBlockfetchAfterForkLocked; if that
// returns an error the fork handler only logs
// "failed to start blockfetch after fork rollback" and reports the fork
// resolved. Nothing else retries, so the queued header stays latched and
// block production stops until an unrelated event clears the queue.
func TestRestartQueuedBlockfetchAfterForkDropsHeadersOnRepeatedNoBlocks(
	t *testing.T,
) {
	ls, requestCount, resyncChan := newNoBlocksLedgerState(
		t,
		"hdr-fork-restart",
	)
	connId := testChainsyncConnId(6103, 3001)

	const attempts = 25
	for range attempts {
		// Mirrors the fork-resolution call sites, which discard the error
		// after logging it.
		_ = restartQueuedBlockfetchAfterForkForTest(ls, connId, nil)
	}

	assert.LessOrEqual(
		t,
		*requestCount,
		blockfetchMaxSameRangeFailures,
		"the fork restart must stop re-requesting an unservable range",
	)
	assert.Equal(
		t,
		0,
		ls.chain.HeaderCount(),
		"fork-resolution restart failures must drop the queued header "+
			"so forging is released",
	)
	testutil.RequireReceive(
		t,
		resyncChan,
		2*time.Second,
		"chainsync resync after repeated fork-restart NoBlocks responses",
	)
}

// TestBlockfetchRangeFailureClearedWhenRangeIsDelivered verifies a peer that
// is merely briefly behind is never punished: once the stuck range's own block
// arrives, its failure record is discarded, so earlier misses cannot combine
// with a later unrelated miss to drop a healthy queue.
func TestBlockfetchRangeFailureClearedWhenRangeIsDelivered(t *testing.T) {
	ls, _, _ := newNoBlocksLedgerState(t, "hdr-delivered")
	connId := testChainsyncConnId(6104, 3001)
	stuckStart, _ := ls.chain.HeaderRange(blockfetchBatchSize)

	for range blockfetchMaxSameRangeFailures * 3 {
		_ = startQueuedBlockfetchForTest(ls, connId, nil)
		require.Positive(
			t,
			ls.chain.HeaderCount(),
			"header queue must survive while the range keeps arriving",
		)
		// The block for the stuck range arrived, so the range is
		// fetchable after all and its failure record is stale.
		ls.noteBlockfetchRangeProgress(stuckStart)
	}

	assert.Equal(t, 1, ls.chain.HeaderCount())
}

// TestBlockfetchRangeFailuresAccumulatePerRangeDespiteInterleavedActivity is
// the regression guard for a bound that tripped only by luck in production.
// The failures against one unfetchable range are minutes apart, and between
// them the node fetches normally from other peers and churns the header queue
// on forks, connection switches and header mismatches. A globally scoped
// consecutive counter is reset by all of that, so it fires only when failures
// happen to land back to back: two identical DevNet runs produced 1 recovery
// against 169 wedge events and 9 against 81.
//
// Accounting is therefore keyed to the range start point and survives both
// interleaved deliveries for other ranges and clearQueuedHeaders churn, so
// repeated failures against the *same* unfetchable range still add up.
func TestBlockfetchRangeFailuresAccumulatePerRangeDespiteInterleavedActivity(
	t *testing.T,
) {
	ls, requestCount, resyncChan := newNoBlocksLedgerState(t, "hdr-interleaved")
	connId := testChainsyncConnId(6105, 3001)
	stuckHeader := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-interleaved")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	otherRange := ocommon.NewPoint(
		999,
		lcommon.NewBlake2b256([]byte("some-other-range")).Bytes(),
	)

	for attempt := 1; attempt <= blockfetchMaxSameRangeFailures; attempt++ {
		require.Equal(
			t,
			1,
			ls.chain.HeaderCount(),
			"stuck header must be queued for attempt %d",
			attempt,
		)
		_ = startQueuedBlockfetchForTest(ls, connId, nil)
		if attempt == blockfetchMaxSameRangeFailures {
			break
		}
		// Between attempts the node keeps working: blocks arrive for
		// other ranges from other peers...
		ls.noteBlockfetchRangeProgress(otherRange)
		// ...and fork, connection-switch and header-mismatch handling
		// repeatedly clears the queue, after which the peer re-offers the
		// same unfetchable header.
		ls.clearQueuedHeaders()
		require.NoError(t, ls.chain.AddBlockHeader(stuckHeader))
	}

	assert.Equal(
		t,
		blockfetchMaxSameRangeFailures,
		*requestCount,
		"each attempt against the stuck range must be counted",
	)
	assert.Equal(
		t,
		0,
		ls.chain.HeaderCount(),
		"repeated failures against the same range must drop the queue "+
			"even when unrelated traffic succeeds in between",
	)
	testutil.RequireReceive(
		t,
		resyncChan,
		2*time.Second,
		"chainsync resync after repeated same-range failures",
	)
}

// TestBlockfetchRangeFailuresDoNotAccumulateAcrossDifferentRanges is the other
// half of the contract: transient misses spread over different ranges must not
// add up into a queue drop, so a peer that is briefly behind on a few distinct
// points is left alone.
func TestBlockfetchRangeFailuresDoNotAccumulateAcrossDifferentRanges(
	t *testing.T,
) {
	ls, _, resyncChan := newNoBlocksLedgerState(t, "hdr-distinct-0")
	connId := testChainsyncConnId(6106, 3001)

	for attempt := range blockfetchMaxSameRangeFailures * 3 {
		_ = startQueuedBlockfetchForTest(ls, connId, nil)
		// Each attempt is against a different queued header, as happens
		// when the chain keeps moving and every miss is a one-off.
		ls.clearQueuedHeaders()
		require.NoError(t, ls.chain.AddBlockHeader(mockHeader{
			hash: lcommon.NewBlake2b256(
				[]byte(fmt.Sprintf("hdr-distinct-%d", attempt+1)),
			),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		}))
	}

	assert.Equal(
		t,
		1,
		ls.chain.HeaderCount(),
		"one-off misses against different ranges must not drop the queue",
	)
	testutil.RequireNoReceive(
		t,
		resyncChan,
		100*time.Millisecond,
		"no resync for transient misses on distinct ranges",
	)
}

// TestHandleEventBlockfetchBatchDoneStopsRepeatingEmptyBatches covers the
// near-tip blockfetch wedge: a peer that answers a queued header's range with
// a batch carrying no blocks (it rolled the block back, so it can no longer
// serve the body) must not be asked for the same range indefinitely.
//
// The queued header is what makes this fatal rather than merely wasteful:
// while a header sits at the head of the queue, chain.AddBlock rejects every
// locally forged block with BlockNotMatchHeaderError ("does not match first
// pending header hash"), so block production stops for as long as the header
// stays latched. After a bounded number of consecutive empty batches the
// pipeline must drop the unfetchable header queue and force a fresh
// intersect instead of re-requesting.
func TestHandleEventBlockfetchBatchDoneStopsRepeatingEmptyBatches(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-empty-batch")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	require.Equal(t, 1, testChain.HeaderCount())

	connId := testChainsyncConnId(6100, 3001)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	resyncChan := make(chan event.ChainsyncResyncEvent, 4)
	eventBus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if resync, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				resyncChan <- resync
			}
		},
	)

	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger:   logger,
			EventBus: eventBus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) error {
				requestCount++
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	// Every batch completes without delivering a block while the header
	// stays queued. The first attempts retry as before; the streak must be
	// bounded well below the number of attempts made here.
	const emptyBatchAttempts = 25
	require.Greater(
		t,
		emptyBatchAttempts,
		blockfetchMaxSameRangeFailures,
		"attempt count must exceed the bound for this test to prove it",
	)
	for i := range emptyBatchAttempts {
		require.NoError(
			t,
			handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
				ConnectionId: connId,
				BatchDone:    true,
			}, nil),
			"batch done %d", i,
		)
	}

	assert.LessOrEqual(
		t,
		requestCount,
		blockfetchMaxSameRangeFailures,
		"the same unfetchable range must not be re-requested "+
			"once the empty-batch streak hits its bound",
	)
	assert.Equal(
		t,
		0,
		testChain.HeaderCount(),
		"the unfetchable queued header must be dropped so locally "+
			"forged blocks are no longer rejected",
	)
	resync := testutil.RequireReceive(
		t,
		resyncChan,
		2*time.Second,
		"chainsync resync after repeated empty blockfetch batches",
	)
	assert.Equal(t, connId, resync.ConnectionId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestHandleEventBlockfetchBatchDoneEmptyBatchStreakResetsOnProgress verifies
// the streak counter tracks *consecutive* empty batches only: a batch that
// delivers a block clears it, so a peer that occasionally returns an empty
// batch is not eventually punished for it.
func TestHandleEventBlockfetchBatchDoneEmptyBatchStreakResetsOnProgress(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-streak-reset")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	connId := testChainsyncConnId(6101, 3001)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: logger,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) error {
				requestCount++
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	// Alternate empty and productive batches well past the bound. The
	// productive batches deliver the queued range itself, so its failure
	// record is discarded each time and the header queue survives.
	queuedStart, _ := testChain.HeaderRange(blockfetchBatchSize)
	for range blockfetchMaxSameRangeFailures * 3 {
		require.NoError(
			t,
			handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
				ConnectionId: connId,
				BatchDone:    true,
			}, nil),
		)
		// Stand in for a delivered block: handleEventBlockfetchBlock both
		// counts the block and discards that range's failure record.
		ls.batchBlocksReceived = 1
		ls.noteBlockfetchRangeProgress(queuedStart)
		require.NoError(
			t,
			handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
				ConnectionId: connId,
				BatchDone:    true,
			}, nil),
		)
	}

	assert.Equal(
		t,
		1,
		testChain.HeaderCount(),
		"queued header must survive when batches keep making progress",
	)
	assert.Positive(t, requestCount)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}
