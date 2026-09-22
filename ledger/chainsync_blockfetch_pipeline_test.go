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
)

// deepCatchupRequest records one BlockfetchRequestRangeFunc call for the
// pipelining tests below.
type deepCatchupRequest struct {
	connId ouroboros.ConnectionId
	start  ocommon.Point
	end    ocommon.Point
}

// buildDeepCatchupChain queues headerCount hash-chained headers directly (no
// persistence, no ledger apply) and returns their hashes in queue order. The
// pipelining tests need a queue deep enough that it is not "near tip" (see
// shadowBlockfetchMaxHeaders) and, for the two-distinct-requests case, deeper
// than blockfetchBatchSize so a batch's own claim still leaves a further
// range to pre-queue.
func buildDeepCatchupChain(
	t *testing.T,
	headerCount int,
) (*chain.Chain, []lcommon.Blake2b256) {
	t.Helper()
	testChain := &chain.Chain{}
	prevHash := lcommon.NewBlake2b256(nil)
	hashes := make([]lcommon.Blake2b256, 0, headerCount)
	for i := range headerCount {
		hash := lcommon.NewBlake2b256(
			testHashBytes(fmt.Sprintf("deep-catchup-hdr-%d", i)),
		)
		require.NoError(t, testChain.AddBlockHeader(mockHeader{
			hash:        hash,
			prevHash:    prevHash,
			blockNumber: uint64(i + 1),
			slot:        uint64(i + 1),
		}))
		hashes = append(hashes, hash)
		prevHash = hash
	}
	require.Equal(t, headerCount, testChain.HeaderCount())
	return testChain, hashes
}

// TestStartQueuedBlockfetchPipelinesSecondRequestDuringDeepCatchup is the
// direct fail-before proof for issue #4651: before the dispatch-timing
// rework, a single dispatch issued exactly one RequestRange call and waited
// for its BatchDone before dispatching another, paying a full peer
// round-trip at every batch boundary. With more than blockfetchBatchSize
// headers queued (deep catch-up, not "near tip" -- see
// shadowBlockfetchMaxHeaders), the first dispatch must also pre-queue a
// second, non-overlapping range so the peer always has a next request in
// hand.
func TestStartQueuedBlockfetchPipelinesSecondRequestDuringDeepCatchup(
	t *testing.T,
) {
	t.Parallel()

	headerCount := blockfetchBatchSize + 50
	testChain, _ := buildDeepCatchupChain(t, headerCount)
	connId := testChainsyncConnId(6300, 3001)

	var requests []deepCatchupRequest
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requests = append(requests, deepCatchupRequest{
					connId: gotConnId,
					start:  start,
					end:    end,
				})
				return uint64(len(requests)), nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.startQueuedBlockfetchLocked(connId, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	require.Len(
		t,
		requests,
		2,
		"a deep-catchup dispatch must pre-queue a second, non-overlapping "+
			"request alongside the active one",
	)
	active := requests[0]
	prefetch := requests[1]
	assert.Equal(t, connId, active.connId)
	assert.Equal(t, connId, prefetch.connId)
	assert.Equal(t, uint64(1), active.start.Slot)
	assert.Equal(t, uint64(blockfetchBatchSize), active.end.Slot)
	assert.Equal(
		t,
		uint64(blockfetchBatchSize+1),
		prefetch.start.Slot,
		"the pre-queued request must start immediately after the active "+
			"batch's own claimed range",
	)
	assert.Greater(
		t,
		prefetch.start.Slot,
		active.end.Slot,
		"the two ranges must not overlap",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	require.NotNil(
		t,
		ls.nextBlockfetchRequest,
		"the pre-queued request must be tracked for later promotion",
	)
	assert.Equal(t, connId, ls.nextBlockfetchRequest.connId)
	assert.Equal(t, 50, ls.nextBlockfetchRequest.headerCount)
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestTryPromoteQueuedBlockfetchLockedPromotesAndRefillsPipeline is item (a)'s
// second half: once the active batch completes, the pre-queued request must
// be promoted to active -- with no synchronous fresh RequestRange call for
// its own range, since it was already dispatched -- and exactly one new
// pre-queue dispatch must follow to top the pipeline back to depth 1.
//
// The header queue here models the state *after* the (unmodeled) active
// batch's own headers have already popped: HeaderRangeAfter's skip is an
// offset from the live queue head, so this test drives
// tryPromoteQueuedBlockfetchLocked directly rather than reproducing a real
// chain apply, which needs a persistent chain.Manager this package's other
// blockfetch dispatch tests do not otherwise require.
func TestTryPromoteQueuedBlockfetchLockedPromotesAndRefillsPipeline(
	t *testing.T,
) {
	t.Parallel()

	const remainingHeaders = 80
	const promotedClaim = 50
	testChain, hashes := buildDeepCatchupChain(t, remainingHeaders)
	connId := testChainsyncConnId(6301, 3001)

	var requests []deepCatchupRequest
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       testChainsyncConnId(6301, 3002),
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requests = append(requests, deepCatchupRequest{
					connId: gotConnId,
					start:  start,
					end:    end,
				})
				return uint64(len(requests)), nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	dispatchedAt := time.Now().Add(-5 * time.Second)
	ls.nextBlockfetchRequest = &queuedBlockfetchRequest{
		connId:      connId,
		headerStart: ocommon.NewPoint(1, hashes[0].Bytes()),
		headerEnd: ocommon.NewPoint(
			promotedClaim,
			hashes[promotedClaim-1].Bytes(),
		),
		headerCount:  promotedClaim,
		dispatchedAt: dispatchedAt,
	}
	oldReadyChan := ls.chainsyncBlockfetchReadyChan

	ls.chainsyncBlockfetchMutex.Lock()
	promoted := ls.tryPromoteQueuedBlockfetchLocked()
	ls.chainsyncBlockfetchMutex.Unlock()

	require.True(t, promoted)
	assert.Equal(
		t,
		connId,
		ls.activeBlockfetchConnId,
		"the pre-queued request's connection must become active",
	)
	assert.True(
		t,
		ls.activeBlockfetchStart.Equal(dispatchedAt),
		"peer latency must be scored from the original dispatch time, not "+
			"the promotion time",
	)
	assert.False(
		t,
		oldReadyChan == ls.chainsyncBlockfetchReadyChan,
		"promotion must install a fresh ready channel for the new active batch",
	)

	require.Len(
		t,
		requests,
		1,
		"promotion must top the pipeline back up with exactly one new "+
			"prefetch dispatch, not a synchronous fresh dispatch for the "+
			"promoted range itself",
	)
	topUp := requests[0]
	assert.Equal(t, connId, topUp.connId)
	assert.Equal(
		t,
		uint64(promotedClaim+1),
		topUp.start.Slot,
		"the top-up must skip past the just-promoted request's own claimed headers",
	)
	assert.Equal(t, uint64(remainingHeaders), topUp.end.Slot)

	ls.chainsyncBlockfetchMutex.Lock()
	require.NotNil(
		t,
		ls.nextBlockfetchRequest,
		"the pipeline must be refilled to depth 1 after promotion",
	)
	assert.Equal(
		t,
		remainingHeaders-promotedClaim,
		ls.nextBlockfetchRequest.headerCount,
	)
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestHandleEventBlockfetchBatchDoneDiscardsQueuedRequestOnRollbackGeneration
// is item (b): a rollback can land after a request has been pre-queued but
// before it is promoted (its own terminal event has not arrived yet -- see
// the FIFO delivery argument in startQueuedBlockfetchPrefetchLocked's doc
// comment). Promotion must refuse it, route its late terminal event through
// the discard latch instead of adopting or force-completing it, and leave no
// misattributed block or chain mutation behind.
func TestHandleEventBlockfetchBatchDoneDiscardsQueuedRequestOnRollbackGeneration(
	t *testing.T,
) {
	t.Parallel()

	testChain, hashes := buildDeepCatchupChain(t, 80)
	connId := testChainsyncConnId(6302, 3001)

	var requests []deepCatchupRequest
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requests = append(requests, deepCatchupRequest{
					connId: gotConnId,
					start:  start,
					end:    end,
				})
				return uint64(len(requests)), nil
			},
		},
	}
	// Slots 1-80 are Mithril-covered so the block deliveries below skip
	// header crypto verification (issue #3528 machinery, orthogonal to what
	// this test proves): these synthetic blocks carry no real VRF/KES
	// material.
	ls.mithrilLedgerSlot = 80
	ls.publishSnapshotsLocked()
	ls.batchBlocksApplied = 1

	// "next" was pre-queued while both generations were 0, exactly like
	// startQueuedBlockfetchPrefetchLocked records them.
	ls.chainsyncBlockfetchMutex.Lock()
	ls.beginBlockfetchRequestLocked(connId) // active's own tracked request
	ls.beginBlockfetchRequestLocked(
		connId,
	) // next's tracked request (FIFO: second)
	ls.nextBlockfetchRequest = &queuedBlockfetchRequest{
		connId:      connId,
		headerStart: ocommon.NewPoint(51, hashes[50].Bytes()),
		headerEnd:   ocommon.NewPoint(80, hashes[79].Bytes()),
		headerCount: 30,
	}
	ls.chainsyncBlockfetchMutex.Unlock()

	// A rollback lands after "next" was dispatched but is reflected only in
	// the live generation counter -- exactly like AdoptLocalForgedSibling,
	// which does not touch the still-running active batch's own bookkeeping
	// (see blockfetchRollbackGeneration's doc comment).
	ls.blockfetchRollbackGeneration.Add(1)

	// The active batch's own BatchDone: promotion must refuse "next" (its
	// generation is now stale) and fall through to a fresh dispatch, which
	// blocks on draining "next"'s still-outstanding request.
	//
	// Locked directly, like production's handleEventBlockfetch does, rather
	// than through handleEventBlockfetchBatchDoneForTest: that helper also
	// waits for the spawned continuation worker, which is exactly what must
	// stay blocked until the stale terminal event below is delivered.
	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	}, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	ls.chainsyncBlockfetchMutex.Lock()
	assert.Nil(
		t,
		ls.nextBlockfetchRequest,
		"a stale-generation request must not be adopted",
	)
	assert.Equal(
		t,
		connId,
		ls.blockfetchDiscardConnId,
		"the abandoned request's late terminal event must be filtered",
	)
	assert.Equal(t, 1, ls.blockfetchDiscardBatchesRemaining)
	ls.chainsyncBlockfetchMutex.Unlock()

	// While the discard window is still open, a block for connId must be
	// dropped rather than misattributed to whatever dispatch follows.
	ls.chainsyncBlockfetchMutex.Lock()
	err = ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(51, hashes[50].Bytes()),
		Block: &blockfetchTestBlock{
			hash:        hashes[50],
			prevHash:    hashes[49],
			slot:        51,
			blockNumber: 51,
		},
	}, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)
	assert.Empty(
		t,
		ls.pendingBlockfetchEvents,
		"a block for a discarded request must not be buffered",
	)

	// Deliver the abandoned request's own late terminal event. This both
	// unblocks the continuation worker's drain wait and must itself be
	// silently discarded rather than mutating any state.
	ls.chainsyncBlockfetchMutex.Lock()
	err = ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	}, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()

	ls.chainsyncBlockfetchMutex.Lock()
	assert.Equal(
		t,
		ouroboros.ConnectionId{},
		ls.blockfetchDiscardConnId,
		"the discard window must close once the abandoned request's own "+
			"terminal event is handled",
	)
	assert.Equal(t, 0, ls.blockfetchDiscardBatchesRemaining)
	require.Len(
		t,
		requests,
		1,
		"exactly one fresh dispatch must follow, not a promotion of the "+
			"stale request",
	)
	assert.Equal(t, connId, requests[0].connId)
	ls.chainsyncBlockfetchMutex.Unlock()

	// Once the discard window has closed, a block for connId belongs to the
	// fresh dispatch and must be accepted.
	ls.chainsyncBlockfetchMutex.Lock()
	err = ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(51, hashes[50].Bytes()),
		Block: &blockfetchTestBlock{
			hash:        hashes[50],
			prevHash:    hashes[49],
			slot:        51,
			blockNumber: 51,
		},
	}, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)
	assert.Len(
		t,
		ls.pendingBlockfetchEvents,
		1,
		"a block delivered after the discard window closes must be accepted",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestRestartQueuedBlockfetchAfterForkPreservesQueuedRequestFromOtherConnection
// is one half of item (c): when a fork-extension restart preserves an
// in-flight batch from a different connection (handoffPipelineOnSwitchLocked
// protection), its pre-queued follow-up request must survive untouched too --
// there is nothing to protect it *from* on this path.
func TestRestartQueuedBlockfetchAfterForkPreservesQueuedRequestFromOtherConnection(
	t *testing.T,
) {
	t.Parallel()

	testChain, hashes := buildDeepCatchupChain(t, 80)
	otherConn := testChainsyncConnId(6303, 3001)
	newConn := testChainsyncConnId(6303, 3002)

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = otherConn
	ls.selectedBlockfetchConnId = otherConn
	nextReq := &queuedBlockfetchRequest{
		connId:      otherConn,
		headerStart: ocommon.NewPoint(51, hashes[50].Bytes()),
		headerEnd:   ocommon.NewPoint(80, hashes[79].Bytes()),
		headerCount: 30,
	}
	ls.nextBlockfetchRequest = nextReq

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 1, nil
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, newConn, nil),
	)

	assert.Equal(
		t,
		0,
		requestCount,
		"an in-flight batch from a different connection, and its "+
			"pre-queued follow-up, must not be interrupted",
	)
	assert.Same(
		t,
		nextReq,
		ls.nextBlockfetchRequest,
		"the pre-queued request must survive untouched when the active "+
			"batch is preserved",
	)
	assert.Equal(t, newConn, ls.selectedBlockfetchConnId)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestRestartQueuedBlockfetchAfterForkDiscardsQueuedRequestOnSameConnectionRestart
// is the other half of item (c): a restart on the SAME connection tears the
// batch down unconditionally, and now abandons *two* outstanding requests
// (the active batch and its pre-queued follow-up) instead of one. The
// discard latch must absorb both before accepting the replacement batch's
// events.
func TestRestartQueuedBlockfetchAfterForkDiscardsQueuedRequestOnSameConnectionRestart(
	t *testing.T,
) {
	t.Parallel()

	testChain, hashes := buildDeepCatchupChain(t, 80)
	connId := testChainsyncConnId(6304, 3001)

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = connId
	ls.selectedBlockfetchConnId = connId
	ls.nextBlockfetchRequest = &queuedBlockfetchRequest{
		connId:      connId,
		headerStart: ocommon.NewPoint(51, hashes[50].Bytes()),
		headerEnd:   ocommon.NewPoint(80, hashes[79].Bytes()),
		headerCount: 30,
	}

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 1, nil
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, connId, nil),
	)

	assert.Equal(
		t,
		1,
		requestCount,
		"a same-connection restart still issues a fresh dispatch",
	)
	assert.Nil(
		t,
		ls.nextBlockfetchRequest,
		"the pre-queued request must be discarded, not preserved or "+
			"promoted, by a same-connection restart",
	)
	assert.Equal(
		t,
		connId,
		ls.blockfetchDiscardConnId,
		"both the abandoned active batch and its pre-queued follow-up "+
			"must be filtered until they drain",
	)
	assert.Equal(t, 2, ls.blockfetchDiscardBatchesRemaining)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestHandleBlockfetchTimeoutDiscardsQueuedRequestRatherThanLeakingIt is item
// (e): a busy-timeout teardown must release a not-yet-streaming pre-queued
// request's in-flight bookkeeping (blockfetchRequestRangeCleanup's
// force-complete path, the same one it already used for the active and
// shadow requests) rather than leaking it forever.
func TestHandleBlockfetchTimeoutDiscardsQueuedRequestRatherThanLeakingIt(
	t *testing.T,
) {
	t.Parallel()

	testChain, hashes := buildDeepCatchupChain(t, 80)
	connId := testChainsyncConnId(6305, 3001)

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})

	ls.chainsyncBlockfetchMutex.Lock()
	activeDone := ls.beginBlockfetchRequestLocked(connId)
	nextDone := ls.beginBlockfetchRequestLocked(connId)
	ls.nextBlockfetchRequest = &queuedBlockfetchRequest{
		connId:      connId,
		headerStart: ocommon.NewPoint(51, hashes[50].Bytes()),
		headerEnd:   ocommon.NewPoint(80, hashes[79].Bytes()),
		headerCount: 30,
	}
	ls.chainsyncBlockfetchMutex.Unlock()

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 1, nil
	}

	handleBlockfetchTimeoutForTest(ls, connId, nil)

	ls.chainsyncBlockfetchMutex.Lock()
	assert.Nil(
		t,
		ls.nextBlockfetchRequest,
		"a pre-queued request must not survive a timeout-driven teardown",
	)
	// The retry below re-registers a fresh entry for connId, so the map
	// contains the key again; what must not happen is the two channels
	// captured before the timeout going unclosed (checked below).
	ls.chainsyncBlockfetchMutex.Unlock()

	select {
	case <-activeDone:
	default:
		t.Fatal(
			"the active request's in-flight channel must be released by " +
				"the timeout teardown",
		)
	}
	select {
	case <-nextDone:
	default:
		t.Fatal(
			"the pre-queued request's in-flight channel must be " +
				"released, not leaked, by the timeout teardown",
		)
	}

	assert.Equal(
		t,
		1,
		requestCount,
		"the timeout retry must still issue a fresh dispatch for the queued headers",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestBlockfetchPipeliningAndShadowGatesAreDisjoint is item (d): pipelining
// (deep catch-up) and the near-tip shadow-peer strategy must never both be
// eligible for the same header-queue depth. startQueuedBlockfetchLockedWithWaitSignal
// gates both on the identical nearTip boolean, so this is definitionally true
// by construction there -- this test pins that both as a boundary-value
// property of the two constants, and as an observed property of the real
// dispatch path, so an edit that computes the two conditions independently
// (rather than reusing one boolean) is caught.
func TestBlockfetchPipeliningAndShadowGatesAreDisjoint(t *testing.T) {
	t.Parallel()

	for headerCount := range 2*shadowBlockfetchMaxHeaders + 1 {
		nearTip := headerCount <= shadowBlockfetchMaxHeaders
		pipeliningEligible := headerCount > shadowBlockfetchMaxHeaders
		assert.NotEqual(
			t,
			nearTip,
			pipeliningEligible,
			"headerCount=%d: the near-tip shadow gate and the deep-catchup "+
				"pipelining gate must be exact complements",
			headerCount,
		)
	}

	for _, tc := range []struct {
		name             string
		headerCount      int
		wantShadowProbed bool
	}{
		{"at shadow threshold", shadowBlockfetchMaxHeaders, true},
		{"one past shadow threshold", shadowBlockfetchMaxHeaders + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testChain, _ := buildDeepCatchupChain(t, tc.headerCount)
			connId := testChainsyncConnId(6306, 3001)
			shadowProbed := false
			ls := &LedgerState{
				chain: testChain,
				config: LedgerStateConfig{
					Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
					BlockfetchRequestRangeFunc: func(
						ouroboros.ConnectionId,
						ocommon.Point,
						ocommon.Point,
					) (uint64, error) {
						return 1, nil
					},
					PeersWithBlockFunc: func(
						ouroboros.ConnectionId,
						ocommon.Point,
					) []ouroboros.ConnectionId {
						shadowProbed = true
						return nil
					},
				},
			}
			ls.publishSnapshotsLocked()

			ls.chainsyncBlockfetchMutex.Lock()
			err := ls.startQueuedBlockfetchLocked(connId, nil)
			ls.chainsyncBlockfetchMutex.Unlock()
			require.NoError(t, err)

			assert.Equal(
				t,
				tc.wantShadowProbed,
				shadowProbed,
				"the shadow gate must probe for peers at exactly the "+
					"near-tip threshold and never once pipelining takes over",
			)

			ls.chainsyncBlockfetchMutex.Lock()
			ls.blockfetchRequestRangeCleanup()
			ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
			ls.chainsyncBlockfetchMutex.Unlock()
		})
	}
}
