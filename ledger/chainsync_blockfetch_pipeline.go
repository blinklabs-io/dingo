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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// queuedBlockfetchRequest is a blockfetch range request dispatched to a peer
// ahead of need -- while the active batch on the same connection is still
// streaming -- so the peer always has a next range in hand and the pipeline
// never idles for a full round-trip at a batch boundary. See
// startQueuedBlockfetchPrefetchLocked and tryPromoteQueuedBlockfetchLocked.
type queuedBlockfetchRequest struct {
	connId      ouroboros.ConnectionId
	requestId   uint64
	headerStart ocommon.Point
	headerEnd   ocommon.Point
	// headerCount is the number of queued headers this request covers (the
	// `available` HeaderRangeAfter reported at dispatch time). Promotion uses
	// it to skip past this request's own claimed headers when it in turn
	// tops the pipeline back up.
	headerCount int
	// rollbackGeneration and chainGeneration are
	// blockfetchRollbackGeneration/chainRollbackGeneration as observed at
	// dispatch time, mirroring blockfetchBatchRollbackGeneration /
	// blockfetchBatchChainGeneration for the active batch. Promotion refuses
	// to adopt this request if either has since moved: the request was made
	// for a chain segment a rollback has since abandoned.
	rollbackGeneration uint64
	chainGeneration    uint64
	// dispatchedAt is when RequestRange was actually called. Promotion uses
	// this, not the promotion time, to score peer latency -- using the later
	// time would understate every pipelined batch's real wire latency.
	dispatchedAt time.Time
}

// startQueuedBlockfetchPrefetchLocked dispatches a second, pre-queued range
// request on connId for the header window immediately after the one the
// active batch just claimed (claimedHeaders), so gouroboros' pipelined queue
// (see blockfetch.WithRequestPipelining) always has a next request in hand
// while the active one streams. It is a no-op when nothing is queued past the
// active batch's own range, when a request is already pre-queued, or when the
// dispatch fails synchronously.
//
// The caller must hold chainsyncBlockfetchMutex; this function releases and
// reacquires it around the external request, like every other blockfetch
// dispatch in this file (see startQueuedBlockfetchLockedWithWaitSignal's own
// doc comment for why: the request must never be issued while holding the
// mutex the receive path needs to publish blocks).
func (ls *LedgerState) startQueuedBlockfetchPrefetchLocked(
	connId ouroboros.ConnectionId,
	claimedHeaders int,
	batchReadyChan chan struct{},
) {
	if ls.config.BlockfetchRequestRangeFunc == nil ||
		ls.nextBlockfetchRequest != nil {
		return
	}
	nextStart, nextEnd, available := ls.chain.HeaderRangeAfter(
		claimedHeaders,
		BlockfetchBatchSize,
	)
	if available == 0 {
		return
	}
	rollbackGeneration := ls.blockfetchRollbackGeneration.Load()
	chainGeneration := ls.chainRollbackGeneration.Load()
	requestDone := ls.beginBlockfetchRequestLocked(connId)
	dispatchedAt := time.Now()
	ls.chainsyncBlockfetchMutex.Unlock()
	requestId, err := ls.config.BlockfetchRequestRangeFunc(
		connId,
		nextStart,
		nextEnd,
	)
	ls.chainsyncBlockfetchMutex.Lock()
	if err != nil {
		ls.endBlockfetchRequestLocked(connId, requestDone)
		ls.config.Logger.Debug(
			"blockfetch prefetch dispatch failed",
			"component", "ledger",
			"connection_id", connId.String(),
			"header_start_slot", nextStart.Slot,
			"error", err,
		)
		return
	}
	// The active batch this prefetch was meant to follow may have been torn
	// down (rollback, fork restart, timeout) while the dispatch above ran
	// without the mutex held. Its own teardown path already discarded
	// whatever it found in ls.nextBlockfetchRequest at the time, so it knows
	// nothing about this request; mark it for discard here instead of
	// installing it; there is no active batch left for it to be promoted
	// into.
	if ls.chainsyncBlockfetchReadyChan != batchReadyChan {
		ls.markBlockfetchRequestForDiscardLocked(connId)
		return
	}
	ls.nextBlockfetchRequest = &queuedBlockfetchRequest{
		connId:             connId,
		requestId:          requestId,
		headerStart:        nextStart,
		headerEnd:          nextEnd,
		headerCount:        available,
		rollbackGeneration: rollbackGeneration,
		chainGeneration:    chainGeneration,
		dispatchedAt:       dispatchedAt,
	}
}

// markBlockfetchRequestForDiscardLocked adds one more abandoned outstanding
// request on connId to the discard latch, so its late blocks/BatchDone are
// ignored rather than mistaken for whatever request replaces it. The caller
// must hold chainsyncBlockfetchMutex.
func (ls *LedgerState) markBlockfetchRequestForDiscardLocked(
	connId ouroboros.ConnectionId,
) {
	if connIdKey(ls.blockfetchDiscardConnId) != "" &&
		sameConnectionId(ls.blockfetchDiscardConnId, connId) {
		ls.blockfetchDiscardBatchesRemaining++
		return
	}
	ls.blockfetchDiscardConnId = connId
	ls.blockfetchDiscardBatchesRemaining = 1
}

// discardNextBlockfetchRequestLocked drops a pre-queued request unconditionally
// rather than trying to preserve or migrate it (every disruption path --
// rollback, fork restart, timeout, connection close -- makes the same
// choice): losing one pre-fetched batch's head start costs one wasted
// request. Its real terminal event, whenever it arrives, is routed to the
// discard latch instead of being force-completed here, matching how
// restartQueuedBlockfetchAfterForkLocked's same-connection branch treats the
// active batch it is also abandoning. On that path the ledger-level
// bookkeeping (blockfetchRequestsInFlight) stays intact until the genuine
// BatchDone drains it, so a same-connection redispatch still waits for it;
// blockfetchRequestRangeCleanup instead releases that bookkeeping itself
// before calling this, because its own callers have no later event left to
// release it. Either way the latch, not the reservation, is what keeps the
// late terminal event from being attributed to the replacement batch. The
// caller must hold chainsyncBlockfetchMutex.
func (ls *LedgerState) discardNextBlockfetchRequestLocked() {
	next := ls.nextBlockfetchRequest
	if next == nil {
		return
	}
	ls.nextBlockfetchRequest = nil
	ls.markBlockfetchRequestForDiscardLocked(next.connId)
}

// tryPromoteQueuedBlockfetchLocked promotes a pre-queued "next" blockfetch
// request (see startQueuedBlockfetchPrefetchLocked) to the active batch,
// reporting whether it did. The caller must hold chainsyncBlockfetchMutex and
// must only call this once the batch that just completed applied at least
// one block: appliedBlockCount == 0 means the active batch's own headers are
// still queued, and promoting "next" ahead of them would strand the chain on
// a gap it can never fill, since chain insertion requires blocks in queued
// order.
//
// Promotion is refused when the pre-queued request's rollback/chain
// generations no longer match the live ones: it was dispatched for a chain
// segment a rollback has since abandoned, exactly like a stale active batch's
// blocks are discarded by blockfetchBatchStillCurrent. Refusal routes the
// request's own eventual terminal event through the discard latch and
// reports false so the caller falls through to its normal fresh-dispatch
// path.
func (ls *LedgerState) tryPromoteQueuedBlockfetchLocked() bool {
	next := ls.nextBlockfetchRequest
	if next == nil {
		return false
	}
	ls.nextBlockfetchRequest = nil
	if next.rollbackGeneration != ls.blockfetchRollbackGeneration.Load() ||
		next.chainGeneration != ls.chainRollbackGeneration.Load() {
		ls.markBlockfetchRequestForDiscardLocked(next.connId)
		return false
	}
	// The completed batch applying a block is not the same thing as its
	// applying every header it claimed. A body that does not fit the chain
	// tip is swallowed as "ignored" rather than returned (see
	// chain.BlockNotFitChainTipError and noteNonExtendingBlockRejection), and
	// a transport-shaped RangeErr can terminate a range after a partial
	// delivery; either leaves the rest of the claimed headers queued at the
	// front. This request starts past that claimed range, so promoting it
	// would leave the still-queued prefix with nothing fetching it -- chain
	// insertion requires blocks in queued order, so the promoted batch's
	// bodies could never be applied -- and the refill below, which skips from
	// the live queue head by headerCount, would land on a range overlapping
	// both. The live queue head being exactly this request's own start is the
	// precondition promotion needs; refuse otherwise and let the caller
	// dispatch afresh from wherever the queue really is.
	queueHead, _, available := ls.chain.HeaderRangeAfter(0, 1)
	if available == 0 || !pointMatches(queueHead, next.headerStart) {
		ls.config.Logger.Debug(
			"discarding pre-queued blockfetch request past the queue head",
			"component", "ledger",
			"connection_id", next.connId.String(),
			"request_start_slot", next.headerStart.Slot,
			"queue_head_slot", queueHead.Slot,
			"queued_headers", available,
		)
		ls.markBlockfetchRequestForDiscardLocked(next.connId)
		return false
	}
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	ls.chainsyncBlockfetchTimerGeneration++
	ls.chainsyncBlockfetchReadyMutex.Lock()
	if ls.chainsyncBlockfetchReadyChan != nil {
		close(ls.chainsyncBlockfetchReadyChan)
	}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chainsyncBlockfetchReadyMutex.Unlock()
	ls.activeBlockfetchConnId = next.connId
	ls.shadowBlockfetchConnId = ouroboros.ConnectionId{}
	ls.shadowBlockReceivedHashes = nil
	ls.batchBlocksReceived = 0
	ls.batchBlocksApplied = 0
	ls.blockfetchBatchRollbackGeneration = next.rollbackGeneration
	ls.blockfetchBatchChainGeneration = next.chainGeneration
	ls.activeBlockfetchStart = next.dispatchedAt
	ls.firstBlockReceived = false
	ls.pendingBlockfetchEvents = ls.pendingBlockfetchEvents[:0]
	ls.blockfetchRequestGeneration++
	ls.blockfetchPrimaryRequestGeneration = 0
	ls.armBlockfetchTimeoutLocked(next.connId)
	ls.config.Logger.Debug(
		"promoted pre-queued blockfetch request to active batch",
		"component", "ledger",
		"connection_id", next.connId.String(),
		"header_start_slot", next.headerStart.Slot,
		"header_end_slot", next.headerEnd.Slot,
	)
	batchReadyChan := ls.chainsyncBlockfetchReadyChan
	ls.startQueuedBlockfetchPrefetchLocked(
		next.connId,
		next.headerCount,
		batchReadyChan,
	)
	return true
}
