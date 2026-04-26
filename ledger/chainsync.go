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
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	cardano "github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/governance"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	// Max number of blocks to fetch in a single blockfetch call
	// This prevents us exceeding the configured recv queue size in the block-fetch protocol
	blockfetchBatchSize = 500

	// When we're still meaningfully behind tip, wait for a small header runway
	// before starting blockfetch. This avoids repeated one-block fetch loops
	// near a fork boundary where peers may not yet serve the first announced
	// block body.
	blockfetchMinBatchHeadersWhenBehind = 8
	blockfetchMaxBatchHeadersWhenBehind = 64
	blockfetchMinBatchGapSlots          = 64

	// Number of received blockfetch blocks to buffer before committing them.
	// Keep this small so downstream iterators still see fresh blocks promptly.
	blockfetchCommitBatchSize = 8

	// Default/fallback slot threshold for blockfetch batches
	blockfetchBatchSlotThresholdDefault = 2500 * 20

	// Timeout for updates on a blockfetch operation. This is based on a 2s BatchStart
	// and a 2s Block timeout for blockfetch
	blockfetchBusyTimeout = 30 * time.Second

	// Interval for rate-limiting non-active connection drop messages
	dropEventLogInterval = 60 * time.Second

	// Interval for periodic sync progress reporting
	syncProgressLogInterval = 30 * time.Second

	// Rollback loop detection thresholds
	rollbackLoopThreshold = 2               // number of rollbacks to same slot before breaking loop
	rollbackLoopWindow    = 5 * time.Minute // time window for rollback loop detection

	// Number of consecutive header mismatches before triggering
	// a chainsync re-sync to recover from persistent forks.
	// A higher threshold gives tryResolveFork more chances to
	// find the common ancestor and reduces disruptive resyncs
	// in multi-producer networks where short forks are expected.
	headerMismatchResyncThreshold = 20

	// Chainsync re-sync reasons
	resyncReasonRollbackAhead    = "rollback point ahead of local tip"
	resyncReasonRollbackNotFound = "rollback point not found"

	maxPeerHeaderHistoryPerConn = 256
)

type peerHeaderRecord struct {
	event    ChainsyncEvent
	prevHash []byte
}

type peerHeaderChain struct {
	order  []string
	byHash map[string]peerHeaderRecord
}

func (ls *LedgerState) handleEventChainsync(evt event.Event) {
	e, ok := evt.Data.(ChainsyncEvent)
	if !ok {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		ls.logUnexpectedChainsyncEventData("ChainsyncEvent", evt)
		return
	}
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	if !ls.isConnectionLive(e.ConnectionId) {
		ls.config.Logger.Debug(
			"ignoring chainsync event from closed connection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"rollback", e.Rollback,
			"slot", e.Point.Slot,
		)
		ls.discardBufferedPeerHeaders(e.ConnectionId)
		delete(ls.peerHeaderHistory, connIdKey(e.ConnectionId))
		return
	}
	if e.Rollback {
		if err := ls.handleEventChainsyncRollback(e); err != nil {
			ls.config.Logger.Error(
				"failed to handle rollback",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.FatalErrorFunc != nil {
				ls.config.FatalErrorFunc(err)
			}
			return
		}
	} else if e.BlockHeader != nil {
		if err := ls.handleEventChainsyncBlockHeader(e); err != nil {
			// Header queue full is expected during bulk sync when
			// pipelined headers arrive faster than blockfetch can
			// drain them. Log at DEBUG to avoid log spam.
			if errors.Is(err, chain.ErrHeaderQueueFull) {
				ls.config.Logger.Debug(
					"failed to handle block header",
					"component", "ledger",
					"error", err,
					"slot", e.Point.Slot,
					"hash", hex.EncodeToString(e.Point.Hash),
				)
				return
			}
			ls.config.Logger.Error(
				"failed to handle block header",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					LedgerErrorEventType,
					event.NewEvent(
						LedgerErrorEventType,
						LedgerErrorEvent{
							Error:     err,
							Operation: "block_header",
							Point:     e.Point,
						},
					),
				)
			}
			return
		}
	}
}

func (ls *LedgerState) isConnectionLive(
	connId ouroboros.ConnectionId,
) bool {
	if ls.config.ConnectionLiveFunc == nil {
		return true
	}
	return ls.config.ConnectionLiveFunc(connId)
}

func (ls *LedgerState) handleEventBlockfetch(evt event.Event) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	e, ok := evt.Data.(BlockfetchEvent)
	if !ok {
		ls.logUnexpectedChainsyncEventData("BlockfetchEvent", evt)
		return
	}
	if e.BatchDone {
		if err := ls.handleEventBlockfetchBatchDone(e); err != nil {
			ls.config.Logger.Error(
				"failed to handle blockfetch batch done",
				"component", "ledger",
				"error", err,
			)
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					LedgerErrorEventType,
					event.NewEvent(
						LedgerErrorEventType,
						LedgerErrorEvent{
							Error:     err,
							Operation: "blockfetch_batch_done",
						},
					),
				)
			}
		}
	} else if e.Block != nil {
		if err := ls.handleEventBlockfetchBlock(e); err != nil {
			if strings.Contains(
				err.Error(),
				"block header crypto verification failed",
			) && ls.config.EventBus != nil {
				ls.config.Logger.Warn(
					"recycling connection after header verification failure",
					"component", "ledger",
					"connection_id", e.ConnectionId.String(),
					"slot", e.Point.Slot,
					"hash", hex.EncodeToString(e.Point.Hash),
				)
				ls.config.EventBus.Publish(
					connmanager.ConnectionRecycleRequestedEventType,
					event.NewEvent(
						connmanager.ConnectionRecycleRequestedEventType,
						connmanager.ConnectionRecycleRequestedEvent{
							ConnectionId: e.ConnectionId,
							Reason:       "block_header_verification_failure",
						},
					),
				)
			}
			ls.config.Logger.Error(
				"failed to handle block",
				"component", "ledger",
				"error", err,
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
			)
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					LedgerErrorEventType,
					event.NewEvent(
						LedgerErrorEventType,
						LedgerErrorEvent{
							Error:     err,
							Operation: "blockfetch_block",
							Point:     e.Point,
						},
					),
				)
			}
		}
	}
}

func (ls *LedgerState) logUnexpectedChainsyncEventData(
	expectedType string,
	evt event.Event,
) {
	ls.config.Logger.Warn(
		"received unexpected event data type",
		"component", "ledger",
		"expected", expectedType,
		"data_type", fmt.Sprintf("%T", evt.Data),
		"event_type", evt.Type,
		"event_timestamp", evt.Timestamp,
		"event", evt,
	)
}

func (ls *LedgerState) handleChainSwitchEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSwitchEvent)
	if !ok {
		return
	}
	var replayConnId ouroboros.ConnectionId
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	replayConnId, err := ls.handoffPipelineOnSwitchLocked(
		e.NewConnectionId,
	)
	if err != nil {
		// The target connection may have closed between chain selection
		// and event processing. Retry with the current active best peer
		// before giving up.
		if ls.config.GetActiveConnectionFunc != nil {
			if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil &&
				!sameConnectionId(*activeConnId, e.NewConnectionId) {
				ls.config.Logger.Info(
					"chain switch target unavailable, retrying with active best peer",
					"component", "ledger",
					"failed_connection_id", e.NewConnectionId.String(),
					"active_connection_id", activeConnId.String(),
					"error", err,
				)
				if retryConnId, retryErr := ls.handoffPipelineOnSwitchLocked(*activeConnId); retryErr == nil {
					replayConnId = retryConnId
					err = nil
				}
			}
		}
		if err != nil {
			ls.config.Logger.Warn(
				"failed to hand off chainsync pipeline on chain switch, resetting pipeline",
				"component", "ledger",
				"connection_id", e.NewConnectionId.String(),
				"error", err,
			)
			// Clear orphaned headers and stale connection refs so the
			// pipeline can accept headers from reconnected peers instead
			// of stalling permanently.
			ls.clearQueuedHeaders()
			ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
		}
	}
	ls.chainsyncBlockfetchMutex.Unlock()
	if err != nil {
		return
	}
	if connIdKey(replayConnId) != "" {
		ls.replayBufferedHeadersAsync(replayConnId)
	}
}

func (ls *LedgerState) handleConnectionClosedEvent(evt event.Event) {
	e, ok := evt.Data.(connmanager.ConnectionClosedEvent)
	if !ok {
		return
	}
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if sameConnectionId(ls.selectedBlockfetchConnId, e.ConnectionId) {
		ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	}
	delete(ls.bufferedHeaderEvents, connIdKey(e.ConnectionId))
	delete(ls.peerHeaderHistory, connIdKey(e.ConnectionId))
	// Cancel in-flight blockfetch if the dead connection owns it.
	// Without this, chainsyncBlockfetchReadyChan stays non-nil and
	// new headers from reconnected peers are queued behind a batch
	// that will never complete, causing a permanent pipeline stall.
	if sameConnectionId(ls.activeBlockfetchConnId, e.ConnectionId) &&
		ls.chainsyncBlockfetchReadyChan != nil {
		ls.config.Logger.Info(
			"canceling blockfetch on closed connection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
		)
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	}
	if sameConnectionId(ls.headerPipelineConnId, e.ConnectionId) {
		ls.clearQueuedHeaders()
	}
}

func (ls *LedgerState) handleEventChainsyncAwaitReply(evt event.Event) {
	e, ok := evt.Data.(ChainsyncAwaitReplyEvent)
	if !ok {
		ls.logUnexpectedChainsyncEventData(
			"ChainsyncAwaitReplyEvent",
			evt,
		)
		return
	}
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()
	if !ls.isConnectionLive(e.ConnectionId) {
		ls.config.Logger.Debug(
			"ignoring await-reply event from closed connection",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
		)
		return
	}
	if ls.chain == nil || ls.chain.HeaderCount() == 0 {
		return
	}
	if ls.config.GetActiveConnectionFunc == nil {
		return
	}
	activeConnId := ls.config.GetActiveConnectionFunc()
	if activeConnId == nil ||
		!sameConnectionId(*activeConnId, e.ConnectionId) {
		return
	}
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil || ls.chain.HeaderCount() == 0 {
		return
	}
	ls.selectedBlockfetchConnId = e.ConnectionId
	ls.config.Logger.Debug(
		"selected chainsync peer entered await reply, flushing queued headers to blockfetch",
		"component", "ledger",
		"connection_id", e.ConnectionId.String(),
		"header_count", ls.chain.HeaderCount(),
	)
	if err := ls.startQueuedBlockfetchLocked(e.ConnectionId); err != nil {
		ls.config.Logger.Error(
			"failed to start blockfetch after await reply",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"error", err,
		)
		if ls.config.EventBus != nil {
			ls.config.EventBus.Publish(
				LedgerErrorEventType,
				event.NewEvent(
					LedgerErrorEventType,
					LedgerErrorEvent{
						Error:     err,
						Operation: "await_reply_blockfetch",
					},
				),
			)
		}
	}
}

// detectConnectionSwitch checks for an active connection change and logs a
// summary of dropped rollback events when a switch is detected. It returns the
// current active connection ID and whether connection filtering is configured.
// When configured is false, callers should skip all connection-based filtering.
func (ls *LedgerState) detectConnectionSwitch() (
	activeConnId *ouroboros.ConnectionId,
	configured bool,
) {
	if ls.config.GetActiveConnectionFunc == nil {
		return nil, false
	}
	activeConnId = ls.config.GetActiveConnectionFunc()
	if activeConnId != nil &&
		(ls.lastActiveConnId == nil ||
			!sameConnectionId(*ls.lastActiveConnId, *activeConnId)) {
		if ls.lastActiveConnId != nil {
			ls.config.Logger.Info(
				"active connection changed",
				"component", "ledger",
				"previous_connection_id", ls.lastActiveConnId.String(),
				"new_connection_id", activeConnId.String(),
				"dropped_rollbacks", ls.dropRollbackCount,
			)
			ls.dropRollbackCount = 0
			ls.headerMismatchCount = 0
			ls.chainsyncBlockfetchMutex.Lock()
			replayConnId, err := ls.handoffPipelineOnSwitchLocked(
				*activeConnId,
			)
			if err != nil {
				ls.config.Logger.Warn(
					"failed to hand off chainsync pipeline after active connection change, resetting pipeline",
					"component", "ledger",
					"connection_id", activeConnId.String(),
					"error", err,
				)
				ls.clearQueuedHeaders()
				ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
			}
			ls.chainsyncBlockfetchMutex.Unlock()
			if err == nil && connIdKey(replayConnId) != "" {
				ls.replayBufferedHeadersAsync(replayConnId)
			}
			// Clear per-connection state (e.g., header dedup cache)
			// so the new connection can re-deliver blocks from the
			// intersection without them being filtered as duplicates.
			if ls.config.ConnectionSwitchFunc != nil {
				ls.config.ConnectionSwitchFunc()
			}
		}
		ls.lastActiveConnId = activeConnId
		// Preserve rollbackHistory across connection switches so the
		// loop detector can still catch repeated rollbacks from the
		// same peer/session. The detector keys on exact rollback
		// point + connection, so peer switches do not poison healthy
		// fork convergence.
	}
	return activeConnId, true
}

func (ls *LedgerState) handoffPipelineOnSwitchLocked(
	newConnId ouroboros.ConnectionId,
) (ouroboros.ConnectionId, error) {
	ls.selectedBlockfetchConnId = newConnId
	headerCount := 0
	if ls.chain != nil {
		headerCount = ls.chain.HeaderCount()
	}

	if connIdKey(newConnId) == "" {
		return ouroboros.ConnectionId{}, nil
	}

	hasBufferedHeadersForNewConn := len(
		ls.bufferedHeaderEvents[connIdKey(newConnId)],
	) > 0

	if ls.chainsyncBlockfetchReadyChan != nil &&
		connIdKey(ls.activeBlockfetchConnId) != "" &&
		!sameConnectionId(ls.activeBlockfetchConnId, newConnId) {
		ls.config.Logger.Debug(
			"canceling in-flight blockfetch batch on chain switch",
			"component", "ledger",
			"previous_connection_id", ls.activeBlockfetchConnId.String(),
			"new_connection_id", newConnId.String(),
			"queued_headers", headerCount,
		)
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	}

	if connIdKey(ls.headerPipelineConnId) != "" &&
		!sameConnectionId(ls.headerPipelineConnId, newConnId) {
		if ls.chainsyncBlockfetchReadyChan == nil &&
			headerCount > 0 &&
			hasBufferedHeadersForNewConn {
			ls.config.Logger.Debug(
				"dropping stale queued header fragment on chain switch",
				"component", "ledger",
				"previous_owner_connection_id",
				ls.headerPipelineConnId.String(),
				"new_connection_id", newConnId.String(),
				"queued_headers", headerCount,
			)
			ls.clearQueuedHeaders()
			return newConnId, nil
		}
		ls.config.Logger.Debug(
			"releasing stale header pipeline owner on chain switch",
			"component", "ledger",
			"previous_owner_connection_id",
			ls.headerPipelineConnId.String(),
			"new_connection_id", newConnId.String(),
			"queued_headers", headerCount,
		)
		ls.headerPipelineConnId = ouroboros.ConnectionId{}
		// Purge the header dedup cache for slots beyond the current
		// block tip. The new connection may have already delivered
		// headers that were deduplicated against the old owner's
		// headers at the ouroboros layer. Without purging, those
		// headers can never be re-delivered, leaving a gap that
		// stalls the pipeline until genuinely new blocks arrive.
		if ls.config.ClearSeenHeadersFromFunc != nil {
			ls.config.ClearSeenHeadersFromFunc(ls.Tip().Point.Slot)
		}
	}

	if ls.chainsyncBlockfetchReadyChan == nil &&
		headerCount > 0 {
		ls.config.Logger.Debug(
			"restarting queued blockfetch on selected connection",
			"component", "ledger",
			"connection_id", newConnId.String(),
			"header_count", headerCount,
		)
		if err := ls.startQueuedBlockfetchLocked(newConnId); err != nil {
			return ouroboros.ConnectionId{}, fmt.Errorf(
				"restart queued blockfetch on switch: %w",
				err,
			)
		}
		return ouroboros.ConnectionId{}, nil
	}

	if ls.chainsyncBlockfetchReadyChan == nil &&
		hasBufferedHeadersForNewConn {
		return newConnId, nil
	}

	return ouroboros.ConnectionId{}, nil
}

func (ls *LedgerState) bufferHeaderEvent(e ChainsyncEvent) {
	if ls.bufferedHeaderEvents == nil {
		ls.bufferedHeaderEvents = make(
			map[string][]ChainsyncEvent,
		)
	}
	key := connIdKey(e.ConnectionId)
	events := ls.bufferedHeaderEvents[key]
	if len(events) > 0 {
		last := events[len(events)-1]
		if last.Point.Slot == e.Point.Slot &&
			bytes.Equal(last.Point.Hash, e.Point.Hash) {
			return
		}
	}
	const maxBufferedHeadersPerConn = 128
	if len(events) >= maxBufferedHeadersPerConn {
		events = append(events[1:], e)
	} else {
		events = append(events, e)
	}
	ls.bufferedHeaderEvents[key] = events
}

func (ls *LedgerState) clearQueuedHeaders() {
	ls.chain.ClearHeaders()
	ls.headerPipelineConnId = ouroboros.ConnectionId{}
	// Purge the header dedup cache for slots beyond the current
	// block tip. Queued headers that were recorded in the dedup
	// cache but just discarded would otherwise block re-delivery
	// on subsequent connections, causing a permanent pipeline stall.
	if ls.config.ClearSeenHeadersFromFunc != nil {
		ls.config.ClearSeenHeadersFromFunc(ls.Tip().Point.Slot)
	}
}

func (ls *LedgerState) recordPeerHeaderHistory(e ChainsyncEvent) {
	if e.BlockHeader == nil || len(e.Point.Hash) == 0 {
		return
	}
	if ls.peerHeaderHistory == nil {
		ls.peerHeaderHistory = make(map[string]*peerHeaderChain)
	}
	key := connIdKey(e.ConnectionId)
	history := ls.peerHeaderHistory[key]
	if history == nil {
		history = &peerHeaderChain{
			order: make([]string, 0, maxPeerHeaderHistoryPerConn),
			byHash: make(map[string]peerHeaderRecord,
				maxPeerHeaderHistoryPerConn),
		}
		ls.peerHeaderHistory[key] = history
	}
	hashKey := hex.EncodeToString(e.Point.Hash)
	if _, ok := history.byHash[hashKey]; ok {
		return
	}
	history.order = append(history.order, hashKey)
	history.byHash[hashKey] = peerHeaderRecord{
		event:    e,
		prevHash: append([]byte(nil), e.BlockHeader.PrevHash().Bytes()...),
	}
	if len(history.order) <= maxPeerHeaderHistoryPerConn {
		return
	}
	evictKey := history.order[0]
	history.order = history.order[1:]
	delete(history.byHash, evictKey)
}

func (ls *LedgerState) findPeerForkPath(
	e ChainsyncEvent,
	initialPrevHash []byte,
) (*ocommon.Point, []ChainsyncEvent, error) {
	prevHash := append([]byte(nil), initialPrevHash...)
	history := ls.peerHeaderHistory[connIdKey(e.ConnectionId)]
	pathReversed := []ChainsyncEvent{e}
	visited := map[string]struct{}{
		hex.EncodeToString(e.Point.Hash): {},
	}
	for depth := 0; depth < maxPeerHeaderHistoryPerConn &&
		len(prevHash) > 0; depth++ {
		ancestorBlock, err := database.BlockByHash(ls.db, prevHash)
		if err == nil {
			point := ocommon.NewPoint(
				ancestorBlock.Slot,
				ancestorBlock.Hash,
			)
			slices.Reverse(pathReversed)
			return &point, pathReversed, nil
		}
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil, fmt.Errorf(
				"lookup ancestor hash %x: %w",
				prevHash,
				err,
			)
		}
		hashKey := hex.EncodeToString(prevHash)
		var (
			record peerHeaderRecord
			ok     bool
		)
		if history != nil {
			record, ok = history.byHash[hashKey]
		}
		if !ok && ls.config.PeerHeaderLookupFunc != nil {
			lookupEvent, lookupPrevHash, found := ls.config.PeerHeaderLookupFunc(
				e.ConnectionId,
				prevHash,
			)
			if found {
				record = peerHeaderRecord{
					event:    lookupEvent,
					prevHash: lookupPrevHash,
				}
				ok = true
			}
		}
		if !ok {
			return nil, nil, nil
		}
		if _, seen := visited[hashKey]; seen {
			return nil, nil, nil
		}
		visited[hashKey] = struct{}{}
		pathReversed = append(pathReversed, record.event)
		prevHash = append(prevHash[:0], record.prevHash...)
	}
	return nil, nil, nil
}

func connIdKey(connId ouroboros.ConnectionId) string {
	if connId.LocalAddr == nil && connId.RemoteAddr == nil {
		return ""
	}
	return connId.String()
}

func sameConnectionId(a, b ouroboros.ConnectionId) bool {
	keyA := connIdKey(a)
	keyB := connIdKey(b)
	if keyA == "" || keyB == "" {
		return keyA == keyB
	}
	return keyA == keyB
}

func desiredBlockfetchBatchHeaders(
	gapSlots uint64,
	gapBlocks uint64,
	maxHeaders int,
) int {
	if maxHeaders <= 0 {
		return 0
	}
	if gapBlocks == 0 {
		if gapSlots == 0 {
			return 0
		}
		return min(1, maxHeaders)
	}
	var minHeaders int
	switch {
	case gapBlocks > 64:
		minHeaders = 8
	case gapBlocks > 16:
		minHeaders = 4
	case gapBlocks > 4:
		minHeaders = 2
	default:
		minHeaders = int(gapBlocks)
	}
	minHeaders = min(minHeaders, blockfetchMaxBatchHeadersWhenBehind)
	return min(minHeaders, maxHeaders)
}

func (ls *LedgerState) requestChainsyncResync(
	connId ouroboros.ConnectionId,
	reason string,
) {
	ls.headerMismatchCount = 0
	ls.rollbackHistory = nil
	delete(ls.bufferedHeaderEvents, connIdKey(connId))
	if ls.config.EventBus == nil {
		return
	}
	ls.config.EventBus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: connId,
				Reason:       reason,
			},
		),
	)
}

func (ls *LedgerState) currentHeaderPipelineOwner() ouroboros.ConnectionId {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil {
		if connIdKey(ls.headerPipelineConnId) != "" {
			return ls.headerPipelineConnId
		}
		if connIdKey(ls.activeBlockfetchConnId) != "" {
			return ls.activeBlockfetchConnId
		}
		return ouroboros.ConnectionId{}
	}
	if ls.chain != nil && ls.chain.HeaderCount() > 0 {
		return ls.headerPipelineConnId
	}
	// Once the shared header queue drains, there is no live pipeline owner.
	// A stale selected blockfetch peer must not monopolize future headers while
	// the pipeline is idle; whichever peer delivers the next usable header gets
	// to seed the next batch.
	ls.headerPipelineConnId = ouroboros.ConnectionId{}
	return ouroboros.ConnectionId{}
}

func (ls *LedgerState) staleSelectedOwnerWouldBufferHeader(
	e ChainsyncEvent,
) bool {
	return connIdKey(ls.selectedBlockfetchConnId) != "" &&
		ls.chainsyncBlockfetchReadyChan == nil &&
		(ls.chain == nil || ls.chain.HeaderCount() == 0) &&
		!sameConnectionId(ls.selectedBlockfetchConnId, e.ConnectionId)
}

func (ls *LedgerState) logIdleSelectedOwnerRelease(e ChainsyncEvent) {
	ls.config.Logger.Debug(
		"releasing idle selected blockfetch owner before header admission",
		"component", "ledger",
		"selected_connection_id", ls.selectedBlockfetchConnId.String(),
		"event_connection_id", e.ConnectionId.String(),
		"slot", e.Point.Slot,
	)
}

func (ls *LedgerState) clearIdleSelectedOwner() {
	if ls.chainsyncBlockfetchReadyChan == nil &&
		(ls.chain == nil || ls.chain.HeaderCount() == 0) {
		ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	}
}

func (ls *LedgerState) shouldBufferHeaderEvent(e ChainsyncEvent) bool {
	ls.chainsyncBlockfetchMutex.Lock()
	if ls.staleSelectedOwnerWouldBufferHeader(e) {
		ls.logIdleSelectedOwnerRelease(e)
		ls.clearIdleSelectedOwner()
	}
	ls.chainsyncBlockfetchMutex.Unlock()
	ownerConnId := ls.currentHeaderPipelineOwner()
	if ownerConnId == (ouroboros.ConnectionId{}) {
		ls.headerPipelineConnId = e.ConnectionId
		return false
	}
	if sameConnectionId(ownerConnId, e.ConnectionId) {
		ls.headerPipelineConnId = e.ConnectionId
		return false
	}
	if ls.headerFitsCurrentPipeline(e) {
		ls.headerPipelineConnId = e.ConnectionId
		ls.selectedBlockfetchConnId = e.ConnectionId
		ls.config.Logger.Debug(
			"accepting compatible header from different connection",
			"component", "ledger",
			"event_connection_id", e.ConnectionId.String(),
			"previous_owner_connection_id", ownerConnId.String(),
			"slot", e.Point.Slot,
		)
		return false
	}
	ls.headerPipelineConnId = ownerConnId
	ls.bufferHeaderEvent(e)
	ls.config.Logger.Debug(
		"buffering header from non-owner connection",
		"component", "ledger",
		"event_connection_id", e.ConnectionId.String(),
		"owner_connection_id", ownerConnId.String(),
		"slot", e.Point.Slot,
	)
	return true
}

func (ls *LedgerState) headerFitsCurrentPipeline(e ChainsyncEvent) bool {
	if ls.chain == nil || e.BlockHeader == nil {
		return false
	}
	prevHash := e.BlockHeader.PrevHash().Bytes()
	headerTip := ls.chain.HeaderTip()
	if len(headerTip.Point.Hash) == 0 {
		return len(prevHash) == 0
	}
	return bytes.Equal(prevHash, headerTip.Point.Hash)
}

func (ls *LedgerState) nextBufferedHeaderConnId() (
	ouroboros.ConnectionId,
	bool,
) {
	if key := connIdKey(ls.selectedBlockfetchConnId); key != "" {
		if events := ls.bufferedHeaderEvents[key]; len(events) > 0 {
			return events[len(events)-1].ConnectionId, true
		}
	}
	var (
		bestConn ouroboros.ConnectionId
		bestTip  uint64
		found    bool
	)
	for _, events := range ls.bufferedHeaderEvents {
		if len(events) == 0 {
			continue
		}
		tipSlot := events[len(events)-1].Tip.Point.Slot
		if !found || tipSlot > bestTip {
			bestConn = events[len(events)-1].ConnectionId
			bestTip = tipSlot
			found = true
		}
	}
	return bestConn, found
}

func (ls *LedgerState) replayBufferedHeadersAsync(
	connId ouroboros.ConnectionId,
) {
	go func() {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		if ls.headerPipelineConnId != (ouroboros.ConnectionId{}) ||
			ls.chain.HeaderCount() > 0 {
			return
		}
		if err := ls.replayBufferedHeaderEvents(connId); err != nil {
			ls.config.Logger.Warn(
				"failed to replay buffered header events",
				"component", "ledger",
				"connection_id", connId.String(),
				"error", err,
			)
		}
	}()
}

func (ls *LedgerState) replayBufferedHeaderEvents(
	connId ouroboros.ConnectionId,
) error {
	key := connIdKey(connId)
	if len(ls.bufferedHeaderEvents[key]) == 0 {
		return nil
	}
	events := append(
		[]ChainsyncEvent(nil),
		ls.bufferedHeaderEvents[key]...,
	)
	delete(ls.bufferedHeaderEvents, key)
	for _, evt := range events {
		if err := ls.handleEventChainsyncBlockHeader(evt); err != nil {
			return err
		}
	}
	return nil
}

func (ls *LedgerState) discardBufferedPeerHeaders(
	connId ouroboros.ConnectionId,
) {
	delete(ls.bufferedHeaderEvents, connIdKey(connId))
	if sameConnectionId(ls.headerPipelineConnId, connId) {
		ls.clearQueuedHeaders()
	}
}

func (ls *LedgerState) handleEventChainsyncRollback(e ChainsyncEvent) error {
	// Filter events from non-active connections when chain selection is enabled
	if activeConnId, configured := ls.detectConnectionSwitch(); configured {
		if activeConnId == nil {
			// No active connection selected yet. Allow the rollback
			// to proceed — the downstream security-parameter-K check
			// and rollback-loop detector still guard against deep or
			// repeated rollbacks. Blanket-dropping here caused
			// pipeline stalls after Mithril bootstrap when chain
			// selection had not yet promoted a peer.
			ls.config.Logger.Debug(
				"no active connection, processing rollback event",
				"connection_id", e.ConnectionId.String(),
				"slot", e.Point.Slot,
				"local_tip_slot", ls.chain.Tip().Point.Slot,
			)
		} else if !sameConnectionId(*activeConnId, e.ConnectionId) {
			ls.discardBufferedPeerHeaders(e.ConnectionId)
			// Event is from non-active connection, skip
			// Rate-limit this message to once per dropEventLogInterval
			now := time.Now()
			if now.Sub(ls.dropRollbackLastLog) >= dropEventLogInterval {
				suppressed := ls.dropRollbackCount
				ls.dropRollbackCount = 0
				ls.dropRollbackLastLog = now
				ls.config.Logger.Debug(
					"dropping rollback from non-active connection and clearing buffered peer headers",
					"component", "ledger",
					"event_connection_id", e.ConnectionId.String(),
					"active_connection_id", activeConnId.String(),
					"slot", e.Point.Slot,
					"cleared_buffered_headers", true,
					"suppressed_since_last_log", suppressed,
				)
			} else {
				ls.dropRollbackCount++
			}
			return nil
		}
	}

	// Rollback loop detection: track recent rollbacks and skip if
	// the same peer repeats the same rollback point too frequently
	// within the detection window.
	now := time.Now()
	connKey := connIdKey(e.ConnectionId)
	ls.rollbackHistory = append(ls.rollbackHistory, rollbackRecord{
		point: ocommon.Point{
			Slot: e.Point.Slot,
			Hash: append([]byte(nil), e.Point.Hash...),
		},
		connKey:   connKey,
		timestamp: now,
	})
	// Prune entries older than the detection window
	cutoff := now.Add(-rollbackLoopWindow)
	pruned := ls.rollbackHistory[:0]
	for _, r := range ls.rollbackHistory {
		if !r.timestamp.Before(cutoff) {
			pruned = append(pruned, r)
		}
	}
	ls.rollbackHistory = pruned
	// Count repeated rollbacks to this exact point from the same
	// connection. Different peers can legitimately converge on the
	// same rollback point during chain selection, and those rollbacks
	// must not be suppressed.
	var slotCount int
	for _, r := range ls.rollbackHistory {
		if r.connKey == connKey && pointMatches(r.point, e.Point) {
			slotCount++
		}
	}
	if slotCount >= rollbackLoopThreshold {
		// Exempt rollbacks to slots where we forged a block — fork
		// resolution on our own block is normal Ouroboros behavior
		// (slot battles), not a pathological loop.
		skipRollback := true
		if checker := ls.loadForgedBlockChecker(); checker != nil {
			if _, forged := checker.WasForgedByUs(e.Point.Slot); forged {
				ls.config.Logger.Info(
					"allowing rollback on forged slot (slot battle resolution)",
					"component", "ledger",
					"slot", e.Point.Slot,
					"count", slotCount,
				)
				skipRollback = false
			}
		}
		if skipRollback {
			ls.config.Logger.Warn(
				"rollback loop detected, skipping rollback to break loop",
				"component", "ledger",
				"slot", e.Point.Slot,
				"count", slotCount,
				"window", rollbackLoopWindow,
			)
			return nil
		}
	}

	// A rollback to the current tip is a no-op — the peer's
	// FindIntersect resolved to the same point we already sit at.
	// Skip the rollback entirely to avoid publishing a spurious
	// "local ledger rollback" resync event that would close all
	// connections and create a reconnect loop.
	localTip := ls.chain.HeaderTip()
	if e.Point.Slot == localTip.Point.Slot &&
		bytes.Equal(e.Point.Hash, localTip.Point.Hash) {
		ls.config.Logger.Debug(
			"rollback to current tip is no-op, skipping",
			"component", "ledger",
			"slot", e.Point.Slot,
			"connection_id", e.ConnectionId.String(),
		)
		return nil
	}

	// A rollback point ahead of our local tip is invalid for the
	// current chain view and typically indicates intersect drift.
	// Trigger a chainsync re-sync instead of failing hard.
	if e.Point.Slot > localTip.Point.Slot {
		ls.config.Logger.Warn(
			"received rollback point ahead of local tip, triggering chainsync re-sync",
			"component", "ledger",
			"rollback_slot", e.Point.Slot,
			"local_tip_slot", localTip.Point.Slot,
			"connection_id", e.ConnectionId.String(),
		)
		ls.resetChainsyncResyncState()
		ls.chainsyncState = SyncingChainsyncState
		if ls.config.EventBus != nil {
			ls.config.EventBus.Publish(
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: e.ConnectionId,
						Reason:       resyncReasonRollbackAhead,
					},
				),
			)
		}
		return nil
	}

	if ls.chainsyncState == SyncingChainsyncState {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: rolling back to %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.chainsyncState = RollbackChainsyncState
	}
	if err := ls.rollbackChainAndState(e.Point); err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			// Missing rollback point can happen when local state and peer
			// chainsync cursor drift. Recover by forcing re-intersect.
			ls.config.Logger.Warn(
				"rollback point not found locally, triggering chainsync re-sync",
				"component", "ledger",
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
				"connection_id", e.ConnectionId.String(),
			)
			ls.resetChainsyncResyncState()
			ls.chainsyncState = SyncingChainsyncState
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					event.ChainsyncResyncEventType,
					event.NewEvent(
						event.ChainsyncResyncEventType,
						event.ChainsyncResyncEvent{
							ConnectionId: e.ConnectionId,
							Reason:       resyncReasonRollbackNotFound,
						},
					),
				)
			}
			return nil
		}
		if errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
			// The peer's chain has diverged beyond K blocks from
			// ours. This is a security violation — we must not
			// follow a chain that requires rolling back more than
			// K blocks. Trigger a chainsync re-sync so the peer
			// governance can reconnect and negotiate a fresh
			// intersection rather than waiting for a protocol
			// timeout.
			ls.config.Logger.Error(
				"chainsync rollback exceeds security "+
					"parameter K, rejecting peer chain",
				"component", "ledger",
				"slot", e.Point.Slot,
				"hash", hex.EncodeToString(e.Point.Hash),
				"connection_id", e.ConnectionId.String(),
			)
			// Restore state: no rollback actually occurred, so
			// we are still syncing. Leaving RollbackChainsyncState
			// would cause a spurious "switched to fork" log and
			// fork metric increment on the next block header.
			ls.chainsyncState = SyncingChainsyncState
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					event.ChainsyncResyncEventType,
					event.NewEvent(
						event.ChainsyncResyncEventType,
						event.ChainsyncResyncEvent{
							ConnectionId: e.ConnectionId,
							Reason:       "rollback exceeds security parameter K",
						},
					),
				)
			}
			return nil
		}
		return fmt.Errorf("chain rollback failed: %w", err)
	}
	return nil
}

// resetChainsyncResyncState clears chainsync-local recovery state before a
// re-sync. It mutates rollbackHistory, headerMismatchCount, and queued
// chain/blockfetch state by calling chain.ClearHeaders and
// blockfetchRequestRangeCleanup (while holding chainsyncBlockfetchMutex).
// Callers must hold chainsyncMutex before invoking this method to avoid races
// with other chainsync operations.
func (ls *LedgerState) resetChainsyncResyncState() {
	ls.rollbackHistory = nil
	ls.headerMismatchCount = 0
	ls.bufferedHeaderEvents = nil
	ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
	ls.clearQueuedHeaders()
	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

func pointMatches(a, b ocommon.Point) bool {
	return a.Slot == b.Slot && bytes.Equal(a.Hash, b.Hash)
}

type LocalRollbackRecoveryResult struct {
	Recovered           bool
	SkipConnectionClose bool
	PrimaryChainTipSlot uint64
}

func (ls *LedgerState) recoverPeerHeaderHistoryFromPointLocked(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) (int, error) {
	history := ls.peerHeaderHistory[connIdKey(connId)]
	if history == nil || len(history.order) == 0 {
		return 0, nil
	}
	for i := len(history.order) - 1; i >= 0; i-- {
		record, ok := history.byHash[history.order[i]]
		if !ok || record.event.Point.Slot <= point.Slot {
			continue
		}
		ancestorPoint, forkPath, err := ls.findPeerForkPath(
			record.event,
			record.prevHash,
		)
		if err != nil {
			return 0, err
		}
		if ancestorPoint == nil || !pointMatches(*ancestorPoint, point) {
			continue
		}
		for _, evt := range forkPath {
			if evt.Point.Slot <= point.Slot {
				continue
			}
			if err := ls.chain.AddBlockHeader(evt.BlockHeader); err != nil {
				ls.clearQueuedHeaders()
				ls.headerPipelineConnId = ouroboros.ConnectionId{}
				return 0, err
			}
		}
		if ls.chain.HeaderCount() == 0 {
			continue
		}
		ls.headerPipelineConnId = connId
		ls.selectedBlockfetchConnId = connId
		return ls.chain.HeaderCount(), nil
	}
	return 0, nil
}

// RecoverAfterLocalRollback resets chainsync-local queued state after a ledger
// rollback, then replays any peer-local header history that still fits the new
// tip. This keeps rollback recovery local to the node instead of re-entering
// FindIntersect on live ChainSync sessions. The result reports whether peer
// history was replayed and whether connection closure should be skipped because
// the primary chain tip is already past the completed rollback point.
func (ls *LedgerState) RecoverAfterLocalRollback(
	connIds []ouroboros.ConnectionId,
	point ocommon.Point,
) LocalRollbackRecoveryResult {
	ls.chainsyncMutex.Lock()
	defer ls.chainsyncMutex.Unlock()

	if ls.chain == nil {
		return LocalRollbackRecoveryResult{}
	}
	ls.RLock()
	lastLocalRollbackSeq := ls.lastLocalRollbackSeq
	lastLocalRollbackPoint := ls.lastLocalRollbackPoint
	ls.RUnlock()
	if lastLocalRollbackSeq > 0 &&
		pointMatches(lastLocalRollbackPoint, point) {
		if chainTipSlot := ls.PrimaryChainTipSlot(); chainTipSlot > point.Slot {
			return LocalRollbackRecoveryResult{
				SkipConnectionClose: true,
				PrimaryChainTipSlot: chainTipSlot,
			}
		}
	}
	ls.resetChainsyncResyncState()

	preferredConnIds := make([]ouroboros.ConnectionId, 0, len(connIds)+1)
	seenConnIds := make(map[string]struct{}, len(connIds)+1)
	if ls.config.GetActiveConnectionFunc != nil {
		if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil {
			key := connIdKey(*activeConnId)
			if key != "" {
				preferredConnIds = append(preferredConnIds, *activeConnId)
				seenConnIds[key] = struct{}{}
			}
		}
	}
	for _, connId := range connIds {
		key := connIdKey(connId)
		if key == "" {
			continue
		}
		if _, ok := seenConnIds[key]; ok {
			continue
		}
		preferredConnIds = append(preferredConnIds, connId)
		seenConnIds[key] = struct{}{}
	}

	for _, connId := range preferredConnIds {
		headerCount, err := ls.recoverPeerHeaderHistoryFromPointLocked(
			connId,
			point,
		)
		if err != nil {
			ls.config.Logger.Warn(
				"failed to recover peer header history after local rollback",
				"component", "ledger",
				"connection_id", connId.String(),
				"slot", point.Slot,
				"error", err,
			)
			continue
		}
		if headerCount == 0 {
			continue
		}
		ls.config.Logger.Info(
			"replayed peer header history after local rollback",
			"component", "ledger",
			"connection_id", connId.String(),
			"rollback_slot", point.Slot,
			"header_count", headerCount,
		)
		ls.chainsyncBlockfetchMutex.Lock()
		if ls.chainsyncBlockfetchReadyChan == nil {
			if err := ls.startQueuedBlockfetchLocked(connId); err != nil {
				// Recovery connection may have closed. Retry with
				// the current active best peer before giving up,
				// otherwise the pipeline stalls until restart.
				if ls.config.GetActiveConnectionFunc != nil {
					if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil &&
						!sameConnectionId(*activeConnId, connId) {
						ls.config.Logger.Info(
							"local rollback recovery connection unavailable, retrying with active best peer",
							"component", "ledger",
							"failed_connection_id", connId.String(),
							"active_connection_id", activeConnId.String(),
							"error", err,
						)
						if retryErr := ls.startQueuedBlockfetchLocked(*activeConnId); retryErr == nil {
							err = nil
						} else {
							err = retryErr
						}
					}
				}
				if err != nil {
					ls.config.Logger.Warn(
						"failed to start blockfetch after local rollback recovery",
						"component", "ledger",
						"connection_id", connId.String(),
						"error", err,
					)
				}
			}
		}
		ls.chainsyncBlockfetchMutex.Unlock()
		return LocalRollbackRecoveryResult{Recovered: true}
	}
	return LocalRollbackRecoveryResult{}
}

func (ls *LedgerState) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	// Detect connection switch so pipeline ownership is handed off
	// even when the first post-switch event is a header rather than
	// a rollback. Without this, headers from a newly-selected active
	// connection are buffered indefinitely because the pipeline owner
	// still points to the old (dead) connection.
	ls.detectConnectionSwitch()

	// Track upstream tip for sync progress reporting
	if e.Tip.Point.Slot > ls.syncUpstreamTipSlot.Load() {
		ls.syncUpstreamTipSlot.Store(e.Tip.Point.Slot)
	}

	// Verify header crypto before accepting it into the header queue.
	// Skip during historical sync (validationEnabled=false) because
	// historical blocks were already validated by the network and the
	// epoch nonce may not be fully computed yet (e.g. Byron→Shelley).
	if ls.validationEnabled {
		if err := ls.verifyBlockHeaderOnlyCrypto(e.BlockHeader); err != nil {
			if ls.config.EventBus != nil {
				ls.config.Logger.Warn(
					"recycling connection after header verification failure",
					"component", "ledger",
					"connection_id", e.ConnectionId.String(),
					"slot", e.Point.Slot,
					"hash", hex.EncodeToString(e.Point.Hash),
				)
				ls.config.EventBus.Publish(
					connmanager.ConnectionRecycleRequestedEventType,
					event.NewEvent(
						connmanager.ConnectionRecycleRequestedEventType,
						connmanager.ConnectionRecycleRequestedEvent{
							ConnectionId: e.ConnectionId,
							Reason:       "header_verification_failure",
						},
					),
				)
			}
			return fmt.Errorf(
				"block header crypto verification failed: %w",
				err,
			)
		}
	}

	if ls.chainsyncState == RollbackChainsyncState {
		ls.config.Logger.Info(
			fmt.Sprintf(
				"ledger: switched to fork at %d.%s",
				e.Point.Slot,
				hex.EncodeToString(e.Point.Hash),
			),
		)
		ls.metrics.forks.Add(1)
	}
	ls.chainsyncState = SyncingChainsyncState
	ls.recordPeerHeaderHistory(e)
	if ls.shouldBufferHeaderEvent(e) {
		return nil
	}
	// Allow us to build up a few blockfetch batches worth of headers,
	// but never exceed the chain's actual header queue capacity.
	allowedHeaderCount := min(
		blockfetchBatchSize*4,
		ls.chain.MaxQueuedHeaders(),
	)
	headerCount := ls.chain.HeaderCount()

	// Add header to chain
	ls.config.Logger.Debug(
		"chainsync header handler entered",
		"component", "ledger",
		"slot", e.Point.Slot,
		"tip_slot", e.Tip.Point.Slot,
		"header_count", headerCount,
		"connection_id", e.ConnectionId.String(),
	)
	if err := ls.chain.AddBlockHeader(e.BlockHeader); err != nil {
		var notFitErr chain.BlockNotFitChainTipError
		if errors.As(err, &notFitErr) {
			localTip := ls.chain.Tip()
			if e.Point.Slot <= localTip.Point.Slot {
				ls.config.Logger.Debug(
					"ignoring stale roll forward behind local tip",
					"component", "ledger",
					"slot", e.Point.Slot,
					"local_tip_slot", localTip.Point.Slot,
					"block_prev_hash", notFitErr.BlockPrevHash(),
					"chain_tip_hash", notFitErr.TipHash(),
					"connection_id", e.ConnectionId.String(),
				)
				return nil
			}
			// Header doesn't fit current chain tip. Clear stale queued
			// headers so subsequent headers are evaluated against the
			// block tip rather than perpetuating the mismatch.
			ls.clearQueuedHeaders()
			ls.headerMismatchCount++
			ls.config.Logger.Debug(
				"block header does not fit chain tip",
				"component", "ledger",
				"slot", e.Point.Slot,
				"block_prev_hash", notFitErr.BlockPrevHash(),
				"chain_tip_hash", notFitErr.TipHash(),
				"consecutive_mismatches", ls.headerMismatchCount,
			)
			// The incoming header's prevHash is the block it extends
			// from — the common ancestor. If that block exists on our
			// chain and the peer's chain is ahead, we roll back to
			// the common ancestor so chainsync can continue.
			resolved, resolveErr := ls.tryResolveFork(
				e, notFitErr,
			)
			if resolveErr != nil {
				if ls.headerMismatchCount > 0 {
					ls.headerMismatchCount--
				}
				return fmt.Errorf(
					"failed resolving fork after header mismatch: %w",
					resolveErr,
				)
			}
			if resolved {
				return nil
			}
			// Fallback: after several consecutive mismatches where
			// we couldn't find the common ancestor, trigger a
			// chainsync re-sync by closing the connection so the
			// peer governance reconnects and negotiates a fresh
			// intersection.
			if ls.headerMismatchCount >= headerMismatchResyncThreshold &&
				ls.config.EventBus != nil {
				ls.config.Logger.Info(
					"persistent chain fork detected, triggering chainsync re-sync",
					"component", "ledger",
					"connection_id", e.ConnectionId.String(),
					"consecutive_mismatches", ls.headerMismatchCount,
				)
				ls.requestChainsyncResync(
					e.ConnectionId,
					"persistent chain fork",
				)
			}
			return nil
		}
		return fmt.Errorf("failed adding chain block header: %w", err)
	}
	// Reset mismatch counter on successful header addition
	ls.headerMismatchCount = 0
	// Wait for additional block headers before fetching block bodies if we're
	// far enough out from upstream tip
	// Use security window as slot threshold if available
	headersReady := headerCount + 1
	// Use the primary chain header tip so the slot and block-number gaps reflect
	// fetched-but-unprocessed blocks that are already queued in ls.chain.
	localChainTip := ochainsync.Tip{}
	if ls.chain != nil {
		localChainTip = ls.chain.HeaderTip()
	}
	localTipSlot := localChainTip.Point.Slot
	blockGap := uint64(0)
	if e.Tip.BlockNumber > localChainTip.BlockNumber {
		blockGap = e.Tip.BlockNumber - localChainTip.BlockNumber
	}
	if e.Tip.Point.Slot > localTipSlot &&
		e.Tip.Point.Slot-localTipSlot >= blockfetchMinBatchGapSlots {
		minBatchHeaders := desiredBlockfetchBatchHeaders(
			e.Tip.Point.Slot-localTipSlot,
			blockGap,
			allowedHeaderCount,
		)
		if headersReady < minBatchHeaders {
			ls.config.Logger.Debug(
				"accumulating minimum header batch before blockfetch",
				"component", "ledger",
				"slot", e.Point.Slot,
				"tip_slot", e.Tip.Point.Slot,
				"local_tip_slot", localTipSlot,
				"header_count", headersReady,
				"minimum_header_count", minBatchHeaders,
			)
			return nil
		}
	}
	slotThreshold := ls.calculateStabilityWindow()
	if e.Point.Slot < e.Tip.Point.Slot &&
		(e.Tip.Point.Slot-e.Point.Slot > slotThreshold) &&
		headersReady < allowedHeaderCount {
		ls.config.Logger.Debug(
			"accumulating headers (far from tip)",
			"component", "ledger",
			"slot", e.Point.Slot,
			"tip_slot", e.Tip.Point.Slot,
			"threshold", slotThreshold,
			"header_count", headersReady,
		)
		return nil
	}
	// We use the blockfetch lock to ensure we aren't starting a batch at the same
	// time as blockfetch starts a new one to avoid deadlocks
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	// Don't start fetch if there's already one in progress
	if ls.chainsyncBlockfetchReadyChan != nil {
		ls.config.Logger.Debug(
			"blockfetch in progress, queuing header",
			"component", "ledger",
			"slot", e.Point.Slot,
			"header_count", ls.chain.HeaderCount(),
		)
		return nil
	}
	// Mark blockfetch as in progress
	ls.selectedBlockfetchConnId = e.ConnectionId
	initialConnId := ls.selectInitialBlockfetchConn(e.ConnectionId)
	ls.config.Logger.Debug(
		"starting blockfetch",
		"component", "ledger",
		"connection_id", initialConnId.String(),
		"header_count", ls.chain.HeaderCount(),
	)
	err := ls.startQueuedBlockfetchLocked(initialConnId)
	if err != nil {
		// The chosen connection's blockfetch protocol may have shut
		// down. Try the header source connection if it's different;
		// otherwise try the active best peer before giving up.
		if !sameConnectionId(e.ConnectionId, initialConnId) {
			ls.config.Logger.Warn(
				"blockfetch start failed, retrying on header source connection",
				"component", "ledger",
				"failed_connection_id", initialConnId.String(),
				"retry_connection_id", e.ConnectionId.String(),
				"error", err,
			)
			if retryErr := ls.startQueuedBlockfetchLocked(e.ConnectionId); retryErr == nil {
				return nil
			}
		}
		// Header source also failed (or was the same connection).
		// Try the current active best peer as a last resort before
		// clearing state — otherwise every subsequent header will
		// fail the same stale lookup.
		if ls.config.GetActiveConnectionFunc != nil {
			if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil &&
				!sameConnectionId(*activeConnId, initialConnId) &&
				!sameConnectionId(*activeConnId, e.ConnectionId) {
				ls.config.Logger.Info(
					"blockfetch connections unavailable, retrying with active best peer",
					"component", "ledger",
					"failed_connection_id", initialConnId.String(),
					"active_connection_id", activeConnId.String(),
					"error", err,
				)
				ls.selectedBlockfetchConnId = *activeConnId
				if retryErr := ls.startQueuedBlockfetchLocked(*activeConnId); retryErr == nil {
					return nil
				}
			}
		}
		// All fallbacks exhausted. Clear stale state so the next
		// header can start a fresh blockfetch attempt.
		ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.requestChainsyncResync(
			initialConnId,
			fmt.Sprintf("blockfetch start failed: %v", err),
		)
		return nil
	}
	return nil
}

// tryResolveFork attempts to resolve a chain fork when an incoming header
// doesn't fit the local chain tip. The incoming header's prevHash identifies
// a block that exists on our local chain. If the peer's immediate prevHash
// is already unknown to us, the connection's chainsync cursor has drifted
// out of continuity with the local header queue and the correct recovery is
// a fresh FindIntersect on that connection rather than repeated mismatch
// counting.
//
// Returns true if the fork was resolved (chain rolled back or a re-sync was
// requested), false if the common ancestor was not found yet. Unexpected
// internal failures are returned as errors so callers do not treat them as
// ordinary header mismatches.
func (ls *LedgerState) tryResolveFork(
	e ChainsyncEvent,
	notFitErr chain.BlockNotFitChainTipError,
) (bool, error) {
	// Only resolve forks when the peer is ahead of us.
	localTip := ls.chain.Tip()
	if e.Tip.Point.Slot <= localTip.Point.Slot {
		return false, nil
	}

	// Walk backward through the peer's recently seen header chain until
	// we find a hash that exists locally. The current header's prevHash is
	// only the common ancestor when the peer is handing us the first header
	// after the fork point; once the winning fork is several headers ahead,
	// we need the peer's recent ancestry to locate the real rollback point.
	prevHashBytes, err := hex.DecodeString(notFitErr.BlockPrevHash())
	if err != nil {
		ls.config.Logger.Warn(
			"failed to decode block prev hash for fork resolution",
			"component", "ledger",
			"error", err,
			"block_prev_hash", notFitErr.BlockPrevHash(),
		)
		return false, nil
	}
	ancestorPoint, forkPath, err := ls.findPeerForkPath(e, prevHashBytes)
	if err != nil {
		return false, fmt.Errorf(
			"unexpected error looking up common ancestor for prev hash %s: %w",
			notFitErr.BlockPrevHash(),
			err,
		)
	}
	if ancestorPoint == nil {
		// The peer's header stream is not continuous with our local chain
		// view or we have not yet seen enough of its ancestry to resolve
		// the fork locally. Request a chainsync re-sync so the intersect
		// protocol finds the common point with the peer.
		ls.config.Logger.Debug(
			"common ancestor not found locally, triggering chainsync re-sync",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"block_prev_hash", notFitErr.BlockPrevHash(),
		)
		ls.requestChainsyncResync(
			e.ConnectionId,
			resyncReasonRollbackNotFound,
		)
		return true, nil
	}
	ancestorBlock, err := database.BlockByHash(ls.db, ancestorPoint.Hash)
	if err != nil {
		return false, fmt.Errorf(
			"failed to reload common ancestor block %s: %w",
			hex.EncodeToString(ancestorPoint.Hash),
			err,
		)
	}

	rollbackPoint := *ancestorPoint

	// If the ancestor IS the local tip, the peer's fork segment extends
	// our chain directly — no rollback is needed. Skip the rollback to
	// avoid publishing a "local ledger rollback" event that would
	// trigger recovery and close all connections unnecessarily.
	if rollbackPoint.Slot == localTip.Point.Slot &&
		bytes.Equal(rollbackPoint.Hash, localTip.Point.Hash) {
		ls.config.Logger.Info(
			"fork extends from current tip, adding headers without rollback",
			"component", "ledger",
			"local_tip_slot", localTip.Point.Slot,
			"peer_tip_slot", e.Tip.Point.Slot,
			"fork_path_headers", len(forkPath),
			"connection_id", e.ConnectionId.String(),
		)
		for _, forkEvent := range forkPath {
			if err := ls.chain.AddBlockHeader(forkEvent.BlockHeader); err != nil {
				ls.config.Logger.Warn(
					"failed to queue header from fork extension",
					"component", "ledger",
					"error", err,
					"slot", forkEvent.Point.Slot,
					"connection_id", forkEvent.ConnectionId.String(),
				)
				return false, nil
			}
		}
		ls.headerMismatchCount = 0
		ls.rollbackHistory = nil
		if ls.config.BlockfetchRequestRangeFunc != nil &&
			ls.chain.HeaderCount() > 0 {
			ls.chainsyncBlockfetchMutex.Lock()
			if err := ls.restartQueuedBlockfetchAfterForkLocked(e.ConnectionId); err != nil {
				ls.config.Logger.Warn(
					"failed to start blockfetch after fork extension",
					"component", "ledger",
					"error", err,
					"connection_id", e.ConnectionId.String(),
				)
			}
			ls.chainsyncBlockfetchMutex.Unlock()
		}
		return true, nil
	}

	ls.config.Logger.Info(
		"fork detected: rolling back to common ancestor",
		"component", "ledger",
		"local_tip_slot", localTip.Point.Slot,
		"peer_tip_slot", e.Tip.Point.Slot,
		"ancestor_slot", ancestorBlock.Slot,
		"ancestor_hash", hex.EncodeToString(ancestorBlock.Hash),
		"connection_id", e.ConnectionId.String(),
	)

	if err := ls.rollbackChainAndState(rollbackPoint); err != nil {
		if errors.Is(err, chain.ErrRollbackExceedsSecurityParam) {
			// Fork exceeds security parameter K. We must not
			// follow a chain that requires rolling back more
			// than K blocks — this is a fundamental Ouroboros
			// security guarantee. Trigger a chainsync re-sync
			// immediately rather than waiting for
			// headerMismatchResyncThreshold retries.
			ls.config.Logger.Error(
				"fork exceeds security parameter K, "+
					"rejecting fork resolution",
				"component", "ledger",
				"ancestor_slot", ancestorBlock.Slot,
				"local_tip_slot",
				ls.chain.Tip().Point.Slot,
				"peer_tip_slot", e.Tip.Point.Slot,
			)
			// Reset mismatch state so the fallback path in the
			// caller does not fire a duplicate resync event.
			ls.headerMismatchCount = 0
			ls.rollbackHistory = nil
			if ls.config.EventBus != nil {
				ls.config.EventBus.Publish(
					event.ChainsyncResyncEventType,
					event.NewEvent(
						event.ChainsyncResyncEventType,
						event.ChainsyncResyncEvent{
							ConnectionId: e.ConnectionId,
							Reason:       "fork resolution exceeds security parameter K",
						},
					),
				)
			}
		} else {
			ls.config.Logger.Error(
				"failed to roll back to common ancestor",
				"component", "ledger",
				"error", err,
				"ancestor_slot", ancestorBlock.Slot,
			)
		}
		return false, nil
	}

	// Mark state as rollback so the next block header event logs
	// "switched to fork" and increments the fork metric.
	ls.chainsyncState = RollbackChainsyncState

	// Rollback succeeded — re-add the known peer fork segment from the
	// common ancestor forward. Re-adding only the latest mismatching header
	// works for one-block forks but fails once the winning fork is already
	// several headers ahead.
	for _, forkEvent := range forkPath {
		if err := ls.chain.AddBlockHeader(forkEvent.BlockHeader); err != nil {
			ls.config.Logger.Warn(
				"failed to queue header after fork rollback",
				"component", "ledger",
				"error", err,
				"slot", forkEvent.Point.Slot,
				"connection_id", forkEvent.ConnectionId.String(),
			)
			// Do not reset mismatch state — let the caller know the
			// resolution failed so subsequent mismatch tracking proceeds.
			return false, nil
		}
	}
	ls.headerMismatchCount = 0
	ls.rollbackHistory = nil
	if ls.config.BlockfetchRequestRangeFunc != nil &&
		ls.chain.HeaderCount() > 0 {
		ls.chainsyncBlockfetchMutex.Lock()
		if err := ls.restartQueuedBlockfetchAfterForkLocked(e.ConnectionId); err != nil {
			ls.config.Logger.Warn(
				"failed to start blockfetch after fork rollback",
				"component", "ledger",
				"error", err,
				"connection_id", e.ConnectionId.String(),
			)
		}
		ls.chainsyncBlockfetchMutex.Unlock()
	}
	return true, nil
}

//nolint:unparam
func (ls *LedgerState) handleEventBlockfetchBlock(e BlockfetchEvent) error {
	// Process blocks in small commit batches so they appear on the
	// chain promptly without paying a full blob transaction cost for
	// every single block. We still flush well before BatchDone to
	// avoid downstream ChainSync idle timeouts.
	if ls.chainsyncBlockfetchReadyChan == nil ||
		!sameConnectionId(e.ConnectionId, ls.activeBlockfetchConnId) {
		return nil
	}

	// Verify block header cryptographic proofs (VRF, KES).
	// Skip during historical sync (validationEnabled=false) because
	// historical blocks were already validated by the network.
	if ls.validationEnabled {
		// Chainsync already verified the queued header before blockfetch started.
		// When the fetched block matches that first queued header by point, a
		// second VRF/KES verification is redundant. Chain insertion still checks
		// that the block matches the queued header hash before accepting it.
		if !ls.chain.FirstHeaderMatchesPoint(e.Point) {
			if err := ls.verifyBlockHeaderCrypto(e.Block); err != nil {
				return fmt.Errorf(
					"block header crypto verification failed: %w",
					err,
				)
			}
		}
	}
	ls.pendingBlockfetchEvents = append(ls.pendingBlockfetchEvents, e)
	ls.batchBlocksReceived++
	if len(ls.pendingBlockfetchEvents) >= blockfetchCommitBatchSize {
		if err := ls.flushPendingBlockfetchBlocks(); err != nil {
			return err
		}
	}
	// Reset timeout timer since we received a block
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Reset(blockfetchBusyTimeout)
	}
	return nil
}

func (ls *LedgerState) nextBlockfetchConnId() (ouroboros.ConnectionId, bool) {
	if connIdKey(ls.selectedBlockfetchConnId) != "" {
		return ls.selectedBlockfetchConnId, true
	}
	if connIdKey(ls.activeBlockfetchConnId) == "" {
		return ouroboros.ConnectionId{}, false
	}
	return ls.activeBlockfetchConnId, true
}

func (ls *LedgerState) nextBlockfetchConnIdExcept(
	excludedConnId ouroboros.ConnectionId,
) (ouroboros.ConnectionId, bool) {
	if connIdKey(ls.selectedBlockfetchConnId) != "" &&
		!sameConnectionId(ls.selectedBlockfetchConnId, excludedConnId) {
		return ls.selectedBlockfetchConnId, true
	}
	if connIdKey(ls.activeBlockfetchConnId) == "" ||
		sameConnectionId(ls.activeBlockfetchConnId, excludedConnId) {
		return ouroboros.ConnectionId{}, false
	}
	return ls.activeBlockfetchConnId, true
}

func (ls *LedgerState) restartQueuedBlockfetchAfterForkLocked(
	connId ouroboros.ConnectionId,
) error {
	if ls.chainsyncBlockfetchReadyChan != nil {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
			ls.chainsyncBlockfetchTimeoutTimer = nil
		}
		ls.chainsyncBlockfetchTimerGeneration++
		ls.chainsyncBlockfetchReadyMutex.Lock()
		if ls.chainsyncBlockfetchReadyChan != nil {
			close(ls.chainsyncBlockfetchReadyChan)
			ls.chainsyncBlockfetchReadyChan = nil
		}
		ls.chainsyncBlockfetchReadyMutex.Unlock()
		if err := ls.flushPendingBlockfetchBlocks(); err != nil {
			ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
			ls.selectedBlockfetchConnId = ouroboros.ConnectionId{}
			return fmt.Errorf(
				"failed to flush stale blockfetch batch before restart: %w",
				err,
			)
		}
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	}
	ls.selectedBlockfetchConnId = connId
	return ls.startQueuedBlockfetchLocked(connId)
}

func (ls *LedgerState) startQueuedBlockfetchLocked(
	connId ouroboros.ConnectionId,
) error {
	if ls.chain.HeaderCount() == 0 {
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return nil
	}
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = connId
	ls.batchBlocksReceived = 0
	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	if err := ls.blockfetchRequestRangeStart(
		connId,
		headerStart,
		headerEnd,
	); err != nil {
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return err
	}
	return nil
}

func (ls *LedgerState) flushPendingBlockfetchBlocks() error {
	if len(ls.pendingBlockfetchEvents) == 0 {
		return nil
	}
	pending := ls.pendingBlockfetchEvents
	ls.pendingBlockfetchEvents = ls.pendingBlockfetchEvents[:0]
	// Commit each block before exposing it on the primary chain. The chain tip
	// is used immediately by fork detection, so batching blob writes behind an
	// already-advanced in-memory tip can strand the node on a fork when ancestor
	// lookups hit uncommitted state.
	for _, pendingEvent := range pending {
		addBlockErr := ls.chain.AddBlockWithPoint(
			pendingEvent.Block,
			pendingEvent.Point,
			nil,
		)
		if addBlockErr == nil {
			ls.checkSlotBattle(pendingEvent, nil)
			continue
		}
		var notFitErr chain.BlockNotFitChainTipError
		var notMatchErr chain.BlockNotMatchHeaderError
		ignored := errors.As(addBlockErr, &notFitErr) ||
			errors.As(addBlockErr, &notMatchErr)
		if !ignored {
			return fmt.Errorf(
				"failed processing block event: add chain block: %w",
				addBlockErr,
			)
		}
		ls.config.Logger.Warn(
			fmt.Sprintf(
				"ignoring blockfetch block: %s",
				addBlockErr,
			),
		)
		if errors.As(addBlockErr, &notMatchErr) {
			ls.clearQueuedHeaders()
		}
		ls.checkSlotBattle(pendingEvent, addBlockErr)
	}
	ls.chain.NotifyIterators()
	return nil
}

// GenesisBlockHash returns the Byron genesis hash from config, which is used
// as the block hash for the synthetic genesis block that holds genesis UTxO data.
// This mirrors how the Shelley epoch nonce uses the Shelley genesis hash.
func GenesisBlockHash(cfg *cardano.CardanoNodeConfig) ([32]byte, error) {
	if cfg == nil || cfg.ByronGenesisHash == "" {
		return [32]byte{}, errors.New(
			"byron genesis hash not available in config",
		)
	}
	hashBytes, err := hex.DecodeString(cfg.ByronGenesisHash)
	if err != nil {
		return [32]byte{}, fmt.Errorf("decode Byron genesis hash: %w", err)
	}
	if len(hashBytes) != 32 {
		return [32]byte{}, fmt.Errorf(
			"invalid Byron genesis hash length: expected 32 bytes, got %d",
			len(hashBytes),
		)
	}
	var hash [32]byte
	copy(hash[:], hashBytes)
	return hash, nil
}

func (ls *LedgerState) createGenesisBlock() error {
	// Get the Byron genesis hash to use as the synthetic block hash.
	// This mirrors how the Shelley epoch nonce uses the Shelley genesis hash.
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	if err != nil {
		return fmt.Errorf("get genesis block hash: %w", err)
	}

	if ls.currentTip.Point.Slot > 0 {
		// Validate existing chain data matches the current genesis config.
		// If genesis CBOR exists in the blob store with the expected hash,
		// the database was created with a matching genesis — nothing to do.
		if ls.db.HasGenesisCbor(0, genesisHash[:]) {
			return nil
		}
		// Check if genesis CBOR exists but with a different hash.
		// This indicates the database was created for a different
		// network (e.g., mainnet DB with preview config) — fail fast.
		if ls.db.HasAnyGenesisCbor(0) {
			ls.config.Logger.Warn(
				"slot-0 CBOR exists but does not match synthetic genesis hash, creating genesis block",
				"component", "ledger",
				"expected_hash", hex.EncodeToString(genesisHash[:]),
			)
		}
		// Genesis CBOR missing (e.g., after Mithril bootstrap which
		// imports ledger state and ImmutableDB blocks but does not
		// create the synthetic genesis block). Fall through to
		// create it now. All storage operations are idempotent.
		ls.config.Logger.Info(
			"genesis block CBOR missing, creating it now",
			"component", "ledger",
		)
	}

	txn := ls.db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Record genesis UTxOs
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Byron genesis UTxOs: %w", err)
		}
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		shelleyGenesisUtxos, err := shelleyGenesis.GenesisUtxos()
		if err != nil {
			return fmt.Errorf("generate Shelley genesis UTxOs: %w", err)
		}
		if len(byronGenesisUtxos)+len(shelleyGenesisUtxos) == 0 {
			return errors.New("failed to generate genesis UTxOs")
		}
		ls.config.Logger.Info(
			fmt.Sprintf("creating %d genesis UTxOs (%d Byron, %d Shelley)",
				len(byronGenesisUtxos)+len(shelleyGenesisUtxos),
				len(byronGenesisUtxos),
				len(shelleyGenesisUtxos),
			),
			"component", "ledger",
		)

		// Group genesis UTxOs by transaction hash
		genesisUtxos := slices.Concat(byronGenesisUtxos, shelleyGenesisUtxos)
		txUtxos := make(map[[32]byte][]lcommon.Utxo)
		for i := range genesisUtxos {
			txHash := genesisUtxos[i].Id.Id()
			var txHashArray [32]byte
			copy(txHashArray[:], txHash.Bytes())

			// Generate CBOR for genesis UTxO outputs since they don't have original CBOR
			cborData, err := cbor.Encode(genesisUtxos[i].Output)
			if err != nil {
				return fmt.Errorf("encode genesis UTxO output to CBOR: %w", err)
			}

			// Create a new Utxo with CBOR-encoded output
			var newOutput lcommon.TransactionOutput
			switch output := genesisUtxos[i].Output.(type) {
			case byron.ByronTransactionOutput:
				newByronOutput := output
				(&newByronOutput).SetCbor(cborData)
				newOutput = newByronOutput
			case shelley.ShelleyTransactionOutput:
				newShelleyOutput := output
				(&newShelleyOutput).SetCbor(cborData)
				newOutput = newShelleyOutput
			default:
				return fmt.Errorf("unsupported genesis UTxO output type: %T", genesisUtxos[i].Output)
			}

			txUtxos[txHashArray] = append(txUtxos[txHashArray], lcommon.Utxo{
				Id:     genesisUtxos[i].Id,
				Output: newOutput,
			})
		}

		// Build synthetic genesis block with proper structure:
		// Block -> Transactions -> Outputs (UTxOs)
		//
		// CBOR structure:
		// [                                    // block: array of transactions
		//   {0: tx_hash, 1: [output, ...]},    // transaction 1
		//   {0: tx_hash, 1: [output, ...]},    // transaction 2
		//   ...
		// ]
		//
		// We track byte offsets for each output within this structure.
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)

		// Sort transaction hashes for deterministic ordering
		txHashes := make([][32]byte, 0, len(txUtxos))
		for txHash := range txUtxos {
			txHashes = append(txHashes, txHash)
		}
		slices.SortFunc(txHashes, func(a, b [32]byte) int {
			return bytes.Compare(a[:], b[:])
		})

		// Build the block structure manually to track exact byte offsets
		// We need to know where each output CBOR starts within the block
		blockCbor, err := buildGenesisBlockCbor(
			txHashes,
			txUtxos,
			utxoOffsets,
			genesisHash,
		)
		if err != nil {
			return fmt.Errorf("build genesis block cbor: %w", err)
		}

		// Store synthetic genesis block CBOR.
		// We use SetGenesisCbor to avoid creating a block index entry that
		// would cause the chain iterator to include it (genesis is already
		// handled separately during initialization).
		if err := ls.db.SetGenesisCbor(0, genesisHash[:], blockCbor, txn); err != nil {
			return fmt.Errorf("store genesis cbor: %w", err)
		}

		// Store each genesis transaction with its UTxOs
		for txHashArray, utxos := range txUtxos {
			if err := ls.db.SetGenesisTransaction(
				txHashArray[:],
				genesisHash[:],
				utxos,
				utxoOffsets,
				txn,
			); err != nil {
				return fmt.Errorf(
					"set genesis transaction %x: %w",
					txHashArray[:8],
					err,
				)
			}
		}

		ls.config.Logger.Info(
			fmt.Sprintf("stored %d genesis transactions with %d total UTxOs",
				len(txUtxos),
				len(genesisUtxos),
			),
			"component", "ledger",
		)

		// Load genesis staking data (pool registrations + delegations)
		genesisPools, _, err := shelleyGenesis.InitialPools()
		if err != nil {
			return fmt.Errorf("parse genesis staking: %w", err)
		}
		if len(genesisPools) > 0 ||
			len(shelleyGenesis.Staking.Stake) > 0 {
			ls.config.Logger.Info(
				fmt.Sprintf(
					"loading genesis staking: %d pools, %d delegations",
					len(genesisPools),
					len(shelleyGenesis.Staking.Stake),
				),
				"component", "ledger",
			)
			if err := ls.db.SetGenesisStaking(
				genesisPools,
				shelleyGenesis.Staking.Stake,
				genesisHash[:],
				txn,
			); err != nil {
				return fmt.Errorf("set genesis staking: %w", err)
			}
		}

		return nil
	})
	return err
}

// buildGenesisBlockCbor creates a CBOR structure representing a synthetic
// genesis block containing transactions with outputs. The structure is:
//
//	[                                    // block: array of transactions
//	  {0: tx_hash, 1: [output, ...]},    // transaction 1
//	  {0: tx_hash, 1: [output, ...]},    // transaction 2
//	  ...
//	]
//
// It populates utxoOffsets with the byte offset of each output within the block.
// Unlike a search-based approach, this function tracks exact byte positions during
// CBOR construction to avoid any possibility of false matches.
// The blockHash parameter is the Byron genesis hash used as the synthetic block hash.
func buildGenesisBlockCbor(
	txHashes [][32]byte,
	txUtxos map[[32]byte][]lcommon.Utxo,
	utxoOffsets map[database.UtxoRef]database.CborOffset,
	blockHash [32]byte,
) ([]byte, error) {
	var buf bytes.Buffer

	// Write outer array header for transactions
	writeCborArrayHeader(&buf, len(txHashes))

	for _, txHash := range txHashes {
		utxos := txUtxos[txHash]

		// Sort outputs by index for deterministic ordering
		slices.SortFunc(utxos, func(a, b lcommon.Utxo) int {
			ai, bi := uint64(a.Id.Index()), uint64(b.Id.Index())
			if ai < bi {
				return -1
			} else if ai > bi {
				return 1
			}
			return 0
		})

		// Write map header with 2 entries: {0: txhash, 1: outputs}
		writeCborMapHeader(&buf, 2)

		// Key 0: tx hash
		writeCborUint(&buf, 0)
		writeCborBytes(&buf, txHash[:])

		// Key 1: outputs array
		writeCborUint(&buf, 1)
		writeCborArrayHeader(&buf, len(utxos))

		// Write each output, tracking offsets
		for _, utxo := range utxos {
			outputCbor := utxo.Output.Cbor()
			if len(outputCbor) == 0 {
				var err error
				outputCbor, err = cbor.Encode(utxo.Output)
				if err != nil {
					return nil, fmt.Errorf("encode output: %w", err)
				}
			}

			// Record offset BEFORE writing the output
			offset := buf.Len()
			outputLen := len(outputCbor)

			// Validate sizes fit in uint32 (fail fast instead of silent truncation)
			if offset > math.MaxUint32 {
				return nil, fmt.Errorf(
					"genesis CBOR offset %d exceeds uint32 max",
					offset,
				)
			}
			if outputLen > math.MaxUint32 {
				return nil, fmt.Errorf(
					"genesis output CBOR length %d exceeds uint32 max",
					outputLen,
				)
			}

			buf.Write(outputCbor)

			ref := database.UtxoRef{
				TxId:      txHash,
				OutputIdx: utxo.Id.Index(),
			}
			//nolint:gosec // uint32 bounds checked above
			utxoOffsets[ref] = database.CborOffset{
				BlockSlot:  0,
				BlockHash:  blockHash,
				ByteOffset: uint32(offset),
				ByteLength: uint32(outputLen),
			}
		}
	}

	return buf.Bytes(), nil
}

// writeCborArrayHeader writes a CBOR array header for n elements.
func writeCborArrayHeader(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 4, n) // Major type 4 = array
}

// writeCborMapHeader writes a CBOR map header for n pairs.
func writeCborMapHeader(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 5, n) // Major type 5 = map
}

// writeCborBytes writes a CBOR byte string.
func writeCborBytes(buf *bytes.Buffer, data []byte) {
	writeCborMajorType(buf, 2, len(data)) // Major type 2 = byte string
	buf.Write(data)
}

// writeCborUint writes a CBOR unsigned integer.
func writeCborUint(buf *bytes.Buffer, n int) {
	writeCborMajorType(buf, 0, n) // Major type 0 = unsigned int
}

// writeCborMajorType writes a CBOR header with the given major type and value.
//
//nolint:gosec // Intentional byte truncation for CBOR encoding of individual octets.
func writeCborMajorType(buf *bytes.Buffer, majorType, n int) {
	header := byte(majorType << 5)
	switch {
	case n < 24:
		buf.WriteByte(header | byte(n))
	case n < 256:
		buf.WriteByte(header | 24)
		buf.WriteByte(byte(n))
	case n < 65536:
		buf.WriteByte(header | 25)
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	case n < 4294967296:
		buf.WriteByte(header | 26)
		buf.WriteByte(byte(n >> 24))
		buf.WriteByte(byte(n >> 16))
		buf.WriteByte(byte(n >> 8))
		buf.WriteByte(byte(n))
	default:
		// 8-byte encoding for values >= 2^32
		buf.WriteByte(header | 27)
		val := uint64(n)
		buf.WriteByte(byte(val >> 56))
		buf.WriteByte(byte(val >> 48))
		buf.WriteByte(byte(val >> 40))
		buf.WriteByte(byte(val >> 32))
		buf.WriteByte(byte(val >> 24))
		buf.WriteByte(byte(val >> 16))
		buf.WriteByte(byte(val >> 8))
		buf.WriteByte(byte(val))
	}
}

// calculateEpochNonce computes the epoch nonce for epoch N+1, the
// end-of-epoch evolving nonce, and the labNonce to save for epoch
// N+2's computation.
//
// The Ouroboros Praos formula is:
//
//	epochNonce(N+1) = candidateNonce(N) ⭒ lastEpochBlockNonce(N)
//
// where lastEpochBlockNonce(N) was saved at the N-1→N transition
// (it's the prevHash of the last block of epoch N-1). This value
// is stored in currentEpoch.LastEpochBlockNonce.
//
// The ⭒ operator has NeutralNonce as identity:
//
//	x ⭒ NeutralNonce = x
//
// For the very first epoch transition (0→1), lastEpochBlockNonce
// is nil (NeutralNonce), so epochNonce = candidateNonce.
//
// Returns (epochNonce, evolvingNonce, candidateNonce, labNonce, error).
// The caller must store candidateNonce as the new epoch's CandidateNonce
// and labNonce as the new epoch's LastEpochBlockNonce so the next
// transition can use them.
func (ls *LedgerState) calculateEpochNonce(
	txn *database.Txn,
	epochStartSlot uint64,
	currentEra eras.EraDesc,
	currentEpoch models.Epoch,
) ([]byte, []byte, []byte, []byte, error) {
	// No epoch nonce in Byron
	if currentEra.Id == 0 {
		return nil, nil, nil, nil, nil
	}
	if ls.config.CardanoNodeConfig == nil {
		return nil, nil, nil, nil, errors.New("CardanoNodeConfig is nil")
	}
	if ls.config.CardanoNodeConfig.ShelleyGenesisHash == "" {
		return nil, nil, nil, nil, errors.New(
			"could not get Shelley genesis hash",
		)
	}
	genesisHashBytes, err := hex.DecodeString(
		ls.config.CardanoNodeConfig.ShelleyGenesisHash,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"decode genesis hash: %w", err,
		)
	}

	// For the initial epoch creation (no blocks yet), the epoch
	// nonce and initial evolving nonce are both the genesis hash.
	// This matches cardano-node where the initial state sets
	// epochNonce, candidateNonce, and evolvingNonce all to the
	// genesis hash. lastEpochBlockNonce is nil (NeutralNonce).
	if len(currentEpoch.Nonce) == 0 {
		return genesisHashBytes, genesisHashBytes, genesisHashBytes, nil, nil
	}

	// In Ouroboros Praos, the evolving nonce carries across epoch
	// boundaries without resetting (PrtclState is never reset).
	// For migration compatibility (epochs stored before this
	// field existed), fall back to genesis hash.
	prevEvolvingNonce := currentEpoch.EvolvingNonce
	if len(prevEvolvingNonce) == 0 {
		prevEvolvingNonce = genesisHashBytes
	}

	// The candidate nonce also carries across epochs independently
	// of the evolving nonce. When 4k/f >= epochLength (e.g., short
	// devnet epochs), the candidate is never updated by any block
	// and stays at its carried value. Fall back to genesis hash
	// for epochs stored before this field existed.
	prevCandidateNonce := currentEpoch.CandidateNonce
	if len(prevCandidateNonce) == 0 {
		prevCandidateNonce = genesisHashBytes
	}

	// When importing from a snapshot, currentEpoch may carry tip-time
	// nonce state (evolving/candidate already advanced through the
	// imported tip slot). In that case, continue accumulation from the
	// next slot rather than replaying from epoch start.
	computeStartSlot := currentEpoch.StartSlot
	computeEpochLength := uint64(currentEpoch.LengthInSlots)
	epochEndSlot := currentEpoch.StartSlot +
		uint64(currentEpoch.LengthInSlots)
	ls.RLock()
	tipSlot := ls.currentTip.Point.Slot
	tipBlockNonceCopy := append([]byte(nil), ls.currentTipBlockNonce...)
	ls.RUnlock()
	if tipSlot >= currentEpoch.StartSlot &&
		tipSlot < epochEndSlot &&
		len(currentEpoch.CandidateNonce) == 32 &&
		len(currentEpoch.EvolvingNonce) == 32 &&
		len(tipBlockNonceCopy) == 32 &&
		bytes.Equal(currentEpoch.EvolvingNonce, tipBlockNonceCopy) {
		if nextSlot := tipSlot + 1; nextSlot < epochEndSlot {
			computeStartSlot = nextSlot
			computeEpochLength = epochEndSlot - nextSlot
		} else {
			// Tip already at/after epoch end: no additional blocks to fold.
			computeEpochLength = 0
		}
	} else if len(currentEpoch.EvolvingNonce) == 32 {
		// Resume fallback: if epoch nonce state was checkpointed at an
		// earlier slot (snapshot import), locate that anchor by matching
		// stored block nonces and continue from the following slot.
		// If no anchor is found, fall through to the defaults which
		// compute from epoch start — this is always correct (just
		// slower) and handles genesis sync where the epoch's
		// EvolvingNonce was set at creation and never updated.
		nonceRows, nonceErr := ls.db.GetBlockNoncesInSlotRange(
			currentEpoch.StartSlot,
			epochEndSlot,
			txn,
		)
		if nonceErr != nil {
			return nil, nil, nil, nil, fmt.Errorf(
				"fetch block nonces in epoch range: %w",
				nonceErr,
			)
		}
		for _, row := range nonceRows {
			if len(row.Nonce) == 32 &&
				bytes.Equal(currentEpoch.EvolvingNonce, row.Nonce) {
				if row.Slot+1 < epochEndSlot {
					computeStartSlot = row.Slot + 1
					computeEpochLength = epochEndSlot -
						computeStartSlot
				} else {
					computeEpochLength = 0
				}
				break
			}
		}
	}

	// Compute candidateNonce (frozen at stability window cutoff)
	// and evolvingNonce (after all blocks) from the remaining
	// current-epoch blocks. Each block's VRF output is accumulated
	// via the Nonce semigroup (⭒) starting from prevEvolvingNonce.
	candidateNonce, evolvingNonce, err := ls.computeCandidateNonce(
		txn,
		prevEvolvingNonce,
		prevCandidateNonce,
		computeStartSlot,
		computeEpochLength,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"compute candidate nonce: %w", err,
		)
	}

	// Compute the labNonce to SAVE for epoch N+2's computation.
	// This is prevHashToNonce(prevHash of last block of current
	// epoch N). It will be stored as LastEpochBlockNonce on the
	// new epoch record.
	var labNonceToSave []byte
	blockLastCurrentEpoch, err := database.BlockBeforeSlotTxn(
		txn,
		epochEndSlot,
	)
	if err != nil {
		if !errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil, nil, nil, fmt.Errorf(
				"lookup block before slot: %w", err,
			)
		}
		// No block — labNonceToSave stays nil (NeutralNonce)
	} else if len(blockLastCurrentEpoch.PrevHash) > 0 {
		labNonceToSave = blockLastCurrentEpoch.PrevHash
	}

	// Use the LAGGED lastEpochBlockNonce from the current epoch
	// record (set at the PREVIOUS transition) in the formula.
	// If nil/empty, it's NeutralNonce (identity): result is
	// just candidateNonce.
	lastEpochBlockNonce := currentEpoch.LastEpochBlockNonce
	if len(lastEpochBlockNonce) == 0 {
		// NeutralNonce is the identity element of ⭒:
		//   candidateNonce ⭒ NeutralNonce = candidateNonce
		// So the epoch nonce is just the candidate nonce.
		ls.config.Logger.Debug(
			"epoch nonce computed (NeutralNonce, using candidateNonce)",
			"component", "ledger",
			"epoch_start_slot", epochStartSlot,
			"candidate_nonce",
			hex.EncodeToString(candidateNonce),
			"lab_nonce_to_save",
			hex.EncodeToString(labNonceToSave),
			"epoch_nonce",
			hex.EncodeToString(candidateNonce),
			"evolving_nonce",
			hex.EncodeToString(evolvingNonce),
		)
		return candidateNonce, evolvingNonce, candidateNonce, labNonceToSave, nil
	}

	// candidateNonce ⭒ lastEpochBlockNonce
	// = blake2b_256(candidateNonce || lastEpochBlockNonce)
	if len(candidateNonce) < 32 ||
		len(lastEpochBlockNonce) < 32 {
		return nil, nil, nil, nil, fmt.Errorf(
			"epoch nonce requires 32-byte inputs: "+
				"candidateNonce=%d, lastEpochBlockNonce=%d",
			len(candidateNonce),
			len(lastEpochBlockNonce),
		)
	}
	result, err := lcommon.CalculateEpochNonce(
		candidateNonce,
		lastEpochBlockNonce,
		nil,
	)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"calculate epoch nonce: %w", err,
		)
	}
	ls.config.Logger.Debug(
		"epoch nonce computed",
		"component", "ledger",
		"epoch_start_slot", epochStartSlot,
		"candidate_nonce", hex.EncodeToString(candidateNonce),
		"last_epoch_block_nonce",
		hex.EncodeToString(lastEpochBlockNonce),
		"lab_nonce_to_save",
		hex.EncodeToString(labNonceToSave),
		"epoch_nonce", hex.EncodeToString(result.Bytes()),
		"evolving_nonce", hex.EncodeToString(evolvingNonce),
	)
	return result.Bytes(), evolvingNonce, candidateNonce, labNonceToSave, nil
}

// processEpochRollover processes an epoch rollover and returns the result without
// mutating LedgerState. This allows callers to capture the computed state in a
// transaction and apply it to in-memory state after the transaction commits.
// Parameters:
//   - txn: database transaction
//   - currentEpoch: current epoch (read-only input)
//   - currentEra: current era descriptor (read-only input)
//   - currentPParams: current protocol parameters (read-only input)
//
// Returns EpochRolloverResult with all computed state, or an error.
// The caller is responsible for:
//   - Applying the result to in-memory state after successful commit
//   - Starting background cleanup goroutines
//   - Calling Scheduler.ChangeInterval if SchedulerIntervalMs > 0
func (ls *LedgerState) processEpochRollover(
	txn *database.Txn,
	currentEpoch models.Epoch,
	currentEra eras.EraDesc,
	currentPParams lcommon.ProtocolParameters,
) (*EpochRolloverResult, error) {
	epochStartSlot := currentEpoch.StartSlot + uint64(
		currentEpoch.LengthInSlots,
	)
	result := &EpochRolloverResult{
		CheckpointWrittenForEpoch: false,
		NewCurrentEra:             currentEra,
		NewCurrentPParams:         currentPParams,
	}

	// Create initial epoch
	if currentEpoch.SlotLength == 0 {
		// Create initial epoch record
		epochSlotLength, epochLength, err := currentEra.EpochLengthFunc(
			ls.config.CardanoNodeConfig,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate epoch length: %w", err)
		}
		tmpNonce, tmpEvolvingNonce, tmpCandidateNonce, tmpLabNonce, err := ls.calculateEpochNonce(
			txn,
			0,
			currentEra,
			currentEpoch,
		)
		if err != nil {
			return nil, fmt.Errorf("calculate epoch nonce: %w", err)
		}
		err = ls.db.SetEpoch(
			epochStartSlot,
			0, // epoch
			tmpNonce,
			tmpEvolvingNonce,
			tmpCandidateNonce,
			tmpLabNonce,
			currentEra.Id,
			epochSlotLength,
			epochLength,
			txn,
		)
		if err != nil {
			return nil, fmt.Errorf("set epoch: %w", err)
		}
		// Load epoch info from DB to populate result
		epochs, err := ls.db.GetEpochs(txn)
		if err != nil {
			return nil, fmt.Errorf("load epochs: %w", err)
		}
		result.NewEpochCache = epochs
		if len(epochs) > 0 {
			result.NewCurrentEpoch = epochs[len(epochs)-1]
			eraDesc := eras.GetEraById(result.NewCurrentEpoch.EraId)
			if eraDesc == nil {
				return nil, fmt.Errorf(
					"unknown era ID %d",
					result.NewCurrentEpoch.EraId,
				)
			}
			result.NewCurrentEra = *eraDesc
			result.NewEpochNum = float64(result.NewCurrentEpoch.EpochId)
		}
		ls.config.Logger.Debug(
			"added initial epoch to DB",
			"epoch", fmt.Sprintf("%+v", result.NewCurrentEpoch),
			"component", "ledger",
		)
		return result, nil
	}
	// EPOCH→HARDFORK ordering invariant.
	//
	// The remainder of this function mirrors the sequencing of cardano-
	// ledger's Conway/Rules/Epoch.hs:374-379 (EPOCH STS), which dispatches
	// HARDFORK only after enactment + pparams write. The relative order
	// matters because the HARDFORK rule branch is selected from the new
	// pparams' major version — a HARDFORK rule that ran before enactment
	// would observe stale pparams and pick the wrong branch.
	//
	// The order, asserted by TestProcessEpochRollover_OrderingInvariant in
	// chainsync_ordering_test.go, is:
	//
	//   1. ComputeAndApplyPParamUpdates  — Shelley-style ppuProtocolVersion
	//      voting path; produces newPParams from on-chain pparam-update
	//      proposals.
	//   2. governance.ProcessEpoch       — Conway-style HardForkInitiation /
	//      ParameterChange enactment; may further mutate pparams.
	//   3. SetPParams                    — persist the enacted pparams.
	//   4. IsHardForkTransition check    — detect inter-era boundary from
	//      the now-final pparams.
	//   5. applyIntraEraHardForkRule     — dispatch the per-major-version
	//      HARDFORK STS rule (e.g. pv3 AVVM removal, pv10 DRep clear).
	//
	// Steps 4 and 5 must observe the post-enactment major version. Step 5
	// must observe the persisted pparams (not just the in-memory ones)
	// because its body issues SQL within `txn` that may join against
	// `pparams` rows.
	updateQuorum := 0
	if shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis(); shelleyGenesis != nil {
		updateQuorum = shelleyGenesis.UpdateQuorum
	}
	newPParams, err := ls.db.ComputeAndApplyPParamUpdates(
		epochStartSlot,
		currentEpoch.EpochId+1, // Target epoch for updates
		currentEra.Id,
		updateQuorum,
		currentPParams,
		currentEra.DecodePParamsUpdateFunc,
		currentEra.PParamsUpdateFunc,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("apply pparam updates: %w", err)
	}

	// Run the CIP-1694 governance tick: enact proposals ratified in the
	// previous epoch (possibly mutating pparams), expire stale proposals,
	// and ratify active proposals whose tallies meet threshold. Any
	// pparams change from enactment is persisted via SetPParams so the
	// next epoch's pparams reflect the enacted state.
	var conwayGenesis *conway.ConwayGenesis
	if ls.config.CardanoNodeConfig != nil {
		conwayGenesis = ls.config.CardanoNodeConfig.ConwayGenesis()
	}
	govOut, err := governance.ProcessEpoch(&governance.EpochInput{
		DB:            ls.db,
		Txn:           txn,
		Logger:        ls.config.Logger,
		PrevEpoch:     currentEpoch.EpochId,
		NewEpoch:      currentEpoch.EpochId + 1,
		BoundarySlot:  epochStartSlot,
		PParams:       newPParams,
		UpdateFn:      currentEra.PParamsUpdateFunc,
		ConwayGenesis: conwayGenesis,
	})
	if err != nil {
		return nil, fmt.Errorf("process governance epoch: %w", err)
	}
	if govOut.PParamsChanged {
		newPParams = govOut.UpdatedPParams
		pparamsCbor, encErr := cbor.Encode(&newPParams)
		if encErr != nil {
			return nil, fmt.Errorf(
				"encode post-enactment pparams: %w", encErr,
			)
		}
		if err := ls.db.SetPParams(
			pparamsCbor,
			epochStartSlot,
			currentEpoch.EpochId+1,
			currentEra.Id,
			txn,
		); err != nil {
			return nil, fmt.Errorf(
				"persist post-enactment pparams: %w", err,
			)
		}
	}
	result.NewCurrentPParams = newPParams

	// Check if the protocol version changed in a way that
	// triggers a hard fork (era transition)
	oldVer, oldErr := GetProtocolVersion(currentPParams)
	newVer, newErr := GetProtocolVersion(newPParams)
	if oldErr != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version from "+
				"current pparams, skipping hard fork "+
				"detection",
			"error", oldErr,
			"pparams_type",
			fmt.Sprintf("%T", currentPParams),
			"component", "ledger",
		)
	}
	if newErr != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version from "+
				"new pparams, skipping hard fork "+
				"detection",
			"error", newErr,
			"pparams_type",
			fmt.Sprintf("%T", newPParams),
			"component", "ledger",
		)
	}
	if oldErr == nil && newErr == nil {
		if IsHardForkTransition(oldVer, newVer) {
			fromEra, _ := EraForVersion(oldVer.Major)
			toEra, _ := EraForVersion(newVer.Major)
			result.HardFork = &HardForkInfo{
				OldVersion: oldVer,
				NewVersion: newVer,
				FromEra:    fromEra,
				ToEra:      toEra,
			}
			ls.config.Logger.Info(
				"hard fork detected via protocol "+
					"parameter update",
				"from_era", fromEra,
				"to_era", toEra,
				"old_version",
				fmt.Sprintf(
					"%d.%d",
					oldVer.Major,
					oldVer.Minor,
				),
				"new_version",
				fmt.Sprintf(
					"%d.%d",
					newVer.Major,
					newVer.Minor,
				),
				"epoch",
				currentEpoch.EpochId+1,
				"component", "ledger",
			)
		}
		// Apply cardano-ledger's per-major-version HARDFORK rule. This
		// runs on ANY major-version bump, including intra-era ones like
		// Conway pv9→pv10 (Plomin, mainnet January 2025) that do not
		// trigger an era change, and inter-era ones like Shelley→Allegra
		// (pv2→pv3) that carry a state rewrite. See cardano-ledger
		// Conway/Rules/HardFork.hs and Allegra/Translation.hs.
		if oldVer.Major != newVer.Major {
			if err := ls.applyIntraEraHardForkRule(
				txn, newVer.Major, epochStartSlot, currentEpoch.EpochId+1,
			); err != nil {
				return nil, fmt.Errorf("apply major-version HARDFORK: %w", err)
			}
		}
	}

	// Create next epoch record
	epochSlotLength, epochLength, err := currentEra.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate epoch length: %w", err)
	}
	tmpNonce, tmpEvolvingNonce, tmpCandidateNonce, tmpLabNonce, err := ls.calculateEpochNonce(
		txn,
		epochStartSlot,
		currentEra,
		currentEpoch,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate epoch nonce: %w", err)
	}
	err = ls.db.SetEpoch(
		epochStartSlot,
		currentEpoch.EpochId+1,
		tmpNonce,
		tmpEvolvingNonce,
		tmpCandidateNonce,
		tmpLabNonce,
		currentEra.Id,
		epochSlotLength,
		epochLength,
		txn,
	)
	if err != nil {
		return nil, fmt.Errorf("set epoch: %w", err)
	}
	// Load epoch info from DB to populate result
	epochs, err := ls.db.GetEpochs(txn)
	if err != nil {
		return nil, fmt.Errorf("load epochs: %w", err)
	}
	result.NewEpochCache = epochs
	if len(epochs) > 0 {
		result.NewCurrentEpoch = epochs[len(epochs)-1]
		eraDesc := eras.GetEraById(result.NewCurrentEpoch.EraId)
		if eraDesc == nil {
			return nil, fmt.Errorf(
				"unknown era ID %d",
				result.NewCurrentEpoch.EraId,
			)
		}
		result.NewCurrentEra = *eraDesc
		result.NewEpochNum = float64(result.NewCurrentEpoch.EpochId)
		result.SchedulerIntervalMs = result.NewCurrentEpoch.SlotLength
	}

	ls.config.Logger.Debug(
		"added next epoch to DB",
		"epoch", fmt.Sprintf("%+v", result.NewCurrentEpoch),
		"component", "ledger",
	)
	return result, nil
}

func (ls *LedgerState) cleanupBlockNoncesBefore(startSlot uint64) {
	if startSlot == 0 {
		return
	}
	ls.config.Logger.Debug(
		fmt.Sprintf(
			"cleaning up non-checkpoint block nonces before slot %d",
			startSlot,
		),
		"component",
		"ledger",
	)
	ls.Lock()
	defer ls.Unlock()
	txn := ls.db.Transaction(true)
	if err := txn.Do(func(txn *database.Txn) error {
		return ls.db.DeleteBlockNoncesBeforeSlotWithoutCheckpoints(startSlot, txn)
	}); err != nil {
		ls.config.Logger.Error(
			fmt.Sprintf("failed to clean up old block nonces: %s", err),
			"component", "ledger",
		)
	}
}

// checkSlotBattle checks whether an incoming block from a peer
// occupies a slot for which the local node has already forged a
// block. If so, it emits a SlotBattleEvent and logs a warning.
//
// The addBlockErr parameter is the error (if any) returned by
// chain.AddBlock for the incoming block. A nil error means the
// remote block was accepted onto the chain (remote won); a
// non-nil error means it was rejected (local won).
//
// The caller must hold ls.Lock() (write lock). This method must not
// acquire ls.RLock(), because sync.RWMutex is not reentrant and
// attempting a read lock while holding the write lock deadlocks.
func (ls *LedgerState) checkSlotBattle(
	e BlockfetchEvent,
	addBlockErr error,
) {
	checker := ls.loadForgedBlockChecker()
	if checker == nil {
		return
	}

	incomingSlot := e.Point.Slot
	localHash, forged := checker.WasForgedByUs(incomingSlot)
	if !forged {
		return
	}

	remoteHash := e.Point.Hash

	// Same hash means same block -- not a battle
	if bytes.Equal(localHash, remoteHash) {
		return
	}

	// Determine winner: if the remote block was rejected (addBlockErr
	// != nil), our local block remains on chain, so we won.
	localWon := addBlockErr != nil

	ls.config.Logger.Warn(
		"slot battle detected",
		"component", "ledger",
		"slot", incomingSlot,
		"local_block_hash", hex.EncodeToString(localHash),
		"remote_block_hash", hex.EncodeToString(remoteHash),
		"local_won", localWon,
	)

	// Increment slot battle metric
	if recorder := ls.loadSlotBattleRecorder(); recorder != nil {
		recorder.RecordSlotBattle()
	}

	if ls.config.EventBus != nil {
		ls.config.EventBus.PublishAsync(
			forging.SlotBattleEventType,
			event.NewEvent(
				forging.SlotBattleEventType,
				forging.SlotBattleEvent{
					Slot:            incomingSlot,
					LocalBlockHash:  localHash,
					RemoteBlockHash: remoteHash,
					Won:             localWon,
				},
			),
		)
	}
}

// selectInitialBlockfetchConn starts blockfetch on the same connection that
// delivered the header. This keeps header and block ingress aligned and leaves
// room for future selection logic if a different connection becomes preferable.
func (ls *LedgerState) selectInitialBlockfetchConn(
	headerConnId ouroboros.ConnectionId,
) ouroboros.ConnectionId {
	return headerConnId
}

func (ls *LedgerState) selectRetryBlockfetchConn(
	currentConnId ouroboros.ConnectionId,
) ouroboros.ConnectionId {
	if ls.config.GetActiveConnectionFunc != nil {
		if activeConnId := ls.config.GetActiveConnectionFunc(); activeConnId != nil {
			return *activeConnId
		}
	}
	return currentConnId
}

func (ls *LedgerState) blockfetchRequestRangeStart(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	if ls.config.BlockfetchRequestRangeFunc == nil {
		return errors.New("blockfetch request range func not configured")
	}
	err := ls.config.BlockfetchRequestRangeFunc(
		connId,
		start,
		end,
	)
	if err != nil {
		return fmt.Errorf("request block range: %w", err)
	}

	// Stop any existing timer before creating a new one
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}

	// Increment generation counter to invalidate any pending timer callbacks
	ls.chainsyncBlockfetchTimerGeneration++
	currentGeneration := ls.chainsyncBlockfetchTimerGeneration

	// Start timeout timer for blockfetch operation
	// The timer fires if no blocks are received within blockfetchBusyTimeout
	// Each received block resets the timer in handleEventBlockfetchBlock
	ls.chainsyncBlockfetchTimeoutTimer = time.AfterFunc(
		blockfetchBusyTimeout,
		func() {
			ls.chainsyncBlockfetchMutex.Lock()
			defer ls.chainsyncBlockfetchMutex.Unlock()
			// Check if this timer callback is stale (a newer timer was started)
			if ls.chainsyncBlockfetchTimerGeneration != currentGeneration {
				return
			}
			ls.handleBlockfetchTimeoutLocked(connId)
		},
	)
	return nil
}

func (ls *LedgerState) blockfetchRequestRangeCleanup() {
	// Stop the timeout timer if running and invalidate any pending callbacks
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	// Increment generation to ensure any pending timer callbacks are ignored
	ls.chainsyncBlockfetchTimerGeneration++
	// Close our blockfetch done signal channel
	ls.chainsyncBlockfetchReadyMutex.Lock()
	defer ls.chainsyncBlockfetchReadyMutex.Unlock()
	if ls.chainsyncBlockfetchReadyChan != nil {
		close(ls.chainsyncBlockfetchReadyChan)
		ls.chainsyncBlockfetchReadyChan = nil
	}
	ls.pendingBlockfetchEvents = ls.pendingBlockfetchEvents[:0]
}

func (ls *LedgerState) handleBlockfetchTimeoutLocked(
	currentConnId ouroboros.ConnectionId,
) {
	headerCount := ls.chain.HeaderCount()
	if headerCount == 0 {
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.config.Logger.Info(
			fmt.Sprintf(
				"blockfetch operation timed out after %s",
				blockfetchBusyTimeout,
			),
			"component",
			"ledger",
			"connection_id",
			currentConnId.String(),
		)
		return
	}

	headerStart, headerEnd := ls.chain.HeaderRange(blockfetchBatchSize)
	retryConnId := ls.selectRetryBlockfetchConn(currentConnId)
	ls.blockfetchRequestRangeCleanup()
	ls.config.Logger.Warn(
		"blockfetch operation timed out, retrying queued range",
		"component", "ledger",
		"previous_connection_id", currentConnId.String(),
		"retry_connection_id", retryConnId.String(),
		"header_start_slot", headerStart.Slot,
		"header_end_slot", headerEnd.Slot,
		"header_count", headerCount,
	)
	if err := ls.startQueuedBlockfetchLocked(retryConnId); err != nil {
		ls.config.Logger.Error(
			"failed to retry blockfetch range after timeout",
			"component", "ledger",
			"connection_id", retryConnId.String(),
			"error", err,
		)
		if nextConnId, ok := ls.nextBlockfetchConnIdExcept(retryConnId); ok {
			ls.config.Logger.Warn(
				"retrying queued range on alternate blockfetch connection",
				"component", "ledger",
				"failed_connection_id", retryConnId.String(),
				"retry_connection_id", nextConnId.String(),
				"header_count", ls.chain.HeaderCount(),
			)
			if retryErr := ls.startQueuedBlockfetchLocked(nextConnId); retryErr != nil {
				ls.config.Logger.Error(
					"failed to restart queued blockfetch after timeout retry failure",
					"component", "ledger",
					"connection_id", nextConnId.String(),
					"error", retryErr,
				)
				if ls.chain.HeaderCount() > 0 && ls.config.EventBus != nil {
					ls.config.EventBus.Publish(
						event.ChainsyncResyncEventType,
						event.NewEvent(
							event.ChainsyncResyncEventType,
							event.ChainsyncResyncEvent{
								ConnectionId: retryConnId,
								Reason:       "blockfetch timeout retry failed on all available connections",
							},
						),
					)
				}
			}
		}
	}
}

func (ls *LedgerState) handleEventBlockfetchBatchDone(e BlockfetchEvent) error {
	// Drop batch-done from a stale connection (e.g., after connection switch)
	if ls.chainsyncBlockfetchReadyChan == nil ||
		!sameConnectionId(e.ConnectionId, ls.activeBlockfetchConnId) {
		return nil
	}
	// Stop the blockfetch timeout timer and invalidate any pending callbacks
	if ls.chainsyncBlockfetchTimeoutTimer != nil {
		ls.chainsyncBlockfetchTimeoutTimer.Stop()
		ls.chainsyncBlockfetchTimeoutTimer = nil
	}
	ls.chainsyncBlockfetchTimerGeneration++
	receivedBlockCount := ls.batchBlocksReceived
	if err := ls.flushPendingBlockfetchBlocks(); err != nil {
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return err
	}
	// Continue fetching as long as there are queued headers
	remainingHeaders := ls.chain.HeaderCount()
	if remainingHeaders > 0 {
		ls.config.Logger.Debug(
			"batch done, checking for more headers",
			"component", "ledger",
			"remaining_headers", remainingHeaders,
		)
	}
	upstreamTipSlot := ls.UpstreamTipSlot()
	if receivedBlockCount == 0 &&
		remainingHeaders > 0 &&
		upstreamTipSlot > ls.Tip().Point.Slot &&
		upstreamTipSlot-ls.Tip().Point.Slot >= blockfetchMinBatchGapSlots {
		retryConnId := ls.selectRetryBlockfetchConn(e.ConnectionId)
		ls.blockfetchRequestRangeCleanup()
		if connIdKey(retryConnId) != "" &&
			!sameConnectionId(retryConnId, e.ConnectionId) {
			ls.config.Logger.Warn(
				"blockfetch batch returned no blocks, retrying queued range on alternate connection",
				"component", "ledger",
				"previous_connection_id", e.ConnectionId.String(),
				"retry_connection_id", retryConnId.String(),
				"remaining_headers", remainingHeaders,
			)
			if err := ls.startQueuedBlockfetchLocked(retryConnId); err != nil {
				ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
				ls.clearQueuedHeaders()
				ls.requestChainsyncResync(
					e.ConnectionId,
					fmt.Sprintf(
						"empty blockfetch batch alternate retry failed: %v",
						err,
					),
				)
				return nil
			}
			return nil
		}
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.config.Logger.Warn(
			"blockfetch batch returned no blocks, requesting chainsync re-sync",
			"component", "ledger",
			"connection_id", e.ConnectionId.String(),
			"remaining_headers", remainingHeaders,
		)
		ls.requestChainsyncResync(
			e.ConnectionId,
			"empty blockfetch batch",
		)
		return nil
	}
	if remainingHeaders == 0 {
		// No more headers to fetch, allow chainsync to collect more
		ls.blockfetchRequestRangeCleanup()
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		if nextConnId, ok := ls.nextBufferedHeaderConnId(); ok {
			ls.replayBufferedHeadersAsync(nextConnId)
		}
		return nil
	}
	// Clean up from blockfetch batch
	ls.blockfetchRequestRangeCleanup()
	nextConnId, ok := ls.nextBlockfetchConnId()
	if !ok {
		ls.config.Logger.Debug(
			"headers pending but no next blockfetch connection is available",
			"component", "ledger",
			"remaining_headers", remainingHeaders,
			"active_blockfetch_connection_id",
			ls.activeBlockfetchConnId.String(),
		)
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		return nil
	}
	// Mark blockfetch as in progress for next batch
	err := ls.startQueuedBlockfetchLocked(nextConnId)
	if err != nil {
		// The connection's blockfetch protocol may have shut down
		// (e.g. peer disconnected mid-batch). Try an alternate
		// connection; if none available, clear all stale queued
		// headers and trigger a resync so the pipeline can restart.
		retryConnId := ls.selectRetryBlockfetchConn(nextConnId)
		if connIdKey(retryConnId) != "" &&
			!sameConnectionId(retryConnId, nextConnId) {
			ls.config.Logger.Warn(
				"blockfetch continuation failed, retrying on alternate connection",
				"component", "ledger",
				"failed_connection_id", nextConnId.String(),
				"retry_connection_id", retryConnId.String(),
				"error", err,
			)
			if retryErr := ls.startQueuedBlockfetchLocked(retryConnId); retryErr == nil {
				return nil
			}
		}
		ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
		ls.clearQueuedHeaders()
		ls.requestChainsyncResync(
			nextConnId,
			fmt.Sprintf("blockfetch continuation failed: %v", err),
		)
		return nil
	}
	return nil
}

// logSyncProgress logs periodic sync progress at INFO level.
// It reports the current slot, upstream tip slot, percentage complete,
// and sync rate in slots per second. syncUpstreamTipSlot is read
// atomically since it is written by the chainsync handler goroutine.
func (ls *LedgerState) logSyncProgress(currentSlot uint64) {
	now := time.Now()
	if now.Sub(ls.syncProgressLastLog) < syncProgressLogInterval {
		return
	}
	upstreamTip := ls.syncUpstreamTipSlot.Load()
	if upstreamTip == 0 {
		// No upstream tip known yet, skip
		return
	}
	elapsed := now.Sub(ls.syncProgressLastLog).Seconds()
	var slotsPerSec float64
	if elapsed > 0 && ls.syncProgressLastSlot > 0 &&
		currentSlot >= ls.syncProgressLastSlot {
		slotsDelta := currentSlot - ls.syncProgressLastSlot
		slotsPerSec = float64(slotsDelta) / elapsed
	}
	var pct float64
	if upstreamTip > 0 {
		pct = float64(currentSlot) / float64(upstreamTip) * 100
		if pct > 100 {
			pct = 100
		}
	}
	// Suppress progress logging when we're near the tip
	if pct >= 99.9 {
		ls.syncProgressLastLog = now
		ls.syncProgressLastSlot = currentSlot
		return
	}
	ls.config.Logger.Info(
		fmt.Sprintf(
			"sync progress: slot %d/%d (%.1f%%), %.0f slots/sec",
			currentSlot,
			upstreamTip,
			pct,
			slotsPerSec,
		),
		"component", "ledger",
	)
	ls.syncProgressLastLog = now
	ls.syncProgressLastSlot = currentSlot
}

// SyncProgress returns the current sync progress as a value between
// 0.0 (unknown/just started) and 1.0 (fully synced). This implements
// the peergov.SyncProgressProvider interface, allowing the peer
// governor to exit bootstrap mode once sync reaches its threshold.
func (ls *LedgerState) SyncProgress() float64 {
	upstreamTip := ls.syncUpstreamTipSlot.Load()
	if upstreamTip == 0 {
		return 0
	}
	ls.RLock()
	currentSlot := ls.currentTip.Point.Slot
	ls.RUnlock()
	progress := float64(currentSlot) / float64(upstreamTip)
	if progress > 1.0 {
		progress = 1.0
	}
	return progress
}
