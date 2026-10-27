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

	"github.com/blinklabs-io/dingo/event"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// processChainsyncRollback applies a peer rollback and handles its failure.
// Callers must hold chainsyncMutex.
func (ls *LedgerState) processChainsyncRollback(
	e ChainsyncEvent,
	pending *pendingPublishes,
) {
	err := ls.handleEventChainsyncRollback(e, pending)
	if err == nil {
		return
	}
	if errors.Is(err, ErrRollbackLoopDetected) {
		// The rollback was skipped to break a pathological loop. Trigger a
		// chainsync re-sync so the peer can negotiate a fresh intersection
		// rather than continuing to send the same rollback point.
		ls.resetChainsyncResyncState(pending)
		ls.setChainsyncState(SyncingChainsyncState)
		// Queued rather than published here: this runs with
		// ls.chainsyncMutex held, and this event's subscriber takes that
		// same mutex via RecoverAfterLocalRollback.
		pending.add(
			ls.config.EventBus,
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					ConnectionId: e.ConnectionId,
					Reason:       event.ChainsyncResyncReasonRollbackLoop,
				},
			),
		)
		return
	}
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
}

// deferOpeningChainsyncRollback holds back the rollback that opens a chainsync
// session when it names a point our chain holds behind its header tip, and
// reports whether it did.
//
// A session opens with MsgRollBackward to its intersection. That point is only
// where the peer's chain and ours were last known to agree; it says nothing
// about whether the peer's chain leaves ours there. A reconnecting peer
// intersects at our tip as of its FindIntersect, and our tip routinely moves on
// before the reply is handled, so rolling back to it undoes blocks the peer may
// well hold. A point that was still a queued header when the peer intersected
// can be committed by the time the rollback runs, so it is deferred too. The
// peer's next header settles the question: see resolveOpeningChainsyncRollback.
//
// Only a session that has not delivered a header qualifies. Once a peer is
// streaming, a rollback is the peer moving off a chain it already showed us and
// is applied as before. Callers must hold chainsyncMutex.
func (ls *LedgerState) deferOpeningChainsyncRollback(e ChainsyncEvent) bool {
	key := connIdKey(e.ConnectionId)
	if key == "" || ls.chain == nil {
		return false
	}
	if _, streaming := ls.chainsyncHeaderSessions[key]; streaming {
		return false
	}
	// A later rollback before any header replaces the earlier one: the peer's
	// cursor is wherever its last rollback put it.
	delete(ls.chainsyncOpeningRollbacks, key)
	if len(e.Point.Hash) == 0 ||
		pointMatches(e.Point, ls.chain.HeaderTip().Point) {
		return false
	}
	if !ls.pointOnLocalChain(e, e.Point, "opening rollback point") {
		return false
	}
	if ls.chainsyncOpeningRollbacks == nil {
		ls.chainsyncOpeningRollbacks = make(map[string]ChainsyncEvent)
	}
	ls.chainsyncOpeningRollbacks[key] = e
	ls.config.Logger.Debug(
		"chainsync session opened behind our tip on our chain, deferring its rollback until the peer's next header",
		"component", "ledger",
		"rollback_slot", e.Point.Slot,
		"rollback_hash", hex.EncodeToString(e.Point.Hash),
		"local_tip_slot", ls.chain.Tip().Point.Slot,
		"peer_tip_slot", e.Tip.Point.Slot,
		"connection_id", e.ConnectionId.String(),
	)
	return true
}

// resolveOpeningChainsyncRollback settles a deferred opening rollback when the
// session delivers its first header, before that header is handled.
//
// The rollback is applied, exactly as it would have been on arrival, only when
// the header builds on the rollback point and is not a block our chain already
// holds: the peer's chain leaves ours at the intersection, and the header can
// only be queued once our chain is back there. A header our chain holds is the
// peer replaying our chain. Any other header does not build on the rollback
// point at all — the ingress apply gate can withhold a session's headers until
// the peer is selected — so the rollback says nothing about it; the ordinary
// header path decides from the header itself, including fork resolution
// against our tip. In both of those cases the rollback only moved the peer's
// cursor and is dropped. Callers must hold chainsyncMutex.
func (ls *LedgerState) resolveOpeningChainsyncRollback(
	e ChainsyncEvent,
	pending *pendingPublishes,
) {
	key := connIdKey(e.ConnectionId)
	if key == "" {
		return
	}
	if ls.chainsyncHeaderSessions == nil {
		ls.chainsyncHeaderSessions = make(map[string]struct{})
	}
	ls.chainsyncHeaderSessions[key] = struct{}{}
	deferred, ok := ls.chainsyncOpeningRollbacks[key]
	if !ok {
		return
	}
	delete(ls.chainsyncOpeningRollbacks, key)
	if e.BlockHeader == nil || ls.chain == nil ||
		!bytes.Equal(e.BlockHeader.PrevHash().Bytes(), deferred.Point.Hash) ||
		ls.pointOnLocalChain(e, e.Point, "opening rollback continuation") {
		ls.config.Logger.Debug(
			"chainsync peer's first header does not leave our chain at its intersection, dropping its opening rollback",
			"component", "ledger",
			"rollback_slot", deferred.Point.Slot,
			"header_slot", e.Point.Slot,
			"connection_id", e.ConnectionId.String(),
		)
		return
	}
	ls.config.Logger.Debug(
		"chainsync peer leaves our chain at its intersection, applying its opening rollback",
		"component", "ledger",
		"rollback_slot", deferred.Point.Slot,
		"header_slot", e.Point.Slot,
		"connection_id", e.ConnectionId.String(),
	)
	ls.processChainsyncRollback(deferred, pending)
}

// pointOnLocalChain reports whether point is a queued header or a block on our
// primary chain at or behind its tip. A failed lookup reports false.
func (ls *LedgerState) pointOnLocalChain(
	e ChainsyncEvent,
	point ocommon.Point,
	subject string,
) bool {
	if ls.chain.HoldsQueuedHeader(point) {
		return true
	}
	onChain, err := ls.historicalPointOnPrimaryChain(point, ls.chain.Tip())
	if err != nil {
		ls.logChainsyncBehindLookupError(e, subject, err)
		return false
	}
	return onChain
}

// forgetChainsyncSession drops the opening-rollback state of a connection.
// Callers must hold chainsyncMutex.
func (ls *LedgerState) forgetChainsyncSession(key string) {
	delete(ls.chainsyncOpeningRollbacks, key)
	delete(ls.chainsyncHeaderSessions, key)
}
