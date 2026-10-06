// Copyright 2025 Blink Labs Software
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

package ouroboros

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// localstatequeryServerConnOpts returns this listener's LocalStateQuery
// server options. trusted gates the relaxed timeout/buffer options below it
// to a listener ConfigureListeners has actually verified is local-only (a
// Unix socket, or TCP bound to loopback) -- see isTrustedNtCListener. A
// listener an operator has bound to a non-loopback address gets gouroboros'
// own defaults (120s mux segment-read timeout, 180s query timeout, 16MB
// reassembly cap) instead: those exist specifically as anti-DoS guards
// against an untrusted remote peer, and relaxing them for every NtC
// connection regardless of reachability would let any client that can reach
// that address hold a connection open indefinitely and grow its reassembly
// buffer to MaxReadBufferSize.
func (o *Ouroboros) localstatequeryServerConnOpts(
	trusted bool,
) []olocalstatequery.LocalStateQueryOptionFunc {
	opts := make([]olocalstatequery.LocalStateQueryOptionFunc, 3, 5)
	opts[0] = olocalstatequery.WithAcquireFunc(
		o.instrumentLocalstatequeryAcquire(o.localstatequeryServerAcquire),
	)
	opts[1] = olocalstatequery.WithQueryFunc(
		o.instrumentLocalstatequeryQuery(o.localstatequeryServerQuery),
	)
	opts[2] = olocalstatequery.WithReleaseFunc(
		o.instrumentLocalstatequeryRelease(o.localstatequeryServerRelease),
	)
	if !trusted {
		return opts
	}
	return append(opts,
		// WithMuxerSegmentReadTimeout(0) (ConfigureListeners) only removes
		// the transport-level cap; LocalStateQuery's client-side protocol
		// also carries its own, separate 180s per-query state-transition
		// timer (QueryTimeout) that fires the same way once a query
		// outlives it -- confirmed live against a real Preview node's
		// whole-UTxO query, which the mux fix alone still let get torn
		// down (ErrProtocolShuttingDown) at almost exactly 180s. Disabled
		// for the same reason as the mux timeout: LocalStateQuery has no
		// protocol-level timeout at all (Ouroboros Network Specification
		// section 3.13.4), and a verified-local-only NtC channel is one
		// where a slow-but-legitimate reply must not be killed either.
		//
		// MaxReadBufferSize likewise overrides gouroboros' default 16MB
		// cap on a reassembled multi-segment reply: confirmed live that a
		// real Preview-scale whole-UTxO-set reply exceeds 512MiB. 2GiB
		// gives headroom for further chain growth without removing the
		// cap outright -- unlike the two timeouts above, an unbounded
		// buffer here is a real unbounded memory-growth risk, not just an
		// unnecessary wait.
		olocalstatequery.WithQueryTimeout(0),
		olocalstatequery.WithMaxReadBufferSize(2<<30),
	)
}

func (o *Ouroboros) instrumentLocalstatequeryAcquire(
	fn func(olocalstatequery.CallbackContext, olocalstatequery.AcquireTarget, bool) error,
) func(olocalstatequery.CallbackContext, olocalstatequery.AcquireTarget, bool) error {
	return func(
		ctx olocalstatequery.CallbackContext,
		acquireTarget olocalstatequery.AcquireTarget,
		reAcquire bool,
	) error {
		start := time.Now()
		err := fn(ctx, acquireTarget, reAcquire)
		o.recordProtocolMessage("localstatequery", err, time.Since(start))
		return err
	}
}

func (o *Ouroboros) instrumentLocalstatequeryQuery(
	fn func(olocalstatequery.CallbackContext, olocalstatequery.QueryWrapper) (any, error),
) func(olocalstatequery.CallbackContext, olocalstatequery.QueryWrapper) (any, error) {
	return func(
		ctx olocalstatequery.CallbackContext,
		query olocalstatequery.QueryWrapper,
	) (any, error) {
		start := time.Now()
		result, err := fn(ctx, query)
		o.recordProtocolMessage("localstatequery", err, time.Since(start))
		return result, err
	}
}

func (o *Ouroboros) instrumentLocalstatequeryRelease(
	fn func(olocalstatequery.CallbackContext) error,
) func(olocalstatequery.CallbackContext) error {
	return func(ctx olocalstatequery.CallbackContext) error {
		start := time.Now()
		err := fn(ctx)
		o.recordProtocolMessage("localstatequery", err, time.Since(start))
		return err
	}
}

// errLocalStateQueryLedgerUnavailable is returned by the LocalStateQuery
// callbacks when this Ouroboros was built without a ledger state.
var errLocalStateQueryLedgerUnavailable = errors.New(
	"local-state-query: ledger state unavailable",
)

// localStateQueryAcquireWait bounds how long an Acquire waits for a ledger
// snapshot when every snapshot the database admits is already held. It
// matches the Acquire timeout gouroboros clients use by default, past which
// the client has given up on the reply anyway.
const localStateQueryAcquireWait = 5 * time.Second

// defaultLocalStateQueryViewMaxLifetime bounds how long a connection may hold
// one acquired ledger snapshot when OuroborosConfig does not say otherwise.
const defaultLocalStateQueryViewMaxLifetime = 5 * time.Minute

// localstatequerySession is the ledger snapshot one connection holds between
// Acquire and Release. All fields are guarded by localstatequeryAcquireMutex.
type localstatequerySession struct {
	view       *ledger.QueryView
	acquiredAt time.Time
	lastQuery  time.Time
	expiry     *time.Timer
}

func (o *Ouroboros) localstatequeryViewMaxLifetime() time.Duration {
	if o.config.LocalStateQueryViewMaxLifetime > 0 {
		return o.config.LocalStateQueryViewMaxLifetime
	}
	return defaultLocalStateQueryViewMaxLifetime
}

// localstatequeryServerAcquire opens a ledger snapshot for this connection's
// LocalStateQuery session and records the point it was acquired at. Every
// Query on the connection is answered from that snapshot until Release,
// re-Acquire, disconnect or expiry, so the session sees one consistent state
// however many blocks are applied meanwhile.
//
// AcquireSpecificPoint's slot AND hash are both recorded -- hash matters
// because identifying a point by slot alone is ambiguous across a rollback (a
// fork switch can leave a different block at the same slot than the one the
// caller acquired). AcquireVolatileTip and AcquireImmutableTip both snapshot
// the live state with no pinned point: only a specific point makes sense to
// hold stable across a slow query, and both tip kinds are, by construction,
// "whatever is live/immutable right now".
//
// A specific point is rejected here, before anything is recorded, unless
// LedgerState.AcquireQueryView confirms every point-aware query type can
// answer for it. The wire protocol has no way to fail a query after a
// successful Acquire, so this is the only protocol-legal place to refuse a
// point this node cannot honor. The two ways that can fail map to the two
// AcquireFailure reasons the protocol defines: a point that has left this
// node's chain (ErrPointNotOnChain) fails the same way an unknown point
// always has; a point still on-chain but older than some query type's own
// retention floor (ErrHistoricalStateUnavailable) fails as "too old".
//
// A re-Acquire closes the connection's previous snapshot before opening the
// replacement, so a connection never needs a second admission slot while its
// own is held; with the cap full it could otherwise never re-Acquire. A failed
// Acquire leaves the previous session registered but with its snapshot
// closed: the protocol returns the connection to Idle on failure, and a client
// that queries anyway gets ErrQueryViewClosed rather than answers from a
// snapshot it no longer holds a slot for.
//
// Not every query type honors a pinned point yet -- see
// ledger.LedgerState.Query's doc comment for which ones do.
func (o *Ouroboros) localstatequeryServerAcquire(
	ctx olocalstatequery.CallbackContext,
	acquireTarget olocalstatequery.AcquireTarget,
	reAcquire bool,
) error {
	if o.ledgerState == nil {
		return errLocalStateQueryLedgerUnavailable
	}
	specific, isSpecific := acquireTarget.(olocalstatequery.AcquireSpecificPoint)
	var point ledger.QueryPoint
	if isSpecific {
		point = ledger.QueryPoint{
			Slot: specific.Point.Slot,
			Hash: specific.Point.Hash,
		}
	}
	// Validate synchronously, at Acquire time, rather than deferring to the
	// first Query: a rejection here has a graceful wire-level AcquireFailure
	// reply (gouroboros' handleAcquire/handleReAcquire both translate
	// ErrAcquireFailurePointNotOnChain/PointTooOld into one), but a rejection
	// surfacing later, from the Query callback, has no such path and tears
	// down the whole connection instead.
	o.localstatequeryAcquireMutex.Lock()
	held := o.localstatequerySessions[ctx.ConnectionId]
	o.localstatequeryAcquireMutex.Unlock()
	if held != nil {
		held.view.Close()
	}
	requestCtx, cancelRequest := o.localstatequeryRequestContext(ctx)
	defer cancelRequest()
	acquireCtx, cancel := context.WithTimeout(
		requestCtx,
		localStateQueryAcquireWait,
	)
	view, err := o.ledgerState.AcquireQueryView(acquireCtx, point)
	cancel()
	if err != nil {
		return o.mapLocalStateQueryAcquireError(ctx, isSpecific, err)
	}
	now := time.Now()
	session := &localstatequerySession{
		view:       view,
		acquiredAt: now,
		lastQuery:  now,
	}
	o.localstatequeryAcquireMutex.Lock()
	previous := o.removeLocalStateQuerySessionLocked(ctx.ConnectionId)
	if isSpecific {
		o.localstatequeryAcquiredPoints[ctx.ConnectionId] = point
	}
	if o.localstatequeryOwners == nil {
		o.localstatequeryOwners = make(
			map[ouroboros.ConnectionId]*olocalstatequery.Server,
		)
	}
	o.localstatequeryOwners[ctx.ConnectionId] = ctx.Server
	if o.localstatequerySessions == nil {
		o.localstatequerySessions = make(
			map[ouroboros.ConnectionId]*localstatequerySession,
		)
	}
	o.localstatequerySessions[ctx.ConnectionId] = session
	session.expiry = time.AfterFunc(
		o.localstatequeryViewMaxLifetime(),
		func() { o.expireLocalStateQuerySession(ctx.ConnectionId, session) },
	)
	o.localstatequeryAcquireMutex.Unlock()
	previous.close()
	return nil
}

// mapLocalStateQueryAcquireError turns an AcquireQueryView failure into the
// error gouroboros expects from an Acquire callback.
func (o *Ouroboros) mapLocalStateQueryAcquireError(
	ctx olocalstatequery.CallbackContext,
	isSpecific bool,
	err error,
) error {
	if isSpecific {
		if errors.Is(err, ledger.ErrPointNotOnChain) {
			return fmt.Errorf(
				"%w: %w",
				olocalstatequery.ErrAcquireFailurePointNotOnChain,
				err,
			)
		}
		if errors.Is(err, ledger.ErrHistoricalStateUnavailable) {
			return fmt.Errorf(
				"%w: %w",
				olocalstatequery.ErrAcquireFailurePointTooOld,
				err,
			)
		}
	}
	// An error matching neither sentinel means something unexpected (a real
	// database error, say, or no snapshot admitted within
	// localStateQueryAcquireWait) happened while opening the snapshot rather
	// than the point genuinely being unqueryable. gouroboros treats any other
	// error from Acquire as a fatal protocol error and tears the connection
	// down, so for a specific point it is mapped to the same
	// AcquireFailurePointTooOld a well-behaved client already handles (retry
	// against a different point); the real cause is logged here since the
	// client only ever sees the generic wire-level rejection. A tip Acquire
	// has no failure reply to map to, so its error is returned as is.
	o.config.Logger.Error(
		"local-state-query Acquire failed unexpectedly",
		"component", "network",
		"connection_id", ctx.ConnectionId.String(),
		"error", err,
	)
	if !isSpecific {
		return fmt.Errorf("open ledger snapshot: %w", err)
	}
	return fmt.Errorf(
		"%w: %w",
		olocalstatequery.ErrAcquireFailurePointTooOld,
		err,
	)
}

func (o *Ouroboros) localstatequeryServerQuery(
	ctx olocalstatequery.CallbackContext,
	query olocalstatequery.QueryWrapper,
) (any, error) {
	if o.ledgerState == nil {
		return nil, errLocalStateQueryLedgerUnavailable
	}
	requestCtx, cancelRequest := o.localstatequeryRequestContext(ctx)
	defer cancelRequest()
	o.localstatequeryAcquireMutex.Lock()
	at := o.localstatequeryAcquiredPoints[ctx.ConnectionId]
	session := o.localstatequerySessions[ctx.ConnectionId]
	if session != nil {
		session.lastQuery = time.Now()
	}
	o.localstatequeryAcquireMutex.Unlock()
	protocolVersion := uint16(0)
	if o.connManager != nil {
		if conn := o.connManager.GetConnectionById(ctx.ConnectionId); conn != nil {
			protocolVersion, _ = conn.ProtocolVersion()
		}
	}
	if session != nil {
		return session.view.Query(requestCtx, query.Query, protocolVersion)
	}
	return o.ledgerState.QueryWithProtocolVersion(
		requestCtx,
		query.Query,
		at,
		protocolVersion,
	)
}

func (o *Ouroboros) localstatequeryServerRelease(
	ctx olocalstatequery.CallbackContext,
) error {
	o.releaseLocalStateQueryAcquiredPointOwner(ctx.ConnectionId, ctx.Server)
	return nil
}

// removeLocalStateQuerySessionLocked forgets everything recorded for connId
// and returns its session, if any, for the caller to close once it has
// released localstatequeryAcquireMutex: closing a view can touch the database
// and must not run under the mutex every Acquire, Query and Release takes.
func (o *Ouroboros) removeLocalStateQuerySessionLocked(
	connId ouroboros.ConnectionId,
) *localstatequerySession {
	session := o.localstatequerySessions[connId]
	delete(o.localstatequerySessions, connId)
	delete(o.localstatequeryAcquiredPoints, connId)
	delete(o.localstatequeryOwners, connId)
	return session
}

// close closes the session's view and cancels its expiry. It is safe on a nil
// session.
func (s *localstatequerySession) close() {
	if s == nil {
		return
	}
	if s.expiry != nil {
		s.expiry.Stop()
	}
	s.view.Close()
}

// expireLocalStateQuerySession closes a session that outlived the maximum
// view lifetime. The map entry stays until the connection releases or
// disconnects, so a client that keeps querying gets a closed-view error
// instead of silently reading live state it never acquired.
func (o *Ouroboros) expireLocalStateQuerySession(
	connId ouroboros.ConnectionId,
	session *localstatequerySession,
) {
	o.localstatequeryAcquireMutex.Lock()
	current := o.localstatequerySessions[connId] == session
	acquiredAt, lastQuery := session.acquiredAt, session.lastQuery
	o.localstatequeryAcquireMutex.Unlock()
	if !current {
		return
	}
	session.view.Close()
	now := time.Now()
	o.config.Logger.Warn(
		"local-state-query ledger snapshot expired",
		"component", "network",
		"connection_id", connId.String(),
		"age", now.Sub(acquiredAt),
		"idle", now.Sub(lastQuery),
		"max_lifetime", o.localstatequeryViewMaxLifetime(),
	)
}

func (o *Ouroboros) releaseLocalStateQueryAcquiredPointOwner(
	connId ouroboros.ConnectionId,
	owner *olocalstatequery.Server,
) {
	o.localstatequeryAcquireMutex.Lock()
	o.cancelLocalStateQueryRequestsLocked(connId, owner)
	_, hasPoint := o.localstatequeryAcquiredPoints[connId]
	_, hasSession := o.localstatequerySessions[connId]
	currentOwner := o.localstatequeryOwners[connId]
	if (!hasPoint && !hasSession) ||
		(currentOwner != nil && currentOwner != owner) {
		o.localstatequeryAcquireMutex.Unlock()
		return
	}
	session := o.removeLocalStateQuerySessionLocked(connId)
	o.localstatequeryAcquireMutex.Unlock()
	session.close()
}

// ReleaseLocalStateQueryAcquiredPointOwner clears connId's session only when
// owner still owns it, closing its ledger snapshot. An ownerless entry is
// cleared by any close, which supports state created before owner tracking or
// by test helpers.
func (o *Ouroboros) ReleaseLocalStateQueryAcquiredPointOwner(
	connId ouroboros.ConnectionId,
	owner *olocalstatequery.Server,
) {
	o.releaseLocalStateQueryAcquiredPointOwner(connId, owner)
}

// ReleaseLocalStateQueryAcquiredPoint unconditionally clears connId's
// session. It is retained for tests and whole-instance cleanup; live
// connection close handling uses ReleaseLocalStateQueryAcquiredPointOwner.
func (o *Ouroboros) ReleaseLocalStateQueryAcquiredPoint(
	connId ouroboros.ConnectionId,
) {
	o.localstatequeryAcquireMutex.Lock()
	session := o.removeLocalStateQuerySessionLocked(connId)
	o.localstatequeryAcquireMutex.Unlock()
	session.close()
}

// closeLocalStateQuerySessions closes every connection's ledger snapshot. It
// runs when this Ouroboros is closed, before the database the snapshots read
// from is torn down.
func (o *Ouroboros) closeLocalStateQuerySessions() {
	o.localstatequeryAcquireMutex.Lock()
	sessions := make([]*localstatequerySession, 0, len(o.localstatequerySessions))
	for connId := range o.localstatequerySessions {
		sessions = append(sessions, o.removeLocalStateQuerySessionLocked(connId))
	}
	o.localstatequeryAcquireMutex.Unlock()
	for _, session := range sessions {
		session.close()
	}
}

// SetLocalStateQueryAcquiredPointForTesting seeds connId's pinned point
// directly, bypassing a real Acquire callback, so the root package can prove
// its NtC connection-closed callback actually clears this map -- the same
// two-package split RegisterLeiosServeWaiterForTesting exists for.
func (o *Ouroboros) SetLocalStateQueryAcquiredPointForTesting(
	connId ouroboros.ConnectionId,
	point ledger.QueryPoint,
) {
	o.localstatequeryAcquireMutex.Lock()
	o.localstatequeryAcquiredPoints[connId] = point
	delete(o.localstatequeryOwners, connId)
	o.localstatequeryAcquireMutex.Unlock()
}

// HasLocalStateQueryAcquiredPointForTesting reports whether connId currently
// has an acquired point or session entry, regardless of whether the recorded
// point is the pinned or the live/cleared zero value -- the presence of the
// entry itself is what a leak looks like, so this checks membership, not
// QueryPoint.pinned().
func (o *Ouroboros) HasLocalStateQueryAcquiredPointForTesting(
	connId ouroboros.ConnectionId,
) bool {
	o.localstatequeryAcquireMutex.Lock()
	defer o.localstatequeryAcquireMutex.Unlock()
	_, hasPoint := o.localstatequeryAcquiredPoints[connId]
	_, hasSession := o.localstatequerySessions[connId]
	return hasPoint || hasSession
}

// localstatequeryRequest tracks reads that must stop when their serving
// connection closes, even while its callback prevents the protocol loop exiting.
type localstatequeryRequest struct {
	owner  *olocalstatequery.Server
	cancel context.CancelFunc
}

func (o *Ouroboros) localstatequeryRequestContext(callback olocalstatequery.CallbackContext) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	request := &localstatequeryRequest{owner: callback.Server, cancel: cancel}
	o.localstatequeryAcquireMutex.Lock()
	if o.localstatequeryRequests == nil {
		o.localstatequeryRequests = make(map[ouroboros.ConnectionId][]*localstatequeryRequest)
	}
	o.localstatequeryRequests[callback.ConnectionId] = append(o.localstatequeryRequests[callback.ConnectionId], request)
	o.localstatequeryAcquireMutex.Unlock()
	cleanup := func() {
		cancel()
		o.localstatequeryAcquireMutex.Lock()
		defer o.localstatequeryAcquireMutex.Unlock()
		requests := o.localstatequeryRequests[callback.ConnectionId]
		for i, current := range requests {
			if current == request {
				requests = append(requests[:i], requests[i+1:]...)
				break
			}
		}
		if len(requests) == 0 {
			delete(o.localstatequeryRequests, callback.ConnectionId)
		} else {
			o.localstatequeryRequests[callback.ConnectionId] = requests
		}
	}
	// Register before checking liveness so a simultaneous connection close
	// cannot fall between the check and registration and strand the read.
	if o.connManager != nil {
		conn := o.connManager.GetConnectionById(callback.ConnectionId)
		if conn == nil || conn.LocalStateQuery() == nil || conn.LocalStateQuery().Server != callback.Server {
			cleanup()
		}
	}
	return ctx, cleanup
}

// cancelLocalStateQueryRequestsLocked cancels and forgets the reads in flight
// on connId that owner is serving. The caller holds
// localstatequeryAcquireMutex.
func (o *Ouroboros) cancelLocalStateQueryRequestsLocked(
	connId ouroboros.ConnectionId,
	owner *olocalstatequery.Server,
) {
	requests := o.localstatequeryRequests[connId]
	remaining := make([]*localstatequeryRequest, 0, len(requests))
	for _, request := range requests {
		if request == nil {
			continue
		}
		if request.owner == owner {
			if request.cancel != nil {
				request.cancel()
			}
			continue
		}
		remaining = append(remaining, request)
	}
	if len(remaining) == 0 {
		delete(o.localstatequeryRequests, connId)
	} else {
		o.localstatequeryRequests[connId] = remaining
	}
}
