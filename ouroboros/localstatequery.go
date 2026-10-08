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
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

var errLocalStateQueryConnectionClosed = errors.New(
	"local-state-query: connection closed during acquire",
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
	// releasePin drops the ledger pin that keeps pruning off the view's
	// point (ledger.LedgerState.PinAcquiredPoint); nil for a tip acquire
	// until its view is reopened. It is idempotent, and runs wherever the
	// view is closed.
	releasePin func()
	// point is the block the view answers for: the acquired point, or the
	// tip a tip acquire snapshotted. An expired view is reopened here. It is
	// unpinned only for a tip acquire on a chain with no block yet.
	point ledger.QueryPoint
	// expired is set when the lifetime timer, not the client, closed view.
	expired bool
}

type localstatequeryAcquisition struct {
	owner  *olocalstatequery.Server
	cancel context.CancelFunc
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
// caller acquired). AcquireVolatileTip snapshots the transaction's live tip;
// AcquireImmutableTip resolves the primary-chain point k blocks behind its tip
// and pins that concrete point.
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
// Acquire forgets the previous session after closing its snapshot: the
// protocol returns the connection to Idle on failure, with no acquired state.
//
// Every query type that reads ledger or consensus state answers at the
// acquired point -- see queryShelleyLeaf's doc comment in ledger/queries.go.
func (o *Ouroboros) localstatequeryServerAcquire(
	ctx olocalstatequery.CallbackContext,
	acquireTarget olocalstatequery.AcquireTarget,
	reAcquire bool,
) error {
	if o.ledgerState == nil {
		return errLocalStateQueryLedgerUnavailable
	}
	// Validate synchronously, at Acquire time, rather than deferring to the
	// first Query: a rejection here has a graceful wire-level AcquireFailure
	// reply (gouroboros' handleAcquire/handleReAcquire both translate
	// ErrAcquireFailurePointNotOnChain/PointTooOld into one), but a rejection
	// surfacing later, from the Query callback, has no such path and tears
	// down the whole connection instead.
	requestCtx, cancelRequest := o.localstatequeryRequestContext(ctx)
	defer cancelRequest()
	acquireCtx, cancel := context.WithTimeout(
		requestCtx,
		localStateQueryAcquireWait,
	)
	acquisition := &localstatequeryAcquisition{
		owner:  ctx.Server,
		cancel: cancel,
	}
	o.localstatequeryAcquireMutex.Lock()
	if ctx.Server != nil && ctx.Server.IsDone() {
		o.localstatequeryAcquireMutex.Unlock()
		cancel()
		return errLocalStateQueryConnectionClosed
	}
	held := o.removeLocalStateQuerySessionLocked(ctx.ConnectionId)
	previousAcquisition := o.localstatequeryAcquisitions[ctx.ConnectionId]
	if o.localstatequeryAcquisitions == nil {
		o.localstatequeryAcquisitions = make(
			map[ouroboros.ConnectionId]*localstatequeryAcquisition,
		)
	}
	o.localstatequeryAcquisitions[ctx.ConnectionId] = acquisition
	o.localstatequeryAcquireMutex.Unlock()
	if previousAcquisition != nil {
		previousAcquisition.cancel()
	}
	held.close()
	point, isSpecific, err := o.resolveLocalStateQueryAcquirePoint(requestCtx, acquireTarget)
	if err != nil {
		o.localstatequeryAcquireMutex.Lock()
		if o.localstatequeryAcquisitions[ctx.ConnectionId] == acquisition {
			delete(o.localstatequeryAcquisitions, ctx.ConnectionId)
		}
		o.localstatequeryAcquireMutex.Unlock()
		cancel()
		return o.mapLocalStateQueryAcquireError(ctx, isSpecific, err)
	}
	// Pin before AcquireQueryView verifies the point: a pruning path that
	// computes its floor after this sees the pin and keeps the point's
	// state, and one that announced its floor first makes the verify refuse
	// the point.
	var releasePin func()
	if isSpecific {
		releasePin = o.ledgerState.PinAcquiredPoint(point.Slot)
	}
	if o.localstatequeryVerifyHook != nil {
		o.localstatequeryVerifyHook()
	}
	view, err := o.ledgerState.AcquireQueryView(acquireCtx, point)
	if err != nil {
		if releasePin != nil {
			releasePin()
		}
		o.localstatequeryAcquireMutex.Lock()
		if o.localstatequeryAcquisitions[ctx.ConnectionId] == acquisition {
			delete(o.localstatequeryAcquisitions, ctx.ConnectionId)
		}
		o.localstatequeryAcquireMutex.Unlock()
		cancel()
		return o.mapLocalStateQueryAcquireError(ctx, isSpecific, err)
	}
	if o.localstatequeryVerifiedHook != nil {
		o.localstatequeryVerifiedHook()
	}
	sessionPoint := point
	if !isSpecific {
		sessionPoint = localStateQueryViewTip(requestCtx, view)
	}
	now := time.Now()
	session := &localstatequerySession{
		view:       view,
		acquiredAt: now,
		lastQuery:  now,
		releasePin: releasePin,
		point:      sessionPoint,
	}
	o.localstatequeryAcquireMutex.Lock()
	if o.localstatequeryAcquisitions[ctx.ConnectionId] != acquisition {
		o.localstatequeryAcquireMutex.Unlock()
		session.close()
		cancel()
		return errLocalStateQueryConnectionClosed
	}
	delete(o.localstatequeryAcquisitions, ctx.ConnectionId)
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
	cancel()
	return nil
}

func (o *Ouroboros) resolveLocalStateQueryAcquirePoint(
	ctx context.Context,
	target olocalstatequery.AcquireTarget,
) (ledger.QueryPoint, bool, error) {
	var point ledger.QueryPoint
	switch target := target.(type) {
	case olocalstatequery.AcquireSpecificPoint:
		point = ledger.QueryPoint{
			Slot: target.Point.Slot,
			Hash: target.Point.Hash,
		}
		if target.Point.Slot == 0 && len(target.Point.Hash) == 0 {
			return ledger.QueryPoint{}, true, fmt.Errorf(
				"%w: chain origin cannot be queried",
				ledger.ErrHistoricalStateUnavailable,
			)
		}
		return point, true, nil
	case olocalstatequery.AcquireVolatileTip:
		return ledger.QueryPoint{}, false, nil
	case olocalstatequery.AcquireImmutableTip:
		immutable, found, err := o.ledgerState.ImmutablePoint(ctx)
		if err != nil {
			return ledger.QueryPoint{}, true, err
		}
		if !found {
			return ledger.QueryPoint{}, true, fmt.Errorf(
				"%w: immutable tip is origin",
				ledger.ErrHistoricalStateUnavailable,
			)
		}
		return ledger.QueryPoint{
			Slot: immutable.Slot,
			Hash: immutable.Hash,
		}, true, nil
	default:
		return ledger.QueryPoint{}, false, fmt.Errorf(
			"unsupported LocalStateQuery acquire target %T",
			target,
		)
	}
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
	var view *ledger.QueryView
	if session != nil {
		session.lastQuery = time.Now()
		view = session.view
	}
	o.localstatequeryAcquireMutex.Unlock()
	protocolVersion := uint16(0)
	if o.connManager != nil {
		if conn := o.connManager.GetConnectionById(ctx.ConnectionId); conn != nil {
			protocolVersion, _ = conn.ProtocolVersion()
		}
	}
	if session != nil {
		result, err := view.Query(requestCtx, query.Query, protocolVersion)
		if !errors.Is(err, ledger.ErrQueryViewClosed) {
			return result, err
		}
		// The protocol has no reply for a failed query, so an error here
		// ends the connection. A view the lifetime timer closed is reopened
		// at the same block instead, which answers exactly as the closed one
		// would have.
		view, err = o.reopenExpiredLocalStateQuerySession(
			requestCtx,
			ctx.ConnectionId,
			session,
		)
		if err != nil {
			return nil, err
		}
		return view.Query(requestCtx, query.Query, protocolVersion)
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
	if s.releasePin != nil {
		s.releasePin()
	}
}

// expireLocalStateQuerySession closes a session's view once it outlives the
// maximum view lifetime, freeing the read transaction it held. The session
// stays recorded until the connection releases or disconnects, and its next
// query reopens a view at the same block (reopenExpiredLocalStateQuerySession)
// rather than reading live state it never acquired.
func (o *Ouroboros) expireLocalStateQuerySession(
	connId ouroboros.ConnectionId,
	session *localstatequerySession,
) {
	o.localstatequeryAcquireMutex.Lock()
	current := o.localstatequerySessions[connId] == session
	acquiredAt, lastQuery := session.acquiredAt, session.lastQuery
	view, releasePin := session.view, session.releasePin
	if current {
		session.expired = true
		session.releasePin = nil
	}
	o.localstatequeryAcquireMutex.Unlock()
	if !current {
		return
	}
	view.Close()
	if releasePin != nil {
		releasePin()
	}
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

// reopenExpiredLocalStateQuerySession gives an expired session a new view at
// the block its old one answered for, so a client that queries past the view
// lifetime keeps its connection and the same answers. The lifetime bounds how
// long one database read transaction is held, not how long a client may
// stay acquired. A session the client released, or one with no block to
// pin, is not reopened; nor is a point this node can no longer answer for
// (rolled back, or past a retention floor), whose error ends the connection.
//
// requestCtx is the query's request context, which the connection's close
// cancels, so a disconnect ends a wait for snapshot admission rather than
// leaving it to open a view nothing will use.
func (o *Ouroboros) reopenExpiredLocalStateQuerySession(
	requestCtx context.Context,
	connId ouroboros.ConnectionId,
	session *localstatequerySession,
) (*ledger.QueryView, error) {
	o.localstatequeryAcquireMutex.Lock()
	current := o.localstatequerySessions[connId] == session
	expired, point := session.expired, session.point
	o.localstatequeryAcquireMutex.Unlock()
	if !current || !expired || len(point.Hash) == 0 {
		return nil, ledger.ErrQueryViewClosed
	}
	ctx, cancel := context.WithTimeout(requestCtx, localStateQueryAcquireWait)
	defer cancel()
	releasePin := o.ledgerState.PinAcquiredPoint(point.Slot)
	if o.localstatequeryVerifyHook != nil {
		o.localstatequeryVerifyHook()
	}
	view, err := o.ledgerState.AcquireQueryView(ctx, point)
	if err != nil {
		releasePin()
		o.config.Logger.Warn(
			"local-state-query could not reopen an expired ledger snapshot",
			"component", "network",
			"connection_id", connId.String(),
			"slot", point.Slot,
			"error", err,
		)
		return nil, fmt.Errorf(
			"reopen expired ledger snapshot at slot %d: %w",
			point.Slot,
			err,
		)
	}
	o.localstatequeryAcquireMutex.Lock()
	if o.localstatequerySessions[connId] != session || !session.expired ||
		requestCtx.Err() != nil {
		o.localstatequeryAcquireMutex.Unlock()
		view.Close()
		releasePin()
		if err := requestCtx.Err(); err != nil {
			return nil, err
		}
		return nil, ledger.ErrQueryViewClosed
	}
	session.view = view
	session.releasePin = releasePin
	session.expired = false
	session.acquiredAt = time.Now()
	session.expiry = time.AfterFunc(
		o.localstatequeryViewMaxLifetime(),
		func() { o.expireLocalStateQuerySession(connId, session) },
	)
	o.localstatequeryAcquireMutex.Unlock()
	o.config.Logger.Info(
		"local-state-query reopened an expired ledger snapshot",
		"component", "network",
		"connection_id", connId.String(),
		"slot", point.Slot,
	)
	return view, nil
}

// localStateQueryViewTip is the block a tip acquire's view snapshotted, read
// from the view itself so it is the snapshot's tip and not a later one. It is
// the unpinned point when the chain has no block or the read fails, which
// leaves the session unable to reopen rather than reopening somewhere else.
func localStateQueryViewTip(
	ctx context.Context,
	view *ledger.QueryView,
) ledger.QueryPoint {
	result, err := view.Query(ctx, &olocalstatequery.ChainPointQuery{}, 0)
	if err != nil {
		return ledger.QueryPoint{}
	}
	tip, ok := result.(ocommon.Point)
	if !ok || len(tip.Hash) == 0 {
		return ledger.QueryPoint{}
	}
	return ledger.QueryPoint{Slot: tip.Slot, Hash: tip.Hash}
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
	acquisition := o.localstatequeryAcquisitions[connId]
	acquisitionOwned := acquisition != nil &&
		(acquisition.owner == nil || acquisition.owner == owner)
	sessionOwned := (hasPoint || hasSession) &&
		(currentOwner == nil || currentOwner == owner)
	if !acquisitionOwned && !sessionOwned {
		o.localstatequeryAcquireMutex.Unlock()
		return
	}
	if acquisitionOwned {
		delete(o.localstatequeryAcquisitions, connId)
	}
	var session *localstatequerySession
	if sessionOwned {
		session = o.removeLocalStateQuerySessionLocked(connId)
	}
	o.localstatequeryAcquireMutex.Unlock()
	if acquisitionOwned {
		acquisition.cancel()
	}
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
	acquisition := o.localstatequeryAcquisitions[connId]
	delete(o.localstatequeryAcquisitions, connId)
	session := o.removeLocalStateQuerySessionLocked(connId)
	o.localstatequeryAcquireMutex.Unlock()
	if acquisition != nil {
		acquisition.cancel()
	}
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
	acquisitions := make(
		[]*localstatequeryAcquisition,
		0,
		len(o.localstatequeryAcquisitions),
	)
	for connId, acquisition := range o.localstatequeryAcquisitions {
		acquisitions = append(acquisitions, acquisition)
		delete(o.localstatequeryAcquisitions, connId)
	}
	o.localstatequeryAcquireMutex.Unlock()
	for _, acquisition := range acquisitions {
		acquisition.cancel()
	}
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
