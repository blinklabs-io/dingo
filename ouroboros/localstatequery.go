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
	"errors"
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

func (o *Ouroboros) localstatequeryServerConnOpts() []olocalstatequery.LocalStateQueryOptionFunc {
	return []olocalstatequery.LocalStateQueryOptionFunc{
		olocalstatequery.WithAcquireFunc(
			o.instrumentLocalstatequeryAcquire(o.localstatequeryServerAcquire),
		),
		olocalstatequery.WithQueryFunc(
			o.instrumentLocalstatequeryQuery(o.localstatequeryServerQuery),
		),
		olocalstatequery.WithReleaseFunc(
			o.instrumentLocalstatequeryRelease(o.localstatequeryServerRelease),
		),
	}
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

// localstatequeryServerAcquire records the point the client asked to pin
// this connection's LocalStateQuery session to (blinklabs-io/dingo#382).
// AcquireSpecificPoint's slot AND hash are both recorded -- hash matters
// because identifying a point by slot alone is ambiguous across a rollback
// (a fork switch can leave a different block at the same slot than the one
// the caller acquired); LedgerState.Query.verifyPointOnChain checks the
// recorded hash against this node's current chain before answering any
// pinned query. AcquireVolatileTip and AcquireImmutableTip both clear any
// previous pin, since only a specific point makes sense to hold stable
// across a slow query -- both tip kinds are, by construction, "whatever is
// live/immutable right now", the same thing querying with no pin at all
// (a zero-value ledger.QueryPoint) already means.
//
// Not every query type honors the recorded point yet -- see
// ledger.LedgerState.Query's doc comment for which ones do.
func (o *Ouroboros) localstatequeryServerAcquire(
	ctx olocalstatequery.CallbackContext,
	acquireTarget olocalstatequery.AcquireTarget,
	reAcquire bool,
) error {
	if specific, ok := acquireTarget.(olocalstatequery.AcquireSpecificPoint); ok {
		point := ledger.QueryPoint{
			Slot: specific.Point.Slot,
			Hash: specific.Point.Hash,
		}
		// Validate synchronously, at Acquire time, rather than deferring to
		// the first Query: a rejection here has a graceful wire-level
		// AcquireFailure reply (gouroboros' handleAcquire/handleReAcquire
		// both translate ErrAcquireFailurePointNotOnChain into one), but a
		// rejection surfacing later, from the Query callback, has no such
		// path and tears down the whole connection instead
		// (blinklabs-io/dingo#4156). This point is deliberately not yet
		// recorded in localstatequeryAcquiredPoints when validation
		// fails, so a client that ignores the failure and queries anyway
		// keeps whatever point (or lack of one) it had before this call.
		if err := o.ledgerState.VerifyPointOnChain(point); err != nil {
			if errors.Is(err, ledger.ErrPointNotOnChain) {
				return fmt.Errorf(
					"%w: %w",
					olocalstatequery.ErrAcquireFailurePointNotOnChain,
					err,
				)
			}
			// Not currently reachable: VerifyPointOnChain only calls the
			// private verifyPointOnChain helper, which can only return
			// ErrPointNotOnChain above. ErrHistoricalStateUnavailable is
			// raised deep inside specific Query handlers instead (e.g.
			// circulating-supply reconstruction and GetUTxOByTxIn's
			// retention-floor check), which this Acquire-time check does
			// not run. Kept here (rather than removed) so this mapping is
			// already in place if VerifyPointOnChain's scope ever grows
			// to cover pruned-but-on-chain points too
			// (blinklabs-io/dingo#4232 review).
			if errors.Is(err, ledger.ErrHistoricalStateUnavailable) {
				return fmt.Errorf(
					"%w: %w",
					olocalstatequery.ErrAcquireFailurePointTooOld,
					err,
				)
			}
			return err
		}
		o.localstatequeryAcquireMutex.Lock()
		o.localstatequeryAcquiredPoints[ctx.ConnectionId] = point
		o.localstatequeryAcquireMutex.Unlock()
		return nil
	}
	o.localstatequeryAcquireMutex.Lock()
	delete(o.localstatequeryAcquiredPoints, ctx.ConnectionId)
	o.localstatequeryAcquireMutex.Unlock()
	return nil
}

func (o *Ouroboros) localstatequeryServerQuery(
	ctx olocalstatequery.CallbackContext,
	query olocalstatequery.QueryWrapper,
) (any, error) {
	o.localstatequeryAcquireMutex.Lock()
	at := o.localstatequeryAcquiredPoints[ctx.ConnectionId]
	o.localstatequeryAcquireMutex.Unlock()
	return o.ledgerState.Query(query.Query, at)
}

func (o *Ouroboros) localstatequeryServerRelease(
	ctx olocalstatequery.CallbackContext,
) error {
	o.localstatequeryAcquireMutex.Lock()
	delete(o.localstatequeryAcquiredPoints, ctx.ConnectionId)
	o.localstatequeryAcquireMutex.Unlock()
	return nil
}

// ReleaseLocalStateQueryAcquiredPoint clears connId's pinned point, the same
// cleanup localstatequeryServerRelease performs for a clean client Release.
// A NtC client that disconnects without ever calling Release skips that
// callback entirely, so without this the map entry would otherwise persist
// until this Ouroboros instance itself is discarded -- a one-entry-per-
// pinned-client leak. Called from the node's NtC connection-closed callback
// (handleConnManagerClosed), the NtC counterpart to HandleConnClosedEvent's
// equivalent cleanup for NtN closes.
func (o *Ouroboros) ReleaseLocalStateQueryAcquiredPoint(
	connId ouroboros.ConnectionId,
) {
	o.localstatequeryAcquireMutex.Lock()
	delete(o.localstatequeryAcquiredPoints, connId)
	o.localstatequeryAcquireMutex.Unlock()
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
	o.localstatequeryAcquireMutex.Unlock()
}

// HasLocalStateQueryAcquiredPointForTesting reports whether connId currently
// has a map entry, regardless of whether the recorded point is the pinned
// or the live/cleared zero value -- the presence of the entry itself is
// what a leak looks like, so this checks membership, not QueryPoint.pinned().
func (o *Ouroboros) HasLocalStateQueryAcquiredPointForTesting(
	connId ouroboros.ConnectionId,
) bool {
	o.localstatequeryAcquireMutex.Lock()
	defer o.localstatequeryAcquireMutex.Unlock()
	_, ok := o.localstatequeryAcquiredPoints[connId]
	return ok
}
