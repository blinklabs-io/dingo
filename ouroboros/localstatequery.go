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
	"time"

	"github.com/blinklabs-io/dingo/ledger"
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
	o.localstatequeryAcquireMutex.Lock()
	defer o.localstatequeryAcquireMutex.Unlock()
	if specific, ok := acquireTarget.(olocalstatequery.AcquireSpecificPoint); ok {
		o.localstatequeryAcquiredPoints[ctx.ConnectionId] = ledger.QueryPoint{
			Slot: specific.Point.Slot,
			Hash: specific.Point.Hash,
		}
	} else {
		delete(o.localstatequeryAcquiredPoints, ctx.ConnectionId)
	}
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
