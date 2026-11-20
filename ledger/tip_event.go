// Copyright 2024 Blink Labs Software
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
	"context"

	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TipEventType carries a TipEvent each time the ledger's durable tip moves.
const TipEventType event.EventType = "ledger.tip"

// TipEvent reports the ledger's durable tip after a commit: a block batch, a
// forged block, or a rollback. Unlike ledger.block, which is published
// before the ledger applies a block, it follows the commit, so a consumer that
// reads storage on receipt sees the state the tip names.
//
// It is a wake signal, not a log: a consumer must read the durable tip and
// catch up from its own position, which also covers blocks applied during
// startup replay, for which no event is published.
type TipEvent struct {
	// Tip is the ledger tip after the commit.
	Tip ochainsync.Tip
	// RollbackTo is the point the ledger rolled back to, or nil when the tip
	// advanced.
	RollbackTo *ocommon.Point
}

// publishTipEvent publishes a TipEvent on the ordered ledger.tip lane. It
// must be called after the commit it describes and with no ledger lock held:
// the ordered lane can wait on a full subscriber.
func (ls *LedgerState) publishTipEvent(
	ctx context.Context,
	tip ochainsync.Tip,
	rollbackTo *ocommon.Point,
) {
	if ls.config.EventBus == nil {
		return
	}
	publishCtx := ls.publishCtx //nolint:contextcheck // the ledger lifecycle context bounds the publish
	if publishCtx == nil {
		publishCtx = ctx
	}
	ls.config.EventBus.PublishOrderedContext(
		publishCtx,
		TipEventType,
		event.NewEvent(TipEventType, TipEvent{
			Tip:        tip,
			RollbackTo: rollbackTo,
		}),
	)
}
