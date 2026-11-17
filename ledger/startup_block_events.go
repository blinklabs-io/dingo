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
	"context"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
)

// startupReplayBarrier travels the chain-update stream behind any rollback the
// startup reconciliation published. The ledger's own handler closes done when
// it reaches the marker, so every Undo that handler was going to publish for
// those rollbacks is on the ledger.block lane first. Other chain-update
// subscribers switch on the payload type and ignore it.
type startupReplayBarrier struct {
	done chan struct{}
}

// waitForChainUpdateHandler returns once handleEventChainUpdate has processed
// every chain-update event published before the call.
func (ls *LedgerState) waitForChainUpdateHandler(ctx context.Context) error {
	if ls.chainUpdateSubID == 0 {
		return nil
	}
	barrier := startupReplayBarrier{done: make(chan struct{})}
	if err := ls.config.EventBus.PublishBlocking(
		chain.ChainUpdateEventType,
		event.NewEvent(chain.ChainUpdateEventType, barrier),
	); err != nil {
		return fmt.Errorf("publish startup replay barrier: %w", err)
	}
	select {
	case <-barrier.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// publishStartupReplayBlockEvents publishes a ledger.block Apply event for
// every block that is on the primary chain but not yet applied to the ledger
// when the node starts, oldest first.
//
// The chain-update handler only sees blocks added while this process runs, so
// blocks stored earlier -- the suffix after a Mithril import, or blocks a
// previous run stored but did not apply -- would otherwise reach the ledger
// through replay with a ledger.tx event and no ledger.block event, leaving a
// subscriber's first block event to be the first live block.
//
// # Ordering contract
//
// Start calls this after primary-chain reconciliation and before the slot
// clock, forger and block-processing goroutine start. The chain-update handler
// publishes the Undo for a reconciliation rollback on its own goroutine, so
// this first waits for that handler to drain; every such Undo therefore
// precedes the replayed Applies. Nothing adds a block to the chain in that
// window, so every replayed Apply precedes the event for any block added
// later, and no block is published both here and by the chain-update handler.
//
// It is a no-op when nothing listens, so a node without a block subscriber
// does not read the backlog from storage.
func (ls *LedgerState) publishStartupReplayBlockEvents(
	ctx context.Context,
) error {
	if ls.chain == nil || ls.config.EventBus == nil ||
		!ls.config.EventBus.HasSubscribers(BlockEventType) {
		return nil
	}
	if err := ls.waitForChainUpdateHandler(ctx); err != nil {
		return err
	}
	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	chainTip := ls.chain.Tip()
	// Compare points, not block numbers: a Byron epoch-boundary block carries
	// the number of the block before it, so an unapplied successor can sit at
	// the ledger tip's height.
	if pointMatches(chainTip.Point, ledgerTip.Point) {
		return nil
	}
	iter, err := ls.chain.FromPointContext(ctx, ledgerTip.Point, false)
	if err != nil {
		return fmt.Errorf(
			"iterate chain from ledger tip %d for startup block events: %w",
			ledgerTip.Point.Slot,
			err,
		)
	}
	defer iter.Cancel()
	for {
		result, err := iter.Next(false)
		if err != nil {
			if errors.Is(err, chain.ErrIteratorChainTip) {
				return nil
			}
			return fmt.Errorf("read chain for startup block events: %w", err)
		}
		if result.Rollback {
			return fmt.Errorf(
				"chain rolled back to slot %d during startup block events",
				result.Point.Slot,
			)
		}
		ls.publishBlockEvent(BlockActionApply, result.Block)
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
