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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleEventChainsyncRollbackToBlockTipDoesNotPublishLedgerRollback
// captures the wedge described in issue #2177. After the
// "fork extends from current tip" branch queues headers, the chain's
// header tip sits ahead of the block tip. If the peer then sends a
// RollBackward to the block tip, the no-op shortcut in
// handleEventChainsyncRollback compares the rollback point against
// HeaderTip and falls through to rollbackChainAndState, which calls
// ls.rollback and publishes a spurious "local ledger rollback" event
// even though the ledger never advanced past the block tip in the
// first place. That event drives RecoverAfterLocalRollback every cycle,
// matching the "replayed peer header history after local rollback"
// log line that fires every plateau cycle in the field reports.
func TestHandleEventChainsyncRollbackToBlockTipDoesNotPublishLedgerRollback(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	// Simulate the "fork extends from current tip" branch having
	// queued a peer header whose prevHash matches the current block
	// tip. The header sits in the chain's header queue, so HeaderTip
	// is ahead of Tip while the ledger has not yet advanced.
	forkHeader := mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("pinned-tip-fork-extend-1"),
		),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 5,
	}
	require.NoError(t, fixture.ls.chain.AddBlockHeader(forkHeader))
	require.Equal(t, 1, fixture.ls.chain.HeaderCount())
	require.NotEqual(
		t,
		fixture.ls.chain.Tip().Point.Slot,
		fixture.ls.chain.HeaderTip().Point.Slot,
		"setup invariant: header tip must be ahead of block tip",
	)

	// Peer sends RollBackward to the block tip. The ledger is
	// already at the block tip — there is nothing for the ledger to
	// roll back. Only the queued header needs to be cleared.
	require.NoError(t, fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.currentTip.Point,
		},
	))

	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"no local ledger rollback event should be published when "+
			"rolling back to a point the ledger already sits at",
	)
}

// TestRollbackAtCurrentTipIsNoop pins the ls.rollback contract that
// the no-op fix relies on: calling rollback with the point the ledger
// already sits at must neither mutate state nor publish a "local
// ledger rollback" resync event. This complements the chainsync-level
// test above by exercising ls.rollback directly, so future callers
// (replay recovery, iterator rollback, reconcile paths) inherit the
// same guarantee.
func TestRollbackAtCurrentTipIsNoop(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	preSeq := fixture.ls.lastLocalRollbackSeq
	require.NoError(t, fixture.ls.rollback(fixture.currentTip.Point))

	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.Equal(
		t,
		preSeq,
		fixture.ls.lastLocalRollbackSeq,
		"lastLocalRollbackSeq should not advance for a no-op rollback",
	)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"no local ledger rollback event should fire when "+
			"rolling back to the existing currentTip",
	)
}
