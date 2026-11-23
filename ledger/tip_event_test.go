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
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

func TestRollbackPublishesTipEvent(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	subID, tipCh := bus.SubscribeWithBuffer(TipEventType, 16)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TipEventType, subID) })

	require.NoError(
		t, ls.rollback(context.Background(), fixture.ancestorTip.Point),
	)

	evt := testutil.RequireReceive(
		t, tipCh, testutil.AsyncWait, "ledger.tip event after rollback",
	)
	got, ok := evt.Data.(TipEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, fixture.ancestorTip, got.Tip)
	require.NotNil(t, got.RollbackTo)
	require.Equal(t, fixture.ancestorTip.Point, *got.RollbackTo)
}

func TestSameTipRollbackPublishesNoTipEvent(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	subID, tipCh := bus.SubscribeWithBuffer(TipEventType, 16)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TipEventType, subID) })

	require.NoError(
		t, ls.rollback(context.Background(), fixture.currentTip.Point),
	)
	require.True(t, bus.FlushOrderedContext(t.Context(), TipEventType))
	require.Empty(t, tipCh)
}

func TestBlockBatchCommitPublishesTipEvent(t *testing.T) {
	t.Parallel()

	ls, lastByron, _ := newByronShelleyBoundaryLedger(t)
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	subID, tipCh := bus.SubscribeWithBuffer(TipEventType, 16)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(TipEventType, subID) })

	parentTip := ochainsync.Tip{}
	fxRollback(t, ls, parentTip.Point)
	require.NoError(t, ls.db.SetTip(parentTip, nil))
	ls.currentTip = parentTip
	ls.currentTipBlockNonce = nil
	ls.validationEnabled = false
	ls.publishSnapshotsLocked()

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{lastByron}}
	close(results)
	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(), results,
	))

	evt := testutil.RequireReceive(
		t, tipCh, testutil.AsyncWait, "ledger.tip event after block commit",
	)
	got, ok := evt.Data.(TipEvent)
	require.True(t, ok, "unexpected payload %T", evt.Data)
	require.Equal(t, lastByron.SlotNumber(), got.Tip.Point.Slot)
	require.Equal(t, lastByron.Hash().Bytes(), got.Tip.Point.Hash)
	require.Equal(t, lastByron.BlockNumber(), got.Tip.BlockNumber)
	require.Nil(t, got.RollbackTo)
	// The event follows the commit: the durable tip already names the block.
	dbTip, err := ls.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, got.Tip, dbTip)
}
