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

package chain

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallerTxnEventsWaitForCommit verifies that transaction-owned updates
// keep their sequencer position but cannot reach subscribers before commit,
// and disappear with the in-memory add when the transaction aborts.
func TestCallerTxnEventsWaitForCommit(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		finish      func(*database.Txn) error
		wantPublish bool
	}{
		{
			name:        "commit publishes",
			finish:      (*database.Txn).Commit,
			wantPublish: true,
		},
		{
			name:   "rollback retracts",
			finish: (*database.Txn).Rollback,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Stop)
			subID, updates := bus.Subscribe(ChainUpdateEventType)
			defer bus.Unsubscribe(ChainUpdateEventType, subID)
			cm, err := NewManager(db, bus)
			require.NoError(t, err)
			c := cm.PrimaryChain()
			blocks, err := testfixtures.GenerateConwayChain(1)
			require.NoError(t, err)
			point := ocommon.Point{
				Slot: blocks[0].SlotNumber(),
				Hash: blocks[0].Hash().Bytes(),
			}
			txn := db.BlobTxn(true)
			defer txn.Release()
			_, err = c.AddBlockWithPointDeferred(blocks[0], point, txn)
			require.NoError(t, err)
			c.PublishPendingChainUpdates()
			testutil.RequireNoReceive(
				t,
				updates,
				50*time.Millisecond,
				"caller transaction event published before the transaction finished",
			)
			require.NoError(t, tc.finish(txn))
			if tc.wantPublish {
				evt := testutil.RequireReceive(
					t,
					updates,
					time.Second,
					"committed caller transaction did not publish",
				)
				_, ok := evt.Data.(ChainBlockEvent)
				require.True(t, ok)
				require.Equal(t, point, c.Tip().Point)
				return
			}
			c.PublishPendingChainUpdates()
			testutil.RequireNoReceive(
				t,
				updates,
				50*time.Millisecond,
				"aborted caller transaction left an update queued",
			)
			require.Equal(t, ocommon.Point{}, c.Tip().Point)
		})
	}
}

// TestNonDeferredRollbackQueuesEventsOnSequencer pins the mechanism rather
// than the timing. A non-deferred Rollback used to publish its chain.update
// inline, after draining the sequencer. A deferred block add that mutated the
// chain *after* the rollback is already on that sequencer, so the drain
// published the add first and the rollback followed -- inverting mutation
// order for every chain.update subscriber.
//
// Enqueueing the rollback's own events on the same sequencer is what makes the
// published order equal the mutation order, so that is what is asserted here:
// after rollbackLocked returns, its events are on the sequencer and nothing
// has been published yet.
func TestNonDeferredRollbackQueuesEventsOnSequencer(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	subId, ch := bus.Subscribe(ChainUpdateEventType)
	defer bus.Unsubscribe(ChainUpdateEventType, subId)

	cm, err := NewManager(nil, bus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(blocks[i], ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}, nil)
		require.NoError(t, addErr)
	}
	c.PublishPendingChainUpdates()
	for range blocks {
		select {
		case <-ch:
		default:
			t.Fatal("expected the three block adds to be published")
		}
	}

	evts, err := c.rollbackLocked(ocommon.Point{
		Slot: blocks[0].SlotNumber(),
		Hash: blocks[0].Hash().Bytes(),
	}, false)
	require.NoError(t, err)
	require.NotEmpty(t, evts, "the rollback removed blocks")

	// Nothing published yet, and the rollback is on the sequencer.
	select {
	case evt := <-ch:
		t.Fatalf(
			"rollbackLocked must not publish; got %T",
			evt.Data,
		)
	default:
	}
	// The sequencer holds every returned event, plus the header
	// invalidation, which is deliberately never handed back to the caller.
	c.pendingUpdatesMutex.Lock()
	queued := make([]event.Event, 0, len(c.pendingUpdates))
	for _, update := range c.pendingUpdates {
		queued = append(queued, update.event)
	}
	c.pendingUpdatesMutex.Unlock()
	countByType := func(evts []event.Event, want event.EventType) int {
		n := 0
		for _, evt := range evts {
			if evt.Type == want {
				n++
			}
		}
		return n
	}
	assert.Equal(
		t,
		countByType(evts, ChainUpdateEventType),
		countByType(queued, ChainUpdateEventType),
		"every rollback chain.update must be on the chain-level sequencer",
	)
	assert.Equal(
		t,
		1,
		countByType(queued, ChainHeaderEventType),
		"the header invalidation rides the same sequencer",
	)
}

// TestDeferredAddAfterNonDeferredRollbackKeepsMutationOrder is the ordering
// consequence: a block added after the rollback must be published after it.
// With the rollback published inline this could not be guaranteed, because the
// inline publish happened after the drain that carried the later add.
func TestDeferredAddAfterNonDeferredRollbackKeepsMutationOrder(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	subId, ch := bus.Subscribe(ChainUpdateEventType)
	defer bus.Unsubscribe(ChainUpdateEventType, subId)

	cm, err := NewManager(nil, bus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(blocks[i], ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}, nil)
		require.NoError(t, addErr)
	}
	c.PublishPendingChainUpdates()
	for range blocks {
		<-ch
	}

	// Mutation order: roll back to block 0, then re-add block 1. Both are
	// enqueued on the one sequencer, so the drain has to publish them in
	// that order.
	rollbackPoint := ocommon.Point{
		Slot: blocks[0].SlotNumber(),
		Hash: blocks[0].Hash().Bytes(),
	}
	evts, err := c.rollbackLocked(rollbackPoint, false)
	require.NoError(t, err)
	require.NotEmpty(t, evts)
	_, err = c.AddBlockWithPointDeferred(blocks[1], ocommon.Point{
		Slot: blocks[1].SlotNumber(),
		Hash: blocks[1].Hash().Bytes(),
	}, nil)
	require.NoError(t, err)

	c.PublishPendingChainUpdates()

	first := <-ch
	_, isRollback := first.Data.(ChainRollbackEvent)
	require.True(
		t,
		isRollback,
		"the rollback mutated the chain first, so it must publish first; got %T",
		first.Data,
	)
	var sawReAdd bool
	for range 2 {
		select {
		case evt := <-ch:
			if add, ok := evt.Data.(ChainBlockEvent); ok {
				assert.Equal(
					t,
					blocks[1].SlotNumber(),
					add.Point.Slot,
				)
				sawReAdd = true
			}
		default:
		}
		if sawReAdd {
			break
		}
	}
	assert.True(t, sawReAdd, "the re-added block publishes after the rollback")
}
