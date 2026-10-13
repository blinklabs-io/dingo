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
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatchRestoreIsSafeLocked pins which chain states a failed add batch may
// write its pre-batch snapshot back over.
//
// addRawBlocks releases both chain locks when its transaction closure returns
// and only then does txn.Do commit, so the Commit-failure restore runs with
// the chain open to everyone else. Rolling the primary chain back while
// blockfetch appends to it is not a corner case -- it is what a ledger
// recovery rewind does on every deterministic rejection -- and the restore
// used to write its snapshot back unconditionally. That raised tipBlockIndex
// to a value above the blocks the concurrent rollback had already deleted, so
// the chain claimed a tip it did not store: the ledger's windowed rewind then
// asked for the point a security parameter behind that tip and was told the
// block did not exist.
func TestBatchRestoreIsSafeLocked(t *testing.T) {
	t.Parallel()

	applied := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("applied")},
		BlockNumber: 10,
	}
	const appliedIndex = uint64(10)
	const appliedGeneration = uint64(1)

	for _, tc := range []struct {
		name  string
		chain *Chain
		want  bool
	}{
		{
			name: "unchanged since the batch",
			chain: &Chain{
				tipBlockIndex:      appliedIndex,
				mutationGeneration: appliedGeneration,
				currentTip:         applied,
			},
			want: true,
		},
		{
			name: "concurrent rollback lowered the tip",
			chain: &Chain{
				tipBlockIndex: appliedIndex - 4,
				currentTip: ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 60,
						Hash: []byte("rolled-back"),
					},
					BlockNumber: 6,
				},
			},
			want: false,
		},
		{
			name: "concurrent append raised the tip",
			chain: &Chain{
				tipBlockIndex: appliedIndex + 1,
				currentTip: ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 110,
						Hash: []byte("appended"),
					},
					BlockNumber: 11,
				},
			},
			want: false,
		},
		{
			name: "same index, different block",
			chain: &Chain{
				tipBlockIndex: appliedIndex,
				currentTip: ochainsync.Tip{
					Point: ocommon.Point{
						Slot: 100,
						Hash: []byte("other-fork"),
					},
					BlockNumber: 10,
				},
			},
			want: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.chain.batchRestoreIsSafeLocked(
				applied, appliedIndex, appliedGeneration,
			)
			if got != tc.want {
				t.Fatalf(
					"batchRestoreIsSafeLocked() = %v, want %v",
					got,
					tc.want,
				)
			}
		})
	}
}

// TestBlockNumberContiguous covers the block-number continuity rule that binds a
// header's self-reported block number to its parent, preventing a forged
// (inflated) number from entering the chain and winning chain selection.
func TestBlockNumberContiguous(t *testing.T) {
	t.Parallel()

	const parent = uint64(100)
	tests := []struct {
		name   string
		eraId  uint8
		number uint64
		wantOK bool
	}{
		{"shelley parent+1 ok", shelley.EraIdShelley, parent + 1, true},
		{
			"shelley same as parent rejected",
			shelley.EraIdShelley,
			parent,
			false,
		},
		{
			"shelley inflated rejected",
			shelley.EraIdShelley,
			parent + 1_000_000,
			false,
		},
		{
			"shelley below parent rejected",
			shelley.EraIdShelley,
			parent - 1,
			false,
		},
		{"shelley zero rejected", shelley.EraIdShelley, 0, false},
		{
			"byron parent+1 ok (normal block)",
			byron.EraIdByron,
			parent + 1,
			true,
		},
		{"byron same as parent ok (EBB)", byron.EraIdByron, parent, true},
		{
			"byron inflated rejected",
			byron.EraIdByron,
			parent + 1_000_000,
			false,
		},
		{"byron below parent rejected", byron.EraIdByron, parent - 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := blockNumberContiguous(tt.eraId, tt.number, parent)
			if got != tt.wantOK {
				t.Fatalf(
					"blockNumberContiguous(era=%d, number=%d, parent=%d) = %v, want %v",
					tt.eraId,
					tt.number,
					parent,
					got,
					tt.wantOK,
				)
			}
		})
	}
}

// TestHeaderSeqIsTotalAcrossChainsSharingABus pins the counter's owner.
//
// ChainHeaderEventType is one topic on the one event bus ChainManager hands to
// every chain it builds -- the primary chain at load, and both fork
// constructors. A consumer (VoteManager.rollbackProtectedLocked) compares the
// sequence numbers it receives against each other as a single total order. A
// per-Chain counter restarts at 1 on a second publishing chain, so that
// consumer would compare a fresh low number against an older high one and
// protect or prune announcements belonging to the wrong mutation.
//
// Today the primary chain is the only publisher, so this is a latent defect
// rather than a live one; NewChain and NewChainFromIntersect are exported and
// have no non-test callers. Owning the counter at the manager makes the
// guarantee structural instead of relying on that staying true.
func TestHeaderSeqIsTotalAcrossChainsSharingABus(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := NewManager(context.Background(), nil, bus)
	require.NoError(t, err)

	primary := cm.PrimaryChain()
	require.NotNil(t, primary)
	// A fork chain as the manager's constructors build one: same manager,
	// same bus. They are not used here because they need a block store to
	// resolve an intersect point, and the field under test is the counter.
	fork := &Chain{id: 2, manager: cm, eventBus: cm.eventBus}

	var got []uint64
	for _, c := range []*Chain{primary, fork, primary, fork, fork} {
		c.mutex.Lock()
		got = append(got, c.nextHeaderSeqLocked())
		c.mutex.Unlock()
	}
	require.Equal(
		t,
		[]uint64{1, 2, 3, 4, 5},
		got,
		"two chains on one bus must stamp one strictly increasing sequence",
	)
}

// TestHeaderSeqConcurrentStampsAreUnique covers the counter being shared
// across chains that stamp under their own locks rather than the manager's, so
// nothing but the atomic serializes them.
func TestHeaderSeqConcurrentStampsAreUnique(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := NewManager(context.Background(), nil, bus)
	require.NoError(t, err)

	chains := []*Chain{
		cm.PrimaryChain(),
		{id: 2, manager: cm, eventBus: cm.eventBus},
		{id: 3, manager: cm, eventBus: cm.eventBus},
	}
	const perChain = 200

	var wg sync.WaitGroup
	seqs := make([][]uint64, len(chains))
	for i, c := range chains {
		wg.Go(func() {
			out := make([]uint64, 0, perChain)
			for range perChain {
				c.mutex.Lock()
				out = append(out, c.nextHeaderSeqLocked())
				c.mutex.Unlock()
			}
			seqs[i] = out
		})
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, len(chains)*perChain)
	for _, out := range seqs {
		for _, seq := range out {
			require.NotZero(t, seq, "zero means unsequenced to consumers")
			_, dup := seen[seq]
			require.False(t, dup, "sequence %d stamped twice", seq)
			seen[seq] = struct{}{}
		}
	}
	require.Len(t, seen, len(chains)*perChain)
}

// TestHeaderSeqFallsBackForManagerlessChain documents the one case the
// manager-owned counter cannot serve: a Chain built as a struct literal
// without a manager, which only this package's tests do. Such a chain shares
// no bus with any other, so its own counter is already a complete order.
func TestHeaderSeqFallsBackForManagerlessChain(t *testing.T) {
	c := &Chain{}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	require.Equal(t, uint64(1), c.nextHeaderSeqLocked())
	require.Equal(t, uint64(2), c.nextHeaderSeqLocked())
}

func TestIntersectPointsKeepsCurrentTipFallback(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })

	persistedBlock := models.Block{
		ID:     initialBlockIndex,
		Slot:   1,
		Hash:   bytes.Repeat([]byte{0x01}, 32),
		Number: 1,
		Cbor:   []byte{0x80},
	}
	require.NoError(t, db.BlockCreate(persistedBlock, nil))

	cm, err := NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()

	pendingTip := ocommon.NewPoint(2, bytes.Repeat([]byte{0xde}, 32))
	c.currentTip = ochainsync.Tip{
		Point:       pendingTip,
		BlockNumber: 2,
	}
	c.tipBlockIndex = initialBlockIndex + 1

	points := c.IntersectPoints(context.Background(), 4)
	require.Len(t, points, 2)
	require.Equal(t, pendingTip.Slot, points[0].Slot)
	require.Equal(t, pendingTip.Hash, points[0].Hash)
	require.Equal(t, persistedBlock.Slot, points[1].Slot)
	require.Equal(t, persistedBlock.Hash, points[1].Hash)
}

func TestIntersectPointsSkipsMissingDenseBlockIndex(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dbtest.CloseDatabase(db) })

	var prevHash []byte
	for slot := uint64(1); slot <= 40; slot++ {
		hash := bytes.Repeat([]byte{byte(slot)}, 32)
		require.NoError(t, db.BlockCreate(models.Block{
			ID:       slot,
			Slot:     slot,
			Hash:     hash,
			Number:   slot,
			Type:     1,
			PrevHash: prevHash,
			Cbor:     []byte{0x80},
		}, nil))
		prevHash = hash
	}

	cm, err := NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	c := cm.PrimaryChain()

	txn := db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return db.Blob().Delete(
			txn.Blob(),
			dbtypes.BlockBlobIndexKey(39),
		)
	}))

	points := c.IntersectPoints(context.Background(), 40)
	require.GreaterOrEqual(t, len(points), intersectDensePointCount)

	pointSlots := make(map[uint64]struct{}, len(points))
	for _, point := range points {
		pointSlots[point.Slot] = struct{}{}
	}
	_, hasMissingIndexSlot := pointSlots[39]
	require.False(t, hasMissingIndexSlot)
	_, hasPreviousDenseSlot := pointSlots[38]
	require.True(t, hasPreviousDenseSlot)
}

// TestCheckEphemeralBufferSpan covers rollbackLocked's buffer precondition.
//
// No public call path reaches the error: AddBlock and reconcile keep a fork's
// tip index and its in-memory buffer in step, and the behavioral rollback
// tests exercise only consistent chains. The check still has to be correct,
// because the deletion loop indexes that buffer per rolled-back block — a
// short buffer detected part-way through would leave the rollback half
// applied. Drive it directly rather than leaving the branch unexecuted.
func TestCheckEphemeralBufferSpan(t *testing.T) {
	t.Parallel()

	points := func(n int) []ocommon.Point {
		out := make([]ocommon.Point, n)
		for i := range out {
			out[i] = ocommon.Point{Slot: uint64(i) * 20} //nolint:gosec
		}
		return out
	}

	for _, tc := range []struct {
		name       string
		persistent bool
		tipIndex   uint64
		lastCommon uint64
		bufferLen  int
		wantErr    bool
	}{
		{
			name:       "buffer exactly spans the fork",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  3,
		},
		{
			name:       "buffer longer than the fork",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  4,
		},
		{
			name:       "buffer one short",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  2,
			wantErr:    true,
		},
		{
			name:       "buffer empty with blocks above the fork",
			tipIndex:   6,
			lastCommon: 3,
			bufferLen:  0,
			wantErr:    true,
		},
		{
			name:       "tip at the fork point needs no buffer",
			tipIndex:   3,
			lastCommon: 3,
			bufferLen:  0,
		},
		{
			name:       "tip below the fork point needs no buffer",
			tipIndex:   2,
			lastCommon: 3,
			bufferLen:  0,
		},
		{
			name:       "persistent chains keep no buffer",
			persistent: true,
			tipIndex:   6,
			lastCommon: 0,
			bufferLen:  0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &Chain{
				persistent:           tc.persistent,
				tipBlockIndex:        tc.tipIndex,
				lastCommonBlockIndex: tc.lastCommon,
				blocks:               points(tc.bufferLen),
			}
			err := c.checkEphemeralBufferSpan()
			if tc.wantErr {
				if !errors.Is(err, ErrRollbackBeyondEphemeralChain) {
					t.Fatalf(
						"want ErrRollbackBeyondEphemeralChain, got %v",
						err,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})
	}
}

// TestRollbackForkDepthSaturates keeps underflow fix covered at
// the unit level.
//
// TestRollbackRejectsPointAheadOfTip asserts that a rollback point above the
// tip is refused outright as not-on-chain, so it no longer drives
// rollbackForkDepth with such an index. The saturating computation still has to
// be correct: any future caller that reaches it with a point above the tip must
// get zero, not a wrapped-around uint64 that reads as a fork deeper than any
// security parameter and denies every peer.
func TestRollbackForkDepthSaturates(t *testing.T) {
	t.Parallel()

	c := &Chain{
		tipBlockIndex: 4,
		currentTip: ochainsync.Tip{
			Point:       ocommon.Point{Slot: 60, Hash: []byte("tip")},
			BlockNumber: 4,
		},
	}
	point := ocommon.Point{Slot: 100, Hash: []byte("ahead")}

	for _, tc := range []struct {
		name               string
		rollbackBlockIndex uint64
		want               uint64
	}{
		{name: "behind tip", rollbackBlockIndex: 1, want: 3},
		{name: "at tip", rollbackBlockIndex: 4, want: 0},
		{name: "one ahead of tip", rollbackBlockIndex: 5, want: 0},
		{name: "far ahead of tip", rollbackBlockIndex: 1 << 40, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := c.rollbackForkDepth(point, tc.rollbackBlockIndex)
			if got != tc.want {
				t.Fatalf(
					"rollbackForkDepth(tip=%d, rollback=%d) = %d, want %d",
					c.tipBlockIndex,
					tc.rollbackBlockIndex,
					got,
					tc.want,
				)
			}
		})
	}
}

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
			cm, err := NewManager(context.Background(), db, bus)
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
			_, err = c.AddBlockWithPointDeferred(
				context.Background(),
				blocks[0],
				point,
				txn,
			)
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

	cm, err := NewManager(context.Background(), nil, bus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(
			context.Background(),
			blocks[i],
			ocommon.Point{
				Slot: blocks[i].SlotNumber(),
				Hash: blocks[i].Hash().Bytes(),
			},
			nil,
		)
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

	evts, err := c.rollbackLocked(context.Background(), ocommon.Point{
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

	cm, err := NewManager(context.Background(), nil, bus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	for i := range blocks {
		_, addErr := c.AddBlockWithPointDeferred(
			context.Background(),
			blocks[i],
			ocommon.Point{
				Slot: blocks[i].SlotNumber(),
				Hash: blocks[i].Hash().Bytes(),
			},
			nil,
		)
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
	evts, err := c.rollbackLocked(context.Background(), rollbackPoint, false)
	require.NoError(t, err)
	require.NotEmpty(t, evts)
	_, err = c.AddBlockWithPointDeferred(
		context.Background(),
		blocks[1],
		ocommon.Point{
			Slot: blocks[1].SlotNumber(),
			Hash: blocks[1].Hash().Bytes(),
		},
		nil,
	)
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
