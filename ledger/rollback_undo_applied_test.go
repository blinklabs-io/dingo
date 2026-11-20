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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// addUnappliedBlock puts a block on the primary chain above the ledger tip,
// as catch-up leaves it, and returns it. Its CBOR does not decode, so an undo
// built for it reports a decode error at its point.
func addUnappliedBlock(t *testing.T, f *chainsyncRollbackFixture) models.Block {
	t.Helper()
	raw := chain.RawBlock{
		Slot:        f.currentTip.Point.Slot + 5,
		Hash:        testHashBytes("unapplied-block"),
		BlockNumber: f.currentTip.BlockNumber + 1,
		Type:        1,
		PrevHash:    f.currentTip.Point.Hash,
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		f.ls.chain.AddRawBlocks(context.Background(), []chain.RawBlock{raw}),
	)
	blk, err := database.BlockByPoint(
		context.Background(),
		f.ls.db,
		ocommon.NewPoint(raw.Slot, raw.Hash),
	)
	require.NoError(t, err)
	return blk
}

// requireUndoOnlyFor receives the undo decode errors and requires exactly
// one, for the applied block at want.
func requireUndoOnlyFor(
	t *testing.T,
	errCh <-chan event.Event,
	want ocommon.Point,
) {
	t.Helper()
	evt := testutil.RequireReceive(t, errCh, testutil.AsyncWait,
		"undo for the applied block")
	got, ok := evt.Data.(LedgerErrorEvent)
	require.True(t, ok)
	require.Equal(t, "rollback_tx_undo_decode", got.Operation)
	require.Equal(t, want, got.Point,
		"the first undo must be for the applied tip, not a block above it")
	testutil.RequireNoReceive(t, errCh, 200*time.Millisecond,
		"no undo for a block the ledger never applied")
}

func TestRollbackUndoSkipsBlocksAboveTheLedgerTip(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	addUnappliedBlock(t, f)
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, errCh := bus.Subscribe(LedgerErrorEventType)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })

	f.ls.transactionEventMutex.Lock()
	err := f.ls.validateAndEmitRollbackUndo(t.Context(), f.ancestorTip.Point)
	f.ls.transactionEventMutex.Unlock()
	require.NoError(t, err)
	requireUndoOnlyFor(t, errCh, f.currentTip.Point)
	_, intentBlocks, pending, err := loadRollbackIntent(f.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Len(t, intentBlocks, 1, "the intent must not replay it either")
}

func TestRollbackUndoSkipsCompetingBlockBelowTheLedgerTip(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	competing := models.Block{
		Slot:     15,
		Hash:     testHashBytes("competing-block-below-tip"),
		PrevHash: f.ancestorTip.Point.Hash,
		Number:   f.currentTip.BlockNumber,
		Type:     1,
		Cbor:     []byte{0x80},
	}
	require.NoError(t, f.ls.db.BlockCreate(competing, nil))

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, errCh := bus.Subscribe(LedgerErrorEventType)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })

	f.ls.transactionEventMutex.Lock()
	err := f.ls.validateAndEmitRollbackUndo(t.Context(), f.ancestorTip.Point)
	f.ls.transactionEventMutex.Unlock()
	require.NoError(t, err)
	requireUndoOnlyFor(t, errCh, f.currentTip.Point)
	_, intentBlocks, pending, err := loadRollbackIntent(f.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Len(t, intentBlocks, 1)
	require.Equal(t, f.currentTip.Point.Hash, intentBlocks[0].Hash)
}

func TestRollbackUndoFromSlotZeroBlockToOrigin(t *testing.T) {
	t.Parallel()
	ls, slotZeroTip, _ := newSlotZeroRollbackLedger(
		t,
		conway.BlockTypeConway,
	)
	t.Cleanup(func() { require.NoError(t, ls.Close()) })
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	subID, errCh := bus.SubscribeWithBuffer(LedgerErrorEventType, 4)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })
	oldTip := ls.Tip().Point

	require.NoError(t, ls.rollbackChainAndStateDeferred(
		t.Context(),
		ocommon.NewPointOrigin(),
		nil,
	))

	for _, want := range []ocommon.Point{oldTip, slotZeroTip.Point} {
		evt := testutil.RequireReceive(t, errCh, testutil.AsyncWait,
			"undo for applied block")
		got, ok := evt.Data.(LedgerErrorEvent)
		require.True(t, ok)
		require.Equal(t, "rollback_tx_undo_decode", got.Operation)
		require.Equal(t, want, got.Point)
	}
	testutil.RequireNoReceive(t, errCh, 200*time.Millisecond,
		"no extra rollback undo")
	require.Equal(t, ocommon.NewPointOrigin(), ls.Tip().Point)
}

func TestChainIteratorRollbackUndoSkipsBlocksAboveTheLedgerTip(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	unapplied := addUnappliedBlock(t, f)
	applied, err := database.BlockByPoint(
		context.Background(),
		f.ls.db,
		f.currentTip.Point,
	)
	require.NoError(t, err)
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, errCh := bus.Subscribe(LedgerErrorEventType)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })
	require.NoError(
		t,
		f.ls.chain.Rollback(context.Background(), f.ancestorTip.Point),
	)

	require.NoError(t, f.ls.processChainIteratorRollback(
		t.Context(),
		f.ancestorTip.Point,
		[]models.Block{unapplied, applied},
	))
	requireUndoOnlyFor(t, errCh, f.currentTip.Point)
}

func TestChainIteratorRollbackAheadOfLedgerTipNeedsNoUndo(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	first := addUnappliedBlock(t, f)
	secondRaw := chain.RawBlock{
		Slot:        first.Slot + 5,
		Hash:        testHashBytes("second-unapplied-block"),
		BlockNumber: first.Number + 1,
		Type:        1,
		PrevHash:    first.Hash,
		Cbor:        []byte{0x80},
	}
	require.NoError(t, f.ls.chain.AddRawBlocks(
		context.Background(),
		[]chain.RawBlock{secondRaw},
	))
	second, err := database.BlockByPoint(
		context.Background(),
		f.ls.db,
		ocommon.NewPoint(secondRaw.Slot, secondRaw.Hash),
	)
	require.NoError(t, err)
	require.NoError(t, f.ls.chain.Rollback(
		context.Background(),
		ocommon.NewPoint(first.Slot, first.Hash),
	))
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, errCh := bus.Subscribe(LedgerErrorEventType)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })

	require.NoError(t, f.ls.processChainIteratorRollback(
		t.Context(),
		ocommon.NewPoint(first.Slot, first.Hash),
		[]models.Block{second},
	))
	testutil.RequireNoReceive(t, errCh, 200*time.Millisecond,
		"no undo for blocks ahead of the durable ledger tip")
	require.Equal(t, f.currentTip, f.ls.currentTip)
}

func TestChainIteratorRollbackUndoUsesResolvedSameSlotTarget(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	applied, err := database.BlockByPoint(
		context.Background(),
		f.ls.db,
		f.currentTip.Point,
	)
	require.NoError(t, err)
	putPrimaryChainOnForkBeyondK(t, f, "resolved-undo-target")
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	f.ls.config.EventBus = bus
	subID, errCh := bus.Subscribe(LedgerErrorEventType)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, subID) })
	competitor := ocommon.NewPoint(
		f.currentTip.Point.Slot,
		testHashBytes("same-slot-rollback-competitor"),
	)

	err = f.ls.processChainIteratorRollback(
		t.Context(),
		competitor,
		[]models.Block{applied},
	)
	require.ErrorIs(t, err, errRestartLedgerPipeline)
	requireUndoOnlyFor(t, errCh, f.currentTip.Point)
	require.Equal(t, f.ancestorTip, f.ls.currentTip)
	_, _, pending, err := loadRollbackIntent(f.ls.db)
	require.NoError(t, err)
	require.False(t, pending)
}

func TestAppliedAncestryIncludesRetainedSameSlotParent(t *testing.T) {
	t.Parallel()
	f := newChainsyncRollbackFixture(t)
	main, err := database.BlockByPoint(
		context.Background(),
		f.ls.db,
		f.currentTip.Point,
	)
	require.NoError(t, err)
	ebb := models.Block{
		Slot:     main.Slot,
		Hash:     testHashBytes("retained-same-slot-parent"),
		PrevHash: f.ancestorTip.Point.Hash,
		Number:   main.Number - 1,
		Type:     0,
		Cbor:     []byte{0x80},
	}
	require.NoError(t, f.ls.db.BlockCreate(ebb, nil))
	main.PrevHash = ebb.Hash

	blocks, err := f.ls.readAppliedBlocksBetweenIncluding(
		t.Context(),
		f.currentTip.Point,
		f.ancestorTip.Point,
		[]models.Block{main},
	)
	require.NoError(t, err)
	require.Len(t, blocks, 2)
	require.Equal(t, main.Hash, blocks[0].Hash)
	require.Equal(t, ebb.Hash, blocks[1].Hash)
	require.Equal(t, f.ancestorTip.Point.Hash, blocks[1].PrevHash)
}
