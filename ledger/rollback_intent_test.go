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
	"bytes"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestRollbackIntentRoundTripsUndoBlocks(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	point := ocommon.Point{Slot: 42, Hash: []byte{1, 2, 3}}
	blocks := []models.Block{{
		Hash:   []byte{4, 5, 6},
		Cbor:   []byte{0x80},
		Slot:   43,
		Number: 7,
		Type:   1,
	}}
	require.NoError(t, persistRollbackIntent(db, point, blocks))

	gotPoint, gotBlocks, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, point, gotPoint)
	require.Equal(t, blocks, gotBlocks)
	require.NoError(t, clearRollbackIntent(db))
	_, _, pending, err = loadRollbackIntent(db)
	require.NoError(t, err)
	require.False(t, pending)
}

func TestValidateAndEmitRollbackUndoRefusesBeforePersistingIntent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(*chainsyncRollbackFixture) error
		want    error
	}{
		{
			name: "Mithril trust boundary",
			prepare: func(f *chainsyncRollbackFixture) error {
				f.ls.mithrilLedgerSlot = f.ancestorTip.Point.Slot + 1
				return nil
			},
			want: ErrRollbackExceedsMithrilBoundary,
		},
		{
			name: "consumed UTxO prune floor",
			prepare: func(f *chainsyncRollbackFixture) error {
				return f.ls.db.SetSyncState(
					database.ConsumedUtxoPruneFloorSyncKey,
					strconv.FormatUint(
						f.ancestorTip.Point.Slot+1,
						10,
					),
					nil,
				)
			},
			want: ErrRollbackBelowUtxoPruneFloor,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			require.NoError(t, tc.prepare(fixture))
			err := fixture.ls.validateAndEmitRollbackUndo(
				fixture.ancestorTip.Point,
			)
			require.ErrorIs(t, err, tc.want)
			_, _, pending, err := loadRollbackIntent(fixture.ls.db)
			require.NoError(t, err)
			require.False(
				t,
				pending,
				"a refused rollback must not persist an intent that startup cannot complete",
			)
		})
	}
}

func TestRollbackNoopDoesNotFinishForeignIntent(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	block, err := database.BlockByPoint(
		fixture.ls.db,
		fixture.currentTip.Point,
	)
	require.NoError(t, err)
	foreignPoint := fixture.ancestorTip.Point
	require.NoError(t, persistRollbackIntent(
		fixture.ls.db,
		foreignPoint,
		[]models.Block{block},
	))

	err = fixture.ls.rollbackWithBlocksAndIntent(
		fixture.currentTip.Point,
		nil,
		false,
		false,
		false,
	)
	require.NoError(t, err)
	gotPoint, gotBlocks, pending, err := loadRollbackIntent(fixture.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, foreignPoint, gotPoint)
	require.Equal(t, []models.Block{block}, gotBlocks)

	aheadPoint := ocommon.NewPoint(
		fixture.currentTip.Point.Slot+1,
		testHashBytes("rollback-intent-ahead-point"),
	)
	err = fixture.ls.rollbackWithBlocksAndIntent(
		aheadPoint,
		nil,
		false,
		false,
		false,
	)
	require.NoError(t, err)
	gotPoint, gotBlocks, pending, err = loadRollbackIntent(fixture.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, foreignPoint, gotPoint)
	require.Equal(t, []models.Block{block}, gotBlocks)
}

func TestRollbackUndoSurvivesMetadataTruncationFailure(t *testing.T) {
	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	blocks := loadTestBlocksWithTxs(t, 2)
	raw := make([]chain.RawBlock, len(blocks))
	for i, block := range blocks {
		raw[i] = chain.RawBlock{
			Slot:        block.Slot,
			Hash:        append([]byte(nil), block.Hash...),
			BlockNumber: block.Number,
			Type:        block.Type,
			Cbor:        append([]byte(nil), block.Cbor...),
		}
		if i > 0 {
			raw[i].PrevHash = append([]byte(nil), raw[i-1].Hash...)
		}
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(raw))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, txSubID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(raw[1].Slot, raw[1].Hash),
		BlockNumber: raw[1].BlockNumber,
	}
	targetPoint := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	targetTip := ochainsync.Tip{
		Point:       targetPoint,
		BlockNumber: raw[0].BlockNumber,
	}
	require.NoError(t, db.SetBlockNonce(
		targetPoint.Hash, targetPoint.Slot, bytes.Repeat([]byte{0x11}, 32), true, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		currentTip.Point.Hash,
		currentTip.Point.Slot,
		bytes.Repeat([]byte{0x22}, 32),
		false,
		nil,
	))
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = bytes.Repeat([]byte{0x22}, 32)

	injected := errors.New("injected metadata truncation failure")
	ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	rollbackErr := ls.rollbackChainAndStateDeferred(targetPoint, nil)
	require.ErrorIs(t, rollbackErr, ErrChainTruncatedLedgerRollbackFailed)
	require.Contains(t, rollbackErr.Error(), injected.Error())

	// The first attempt emitted its live undo, but the metadata failure leaves
	// the outbox as the recovery source of truth after the process disappears.
	firstEvent := testutil.RequireReceive(
		t, txCh, 2*time.Second, "expected live rollback undo event",
	)
	firstUndo, ok := firstEvent.Data.(TransactionEvent)
	require.True(t, ok)
	require.True(t, firstUndo.Rollback)

	intentPoint, intentBlocks, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, targetPoint, intentPoint)
	require.Len(t, intentBlocks, 1)
	require.Equal(t, blocks[1].Cbor, intentBlocks[0].Cbor)
	require.Equal(t, targetPoint, ls.chain.Tip().Point)

	require.NoError(t, dbtest.CloseDatabase(db))
	db, err = dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	cm, err = chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	recoveryBus := event.NewEventBus(nil, nil)
	t.Cleanup(recoveryBus.Stop)
	recoverySubID, recoveryCh := recoveryBus.SubscribeWithBuffer(
		TransactionEventType,
		64,
	)
	require.NotZero(t, recoverySubID)
	t.Cleanup(func() {
		recoveryBus.Unsubscribe(TransactionEventType, recoverySubID)
	})

	recoveredLS, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          recoveryBus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	recoveredLS.currentTip = currentTip
	recoveredLS.currentTipBlockNonce = bytes.Repeat([]byte{0x22}, 32)
	recoveredLS.rollbackTruncateAfterSlotFunc = nil
	require.NoError(t, recoveredLS.recoverRollbackIntent())

	recoveredEvent := testutil.RequireReceive(
		t, recoveryCh, 2*time.Second, "expected recovered rollback undo event",
	)
	recoveredUndo, ok := recoveredEvent.Data.(TransactionEvent)
	require.True(t, ok)
	require.True(t, recoveredUndo.Rollback)
	require.Equal(t, firstUndo.Transaction.Hash(), recoveredUndo.Transaction.Hash())
	require.Equal(t, blocks[1].Slot, recoveredUndo.Point.Slot)
	require.Equal(t, blocks[1].Hash, recoveredUndo.Point.Hash)

	_, _, pending, err = loadRollbackIntent(db)
	require.NoError(t, err)
	require.False(t, pending)
	dbTip, err := db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, targetTip, dbTip)
}

// TestRecoverRollbackIntentAheadOfLedgerDeliversUndo pins the at-least-once
// outbox contract on the branch that cannot complete the rollback: an intent
// whose point sits above the applied ledger tip has nothing left to truncate,
// but its captured bodies are the only remaining record of blocks a consumer
// was already told to apply, so they must be delivered before the record is
// cleared.
func TestRecoverRollbackIntentAheadOfLedgerDeliversUndo(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	blocks := loadTestBlocksWithTxs(t, 2)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	subID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	intentPoint := ocommon.NewPoint(blocks[0].Slot, blocks[0].Hash)
	require.NoError(
		t,
		persistRollbackIntent(db, intentPoint, []models.Block{blocks[1]}),
	)

	// The applied ledger sits below the intent point, so no metadata rollback
	// is possible and the record cannot be replayed as a rollback.
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nil
	require.NoError(t, ls.recoverRollbackIntent())

	undoEvent := testutil.RequireReceive(
		t, txCh, 2*time.Second,
		"expected undo delivery for a discarded ahead-of-ledger intent",
	)
	undo, ok := undoEvent.Data.(TransactionEvent)
	require.True(t, ok)
	require.True(t, undo.Rollback)
	require.Equal(t, blocks[1].Slot, undo.Point.Slot)
	require.Equal(t, blocks[1].Hash, undo.Point.Hash)

	_, _, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.False(t, pending)
}

// TestEnsureRollbackIntentRetainsSupersededPayload pins the coalescing of a
// deeper rollback onto a pending record. The first rollback's chain truncation
// already deleted the bodies its intent captured, so re-reading the live chain
// at the deeper point cannot see them; overwriting the record with that re-read
// loses the only copy.
func TestEnsureRollbackIntentRetainsSupersededPayload(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	blocks := loadTestBlocksWithTxs(t, 3)
	raw := make([]chain.RawBlock, len(blocks))
	for i, block := range blocks {
		raw[i] = chain.RawBlock{
			Slot:        block.Slot,
			Hash:        append([]byte(nil), block.Hash...),
			BlockNumber: block.Number,
			Type:        block.Type,
			Cbor:        append([]byte(nil), block.Cbor...),
		}
		if i > 0 {
			raw[i].PrevHash = append([]byte(nil), raw[i-1].Hash...)
		}
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(raw))

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	subID, _ := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(raw[2].Slot, raw[2].Hash),
		BlockNumber: raw[2].BlockNumber,
	}
	require.NoError(t, db.SetTip(tip, nil))
	ls.currentTip = tip

	middlePoint := ocommon.NewPoint(raw[1].Slot, raw[1].Hash)
	require.NoError(t, ls.validateAndEmitRollbackUndo(middlePoint))
	_, firstBlocks, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Len(t, firstBlocks, 1)
	require.Equal(t, blocks[2].Slot, firstBlocks[0].Slot)

	// The chain truncation that follows the first intent deletes the captured
	// body, so it exists nowhere else once the record is rewritten.
	require.NoError(t, cm.PrimaryChain().Rollback(middlePoint))
	stillThere, err := ls.readBlocksAboveSlot(middlePoint.Slot)
	require.NoError(t, err)
	require.Empty(t, stillThere)

	deeperPoint := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	require.NoError(t, ls.ensureRollbackIntent(deeperPoint, nil))

	gotPoint, gotBlocks, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, deeperPoint, gotPoint)
	gotSlots := make([]uint64, 0, len(gotBlocks))
	for _, block := range gotBlocks {
		gotSlots = append(gotSlots, block.Slot)
	}
	require.ElementsMatch(
		t,
		[]uint64{blocks[1].Slot, blocks[2].Slot},
		gotSlots,
		"a deeper rollback must retain the superseded record's payload",
	)
}
