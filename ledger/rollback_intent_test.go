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
	"errors"
	"io"
	"log/slog"
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
		targetPoint.Hash, targetPoint.Slot, []byte("target"), true, nil,
	))
	require.NoError(t, db.SetBlockNonce(
		currentTip.Point.Hash, currentTip.Point.Slot, []byte("current"), false, nil,
	))
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = []byte("current")

	injected := errors.New("injected metadata truncation failure")
	ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	require.ErrorIs(
		t,
		ls.rollbackChainAndStateDeferred(targetPoint, nil),
		injected,
	)

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
	recoveredLS.currentTipBlockNonce = []byte("current")
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
