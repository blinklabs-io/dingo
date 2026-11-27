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
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestRollbackIntentRoundTripsUndoBlocks(t *testing.T) {
	t.Parallel()
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
	require.NoError(t, persistRollbackIntent(db, point, blocks, nil))

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

func TestValidateAndEmitRollbackUndoRefusesBeforePersistingIntent(
	t *testing.T,
) {
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
			name: "live UTxO set journal",
			prepare: func(f *chainsyncRollbackFixture) error {
				fxEmptyJournal(t, f.ls)
				return nil
			},
			want: ErrRollbackBelowUtxoJournal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fixture := newChainsyncRollbackFixture(t)
			require.NoError(t, tc.prepare(fixture))
			err := fixture.ls.validateAndEmitRollbackUndo(context.Background(),
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
		context.Background(),
		fixture.ls.db,
		fixture.currentTip.Point,
	)
	require.NoError(t, err)
	foreignPoint := fixture.ancestorTip.Point
	require.NoError(t, persistRollbackIntent(
		fixture.ls.db,
		foreignPoint,
		[]models.Block{block},
		nil,
	))

	err = fixture.ls.rollbackWithBlocksAndIntent(
		context.Background(),
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
		context.Background(),
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
	t.Parallel()
	dataDir := t.TempDir()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)

	cm, err := chain.NewManager(context.Background(), db, nil)
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
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), raw),
	)

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, txSubID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	// The set lives under the data directory, where the restart below finds
	// it again.
	set, closeSet := OpenLiveSetAtForTest(t, db, dataDir)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		UtxoSet:           set,
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
	require.NoError(t, seedBlockNonce(
		t,
		db,
		targetPoint.Hash,
		targetPoint.Slot,
		bytes.Repeat([]byte{0x11}, 32),
		true,
		nil,
	))
	require.NoError(t, seedBlockNonce(t, db,
		currentTip.Point.Hash,
		currentTip.Point.Slot,
		bytes.Repeat([]byte{0x22}, 32),
		false,
		nil,
	))
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = bytes.Repeat([]byte{0x22}, 32)
	fxJournalChain(t, ls)

	injected := errors.New("injected metadata truncation failure")
	ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		ledgerdb.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	rollbackErr := ls.rollbackChainAndStateDeferred(
		context.Background(),
		targetPoint,
		nil,
	)
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

	require.NoError(t, closeSet())
	require.NoError(t, dbtest.CloseDatabase(db))
	db, err = dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	cm, err = chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	recoveryBus := newTestEventBus(t, nil)
	t.Cleanup(recoveryBus.Stop)
	recoverySubID, recoveryCh := recoveryBus.SubscribeWithBuffer(
		TransactionEventType,
		64,
	)
	require.NotZero(t, recoverySubID)
	t.Cleanup(func() {
		recoveryBus.Unsubscribe(TransactionEventType, recoverySubID)
	})

	recoveredSet, _ := OpenLiveSetAtForTest(t, db, dataDir)
	recoveredLS, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		UtxoSet:           recoveredSet,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          recoveryBus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	recoveredLS.currentTip = currentTip
	recoveredLS.currentTipBlockNonce = bytes.Repeat([]byte{0x22}, 32)
	recoveredLS.rollbackTruncateAfterSlotFunc = nil
	require.NoError(t, recoveredLS.recoverRollbackIntent(context.Background()))

	recoveredEvent := testutil.RequireReceive(
		t, recoveryCh, 2*time.Second, "expected recovered rollback undo event",
	)
	recoveredUndo, ok := recoveredEvent.Data.(TransactionEvent)
	require.True(t, ok)
	require.True(t, recoveredUndo.Rollback)
	require.Equal(
		t,
		firstUndo.Transaction.Hash(),
		recoveredUndo.Transaction.Hash(),
	)
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
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	blocks := loadTestBlocksWithTxs(t, 2)
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	subID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		UtxoSet:           OpenLiveSetForTest(t, db),
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	intentPoint := ocommon.NewPoint(blocks[0].Slot, blocks[0].Hash)
	require.NoError(
		t,
		persistRollbackIntent(db, intentPoint, []models.Block{blocks[1]}, nil),
	)

	// The applied ledger sits below the intent point, so no metadata rollback
	// is possible and the record cannot be replayed as a rollback.
	ls.currentTip = ochainsync.Tip{}
	ls.currentTipBlockNonce = nil
	require.NoError(t, ls.recoverRollbackIntent(context.Background()))

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
	cm, err := chain.NewManager(context.Background(), db, nil)
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
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), raw),
	)

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	subID, _ := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		UtxoSet:           OpenLiveSetForTest(t, db),
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
	require.NoError(
		t,
		ls.validateAndEmitRollbackUndo(context.Background(), middlePoint),
	)
	_, firstBlocks, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Len(t, firstBlocks, 1)
	require.Equal(t, blocks[2].Slot, firstBlocks[0].Slot)

	// The chain truncation that follows the first intent deletes the captured
	// body, so it exists nowhere else once the record is rewritten.
	require.NoError(
		t,
		cm.PrimaryChain().Rollback(context.Background(), middlePoint),
	)
	stillThere, err := ls.readBlocksAboveSlot(
		context.Background(),
		middlePoint.Slot,
	)
	require.NoError(t, err)
	require.Empty(t, stillThere)

	deeperPoint := ocommon.NewPoint(raw[0].Slot, raw[0].Hash)
	require.NoError(
		t,
		ls.ensureRollbackIntent(context.Background(), deeperPoint, nil),
	)

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

// rolledBackBlockBodies counts the rolled-back bodies the block store keeps.
func rolledBackBlockBodies(t *testing.T, db *database.Database) int {
	t.Helper()
	txn := db.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	prefix := []byte(dbtypes.RolledBackBlockKeyPrefix)
	it := txn.BlobStore().NewIterator(
		txn.Blob(), dbtypes.BlobIteratorOptions{Prefix: prefix},
	)
	require.NotNil(t, it)
	defer it.Close()
	count := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	require.NoError(t, it.Err())
	return count
}

// The record names blocks; it does not carry them. A body the chain no
// longer holds is kept by the block store instead, and a body still on the
// chain is not copied at all.
func TestRollbackIntentRecordCarriesNoBlockBodies(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	onChain := models.Block{
		ID: 1, Slot: 10, Number: 1, Type: 1,
		Hash: bytes.Repeat([]byte{1}, 32),
		Cbor: bytes.Repeat([]byte{0x81}, 4096),
	}
	require.NoError(t, db.BlockCreate(onChain, nil))
	removed := models.Block{
		ID: 2, Slot: 20, Number: 2, Type: 1,
		Hash: bytes.Repeat([]byte{2}, 32),
		Cbor: bytes.Repeat([]byte{0x82}, 4096),
	}
	point := ocommon.Point{Slot: 5, Hash: []byte{5}}
	require.NoError(t, persistRollbackIntent(
		db, point, []models.Block{removed, onChain}, nil,
	))

	record, err := db.GetChainState(database.RollbackIntentKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, record)
	require.Less(t, len(record), 1024, "the record carries block bodies")
	require.Nil(t, db.Metadata(), "no sync_state can hold the intent")
	require.Equal(t, 1, rolledBackBlockBodies(t, db))

	_, got, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Len(t, got, 2)
	require.Equal(t, removed.Cbor, got[0].Cbor)
	require.Equal(t, onChain.Cbor, got[1].Cbor)
}

// rollbackCrashFixture is a two-block chain the ledger has applied, with a
// transaction subscriber, on a data directory a second ledger can reopen.
type rollbackCrashFixture struct {
	dataDir  string
	db       *database.Database
	ls       *LedgerState
	closeSet func() error
	txCh     <-chan event.Event
	// unsubscribe removes the transaction subscriber, the only consumer of
	// rollback undo events the fixture has.
	unsubscribe func()
	blocks      []models.Block
	tip         ochainsync.Tip
	target      ochainsync.Tip
}

func newRollbackCrashFixture(t *testing.T) *rollbackCrashFixture {
	t.Helper()
	f := &rollbackCrashFixture{dataDir: t.TempDir()}
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: f.dataDir})
	require.NoError(t, err)
	f.db = db
	// The set lives under the data directory before anything is seeded into
	// it, so a restart finds what was seeded.
	OpenLiveSetAtForTest(t, db, f.dataDir)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	f.blocks = loadTestBlocksWithTxs(t, 2)
	raw := make([]chain.RawBlock, len(f.blocks))
	for i, block := range f.blocks {
		raw[i] = chain.RawBlock{
			Slot:        block.Slot,
			Hash:        bytes.Clone(block.Hash),
			BlockNumber: block.Number,
			Type:        block.Type,
			Cbor:        bytes.Clone(block.Cbor),
		}
		if i > 0 {
			raw[i].PrevHash = bytes.Clone(raw[i-1].Hash)
		}
	}
	require.NoError(
		t, cm.PrimaryChain().AddRawBlocks(context.Background(), raw),
	)
	f.tip = ochainsync.Tip{
		Point:       ocommon.NewPoint(raw[1].Slot, raw[1].Hash),
		BlockNumber: raw[1].BlockNumber,
	}
	f.target = ochainsync.Tip{
		Point:       ocommon.NewPoint(raw[0].Slot, raw[0].Hash),
		BlockNumber: raw[0].BlockNumber,
	}
	require.NoError(t, seedBlockNonce(t, db,
		f.target.Point.Hash, f.target.Point.Slot,
		bytes.Repeat([]byte{0x11}, 32), true, nil,
	))
	require.NoError(t, seedBlockNonce(t, db,
		f.tip.Point.Hash, f.tip.Point.Slot,
		bytes.Repeat([]byte{0x22}, 32), false, nil,
	))
	require.NoError(t, db.SetTip(f.tip, nil))
	f.ls, f.txCh, f.closeSet = f.openLedger(t, cm)
	f.ls.currentTip = f.tip
	f.ls.currentTipBlockNonce = bytes.Repeat([]byte{0x22}, 32)
	fxJournalChain(t, f.ls)
	return f
}

func (f *rollbackCrashFixture) openLedger(
	t *testing.T,
	cm *chain.ChainManager,
) (*LedgerState, <-chan event.Event, func() error) {
	t.Helper()
	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	subID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	require.NotZero(t, subID)
	var unsubscribeOnce sync.Once
	f.unsubscribe = func() {
		unsubscribeOnce.Do(func() {
			bus.Unsubscribe(TransactionEventType, subID)
		})
	}
	t.Cleanup(f.unsubscribe)
	set, closeSet := OpenLiveSetAtForTest(t, f.db, f.dataDir)
	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          f.db,
		UtxoSet:           set,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	// Registered after the set and the database were opened, so it runs
	// before their own cleanup closes them.
	t.Cleanup(func() { _ = ls.Close() })
	return ls, txCh, closeSet
}

// restart closes the ledger and the database and opens both again over the
// same data directory, as a process that stopped and started would.
func (f *rollbackCrashFixture) restart(t *testing.T) {
	t.Helper()
	// A committed rollback queues work that reads the set; Close waits for
	// it, so the set is not closed under it.
	require.NoError(t, f.ls.Close())
	require.NoError(t, f.closeSet())
	require.NoError(t, dbtest.CloseDatabase(f.db))
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: f.dataDir})
	require.NoError(t, err)
	f.db = db
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))
	f.ls, f.txCh, f.closeSet = f.openLedger(t, cm)
	durable, err := db.GetTip(nil)
	require.NoError(t, err)
	f.ls.currentTip = durable
}

func (f *rollbackCrashFixture) requireUndo(t *testing.T, msg string) {
	t.Helper()
	evt := testutil.RequireReceive(t, f.txCh, 2*time.Second, msg)
	undo, ok := evt.Data.(TransactionEvent)
	require.True(t, ok)
	require.True(t, undo.Rollback)
	require.Equal(t, f.blocks[1].Slot, undo.Point.Slot)
	require.Equal(t, f.blocks[1].Hash, undo.Point.Hash)
}

// A stop after the chain truncation and before the ledger's own rollback
// leaves the intent pending and the rolled-back block off the chain. Its body
// is still in the block store, so the restart replays the undo, rolls the
// ledger back, clears the intent and only then deletes the body.
func TestRollbackResumesAfterAStopBetweenChainTruncationAndLedgerUndo(
	t *testing.T,
) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	injected := errors.New("stopped before the ledger rollback")
	f.ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point, uint64, ledgerdb.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	err := f.ls.rollbackChainAndStateDeferred(
		context.Background(), f.target.Point, nil,
	)
	require.ErrorIs(t, err, ErrChainTruncatedLedgerRollbackFailed)
	f.requireUndo(t, "live undo before the truncation")

	require.Equal(t, f.target.Point, f.ls.chain.Tip().Point)
	_, err = database.BlockByPoint(
		context.Background(), f.db, f.tip.Point,
	)
	require.ErrorIs(t, err, models.ErrBlockNotFound,
		"the rolled-back block is still a chain block")
	require.Equal(t, 1, rolledBackBlockBodies(t, f.db),
		"the chain truncation did not keep the rolled-back body")
	durable, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.tip, durable, "the ledger rollback was not interrupted")

	f.restart(t)
	require.NoError(t, f.ls.recoverRollbackIntent(context.Background()))
	f.requireUndo(t, "undo replayed from the kept body")

	_, _, pending, err := loadRollbackIntent(f.db)
	require.NoError(t, err)
	require.False(t, pending)
	require.Zero(t, rolledBackBlockBodies(t, f.db),
		"the body outlived the intent that named it")
	durable, err = f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.target, durable)
}

// A stop after the ledger's rollback committed and before the intent cleared
// replays the undo once more -- delivery is at least once -- and finishes
// without rolling anything back again.
func TestRollbackResumesAfterAStopBetweenLedgerUndoAndIntentClear(
	t *testing.T,
) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	require.NoError(t, f.ls.rollbackChainAndStateDeferred(
		context.Background(), f.target.Point, nil,
	))
	f.requireUndo(t, "live undo")
	durable, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.target, durable)
	_, _, pending, err := loadRollbackIntent(f.db)
	require.NoError(t, err)
	require.False(t, pending)
	require.Zero(t, rolledBackBlockBodies(t, f.db))

	// Put back what the stop would have left: the intent and the body it
	// names, with the chain and the ledger already at the target.
	require.NoError(t, persistRollbackIntent(
		f.db, f.target.Point, []models.Block{f.blocks[1]}, nil,
	))
	require.Equal(t, 1, rolledBackBlockBodies(t, f.db))

	f.restart(t)
	require.NoError(t, f.ls.recoverRollbackIntent(context.Background()))
	f.requireUndo(t, "undo replayed after the ledger rollback committed")

	_, _, pending, err = loadRollbackIntent(f.db)
	require.NoError(t, err)
	require.False(t, pending)
	require.Zero(t, rolledBackBlockBodies(t, f.db))
	durable, err = f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.target, durable)
	require.Equal(t, f.target.Point, f.ls.chain.Tip().Point)
}

// A stop after the intent cleared and before its bodies were deleted leaves
// bodies nothing names. The restart deletes them and replays nothing.
func TestRollbackResumesAfterAStopBetweenIntentClearAndBodyDeletion(
	t *testing.T,
) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	require.NoError(t, f.ls.rollbackChainAndStateDeferred(
		context.Background(), f.target.Point, nil,
	))
	f.requireUndo(t, "live undo")
	require.NoError(t, f.db.RetainRolledBackBlock(
		f.blocks[1].Slot, f.blocks[1].Hash, f.blocks[1].Cbor,
	))
	require.Equal(t, 1, rolledBackBlockBodies(t, f.db))

	f.restart(t)
	require.NoError(t, f.ls.recoverRollbackIntent(context.Background()))
	require.Zero(t, rolledBackBlockBodies(t, f.db),
		"a body no intent names survived the restart")
	testutil.RequireNoReceive(t, f.txCh, 100*time.Millisecond,
		"a cleared intent was replayed")
	durable, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.target, durable)
}

// A pending intent holds its bodies against a release: only a cleared intent
// lets them go.
func TestPendingRollbackIntentHoldsItsBodies(t *testing.T) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	removed := models.Block{
		ID: 9, Slot: f.tip.Point.Slot + 100, Number: 9, Type: 1,
		Hash: bytes.Repeat([]byte{9}, 32), Cbor: []byte{0x80},
	}
	require.NoError(t, persistRollbackIntent(
		f.db, f.target.Point, []models.Block{removed}, nil,
	))
	f.ls.releaseRolledBackBlocks(context.Background())
	require.Equal(t, 1, rolledBackBlockBodies(t, f.db))

	require.NoError(t, clearRollbackIntent(f.db))
	f.ls.releaseRolledBackBlocks(context.Background())
	require.Zero(t, rolledBackBlockBodies(t, f.db))
}

func TestRollbackIntentAcceptsWriterMaximumBlockCount(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	blocks := make([]models.Block, maxRollbackIntentBlocks)
	for i := range blocks {
		blocks[i] = models.Block{
			Hash: []byte{byte(i), byte(i >> 8)},
			Cbor: []byte{0x80},
			Slot: uint64(i + 2),
		}
	}
	point := ocommon.Point{Slot: 1, Hash: []byte{1}}
	require.NoError(t, persistRollbackIntent(db, point, blocks, nil))

	gotPoint, gotBlocks, pending, err := loadRollbackIntent(db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, point, gotPoint)
	require.Len(t, gotBlocks, maxRollbackIntentBlocks)
}

// setTip returns the tip of the LedgerDB set the ledger was opened with.
func (f *rollbackCrashFixture) setTip(t *testing.T) ocommon.Point {
	t.Helper()
	set, ok := f.ls.config.UtxoSet.(interface {
		Tip() (ocommon.Point, bool, error)
	})
	require.True(t, ok)
	tip, found, err := set.Tip()
	require.NoError(t, err)
	require.True(t, found)
	return tip
}

// A rollback lowers the stored tip before its revert commits. A stop between
// the two leaves the tip at the target while the LedgerDB set, which is
// rolled back only after that commit, still stands at the abandoned block.
// Opening the set rolls it back to the stored tip from its journal, and the
// restart then replays the undo and finishes at the target.
func TestRollbackResumesAfterAStopBetweenTipRetreatAndRevertCommit(
	t *testing.T,
) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	injected := errors.New("stopped before the revert committed")
	f.ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point, uint64, ledgerdb.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	err := f.ls.rollbackChainAndStateDeferred(
		context.Background(), f.target.Point, nil,
	)
	require.ErrorIs(t, err, ErrChainTruncatedLedgerRollbackFailed)
	f.requireUndo(t, "live undo before the truncation")
	require.Equal(t, f.tip.Point, f.setTip(t),
		"the set was rolled back before the revert committed")
	// The commit of the revert writes the retreating tip first.
	require.NoError(t, f.db.SetTip(f.target, nil))

	f.restart(t)
	require.Equal(t, f.target, f.ls.currentTip)
	require.Equal(t, f.target.Point, f.setTip(t),
		"the set opened ahead of the stored tip")
	require.NoError(t, f.ls.recoverRollbackIntent(context.Background()))
	f.requireUndo(t, "undo replayed from the kept body")

	_, _, pending, err := loadRollbackIntent(f.db)
	require.NoError(t, err)
	require.False(t, pending)
	require.Zero(t, rolledBackBlockBodies(t, f.db))
	durable, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.target, durable)
	require.Equal(t, f.target.Point, f.setTip(t))
	require.Equal(t, f.target.Point, f.ls.chain.Tip().Point)
}

// A mark that names another point than the stored tip belongs to a revert
// the ledger has moved past. It is removed and nothing is reverted.
func TestStartDropsATipRevertMarkTheLedgerMovedPast(t *testing.T) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	require.NoError(t, f.db.SetChainState(
		database.TipRevertPendingKey,
		strconv.FormatUint(f.target.Point.Slot, 10)+":"+
			strconv.FormatUint(f.target.BlockNumber, 10)+":"+
			hex.EncodeToString(f.target.Point.Hash),
		nil,
	))
	require.NoError(
		t, f.ls.repeatInterruptedTipRevert(context.Background()),
	)
	_, found, err := f.db.TipRevertPending()
	require.NoError(t, err)
	require.False(t, found)
	durable, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.tip, durable)
	require.Equal(t, f.tip.Point, f.setTip(t))
}

// A record that cannot be read must not stop every later start: recovery
// removes it, replays nothing, and releases the bodies it can no longer name.
func TestRecoverRollbackIntentDiscardsAnUnreadableRecord(t *testing.T) {
	t.Parallel()
	f := newRollbackCrashFixture(t)
	require.NoError(t, f.db.RetainRolledBackBlock(
		f.blocks[1].Slot+1, bytes.Repeat([]byte{7}, 32), []byte{0x80},
	))
	require.NoError(t, f.db.SetChainState(
		database.RollbackIntentKey, `{"format_version":1}`, nil,
	))
	_, _, _, err := loadRollbackIntent(f.db)
	require.ErrorIs(t, err, errInvalidRollbackIntent)

	require.NoError(t, f.ls.recoverRollbackIntent(context.Background()))
	record, err := f.db.GetChainState(database.RollbackIntentKey, nil)
	require.NoError(t, err)
	require.Empty(t, record)
	testutil.RequireNoReceive(t, f.txCh, 100*time.Millisecond,
		"an unreadable intent was replayed")
	durable, err := f.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.tip, durable)
	require.Zero(t, rolledBackBlockBodies(t, f.db))
}
