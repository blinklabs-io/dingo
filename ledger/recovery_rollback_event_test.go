// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type recoveryRollbackEventFixture struct {
	ls             *LedgerState
	bus            *event.EventBus
	events         <-chan event.Event
	blocks         []models.Block
	ancestorTip    ochainsync.Tip
	currentTip     ochainsync.Tip
	unappliedPoint ocommon.Point
}

func newRecoveryRollbackEventFixture(
	t *testing.T,
) *recoveryRollbackEventFixture {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 10}))

	blocks := loadTestBlocksWithTxs(t, 3)
	raw := make([]chain.RawBlock, 0, len(blocks)+1)
	for i := range blocks {
		blocks[i].Number = uint64(i + 1)
		if i > 0 {
			blocks[i].PrevHash = slices.Clone(blocks[i-1].Hash)
		}
		raw = append(raw, chain.RawBlock{
			Slot:        blocks[i].Slot,
			Hash:        blocks[i].Hash,
			BlockNumber: blocks[i].Number,
			Type:        blocks[i].Type,
			PrevHash:    blocks[i].PrevHash,
			Cbor:        blocks[i].Cbor,
		})
	}
	unapplied := chain.RawBlock{
		Slot:        blocks[len(blocks)-1].Slot + 1,
		Hash:        testHashBytes("recovery-rollback-unapplied"),
		BlockNumber: uint64(len(blocks) + 1),
		Type:        1,
		PrevHash:    slices.Clone(blocks[len(blocks)-1].Hash),
		Cbor:        []byte{0x80},
	}
	raw = append(raw, unapplied)
	require.NoError(t, cm.PrimaryChain().AddRawBlocks(context.Background(), raw))

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	subID, events := bus.SubscribeWithBuffer(TransactionEventType, 4096)
	require.NotZero(t, subID)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, subID) })

	ls, err := newReplayRecoveryLedgerState(t, LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfgWithK(t, 10),
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.config.EventBus = bus
	ls.metrics.init(prometheus.NewRegistry())
	require.NoError(t, cm.SetLedger(ls))

	for i := range blocks {
		require.NoError(t, db.SetBlockNonce(
			blocks[i].Hash,
			blocks[i].Slot,
			testHashBytes(fmt.Sprintf("recovery-nonce-%d", i)),
			i == 0,
			nil,
		))
	}
	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(blocks[0].Slot, blocks[0].Hash),
		BlockNumber: blocks[0].Number,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(blocks[2].Slot, blocks[2].Hash),
		BlockNumber: blocks[2].Number,
	}
	require.NoError(t, db.SetTip(currentTip, nil))
	ls.currentTip = currentTip
	ls.currentTipBlockNonce = testHashBytes("recovery-nonce-2")
	ls.publishSnapshotsLocked()

	return &recoveryRollbackEventFixture{
		ls:             ls,
		bus:            bus,
		events:         events,
		blocks:         blocks,
		ancestorTip:    ancestorTip,
		currentTip:     currentTip,
		unappliedPoint: ocommon.NewPoint(unapplied.Slot, unapplied.Hash),
	}
}

func requireRecoveryRollbackEvents(
	t *testing.T,
	f *recoveryRollbackEventFixture,
) {
	t.Helper()

	want := 0
	for _, block := range f.blocks[1:] {
		decoded, err := block.Decode()
		require.NoError(t, err)
		want += len(decoded.Transactions())
	}
	require.Positive(t, want)
	require.True(t, f.bus.FlushOrderedContext(t.Context(), TransactionEventType))

	got := make([]TransactionEvent, 0, want)
	for range want {
		evt := testutil.RequireReceive(
			t,
			f.events,
			testutil.AsyncWait,
			"recovery rollback transaction event",
		)
		txEvent, ok := evt.Data.(TransactionEvent)
		require.True(t, ok, "unexpected event payload %T", evt.Data)
		require.True(t, txEvent.Rollback)
		require.NotEqual(t, f.unappliedPoint, txEvent.Point)
		got = append(got, txEvent)
	}

	pos := 0
	for _, block := range slices.Backward(f.blocks[1:]) {
		decoded, err := block.Decode()
		require.NoError(t, err)
		for txIndex := range slices.Backward(decoded.Transactions()) {
			require.Equal(t, block.Slot, got[pos].Point.Slot)
			require.Equal(t, uint32(txIndex), got[pos].TxIndex) //nolint:gosec
			pos++
		}
	}
	testutil.RequireNoReceive(
		t,
		f.events,
		250*time.Millisecond,
		"recovery must emit exactly one undo per applied transaction",
	)
}

func replaceRecoveryPrimaryChain(
	t *testing.T,
	f *recoveryRollbackEventFixture,
	name string,
) {
	t.Helper()

	require.NoError(t, f.ls.chain.Rollback(t.Context(), f.ancestorTip.Point))
	fork := chain.RawBlock{
		Slot:        f.ancestorTip.Point.Slot + 1,
		Hash:        testHashBytes(name),
		BlockNumber: f.ancestorTip.BlockNumber + 1,
		Type:        1,
		PrevHash:    slices.Clone(f.ancestorTip.Point.Hash),
		Cbor:        []byte{0x80},
	}
	require.NoError(t, f.ls.chain.AddRawBlocks(t.Context(), []chain.RawBlock{fork}))
	f.unappliedPoint = ocommon.NewPoint(fork.Slot, fork.Hash)
}

func TestReplayRecoveryPublishesAppliedTransactionUndoEvents(t *testing.T) {
	t.Parallel()

	f := newRecoveryRollbackEventFixture(t)
	producerTxHash := testHashBytes("recovery-rollback-producer")
	seedReplayRecoveryTransaction(
		t,
		f.ls.db,
		producerTxHash,
		f.blocks[1].Hash,
		f.blocks[1].Slot,
	)

	recovered, err := f.ls.tryRecoverFromTxValidationError(&txValidationError{
		BlockPoint: f.currentTip.Point,
		TxHash:     testHashBytes("recovery-rollback-failing"),
		Inputs: []lcommon.TransactionInput{
			&replayRecoveryInput{txId: producerTxHash, index: 0},
		},
		Cause: errors.New("bad input"),
	})
	require.NoError(t, err)
	require.True(t, recovered)
	require.Equal(t, f.ancestorTip, f.ls.currentTip)
	require.Equal(t, f.unappliedPoint, f.ls.chain.Tip().Point)
	requireRecoveryRollbackEvents(t, f)
}

func TestReplayRecoveryWithoutRollbackPublishesNoUndoEvents(t *testing.T) {
	t.Parallel()

	f := newRecoveryRollbackEventFixture(t)
	recovered, err := f.ls.tryRecoverFromTxValidationError(&txValidationError{
		BlockPoint: f.currentTip.Point,
		TxHash:     testHashBytes("recovery-no-rollback"),
		Cause:      errors.New("bad input"),
	})
	require.NoError(t, err)
	require.False(t, recovered)
	require.Equal(t, f.currentTip, f.ls.currentTip)
	testutil.RequireNoReceive(
		t,
		f.events,
		250*time.Millisecond,
		"declined replay recovery must not publish undo events",
	)
}

func TestReplayRecoveryPublishesUndoWhenPrimaryChainNeedsNoRewind(
	t *testing.T,
) {
	t.Parallel()

	f := newRecoveryRollbackEventFixture(t)
	f.ls.currentEra.Id = 1
	f.ls.config.CardanoNodeConfig.ShelleyGenesis().SecurityParam = 1
	producerTxHash := testHashBytes("recovery-held-primary-producer")
	seedReplayRecoveryTransaction(
		t,
		f.ls.db,
		producerTxHash,
		f.blocks[1].Hash,
		f.blocks[1].Slot,
	)
	failure := &txValidationError{
		BlockPoint: f.currentTip.Point,
		TxHash:     testHashBytes("recovery-held-primary-failing"),
		Inputs: []lcommon.TransactionInput{
			&replayRecoveryInput{txId: producerTxHash, index: 0},
			&replayRecoveryInput{
				txId:  testHashBytes("recovery-held-primary-unresolved"),
				index: 0,
			},
		},
		Cause: errors.New("bad input"),
	}
	candidate, err := f.ls.findReplayRecoveryCandidate(failure)
	require.NoError(t, err)
	require.NotNil(t, candidate)
	require.True(t, candidate.ProducerUnresolved)
	require.Equal(t, f.ancestorTip.Point, candidate.RollbackPoint)
	removed := make([]models.Block, 0, len(f.blocks)-1)
	for _, block := range f.blocks[1:] {
		stored, lookupErr := database.BlockByPoint(
			t.Context(),
			f.ls.db,
			ocommon.NewPoint(block.Slot, block.Hash),
		)
		require.NoError(t, lookupErr)
		removed = append(removed, stored)
	}
	require.NoError(t, f.ls.chain.Rollback(t.Context(), candidate.RollbackPoint))
	for _, block := range removed {
		require.NoError(t, f.ls.db.BlockCreate(block, nil))
	}

	recovered, err := f.ls.tryRecoverFromTxValidationError(failure)
	require.NoError(t, err)
	require.True(t, recovered)
	require.Equal(t, f.ancestorTip, f.ls.currentTip)
	require.Equal(t, f.ancestorTip.Point, f.ls.chain.Tip().Point)
	requireRecoveryRollbackEvents(t, f)
}

func TestPartialCommitRecoveryPublishesAppliedTransactionUndoEvents(
	t *testing.T,
) {
	t.Parallel()

	f := newRecoveryRollbackEventFixture(t)
	replaceRecoveryPrimaryChain(t, f, "partial-commit-recovery-fork")

	metadataErr := errors.New("metadata commit failed")
	err := f.ls.handleDBTxnResult(t.Context(), database.PartialCommitError{
		MetadataErr:     metadataErr,
		CommitTimestamp: 1,
	})
	require.ErrorIs(t, err, metadataErr)
	require.NotErrorIs(t, err, dbtypes.ErrPartialCommit)
	require.Equal(t, f.ancestorTip, f.ls.currentTip)
	requireRecoveryRollbackEvents(t, f)
}

func TestPartialCommitRecoveryTruncationFailurePublishesNoUndoEvents(
	t *testing.T,
) {
	t.Parallel()

	f := newRecoveryRollbackEventFixture(t)
	replaceRecoveryPrimaryChain(t, f, "partial-commit-failure-fork")
	injected := errors.New("injected metadata truncation failure")
	f.ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	metadataErr := errors.New("metadata commit failed")
	err := f.ls.handleDBTxnResult(t.Context(), database.PartialCommitError{
		MetadataErr:     metadataErr,
		CommitTimestamp: 1,
	})
	require.ErrorIs(t, err, injected)
	require.ErrorIs(t, err, metadataErr)
	require.Equal(t, f.currentTip, f.ls.currentTip)
	durableTip, err := f.ls.db.GetTip(nil)
	require.NoError(t, err)
	require.Equal(t, f.currentTip, durableTip)
	require.True(t, f.bus.FlushOrderedContext(t.Context(), TransactionEventType))
	testutil.RequireNoReceive(
		t,
		f.events,
		250*time.Millisecond,
		"failed metadata truncation must not publish undo events",
	)
	intentPoint, intentBlocks, pending, err := loadRollbackIntent(f.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, f.ancestorTip.Point, intentPoint)
	require.Len(t, intentBlocks, len(f.blocks)-1)
}

func TestPartialCommitRecoveryWithoutAppliedRollbackPublishesNoUndoEvents(
	t *testing.T,
) {
	t.Parallel()

	f := newRecoveryRollbackEventFixture(t)
	metadataErr := errors.New("metadata commit failed")
	err := f.ls.handleDBTxnResult(t.Context(), database.PartialCommitError{
		MetadataErr:     metadataErr,
		CommitTimestamp: 1,
	})
	require.ErrorIs(t, err, metadataErr)
	require.NotErrorIs(t, err, dbtypes.ErrPartialCommit)
	require.Equal(t, f.currentTip, f.ls.currentTip)
	testutil.RequireNoReceive(
		t,
		f.events,
		250*time.Millisecond,
		"partial-commit cleanup without metadata rollback must not publish undo events",
	)
}
