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
	"io"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestApplyByronBlockRecordsAppliedPoint applies a real Byron block through
// the production batch-apply loop. Byron has no evolving nonce, so the
// block_nonce row it leaves is the only durable record that the block was
// applied, and the reconciler's applied-point lookups depend on it.
func TestApplyByronBlockRecordsAppliedPoint(t *testing.T) {
	t.Parallel()

	ls, lastByron, _ := newByronShelleyBoundaryLedger(t)
	byronPoint := ocommon.NewPoint(
		lastByron.SlotNumber(),
		lastByron.Hash().Bytes(),
	)
	// Restart from origin so applying the block is a forward step that needs
	// no earlier Byron history.
	parentTip := ochainsync.Tip{}
	require.NoError(t, ls.db.DeleteBlockNoncesAfterPoint(parentTip.Point, nil))
	require.NoError(t, ls.db.SetTip(parentTip, nil))
	ls.currentTip = parentTip
	ls.currentTipBlockNonce = nil
	ls.validationEnabled = false
	ls.publishSnapshotsLocked()

	results := make(chan readChainResult, 1)
	results <- readChainResult{blocks: []gledger.Block{lastByron}}
	close(results)
	require.NoError(t, ls.ledgerProcessBlocksFromSource(
		context.Background(),
		results,
	))
	require.Equal(t, byronPoint, ls.currentTip.Point)

	rows, err := ls.db.GetBlockNoncesInSlotRange(
		byronPoint.Slot,
		byronPoint.Slot+1,
		nil,
	)
	require.NoError(t, err)
	require.Len(
		t,
		rows,
		1,
		"an applied Byron block must leave an applied-point row",
	)
	require.Equal(t, byronPoint.Hash, rows[0].Hash)
	require.Empty(t, rows[0].Nonce, "Byron has no evolving nonce")

	floor, ok, err := ls.durableAppliedFloor()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, byronPoint, floor)
}

// addNilNonceAppliedBlock extends the fixture's primary chain with one block
// whose applied-point row carries no evolving nonce, the shape Byron blocks
// leave, and returns its point.
func addNilNonceAppliedBlock(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
	name string,
) ocommon.Point {
	t.Helper()
	tip := fixture.currentTip
	point := ocommon.NewPoint(tip.Point.Slot+5, testHashBytes(name))
	require.NoError(
		t,
		fixture.ls.chain.AddRawBlocks(t.Context(), []chain.RawBlock{{
			Slot:        point.Slot,
			Hash:        point.Hash,
			BlockNumber: tip.BlockNumber + 1,
			Type:        1,
			PrevHash:    tip.Point.Hash,
			Cbor:        []byte{0x80},
		}}),
	)
	require.NoError(t, fixture.ls.db.SetBlockNonce(
		point.Hash, point.Slot, nil, false, nil,
	))
	return point
}

// TestLatestLedgerPrimaryChainAncestorFindsNilNonceAppliedPoint requires the
// common-ancestor search to return an applied block whose row has no nonce
// instead of skipping to an older block that has one.
func TestLatestLedgerPrimaryChainAncestorFindsNilNonceAppliedPoint(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	byronLike := addNilNonceAppliedBlock(t, fixture, "ancestor-nil-nonce")

	diverged := ocommon.NewPoint(
		byronLike.Slot+10,
		testHashBytes("ancestor-diverged"),
	)
	ancestor, ok, err := fixture.ls.latestLedgerPrimaryChainAncestor(
		t.Context(),
		diverged,
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, byronLike, ancestor)
}

// TestDurableAppliedFloorIncludesNilNonceAppliedPoint requires the recovery
// floor to be the highest applied block even when its row has no nonce.
func TestDurableAppliedFloorIncludesNilNonceAppliedPoint(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	byronLike := addNilNonceAppliedBlock(t, fixture, "floor-nil-nonce")

	floor, ok, err := fixture.ls.durableAppliedFloor()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, byronLike, floor)
}

// TestReconcileDivergenceUndoesAppliedByronBlock diverges the primary chain
// from a ledger whose applied tip is a real mainnet Byron block carrying two
// transactions (slot 4471207). The common ancestor is also a Byron-shaped
// point with no nonce. Reconciliation must find that ancestor and deliver a
// rollback event for each transaction, newest first.
func TestReconcileDivergenceUndoesAppliedByronBlock(t *testing.T) {
	t.Parallel()

	byronBlock := loadBoundaryBlock(t, "mainnet-byron-4471207.cbor", 1)
	byronTxs := byronBlock.Transactions()
	require.Len(t, byronTxs, 2)

	db := newTestDB(t)
	cm, err := chain.NewManager(t.Context(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 2}))

	ancestorTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			byronBlock.SlotNumber()-1,
			byronBlock.PrevHash().Bytes(),
		),
		BlockNumber: byronBlock.BlockNumber() - 1,
	}
	byronTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			byronBlock.SlotNumber(),
			byronBlock.Hash().Bytes(),
		),
		BlockNumber: byronBlock.BlockNumber(),
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(t.Context(), []chain.RawBlock{
			{
				Slot:        ancestorTip.Point.Slot,
				Hash:        ancestorTip.Point.Hash,
				BlockNumber: ancestorTip.BlockNumber,
				Type:        1,
				Cbor:        []byte{0x80},
			},
			{
				Slot:        byronTip.Point.Slot,
				Hash:        byronTip.Point.Hash,
				BlockNumber: byronTip.BlockNumber,
				Type:        1,
				PrevHash:    ancestorTip.Point.Hash,
				Cbor:        byronBlock.Cbor(),
			},
		}),
	)

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	// The rows block application writes for Byron blocks: no nonce.
	for _, tip := range []ochainsync.Tip{ancestorTip, byronTip} {
		require.NoError(t, db.SetBlockNonce(
			tip.Point.Hash, tip.Point.Slot, nil, false, nil,
		))
	}
	require.NoError(t, db.SetTip(byronTip, nil))
	ls.currentTip = byronTip
	ls.currentTipBlockNonce = nil
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })
	errSubID, errCh := bus.SubscribeWithBuffer(LedgerErrorEventType, 64)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, errSubID) })

	require.NoError(t, ls.chain.Rollback(t.Context(), ancestorTip.Point))
	require.NoError(t, ls.chain.AddRawBlocks(t.Context(), []chain.RawBlock{{
		Slot:        byronTip.Point.Slot + 5,
		Hash:        testHashBytes("byron-reconcile-fork"),
		BlockNumber: byronTip.BlockNumber,
		Type:        1,
		PrevHash:    ancestorTip.Point.Hash,
		Cbor:        []byte{0x80},
	}}))

	require.NoError(t, ls.reconcilePrimaryChainTipWithLedgerTip(t.Context()))

	for i, tx := range slices.Backward(byronTxs) {
		evt := testutil.RequireReceive(
			t, txCh, testutil.AsyncWait,
			"rollback event for an applied Byron transaction",
		)
		te, ok := evt.Data.(TransactionEvent)
		require.True(t, ok, "unexpected payload %T", evt.Data)
		require.True(t, te.Rollback)
		require.Equal(t, byronTip.Point, te.Point)
		require.Equal(t, uint32(i), te.TxIndex) //nolint:gosec
		require.Equal(t, tx.Hash(), te.Transaction.Hash())
	}
	testutil.RequireNoReceive(
		t, txCh, 250*time.Millisecond,
		"expected exactly one rollback event per Byron transaction",
	)
	testutil.RequireNoReceive(
		t, errCh, 50*time.Millisecond,
		"the applied Byron block must decode for its undo events",
	)
	require.Equal(t, ancestorTip, ls.currentTip)
}
