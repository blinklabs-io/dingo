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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// startupReplayFixture stores blockCount blocks on the primary chain and
// returns a LedgerState whose applied tip is the block numbered tipNumber
// (0 for origin) together with a subscriber channel for ledger.block events.
func startupReplayFixture(
	t *testing.T,
	blockCount int,
	tipNumber int,
) (*LedgerState, []chain.RawBlock, <-chan event.Event) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(testSecurityParamLedger{securityParam: 10}))

	raw := make([]chain.RawBlock, 0, blockCount)
	var prev []byte
	for i := 1; i <= blockCount; i++ {
		h := testHashBytes(fmt.Sprintf("startup-replay-block-%d", i))
		raw = append(raw, chain.RawBlock{
			Slot:        uint64(i * 10),
			Hash:        h,
			BlockNumber: uint64(i),
			Type:        1,
			PrevHash:    prev,
			Cbor:        []byte{0x80},
		})
		prev = h
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks(context.Background(), raw),
	)

	bus := newTestEventBus(t, nil)
	t.Cleanup(bus.Stop)
	subID, blockCh := bus.SubscribeWithBuffer(BlockEventType, 64)
	require.NotEqual(t, event.EventSubscriberId(0), subID)
	t.Cleanup(func() { bus.Unsubscribe(BlockEventType, subID) })

	ls, err := NewLedgerState(LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: newTestShelleyGenesisCfg(t),
		EventBus:          bus,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	ls.currentTip = ochainsync.Tip{}
	if tipNumber > 0 {
		b := raw[tipNumber-1]
		ls.currentTip = ochainsync.Tip{
			Point:       ocommon.NewPoint(b.Slot, b.Hash),
			BlockNumber: b.BlockNumber,
		}
	}
	return ls, raw, blockCh
}

func requireApplyEvent(
	t *testing.T,
	ch <-chan event.Event,
	want chain.RawBlock,
) {
	t.Helper()
	evt := testutil.RequireReceive(
		t, ch, testutil.AsyncWait,
		fmt.Sprintf("block event for block %d", want.BlockNumber),
	)
	be, ok := evt.Data.(BlockEvent)
	require.True(t, ok)
	require.Equal(t, BlockActionApply, be.Action)
	require.Equal(t, want.Hash, be.Block.Hash)
	require.Equal(t, want.Slot, be.Point.Slot)
}

func TestPublishStartupReplayBlockEventsSkipsAppliedBlocks(t *testing.T) {
	t.Parallel()
	ls, raw, blockCh := startupReplayFixture(t, 5, 2)

	require.NoError(
		t,
		ls.publishStartupReplayBlockEvents(context.Background()),
	)
	for _, b := range raw[2:] {
		requireApplyEvent(t, blockCh, b)
	}
	testutil.RequireNoReceive(
		t, blockCh, 100*time.Millisecond, "applied blocks are not replayed",
	)
}

func TestStartupReplayBlockEventsPrecedeLiveBlockEvent(t *testing.T) {
	t.Parallel()
	ls, raw, blockCh := startupReplayFixture(t, 3, 0)

	require.NoError(
		t,
		ls.publishStartupReplayBlockEvents(context.Background()),
	)
	live := models.Block{
		Slot:   raw[2].Slot + 10,
		Hash:   testHashBytes("startup-replay-live"),
		Number: raw[2].BlockNumber + 1,
	}
	ls.handleEventChainUpdate(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{Block: live},
	))

	for _, b := range raw {
		requireApplyEvent(t, blockCh, b)
	}
	requireApplyEvent(t, blockCh, chain.RawBlock{
		Slot: live.Slot, Hash: live.Hash,
	})
}

// TestLedgerStateStartPublishesReplayBlockEvents drives Start itself: blocks
// stored before the ledger starts, with its tip at the origin, surface as
// ledger.block Apply events in chain order.
func TestLedgerStateStartPublishesReplayBlockEvents(t *testing.T) {
	t.Parallel()
	ls, raw, blockCh := startupReplayFixture(t, 3, 0)
	ls.config.ManualBlockProcessing = true
	ls.config.PromRegistry = prometheus.NewRegistry()
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	ls.config.CardanoNodeConfig = nodeCfg
	t.Cleanup(ls.publishCancel)

	require.NoError(t, ls.Start(t.Context()))
	t.Cleanup(func() { _ = ls.Close() })

	for _, b := range raw {
		requireApplyEvent(t, blockCh, b)
	}
}

// TestStartupReplayWaitsForReconciliationUndoEvents holds the chain-update
// handler as a slow subscriber would while a reconciliation rollback is
// still queued behind it. The replay must not publish an Apply until the
// handler has published that rollback's Undo.
func TestStartupReplayWaitsForReconciliationUndoEvents(t *testing.T) {
	t.Parallel()
	ls, raw, blockCh := startupReplayFixture(t, 3, 0)

	gate := make(chan struct{})
	releaseGate := sync.OnceFunc(func() { close(gate) })
	// Registered after the fixture's bus cleanup, so it runs first and a
	// failing assertion cannot leave the handler parked at Stop.
	t.Cleanup(releaseGate)
	ls.chainUpdateSubID = ls.config.EventBus.SubscribeFunc(
		chain.ChainUpdateEventType,
		func(evt event.Event) {
			<-gate
			ls.handleEventChainUpdate(evt)
		},
	)
	undone := models.Block{
		Slot:   raw[0].Slot,
		Hash:   testHashBytes("startup-replay-abandoned-fork"),
		Number: raw[0].BlockNumber,
	}
	ls.config.EventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(
			chain.ChainUpdateEventType,
			chain.ChainRollbackEvent{RolledBackBlocks: []models.Block{undone}},
		),
	)

	replayDone := make(chan error, 1)
	go func() {
		replayDone <- ls.publishStartupReplayBlockEvents(context.Background())
	}()
	testutil.RequireNoReceive(
		t, blockCh, 200*time.Millisecond,
		"replay must wait for the queued rollback's Undo",
	)

	releaseGate()
	evt := testutil.RequireReceive(
		t, blockCh, testutil.AsyncWait, "undo event",
	)
	be, ok := evt.Data.(BlockEvent)
	require.True(t, ok)
	require.Equal(t, BlockActionUndo, be.Action)
	require.Equal(t, undone.Hash, be.Block.Hash)
	for _, b := range raw {
		requireApplyEvent(t, blockCh, b)
	}
	require.NoError(t, <-replayDone)
}
