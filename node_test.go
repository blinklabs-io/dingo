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

package dingo

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNodeTestConnId(id uint) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 6000,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: int(id),
		},
	}
}

type nodeTestSecurityParamLedger struct {
	securityParam int
}

func (m nodeTestSecurityParamLedger) SecurityParam() int {
	return m.securityParam
}

func nodeTestHashBytes(seed string) []byte {
	sum := sha256.Sum256([]byte(seed))
	return append([]byte(nil), sum[:]...)
}

func newNodeTestCardanoNodeCfg(t testing.TB) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	return cfg
}

func newNodeTestDivergedLedger(
	t *testing.T,
) (*ledger.LedgerState, ochainsync.Tip, ochainsync.Tip, ochainsync.Tip) {
	t.Helper()
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(t, cm.SetLedger(nodeTestSecurityParamLedger{securityParam: 432}))

	ancestorHash := nodeTestHashBytes("node-recycler-ancestor")
	currentHash := nodeTestHashBytes("node-recycler-current")
	ancestorBlock := chain.RawBlock{
		Slot:        10,
		Hash:        ancestorHash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	currentBlock := chain.RawBlock{
		Slot:        20,
		Hash:        currentHash,
		BlockNumber: 2,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(t, cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
		ancestorBlock,
		currentBlock,
	}))

	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash),
		BlockNumber: ancestorBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	require.NoError(
		t,
		db.SetBlockNonce(
			ancestorTip.Point.Hash,
			ancestorTip.Point.Slot,
			[]byte("nonce-ancestor"),
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			[]byte("nonce-current"),
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ledgerState, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:              db,
		ChainManager:          cm,
		CardanoNodeConfig:     newNodeTestCardanoNodeCfg(t),
		Logger:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		ManualBlockProcessing: true,
		DatabaseWorkerPoolConfig: ledger.DatabaseWorkerPoolConfig{
			WorkerPoolSize: 1,
			TaskQueueSize:  1,
			Disabled:       true,
		},
	})
	require.NoError(t, err)
	require.NoError(t, ledgerState.Start(context.Background()))
	t.Cleanup(func() {
		if ledgerState.Scheduler != nil {
			ledgerState.Scheduler.Stop()
		}
		require.NoError(t, ledgerState.Close())
	})

	forkBlock := chain.RawBlock{
		Slot:        50,
		Hash:        nodeTestHashBytes("node-recycler-fork"),
		BlockNumber: currentBlock.BlockNumber + 1,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(t, ledgerState.Chain().Rollback(ancestorTip.Point))
	require.NoError(t, ledgerState.Chain().AddRawBlocks([]chain.RawBlock{forkBlock}))
	forkTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(forkBlock.Slot, forkBlock.Hash),
		BlockNumber: forkBlock.BlockNumber,
	}
	require.Equal(t, currentTip, ledgerState.Tip())
	require.Equal(t, forkTip, ledgerState.PrimaryChainTip())
	return ledgerState, ancestorTip, currentTip, forkTip
}

func TestHandleChainSwitchEventUpdatesActiveConnection(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.DefaultConfig(),
	)
	connA := newNodeTestConnId(3001)
	connB := newNodeTestConnId(3002)
	state.AddClientConnId(connA)
	state.AddClientConnId(connB)
	state.SetClientConnId(connA)
	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	pointB := ocommon.NewPoint(200, []byte("hash-b"))
	tipA := ochainsync.Tip{Point: pointA, BlockNumber: 10}
	tipB := ochainsync.Tip{Point: pointB, BlockNumber: 20}
	state.UpdateClientTip(connA, pointA, tipA)
	state.UpdateClientTip(connB, pointB, tipB)
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
	}

	n.handleChainSwitchEvent(
		event.NewEvent(
			chainselection.ChainSwitchEventType,
			chainselection.ChainSwitchEvent{
				PreviousConnectionId: connA,
				NewConnectionId:      connB,
				NewTip:               tipB,
			},
		),
	)

	active := state.GetClientConnId()
	require.NotNil(t, active)
	clientA := state.GetTrackedClient(connA)
	clientB := state.GetTrackedClient(connB)
	require.NotNil(t, clientA)
	require.NotNil(t, clientB)
	assert.Equal(t, connB, *active)
	assert.Equal(t, pointA, clientA.Cursor)
	assert.Equal(t, pointB, clientB.Cursor)
	assert.Equal(t, uint64(1), clientA.HeadersRecv)
	assert.Equal(t, uint64(1), clientB.HeadersRecv)
}

func TestChainsyncIngressEligibilityCacheDefaultsAndUpdates(t *testing.T) {
	connId := newNodeTestConnId(3003)
	n := &Node{}

	assert.False(t, n.isChainsyncIngressEligible(connId))

	n.handlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: connId,
			Eligible:     false,
		},
	))
	assert.False(t, n.isChainsyncIngressEligible(connId))

	n.handlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: connId,
			Eligible:     true,
		},
	))
	assert.True(t, n.isChainsyncIngressEligible(connId))

	n.deleteChainsyncIngressEligibility(connId)
	assert.False(t, n.isChainsyncIngressEligible(connId))
}

func TestPlateauThreshold(t *testing.T) {
	assert.Equal(t, 4*time.Minute, plateauThreshold(2*time.Minute))
	assert.Equal(t, 6*time.Minute, plateauThreshold(3*time.Minute))
}

func TestShouldRecycleLocalTipPlateau(t *testing.T) {
	now := time.Unix(1_000, 0)
	lastProgressAt := now.Add(-5 * time.Minute)
	threshold := 4 * time.Minute
	cooldown := 2 * time.Minute

	assert.True(t, shouldRecycleLocalTipPlateau(
		now,
		lastProgressAt,
		100,
		120,
		nil,
		cooldown,
		threshold,
	))

	assert.False(t, shouldRecycleLocalTipPlateau(
		now,
		now.Add(-3*time.Minute),
		100,
		120,
		nil,
		cooldown,
		threshold,
	))

	lastRecycledAt := now.Add(-1 * time.Minute)
	assert.False(t, shouldRecycleLocalTipPlateau(
		now,
		lastProgressAt,
		100,
		120,
		&lastRecycledAt,
		cooldown,
		threshold,
	))

	assert.False(t, shouldRecycleLocalTipPlateau(
		now,
		lastProgressAt,
		120,
		120,
		nil,
		cooldown,
		threshold,
	))
}

func TestIsLedgerApplicationBacklog(t *testing.T) {
	// Leios deep catch-up: header/primary chain caught up to the peer while
	// the applied ledger tip lags far behind -> backlog (do not recycle).
	assert.True(t, isLedgerApplicationBacklog(1_488_398, 3_082_751, 3_082_751))
	// Primary chain slightly behind the peer but the residual header gap is
	// tiny next to the apply backlog -> still a backlog.
	assert.True(t, isLedgerApplicationBacklog(1_488_398, 3_082_700, 3_082_751))
	// Genuine chainsync/header stall: nothing fetched beyond the applied tip
	// while the peer is far ahead -> not a backlog (recycle path applies).
	assert.False(t, isLedgerApplicationBacklog(1_488_398, 1_488_398, 3_082_751))
	// Header chain lags: residual header gap dominates the apply backlog ->
	// not a backlog (headers are actually missing).
	assert.False(t, isLedgerApplicationBacklog(1_000, 1_100, 3_000))
	// No primary-chain data (e.g. nil ledger state -> 0) -> not a backlog, so
	// existing recycle behavior is preserved.
	assert.False(t, isLedgerApplicationBacklog(100, 0, 120))
	// Apply backlog exactly equals residual header gap -> treat as backlog.
	assert.True(t, isLedgerApplicationBacklog(100, 150, 200))
}

func TestProcessChainsyncRecyclerTickKeepsStalledRecyclerRunning(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)

	// Two eligible peers so the stall guard does not suppress
	// the recycle (single-peer guard is tested separately).
	// Add connId2 first so connId has a more recent LastActivity;
	// promoteBestClientLocked will then select connId as active
	// after both clients stall, making the recycle deterministic.
	connId := newNodeTestConnId(1)
	connId2 := newNodeTestConnId(2)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   2,
			StallTimeout: time.Millisecond,
		},
	)
	require.True(t, state.AddClientConnId(connId2))
	time.Sleep(time.Millisecond)
	require.True(t, state.AddClientConnId(connId))

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("best")},
		BlockNumber: 60,
	}, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
	}

	time.Sleep(5 * time.Millisecond)

	now := time.Now()
	lastProgressSlot := uint64(100)
	lastProgressAt := now
	recycleAt := map[string]time.Time{
		connId.String(): now.Add(-time.Second),
	}
	lastRecycled := make(map[string]time.Time)

	n.processChainsyncRecyclerTick(
		now,
		100,
		chainsync.Config{
			MaxClients:   2,
			StallTimeout: time.Millisecond,
		},
		recycleAt,
		lastRecycled,
		&lastProgressSlot,
		&lastProgressAt,
		plateauThreshold(2*time.Minute),
		time.Second,
		2*time.Minute,
	)

	select {
	case evt := <-recycleCh:
		recycleEvt, ok := evt.Data.(connmanager.ConnectionRecycleRequestedEvent)
		require.True(t, ok)
		assert.Equal(t, connId, recycleEvt.ConnectionId)
		assert.Equal(
			t,
			"stalled_active_connection",
			recycleEvt.Reason,
		)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected stalled recycler event")
	}
}

func TestProcessChainsyncRecyclerTickSkipsRecycleOnlyPeer(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)

	connId := newNodeTestConnId(1)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Millisecond,
		},
	)
	require.True(t, state.AddClientConnId(connId))

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("best")},
		BlockNumber: 60,
	}, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
	}

	time.Sleep(5 * time.Millisecond)

	now := time.Now()
	lastProgressSlot := uint64(100)
	lastProgressAt := now
	grace := time.Second
	recycleAt := map[string]time.Time{
		connId.String(): now.Add(-time.Second),
	}
	lastRecycled := make(map[string]time.Time)

	n.processChainsyncRecyclerTick(
		now,
		100,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Millisecond,
		},
		recycleAt,
		lastRecycled,
		&lastProgressSlot,
		&lastProgressAt,
		plateauThreshold(2*time.Minute),
		grace,
		2*time.Minute,
	)

	// No recycle event should be emitted for the only peer.
	select {
	case evt := <-recycleCh:
		t.Fatalf("unexpected recycle event: %+v", evt)
	case <-time.After(50 * time.Millisecond):
		// Expected: recycle suppressed.
	}

	// Grace timer should be rescheduled.
	dueAt, ok := recycleAt[connId.String()]
	require.True(t, ok, "recycle entry should still exist")
	assert.True(
		t,
		dueAt.After(now),
		"due time should be pushed forward",
	)
}

func TestProcessChainsyncRecyclerTickRecyclesLocalTipPlateau(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(
		event.ChainsyncResyncEventType,
	)

	activeConn := newNodeTestConnId(2)
	secondConn := newNodeTestConnId(2001)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   2,
			StallTimeout: time.Hour,
		},
	)
	require.True(t, state.AddClientConnId(activeConn))
	require.True(t, state.AddClientConnId(secondConn))
	state.SetClientConnId(activeConn)

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(activeConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("best")},
		BlockNumber: 60,
	}, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
	}

	now := time.Now()
	lastProgressSlot := uint64(100)
	lastProgressAt := now.Add(-5 * time.Minute)
	recycleAt := make(map[string]time.Time)
	lastRecycled := make(map[string]time.Time)

	n.processChainsyncRecyclerTick(
		now,
		100,
		chainsync.Config{
			MaxClients:   2,
			StallTimeout: time.Hour,
		},
		recycleAt,
		lastRecycled,
		&lastProgressSlot,
		&lastProgressAt,
		4*time.Minute,
		time.Second,
		2*time.Minute,
	)

	select {
	case evt := <-resyncCh:
		resyncEvt, ok := evt.Data.(event.ChainsyncResyncEvent)
		require.True(t, ok)
		assert.Equal(t, activeConn, resyncEvt.ConnectionId)
		assert.Equal(
			t,
			event.ChainsyncResyncReasonLocalTipPlateau,
			resyncEvt.Reason,
		)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected plateau resync event")
	}
}

func TestProcessChainsyncRecyclerTickReconcilesBeforeBacklogSuppression(
	t *testing.T,
) {
	ledgerState, ancestorTip, currentTip, forkTip := newNodeTestDivergedLedger(t)
	require.True(
		t,
		isLedgerApplicationBacklog(
			currentTip.Point.Slot,
			forkTip.Point.Slot,
			forkTip.Point.Slot,
		),
		"fixture must look like a ledger application backlog",
	)

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(
		event.ChainsyncResyncEventType,
	)

	connId := newNodeTestConnId(8)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Hour,
		},
	)
	require.True(t, state.AddClientConnId(connId))
	state.SetClientConnId(connId)

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(connId, forkTip, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
		ledgerState:    ledgerState,
	}

	now := time.Now()
	lastProgressSlot := currentTip.Point.Slot
	lastProgressAt := now.Add(-25 * time.Minute)
	recycleAt := make(map[string]time.Time)
	lastRecycled := make(map[string]time.Time)

	n.processChainsyncRecyclerTick(
		now,
		currentTip.Point.Slot,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Hour,
		},
		recycleAt,
		lastRecycled,
		&lastProgressSlot,
		&lastProgressAt,
		4*time.Minute,
		time.Second,
		2*time.Minute,
	)

	assert.Equal(t, ancestorTip, ledgerState.Tip())
	testutil.RequireNoReceive(
		t,
		resyncCh,
		50*time.Millisecond,
		"chainsync resync should not fire when ledger reconcile repairs the plateau",
	)
}

// TestProcessChainsyncRecyclerTickResyncsPlateauOnlyPeer verifies the
// single-eligible-peer plateau recovery. A flaky/stalled relay that has
// stopped advancing the local tip (while chain selection still tracks it at
// a higher tip) must not wedge the node permanently. With no spare peer, a
// peer RECYCLE (drop + failover) is impossible, but a plateau RESYNC -- which
// closes the connection so peer governance reconnects to the same remote with
// fresh intersect points -- is still the right recovery and must be attempted,
// not skipped.
func TestProcessChainsyncRecyclerTickResyncsPlateauOnlyPeer(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(
		event.ChainsyncResyncEventType,
	)

	connId := newNodeTestConnId(3)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Hour,
		},
	)
	require.True(t, state.AddClientConnId(connId))
	state.SetClientConnId(connId)

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("best")},
		BlockNumber: 60,
	}, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
	}

	now := time.Now()
	lastProgressSlot := uint64(100)
	originalProgress := now.Add(-5 * time.Minute)
	lastProgressAt := originalProgress
	recycleAt := make(map[string]time.Time)
	lastRecycled := make(map[string]time.Time)

	n.processChainsyncRecyclerTick(
		now,
		100,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Hour,
		},
		recycleAt,
		lastRecycled,
		&lastProgressSlot,
		&lastProgressAt,
		4*time.Minute,
		time.Second,
		2*time.Minute,
	)

	// A plateau resync targeting the only eligible peer must be emitted
	// so the stalled connection is closed and re-established from the
	// current local tip. This is the only recovery path when no spare
	// eligible peer exists, and is what unwedges a single-relay node.
	evt := testutil.RequireReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"plateau resync event for the only eligible peer",
	)
	resyncEvt, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalTipPlateau,
		resyncEvt.Reason,
	)

	// The plateau clock and recycle cooldown must be updated so the resync
	// is gated to the cooldown cadence and a healthy single peer is never
	// churned every tick.
	assert.True(
		t,
		lastProgressAt.After(originalProgress),
		"lastProgressAt should be reset after plateau resync",
	)
	_, recorded := lastRecycled[connId.String()]
	assert.True(
		t,
		recorded,
		"plateau resync should record a cooldown timestamp",
	)
}

// TestProcessChainsyncRecyclerTickPlateauOnlyPeerRespectsCooldown verifies that
// after a single-peer plateau resync fires, a second tick within the recycle
// cooldown does NOT fire another resync. This keeps a flaky single peer being
// retried at the cooldown cadence (forward progress without churn) rather than
// reconnecting on every stall-check tick.
func TestProcessChainsyncRecyclerTickPlateauOnlyPeerRespectsCooldown(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(
		event.ChainsyncResyncEventType,
	)

	connId := newNodeTestConnId(7)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Hour,
		},
	)
	require.True(t, state.AddClientConnId(connId))
	state.SetClientConnId(connId)

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 120, Hash: []byte("best")},
		BlockNumber: 60,
	}, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
	}

	cfg := chainsync.Config{
		MaxClients:   1,
		StallTimeout: time.Hour,
	}
	lastProgressSlot := uint64(100)
	recycleAt := make(map[string]time.Time)
	lastRecycled := make(map[string]time.Time)

	// First tick: plateau detected, resync fires and records cooldown.
	now := time.Now()
	lastProgressAt := now.Add(-5 * time.Minute)
	n.processChainsyncRecyclerTick(
		now, 100, cfg, recycleAt, lastRecycled,
		&lastProgressSlot, &lastProgressAt,
		4*time.Minute, time.Second, 2*time.Minute,
	)
	evt := testutil.RequireReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"first plateau resync event",
	)
	resyncEvt, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalTipPlateau,
		resyncEvt.Reason,
	)

	// Second tick still within cooldown: even though the plateau clock is
	// pushed back to look stalled again, the cooldown must suppress a new
	// resync.
	now2 := now.Add(30 * time.Second)
	lastProgressAt = now2.Add(-5 * time.Minute)
	n.processChainsyncRecyclerTick(
		now2, 100, cfg, recycleAt, lastRecycled,
		&lastProgressSlot, &lastProgressAt,
		4*time.Minute, time.Second, 2*time.Minute,
	)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		50*time.Millisecond,
		"plateau resync should be suppressed within recycle cooldown",
	)
}

func TestProcessChainsyncRecyclerTickRealignsOtherPeersOnPlateau(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(
		event.ChainsyncResyncEventType,
	)

	stalledConn := newNodeTestConnId(4001)
	candidateConn := newNodeTestConnId(4002)
	farBehindConn := newNodeTestConnId(4003)
	state := chainsync.NewStateWithConfig(
		bus,
		nil,
		chainsync.Config{
			MaxClients:   3,
			StallTimeout: time.Hour,
		},
	)
	require.True(t, state.AddClientConnId(stalledConn))
	require.True(t, state.AddClientConnId(candidateConn))
	require.True(t, state.AddClientConnId(farBehindConn))
	state.SetClientConnId(stalledConn)
	// Stalled active peer reported a tip past local tip.
	stalledPoint := ocommon.NewPoint(120, []byte("stalled"))
	stalledTip := ochainsync.Tip{Point: stalledPoint, BlockNumber: 60}
	state.UpdateClientTip(stalledConn, stalledPoint, stalledTip)
	// Candidate peer's chainsync cursor has advanced past local tip
	// (we only marked it deduped while the active peer was the sole
	// publisher); without realignment its next RollForward delivers a
	// header beyond the local block tip and the fork resolver fails.
	candidatePoint := ocommon.NewPoint(150, []byte("candidate"))
	candidateTip := ochainsync.Tip{Point: candidatePoint, BlockNumber: 75}
	state.UpdateClientTip(candidateConn, candidatePoint, candidateTip)
	// A peer whose cursor sits at-or-below local tip does not need
	// realigning; it can deliver headers from local-tip+1 directly.
	farBehindPoint := ocommon.NewPoint(80, []byte("behind"))
	farBehindTip := ochainsync.Tip{Point: farBehindPoint, BlockNumber: 40}
	state.UpdateClientTip(farBehindConn, farBehindPoint, farBehindTip)

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	selector.UpdatePeerTip(stalledConn, stalledTip, nil)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
		eventBus:       bus,
	}

	now := time.Now()
	lastProgressSlot := uint64(100)
	lastProgressAt := now.Add(-5 * time.Minute)
	recycleAt := make(map[string]time.Time)
	lastRecycled := make(map[string]time.Time)

	n.processChainsyncRecyclerTick(
		now,
		100,
		chainsync.Config{
			MaxClients:   3,
			StallTimeout: time.Hour,
		},
		recycleAt,
		lastRecycled,
		&lastProgressSlot,
		&lastProgressAt,
		4*time.Minute,
		time.Second,
		2*time.Minute,
	)

	gotPlateauForStalled := false
	gotRealignForCandidate := false
	timeout := time.After(200 * time.Millisecond)
	for !gotPlateauForStalled || !gotRealignForCandidate {
		select {
		case evt := <-resyncCh:
			resyncEvt, ok := evt.Data.(event.ChainsyncResyncEvent)
			require.True(t, ok)
			switch {
			case resyncEvt.Reason == event.ChainsyncResyncReasonLocalTipPlateau &&
				resyncEvt.ConnectionId == stalledConn:
				gotPlateauForStalled = true
			case resyncEvt.Reason == event.ChainsyncResyncReasonPostPlateauRealign &&
				resyncEvt.ConnectionId == candidateConn:
				gotRealignForCandidate = true
			case resyncEvt.Reason == event.ChainsyncResyncReasonPostPlateauRealign &&
				resyncEvt.ConnectionId == farBehindConn:
				t.Fatalf(
					"unexpected realign for peer at-or-below local tip: %+v",
					resyncEvt,
				)
			default:
				t.Fatalf("unexpected resync event: %+v", resyncEvt)
			}
		case <-timeout:
			t.Fatalf(
				"missing resync events: plateau=%v realign=%v",
				gotPlateauForStalled, gotRealignForCandidate,
			)
		}
	}
}

func TestRunStallCheckerTickRecoversAndAllowsFutureTicks(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	var ticks atomic.Int32

	assert.NotPanics(t, func() {
		n.runStallCheckerTick(func() {
			ticks.Add(1)
			panic("boom")
		})
	})

	n.runStallCheckerTick(func() {
		ticks.Add(1)
	})

	assert.Equal(t, int32(2), ticks.Load())
}

func TestRunStallCheckerLoopRecoversAndSupportsRestart(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	var attempts atomic.Int32

	assert.NotPanics(t, func() {
		for {
			recovered := n.runStallCheckerLoop(func() {
				if attempts.Add(1) == 1 {
					panic("boom")
				}
			})
			if !recovered {
				return
			}
		}
	})

	assert.Equal(t, int32(2), attempts.Load())
}

func TestStopReturnsSameShutdownErrorAfterFirstCall(t *testing.T) {
	wantErr := errors.New("shutdown failed")
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		shutdownFuncs: []func(context.Context) error{
			func(context.Context) error {
				return wantErr
			},
		},
	}

	firstErr := n.Stop()
	secondErr := n.Stop()
	require.ErrorIs(t, firstErr, wantErr)
	require.ErrorIs(t, secondErr, wantErr)
	require.Equal(t, firstErr, secondErr)
}

func TestCloseWithShutdownTimeoutReturnsTimeoutError(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	releaseClose := make(chan struct{})
	closeDone := make(chan struct{})

	err := n.closeWithShutdownTimeout(
		context.Background(),
		"test",
		0,
		func() error {
			defer close(closeDone)
			<-releaseClose
			return nil
		},
	)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(releaseClose)
	testutil.RequireReceive(
		t,
		closeDone,
		time.Second,
		"close function completion",
	)
}

// TestNodePeerEligibilityEventUpdatesChainSelector verifies the node wiring:
// a PeerEligibilityChangedEvent published on the event bus must be forwarded
// to the ChainSelector so that the now-ineligible peer is no longer selected.
func TestNodePeerEligibilityEventUpdatesChainSelector(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		EvaluationInterval: time.Hour, // driven by trigger, not ticker
	})
	require.NoError(t, cs.Start(t.Context()))

	connId := newNodeTestConnId(5001)
	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.NewPoint(100, []byte("tip")),
		BlockNumber: 50,
	}, nil)
	require.NotNil(t, cs.GetBestPeer(), "peer should be selected before ineligibility")

	// Mirror the subscription wiring in node.go.
	bus.SubscribeFunc(peergov.PeerEligibilityChangedEventType, func(evt event.Event) {
		e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
		if !ok {
			return
		}
		cs.SetConnectionEligible(e.ConnectionId, e.Eligible)
	})

	bus.Publish(
		peergov.PeerEligibilityChangedEventType,
		event.NewEvent(
			peergov.PeerEligibilityChangedEventType,
			peergov.PeerEligibilityChangedEvent{ConnectionId: connId, Eligible: false},
		),
	)

	require.Eventually(t, func() bool {
		return cs.GetBestPeer() == nil
	}, time.Second, 5*time.Millisecond,
		"ineligible peer must not be selected after eligibility event")
}

// TestNodePeerPriorityEventUpdatesChainSelector verifies the node wiring:
// a PeerPriorityChangedEvent published on the event bus must be forwarded
// to the ChainSelector so that the higher-priority peer wins equal-tip
// selection.
func TestNodePeerPriorityEventUpdatesChainSelector(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{})
	lowPrioConn := newNodeTestConnId(5002)
	highPrioConn := newNodeTestConnId(5003)

	equalTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, []byte("equal")),
		BlockNumber: 50,
	}
	cs.UpdatePeerTip(lowPrioConn, equalTip, nil)
	cs.UpdatePeerTip(highPrioConn, equalTip, nil)

	// Mirror the subscription wiring in node.go.
	bus.SubscribeFunc(peergov.PeerPriorityChangedEventType, func(evt event.Event) {
		e, ok := evt.Data.(peergov.PeerPriorityChangedEvent)
		if !ok {
			return
		}
		cs.SetConnectionPriority(e.ConnectionId, e.Priority)
	})

	bus.Publish(
		peergov.PeerPriorityChangedEventType,
		event.NewEvent(
			peergov.PeerPriorityChangedEventType,
			peergov.PeerPriorityChangedEvent{ConnectionId: highPrioConn, Priority: 50},
		),
	)

	// SelectBestChain does a pure comparison with no incumbent bias, so once
	// the priority event has been processed the higher-priority peer wins.
	require.Eventually(t, func() bool {
		best := cs.SelectBestChain()
		return best != nil && *best == highPrioConn
	}, time.Second, 5*time.Millisecond,
		"higher-priority peer must win equal-tip selection after priority event")
}
