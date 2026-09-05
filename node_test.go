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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEffectiveBarkHostDefaultsToLoopbackWhenLifecycleEnabled guards a real
// P0 gap: bark.go's own empty-Host default is "0.0.0.0" (all interfaces),
// which would expose the database lifecycle service's unauthenticated,
// destructive Restore/Truncate RPCs on every interface by default. An
// operator's explicit --bark-host must still always win.
func TestEffectiveBarkHostDefaultsToLoopbackWhenLifecycleEnabled(t *testing.T) {
	require.Equal(t, "127.0.0.1", effectiveBarkHost("", true))
	require.Equal(t, "", effectiveBarkHost("", false))
	require.Equal(t, "0.0.0.0", effectiveBarkHost("0.0.0.0", true))
	require.Equal(t, "10.0.0.5", effectiveBarkHost("10.0.0.5", false))
}

func TestBackfillRewardLiveStakeAtStartup(t *testing.T) {
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	stakeKey := make([]byte, 28)
	stakeKey[0] = 0x51
	missingStakeKey := make([]byte, 28)
	missingStakeKey[0] = 0x52
	_, err = raw.Exec(`
INSERT INTO account (staking_key, pool, added_slot, active)
VALUES (?, ?, 50, TRUE), (?, ?, 60, TRUE)`,
		stakeKey, make([]byte, 28),
		missingStakeKey, make([]byte, 28),
	)
	require.NoError(t, err)
	// Simulate a post-upgrade write that populated only one credential. The
	// startup check must detect the missing canonical credential, not merely
	// test whether reward_live_stake is empty.
	_, err = raw.Exec(`
INSERT INTO reward_live_stake
    (staking_key, credential_tag, utxo_stake, reward_stake, total_stake,
     registered, updated_slot)
VALUES (?, 0, '0', '0', '0', TRUE, 75)`,
		stakeKey,
	)
	require.NoError(t, err)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(100, make([]byte, 32)),
	}, nil))
	needed, err := db.Metadata().RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.True(t, needed)

	n := &Node{
		db: db,
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	require.NoError(t, n.backfillRewardLiveStake())

	needed, err = db.Metadata().RewardLiveStakeNeedsBackfill(nil)
	require.NoError(t, err)
	require.False(t, needed)
	for _, key := range [][]byte{stakeKey, missingStakeKey} {
		var live models.RewardLiveStake
		require.NoError(t, raw.QueryRow(`
SELECT staking_key, credential_tag, registered, updated_slot
FROM reward_live_stake
WHERE credential_tag = ? AND staking_key = ?`,
			0, key,
		).Scan(
			&live.StakingKey,
			&live.CredentialTag,
			&live.Registered,
			&live.UpdatedSlot,
		))
		require.Equal(t, uint64(100), live.UpdatedSlot)
	}
}

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

type nodeTestLogSignalHandler struct {
	message string
	seen    chan struct{}
}

type nodeTestLogCountHandler struct {
	message string
	count   *atomic.Int32
}

func (h nodeTestLogCountHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h nodeTestLogCountHandler) Handle(
	_ context.Context,
	record slog.Record,
) error {
	if record.Message == h.message {
		h.count.Add(1)
	}
	return nil
}

func (h nodeTestLogCountHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h nodeTestLogCountHandler) WithGroup(string) slog.Handler {
	return h
}

func (h nodeTestLogSignalHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h nodeTestLogSignalHandler) Handle(
	_ context.Context,
	record slog.Record,
) error {
	if record.Message == h.message {
		select {
		case h.seen <- struct{}{}:
		default:
		}
	}
	return nil
}

func (h nodeTestLogSignalHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h nodeTestLogSignalHandler) WithGroup(string) slog.Handler {
	return h
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
	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	pointB := ocommon.NewPoint(200, []byte("hash-b"))
	tipA := ochainsync.Tip{Point: pointA, BlockNumber: 10}
	tipB := ochainsync.Tip{Point: pointB, BlockNumber: 20}
	state.UpdateClientTip(connA, pointA, tipA)
	state.UpdateClientTip(connB, pointB, tipB)
	state.SetClientConnId(connA)
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

func TestChainSelectionDoesNotPromoteUntrackedFallback(t *testing.T) {
	for _, selectorFirst := range []bool{true, false} {
		name := "state-removal-first"
		if selectorFirst {
			name = "selector-removal-first"
		}
		t.Run(name, func(t *testing.T) {
			state := chainsync.NewStateWithConfig(
				nil,
				nil,
				chainsync.DefaultConfig(),
			)
			selector := chainselection.NewChainSelector(
				chainselection.ChainSelectorConfig{},
			)
			selected := newNodeTestConnId(3101)
			fallback := newNodeTestConnId(3102)
			require.True(t, state.AddClientConnId(selected))
			require.True(t, state.AddClientConnId(fallback))

			selectedPoint := ocommon.NewPoint(100, []byte("selected"))
			selectedTip := ochainsync.Tip{
				Point:       selectedPoint,
				BlockNumber: 10,
			}
			state.UpdateClientTip(selected, selectedPoint, selectedTip)
			require.True(t, selector.UpdatePeerTip(selected, selectedTip, nil))
			state.SetClientConnId(selected)
			best := selector.GetBestPeer()
			require.NotNil(t, best)
			require.Equal(t, selected, *best)
			trackedFallback := state.GetTrackedClient(fallback)
			require.NotNil(t, trackedFallback)
			require.Zero(
				t, trackedFallback.HeadersRecv,
				"fallback must still be connected but untracked by ChainSync",
			)

			n := &Node{
				config: Config{
					logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
				},
				chainsyncState: state,
				chainSelector:  selector,
			}
			removeFromSelector := func() {
				selector.RemovePeer(selected)
				require.Nil(t, selector.GetBestPeer())
				n.handleChainSelectedNoneEvent(event.NewEvent(
					chainselection.ChainSelectedNoneEventType,
					chainselection.ChainSelectedNoneEvent{
						PreviousConnectionId: selected,
					},
				))
			}
			if selectorFirst {
				removeFromSelector()
				state.RemoveClientConnId(selected)
			} else {
				state.RemoveClientConnId(selected)
				removeFromSelector()
			}

			require.Nil(
				t,
				state.GetClientConnId(),
				"an untracked fallback must not become the ledger source",
			)

			fallbackPoint := ocommon.NewPoint(110, []byte("fallback"))
			fallbackTip := ochainsync.Tip{
				Point:       fallbackPoint,
				BlockNumber: 11,
			}
			state.UpdateClientTip(fallback, fallbackPoint, fallbackTip)
			require.True(t, selector.UpdatePeerTip(fallback, fallbackTip, nil))
			best = selector.GetBestPeer()
			require.NotNil(t, best)
			require.Equal(t, fallback, *best)
			n.handleChainSwitchEvent(event.NewEvent(
				chainselection.ChainSwitchEventType,
				chainselection.ChainSwitchEvent{
					PreviousConnectionId: selected,
					NewConnectionId:      fallback,
					NewTip:               fallbackTip,
				},
			))
			active := state.GetClientConnId()
			require.NotNil(t, active)
			require.Equal(t, fallback, *active)
		})
	}
}

func TestHandleChainSelectedNoneEventDoesNotClearReselectedConnection(
	t *testing.T,
) {
	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	conn := newNodeTestConnId(3103)
	require.True(t, state.AddClientConnId(conn))
	tipPoint := ocommon.NewPoint(120, []byte("reselected"))
	state.UpdateClientTip(conn, tipPoint, ochainsync.Tip{Point: tipPoint})
	require.True(t, state.TrySetClientConnId(conn))
	tip := ochainsync.Tip{
		Point:       tipPoint,
		BlockNumber: 12,
	}
	require.True(t, selector.UpdatePeerTip(conn, tip, nil))
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector:  selector,
	}

	n.handleChainSelectedNoneEvent(event.NewEvent(
		chainselection.ChainSelectedNoneEventType,
		chainselection.ChainSelectedNoneEvent{
			PreviousConnectionId: conn,
		},
	))

	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, conn, *active)
}

func TestHandleChainSelectedNoneEventCoalescesLifecycleContention(
	t *testing.T,
) {
	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	conn := newNodeTestConnId(3104)
	newerPrevious := newNodeTestConnId(3106)
	require.True(t, state.AddClientConnId(conn))
	point := ocommon.NewPoint(130, []byte("selected"))
	state.UpdateClientTip(conn, point, ochainsync.Tip{Point: point})
	require.True(t, state.TrySetClientConnId(conn))

	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{},
	)
	var logCount atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config: Config{
			logger: slog.New(nodeTestLogCountHandler{
				message: "chain selection stalled: no selectable peer",
				count:   &logCount,
			}),
		},
		chainsyncState: state,
		chainSelector:  selector,
	}
	n.startChainSelectedNoneWorker(ctx)
	t.Cleanup(func() {
		cancel()
		n.waitChainSelectedNoneWorker()
	})

	n.liveLifecycleMu.Lock()
	for i := range 64 {
		previous := conn
		if i == 63 {
			// A newer coalesced transition can name a peer whose intervening
			// switch was skipped while the lifecycle lock was held. Selection is
			// still none, so the older registry-active peer must still be cleared.
			previous = newerPrevious
		}
		n.handleChainSelectedNoneEvent(event.NewEvent(
			chainselection.ChainSelectedNoneEventType,
			chainselection.ChainSelectedNoneEvent{
				PreviousConnectionId: previous,
			},
		))
	}
	n.liveLifecycleMu.Unlock()

	require.Eventually(t, func() bool {
		return logCount.Load() == 1 && state.GetClientConnId() == nil
	}, time.Second, time.Millisecond)
	require.Never(t, func() bool {
		return logCount.Load() > 1
	}, 100*time.Millisecond, time.Millisecond,
		"a contended event burst must be handled by one coalesced worker")
}

func TestChainSelectedNoneWorkerCancelsDuringLifecycleContention(
	t *testing.T,
) {
	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	conn := newNodeTestConnId(3105)
	require.True(t, state.AddClientConnId(conn))
	point := ocommon.NewPoint(140, []byte("selected"))
	state.UpdateClientTip(conn, point, ochainsync.Tip{Point: point})
	require.True(t, state.TrySetClientConnId(conn))

	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
		chainSelector: chainselection.NewChainSelector(
			chainselection.ChainSelectorConfig{},
		),
	}
	n.startChainSelectedNoneWorker(ctx)
	n.liveLifecycleMu.Lock()
	n.handleChainSelectedNoneEvent(event.NewEvent(
		chainselection.ChainSelectedNoneEventType,
		chainselection.ChainSelectedNoneEvent{
			PreviousConnectionId: conn,
		},
	))
	cancel()
	n.waitChainSelectedNoneWorker()
	n.liveLifecycleMu.Unlock()

	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, conn, *active)
}

// TestHandleChainSwitchEventNilChainsyncStateDoesNotPanic covers the window
// during a live database restore/truncate where n.chainsyncState is nil
// between closeStorageForLiveLifecycleOp and reinitializeNetworkingCore.
// chainSelector's evaluation loop is never paused during quiesce, so it can
// still emit a ChainSwitchEvent in that window.
func TestHandleChainSwitchEventNilChainsyncStateDoesNotPanic(t *testing.T) {
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}

	require.NotPanics(t, func() {
		n.handleChainSwitchEvent(
			event.NewEvent(
				chainselection.ChainSwitchEventType,
				chainselection.ChainSwitchEvent{
					NewConnectionId: newNodeTestConnId(3003),
					NewTip: ochainsync.Tip{
						Point: ocommon.NewPoint(100, []byte("hash-a")),
					},
				},
			),
		)
	})
}

// TestHandleChainSwitchEventSkipsUpdateDuringLiveLifecycleOp covers the same
// window from the other side: n.chainsyncState has already been rebuilt to a
// non-nil value, but a live restore/truncate still holds n.liveLifecycleMu
// (held for its entire quiesce-through-reinitialize duration), so the
// handler must not block waiting for it -- it should skip the update rather
// than stall the EventBus dispatch goroutine behind a possibly long-running
// operation.
func TestHandleChainSwitchEventSkipsUpdateDuringLiveLifecycleOp(t *testing.T) {
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
	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	state.UpdateClientTipWithoutDedup(
		connA, pointA, ochainsync.Tip{Point: pointA},
	)
	require.True(t, state.TrySetClientConnId(connA))
	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		chainsyncState: state,
	}

	n.liveLifecycleMu.Lock()
	defer n.liveLifecycleMu.Unlock()

	n.handleChainSwitchEvent(
		event.NewEvent(
			chainselection.ChainSwitchEventType,
			chainselection.ChainSwitchEvent{
				PreviousConnectionId: connA,
				NewConnectionId:      connB,
				NewTip: ochainsync.Tip{
					Point: ocommon.NewPoint(200, []byte("hash-b")),
				},
			},
		),
	)

	active := state.GetClientConnId()
	require.NotNil(t, active)
	assert.Equal(t, connA, *active)
}

func TestLedgerStateConfigSkipsChainsyncReadDuringLiveLifecycleOp(
	t *testing.T,
) {
	state := chainsync.NewStateWithConfig(
		nil,
		nil,
		chainsync.DefaultConfig(),
	)
	connId := newNodeTestConnId(3001)
	require.True(t, state.AddClientConnId(connId))
	point := ocommon.NewPoint(100, []byte("header"))
	state.UpdateClientTipWithoutDedup(
		connId, point, ochainsync.Tip{Point: point},
	)
	require.True(t, state.TrySetClientConnId(connId))
	n := &Node{
		chainsyncState: state,
		config:         Config{cfg: &internalconfig.Config{}},
	}
	config := n.ledgerStateConfig()

	active := config.GetActiveConnectionFunc()
	require.NotNil(t, active)
	assert.Equal(t, connId, *active)

	n.liveLifecycleMu.Lock()
	active = config.GetActiveConnectionFunc()
	n.liveLifecycleMu.Unlock()
	assert.Nil(t, active)
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

// TestStartupFailureCleanupCancelsBeforeAllowingShutdown verifies the
// signal-during-startup lifecycle boundary. Run owns startupLifecycleMu while
// it unwinds its LIFO stack; shutdown must wait for that rollback rather than
// closing the same partially initialized resource concurrently.
func TestStartupFailureCleanupCancelsBeforeAllowingShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rollbackStarted := make(chan struct{})
	releaseRollback := make(chan struct{})
	var releaseRollbackOnce sync.Once
	release := func() { releaseRollbackOnce.Do(func() { close(releaseRollback) }) }
	defer release()
	rollbackDone := make(chan struct{})
	shutdownFuncStarted := make(chan struct{})
	shutdownDone := make(chan error, 1)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		ctx:    ctx,
		cancel: cancel,
		shutdownFuncs: []func(context.Context) error{
			func(context.Context) error {
				close(shutdownFuncStarted)
				return nil
			},
		},
	}

	// Match Run's startup section: cleanupFailedStartup owns the gate until
	// every started component's rollback completes.
	n.startupLifecycleMu.Lock()
	go func() {
		defer close(rollbackDone)
		n.cleanupFailedStartup([]func(){func() {
			close(rollbackStarted)
			<-releaseRollback
		}})
	}()
	testutil.RequireReceive(
		t,
		rollbackStarted,
		time.Second,
		"startup rollback to begin",
	)
	require.ErrorIs(t, ctx.Err(), context.Canceled)

	go func() {
		shutdownDone <- n.shutdown()
	}()
	// If shutdown did not take the same gate, its phase-four callback would
	// run while the startup rollback is intentionally blocked above.
	testutil.RequireNoReceive(
		t,
		shutdownFuncStarted,
		50*time.Millisecond,
		"normal shutdown while startup rollback owns the lifecycle gate",
	)

	release()
	testutil.RequireReceive(
		t,
		rollbackDone,
		time.Second,
		"startup rollback completion",
	)
	testutil.RequireReceive(
		t,
		shutdownFuncStarted,
		time.Second,
		"normal shutdown after startup rollback completion",
	)
	require.NoError(t, <-shutdownDone)
}

func TestShutdownClosesEventBusBeforeFinalCleanup(t *testing.T) {
	const eventType event.EventType = "test.shutdown.order"

	bus := event.NewEventBus(nil, nil)
	_, _ = bus.SubscribeWithBuffer(eventType, 1)
	bus.Publish(eventType, event.NewEvent(eventType, "fill"))

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		bus.Publish(eventType, event.NewEvent(eventType, "blocked"))
	}()
	testutil.RequireNoReceive(
		t,
		publishDone,
		50*time.Millisecond,
		"event publisher should be backpressured before shutdown",
	)

	n := &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		eventBus: bus,
		shutdownFuncs: []func(context.Context) error{
			func(context.Context) error {
				select {
				case <-publishDone:
					return nil
				case <-time.After(time.Second):
					return errors.New(
						"event bus was not closed before final cleanup",
					)
				}
			},
		},
	}

	require.NoError(t, n.Stop())
	testutil.RequireReceive(
		t,
		publishDone,
		time.Second,
		"backpressured publisher did not exit after node shutdown",
	)
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

// TestShutdownDoesNotCloseDatabaseWhenLedgerDrainIsUnconfirmed protects the
// storage safety boundary shared with live Restore/Truncate. LedgerState.Close
// can time out while a database worker is still using the database; normal
// shutdown must not close the database or its provider-owned stores in that
// state.
func TestShutdownDoesNotCloseDatabaseWhenLedgerDrainIsUnconfirmed(
	t *testing.T,
) {
	n, _ := newLiveLifecycleTestNodeWithGenesis(
		t,
		1,
		nil,
		ledger.DatabaseWorkerPoolConfig{WorkerPoolSize: 1, TaskQueueSize: 1},
	)

	origTimeout := ledger.CloseDBWorkerPoolShutdownTimeout
	ledger.CloseDBWorkerPoolShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { ledger.CloseDBWorkerPoolShutdownTimeout = origTimeout })

	started := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})
	defer func() {
		close(release)
		testutil.RequireReceive(
			t,
			workerDone,
			time.Second,
			"database worker drain",
		)
	}()
	go func() {
		defer close(workerDone)
		_ = n.ledgerState.SubmitAsyncDBOperation(
			func(*database.Database) error {
				close(started)
				<-release
				return nil
			},
		)
	}()
	<-started

	shutdownErr := n.shutdown()
	require.Error(t, shutdownErr)
	require.ErrorContains(t, shutdownErr, "database worker pool")
	require.ErrorContains(t, shutdownErr, "database close skipped")

	// The ledger worker is still blocked, so the database must remain usable.
	require.NoError(t, n.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, make([]byte, 32)),
	}, nil))
}

// TestCleanupFailedStartupSkipsDatabaseCloseWhenLedgerDrainIsUnconfirmed
// covers the startup-failure LIFO rollback path with the same guard
// TestShutdownDoesNotCloseDatabaseWhenLedgerDrainIsUnconfirmed covers for the
// normal signal-driven path: cleanupFailedStartup runs the same ledgerState
// timeout Run() registers, and the earlier-registered (so later-run) db.Close
// and pluginHost.Stop LIFO stops must skip closing storage a still-running
// background goroutine may be using, not silently discard the drain failure.
//
// Unlike that shutdown() test, this one hand-builds the rollback slice
// rather than driving Run() to a real startup failure: shutdown()'s phase
// ordering is hard-coded directly in that function, so calling it exercises
// the real order; cleanupFailedStartup's ordering is purely a property of
// which `started = append(started, ...)` calls Run() happens to reach before
// failing, assembled across ~30 such calls interleaved through Run()'s
// startup sequence, each registered immediately after the resource it tears
// down becomes available -- so driving the real path here would mean
// injecting a failure at a specific point inside that sequence rather than
// calling one self-contained function. This test therefore only proves the
// guard logic is correct given the order Run() is documented (here and in
// ARCHITECTURE.md) to register it in; it cannot catch a future edit to Run()
// that reorders the db.Close/pluginHost.Stop/ledgerState.Close registrations
// relative to each other. Matches this file's existing convention for
// exercising cleanupFailedStartup with a hand-built `started` (see the
// startup-lifecycle-gate test above) and newLiveLifecycleTestNode's own
// documented pattern of wiring a real Node without going through Run().
func TestCleanupFailedStartupSkipsDatabaseCloseWhenLedgerDrainIsUnconfirmed(
	t *testing.T,
) {
	n, _ := newLiveLifecycleTestNodeWithGenesis(
		t,
		1,
		nil,
		ledger.DatabaseWorkerPoolConfig{WorkerPoolSize: 1, TaskQueueSize: 1},
	)

	origTimeout := ledger.CloseDBWorkerPoolShutdownTimeout
	ledger.CloseDBWorkerPoolShutdownTimeout = 10 * time.Millisecond
	t.Cleanup(func() { ledger.CloseDBWorkerPoolShutdownTimeout = origTimeout })

	started := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan struct{})
	defer func() {
		close(release)
		testutil.RequireReceive(
			t,
			workerDone,
			time.Second,
			"database worker drain",
		)
	}()
	go func() {
		defer close(workerDone)
		_ = n.ledgerState.SubmitAsyncDBOperation(
			func(*database.Database) error {
				close(started)
				<-release
				return nil
			},
		)
	}()
	<-started

	// Mirror Run's exact registration order and skip logic: ledgerState.Close
	// registered last (so run first in LIFO) sets the flag; db.Close and
	// pluginHost.Stop, registered earlier (so run later), check it.
	ledgerStateDrainConfirmed := true
	var pluginHostStopped, dbClosed bool
	rollback := []func(){
		func() {
			if !ledgerStateDrainConfirmed {
				return
			}
			dbClosed = true
			_ = n.db.Close()
		},
		func() {
			if !ledgerStateDrainConfirmed {
				return
			}
			pluginHostStopped = true
			_ = n.pluginHost.Stop(context.Background())
		},
		func() {
			if err := n.ledgerState.Close(); err != nil {
				ledgerStateDrainConfirmed = false
			}
		},
	}
	n.startupLifecycleMu.Lock()
	n.cleanupFailedStartup(rollback)

	assert.False(t, dbClosed, "db.Close must be skipped when the ledger drain is unconfirmed")
	assert.False(
		t,
		pluginHostStopped,
		"pluginHost.Stop must be skipped when the ledger drain is unconfirmed",
	)
	// The ledger worker is still blocked, so the database must remain usable.
	require.NoError(t, n.db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, make([]byte, 32)),
	}, nil))
}

// newChainSelectorSubscriptionTestNode builds the minimal node
// subscribeChainSelectorEvents needs, so tests can register the production
// subscriptions instead of reimplementing them.
func newChainSelectorSubscriptionTestNode(
	t *testing.T,
	bus *event.EventBus,
	cs *chainselection.ChainSelector,
) *Node {
	t.Helper()
	return &Node{
		config: Config{
			logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		eventBus:      bus,
		chainSelector: cs,
	}
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
	require.NotNil(
		t,
		cs.GetBestPeer(),
		"peer should be selected before ineligibility",
	)

	// Exercise the real node wiring rather than a copy of it.
	newChainSelectorSubscriptionTestNode(t, bus, cs).
		subscribeChainSelectorEvents()

	bus.Publish(
		peergov.PeerEligibilityChangedEventType,
		event.NewEvent(
			peergov.PeerEligibilityChangedEventType,
			peergov.PeerEligibilityChangedEvent{
				ConnectionId: connId,
				Eligible:     false,
			},
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

	// Exercise the real node wiring rather than a copy of it.
	newChainSelectorSubscriptionTestNode(t, bus, cs).
		subscribeChainSelectorEvents()

	bus.Publish(
		peergov.PeerPriorityChangedEventType,
		event.NewEvent(
			peergov.PeerPriorityChangedEventType,
			peergov.PeerPriorityChangedEvent{
				ConnectionId: highPrioConn,
				Priority:     50,
			},
		),
	)

	// SelectBestChain does a pure comparison with no incumbent bias, so once
	// the priority event has been processed the higher-priority peer wins.
	require.Eventually(
		t,
		func() bool {
			best := cs.SelectBestChain()
			return best != nil && *best == highPrioConn
		},
		time.Second,
		5*time.Millisecond,
		"higher-priority peer must win equal-tip selection after priority event",
	)
}

// A close/stop failure surfaced during the startup-cleanup unwind must
// actually reach the log, not just be swallowed by the caller's `_ =`.
func TestLogErrIfNotNilLogsOnError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	logErrIfNotNil(
		logger,
		"failed to stop leader election during cleanup",
		errors.New("epoch transition in flight"),
	)

	out := buf.String()
	if !strings.Contains(out, "failed to stop leader election during cleanup") {
		t.Fatalf("expected log message in output, got: %s", out)
	}
	if !strings.Contains(out, "epoch transition in flight") {
		t.Fatalf("expected error detail in output, got: %s", out)
	}
}

// The common case -- a clean stop -- must stay silent, or every successful
// shutdown would log a spurious error line.
func TestLogErrIfNotNilStaysQuietOnNil(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	logErrIfNotNil(logger, "failed to stop leader election during cleanup", nil)

	if buf.Len() != 0 {
		t.Fatalf(
			"expected no log output for a nil error, got: %s",
			buf.String(),
		)
	}
}
