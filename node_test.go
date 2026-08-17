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
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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
	state.SetClientConnId(connA)
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
					return errors.New("event bus was not closed before final cleanup")
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
