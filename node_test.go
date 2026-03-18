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
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
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
	assert.Equal(t, connB, *active)
	assert.Equal(t, pointA, state.GetTrackedClient(connA).Cursor)
	assert.Equal(t, pointB, state.GetTrackedClient(connB).Cursor)
	assert.Equal(t, uint64(1), state.GetTrackedClient(connA).HeadersRecv)
	assert.Equal(t, uint64(1), state.GetTrackedClient(connB).HeadersRecv)
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

func TestProcessChainsyncRecyclerTickKeepsStalledRecyclerRunning(
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
			"stalled_connection_no_active_selection",
			recycleEvt.Reason,
		)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected stalled recycler event")
	}
}

func TestProcessChainsyncRecyclerTickRecyclesLocalTipPlateau(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)

	connId := newNodeTestConnId(2)
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
	lastProgressAt := now.Add(-5 * time.Minute)
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

	select {
	case evt := <-recycleCh:
		recycleEvt, ok := evt.Data.(connmanager.ConnectionRecycleRequestedEvent)
		require.True(t, ok)
		assert.Equal(t, connId, recycleEvt.ConnectionId)
		assert.Equal(t, "local_tip_plateau", recycleEvt.Reason)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected plateau recycler event")
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
