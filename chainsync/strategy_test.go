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

package chainsync_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// --- Strategy parsing / formatting ---

func TestParseHeaderSyncStrategy(t *testing.T) {
	tests := []struct {
		input   string
		want    chainsync.HeaderSyncStrategy
		wantErr bool
	}{
		{"primary", chainsync.HeaderSyncStrategyPrimary, false},
		{"parallel", chainsync.HeaderSyncStrategyParallel, false},
		{"round-robin", chainsync.HeaderSyncStrategyRoundRobin, false},
		{"roundrobin", chainsync.HeaderSyncStrategyRoundRobin, false},
		{"round_robin", chainsync.HeaderSyncStrategyRoundRobin, false},
		{"PRIMARY", chainsync.HeaderSyncStrategyPrimary, false},
		{"  parallel  ", chainsync.HeaderSyncStrategyParallel, false},
		// Empty string falls back to the default (primary).
		{"", chainsync.HeaderSyncStrategyPrimary, false},
		{"bogus", chainsync.HeaderSyncStrategyPrimary, true},
	}
	for _, tc := range tests {
		got, err := chainsync.ParseHeaderSyncStrategy(tc.input)
		if tc.wantErr {
			require.Error(t, err, "input %q", tc.input)
			continue
		}
		require.NoError(t, err, "input %q", tc.input)
		require.Equal(t, tc.want, got, "input %q", tc.input)
	}
}

func TestHeaderSyncStrategy_String(t *testing.T) {
	require.Equal(t, "primary", chainsync.HeaderSyncStrategyPrimary.String())
	require.Equal(t, "parallel", chainsync.HeaderSyncStrategyParallel.String())
	require.Equal(
		t,
		"round-robin",
		chainsync.HeaderSyncStrategyRoundRobin.String(),
	)
}

func TestDefaultConfig_HeaderSyncStrategyIsPrimary(t *testing.T) {
	require.Equal(
		t,
		chainsync.HeaderSyncStrategyPrimary,
		chainsync.DefaultConfig().HeaderSyncStrategy,
	)
}

// --- ShouldPublishHeader: primary (default) preserves existing behavior ---

// Under the primary strategy a new header from any eligible peer publishes,
// and the active peer replays a duplicate first seen from another peer. This
// matches the behavior before the strategy gate existed.
func TestShouldPublishHeader_Primary_PreservesReplay(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyPrimary
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))
	s.SetClientConnId(connA) // A is the active/primary peer

	point := ocommon.NewPoint(100, []byte("hash-1"))
	tip := ochainsync.Tip{Point: point}

	// Non-active eligible peer B reports the header first: new -> publish.
	isNew := s.UpdateClientTip(connB, point, tip)
	require.True(t, isNew)
	require.True(t, s.ShouldPublishHeader(connB, point, isNew))

	// Active peer A reports the same header: duplicate, but the active peer
	// replays a header first seen elsewhere -> publish.
	isNew = s.UpdateClientTip(connA, point, tip)
	require.False(t, isNew)
	require.True(t, s.ShouldPublishHeader(connA, point, isNew))

	// Non-active peer B reports the same header again: duplicate, not active
	// -> do not replay.
	isNew = s.UpdateClientTip(connB, point, tip)
	require.False(t, isNew)
	require.False(t, s.ShouldPublishHeader(connB, point, isNew))
}

// --- ShouldPublishHeader: parallel consumes from multiple peers, deduped ---

// Under the parallel strategy multiple eligible peers may supply headers
// concurrently. A duplicate from any peer is never replayed, so a header
// never enters ledger processing twice.
func TestShouldPublishHeader_Parallel_MultiPeerNoDoubleIngress(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyParallel
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))
	s.SetClientConnId(connA)

	pointA := ocommon.NewPoint(100, []byte("hash-100"))
	pointB := ocommon.NewPoint(101, []byte("hash-101"))
	tipA := ochainsync.Tip{Point: pointA}
	tipB := ochainsync.Tip{Point: pointB}

	// B is first to report slot 100: publish.
	isNew := s.UpdateClientTip(connB, pointA, tipA)
	require.True(t, isNew)
	require.True(t, s.ShouldPublishHeader(connB, pointA, isNew))

	// Active peer A then reports the same slot 100 header: duplicate, no
	// replay in parallel mode -> suppressed (no double ingress).
	isNew = s.UpdateClientTip(connA, pointA, tipA)
	require.False(t, isNew)
	require.False(t, s.ShouldPublishHeader(connA, pointA, isNew))

	// A is first to report a different header (slot 101): publish. Multiple
	// eligible peers supply different headers concurrently.
	isNew = s.UpdateClientTip(connA, pointB, tipB)
	require.True(t, isNew)
	require.True(t, s.ShouldPublishHeader(connA, pointB, isNew))
}

// --- ShouldPublishHeader: round-robin rotates a single driver ---

func TestShouldPublishHeader_RoundRobin_SingleRotatingDriver(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyRoundRobin
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))

	point := ocommon.NewPoint(100, []byte("hash-1"))

	// Exactly one of the two peers is the current driver for a new header.
	driveA := s.ShouldPublishHeader(connA, point, true)
	driveB := s.ShouldPublishHeader(connB, point, true)
	require.NotEqual(
		t,
		driveA,
		driveB,
		"exactly one peer should drive ingress under round-robin",
	)

	// Advancing the rotation flips the driver (two eligible peers).
	s.AdvanceHeaderSyncRotation()
	require.Equal(t, driveA, s.ShouldPublishHeader(connB, point, true))
	require.Equal(t, driveB, s.ShouldPublishHeader(connA, point, true))
}

// A duplicate header first seen from another connection is replayed only by
// the current round-robin driver.
func TestShouldPublishHeader_RoundRobin_DriverReplaysSeenElsewhere(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyRoundRobin
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))

	point := ocommon.NewPoint(100, []byte("hash-1"))
	tip := ochainsync.Tip{Point: point}

	// Identify the driver and the non-driver at the current rotation.
	var driver, other = connA, connB
	if !s.ShouldPublishHeader(connA, point, true) {
		driver, other = connB, connA
	}

	// The non-driver records the header first but is suppressed.
	isNew := s.UpdateClientTip(other, point, tip)
	require.True(t, isNew)
	require.False(t, s.ShouldPublishHeader(other, point, isNew))

	// The driver then reports the same header: duplicate seen elsewhere ->
	// driver replays it.
	isNew = s.UpdateClientTip(driver, point, tip)
	require.False(t, isNew)
	require.True(t, s.ShouldPublishHeader(driver, point, isNew))
}

// --- Divergence still produces fork handling, not silent suppression ---

func TestShouldPublishHeader_DivergenceKeepsForkHandling(t *testing.T) {
	bus := newTestEventBus(t)
	_, forkCh := bus.Subscribe(chainsync.ForkDetectedEventType)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyParallel
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))

	pointA := ocommon.NewPoint(100, []byte("hash-a"))
	pointB := ocommon.NewPoint(100, []byte("hash-b")) // same slot, diff hash
	tipA := ochainsync.Tip{Point: pointA}
	tipB := ochainsync.Tip{Point: pointB}

	// A's header at slot 100 is new -> publish.
	isNew := s.UpdateClientTip(connA, pointA, tipA)
	require.True(t, isNew)
	require.True(t, s.ShouldPublishHeader(connA, pointA, isNew))

	// B's divergent header at the same slot is a distinct hash: it is treated
	// as new (a fork candidate) and is published rather than silently
	// suppressed, and a fork-detected event is emitted.
	isNew = s.UpdateClientTip(connB, pointB, tipB)
	require.True(t, isNew)
	require.True(t, s.ShouldPublishHeader(connB, pointB, isNew))

	testutilRequireForkEvent(t, forkCh)
}

// --- Failover does not strand ingestion ---

// When the active/primary peer disconnects, the remaining eligible peer is
// promoted and can drive ingestion (primary replays apply to the new active).
func TestShouldPublishHeader_PrimaryFailoverDoesNotStrand(t *testing.T) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyPrimary
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))
	s.SetClientConnId(connA)

	// The active peer disconnects; B should be promoted.
	s.RemoveClientConnId(connA)
	active := s.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connB, *active)

	// Ingestion continues from the remaining peer.
	point := ocommon.NewPoint(200, []byte("hash-2"))
	tip := ochainsync.Tip{Point: point}
	isNew := s.UpdateClientTip(connB, point, tip)
	require.True(t, isNew)
	require.True(t, s.ShouldPublishHeader(connB, point, isNew))
}

// Under round-robin, removing the current driver must not strand ingestion:
// the remaining eligible peer becomes the driver.
func TestShouldPublishHeader_RoundRobinDriverRemovalDoesNotStrand(
	t *testing.T,
) {
	bus := newTestEventBus(t)
	cfg := chainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = chainsync.HeaderSyncStrategyRoundRobin
	s := newTestState(t, bus, cfg)

	connA := newTestConnId(1)
	connB := newTestConnId(2)
	require.True(t, s.AddClientConnId(connA))
	require.True(t, s.AddClientConnId(connB))

	point := ocommon.NewPoint(100, []byte("hash-1"))

	driver, other := connA, connB
	if !s.ShouldPublishHeader(connA, point, true) {
		driver, other = connB, connA
	}

	// Remove the current driver; the remaining peer must be able to drive.
	s.RemoveClientConnId(driver)
	require.True(
		t,
		s.ShouldPublishHeader(other, point, true),
		"remaining peer should become the round-robin driver",
	)
}

func testutilRequireForkEvent(t *testing.T, ch <-chan event.Event) {
	t.Helper()
	select {
	case evt := <-ch:
		_, ok := evt.Data.(chainsync.ForkDetectedEvent)
		require.True(t, ok, "expected ForkDetectedEvent")
	case <-time.After(2 * time.Second):
		t.Fatal("expected a fork-detected event")
	}
}
