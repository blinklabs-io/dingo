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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rootTestSlots = 3

// newRootCapacityState returns a State with rootTestSlots client slots in
// which the connections made by newTestConnId with an id of 100 or more are
// configured roots.
func newRootCapacityState(t *testing.T) *chainsync.State {
	t.Helper()
	return chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   rootTestSlots,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(connId ouroboros.ConnectionId) bool {
			return connId.RemoteAddr.(*net.TCPAddr).Port >= 100
		},
	})
}

// fillWithDiscoveredPeers takes every slot with non-root peers and returns
// their connection IDs. A ConnectionId compares by address pointer, so tests
// keep the values they register rather than rebuilding them.
func fillWithDiscoveredPeers(
	t *testing.T,
	state *chainsync.State,
) []ouroboros.ConnectionId {
	t.Helper()
	var peers []ouroboros.ConnectionId
	for id := uint(1); id <= rootTestSlots; id++ {
		connId := newTestConnId(id)
		require.True(
			t,
			state.TryAddClientConnIdWithDirection(
				connId, rootTestSlots, true,
			),
		)
		peers = append(peers, connId)
	}
	require.Equal(t, rootTestSlots, state.ClientConnCount())
	return peers
}

func observabilityOnly(
	t *testing.T,
	state *chainsync.State,
	connId ouroboros.ConnectionId,
) bool {
	t.Helper()
	observed, exists := state.ClientObservabilityOnly(connId)
	require.True(t, exists)
	return observed
}

// A configured root registering when discovered peers hold every slot takes
// one from them; the displaced peer stays connected as an observer.
func TestRootPreemptsDiscoveredPeerWhenSlotsFull(t *testing.T) {
	t.Parallel()
	state := newRootCapacityState(t)
	peers := fillWithDiscoveredPeers(t, state)
	root := newTestConnId(100)

	require.True(
		t,
		state.TryAddClientConnIdWithDirection(root, rootTestSlots, true),
		"a root must be admitted when discovered peers fill every slot",
	)

	assert.False(t, observabilityOnly(t, state, root))
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
	observers := 0
	for _, peer := range peers {
		if observabilityOnly(t, state, peer) {
			observers++
		}
	}
	assert.Equal(t, 1, observers, "exactly one discovered peer is displaced")
}

func TestNonRootDoesNotPreemptWhenSlotsFull(t *testing.T) {
	t.Parallel()
	state := newRootCapacityState(t)
	fillWithDiscoveredPeers(t, state)

	assert.False(
		t,
		state.TryAddClientConnIdWithDirection(
			newTestConnId(4), rootTestSlots, true,
		),
	)
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
}

func TestRootDoesNotPreemptRoots(t *testing.T) {
	t.Parallel()
	state := newRootCapacityState(t)
	for id := uint(100); id < 100+rootTestSlots; id++ {
		require.True(t, state.TryAddClientConnIdWithDirection(
			newTestConnId(id), rootTestSlots, true,
		))
	}

	assert.False(
		t,
		state.TryAddClientConnIdWithDirection(
			newTestConnId(100+rootTestSlots), rootTestSlots, true,
		),
		"roots must not displace each other",
	)
}

// A root tracked as an observer is promoted into a full pool the same way.
func TestRootObserverPromotionPreemptsDiscoveredPeer(t *testing.T) {
	t.Parallel()
	state := newRootCapacityState(t)
	fillWithDiscoveredPeers(t, state)
	root := newTestConnId(100)
	require.True(t, state.TryAddObservedClientConnIdWithDirection(root, true))

	require.True(t, state.SetClientObservabilityOnly(root, false))

	assert.False(t, observabilityOnly(t, state, root))
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
}

// A slot freed by a disconnect goes to a connected root that was holding an
// observer slot, without waiting for the root's next message.
func TestFreedSlotPromotesConnectedRoot(t *testing.T) {
	t.Parallel()
	state := chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   1,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(connId ouroboros.ConnectionId) bool {
			return connId.RemoteAddr.(*net.TCPAddr).Port >= 100
		},
	})
	first, second := newTestConnId(100), newTestConnId(101)
	require.True(t, state.TryAddClientConnIdWithDirection(first, 1, true))
	require.True(t, state.TryAddObservedClientConnIdWithDirection(second, true))

	state.RemoveClientConnId(first)

	assert.False(
		t,
		observabilityOnly(t, state, second),
		"the freed slot must go to the connected root",
	)
}

// A root handed a freed slot wakes a wait registered for its promotion, so a
// roll-forward callback it parked as an observer does not wait on after the
// connection became an ingress source.
func TestFreedSlotPromotionWakesPromotionWait(t *testing.T) {
	t.Parallel()
	state := chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   1,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(connId ouroboros.ConnectionId) bool {
			return connId.RemoteAddr.(*net.TCPAddr).Port >= 100
		},
	})
	first, second := newTestConnId(100), newTestConnId(101)
	require.True(t, state.TryAddClientConnIdWithDirection(first, 1, true))
	require.True(t, state.TryAddObservedClientConnIdWithDirection(second, true))
	wake, unregister := state.AwaitClientPromotion(second)
	defer unregister()

	state.RemoveClientConnId(first)

	require.False(t, observabilityOnly(t, state, second))
	select {
	case <-wake:
	default:
		t.Fatal("a root promoted into a freed slot did not wake its promotion wait")
	}
}

// A slot freed by demoting a client to an observer also goes to a connected
// root that was holding an observer slot.
func TestDemotionPromotesConnectedRoot(t *testing.T) {
	t.Parallel()
	state := chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   1,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(connId ouroboros.ConnectionId) bool {
			return connId.RemoteAddr.(*net.TCPAddr).Port >= 100
		},
	})
	first, second := newTestConnId(100), newTestConnId(101)
	require.True(t, state.TryAddClientConnIdWithDirection(first, 1, true))
	require.True(t, state.TryAddObservedClientConnIdWithDirection(second, true))

	require.True(t, state.SetClientObservabilityOnly(first, true))

	assert.False(
		t,
		observabilityOnly(t, state, second),
		"the slot freed by the demotion must go to the connected root",
	)
}

// Demoting a root must not hand the slot it freed straight back to it.
func TestDemotedRootIsNotRepromoted(t *testing.T) {
	t.Parallel()
	state := chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   1,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	root := newTestConnId(100)
	require.True(t, state.TryAddClientConnIdWithDirection(root, 1, true))

	require.True(t, state.SetClientObservabilityOnly(root, true))

	assert.True(
		t,
		observabilityOnly(t, state, root),
		"a demoted root must stay an observer",
	)
}

// newStaleTipState returns a root-capacity State in which a tip below the
// returned threshold is stale. Raising it models the wall clock advancing.
func newStaleTipState(t *testing.T) (*chainsync.State, *atomic.Uint64) {
	t.Helper()
	threshold := &atomic.Uint64{}
	state := chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   rootTestSlots,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(connId ouroboros.ConnectionId) bool {
			return connId.RemoteAddr.(*net.TCPAddr).Port >= 100
		},
		AdvertisedTipStale: func(tip ochainsync.Tip) bool {
			return tip.Point.Slot < threshold.Load()
		},
	})
	return state, threshold
}

func addEligible(
	t *testing.T,
	state *chainsync.State,
	connId ouroboros.ConnectionId,
) {
	t.Helper()
	require.True(t, state.TryAddClientConnIdWithDirection(
		connId, rootTestSlots, true,
	))
}

func addObserved(
	t *testing.T,
	state *chainsync.State,
	connId ouroboros.ConnectionId,
) {
	t.Helper()
	require.True(t, state.TryAddObservedClientConnIdWithDirection(connId, true))
}

func requireObserving(
	t *testing.T,
	state *chainsync.State,
	connId ouroboros.ConnectionId,
	want bool,
	msg string,
) {
	t.Helper()
	observing, exists := state.ClientObservabilityOnly(connId)
	require.True(t, exists)
	require.Equal(t, want, observing, msg)
}

func staleTestTip(slot uint64) ochainsync.Tip {
	return ochainsync.Tip{Point: ocommon.NewPoint(slot, []byte("tip"))}
}

// A freed slot must not go to a root advertising a stale tip: idle at that tip
// it may never send the message that would demote it again.
func TestFreedSlotSkipsStaleAdvertisingRoot(t *testing.T) {
	t.Parallel()
	state, threshold := newStaleTipState(t)
	holder := newTestConnId(1)
	staleRoot := newTestConnId(100)
	addEligible(t, state, holder)
	addObserved(t, state, staleRoot)
	threshold.Store(100)
	require.True(
		t,
		state.RecordClientAdvertisedTip(staleRoot, staleTestTip(10)),
	)

	require.True(t, state.SetClientObservabilityOnly(holder, true))
	requireObserving(t, state, staleRoot, true,
		"a stale-advertising root must not take the freed slot")
	require.False(t, state.SetClientObservabilityOnly(staleRoot, false),
		"a stale-advertising root must refuse direct promotion")

	// The rule can lift with no message from the peer, as on Genesis exit.
	threshold.Store(0)
	require.True(t, state.SetClientObservabilityOnly(staleRoot, false),
		"a tip no longer stale must be promotable")
}

// A stale-advertising root must not consume the handoff. Map iteration picks
// the candidate, so one round would pass by chance half the time.
func TestFreedSlotReachesFreshRootPastStaleRoot(t *testing.T) {
	t.Parallel()
	for range 64 {
		state, threshold := newStaleTipState(t)
		holder := newTestConnId(1)
		staleRoot := newTestConnId(100)
		freshRoot := newTestConnId(101)
		addEligible(t, state, holder)
		addObserved(t, state, staleRoot)
		addObserved(t, state, freshRoot)
		threshold.Store(100)
		state.RecordClientAdvertisedTip(staleRoot, staleTestTip(10))
		state.RecordClientAdvertisedTip(freshRoot, staleTestTip(500))

		require.True(t, state.SetClientObservabilityOnly(holder, true))
		requireObserving(t, state, freshRoot, false,
			"the freed slot must reach the fresh root")
	}
}

// Promotion judges the stored tip when it is decided: a tip fresh when the
// peer last spoke can go stale while the peer stays silent.
func TestObserverTipAgingStaleRefusesPromotion(t *testing.T) {
	t.Parallel()
	state, threshold := newStaleTipState(t)
	observer := newTestConnId(1)
	addObserved(t, state, observer)
	require.False(
		t,
		state.RecordClientAdvertisedTip(observer, staleTestTip(50)),
	)

	threshold.Store(100)
	require.False(t, state.SetClientObservabilityOnly(observer, false),
		"a tip that aged past the threshold must refuse promotion")
	requireObserving(t, state, observer, true,
		"the observer must keep waiting")
}

// An eligible peer sends nothing when its tip ages past the threshold, so the
// sweep demotes it. A client with no advertised tip is left alone.
func TestDemoteStaleAdvertisingClientsAgesOutIdleClient(t *testing.T) {
	t.Parallel()
	state, threshold := newStaleTipState(t)
	idle := newTestConnId(1)
	unknown := newTestConnId(2)
	addEligible(t, state, idle)
	addEligible(t, state, unknown)
	require.False(t, state.RecordClientAdvertisedTip(idle, staleTestTip(50)))
	require.Empty(t, state.DemoteStaleAdvertisingClients(),
		"a fresh tip must not be demoted")

	threshold.Store(100)
	require.Equal(
		t,
		[]ouroboros.ConnectionId{idle},
		state.DemoteStaleAdvertisingClients(),
	)
	requireObserving(t, state, idle, true,
		"an idle peer whose tip aged out must lose its slot")
	requireObserving(t, state, unknown, false,
		"a client with no advertised tip must keep its slot")
	require.Empty(t, state.DemoteStaleAdvertisingClients(),
		"a demoted client must not be demoted again")
}

// newPausableStaleTipState is newStaleTipState with a pause: after the returned
// arm is called, the next staleness judgement closes judging and then blocks
// until release, holding the decision between its verdict and its effect.
func newPausableStaleTipState(t *testing.T) (
	state *chainsync.State,
	threshold *atomic.Uint64,
	arm func(),
	judging <-chan struct{},
	release func(),
) {
	t.Helper()
	threshold = &atomic.Uint64{}
	judged := make(chan struct{})
	released := make(chan struct{})
	var armed atomic.Bool
	var releaseOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(released) }) }
	// A failing assertion must not leave the decision goroutine blocked.
	t.Cleanup(release)
	state = chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   rootTestSlots,
		StallTimeout: chainsync.DefaultStallTimeout,
		AdvertisedTipStale: func(tip ochainsync.Tip) bool {
			stale := tip.Point.Slot < threshold.Load()
			if armed.CompareAndSwap(true, false) {
				close(judged)
				<-released
			}
			return stale
		},
	})
	return state, threshold, func() { armed.Store(true) }, judged, release
}

// A peer that advertises a fresh tip after the sweep judged its old one must
// keep its slot: the verdict is about a tip the peer no longer advertises.
func TestDemoteStaleAdvertisingClientsKeepsRefreshedClient(t *testing.T) {
	t.Parallel()
	state, threshold, arm, judging, release := newPausableStaleTipState(t)
	client := newTestConnId(1)
	addEligible(t, state, client)
	require.False(t, state.RecordClientAdvertisedTip(client, staleTestTip(50)))
	threshold.Store(100)

	arm()
	demoted := make(chan []ouroboros.ConnectionId, 1)
	go func() { demoted <- state.DemoteStaleAdvertisingClients() }()
	testutil.RequireReceive(t, judging, 5*time.Second, "sweep judged the tip")
	require.False(t, state.RecordClientAdvertisedTip(client, staleTestTip(500)))
	release()

	got := testutil.RequireReceive(t, demoted, 5*time.Second, "sweep finished")
	require.Empty(t, got, "a refreshed client must not be reported demoted")
	requireObserving(t, state, client, false,
		"a client that refreshed its tip must keep its slot")
}

// A peer that advertises a stale tip after promotion judged its old one must
// not take the slot: idle at that tip it would hold it until the next sweep.
func TestPromotionRefusedWhenTipGoesStaleWhileJudging(t *testing.T) {
	t.Parallel()
	state, threshold, arm, judging, release := newPausableStaleTipState(t)
	observer := newTestConnId(1)
	addObserved(t, state, observer)
	threshold.Store(100)
	require.False(
		t,
		state.RecordClientAdvertisedTip(observer, staleTestTip(500)),
	)

	arm()
	promoted := make(chan bool, 1)
	go func() {
		promoted <- state.SetClientObservabilityOnly(observer, false)
	}()
	testutil.RequireReceive(
		t,
		judging,
		5*time.Second,
		"promotion judged the tip",
	)
	require.True(t, state.RecordClientAdvertisedTip(observer, staleTestTip(10)))
	release()

	require.False(
		t,
		testutil.RequireReceive(
			t,
			promoted,
			5*time.Second,
			"promotion finished",
		),
		"a promotion judged on a replaced tip must be refused",
	)
	requireObserving(t, state, observer, true,
		"a client now advertising a stale tip must stay an observer")
}
