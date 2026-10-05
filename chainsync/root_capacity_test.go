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
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	ouroboros "github.com/blinklabs-io/gouroboros"
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
