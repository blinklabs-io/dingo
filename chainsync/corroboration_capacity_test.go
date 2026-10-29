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
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/chainsync"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In these tests a connection whose remote port is 200 or more is a
// corroboration-capable peer (one with an identity), and a port of 100 to 199
// is a configured root. Everything below 100 is an identity-less peer.
func newCorroborationState(
	t *testing.T,
	active *atomic.Bool,
) *chainsync.State {
	t.Helper()
	return chainsync.NewStateWithConfig(nil, nil, chainsync.Config{
		MaxClients:   rootTestSlots,
		StallTimeout: chainsync.DefaultStallTimeout,
		IsRoot: func(connId ouroboros.ConnectionId) bool {
			port := connId.RemoteAddr.(*net.TCPAddr).Port
			return port >= 100 && port < 200
		},
		CanCorroborate: func(connId ouroboros.ConnectionId) bool {
			return active.Load() &&
				connId.RemoteAddr.(*net.TCPAddr).Port >= 200
		},
	})
}

func countObservers(
	t *testing.T,
	state *chainsync.State,
	peers []ouroboros.ConnectionId,
) int {
	t.Helper()
	observers := 0
	for _, peer := range peers {
		if observabilityOnly(t, state, peer) {
			observers++
		}
	}
	return observers
}

// While corroboration is in effect an identified peer takes an eligible slot
// from identity-less peers, which cannot make progress, and the displaced peer
// stays connected as an observer.
func TestCorroborationCapablePeerPreemptsIdentityLessPeer(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	peers := fillWithDiscoveredPeers(t, state)
	identified := newTestConnId(200)

	require.True(
		t,
		state.TryAddClientConnIdWithDirection(
			identified, rootTestSlots, true,
		),
		"an identified peer must be admitted when identity-less peers fill every slot",
	)

	assert.False(t, observabilityOnly(t, state, identified))
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
	assert.Equal(t, 1, countObservers(t, state, peers))
}

// An identified peer tracked as an observer is promoted into a full pool the
// same way when its next header asks for ingress.
func TestCorroborationCapableObserverPromotionPreemptsIdentityLessPeer(
	t *testing.T,
) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	peers := fillWithDiscoveredPeers(t, state)
	identified := newTestConnId(200)
	require.True(
		t,
		state.TryAddObservedClientConnIdWithDirection(identified, true),
	)

	require.True(t, state.SetClientObservabilityOnly(identified, false))

	assert.False(t, observabilityOnly(t, state, identified))
	assert.Equal(t, 1, countObservers(t, state, peers))
}

func TestCorroborationCapablePeerDoesNotPreemptWhenInactive(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	state := newCorroborationState(t, &active)
	fillWithDiscoveredPeers(t, state)

	assert.False(
		t,
		state.TryAddClientConnIdWithDirection(
			newTestConnId(200), rootTestSlots, true,
		),
	)
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
}

func TestIdentityLessPeerDoesNotPreemptWhenCorroborationActive(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	fillWithDiscoveredPeers(t, state)

	assert.False(
		t,
		state.TryAddClientConnIdWithDirection(
			newTestConnId(4), rootTestSlots, true,
		),
	)
}

// Identified peers do not displace each other, or a configured root.
func TestCorroborationCapablePeerKeepsIdentifiedPeersAndRoots(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	var held []ouroboros.ConnectionId
	for _, id := range []uint{100, 200, 201} {
		connId := newTestConnId(id)
		require.True(t, state.TryAddClientConnIdWithDirection(
			connId, rootTestSlots, true,
		))
		held = append(held, connId)
	}

	assert.False(
		t,
		state.TryAddClientConnIdWithDirection(
			newTestConnId(202), rootTestSlots, true,
		),
	)
	for _, connId := range held {
		assert.False(t, observabilityOnly(t, state, connId))
	}
}
