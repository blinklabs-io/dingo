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
	"fmt"
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
		CorroborationIdentity: func(connId ouroboros.ConnectionId) string {
			port := connId.RemoteAddr.(*net.TCPAddr).Port
			if !active.Load() || port < 200 {
				return ""
			}
			// Ports 200-299 are distinct identities; 300 and above map to
			// identity "pool-<port/100>" so several ports share one pool.
			if port < 300 {
				return fmt.Sprintf("peer-%d", port)
			}
			return fmt.Sprintf("pool-%d", port/100)
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

// Several connections of one pool identity must not take every slot: a second
// connection of an identity already holding an ingress slot cannot displace an
// identity-less peer, so the remaining slots stay available to other
// candidates.
func TestCorroborationSameIdentityCannotFillAllSlots(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	peers := fillWithDiscoveredPeers(t, state)

	require.True(t, state.TryAddClientConnIdWithDirection(
		newTestConnId(300), rootTestSlots, true,
	))
	for _, port := range []uint{301, 302, 303} {
		state.TryAddClientConnIdWithDirection(
			newTestConnId(port), rootTestSlots, true,
		)
	}

	assert.Equal(
		t,
		1,
		countObservers(t, state, peers),
		"only the first connection of an identity may displace a peer",
	)
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
}

// A distinct identity can still take a slot from a duplicate connection of an
// identity that another ingress client already represents, so duplicates never
// crowd out a distinct witness.
func TestCorroborationDistinctIdentityDisplacesDuplicateIdentity(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	var dups []ouroboros.ConnectionId
	for _, port := range []uint{300, 301, 302} {
		connId := newTestConnId(port)
		require.True(t, state.TryAddClientConnIdWithDirection(
			connId, rootTestSlots, true,
		))
		dups = append(dups, connId)
	}
	distinct := newTestConnId(200)

	require.True(t, state.TryAddClientConnIdWithDirection(
		distinct, rootTestSlots, true,
	))

	assert.False(t, observabilityOnly(t, state, distinct))
	assert.Equal(t, 1, countObservers(t, state, dups))
}

// A duplicate-identity connection that was displaced cannot take the slot back
// while another connection of its identity holds one, so displacement does not
// cycle.
func TestCorroborationDisplacedDuplicateDoesNotCycle(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	var dups []ouroboros.ConnectionId
	for _, port := range []uint{300, 301, 302} {
		connId := newTestConnId(port)
		require.True(t, state.TryAddClientConnIdWithDirection(
			connId, rootTestSlots, true,
		))
		dups = append(dups, connId)
	}
	require.True(t, state.TryAddClientConnIdWithDirection(
		newTestConnId(200), rootTestSlots, true,
	))
	var displaced ouroboros.ConnectionId
	for _, connId := range dups {
		if observabilityOnly(t, state, connId) {
			displaced = connId
		}
	}
	require.NotZero(t, displaced.RemoteAddr)

	assert.False(t, state.SetClientObservabilityOnly(displaced, false))
	assert.True(t, observabilityOnly(t, state, displaced))
}

// A configured root keeps its claim on a slot while corroboration is in
// effect, even when every slot holds a distinct identity: the identity
// restriction on victims applies only to non-root claimants.
func TestCorroborationRootPreemptsDistinctIdentities(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	var identified []ouroboros.ConnectionId
	for _, port := range []uint{200, 201, 202} {
		connId := newTestConnId(port)
		require.True(t, state.TryAddClientConnIdWithDirection(
			connId, rootTestSlots, true,
		))
		identified = append(identified, connId)
	}
	root := newTestConnId(100)

	require.True(
		t,
		state.TryAddClientConnIdWithDirection(root, rootTestSlots, true),
		"a configured root must be admitted when distinct identities fill every slot",
	)

	assert.False(t, observabilityOnly(t, state, root))
	assert.Equal(t, rootTestSlots, state.ClientConnCount())
	assert.Equal(t, 1, countObservers(t, state, identified))
}

// A configured root displaces an identity-less client before a distinct
// identity, even a more recently active one, so admitting the root does not cost a witness when it need not.
func TestCorroborationRootPrefersIdentityLessVictim(t *testing.T) {
	t.Parallel()
	var active atomic.Bool
	active.Store(true)
	state := newCorroborationState(t, &active)
	var identified []ouroboros.ConnectionId
	for _, port := range []uint{200, 201} {
		connId := newTestConnId(port)
		require.True(t, state.TryAddClientConnIdWithDirection(
			connId, rootTestSlots, true,
		))
		identified = append(identified, connId)
	}
	// Added last, so it is the most recently active and longest idle alone
	// would pick an identified peer.
	identityLess := newTestConnId(1)
	require.True(t, state.TryAddClientConnIdWithDirection(
		identityLess, rootTestSlots, true,
	))

	require.True(t, state.TryAddClientConnIdWithDirection(
		newTestConnId(100), rootTestSlots, true,
	))

	assert.True(t, observabilityOnly(t, state, identityLess))
	assert.Zero(t, countObservers(t, state, identified))
}
