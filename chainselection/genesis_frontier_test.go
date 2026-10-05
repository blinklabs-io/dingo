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

package chainselection

import (
	"fmt"
	"sync/atomic"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// feedChain delivers one header per step slots on a named chain from start to
// end inclusive, with block numbers advancing at one tenth the slot rate.
func feedChain(
	cs *ChainSelector,
	conn ouroboros.ConnectionId,
	chain string,
	start, end, step uint64,
) {
	for slot := start; slot <= end; slot += step {
		cs.UpdatePeerTip(
			conn,
			genesisTip(slot, fmt.Sprintf("%s%d", chain, slot), slot/10),
			nil,
		)
	}
}

func syncTargetAvailable(cs *ChainSelector, conn ouroboros.ConnectionId) bool {
	_, ok := cs.GetPeerSyncTarget(conn)
	return ok
}

// A frontier no other peer confirms must not make other candidates look
// behind, and the default configuration (no corroboration threshold) must hold
// the same line.
func TestGenesisBehindExclusionIgnoresUncorroboratedFrontier(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      20,
		GenesisWindowSlots: 100,
	})
	fast := corrConn(1)
	honest := corrConn(2)
	feedChain(cs, fast, "a", 910, 1000, 10)
	feedChain(cs, honest, "b", 580, 600, 10)

	assert.True(
		t,
		syncTargetAvailable(cs, honest),
		"an uncorroborated frontier must not exclude the honest candidate",
	)

	// Control: once an independent peer confirms the fast chain its frontier
	// is trusted and the lagging candidate is behind it.
	witness := corrConn(3)
	feedChain(cs, witness, "a", 910, 1000, 10)
	assert.True(t, syncTargetAvailable(cs, fast))
	assert.False(
		t,
		syncTargetAvailable(cs, honest),
		"a corroborated frontier must still exclude a candidate k behind it",
	)
}

// A witness must independently deliver the candidate's current point. Sharing
// an older point does not authorize the candidate's unobserved suffix.
func TestGenesisWitnessRequiresCandidateTip(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      20,
		GenesisWindowSlots: 100,
	})
	fast := corrConn(1)
	stale := corrConn(2)
	feedChain(cs, fast, "a", 910, 1000, 10)
	feedChain(cs, stale, "a", 910, 910, 10)
	cs.mutex.Lock()
	// Keepalive traffic keeps the witness fresh without advancing its chain.
	cs.peerTips[stale].Touch()
	staleTip := cs.peerTips[stale]
	fastTip := cs.peerTips[fast]
	confirms := fastTip.confirmsRecentChain(staleTip)
	cs.mutex.Unlock()
	require.False(
		t,
		confirms,
		"a shared ancestor must not authorize the candidate suffix",
	)

	assert.Zero(t, cs.corroboratingPeers(fast))
	assert.False(t, cs.frontierTrusted(fast))

	near := corrConn(4)
	feedChain(cs, near, "a", 910, 990, 10)
	assert.Zero(t, cs.corroboratingPeers(fast))
	assert.False(t, cs.frontierTrusted(fast))

	current := corrConn(5)
	feedChain(cs, current, "a", 910, 1000, 10)
	assert.Equal(t, 1, cs.corroboratingPeers(fast))
	assert.True(t, cs.frontierTrusted(fast))
}

// A frontier whose connection is gone must not exclude live candidates, even
// when peers still on record confirm it.
func TestGenesisBehindExclusionIgnoresUnavailableFrontier(t *testing.T) {
	t.Parallel()
	fast := corrConn(1)
	var fastLive atomic.Bool
	fastLive.Store(true)
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      20,
		GenesisWindowSlots: 100,
		ConnectionLive: func(c ouroboros.ConnectionId) bool {
			return c != fast || fastLive.Load()
		},
	})
	witness := corrConn(2)
	honest := corrConn(3)
	feedChain(cs, fast, "a", 910, 1000, 10)
	feedChain(cs, witness, "a", 910, 1000, 10)
	feedChain(cs, honest, "b", 580, 600, 10)
	require.False(t, syncTargetAvailable(cs, honest))

	fastLive.Store(false)
	assert.True(
		t,
		syncTargetAvailable(cs, honest),
		"an unavailable frontier must not exclude live candidates",
	)
}

// With the corroboration gate on, an uncorroborated high frontier is denied
// selection and must not also exclude the honest roots that corroborate each
// other, or the node has nothing to select.
func TestGenesisRecoversOntoHonestRootBehindUncorroboratedFrontier(
	t *testing.T,
) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		GenesisWindowSlots:    100,
		MinCorroboratingPeers: 1,
	})
	fast := corrConn(1)
	rootA := corrConn(2)
	rootB := corrConn(3)
	feedChain(cs, fast, "a", 910, 1000, 10)
	feedChain(cs, rootA, "b", 540, 600, 10)
	feedChain(cs, rootB, "b", 540, 600, 10)

	cs.EvaluateAndSwitch()
	best := cs.GetBestPeer()
	require.NotNil(t, best, "honest roots must be selectable")
	assert.NotEqual(t, fast, *best)
}

// Witnesses are grouped by the identity the configured resolver reports, so
// distinct addresses of one operator are a single witness.
func TestGenesisWitnessIdentityUsesPeerIdentity(t *testing.T) {
	t.Parallel()
	fast := corrConn(1)
	sharedA := corrConn(2)
	sharedB := corrConn(3)
	other := corrConn(4)
	operator := map[ouroboros.ConnectionId]string{
		fast:    "group:fast",
		sharedA: "group:shared",
		sharedB: "group:shared",
		other:   "group:other",
	}
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      20,
		GenesisWindowSlots: 100,
		PeerIdentity: func(c ouroboros.ConnectionId) string {
			return operator[c]
		},
	})
	for conn := range operator {
		feedChain(cs, conn, "a", 910, 1000, 10)
	}
	assert.Equal(t, 2, cs.corroboratingPeers(fast))

	operator[other] = "group:fast"
	assert.Equal(
		t,
		1,
		cs.corroboratingPeers(fast),
		"a witness in the candidate's own group is not independent",
	)
}

func TestGenesisSelectionComputesBestTrustedFrontierOnce(t *testing.T) {
	t.Parallel()
	const peerCount = 16
	var identityCalls atomic.Uint64
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		GenesisWindowSlots:    100,
		MinCorroboratingPeers: 1,
		PeerIdentity: func(conn ouroboros.ConnectionId) string {
			identityCalls.Add(1)
			return conn.String()
		},
	})
	for i := range peerCount {
		conn := corrConn(i + 1)
		feedChain(cs, conn, "shared", 910, 1000, 10)
	}

	cs.mutex.Lock()
	identityCalls.Store(0)
	best := cs.selectBestChainLocked()
	calls := identityCalls.Load()
	cs.mutex.Unlock()

	require.NotNil(t, best)
	// Genesis-mode horizon checking and candidate selection each need one
	// candidate corroboration pass. Recomputing the trusted frontier for every
	// candidate would exceed this quadratic bound.
	maxCalls := uint64(peerCount * peerCount * 3)
	assert.LessOrEqual(t, calls, maxCalls)
}

func (cs *ChainSelector) frontierTrusted(conn ouroboros.ConnectionId) bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.frontierTrustedLocked(conn, cs.peerTips[conn])
}

func (cs *ChainSelector) corroboratingPeers(conn ouroboros.ConnectionId) int {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.corroboratingPeersLocked(conn, cs.peerTips[conn])
}
