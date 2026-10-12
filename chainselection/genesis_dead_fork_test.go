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
	"math"
	"testing"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// feedAdvertising delivers one header per step slots on a named chain while the
// peer keeps advertising adv as its tip.
func feedAdvertising(
	t *testing.T,
	cs *ChainSelector,
	conn ouroboros.ConnectionId,
	adv ochainsync.Tip,
	chain string,
	start, end, step uint64,
) {
	t.Helper()
	for slot := start; slot <= end; slot += step {
		require.True(t, cs.updatePeerTipObserved(
			conn,
			adv,
			genesisTip(slot, fmt.Sprintf("%s%d", chain, slot), slot/10),
			nil,
		))
	}
}

const (
	deadForkHonestPeers = 4
	// Honest peers advertise a chain far longer than the dead fork's.
	deadForkHonestTipSlot = uint64(50000)
)

// newGenesisWallClockSelector returns a Genesis selector whose wall clock reads
// wallSlot.
func newGenesisWallClockSelector(wallSlot uint64) *ChainSelector {
	return NewChainSelector(ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      20,
		GenesisWindowSlots: 100,
		WallClockSlot: func() (uint64, bool) {
			return wallSlot, true
		},
		DisableEventSubscriptions: true,
	})
}

func newDeadForkSelector(t *testing.T, localTip ochainsync.Tip) (
	*ChainSelector,
	ouroboros.ConnectionId,
	[]ouroboros.ConnectionId,
) {
	t.Helper()
	// The honest peers advertise tips at the wall clock; the dead fork's
	// last block is far older.
	cs := newGenesisWallClockSelector(deadForkHonestTipSlot)
	// A local tip exists, so advertised tips are not bounded by the first
	// peer's: honest peers legitimately advertise far ahead while catching up.
	cs.SetLocalTip(localTip)
	dead := corrConn(1)
	// The dead fork's last block: advertised and fully delivered.
	feedAdvertising(
		t, cs, dead,
		genesisTip(2000, "dead-tip", 200),
		"d", 1000, 1900, 10,
	)
	var honest []ouroboros.ConnectionId
	for i := range deadForkHonestPeers {
		conn := corrConn(10 + i)
		honest = append(honest, conn)
		feedAdvertising(
			t, cs, conn,
			genesisTip(
				deadForkHonestTipSlot,
				"honest-tip",
				deadForkHonestTipSlot/10,
			),
			"h", 1000, 1200, 10,
		)
	}
	return cs, dead, honest
}

// A peer that has delivered its whole short fork leads on observed density,
// but its advertised tip is older than the wall clock and far behind the tip
// the other peers advertise. It
// must not be selected over peers still streaming the longer chain.
func TestGenesisSelectionRejectsPeerAdvertisingFarBehindTips(t *testing.T) {
	t.Parallel()
	cs, dead, honest := newDeadForkSelector(t, genesisTip(1000, "local", 100))

	best := cs.SelectBestChain()
	require.NotNil(t, best)
	assert.NotEqual(t, dead, *best,
		"a peer advertising a tip far behind the other peers was selected")
	assert.Contains(t, honest, *best)
}

// The stale peer's advertised block number is unverified. Advertising an old
// slot with an inflated block number must not lift it above the fresh peers'
// tips and keep it selectable; only its delivered frontier counts.
func TestGenesisSelectionRejectsStalePeerInflatingBlockNumber(t *testing.T) {
	t.Parallel()
	cs, dead, honest := newDeadForkSelector(t, genesisTip(1000, "local", 100))
	feedAdvertising(
		t, cs, dead,
		genesisTip(2000, "dead-tip", math.MaxUint64),
		"d", 1910, 1910, 10,
	)

	best := cs.SelectBestChain()
	require.NotNil(t, best)
	assert.NotEqual(t, dead, *best,
		"a stale peer inflating its advertised block number was selected")
	assert.Contains(t, honest, *best)
}

// A corroborated dead-fork frontier must not make the honest peers look more
// than k blocks behind: a second connection delivering the same fork lets the
// frontier be trusted, and the honest peers must stay selectable.
func TestGenesisCorroboratedDeadForkDoesNotExcludeHonestPeers(t *testing.T) {
	t.Parallel()
	cs, dead, honest := newDeadForkSelector(t, genesisTip(1000, "local", 100))
	feedAdvertising(
		t, cs, corrConn(2),
		genesisTip(2000, "dead-tip", 200),
		"d", 1000, 1900, 10,
	)
	require.True(t, cs.frontierTrusted(dead),
		"fixture: the dead-fork frontier must be corroborated")

	best := cs.SelectBestChain()
	require.NotNil(t, best,
		"honest peers were excluded as behind the dead-fork frontier")
	assert.Contains(t, honest, *best)
}

// Genesis mode must not exit at a horizon only a stale-tipped peer supports
// while the other peers advertise fresh tips beyond it.
func TestGenesisExitIgnoresStaleTipPeer(t *testing.T) {
	t.Parallel()
	cs, _, _ := newDeadForkSelector(t, genesisTip(1000, "local", 100))
	cs.SetLocalTip(genesisTip(1900, "local", 190))

	assert.Equal(t, SelectionModeGenesis, cs.SelectionMode(),
		"Genesis exited at a horizon only the dead-fork peer supports")
}

// Control: when the peers agree on the network tip, no peer is stale and the
// exit behavior is unchanged.
func TestGenesisExitWhenPeersAgreeOnTip(t *testing.T) {
	t.Parallel()
	cs := newGenesisWallClockSelector(2000)
	cs.SetLocalTip(genesisTip(1000, "local", 100))
	for i := range deadForkHonestPeers {
		feedAdvertising(
			t, cs, corrConn(10+i),
			genesisTip(2000, "tip", 200),
			"c", 1000, 1900, 10,
		)
	}
	cs.SetLocalTip(genesisTip(1900, "local", 190))
	assert.Equal(t, SelectionModePraos, cs.SelectionMode())
}

const colluderTipBlock = uint64(90000)

// newColluderSelector models one honest peer and two connections that deliver
// only the first headers of the real chain while advertising an inflated tip.
// The honest peer's tip is at the wall clock, so no claim by the colluders can
// make it stale.
func newColluderSelector(
	t *testing.T,
	honestDeliveredTo uint64,
) (*ChainSelector, ouroboros.ConnectionId) {
	t.Helper()
	cs := newGenesisWallClockSelector(2000)
	cs.SetLocalTip(genesisTip(1000, "local", 100))
	honest := corrConn(10)
	feedAdvertising(
		t, cs, honest, genesisTip(2000, "tip", 200),
		"h", 1000, honestDeliveredTo, 10,
	)
	for i := range 2 {
		feedAdvertising(
			t, cs, corrConn(20+i),
			genesisTip(2000, "lie", colluderTipBlock),
			"h", 1000, 1050, 10,
		)
	}
	return cs, honest
}

// Connections that advertise an inflated tip and deliver a few real headers
// must not push the one honest peer out of selection.
func TestGenesisSelectionKeepsHonestPeerAgainstColludingTips(t *testing.T) {
	t.Parallel()
	cs, honest := newColluderSelector(t, 1500)

	best := cs.SelectBestChain()
	require.NotNil(t, best)
	assert.Equal(t, honest, *best,
		"colluding advertised tips displaced the honest peer")
}

// The same colluders must not remove the honest peer from the Genesis exit
// horizon: once the local tip reaches the honest peer's tip, Genesis exits.
func TestGenesisExitReachedAgainstColludingTips(t *testing.T) {
	t.Parallel()
	cs, _ := newColluderSelector(t, 2000)
	cs.SetLocalTip(genesisTip(1990, "local", 199))

	assert.Equal(t, SelectionModePraos, cs.SelectionMode(),
		"colluding advertised tips kept Genesis from exiting")
}

// newSoleStalePeerSelector returns a selector whose only candidate is a peer
// that advertises a tip more than one Genesis window older than the wall clock.
func newSoleStalePeerSelector(t *testing.T) (
	*ChainSelector,
	ouroboros.ConnectionId,
) {
	t.Helper()
	cs := newGenesisWallClockSelector(deadForkHonestTipSlot)
	cs.SetLocalTip(genesisTip(1000, "local", 100))
	dead := corrConn(1)
	feedAdvertising(
		t, cs, dead,
		genesisTip(2000, "dead-tip", 200),
		"d", 1000, 1900, 10,
	)
	return cs, dead
}

// A wall-clock-stale peer must not be selected even when no other peer is
// available: selection waits instead of following a chain the network has
// stopped extending.
func TestGenesisSelectionWaitsWhenOnlyPeerIsWallClockStale(t *testing.T) {
	t.Parallel()
	cs, dead := newSoleStalePeerSelector(t)

	best := cs.SelectBestChain()
	assert.Nil(t, best,
		"the sole wall-clock-stale peer was selected: %v", best)
	assert.False(t, cs.EvaluateAndSwitch())
	assert.Nil(t, cs.GetBestPeer())

	// A fresh peer arriving later is selected, not the stale one.
	fresh := corrConn(10)
	feedAdvertising(
		t, cs, fresh,
		genesisTip(deadForkHonestTipSlot, "honest-tip", 5000),
		"h", 1000, 1200, 10,
	)
	best = cs.SelectBestChain()
	require.NotNil(t, best)
	assert.Equal(t, fresh, *best)
	assert.NotEqual(t, dead, *best)
}

// When the last honest peer disconnects, the stale peer must not be picked up
// by the after-disconnect selection.
func TestGenesisRemovePeerDoesNotSelectSoleWallClockStalePeer(t *testing.T) {
	t.Parallel()
	cs, dead := newSoleStalePeerSelector(t)
	honest := corrConn(10)
	feedAdvertising(
		t, cs, honest,
		genesisTip(deadForkHonestTipSlot, "honest-tip", 5000),
		"h", 1000, 1200, 10,
	)
	best := cs.GetBestPeer()
	require.NotNil(t, best)
	require.Equal(t, honest, *best, "fixture: honest peer must be selected")

	cs.RemovePeer(honest)

	best = cs.GetBestPeer()
	assert.Nil(t, best,
		"the stale peer %v was selected after the honest peer left", dead)
}

// The apply gate keeps a wall-clock-stale peer's headers out of the ledger's
// shared header queue; otherwise they remain queued after selection moves to
// honest peers and make those peers' headers look like a persistent fork.
func TestGenesisShouldApplyIngressRejectsWallClockStalePeer(t *testing.T) {
	t.Parallel()
	cs, dead, honest := newDeadForkSelector(t, genesisTip(1000, "local", 100))
	assert.False(t, cs.ShouldApplyIngress(dead),
		"headers from a wall-clock-stale peer are admitted to the ledger")
	// Control: honest peers are unaffected.
	assert.True(t, cs.ShouldApplyIngress(honest[0]))
	// Unknown peers are not gated by staleness.
	assert.True(t, cs.ShouldApplyIngress(corrConn(99)))
}

// Control: the gate is inactive without a wall clock and outside Genesis mode.
func TestShouldApplyIngressStaleRuleInactiveWithoutGenesisClock(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]ChainSelectorConfig{
		"no wall clock": {
			GenesisMode:        true,
			SecurityParam:      20,
			GenesisWindowSlots: 100,
		},
		"clock without slot": {
			GenesisMode:        true,
			SecurityParam:      20,
			GenesisWindowSlots: 100,
			WallClockSlot: func() (uint64, bool) {
				return 0, false
			},
		},
		"praos mode": {
			SecurityParam: 20,
			WallClockSlot: func() (uint64, bool) {
				return deadForkHonestTipSlot, true
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg.DisableEventSubscriptions = true
			cs := NewChainSelector(cfg)
			cs.SetLocalTip(genesisTip(1000, "local", 100))
			dead := corrConn(1)
			feedAdvertising(
				t, cs, dead,
				genesisTip(2000, "dead-tip", 200),
				"d", 1000, 1900, 10,
			)
			assert.True(t, cs.ShouldApplyIngress(dead))
			best := cs.SelectBestChain()
			require.NotNil(t, best)
			assert.Equal(t, dead, *best)
		})
	}
}
