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
	"sync"
	"testing"
	"time"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A chain that produces no block for longer than catchUpPinStallTimeout is
// quiet, not stalled: the peer that delivers the next block a moment before
// the incumbent must not take the active connection from it.
func TestPinHoldsWhenNextBlockFollowsQuietChain(t *testing.T) {
	t.Parallel()

	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "b5000"))
	cs.UpdatePeerTip(incumbent, tip(5000, 5000, "b5000"), nil)
	cs.UpdatePeerTip(challenger, tip(5000, 5000, "b5000"), nil)
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// The evaluation loop keeps running while no block is produced.
	for range 2 {
		clk.Advance(defaultEvaluationInterval)
		cs.EvaluateAndSwitch()
	}
	clk.Advance(5 * time.Second)

	cs.UpdatePeerTip(challenger, tip(5001, 5025, "b5001"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"a one-block head start after a quiet spell must not release the pin")

	cs.UpdatePeerTip(incumbent, tip(5001, 5025, "b5001"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer())
}

// The stall clock follows the applied tip as each evaluation reads it, not
// only as SetLocalTip last sampled it: a block applied since the last sample
// is progress.
func TestPinStallClockFollowsAppliedTipBetweenSamples(t *testing.T) {
	t.Parallel()

	var appliedMu sync.Mutex
	applied := tip(5000, 5000, "b5000")
	setApplied := func(next ochainsync.Tip) {
		appliedMu.Lock()
		applied = next
		appliedMu.Unlock()
	}

	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{
		AppliedTip: func() (ochainsync.Tip, bool) {
			appliedMu.Lock()
			defer appliedMu.Unlock()
			return applied, true
		},
	})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "b5000"))
	cs.UpdatePeerTip(incumbent, tip(5001, 5001, "b5001"), nil)
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Block 5001 is applied, but SetLocalTip is not called again.
	clk.Advance(15 * time.Second)
	setApplied(tip(5001, 5001, "b5001"))
	cs.UpdatePeerTip(challenger, tip(5002, 5015, "b5002"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// Twenty-two seconds after the sample, seven after the applied block.
	clk.Advance(7 * time.Second)
	cs.UpdatePeerTip(challenger, tip(5003, 5022, "b5003"), nil)
	assert.Equal(t, incumbent, *cs.GetBestPeer(),
		"a block applied since the last SetLocalTip must re-arm the stall clock")
}

// Restarting the stall clock while nothing waits to be applied must not
// keep the pin on an incumbent that stops delivering: once a challenger is
// ahead and nothing is applied for catchUpPinStallTimeout, it takes over.
func TestPinStallEscapeAfterQuietChainStillReleasesSilentIncumbent(
	t *testing.T,
) {
	t.Parallel()

	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "b5000"))
	cs.UpdatePeerTip(incumbent, tip(5000, 5000, "b5000"), nil)
	cs.UpdatePeerTip(challenger, tip(5000, 5000, "b5000"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	clk.Advance(defaultEvaluationInterval)
	cs.EvaluateAndSwitch()

	// The incumbent never delivers 5001.
	clk.Advance(time.Second)
	cs.UpdatePeerTip(challenger, tip(5001, 5011, "b5001"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	clk.Advance(catchUpPinStallTimeout - 2*time.Second)
	cs.EvaluateAndSwitch()
	require.Equal(t, incumbent, *cs.GetBestPeer(),
		"must stay pinned until the challenger has been ahead for the timeout")

	clk.Advance(2 * time.Second)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"a silent incumbent must still be left once nothing is applied for the timeout")
}

// In Genesis mode a challenger serving headers nobody else has delivered is
// not a trusted frontier, yet it is all the node can apply next. The stall
// clock must keep running while it waits, or an incumbent that stopped
// delivering stays pinned for as long as it keeps its connection.
func TestPinStallEscapeReleasesForUncorroboratedGenesisChallenger(
	t *testing.T,
) {
	t.Parallel()

	clk := &fakeClock{now: time.Unix(1_700_000_000, 0)}
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:       true,
		SecurityParam:     2160,
		StaleTipThreshold: time.Hour,
	})
	installFakeClock(cs, clk)

	incumbent := newTestConnectionId(1)
	challenger := newTestConnectionId(2)

	cs.SetLocalTip(tip(5000, 5000, "b5000"))
	cs.UpdatePeerTip(incumbent, tip(5000, 5000, "b5000"), nil)
	cs.UpdatePeerTip(challenger, tip(5000, 5000, "b5000"), nil)
	require.NotNil(t, cs.GetBestPeer())
	require.Equal(t, incumbent, *cs.GetBestPeer())

	// The incumbent never delivers 5001.
	clk.Advance(time.Second)
	cs.UpdatePeerTip(challenger, tip(5001, 5001, "b5001"), nil)
	require.Equal(t, incumbent, *cs.GetBestPeer())

	for range 5 {
		clk.Advance(5 * time.Second)
		cs.EvaluateAndSwitch()
	}
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, challenger, *cs.GetBestPeer(),
		"an uncorroborated frontier past the applied tip is still pending work")
}
