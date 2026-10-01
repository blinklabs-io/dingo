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
	ouroboros "github.com/blinklabs-io/gouroboros"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const loeTestK = 5

func loeTip(prefix string, block uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: block * 100,
			Hash: []byte(fmt.Sprintf("%s%d", prefix, block)),
		},
		BlockNumber: block,
	}
}

// feedLoEChain delivers blocks from..to one at a time, as chainsync does.
func feedLoEChain(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	prefix string,
	from, to uint64,
) {
	for block := from; block <= to; block++ {
		cs.UpdatePeerTip(connId, loeTip(prefix, block), nil)
	}
}

// newLoEScenario builds two candidates that agree up to block 10 and then
// fork: A runs to blockA, B to blockB. The local chain is at the fork point.
func newLoEScenario(
	genesis bool,
	blockA, blockB uint64,
) (*ChainSelector, ouroboros.ConnectionId, ouroboros.ConnectionId) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   genesis,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, blockA)
	feedLoEChain(cs, b, "b", 11, blockB)
	// Set after the peers deliver: a local tip within the Genesis window of
	// the best peer would leave Genesis mode, which is the cap's off switch.
	cs.SetLocalTip(loeTip("c", 10))
	return cs, a, b
}

func TestLimitOnEagernessCapsSelectionAtKPastIntersection(t *testing.T) {
	t.Parallel()
	cs, a, _ := newLoEScenario(true, 20, 12)
	require.NotNil(t, cs.SelectBestChain())

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	require.True(t, limit.Intersected)
	assert.Equal(t, uint64(1000), limit.Point.Slot)
	assert.Equal(t, uint64(10+loeTestK), limit.BlockNumber)

	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.Equal(t, uint64(10+loeTestK), tip.BlockNumber,
		"selection must stop k past the candidate intersection")
	assert.Equal(t, "a15", string(tip.Point.Hash))
	require.Equal(t, a, *cs.SelectBestChain())
}

func TestLimitOnEagernessDisabledSelectionAdvancesPastLimit(t *testing.T) {
	t.Parallel()
	cs, _, _ := newLoEScenario(false, 20, 12)
	require.NotNil(t, cs.SelectBestChain())

	assert.False(t, cs.EagernessLimit().Active)
	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.Equal(t, uint64(20), tip.BlockNumber,
		"with the cap off the same scenario advances past the limit")
}

func TestLimitOnEagernessSingleCandidateDoesNotWedge(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 40)
	cs.SetLocalTip(loeTip("c", 10))
	require.NotNil(t, cs.SelectBestChain())

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	assert.GreaterOrEqual(t, limit.BlockNumber, uint64(40))
	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.Equal(t, uint64(40), tip.BlockNumber)
}

func TestLimitOnEagernessWithoutCommonPointFallsBackToLocalTip(t *testing.T) {
	t.Parallel()
	// A runs far past the fork, so its retained fragment no longer holds the
	// fork point and shares no point with B.
	cs, _, _ := newLoEScenario(true, 40, 12)
	require.NotNil(t, cs.SelectBestChain())

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	assert.False(t, limit.Intersected)
	assert.Equal(t, uint64(10+loeTestK), limit.BlockNumber)
	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.LessOrEqual(t, tip.BlockNumber, limit.BlockNumber)
}

func TestLimitOnEagernessCandidatesBeyondLimitKeepIncumbent(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	// The incumbent has the higher connection ID, so the connection-ID
	// tiebreak alone would displace it.
	a := newTestConnectionId(2)
	b := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 16)
	cs.SetLocalTip(loeTip("c", 10))
	cs.EvaluateAndSwitch()
	require.Equal(t, a, *cs.GetBestPeer())

	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, b, "b", 11, 19)
	cs.EvaluateAndSwitch()

	limit := cs.EagernessLimit()
	require.True(t, limit.Intersected)
	require.Equal(t, uint64(10+loeTestK), limit.BlockNumber)
	require.Equal(t, a, *cs.GetBestPeer(),
		"candidates equal under the limit must not displace the incumbent")
}
