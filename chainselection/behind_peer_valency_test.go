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
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tipAt builds a chainsync tip with a distinct hash per block number.
func behindPeerTipAt(blockNumber uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: blockNumber * 2,
			Hash: []byte("behind-peer-" + string(rune('a'+blockNumber%26))),
		},
		BlockNumber: blockNumber,
	}
}

// A block producer with a single configured upstream must keep that upstream
// usable while it is behind on our own chain. The intersect ladder resolves a
// peer this far back to a rung past K, which the chainsync layer used to treat
// as an over-K fork and evict; chain selection itself has no such notion, and
// this pins the invariant the eviction was breaking: one behind-but-inside-K
// peer stays registered and selectable, so the producer never reaches the
// "chain selection stalled: no selectable peer" state, and it is selected
// immediately once it overtakes us.
func TestValencyOneUpstreamBehindStaysSelectable(t *testing.T) {
	const securityParam = 108
	const localBlock = 115195
	// 65 blocks behind: the point at which the intersect ladder's first rung
	// past K (128 at K=108) starts manufacturing an over-K rollback, while
	// the peer is still well inside K.
	const behindBlock = localBlock - 65

	cs := NewChainSelector(ChainSelectorConfig{})
	cs.SetSecurityParam(securityParam)
	cs.SetLocalTip(behindPeerTipAt(localBlock))

	connId := newTestConnectionId(1)
	require.True(t, cs.UpdatePeerTip(connId, behindPeerTipAt(behindBlock), nil))

	require.Equal(t, 1, cs.PeerCount(), "the only upstream must be retained")
	best := cs.SelectBestChain()
	require.NotNil(
		t,
		best,
		"a sole upstream inside K must remain selectable, otherwise the "+
			"producer stalls with no selectable peer",
	)
	assert.Equal(t, connId, *best)

	// The peer catches up and overtakes us; it must be selected.
	require.True(t, cs.UpdatePeerTip(connId, behindPeerTipAt(localBlock+5), nil))
	cs.SetLocalTip(behindPeerTipAt(localBlock))
	best = cs.SelectBestChain()
	require.NotNil(t, best)
	assert.Equal(t, connId, *best)
}

// A sole upstream further behind than K is not worth syncing from, so chain
// selection declines to select it — but it must still be retained, because it
// is the only peer we have and it becomes selectable again the moment it
// catches back up to within K. Evicting it instead (the over-K chainsync
// denial) leaves the node with no upstream at all.
func TestValencyOneUpstreamBeyondKIsRetainedNotEvicted(t *testing.T) {
	const securityParam = 108
	const localBlock = 115195
	// The lag observed in the field: 119 blocks, past K.
	const behindBlock = localBlock - 119

	cs := NewChainSelector(ChainSelectorConfig{})
	cs.SetSecurityParam(securityParam)
	cs.SetLocalTip(behindPeerTipAt(localBlock))

	connId := newTestConnectionId(1)
	require.True(t, cs.UpdatePeerTip(connId, behindPeerTipAt(behindBlock), nil))

	assert.Equal(t, 1, cs.PeerCount(), "the only upstream must be retained")
	assert.Nil(
		t,
		cs.SelectBestChain(),
		"a peer more than K behind is not a useful sync source",
	)

	// It catches up to within K and becomes usable again without any
	// reconnect, denial cooldown, or operator intervention.
	require.True(
		t,
		cs.UpdatePeerTip(connId, behindPeerTipAt(localBlock-10), nil),
	)
	best := cs.SelectBestChain()
	require.NotNil(t, best)
	assert.Equal(t, connId, *best)
}
