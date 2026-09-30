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

	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChainSelectorByronEBBBeatsRegularIncumbentAtEqualBlockNumber exercises
// normal multi-peer selection: an incumbent peer
// on a Byron regular tip, and a second peer that delivers the EBB successor
// sharing the same protocol block number, the way Byron routes an EBB and
// its predecessor. Without the era-aware tiebreak this is exactly
// TestIncumbentAdvantageNoSwitchAtEqualBlockNumber's shape (Praos alone
// calls it ChainEqual and keeps the incumbent); the Byron EBB tiebreak must
// override that and switch to the peer with the boundary block.
func TestChainSelectorByronEBBBeatsRegularIncumbentAtEqualBlockNumber(
	t *testing.T,
) {
	cs := NewChainSelector(ChainSelectorConfig{})

	connId1 := newTestConnectionId(1)
	connId2 := newTestConnectionId(2)

	const blockNumber = 50

	mainHeader := &byron.ByronMainBlockHeader{}
	mainHeader.ConsensusData.Difficulty.Value = blockNumber
	mainView, ok := GetPraosTiebreakerView(mainHeader)
	require.False(t, ok, "Byron header has no Praos select view")

	accepted := cs.updatePeerTipObservedPraosView(
		connId1,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("regular-tip")},
			BlockNumber: blockNumber,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("regular-tip")},
			BlockNumber: blockNumber,
		},
		nil,
		mainView,
	)
	require.True(t, accepted)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId1, *cs.GetBestPeer(),
		"the first (and only) peer must be selected")

	ebbHeader := &byron.ByronEpochBoundaryBlockHeader{}
	ebbHeader.ConsensusData.Difficulty.Value = blockNumber
	ebbView, ok := GetPraosTiebreakerView(ebbHeader)
	require.False(t, ok, "Byron header has no Praos select view")

	accepted = cs.updatePeerTipObservedPraosView(
		connId2,
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 101, Hash: []byte("ebb-tip")},
			BlockNumber: blockNumber,
		},
		ochainsync.Tip{
			Point:       ocommon.Point{Slot: 101, Hash: []byte("ebb-tip")},
			BlockNumber: blockNumber,
		},
		nil,
		ebbView,
	)
	require.True(t, accepted)
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId2, *cs.GetBestPeer(),
		"the peer reporting the Byron EBB successor must win over the "+
			"regular-tip incumbent at the same block number")
}
