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
	"math"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenesisWindowSlotsForParams(t *testing.T) {
	assert.Equal(t, uint64(129600), GenesisWindowSlotsForParams(2160, 0.05))
	assert.Equal(t, defaultGenesisWindowSlots, GenesisWindowSlotsForParams(0, 0.05))
	assert.Equal(t, defaultGenesisWindowSlots, GenesisWindowSlotsForParams(2160, 0))
	assert.Equal(
		t,
		defaultGenesisWindowSlots,
		GenesisWindowSlotsForParams(2160, math.NaN()),
	)
}

func TestChainSelectorGenesisObservedDensityTracksRollingWindow(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: 10,
	})
	require.Equal(t, SelectionModeGenesis, cs.SelectionMode())
	require.Equal(t, uint64(30), cs.GenesisWindowSlots())

	connId := newTestConnectionId(1)
	for _, tip := range []ochainsync.Tip{
		{
			Point:       ocommon.Point{Slot: 1, Hash: []byte("slot-1")},
			BlockNumber: 1,
		},
		{
			Point:       ocommon.Point{Slot: 6, Hash: []byte("slot-6")},
			BlockNumber: 2,
		},
		{
			Point:       ocommon.Point{Slot: 12, Hash: []byte("slot-12")},
			BlockNumber: 3,
		},
	} {
		cs.UpdatePeerTip(connId, tip, nil)
	}

	peerTip := cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, []uint64{1, 6, 12}, peerTip.observedSlots)
	assert.Equal(t, uint64(3), peerTip.observedDensity(cs.GenesisWindowSlots()))

	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 41, Hash: []byte("slot-41")},
		BlockNumber: 4,
	}, nil)

	peerTip = cs.GetPeerTip(connId)
	require.NotNil(t, peerTip)
	assert.Equal(t, []uint64{12, 41}, peerTip.observedSlots)
	assert.Equal(t, uint64(2), peerTip.observedDensity(cs.GenesisWindowSlots()))
}

func TestChainSelectorGenesisPrefersObservedDensity(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: 20,
	})

	denseConn := newTestConnectionId(1)
	sparseConn := newTestConnectionId(2)

	for _, tip := range []ochainsync.Tip{
		{
			Point:       ocommon.Point{Slot: 90, Hash: []byte("dense-90")},
			BlockNumber: 90,
		},
		{
			Point:       ocommon.Point{Slot: 95, Hash: []byte("dense-95")},
			BlockNumber: 95,
		},
		{
			Point:       ocommon.Point{Slot: 100, Hash: []byte("dense-100")},
			BlockNumber: 100,
		},
	} {
		cs.UpdatePeerTip(denseConn, tip, nil)
	}

	cs.UpdatePeerTip(sparseConn, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 102, Hash: []byte("sparse-102")},
		BlockNumber: 102,
	}, nil)

	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, denseConn, *cs.GetBestPeer())

	denseTip := cs.GetPeerTip(denseConn)
	sparseTip := cs.GetPeerTip(sparseConn)
	require.NotNil(t, denseTip)
	require.NotNil(t, sparseTip)
	assert.Equal(t, uint64(3), denseTip.observedDensity(cs.GenesisWindowSlots()))
	assert.Equal(t, uint64(1), sparseTip.observedDensity(cs.GenesisWindowSlots()))
}

func TestChainSelectorGenesisTransitionsBackToPraos(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: 10,
	})

	connId := newTestConnectionId(1)
	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("peer")},
		BlockNumber: 100,
	}, nil)

	require.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 60, Hash: []byte("local-60")},
		BlockNumber: 60,
	})
	require.Equal(t, SelectionModeGenesis, cs.SelectionMode())

	cs.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 75, Hash: []byte("local-75")},
		BlockNumber: 75,
	})
	require.Equal(t, SelectionModePraos, cs.SelectionMode())
	require.NotNil(t, cs.GetBestPeer())
	assert.Equal(t, connId, *cs.GetBestPeer())
}
