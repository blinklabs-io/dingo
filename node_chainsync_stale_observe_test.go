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

package dingo

import (
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chainselection"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func staleObserveConnId() ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("10.0.0.1"), Port: 3001},
	}
}

// newStaleObserveNode returns a node whose selector runs in Genesis mode with a
// wall clock at wall and the default corroboration threshold of zero.
func newStaleObserveNode(genesis bool, wall uint64) *Node {
	return &Node{
		chainSelector: chainselection.NewChainSelector(
			chainselection.ChainSelectorConfig{
				GenesisMode:        genesis,
				SecurityParam:      20,
				GenesisWindowSlots: 100,
				WallClockSlot: func() (uint64, bool) {
					return wall, true
				},
				DisableEventSubscriptions: true,
			},
		),
	}
}

// With corroboration off (the default), the apply gate still depends on the
// peer's advertised tip, so the observation must land before the gate runs;
// an asynchronous observation lets a stale peer's first header through.
func TestChainsyncObservePeerTipSyncWhenStaleRuleActive(t *testing.T) {
	t.Parallel()
	n := newStaleObserveNode(true, 50000)
	conn := staleObserveConnId()
	require.False(t, n.chainSelector.GenesisCorroborationActive(),
		"fixture: corroboration must be off")

	disposition := n.chainsyncObservePeerTip(chainselection.PeerTipUpdateEvent{
		ConnectionId: conn,
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("stale-tip")),
			BlockNumber: 2,
		},
		ObservedTip: ochainsync.Tip{
			Point:       ocommon.NewPoint(100, []byte("hdr")),
			BlockNumber: 1,
		},
	})

	assert.NotEqual(t, chainselection.PeerTipUpdateUnhandled, disposition,
		"tip observation was left asynchronous while the stale rule is active")
	assert.False(t, n.chainsyncApplyEligible(conn),
		"a wall-clock-stale peer's header passes the apply gate")
}

func TestChainsyncObservePeerRollbackSyncWhenStaleRuleActive(t *testing.T) {
	t.Parallel()
	n := newStaleObserveNode(true, 50000)
	conn := staleObserveConnId()

	handled := n.chainsyncObservePeerRollback(chainselection.PeerRollbackEvent{
		ConnectionId: conn,
		Point:        ocommon.NewPoint(100, []byte("hdr")),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("stale-tip")),
			BlockNumber: 2,
		},
	})

	assert.True(t, handled,
		"rollback observation was left asynchronous while the stale rule is active")
	assert.False(t, n.chainsyncApplyEligible(conn),
		"a wall-clock-stale peer's rollback passes the apply gate")
}

// Outside Genesis mode nothing orders observation, so the throughput-friendly
// asynchronous path is kept.
func TestChainsyncObservePeerTipAsyncOutsideGenesis(t *testing.T) {
	t.Parallel()
	n := newStaleObserveNode(false, 50000)
	disposition := n.chainsyncObservePeerTip(chainselection.PeerTipUpdateEvent{
		ConnectionId: staleObserveConnId(),
	})
	assert.Equal(t, chainselection.PeerTipUpdateUnhandled, disposition)
	handled := n.chainsyncObservePeerRollback(chainselection.PeerRollbackEvent{
		ConnectionId: staleObserveConnId(),
	})
	assert.False(t, handled)
}
