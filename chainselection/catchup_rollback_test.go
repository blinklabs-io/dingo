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
	"bytes"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newReintersectSelector returns a selector whose applied tip is bandLocal,
// tracking conn with a delivered frontier at bandLocal+3, and whose
// own chain holds every header up to localChainTip. It counts the tips it
// refuses as implausible.
func newReintersectSelector(
	t *testing.T,
	conn ouroboros.ConnectionId,
	localChainTip uint64,
	refused *atomic.Int32,
) *ChainSelector {
	t.Helper()
	c := bandChain()
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam:             bandK,
		DisableEventSubscriptions: true,
		AppliedTip: func() (ochainsync.Tip, bool) {
			return c.tip(bandLocal), true
		},
		LocalPointBlockNumber: func(point ocommon.Point) (uint64, bool) {
			for block := uint64(1); block <= localChainTip; block++ {
				tip := c.tip(block)
				if tip.Point.Slot == point.Slot &&
					bytes.Equal(tip.Point.Hash, point.Hash) {
					return block, true
				}
			}
			return 0, false
		},
		OnPeerTipRejected: func(PeerTipRejection) { refused.Add(1) },
	})
	cs.SetLocalTip(c.tip(bandLocal))
	for block := uint64(bandLocal + 1); block <= bandLocal+3; block++ {
		require.True(t, cs.updatePeerTipObservedLinked(
			conn, c.tip(4_687_076), c.tip(block), nil,
			PraosTiebreakerView{},
			deliveredLink{prevHash: c.prevHash(block)},
		))
	}
	return cs
}

// A re-intersect lands on the node's own chain, whose header tip may lead the
// applied tip by more than twice K, beyond any header the peer delivered on
// this connection. The peer's next header continues from that point, so it is
// bounded by the point's block number: past the applied catch-up ceiling it
// is held until the ledger catches up and staged pending, never refused as
// implausible.
func TestReintersectBeyondCatchUpCeilingIsNotRefused(t *testing.T) {
	t.Parallel()

	c := bandChain()
	point := uint64(bandLocal + 5*bandK)
	var refused atomic.Int32
	conn := newTestConnectionId(2)
	cs := newReintersectSelector(t, conn, point, &refused)

	cs.HandlePeerRollbackEvent(event.Event{Data: PeerRollbackEvent{
		ConnectionId: conn,
		Point:        c.tip(point).Point,
		Tip:          c.tip(4_687_076),
	}})

	next := c.tip(point + 1)
	require.True(t, cs.catchUpBandHold(conn, next, false),
		"the band wait holds the next header until the ledger catches up")
	update := bandUpdate(point + 1)
	update.ConnectionId = conn
	require.Equal(t, PeerTipUpdatePending,
		cs.PreparePeerTipAdmissionDisposition(update),
		"a header past the applied ceiling is staged pending")
	require.Zero(t, refused.Load(),
		"an honest header continuing the re-intersect point is not refused")
}

// A rollback point the node's chain does not hold keeps an unknown block
// number, so the next header past the applied catch-up ceiling is still
// refused: only the node's own chain vouches for a point's block number.
func TestRollbackOffLocalChainBeyondCeilingStaysRefused(t *testing.T) {
	t.Parallel()

	c := bandChain()
	point := uint64(bandLocal + 5*bandK)
	var refused atomic.Int32
	conn := newTestConnectionId(2)
	cs := newReintersectSelector(t, conn, point-1, &refused)

	cs.HandlePeerRollbackEvent(event.Event{Data: PeerRollbackEvent{
		ConnectionId: conn,
		Point:        c.tip(point).Point,
		Tip:          c.tip(4_687_076),
	}})

	require.False(t, cs.catchUpBandHold(conn, c.tip(point+1), false))
	update := bandUpdate(point + 1)
	update.ConnectionId = conn
	cs.PreparePeerTipAdmissionDisposition(update)
	require.Equal(t, int32(1), refused.Load())
}

// A rollback point inside the peer's delivered history keeps the block
// number recorded there, whatever the node's chain reports.
func TestRollbackWithinDeliveredHistoryKeepsRecordedBlock(t *testing.T) {
	t.Parallel()

	c := bandChain()
	var refused atomic.Int32
	conn := newTestConnectionId(2)
	cs := newReintersectSelector(t, conn, bandLocal+5*bandK, &refused)

	cs.HandlePeerRollbackEvent(event.Event{Data: PeerRollbackEvent{
		ConnectionId: conn,
		Point:        c.tip(bandLocal + 2).Point,
		Tip:          c.tip(4_687_076),
	}})

	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	require.Equal(t, uint64(bandLocal+2),
		cs.peerTips[conn].SelectionTip().BlockNumber)
	require.Zero(t, refused.Load())
}
