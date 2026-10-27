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

package ledger

import (
	"context"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
)

// peerTipAhead is the tip a peer far ahead of a syncing node advertises: it
// says nothing about where the peer's chain meets ours.
func (f *behindPeerFixture) peerTipAhead() ochainsync.Tip {
	tip := f.tipAtDepth(0)
	return ochainsync.Tip{
		Point: ocommon.NewPoint(
			tip.Point.Slot+100_000,
			testHashBytes("opening-rollback-peer-tip"),
		),
		BlockNumber: tip.BlockNumber + 5_000,
	}
}

func (f *behindPeerFixture) sendChainsync(e ChainsyncEvent) {
	e.ConnectionId = f.connId
	f.ls.handleEventChainsync(event.NewEvent(ChainsyncEventType, e))
}

func (f *behindPeerFixture) sendOpeningRollback(depth int) {
	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(depth),
		Tip:      f.peerTipAhead(),
	})
}

// sendHeader delivers a roll-forward header built on the block at
// parentDepth. With hashSeed empty it is the block our chain holds there.
func (f *behindPeerFixture) sendHeader(parentDepth int, hashSeed string) {
	parent := f.tipAtDepth(parentDepth)
	hash := f.pointAtDepth(parentDepth - 1).Hash
	if hashSeed != "" {
		hash = testHashBytes(hashSeed)
	}
	point := ocommon.NewPoint(parent.Point.Slot+10, hash)
	f.sendChainsync(ChainsyncEvent{
		Point: point,
		Tip:   f.peerTipAhead(),
		BlockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(parent.Point.Hash),
			blockNumber: parent.BlockNumber + 1,
			slot:        point.Slot,
		},
		BlockNumber: parent.BlockNumber + 1,
	})
}

// A peer that reconnects intersects at our tip as of its FindIntersect. Our
// tip moves on before its opening MsgRollBackward arrives, so that rollback
// names a point on our own chain behind our applied tip. It must move only the
// peer's cursor: rolling our chain back to it undid applied blocks, published a
// local-rollback recovery that closed every connection, and broke downstream
// chain iterators. The peer then replays the block we already hold.
func TestOpeningRollbackBehindOurTipKeepsOurChain(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	f.sendOpeningRollback(1)
	require.Equal(
		t,
		localTip,
		f.ls.chain.Tip(),
		"a session's opening rollback to a point on our chain must not roll our chain back",
	)

	f.sendHeader(1, "")
	assert.Equal(
		t,
		localTip,
		f.ls.chain.Tip(),
		"a peer replaying our own chain must not roll it back",
	)
	assert.Equal(t, SyncingChainsyncState, f.ls.chainsyncState)
	assert.Empty(t, f.ls.chainsyncOpeningRollbacks)
	f.requireNoResyncEvent(t)
}

// The deferral must not change fork following: a peer whose first header
// leaves our chain at its intersection gets the rollback it asked for.
func TestOpeningRollbackAppliedWhenPeerLeavesOurChain(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	ancestor := f.pointAtDepth(2)

	f.sendOpeningRollback(2)
	f.sendHeader(2, "opening-rollback-fork-block")

	assert.Equal(
		t,
		ancestor,
		f.ls.chain.Tip().Point,
		"a peer diverging at its intersection must roll our chain back to it",
	)
	assert.Empty(t, f.ls.chainsyncOpeningRollbacks)
}

// The ingress apply gate can withhold a session's headers until the peer is
// selected, so its first header reaching the ledger need not build on the
// rollback point. Such a header carries no evidence that the peer's chain
// leaves ours there, and the rollback must not undo our chain for it.
func TestOpeningRollbackDroppedWhenFirstHeaderSkipsAhead(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()

	f.sendOpeningRollback(3)
	unknownParent := testHashBytes("opening-rollback-withheld-parent")
	point := ocommon.NewPoint(localTip.Point.Slot+20, testHashBytes("opening-rollback-ahead"))
	f.sendChainsync(ChainsyncEvent{
		Point: point,
		Tip:   f.peerTipAhead(),
		BlockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256(point.Hash),
			prevHash:    lcommon.NewBlake2b256(unknownParent),
			blockNumber: localTip.BlockNumber + 2,
			slot:        point.Slot,
		},
		BlockNumber: localTip.BlockNumber + 2,
	})

	assert.Equal(t, localTip, f.ls.chain.Tip())
	assert.Empty(t, f.ls.chainsyncOpeningRollbacks)
}

// A peer that intersected at a header still queued ahead of our blocks gets
// the same treatment: the header can be committed before the rollback runs,
// and a peer replaying our queue must not trim it.
func TestOpeningRollbackToQueuedHeaderKeepsQueue(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	localTip := f.ls.chain.Tip()
	parent := localTip
	queued := make([]mockHeader, 0, 2)
	for i, seed := range []string{"opening-rollback-queued-1", "opening-rollback-queued-2"} {
		hdr := mockHeader{
			hash:        lcommon.NewBlake2b256(testHashBytes(seed)),
			prevHash:    lcommon.NewBlake2b256(parent.Point.Hash),
			blockNumber: parent.BlockNumber + 1,
			slot:        localTip.Point.Slot + uint64(i+1)*10,
		}
		require.NoError(t, f.ls.chain.AddBlockHeader(context.Background(), hdr))
		queued = append(queued, hdr)
		parent = ochainsync.Tip{
			Point:       ocommon.NewPoint(hdr.slot, hdr.hash.Bytes()),
			BlockNumber: hdr.blockNumber,
		}
	}
	require.Equal(t, 2, f.ls.chain.HeaderCount())

	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    ocommon.NewPoint(queued[0].slot, queued[0].hash.Bytes()),
		Tip:      f.peerTipAhead(),
	})
	require.Equal(
		t,
		2,
		f.ls.chain.HeaderCount(),
		"an opening rollback to a queued header must not trim the queue",
	)
	f.sendChainsync(ChainsyncEvent{
		Point:       ocommon.NewPoint(queued[1].slot, queued[1].hash.Bytes()),
		Tip:         f.peerTipAhead(),
		BlockHeader: queued[1],
		BlockNumber: queued[1].blockNumber,
	})

	assert.Equal(t, localTip, f.ls.chain.Tip())
	assert.Equal(
		t,
		2,
		f.ls.chain.HeaderCount(),
		"a peer replaying our queued headers must not trim them",
	)
}

// Once a session has delivered a header, a rollback is the peer moving off a
// chain it already showed us and applies at once, as before.
func TestStreamingSessionRollbackStillApplies(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)

	f.sendHeader(1, "")
	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(2),
		Tip:      f.peerTipAhead(),
	})

	assert.Equal(t, f.pointAtDepth(2), f.ls.chain.Tip().Point)
	assert.Empty(t, f.ls.chainsyncOpeningRollbacks)
}

// A connection's opening-rollback state ends with the connection.
func TestOpeningRollbackStateClearedOnConnectionClose(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)

	f.sendOpeningRollback(1)
	require.Len(t, f.ls.chainsyncOpeningRollbacks, 1)
	f.ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: f.connId},
	))

	assert.Empty(t, f.ls.chainsyncOpeningRollbacks)
	assert.Empty(t, f.ls.chainsyncHeaderSessions)
}
