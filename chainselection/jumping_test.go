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
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// loeBase is the first block number the jumping scenarios deliver. The local
// tip stays at origin, so every delivered slot must lie more than a Genesis
// window past it or the selector leaves Genesis mode.
const loeBase = 1000

// feedLoEChainFrom delivers blocks loeBase+from..loeBase+to.
func feedLoEChainFrom(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	prefix string,
	from, to uint64,
) {
	feedLoEChain(cs, connId, prefix, loeBase+from, loeBase+to)
}

// newJumpingSelector builds a Genesis selector whose apply gate needs two
// independent witnesses, with ChainSync Jumping set as given. Only the
// connections in ingress are eligible for ingress; a jumper is not.
func newJumpingSelector(
	jumping bool,
	ingress ...ouroboros.ConnectionId,
) *ChainSelector {
	return NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         loeTestK,
		GenesisWindowSlots:    2000,
		MinCorroboratingPeers: 2,
		ChainSyncJumping:      jumping,
		ConnectionEligible: func(connId ouroboros.ConnectionId) bool {
			return slices.Contains(ingress, connId)
		},
	})
}

func loePoint(prefix string, offset uint64) ocommon.Point {
	return loeTip(prefix, loeBase+offset).Point
}

// jumpTo records an agreement by jumper with the dynamo's point at offset.
func jumpTo(
	cs *ChainSelector,
	jumper, dynamo ouroboros.ConnectionId,
	offset uint64,
) bool {
	return cs.RecordJumpAgreement(
		jumper,
		dynamo,
		loePoint("c", offset),
		loeTip("c", loeBase+offset+2),
	)
}

// One dynamo streams; two identified jumpers answer a jump to its point and
// stream nothing. Their jump fragments are Limit on Eagerness candidates and
// witnesses under their own identities, so the dynamo's headers up to k past
// the jump point are apply-eligible.
func TestChainSyncJumpingCorroboratesDynamoWithoutJumperStreams(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	jumperA := corrConn(2)
	jumperB := corrConn(3)
	cs := newJumpingSelector(true, dynamo)
	feedLoEChainFrom(cs, dynamo, "c", 1, 12)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"a lone dynamo has no witness")

	agreedA := jumpTo(cs, jumperA, dynamo, 10)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"one jumper is below the two-witness threshold")
	agreedB := jumpTo(cs, jumperB, dynamo, 10)
	require.True(
		t,
		cs.ShouldApplyIngress(dynamo),
		"two identified jumpers holding the jump point must vouch for the dynamo",
	)
	require.True(t, agreedA)
	require.True(t, agreedB)

	limit := cs.EagernessLimit()
	require.True(t, limit.Intersected,
		"jump fragments share the jump point with the dynamo")
	require.Equal(t, loePoint("c", 10), limit.Point)
	require.Equal(t, uint64(loeBase+10+loeTestK), limit.BlockNumber)

	require.Nil(t, cs.GetPeerTip(jumperA), "a jumper streams no headers")
	require.Nil(t, cs.GetPeerTip(jumperB), "a jumper streams no headers")
	selected := cs.SelectBestChain()
	require.NotNil(t, selected)
	require.Equal(t, dynamo, *selected, "a jumper is never selected")

	feedLoEChainFrom(cs, dynamo, "c", 13, 11+loeTestK)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"the dynamo is still held k past the last jump")
	require.True(t, jumpTo(cs, jumperA, dynamo, 14))
	require.True(t, jumpTo(cs, jumperB, dynamo, 14))
	require.True(t, cs.ShouldApplyIngress(dynamo),
		"the next jump moves the limit with the dynamo")
}

// A jumper streams nothing by design, so its jump record ages whenever no
// round refreshes it, as while the dynamo waits at the forecast horizon. The
// record must still vouch once it is older than the stale-tip threshold: the
// jumper's live connection, not header traffic, shows it is still there.
func TestChainSyncJumpingRecordOutlivesStaleTipThreshold(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	jumperA := corrConn(2)
	jumperB := corrConn(3)
	cs := newJumpingSelector(true, dynamo)
	now := time.Unix(1_700_000_000, 0)
	cs.nowFn = func() time.Time { return now }
	feedLoEChainFrom(cs, dynamo, "c", 1, 12)
	require.True(t, jumpTo(cs, jumperA, dynamo, 10))
	require.True(t, jumpTo(cs, jumperB, dynamo, 10))
	require.True(t, cs.ShouldApplyIngress(dynamo))

	now = now.Add(cs.config.StaleTipThreshold + time.Second)
	feedLoEChainFrom(cs, dynamo, "c", 13, 13)
	require.True(
		t,
		cs.ShouldApplyIngress(dynamo),
		"jump records past the stale-tip threshold must still vouch",
	)
}

// Jump records keep the identity policy: connections under one identity, or
// under the dynamo's own, count once or not at all.
func TestChainSyncJumpingKeepsIdentityPolicy(t *testing.T) {
	t.Parallel()
	dynamo := corrConnAddr("10.0.0.1", 3001)
	cs := newJumpingSelector(true, dynamo)
	sameAsDynamo := corrConnAddr("10.0.0.1", 3002)
	sybilA := corrConnAddr("10.0.0.9", 3001)
	sybilB := corrConnAddr("10.0.0.9", 3002)
	feedLoEChainFrom(cs, dynamo, "c", 1, 12)

	require.True(t, jumpTo(cs, sameAsDynamo, dynamo, 10))
	require.True(t, jumpTo(cs, sybilA, dynamo, 10))
	require.True(t, jumpTo(cs, sybilB, dynamo, 10))
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"the dynamo's own identity and one Sybil identity are one witness")

	independent := corrConn(4)
	require.True(t, jumpTo(cs, independent, dynamo, 10))
	require.True(t, cs.ShouldApplyIngress(dynamo),
		"a second distinct identity completes the threshold")
}

// A jump record is refused when the feature is off, when the point is not on
// the dynamo's fragment, when the dynamo does not stream, and when the
// jumper itself streams.
func TestChainSyncJumpingRefusesInvalidAgreements(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	streaming := corrConn(2)
	jumper := corrConn(3)
	other := corrConn(4)

	off := newJumpingSelector(false, dynamo)
	feedLoEChainFrom(off, dynamo, "c", 1, 12)
	require.False(t, jumpTo(off, jumper, dynamo, 10),
		"ChainSync Jumping is off")
	require.False(t, jumpTo(off, other, dynamo, 10))
	require.False(t, off.ShouldApplyIngress(dynamo))

	cs := newJumpingSelector(true, dynamo, streaming)
	feedLoEChainFrom(cs, dynamo, "c", 1, 12)
	feedLoEChainFrom(cs, streaming, "c", 1, 3)
	require.False(t, cs.RecordJumpAgreement(
		jumper, dynamo, loePoint("x", 10), loeTip("c", loeBase+12),
	), "a point the dynamo's fragment does not hold")
	require.False(t, jumpTo(cs, jumper, corrConn(9), 10),
		"a dynamo that streams nothing")
	require.False(t, jumpTo(cs, streaming, dynamo, 10),
		"a streaming peer is not a jumper")
	require.False(t, jumpTo(cs, dynamo, dynamo, 10),
		"the dynamo cannot jump to itself")
	require.True(t, jumpTo(cs, jumper, dynamo, 10),
		"a valid agreement is recorded")
}

// A jumper that refuses a jump becomes an objector. Until it streams, its
// last agreed fragment holds the limit at k past the last agreed point; once
// it streams its fork, the fork is admitted only up to k past the point it
// shares with the dynamo.
func TestChainSyncJumpingObjectorForkBoundedByEagernessLimit(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	jumperA := corrConn(2)
	jumperB := corrConn(3)
	objector := corrConn(4)
	cs := newJumpingSelector(true, dynamo, objector)
	feedLoEChainFrom(cs, dynamo, "c", 1, 13)
	jumpTo(cs, jumperA, dynamo, 10)
	jumpTo(cs, jumperB, dynamo, 10)
	jumpTo(cs, objector, dynamo, 10)

	feedLoEChainFrom(cs, dynamo, "c", 14, 15)
	jumpTo(cs, jumperA, dynamo, 15)
	jumpTo(cs, jumperB, dynamo, 15)
	rejected := cs.RecordJumpRejection(objector)

	limit := cs.EagernessLimit()
	require.Equal(t, loePoint("c", 10), limit.Point,
		"an unresolved objector holds the limit at its last agreed point")
	require.Equal(t, uint64(loeBase+10+loeTestK), limit.BlockNumber)
	require.True(t, rejected)
	require.True(t, cs.ShouldApplyIngress(dynamo))
	feedLoEChainFrom(cs, dynamo, "c", 16, 16)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"the dynamo waits for the objector's disagreement to resolve")
	require.False(t, jumpTo(cs, objector, dynamo, 15),
		"an objector streams and is not asked to jump")

	feedLoEChainFrom(cs, objector, "c", 11, 12)
	feedLoEChainFrom(cs, objector, "x", 13, 12+loeTestK)
	limit = cs.EagernessLimit()
	require.Equal(t, loePoint("c", 12), limit.Point,
		"the streamed objector fragment supersedes its frozen jump record")
	require.Equal(t, uint64(loeBase+12+loeTestK), limit.BlockNumber)
	require.True(t, cs.ShouldApplyIngress(objector),
		"the fork is admitted k past the point the jumpers and dynamo hold")

	feedLoEChainFrom(cs, objector, "x", 13+loeTestK, 13+loeTestK)
	require.False(t, cs.ShouldApplyIngress(objector),
		"the fork past the Limit on Eagerness must not be applied")
	within, tracked := cs.eagernessAdmissionStatus(
		objector,
		loeBase+14+loeTestK,
		nil,
	)
	require.True(t, tracked)
	require.False(t, within)
}

// Removing a jumper's connection removes its witness.
func TestChainSyncJumpingRecordRemovedWithPeer(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	jumperA := corrConn(2)
	jumperB := corrConn(3)
	cs := newJumpingSelector(true, dynamo)
	feedLoEChainFrom(cs, dynamo, "c", 1, 12)
	jumpTo(cs, jumperA, dynamo, 10)
	jumpTo(cs, jumperB, dynamo, 10)
	require.True(t, cs.ShouldApplyIngress(dynamo))

	cs.RemovePeer(jumperB)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"a removed jumper no longer vouches")
}

// An objector's stream opens with MsgRollBackward to its last agreed point,
// which registers a record that has delivered no header. That record proves
// nothing about the objector's chain, so the frozen jump record keeps bounding
// the Limit on Eagerness, and keeps counting as a witness, until the stream
// delivers a header.
func TestChainSyncJumpingObjectorRollbackKeepsJumpRecord(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	jumperA := corrConn(2)
	jumperB := corrConn(3)
	objector := corrConn(4)
	cs := newJumpingSelector(true, dynamo, objector)
	feedLoEChainFrom(cs, dynamo, "c", 1, 15)
	require.True(t, jumpTo(cs, jumperA, dynamo, 10))
	require.True(t, jumpTo(cs, jumperB, dynamo, 10))
	require.True(t, jumpTo(cs, objector, dynamo, 10))
	require.True(t, jumpTo(cs, jumperA, dynamo, 15))
	require.True(t, jumpTo(cs, jumperB, dynamo, 15))
	require.True(t, cs.RecordJumpRejection(objector))
	require.Equal(t, loePoint("c", 10), cs.EagernessLimit().Point)

	cs.HandlePeerRollbackEvent(event.NewEvent(PeerRollbackEventType,
		PeerRollbackEvent{
			ConnectionId: objector,
			Point:        loePoint("c", 10),
			Tip:          loeTip("c", loeBase+20),
		}))
	require.NotNil(t, cs.GetPeerTip(objector),
		"the rollback registers the objector's stream")

	limit := cs.EagernessLimit()
	require.Equal(t, loePoint("c", 10), limit.Point,
		"a stream that delivered no header must not release the objector's limit")
	require.Equal(t, uint64(loeBase+10+loeTestK), limit.BlockNumber)
	feedLoEChainFrom(cs, dynamo, "c", 16, 11+loeTestK)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"the dynamo still waits k past the objector's last agreed point")

	feedLoEChainFrom(cs, objector, "c", 11, 12)
	require.Equal(t, loePoint("c", 12), cs.EagernessLimit().Point,
		"the first delivered header supersedes the frozen jump record")
}

// stageLoEChainFrom stages blocks loeBase+from..loeBase+to from connId as
// ChainSync ingress does before the apply gate, without committing them.
func stageLoEChainFrom(
	t *testing.T,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	prefix string,
	from, to uint64,
) {
	t.Helper()
	for offset := from; offset <= to; offset++ {
		tip := loeTip(prefix, loeBase+offset)
		require.True(t, cs.PreparePeerTipAdmission(PeerTipUpdateEvent{
			ConnectionId: connId,
			AdmissionID:  offset,
			Tip:          loeTip(prefix, loeBase+to),
			ObservedTip:  tip,
		}))
	}
}

// A dynamo whose stream opened with MsgRollBackward and whose headers since
// are held at the apply gate for want of a witness has committed nothing.
// Its jump fragment must be its candidate fragment, staged headers included,
// as the reference jumps to the dynamo's csCandidate: otherwise no jump can
// be issued or recorded, no jumper ever witnesses the dynamo, and the dynamo
// is held at corroboration for good.
func TestChainSyncJumpingDynamoHeldAtCorroborationIsJumpedTo(t *testing.T) {
	t.Parallel()
	dynamo := corrConn(1)
	jumperA := corrConn(2)
	jumperB := corrConn(3)
	cs := newJumpingSelector(true, dynamo)
	cs.HandlePeerRollbackEvent(event.NewEvent(PeerRollbackEventType,
		PeerRollbackEvent{
			ConnectionId: dynamo,
			Point:        loePoint("c", 0),
			Tip:          loeTip("c", loeBase+12),
		}))
	require.NotNil(t, cs.GetPeerTip(dynamo),
		"the rollback registers the dynamo")
	stageLoEChainFrom(t, cs, dynamo, "c", 1, 12)
	require.False(t, cs.ShouldApplyIngress(dynamo),
		"a lone dynamo is held at corroboration")

	points, head, ok := cs.JumpFragment(dynamo)
	require.True(t, ok,
		"a dynamo held at corroboration must still offer its candidate fragment")
	require.Equal(t, uint64(loeBase+12), head)
	require.Equal(t, loePoint("c", 12), points[len(points)-1])

	require.True(t, jumpTo(cs, jumperA, dynamo, 10),
		"an agreement on a staged dynamo point must be recorded")
	require.True(t, jumpTo(cs, jumperB, dynamo, 10))
	require.True(t, cs.ShouldApplyIngress(dynamo),
		"two identified jumpers holding the jump point must release the dynamo")

	require.False(t, cs.RecordJumpAgreement(
		jumperA, dynamo, loePoint("x", 11), loeTip("x", loeBase+11),
	), "a point the dynamo's candidate fragment does not hold")
}
