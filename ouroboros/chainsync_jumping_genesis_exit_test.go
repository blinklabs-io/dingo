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

package ouroboros

import (
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newCsjGenesisHarness wires the harness to a real Genesis chain selector:
// ChainSync connections are the node's tracked clients and every
// MsgAwaitReply reaches the selector as an idle proof, as in the node. Every
// test point counts as on the local chain, so the exit rule reduces to "every
// connection is idle". A positive maxClients caps the ingress slots.
func newCsjGenesisHarness(
	t *testing.T,
	maxClients int,
) (*csjHarness, *chainselection.ChainSelector) {
	t.Helper()
	h := newCsjHarness(t, true)
	if maxClients > 0 {
		h.o.chainsyncState = dchainsync.NewStateWithConfig(
			nil,
			nil,
			dchainsync.Config{MaxClients: maxClients},
		)
	}
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:               true,
		SecurityParam:             10,
		DisableEventSubscriptions: true,
		PointOnLocalChain:         func(ocommon.Point) bool { return true },
		ChainSyncConnections:      h.o.chainsyncState.GetClientConnIds,
	})
	cs.SetLocalTip(ochainsync.Tip{Point: csjPoint(1), BlockNumber: 1})
	// The harness peers deliver no header, so the point the client reports
	// is empty; a real peer awaits at the last header it sent, on the chain.
	h.o.config.ChainsyncObserveAwaitReply = func(
		connId ouroboros.ConnectionId,
		_ ocommon.Point,
	) {
		cs.SetPeerIdling(connId, csjPoint(1))
	}
	h.o.csj.cfg.Active = func() bool {
		active, _ := cs.GenesisSelectionState()
		return active
	}
	return h, cs
}

func requireGenesis(t *testing.T, cs *chainselection.ChainSelector, msg string) {
	t.Helper()
	active, _ := cs.GenesisSelectionState()
	require.True(t, active, msg)
}

// sortedByConnId orders peers as the election does among equal candidates.
func sortedByConnId(peers []*csjPeer) []*csjPeer {
	sorted := slices.Clone(peers)
	slices.SortFunc(sorted, func(a, b *csjPeer) int {
		return compareConnIds(a.connId, b.connId)
	})
	return sorted
}

// A jumper sends no RequestNext, so it is never answered with MsgAwaitReply
// and is never idle. The exit rule needs every connection idle, so Genesis
// ends only once the dynamo and every jumper in turn have streamed to their
// tip and awaited: a dynamo that awaits is disengaged and the next one is
// elected.
func TestChainsyncJumpingGenesisExitsOnceEveryPeerAwaited(t *testing.T) {
	t.Parallel()
	h, cs := newCsjGenesisHarness(t, 0)
	dynamo, jumpers := h.startDynamoAndJumpers(100, 100)
	first, second := sortedByConnId(jumpers)[0], sortedByConnId(jumpers)[1]
	requireGenesis(t, cs, "fixture: Genesis before any peer awaited")

	dynamo.awaitReply(t)
	first.requireStreams(t, "first jumper elected after the dynamo awaited")
	require.True(t, h.o.csjIsDynamo(first.connId))
	require.False(t, h.o.csjIsDynamo(dynamo.connId),
		"a peer that awaited is disengaged, not dynamo")
	requireGenesis(t, cs, "two jumpers have not awaited")

	first.awaitReply(t)
	second.requireStreams(t, "second jumper elected after the first awaited")
	requireGenesis(t, cs, "the last jumper has not awaited")

	second.awaitReply(t)
	require.Eventually(t, func() bool {
		active, _ := cs.GenesisSelectionState()
		return !active
	}, 5*time.Second, time.Millisecond,
		"Genesis must end once the dynamo and every jumper awaited")
}

// A jumper elected dynamo that has not reached its tip is not idle, and
// keeps Genesis however many peers before it awaited.
func TestChainsyncJumpingGenesisStaysWhileJumperStreams(t *testing.T) {
	t.Parallel()
	h, cs := newCsjGenesisHarness(t, 0)
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]

	dynamo.awaitReply(t)
	jumper.requireStreams(t, "jumper elected after the dynamo awaited")
	require.True(t, h.o.csjIsDynamo(jumper.connId))

	testutil.RequireNever(t, func() bool {
		active, _ := cs.GenesisSelectionState()
		return !active
	}, 200*time.Millisecond, 5*time.Millisecond,
		"Genesis ended while a peer was still streaming to its tip")
}

// A dynamo that awaited is idle at its tip on purpose and nobody replaces it
// when no peer remains, so the stall watchdog must not recycle it.
func TestChainsyncJumpingStallWatchdogSparesAwaitingPeer(t *testing.T) {
	t.Parallel()
	const timeout = time.Minute
	start := time.Unix(1_700_000_000, 0)
	h, recycles := newCsjStallHarness(t, timeout)
	h.o.csj.now = func() time.Time { return start }
	dynamo := h.addPeer("dynamo", 100)
	h.promote(dynamo)
	dynamo.requireStreams(t, "dynamo")

	h.o.csjOnAwaitReply(dynamo.connId)
	require.False(t, h.o.csjIsDynamo(dynamo.connId))
	h.o.csjCheckDynamoStall(start.Add(10 * timeout))
	testutil.RequireNoReceive(t, recycles, 50*time.Millisecond,
		"a peer idle at its tip after MsgAwaitReply was recycled as a stalled dynamo")
}

// The dynamo that awaited holds the only ingress slot. The next dynamo needs
// one, so the caught-up peer yields it; it stays connected as an observer,
// whose await still counts as idle. Otherwise the jumper is never elected,
// never streams, and Genesis never ends.
func TestChainsyncJumpingGenesisExitsWithOneIngressSlot(t *testing.T) {
	t.Parallel()
	h, cs := newCsjGenesisHarness(t, 1)
	dynamo, jumpers := h.startDynamoAndJumpers(100)
	jumper := jumpers[0]

	dynamo.awaitReply(t)
	jumper.requireStreams(t, "jumper elected into the slot the dynamo yielded")
	require.True(t, h.o.csjIsDynamo(jumper.connId))
	observer, tracked := h.o.chainsyncState.ClientObservabilityOnly(
		dynamo.connId,
	)
	require.True(t, tracked, "the caught-up peer stays connected")
	require.True(t, observer, "the caught-up peer yielded its slot")
	requireGenesis(t, cs, "the jumper has not awaited")

	jumper.awaitReply(t)
	require.Eventually(t, func() bool {
		active, _ := cs.GenesisSelectionState()
		return !active
	}, 5*time.Second, time.Millisecond,
		"Genesis must end once both peers awaited")
}

// A disengaged peer without an identity is not adopted back into jumping: it
// awaited at its tip, so it is idle, and as a jumper it would never be idle
// again.
func TestChainsyncJumpingRefreshLeavesDisengagedUnidentifiedPeer(t *testing.T) {
	t.Parallel()
	h := newCsjHarness(t, true)
	h.o.demotionTimeout = 200 * time.Millisecond
	dynamo, _ := h.startDynamoAndJumpers()
	other := h.addPeer("unidentified", 100)
	h.unidentified.Store(other.connId, struct{}{})
	h.promote(other)
	other.requireJump(t, "unidentified jumper")
	h.requireAgreement(other.connId, dynamo.connId, csjPoint(10))
	// From now on the peer answers every jump with IntersectNotFound.
	other.tipBlock.Store(0)
	h.fragment.Store(20)
	h.deliver(dynamo.connId, 20)
	other.requireJump(t, "refused jump")
	testutil.RequireReceive(t, h.rejections, 5*time.Second, "rejection")
	other.requireStreams(t, "objector")

	other.awaitReply(t)
	require.Eventually(t, func() bool {
		h.o.csj.mu.Lock()
		defer h.o.csj.mu.Unlock()
		_, ok := h.o.csj.disengaged[other.connId]
		return ok
	}, 5*time.Second, time.Millisecond, "objector disengaged at its tip")
	h.o.csjRefresh()
	testutil.RequireNoReceive(t, h.stopped, 500*time.Millisecond,
		"a disengaged peer was restarted as a jumper")
}
