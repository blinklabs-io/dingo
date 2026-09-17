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
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chainSwitchBarrierTimeout bounds the wait for the barrier below. It is a
// deadlock bound, not a settling delay: the barrier is already queued behind
// whatever the selector decided by the time the wait starts, so the normal
// cost is one lane hand-off.
const chainSwitchBarrierTimeout = 30 * time.Second

// chainSwitchBarrier is a sentinel published through the chain-switch ordered
// lane so the test below can tell "no switch was decided" from "the switch has
// not been delivered yet".
//
// ChainSelector.publishSelection routes chain switches through
// EventBus.PublishOrdered (blinklabs-io/dingo#3550), so
// HandlePeerTipUpdateEvent returns before the lane worker has handed the event
// to any subscriber. A lane is a FIFO drained by exactly one worker, so a
// sentinel enqueued after those switches is delivered after them: receiving it
// back is proof that every switch published earlier on this goroutine has
// already reached the subscription. Its Data type is not ChainSwitchEvent, so
// it is skipped rather than counted as a decision. Same construction as
// switchBarrier in ouroboros/consensus_conformance_test.go.
type chainSwitchBarrier struct{}

// TestCanonicalFrontierCrossingDoesNotCloseAPeerAheadOfLocalTip closes the loop
// between chain selection and the ledger's fresh-cursor handling.
//
// Two canonical public roots advertise the SAME tip while their delivered
// frontiers cross by more than k. Every peer-to-peer ChainSwitchEvent the
// selector publishes for a peer whose delivered frontier is ahead of the local
// tip makes chainSwitchNeedsFreshCursorLocked request a resync, and
// ChainsyncResyncReasonChainSwitchCursorAhead requires a fresh connection — so
// each such switch closes the selected connection, resets its cursor back to
// the local tip and hands the frontier lead to the other peer. That is the
// self-sustaining switch loop observed during Preview from-genesis validation.
//
// The assertion is on the pair, not on either component alone: no chain-switch
// event this scenario produces may ask the ledger for a fresh cursor.
func TestCanonicalFrontierCrossingDoesNotCloseAPeerAheadOfLocalTip(
	t *testing.T,
) {
	chainManager, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := chainManager.PrimaryChain()
	require.NoError(t, testChain.AddLocalBlock(&mockBabbageBlock{slot: 100}))
	require.Zero(t, testChain.HeaderCount())
	localTip := testChain.Tip()

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	selectorBus := event.NewEventBus(nil, nil)
	t.Cleanup(selectorBus.Stop)
	_, switchCh := selectorBus.Subscribe(chainselection.ChainSwitchEventType)

	const securityParam = 432 // Preview k
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			EventBus:      selectorBus,
			SecurityParam: securityParam,
			Logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
			// The rollback subscription is not needed here and would race with
			// the synchronous tip updates below.
			DisableEventSubscriptions: true,
		},
	)
	selector.SetLocalTip(localTip)

	rootA := testChainsyncConnId(6000, 3001)
	rootB := testChainsyncConnId(6000, 3002)
	// The tip both canonical public roots advertised in the reproduction.
	advertised := ochainsync.Tip{
		Point: ocommon.NewPoint(
			121697834,
			[]byte("canonical-preview-tip"),
		),
		BlockNumber: 4625199,
	}
	frontier := func(block uint64) ochainsync.Tip {
		return ochainsync.Tip{
			Point: ocommon.NewPoint(
				600000+block,
				[]byte("hdr-"+strconv.FormatUint(block, 10)),
			),
			BlockNumber: block,
		}
	}
	deliver := func(connId ouroboros.ConnectionId, block uint64) {
		t.Helper()
		observed := frontier(block)
		selector.HandlePeerTipUpdateEvent(event.NewEvent(
			chainselection.PeerTipUpdateEventType,
			chainselection.PeerTipUpdateEvent{
				ConnectionId: connId,
				Tip:          advertised,
				ObservedTip:  observed,
				PraosView: chainselection.PraosTiebreakerViewFromTip(
					observed,
					nil,
					chainselection.PraosTiebreakerConfigUnknown(),
				),
			},
		))
	}

	// Both roots start on the same delivered header, well ahead of the local
	// tip, then their delivered frontiers cross repeatedly. Each step stays
	// within k of the peer's own previous frontier so the plausibility bound
	// accepts it; the crossings at 28842 and 29706 put the lead at 742 blocks,
	// the gap seen in the reproduction.
	deliver(rootA, 27900)
	deliver(rootB, 27900)
	for _, step := range []struct {
		conn  ouroboros.ConnectionId
		block uint64
	}{
		{rootB, 28029},
		{rootA, 28100},
		{rootB, 28461},
		{rootB, 28842},
		{rootA, 28532},
		{rootA, 28964},
		{rootB, 29274},
		{rootB, 29706},
	} {
		deliver(step.conn, step.block)
	}

	// The selector publishes through an ordered lane, so a switch it decided
	// during the deliveries above may not have reached switchCh yet. Enqueue a
	// barrier behind those switches and read until it comes back: everything
	// ahead of it in the lane's FIFO has been delivered by then.
	require.True(
		t,
		selectorBus.PublishOrdered(
			chainselection.ChainSwitchEventType,
			event.NewEvent(
				chainselection.ChainSwitchEventType,
				chainSwitchBarrier{},
			),
		),
		"event bus refused the chain-switch barrier",
	)
	var checked int
	for drained := false; !drained; {
		evt := testutil.RequireReceive(
			t,
			switchCh,
			chainSwitchBarrierTimeout,
			"chain-switch barrier",
		)
		switch switchEvent := evt.Data.(type) {
		case chainSwitchBarrier:
			drained = true
		case chainselection.ChainSwitchEvent:
			checked++
			assert.False(
				t,
				ls.chainSwitchNeedsFreshCursorLocked(
					switchEvent,
					switchEvent.NewConnectionId,
				),
				"a delivered-frontier crossing between peers on the same advertised chain must not close the selected connection (switch to %s at delivered block %d)",
				switchEvent.NewConnectionId.String(),
				switchEvent.NewObservedTip.BlockNumber,
			)
		default:
			// Only the selector and the barrier above publish on this
			// lane, so anything else is a bug in one of them.
			t.Fatalf("unexpected %T on the chain_switch lane", evt.Data)
		}
	}
	require.Positive(
		t,
		checked,
		"the scenario must publish at least the initial selection event",
	)
}
