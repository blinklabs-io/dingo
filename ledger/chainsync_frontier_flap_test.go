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

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	// The selector publishes synchronously from the update call, so everything
	// it decided is already buffered on the subscriber channel.
	var checked int
	for drained := false; !drained; {
		select {
		case evt := <-switchCh:
			switchEvent, ok := evt.Data.(chainselection.ChainSwitchEvent)
			require.True(t, ok)
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
			drained = true
		}
	}
	require.Positive(
		t,
		checked,
		"the scenario must publish at least the initial selection event",
	)
}
