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
	"encoding/binary"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func newLinkedTestHeaders(count int) []gledger.BlockHeader {
	headers := make([]gledger.BlockHeader, 0, count)
	var prev gledger.Blake2b256
	for i := 1; i <= count; i++ {
		var hash gledger.Blake2b256
		hash[0] = 0xc0
		binary.BigEndian.PutUint64(hash[1:9], uint64(i))
		headers = append(headers, &testBlockHeader{
			hash:        hash,
			prevHash:    prev,
			blockNumber: uint64(i),
			slotNumber:  uint64(i) * 20,
		})
		prev = hash
	}
	return headers
}

// A peer that times out waiting for corroboration keeps its candidate
// frontier. Another peer can use that history to deliver the withheld chain
// prefix to the ledger in order.
func TestChainsyncUncorroboratedPeerAheadDoesNotStrandChainPrefix(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         400,
		GenesisWindowSlots:    1_000_000,
		MinCorroboratingPeers: 1,
	})
	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	// Distinct remote hosts so the two peers count as independent witnesses.
	connA := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip:     cs.PreparePeerTipAdmissionDisposition,
		ChainsyncResolvePeerTip:     resolvePeerTipLikeNode(cs),
		ChainsyncApplyEligible:      cs.ShouldApplyIngress,
		ChainsyncAwaitCorroboration: cs.AwaitIngressCorroboration,
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.corroborationHoldTimeout = 10 * time.Millisecond
	o.chainsyncScheduleAt = func(time.Time, func()) func() { return func() {} }

	// Keep the first peer on the same session through repeated hold timeouts.
	headers := newLinkedTestHeaders(300)
	tipHeader := headers[len(headers)-1]
	// The advertised tip is far past what the peers deliver here, so the
	// selector stays in Genesis mode for the whole test.
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(10_000_000, []byte("advertised-tip")),
		BlockNumber: 500_000,
	}
	deliver := func(connId ouroboros.ConnectionId) <-chan error {
		done := make(chan error, 1)
		go func() {
			for _, header := range headers {
				if err := o.chainsyncClientRollForward(
					ochainsync.CallbackContext{ConnectionId: connId},
					0,
					header,
					tip,
				); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		return done
	}

	doneA := deliver(connA)
	select {
	case err := <-doneA:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("first peer did not advance through bounded corroboration holds")
	}
	o.futureHeaderResyncMu.Lock()
	firstPeerResync := o.futureHeaderResyncs[connA]
	o.futureHeaderResyncMu.Unlock()
	require.Nil(t, firstPeerResync,
		"hold timeouts must preserve the candidate stream")
	doneB := deliver(connB)

	var next uint64 = 1
	deadline := time.After(30 * time.Second)
	for next <= tipHeader.BlockNumber() {
		select {
		case evt := <-ledgerCh:
			data, ok := evt.Data.(ledger.ChainsyncEvent)
			require.True(t, ok)
			blockNumber := data.BlockHeader.BlockNumber()
			// A duplicate from the other peer is expected; a header past the
			// next one leaves a gap the ledger cannot fill.
			require.LessOrEqual(
				t,
				blockNumber,
				next,
				"ledger received block %d before block %d",
				blockNumber,
				next,
			)
			if blockNumber == next {
				next++
			}
			if data.PeerTipAdmission != nil {
				data.PeerTipAdmission(true)
			}
		case <-deadline:
			t.Fatalf("ledger stopped at block %d", next-1)
		}
	}
	select {
	case err := <-doneB:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("second peer callback did not finish after the chain was applied")
	}
}

func resolvePeerTipLikeNode(
	cs *chainselection.ChainSelector,
) func(chainselection.PeerTipUpdateEvent, bool) {
	return func(e chainselection.PeerTipUpdateEvent, admitted bool) {
		if admitted {
			cs.CommitPeerTipAdmissionPinned(e)
			return
		}
		cs.RejectPeerTipAdmission(e)
	}
}

// A held callback must return before the chainsync client's reply timeout.
// The header stays withheld, while the session remains available for later
// observations and corroboration.
func TestChainsyncCorroborationHoldExpiresWithoutResync(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         3,
		MinCorroboratingPeers: 1,
	})
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip:     cs.PreparePeerTipAdmissionDisposition,
		ChainsyncResolvePeerTip:     resolvePeerTipLikeNode(cs),
		ChainsyncApplyEligible:      cs.ShouldApplyIngress,
		ChainsyncAwaitCorroboration: cs.AwaitIngressCorroboration,
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.corroborationHoldTimeout = 200 * time.Millisecond
	// The scheduled re-intersection is observed through its marker; the timer
	// itself is not run.
	o.chainsyncScheduleAt = func(time.Time, func()) func() { return func() {} }

	header := newLinkedTestHeaders(1)[0]
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(10_000_000, []byte("advertised-tip")),
		BlockNumber: 500_000,
	}
	done := make(chan error, 1)
	go func() {
		done <- o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			header,
			tip,
		)
	}()
	testutil.RequireNoReceive(t, done, 100*time.Millisecond,
		"an uncorroborated header must be held")
	require.NoError(t, testutil.RequireReceive(t, done, 5*time.Second,
		"the hold must end without a witness"))
	o.futureHeaderResyncMu.Lock()
	scheduled := o.futureHeaderResyncs[conn]
	o.futureHeaderResyncMu.Unlock()
	require.Nil(t, scheduled, "an expired hold must preserve the candidate frontier")
	testutil.RequireNoReceive(t, ledgerCh, 100*time.Millisecond,
		"an uncorroborated header reached the ledger")
}
