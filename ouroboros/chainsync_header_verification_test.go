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
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// nonByronTestHeader wraps testBlockHeader to report a post-Byron era so
// header-crypto verification takes the Praos path (epoch/nonce lookup)
// instead of the Byron PBFT path, which requires a concrete Byron header
// type.
type nonByronTestHeader struct {
	*testBlockHeader
}

func (h nonByronTestHeader) Era() gledger.Era {
	return babbage.EraBabbage
}

// TestChainsyncClientRollForwardExcludesHeaderFailingCryptoVerification
// proves that a header whose crypto verification returns a definite (not
// deferred) error is excluded from chain-selection observation and triggers
// a connection recycle, instead of being allowed to influence Genesis
// density or corroboration (dingo #3517).
func TestChainsyncClientRollForwardExcludesHeaderFailingCryptoVerification(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6010", "1.1.1.2:3001")
	require.True(t, state.AddClientConnId(conn))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(gledger.BlockHeader) error {
		return errors.New("boom: invalid VRF proof")
	}

	header := newTestBlockHeader(200, 1, 0xcc)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	))

	select {
	case <-tipCh:
		t.Fatal(
			"a header failing crypto verification must not be observed " +
				"for chain selection",
		)
	case <-time.After(200 * time.Millisecond):
	}

	select {
	case evt := <-recycleCh:
		data, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
		require.True(t, ok)
		require.Equal(t, conn, data.ConnectionId)
		require.Equal(t, "header_verification_failure", data.Reason)
	case <-time.After(time.Second):
		t.Fatal(
			"expected a connection recycle request after crypto verification failure",
		)
	}
}

// TestChainsyncClientRollForwardObservesHeaderWithDeferredCryptoVerification
// proves that a header this node cannot yet confirm (ValidateChainSelection-
// HeaderCrypto returns a deferred error, e.g. because local ledger state has
// not caught up to it) is still observed for chain selection. This preserves
// legitimate fast-sync/Genesis-bootstrap behavior, where an honest peer
// racing ahead of local ledger application must not be excluded.
//
// The verifier here is the real ledger.LedgerState.ValidateChainSelection-
// HeaderCrypto (not a fake), driven into its deferred path by a bare ledger
// with no cached epoch/nonce data -- proving the actual ledger method, not
// just the branching around it.
func TestChainsyncClientRollForwardObservesHeaderWithDeferredCryptoVerification(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6011", "1.1.1.3:3001")
	require.True(t, state.AddClientConnId(conn))

	ls := newTestLedgerState(t)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	// Force the readiness gate on and use the real verifier: a bare
	// LedgerState with no cached epoch/nonce data for this slot deterministically
	// returns a deferred error, matching what ShouldVerifyChainSelectionHeaderCrypto
	// would itself have skipped verification for -- so a caller relying only on
	// the real verifier's own error classification, without the readiness gate,
	// must still get fast-sync-safe (deferred, not excluded) behavior.
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = ls.ValidateChainSelectionHeaderCrypto

	var hash gledger.Blake2b256
	hash[0] = 0xdd
	header := nonByronTestHeader{&testBlockHeader{
		hash:        hash,
		blockNumber: 1,
		slotNumber:  200,
	}}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	))

	select {
	case evt := <-tipCh:
		_, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(
			t,
			ok,
			"a deferred verification result must still leave the header "+
				"eligible for chain-selection observation",
		)
	case <-time.After(time.Second):
		t.Fatal("expected PeerTipUpdateEvent despite deferred verification")
	}
	select {
	case <-recycleCh:
		t.Fatal(
			"a deferred verification result must not recycle the connection",
		)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestChainsyncClientRollForwardCompetingPeersOnlyVerifiedHeaderCounted
// proves the verification gate applies independently to every ingress-eligible
// peer, not only the currently apply-eligible one -- covering the acceptance
// criterion that competing (candidate) peers are subject to the same check as
// the applied chain. Two peers deliver headers for the same round; one fails
// crypto verification and must be excluded, the other passes and must be
// observed.
func TestChainsyncClientRollForwardCompetingPeersOnlyVerifiedHeaderCounted(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	state := dchainsync.NewState(bus, nil)
	goodConn := newTestConnId("127.0.0.1:6012", "10.0.0.10:3001")
	badConn := newTestConnId("127.0.0.1:6012", "10.0.0.11:3001")
	require.True(t, state.AddClientConnId(goodConn))
	require.True(t, state.AddClientConnId(badConn))

	badHeader := newTestBlockHeader(300, 1, 0xee)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		// Both peers are ingress-eligible candidates, e.g. competing during
		// Genesis bootstrap -- neither is apply-eligible, matching a peer that
		// has not yet won corroboration/selection.
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return false
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(h gledger.BlockHeader) error {
		if h.Hash() == badHeader.Hash() {
			return errors.New("boom: invalid VRF proof")
		}
		return nil
	}

	goodHeader := newTestBlockHeader(300, 1, 0xff)
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: badConn},
		0,
		badHeader,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(300, badHeader.Hash().Bytes()),
			BlockNumber: 1,
		},
	))
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: goodConn},
		0,
		goodHeader,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(300, goodHeader.Hash().Bytes()),
			BlockNumber: 1,
		},
	))

	select {
	case evt := <-tipCh:
		data, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(t, ok)
		require.Equal(
			t,
			goodConn,
			data.ConnectionId,
			"only the peer with a verified header may be observed",
		)
	case <-time.After(time.Second):
		t.Fatal("expected the verified peer's header to be observed")
	}
	select {
	case <-tipCh:
		t.Fatal(
			"the peer with a header failing crypto verification must not " +
				"also be observed",
		)
	case <-time.After(200 * time.Millisecond):
	}
}
