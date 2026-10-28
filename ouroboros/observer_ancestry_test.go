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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

func TestChainsyncObserverDeliveryAuthenticatesLinkedRawAncestry(t *testing.T) {
	t.Parallel()
	deferred := newTestLedgerState(t).ValidateChainSelectionHeaderCrypto(
		nonByronTestHeader{newTestBlockHeader(200, 1, 0xaa).(*testBlockHeader)},
	)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferred))

	for _, tc := range []struct {
		name       string
		firstErr   error
		skipFirst  bool
		otherPeer  bool
		wantChild  bool
		wantStored bool
	}{
		{name: "authenticated", wantChild: true, wantStored: true},
		{name: "deferred", firstErr: deferred,
			wantChild: true, wantStored: true},
		{name: "withheld", firstErr: ledger.ErrHeaderVerificationWithheld,
			wantChild: true, wantStored: true},
		{name: "invalid", firstErr: errors.New("invalid ancestor signature")},
		{name: "missing", skipFirst: true},
		{name: "other peer", otherPeer: true, wantStored: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPatienceRollForwardFixture(t, false)
			parent := newTestBlockHeader(100, 1, 0xaa)
			child := newTestBlockHeader(101, 2, 0xbb).(*testBlockHeader)
			child.prevHash = parent.Hash()
			originalConn := f.conn
			childAuthenticated := false
			calls := 0
			f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool {
				return true
			}
			// Substitute only crypto computation: the production roll-forward
			// handler must retain the first delivery and supply this connection's
			// exact raw predecessor to authentication of the second delivery.
			f.o.chainSelectionVerifyHeaderCrypto = func(
				conn ouroboros.ConnectionId,
				header gledger.BlockHeader,
			) (uint64, error) {
				calls++
				if header.Hash() == parent.Hash() {
					return 42, tc.firstErr
				}
				ancestor, _, ok := f.state.LookupObservedHeader(
					conn, header.PrevHash().Bytes(),
				)
				if !ok {
					return 0, ledger.ErrHeaderVerificationWithheld
				}
				require.Zero(t, ancestor.HeaderCryptoVerificationGeneration,
					"observer raw ancestry must not inherit delivery admission")
				require.Equal(t, parent.Hash(), ancestor.BlockHeader.Hash())
				childAuthenticated = true
				return 42, nil
			}
			delivered := make(chan chainselection.PeerTipUpdateEvent, 2)
			f.o.config.ChainsyncRecordDeliveredPeerTip = func(
				update chainselection.PeerTipUpdateEvent,
			) {
				delivered <- update
			}
			observeCalls := 0
			f.o.config.ChainsyncObservePeerTip = func(
				chainselection.PeerTipUpdateEvent,
			) chainselection.PeerTipUpdateDisposition {
				observeCalls++
				return chainselection.PeerTipUpdateAccepted
			}
			_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
			if !tc.skipFirst {
				f.rollForward(t, parent)
			}
			if tc.otherPeer {
				f.conn = newTestConnId("127.0.0.1:6001", "10.0.0.2:3001")
				require.True(t, f.state.AddClientConnId(f.conn))
			}
			f.rollForward(t, child)

			require.Equal(
				t,
				tc.wantChild,
				childAuthenticated,
				"a linked observer child must authenticate using its raw parent",
			)
			wantCalls := 2
			if tc.skipFirst || tc.name == "invalid" {
				wantCalls = 1
			}
			require.Equal(t, wantCalls, calls)
			_, _, stored := f.state.LookupObservedHeader(
				originalConn, parent.Hash().Bytes(),
			)
			require.Equal(t, tc.wantStored, stored,
				"only valid or retryable observer ancestry may be retained")
			childDelivered := false
			for len(delivered) > 0 {
				update := <-delivered
				require.Zero(t, update.AdmissionID)
				if string(
					update.ObservedTip.Point.Hash,
				) == string(
					child.Hash().Bytes(),
				) {
					childDelivered = true
					require.EqualValues(t, 42,
						update.HeaderCryptoVerificationGeneration)
				}
			}
			require.Equal(t, tc.wantChild, childDelivered)
			require.Zero(t, observeCalls,
				"observer ancestry must not enter selector admission")
			testutil.RequireNoReceive(t, ledgerCh, 20*time.Millisecond,
				"observer ancestry must not reach ledger application")
		})
	}
}

func TestChainsyncObserverRawAncestryStillRequiresAdmissionOnPromotion(
	t *testing.T,
) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	eligible := false
	f.o.config.ChainsyncIngressEligible = func(ouroboros.ConnectionId) bool {
		return eligible
	}
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	verifyCalls := 0
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId, gledger.BlockHeader,
	) (uint64, error) {
		verifyCalls++
		return 0, ledger.ErrHeaderVerificationWithheld
	}
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(
		chainselection.PeerTipUpdateEvent,
	) {
		t.Fatal("withheld observer must not supply corroboration evidence")
	}
	header := newTestBlockHeader(100, 1, 0xaa)
	f.rollForward(t, header)
	raw, _, ok := f.state.LookupObservedHeader(f.conn, header.Hash().Bytes())
	require.True(t, ok)
	require.Zero(t, raw.HeaderCryptoVerificationGeneration)

	eligible = true
	admissionCalls := 0
	f.o.chainsyncHeaderAdmission = func(
		context.Context, ledger.ChainsyncEvent,
	) (bool, error) {
		admissionCalls++
		return false, nil
	}
	f.rollForward(t, header)
	require.Equal(t, 1, admissionCalls,
		"promotion must still enforce the future/clock admission boundary")
	require.Equal(t, 1, verifyCalls,
		"refused admission must stop before authentication or selection")
}

func TestChainsyncObserverRawAncestryRejectsReplacedSession(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	original := f.state.ClientSession(f.conn)
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	f.o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId, gledger.BlockHeader,
	) (uint64, error) {
		// Deterministically replace the bearer while authentication is in
		// flight, before the production handler tries to retain its header.
		f.state.RemoveClientConnId(f.conn)
		require.True(t, f.state.AddClientConnId(f.conn))
		require.NotEqual(t, original, f.state.ClientSession(f.conn))
		return 42, nil
	}
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(
		chainselection.PeerTipUpdateEvent,
	) {
	}
	header := newTestBlockHeader(100, 1, 0xaa)
	f.rollForward(t, header)
	_, _, ok := f.state.LookupObservedHeader(f.conn, header.Hash().Bytes())
	require.False(t, ok,
		"an old observer callback must not populate a replacement session")
}

func TestChainsyncObserverVerificationFailureRecyclesOnlyDefiniteErrors(
	t *testing.T,
) {
	t.Parallel()
	deferred := newTestLedgerState(
		t,
	).ValidateChainSelectionHeaderCrypto(nonByronTestHeader{newTestBlockHeader(200, 1, 0xaa).(*testBlockHeader)})
	require.True(t, ledger.IsHeaderVerificationDeferred(deferred))
	for _, tc := range []struct {
		name   string
		err    error
		failed bool
	}{
		{"valid", nil, false},
		{"invalid crypto", errors.New("invalid VRF proof"), true},
		{"deferred", deferred, false},
		{"withheld", ledger.ErrHeaderVerificationWithheld, false},
		{"forecast horizon", ledger.ErrHeaderBeyondForecastHorizon, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newPatienceRollForwardFixture(t, false)
			_, recycleCh := f.o.eventBus.Subscribe(
				ledger.ConnectionRecycleRequestedEventType,
			)
			delivered := 0
			f.o.config.ChainsyncRecordDeliveredPeerTip = func(chainselection.PeerTipUpdateEvent) { delivered++ }
			f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
			calls := 0
			f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) { calls++; return 42, tc.err }
			f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))
			client := f.state.GetTrackedClient(f.conn)
			require.NotNil(t, client)
			require.Equal(
				t,
				tc.failed,
				client.Status == dchainsync.ClientStatusFailed,
			)
			if tc.failed {
				evt := testutil.RequireReceive(
					t,
					recycleCh,
					time.Second,
					"definite observer failure must request recycling",
				)
				data, ok := evt.Data.(ledger.ConnectionRecycleRequestedEvent)
				require.True(t, ok)
				require.Equal(t, f.conn, data.ConnectionId)
				require.Equal(t, "header_verification_failure", data.Reason)
				require.True(t, client.ObservabilityOnly)
			} else {
				testutil.RequireNoReceive(t, recycleCh, 20*time.Millisecond, "retryable observer verification must not penalize the peer")
			}
			f.rollForward(t, newTestBlockHeader(101, 2, 0xbb))
			if tc.failed {
				require.Equal(
					t,
					1,
					calls,
					"a failed observer must not re-enter verification",
				)
				require.Zero(t, delivered)
			} else {
				require.Equal(t, 2, calls)
			}
		})
	}
}
