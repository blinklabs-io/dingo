// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestChainsyncAdmissionExpiryReverifiesBeforeStaging(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	f.state.SetClientConnId(f.conn)
	_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
	var generation atomic.Uint64
	generation.Store(217)
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			SecurityParam: 432,
			HeaderCryptoVerificationGenerationCurrent: func(g uint64) bool { return g == generation.Load() },
		},
	)
	header := newTestBlockHeader(100, 1, 0xaa)
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
	var verifications atomic.Uint64
	f.o.chainSelectionVerifyHeaderCrypto = func(_ ouroboros.ConnectionId, h gledger.BlockHeader) (uint64, error) {
		require.Equal(
			t,
			header.Hash(),
			h.Hash(),
			"retry must authenticate the same header",
		)
		verifications.Add(1)
		return generation.Load(), nil
	}
	f.o.config.ChainsyncRecoverDeliveredPeerTip = func(context.Context, ochainsync.CallbackContext, chainselection.PeerTipUpdateEvent, gledger.BlockHeader, func() bool) {
		generation.CompareAndSwap(217, 237)
	}
	var dispositions []chainselection.PeerTipUpdateDisposition
	f.o.config.ChainsyncObservePeerTip = func(update chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
		result := selector.PreparePeerTipAdmissionDisposition(update)
		dispositions = append(dispositions, result)
		return result
	}
	f.o.config.ChainsyncResolvePeerTip = func(chainselection.PeerTipUpdateEvent, bool) {}
	require.NoError(
		t,
		f.rollForwardError(header),
		"local publication must not terminate an honest peer's protocol",
	)
	require.Equal(
		t,
		[]chainselection.PeerTipUpdateDisposition{
			chainselection.PeerTipUpdateDeferred,
			chainselection.PeerTipUpdateAccepted,
		},
		dispositions,
		"publication must trigger authentication before another staging attempt",
	)
	require.EqualValues(t, 2, verifications.Load())
	evt := testutil.RequireReceive(
		t,
		ledgerCh,
		5*time.Second,
		"the reauthenticated header must reach ordinary ledger admission",
	)
	e := evt.Data.(ledger.ChainsyncEvent)
	require.EqualValues(
		t,
		237,
		e.HeaderCryptoVerificationGeneration,
		"ledger admission must receive the renewed verdict",
	)
	require.NotNil(t, e.PeerTipUpdate)
	require.EqualValues(
		t,
		237,
		e.PeerTipUpdate.HeaderCryptoVerificationGeneration,
	)
	require.Equal(t, header.Hash(), e.BlockHeader.Hash())
}

func TestChainsyncAdmissionExpiryRetainsFailureBoundaries(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"rejected", "crypto failure", "retry bound", "session replaced", "zero verdict", "local deferral", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newPatienceRollForwardFixture(t, true)
			f.state.SetClientConnId(f.conn)
			_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
			f.o.chainSelectionShouldVerifyHeaderCrypto = func(ocommon.Point) bool { return true }
			verifications, observations, delivered := 0, 0, 0
			f.o.config.ChainsyncRecordDeliveredPeerTip = func(chainselection.PeerTipUpdateEvent) { delivered++ }
			cryptoErr := errors.New("bad KES signature on renewed verdict")
			transientErr := transientContextErr(t)
			done := make(chan any)
			if mode == "cancelled" {
				f.o.config.ChainsyncHeaderVerificationSettled = func() <-chan struct{} { return make(chan struct{}) }
			}
			f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
				verifications++
				if verifications > 1 {
					if mode == "crypto failure" {
						return 0, cryptoErr
					}
					if mode == "session replaced" {
						f.state.RemoveClientConnId(f.conn)
						require.True(t, f.state.AddClientConnId(f.conn))
					}
					if mode == "zero verdict" {
						return 0, nil
					}
					if mode == "local deferral" {
						return 0, transientErr
					}
					if mode == "cancelled" {
						close(done)
						return 0, transientErr
					}
				}
				return 42, nil
			}
			f.o.config.ChainsyncObservePeerTip = func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
				observations++
				if mode == "rejected" {
					return chainselection.PeerTipUpdateRejected
				}
				return chainselection.PeerTipUpdateDeferred
			}
			header := newTestBlockHeader(100, 1, 0xaa)
			var err error
			if mode == "cancelled" {
				err = f.o.chainsyncClientRollForwardAt(
					ochainsync.CallbackContext{
						ConnectionId:       f.conn,
						ConnectionDoneChan: done,
					},
					0,
					header,
					ochainsync.Tip{BlockNumber: 1_000_000},
					f.now,
				)
			} else {
				err = f.rollForwardError(header)
			}
			switch mode {
			case "rejected":
				require.ErrorIs(t, err, errChainsyncCandidateRejected)
				require.Equal(t, 1, verifications)
			case "crypto failure":
				require.ErrorIs(t, err, cryptoErr)
				require.Equal(t, 1, observations)
			case "retry bound":
				require.NoError(t, err)
				require.Equal(t, 4, observations,
					"the staging retry budget must also bound authentication")
				require.Equal(
					t,
					4,
					verifications,
					"the staging retry budget must also bound authentication",
				)
			case "session replaced":
				require.ErrorIs(t, err, ochainsync.ErrStopSyncProcess)
				require.Equal(t, 1, observations)
				require.Equal(
					t,
					1,
					delivered,
					"renewed authentication must not reach a replacement bearer",
				)
			case "zero verdict", "local deferral":
				require.NoError(t, err)
				require.Equal(t, 1, observations)
			case "cancelled":
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, observations)
			}
			select {
			case <-ledgerCh:
				t.Fatal(
					"a rejected, unauthenticated or exhausted retry must never reach ledger admission",
				)
			default:
			}
		})
	}
}

func TestChainsyncAdmissionExpiryRefreshesDeferredRecoveryBaseline(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	f.o.ledgerState = newTestLedgerState(t)
	baseline := ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("old")), BlockNumber: 1}
	current := ochainsync.Tip{Point: ocommon.NewPoint(200, []byte("published")), BlockNumber: 2}
	setTestLedgerTip(t, f.o, current)
	f.o.config.ChainsyncObservePeerTip = func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
		return chainselection.PeerTipUpdateDeferred
	}
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		return 0, ledger.ErrHeaderAncestryUnavailable
	}
	e := ledger.ChainsyncEvent{BlockHeader: newTestBlockHeader(400, 3, 0xb9)}
	update := chainselection.PeerTipUpdateEvent{HeaderCryptoVerificationGeneration: 217}
	disposition, err := f.o.observeChainsyncPeerTip(ochainsync.CallbackContext{ConnectionId: f.conn}, &e, &update, f.state.ClientSession(f.conn), &baseline)
	require.NoError(t, err)
	require.Equal(t, chainselection.PeerTipUpdateDeferred, disposition)
	require.Equal(t, current, baseline, "recovery must use the ledger baseline from the final verification attempt")
}
