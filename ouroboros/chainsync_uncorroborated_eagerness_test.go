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
	"sync"
	"sync/atomic"
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

func TestUncorroboratedIngressWaitsForEagernessAfterObservation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"no corroboration hook", "corroboration declined", "corroboration hold expired"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newPatienceRollForwardFixture(t, true)
			_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
			var order []string
			f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
			f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
				order = append(order, "authenticate")
				return 42, nil
			}
			f.o.config.ChainsyncObservePeerTip = func(update chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
				order = append(order, "observe")
				return chainselection.PeerTipUpdateAccepted
			}
			f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool {
				order = append(order, "apply gate")
				return false
			}
			if mode != "no corroboration hook" {
				f.o.corroborationHoldTimeout = time.Millisecond
				f.o.config.ChainsyncAwaitCorroboration = func(ctx context.Context, _ ouroboros.ConnectionId) (bool, error) {
					order = append(order, "corroboration")
					if mode == "corroboration hold expired" {
						<-ctx.Done()
						return false, ctx.Err()
					}
					return false, nil
				}
			}
			entered, release := make(chan uint64, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			f.o.config.ChainsyncAwaitEagerness = func(ctx context.Context, _ ouroboros.ConnectionId, block uint64, _ func() ochainsync.Tip) error {
				order = append(order, "eagerness")
				entered <- block
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			done := make(chan error, 1)
			go func() { done <- f.rollForwardError(newTestBlockHeader(100, 7, 0xaa)) }()
			select {
			case block := <-entered:
				require.EqualValues(t, 7, block)
			case err := <-done:
				t.Fatalf("uncorroborated ingress bypassed the eagerness limit: %v", err)
			case <-time.After(time.Second):
				t.Fatal("uncorroborated ingress did not reach the eagerness gate")
			}
			want := []string{"authenticate", "observe", "apply gate", "eagerness"}
			if mode != "no corroboration hook" {
				want = []string{"authenticate", "observe", "apply gate", "corroboration", "eagerness"}
			}
			require.Equal(t, want, order, "observe authenticated candidates before applying either wait")
			select {
			case err := <-done:
				t.Fatalf("uncorroborated stream advanced while held at LoE: %v", err)
			default:
			}
			unblock()
			require.NoError(t, testutil.RequireReceive(t, done, time.Second, "released LoE callback"))
			select {
			case <-ledgerCh:
				t.Fatal("eagerness admission cannot grant corroboration or ledger ingress")
			default:
			}
			other := newTestConnId("127.0.0.1:6001", "10.0.0.2:3001")
			require.True(t, f.state.RecordHeaderForDedup(other, ocommon.NewPoint(100, newTestBlockHeader(100, 7, 0xaa).Hash().Bytes())),
				"a withheld header must remain available from another peer")
		})
	}
}

func TestUncorroboratedEagernessWaitEndsWithConnection(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
	f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool { return false }
	entered, connectionDone := make(chan any), make(chan any)
	var once sync.Once
	cancel := func() { once.Do(func() { close(connectionDone) }) }
	t.Cleanup(cancel)
	f.o.config.ChainsyncAwaitEagerness = func(ctx context.Context, _ ouroboros.ConnectionId, _ uint64, _ func() ochainsync.Tip) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	header := newTestBlockHeader(100, 7, 0xaa)
	go func() {
		done <- f.o.chainsyncClientRollForwardAt(ochainsync.CallbackContext{
			ConnectionId: f.conn, ConnectionDoneChan: connectionDone,
		}, 0, header, ochainsync.Tip{Point: ocommon.NewPoint(100, header.Hash().Bytes()), BlockNumber: 7}, f.now)
	}()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("uncorroborated ingress skipped its cancellable eagerness wait: %v", err)
	case <-time.After(time.Second):
		t.Fatal("uncorroborated ingress did not reach LoE")
	}
	cancel()
	require.ErrorIs(t, testutil.RequireReceive(t, done, time.Second, "connection shutdown must cancel LoE"), context.Canceled)
	select {
	case <-ledgerCh:
		t.Fatal("cancelled eagerness header entered the ledger")
	default:
	}
	require.Zero(t, f.state.GetTrackedClient(f.conn).HeadersRecv)
}

func TestEagernessExcludesObserverAndRejectedHeader(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"observer", "bad crypto", "withheld producer", "pending selector"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newPatienceRollForwardFixture(t, mode != "observer")
			var calls atomic.Int32
			f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool { return false }
			f.o.config.ChainsyncAwaitEagerness = func(context.Context, ouroboros.ConnectionId, uint64, func() ochainsync.Tip) error {
				calls.Add(1)
				return nil
			}
			f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
			f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
				switch mode {
				case "bad crypto":
					return 0, errors.New("bad VRF signature")
				case "withheld producer":
					return 0, ledger.ErrHeaderVerificationWithheld
				default:
					return 42, nil
				}
			}
			if mode == "pending selector" {
				f.o.config.ChainsyncObservePeerTip = func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
					return chainselection.PeerTipUpdatePending
				}
			}
			f.rollForward(t, newTestBlockHeader(100, 7, 0xaa))
			require.Zero(t, calls.Load(), "only authenticated, admitted ingress candidates count toward LoE")
		})
	}
}

func TestUncorroboratedEagernessPreservesOverlapUntilSlowerPeerCatchesUp(t *testing.T) {
	t.Parallel()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	const k = uint64(3)
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode: true, SecurityParam: k, GenesisWindowSlots: 10000,
		// Neither of these two candidates can authorize ledger ingress alone.
		MinCorroboratingPeers: 2,
	})
	state := dchainsync.NewState(bus, nil)
	a := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	b := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(a))
	require.True(t, state.AddClientConnId(b))
	entered := make(chan struct{}, 1)
	o := newOuroboros(OuroborosConfig{
		EventBus:                    bus,
		ChainsyncIngressEligible:    func(ouroboros.ConnectionId) bool { return true },
		ChainsyncObservePeerTip:     cs.PreparePeerTipAdmissionDisposition,
		ChainsyncResolvePeerTip:     resolvePeerTipLikeNode(cs),
		ChainsyncApplyEligible:      cs.ShouldApplyIngress,
		ChainsyncAwaitCorroboration: cs.AwaitIngressCorroboration,
		ChainsyncAwaitEagerness: func(ctx context.Context, conn ouroboros.ConnectionId, block uint64, applied func() ochainsync.Tip) error {
			if conn == a && block == 6 {
				entered <- struct{}{}
			}
			return cs.AwaitEagernessLimit(ctx, conn, block, applied)
		},
	})
	o.chainsyncState = state
	o.corroborationHoldTimeout = time.Millisecond
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) { return 42, nil }
	headers := newLinkedTestHeaders(6)
	tip := ochainsync.Tip{Point: ocommon.NewPoint(1000000, []byte("advertised")), BlockNumber: 50000}
	connectionDone := make(chan any)
	var once sync.Once
	cancel := func() { once.Do(func() { close(connectionDone) }) }
	t.Cleanup(cancel)
	deliver := func(conn ouroboros.ConnectionId, h gledger.BlockHeader) error {
		return o.chainsyncClientRollForwardAt(ochainsync.CallbackContext{
			ConnectionId: conn, ConnectionDoneChan: connectionDone,
		}, 0, h, tip, time.Now())
	}
	for _, h := range headers[:5] {
		require.NoError(t, deliver(a, h), "a lone candidate must still bootstrap")
	}
	require.NoError(t, deliver(b, headers[0]))
	require.False(t, cs.ShouldApplyIngress(a))
	before := cs.EagernessLimit()
	require.True(t, before.Intersected)
	require.EqualValues(t, 1+k, before.BlockNumber)
	require.Equal(t, headers[0].Hash().Bytes(), before.Point.Hash)
	done := make(chan error, 1)
	go func() { done <- deliver(a, headers[5]) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("uncorroborated fast peer bypassed the exact eagerness anchor: %v", err)
	case <-time.After(time.Second):
		t.Fatal("fast peer did not enter its eagerness wait")
	}
	require.Eventually(t, func() bool { return cs.EagernessPaused(a) }, time.Second, time.Millisecond)
	require.Equal(t, before, cs.EagernessLimit(), "fast observation must not move or clear the shared anchor")
	for _, h := range headers[1:] {
		require.NoError(t, deliver(b, h), "slower same-chain observations must advance shared overlap")
	}
	require.NoError(t, testutil.RequireReceive(t, done, time.Second, "new shared overlap must release fast peer"))
	after := cs.EagernessLimit()
	require.EqualValues(t, 6+k, after.BlockNumber)
	require.Equal(t, headers[5].Hash().Bytes(), after.Point.Hash)
	require.False(t, cs.ShouldApplyIngress(a), "eagerness progress must not manufacture corroboration")
	select {
	case <-ledgerCh:
		t.Fatal("uncorroborated candidates reached the ledger")
	default:
	}
}

// A callback parked in the eagerness wait stops the protocol's receive loop,
// so the wait must end before the protocol's reply timers do. Expiry is not a
// failure: the header stays out of the ledger and the connection re-intersects.
func TestEagernessWaitExpiresIntoResync(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	_, ledgerCh := f.o.eventBus.Subscribe(ledger.ChainsyncEventType)
	f.o.eagernessWaitTimeout = 10 * time.Millisecond
	f.o.chainsyncScheduleAt = func(time.Time, func()) func() { return func() {} }
	f.o.config.ChainsyncApplyEligible = func(ouroboros.ConnectionId) bool { return false }
	f.o.config.ChainsyncAwaitEagerness = func(ctx context.Context, _ ouroboros.ConnectionId, _ uint64, _ func() ochainsync.Tip) error {
		<-ctx.Done()
		return ctx.Err()
	}
	connectionDone := make(chan any)
	var once sync.Once
	closeConn := func() { once.Do(func() { close(connectionDone) }) }
	// A callback that never returns on a broken build still ends with the test.
	t.Cleanup(closeConn)
	header := newTestBlockHeader(100, 7, 0xaa)
	done := make(chan error, 1)
	go func() {
		done <- f.o.chainsyncClientRollForwardAt(ochainsync.CallbackContext{
			ConnectionId: f.conn, ConnectionDoneChan: connectionDone,
		}, 0, header, ochainsync.Tip{Point: ocommon.NewPoint(100, header.Hash().Bytes()), BlockNumber: 7}, f.now)
	}()
	select {
	case err := <-done:
		require.NoError(t, err, "an expired eagerness wait must not fail the connection")
	case <-time.After(5 * time.Second):
		t.Fatal("eagerness wait was not bounded")
	}
	f.o.futureHeaderResyncMu.Lock()
	scheduled := f.o.futureHeaderResyncs[f.conn]
	f.o.futureHeaderResyncMu.Unlock()
	require.NotNil(t, scheduled, "expiry must re-intersect the connection")
	require.Equal(t, event.ChainsyncResyncReasonEagernessWaitExpired, scheduled.reason)
	select {
	case <-ledgerCh:
		t.Fatal("a header past the limit entered the ledger")
	default:
	}
}
