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
	"go.uber.org/goleak"
)

const (
	bandTestK     = 4
	bandTestLocal = 1_000
	// bandTestFirst is past reference+K for the reference peer's frontier.
	bandTestFirst = bandTestLocal + 2*bandTestK + 4
)

func bandTestHash(block uint64) gledger.Blake2b256 {
	var h gledger.Blake2b256
	h[0], h[1], h[2] = byte(block>>8), byte(block), 0xb7
	return h
}

func bandTestHeader(block uint64) gledger.BlockHeader {
	return nonByronTestHeader{&testBlockHeader{
		hash:        bandTestHash(block),
		prevHash:    bandTestHash(block - 1),
		blockNumber: block,
		slotNumber:  block * 20,
	}}
}

type catchUpBandFixture struct {
	o        *Ouroboros
	state    *dchainsync.State
	selector *chainselection.ChainSelector
	conn     ouroboros.ConnectionId
	applied  atomic.Uint64
	ledgerCh <-chan event.Event
	resyncCh <-chan event.Event
}

// newCatchUpBandFixture wires one connection through the production callback
// with the real chain selector and a verifier that, like the ledger's
// peer-relative fork-path check, cannot authenticate a header whose parent is
// neither the intersection nor in the connection's observed ancestry.
func newCatchUpBandFixture(t *testing.T) *catchUpBandFixture {
	t.Helper()
	f := &catchUpBandFixture{
		conn: newTestConnId("127.0.0.1:6031", "1.1.1.9:3001"),
	}
	f.applied.Store(bandTestLocal)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, f.ledgerCh = bus.Subscribe(ledger.ChainsyncEventType)
	_, f.resyncCh = bus.Subscribe(event.ChainsyncResyncEventType)
	f.state = dchainsync.NewState(bus, nil)
	require.True(t, f.state.AddClientConnId(f.conn))

	tipAt := func(block uint64) ochainsync.Tip {
		return ochainsync.Tip{
			Point:       ocommon.NewPoint(block*20, bandTestHash(block).Bytes()),
			BlockNumber: block,
		}
	}
	f.selector = chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		SecurityParam:             bandTestK,
		DisableEventSubscriptions: true,
		AppliedTip: func() (ochainsync.Tip, bool) {
			return tipAt(f.applied.Load()), true
		},
	})
	f.selector.SetLocalTip(tipAt(bandTestLocal))
	// A reference inside (local, local+K].
	require.True(t, f.selector.UpdatePeerTip(
		newTestConnId("127.0.0.1:6032", "1.1.1.10:3001"),
		tipAt(bandTestLocal+2),
		nil,
	))

	ls := newTestLedgerState(t)
	deferredErr := ls.ValidateChainSelectionHeaderCrypto(
		nonByronTestHeader{&testBlockHeader{blockNumber: 1, slotNumber: 200}},
	)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
	f.o = newOuroboros(OuroborosConfig{
		EventBus:       bus,
		LedgerState:    ls,
		ChainsyncState: f.state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip:   f.selector.PreparePeerTipAdmissionDisposition,
		ChainsyncAwaitCatchUpBand: f.selector.AwaitCatchUpBand,
	})
	f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	f.o.chainSelectionVerifyHeaderCrypto = func(
		conn ouroboros.ConnectionId,
		header gledger.BlockHeader,
	) (uint64, error) {
		prev := header.PrevHash().Bytes()
		if header.PrevHash() == bandTestHash(bandTestFirst-1) {
			return 1, nil // the intersection
		}
		if _, _, ok := f.state.LookupObservedHeader(conn, prev); ok {
			return 1, nil
		}
		return 0, deferredErr
	}
	return f
}

func (f *catchUpBandFixture) rollForward(
	ctx ochainsync.CallbackContext,
	block uint64,
) error {
	header := bandTestHeader(block)
	ctx.ConnectionId = f.conn
	return f.o.chainsyncClientRollForward(
		ctx,
		0,
		header,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(4_687_076*20, []byte("peer-tip")),
			BlockNumber: 4_687_076,
		},
	)
}

func (f *catchUpBandFixture) observed(block uint64) bool {
	_, _, ok := f.state.LookupObservedHeader(f.conn, bandTestHash(block).Bytes())
	return ok
}

// A connection whose headers run past the band while the ledger lags is
// parked, not closed: no header is recorded or published early, and once the
// applied tip reaches them each header is verified against the connection's
// own ancestry, observed, and published in order.
func TestChainsyncClientRollForwardBackpressuresCatchUpOvershoot(t *testing.T) {
	t.Parallel()
	f := newCatchUpBandFixture(t)

	const headers = 3
	results := make(chan error, headers)
	go func() {
		for i := range uint64(headers) {
			results <- f.rollForward(ochainsync.CallbackContext{}, bandTestFirst+i)
		}
	}()

	testutil.RequireNoReceive(t, results, 400*time.Millisecond,
		"a header past the band must park the callback, not return")
	require.False(t, f.observed(bandTestFirst),
		"a parked header must not enter the observed ancestry early")
	testutil.RequireNoReceive(t, f.ledgerCh, 10*time.Millisecond,
		"a parked header must not reach the ledger")
	require.Nil(t, f.selector.GetPeerTip(f.conn))

	// The reference goes stale at 1003 but the header is still past the
	// catch-up ceiling. Releasing it here would send it down the far-claim
	// path, which withholds it from the observed ancestry, so the connection
	// must stay parked until the header is admissible.
	f.applied.Store(bandTestLocal + 3)
	testutil.RequireNoReceive(t, results, 400*time.Millisecond,
		"a held connection must stay parked after its reference goes stale")
	require.False(t, f.observed(bandTestFirst))
	testutil.RequireNoReceive(t, f.ledgerCh, 10*time.Millisecond,
		"a still-parked header must not reach the ledger")
	// The applied catch-up ceiling, not the connection's own frontier, bounds
	// each header, so every header waits for the ledger to come within 2K of
	// it and is then released in order without leaving the observed ancestry.
	for i := range uint64(headers) {
		if i > 0 {
			testutil.RequireNoReceive(t, results, 400*time.Millisecond,
				"a header past the applied ceiling must park the callback")
			require.False(t, f.observed(bandTestFirst+i),
				"a parked header must not enter the observed ancestry early")
		}
		f.applied.Store(bandTestFirst + i - 2*bandTestK)
		require.NoError(t, testutil.RequireReceive(t, results, 10*time.Second,
			"headers must be processed once the applied tip advances"),
			"header %d must not close the connection", i+1)
		evt := testutil.RequireReceive(t, f.ledgerCh, 5*time.Second,
			"each header must reach the ledger")
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, bandTestHash(bandTestFirst+i).Bytes(), data.Point.Hash,
			"headers must be published in order")
		require.True(t, f.observed(bandTestFirst+i),
			"header %d must be in the connection's observed ancestry", i+1)
	}
	testutil.RequireNoReceive(t, f.resyncCh, 200*time.Millisecond,
		"no deferred-header recovery may recycle the connection")
	f.o.deferredHeaderRecoveryMu.Lock()
	_, flagged := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.False(t, flagged)
}

// A fabricated chain whose frontier is never admissible is not adopted while
// the connection waits, and the bounded wait ends in the ordinary rejection
// rather than in selection or a fetch.
func TestChainsyncClientRollForwardCatchUpBandWaitIsBounded(t *testing.T) {
	t.Parallel()
	f := newCatchUpBandFixture(t)
	f.o.catchUpBandWaitTimeout = 300 * time.Millisecond

	err := f.rollForward(ochainsync.CallbackContext{}, bandTestFirst)
	require.ErrorIs(t, err, errChainsyncCandidateRejected,
		"after the bound the header meets the unchanged plausibility verdict")
	testutil.RequireNoReceive(t, f.ledgerCh, 100*time.Millisecond,
		"a header that was never admissible must not reach the ledger")
	require.Nil(t, f.selector.GetPeerTip(f.conn))
	require.False(t, f.observed(bandTestFirst))
}

// Not t.Parallel: goleak inspects every goroutine in the process.
//
// Closing the connection while the callback is parked releases it, and leaves
// no goroutine behind.
func TestChainsyncClientRollForwardCatchUpBandWaitStopsOnConnectionClose(
	t *testing.T,
) {
	f := newCatchUpBandFixture(t)
	leakOpt := goleak.IgnoreCurrent()

	done := make(chan any)
	result := make(chan error, 1)
	go func() {
		result <- f.rollForward(
			ochainsync.CallbackContext{ConnectionDoneChan: done},
			bandTestFirst,
		)
	}()
	testutil.RequireNoReceive(t, result, 300*time.Millisecond,
		"the callback must be parked before the connection closes")
	close(done)
	err := testutil.RequireReceive(t, result, 5*time.Second,
		"closing the connection must release the parked callback")
	require.ErrorIs(t, err, context.Canceled)
	testutil.RequireNoReceive(t, f.ledgerCh, 100*time.Millisecond,
		"a header parked at close must not reach the ledger")
	require.False(t, f.observed(bandTestFirst))
	goleak.VerifyNone(t, leakOpt)
}

func TestCatchUpBandWaitTimeoutStaysUnderStallTimeout(t *testing.T) {
	t.Parallel()

	for stall, want := range map[time.Duration]time.Duration{
		0:                defaultCatchUpBandWaitTimeout,
		2 * time.Minute:  defaultCatchUpBandWaitTimeout,
		30 * time.Second: 15 * time.Second,
		time.Hour:        defaultCatchUpBandWaitTimeout,
	} {
		require.Equal(t, want, catchUpBandWaitTimeoutFor(stall),
			"stall timeout %s", stall)
	}
}
