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
	"fmt"
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

func beyondForecastHorizonErr() error {
	return fmt.Errorf(
		"header verification deferred: %w",
		ledger.ErrHeaderBeyondForecastHorizon,
	)
}

type forecastHorizonFixture struct {
	o        *Ouroboros
	conn     ouroboros.ConnectionId
	tipCh    <-chan event.Event
	ledgerCh <-chan event.Event
	recycle  <-chan event.Event
}

func newForecastHorizonFixture(
	t *testing.T,
	remote string,
) forecastHorizonFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6020", remote)
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
	return forecastHorizonFixture{
		o:        o,
		conn:     conn,
		tipCh:    tipCh,
		ledgerCh: ledgerCh,
		recycle:  recycleCh,
	}
}

func (f forecastHorizonFixture) rollForward(
	t *testing.T,
	header gledger.BlockHeader,
) {
	t.Helper()
	f.rollForwardWithTip(t, header, ochainsync.Tip{
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		BlockNumber: header.BlockNumber(),
	})
}

// rollForwardWithTip delivers header with the peer advertising tip.
func (f forecastHorizonFixture) rollForwardWithTip(
	t *testing.T,
	header gledger.BlockHeader,
	tip ochainsync.Tip,
) {
	t.Helper()
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		tip,
	))
}

// A header the ledger cannot forecast is withheld from chain selection and
// from the ledger's header queue, and the peer is not penalized for it:
// unlike other deferred results, it cannot be validated at all yet.
func TestChainsyncClientRollForwardWithholdsHeaderPastForecastHorizon(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.20:3001")
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		return 0, beyondForecastHorizonErr()
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xa1))

	testutil.RequireNoReceive(
		t,
		f.tipCh,
		200*time.Millisecond,
		"a header past the forecast horizon must not extend the peer's candidate",
	)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		200*time.Millisecond,
		"a header past the forecast horizon must not be queued for blockfetch",
	)
	testutil.RequireNoReceive(
		t,
		f.recycle,
		200*time.Millisecond,
		"a header past the forecast horizon is not a peer fault",
	)
}

// The horizon can move back after admission let a header through, for
// example on a ledger rollback. The header then waits for admission again and
// counts once it verifies.
func TestChainsyncClientRollForwardRewaitsForecastHorizonBeforeObserving(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.21:3001")
	var admissions, verifications atomic.Int32
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		admissions.Add(1)
		return true, nil
	}
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		if verifications.Add(1) == 1 {
			return 0, beyondForecastHorizonErr()
		}
		return 42, nil
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xa2))
	require.EqualValues(t, 2, admissions.Load(),
		"a past-horizon verification result must wait for admission again")
	require.EqualValues(t, 2, verifications.Load())

	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"a header verified after the re-wait must be observed",
	)
	testutil.RequireReceive(
		t,
		f.ledgerCh,
		testutil.AsyncWait,
		"a header verified after the re-wait must reach the ledger",
	)
}

func TestForecastHorizonWaitRecoversVerifiedCandidate(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.29:3001")
	t.Cleanup(f.o.stopFutureHeaderResyncs)
	recoveryConn := newTestConnId("127.0.0.1:6020", "10.0.0.30:3001")
	prepared := make(chan chainselection.PeerTipUpdateEvent, 1)
	removed := make(chan chainselection.PeerTipUpdateEvent, 1)
	f.o.config.ChainsyncPrepareForecastWitness = func(
		update chainselection.PeerTipUpdateEvent,
	) []ouroboros.ConnectionId {
		prepared <- update
		return []ouroboros.ConnectionId{recoveryConn}
	}
	f.o.config.ChainsyncRemoveForecastWitness = func(
		update chainselection.PeerTipUpdateEvent,
	) {
		removed <- update
	}
	f.o.chainsyncHeaderAdmission = func(
		_ context.Context,
		e ledger.ChainsyncEvent,
	) (bool, error) {
		require.NotNil(t, e.ForecastHorizonWait)
		e.ForecastHorizonWait()
		return true, nil
	}
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	header := newTestBlockHeader(400, 1, 0xa3)
	e := ledger.ChainsyncEvent{
		ConnectionId: f.conn,
		BlockHeader:  header,
		Point:        ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes()),
			BlockNumber: header.BlockNumber(),
		},
	}
	accepted, err := f.o.admitChainsyncHeader(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		e,
	)
	require.NoError(t, err)
	require.True(t, accepted)
	staged := testutil.RequireReceive(
		t, prepared, testutil.AsyncWait, "forecast witness was not staged",
	)
	require.Equal(t, f.conn, staged.ConnectionId)
	require.Equal(t, e.Point, staged.ObservedTip.Point)
	require.Equal(t, staged, testutil.RequireReceive(
		t, removed, testutil.AsyncWait, "forecast witness was not removed",
	))
	resync := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait, "verified candidate was not replayed",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, recoveryConn, data.ConnectionId)
	require.Equal(t,
		event.ChainsyncResyncReasonForecastCorroborationRecovery,
		data.Reason,
	)
}

// A connection that closes while a header waits in admission must end the
// wait. The client's DoneChan cannot: it closes only after this callback's
// protocol loop returns.
func TestChainsyncAdmissionWaitEndsWhenConnectionCloses(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.22:3001")
	waiting := make(chan struct{})
	f.o.chainsyncHeaderAdmission = func(
		ctx context.Context,
		_ ledger.ChainsyncEvent,
	) (bool, error) {
		close(waiting)
		<-ctx.Done()
		return false, ctx.Err()
	}
	connDone := make(chan any)
	header := newTestBlockHeader(400, 1, 0xa3)
	returned := make(chan error, 1)
	go func() {
		returned <- f.o.chainsyncClientRollForward(
			ochainsync.CallbackContext{
				ConnectionId:       f.conn,
				ConnectionDoneChan: connDone,
			},
			0,
			header,
			ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		)
	}()
	testutil.RequireReceive(
		t,
		waiting,
		testutil.AsyncWait,
		"admission did not start waiting",
	)

	close(connDone)
	err := testutil.RequireReceive(
		t,
		returned,
		5*time.Second,
		"closing the connection must end the admission wait",
	)
	require.ErrorIs(t, err, context.Canceled)
}

// Admission reports a held header that cannot connect to the local chain, as
// happens to a promoted observer whose cursor ran far ahead. The connection
// re-intersects from the local chain with a dedicated, non-penalizing reason,
// and the callback returns instead of holding the protocol until its state
// timer closes the connection.
func TestChainsyncClientRollForwardResyncsDisconnectedHeader(t *testing.T) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.23:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		return false, fmt.Errorf(
			"wait for forecast horizon: %w",
			ledger.ErrChainsyncHeaderDisconnected,
		)
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xa4))

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"a disconnected header must request a re-intersection",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonDisconnectedHeaderAdmission,
		data.Reason,
	)
	require.False(t, chainsyncResyncDeniesPeer(data.Reason))
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		200*time.Millisecond,
		"a disconnected header must not extend the peer's candidate",
	)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		200*time.Millisecond,
		"a disconnected header must not be queued for blockfetch",
	)
	testutil.RequireNoReceive(
		t,
		f.recycle,
		200*time.Millisecond,
		"a disconnected header is not a peer fault",
	)
}

func TestChainsyncClientRollForwardResyncsHeaderWithNoForecastWork(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.24:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		return false, fmt.Errorf(
			"wait for forecast horizon: %w",
			ledger.ErrChainsyncHeaderNeedsResync,
		)
	}

	f.rollForward(t, newTestBlockHeader(400, 1, 0xa5))

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"a forecast wait with no local block work must re-intersect",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonForecastHorizonNoWork,
		data.Reason,
	)
	require.False(t, chainsyncResyncDeniesPeer(data.Reason))
	testutil.RequireNoReceive(t, f.tipCh, 200*time.Millisecond,
		"a held header must not extend the peer candidate")
	testutil.RequireNoReceive(t, f.ledgerCh, 200*time.Millisecond,
		"a held header must not reach blockfetch")
	testutil.RequireNoReceive(t, f.recycle, 200*time.Millisecond,
		"a missing local block range is not a peer fault")
}

// setLocalTip stubs the local tip block number and the security parameter k.
//
// It also lets header verification pass, so a header that is not resynced
// proceeds to chain selection instead of reaching an unset seam.
func (f forecastHorizonFixture) setLocalTip(tipBlock, k uint64) {
	f.o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		return 42, nil
	}
	f.o.chainsyncLocalTipBlock = func() (uint64, uint64) {
		return tipBlock, k
	}
}

// setBestPeer stubs whether chain selection has a best peer.
func (f forecastHorizonFixture) setBestPeer(present bool) {
	f.o.chainsyncHasBestPeer = func() bool { return present }
}

// peerTipAt is a peer tip at block number blockNumber.
func peerTipAt(blockNumber uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(blockNumber, []byte{0xee}),
		BlockNumber: blockNumber,
	}
}

func requireBehindTipResync(
	t *testing.T,
	f forecastHorizonFixture,
	resyncCh <-chan event.Event,
) {
	t.Helper()
	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"a header far behind the local tip must request a re-intersection",
	)
	data, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, data.ConnectionId)
	require.Equal(t, event.ChainsyncResyncReasonBehindLocalTip, data.Reason)
	require.False(t, chainsyncResyncDeniesPeer(data.Reason))
}

// An ingress-eligible connection whose cursor trails the local tip by more
// than k can never contribute, so it re-intersects from the local chain. The
// header does not extend the peer's candidate or reach the ledger, a second
// far-behind header on the same connection does not request a second resync,
// and the peer is not penalized.
func TestChainsyncClientRollForwardResyncsEligibleClientFarBehindTip(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.24:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(1000, 10)
	f.setBestPeer(false)

	// k+1 blocks behind the tip is the smallest distance that is resynced.
	f.rollForwardWithTip(t, newTestBlockHeader(400, 989, 0xa5), peerTipAt(1000))
	requireBehindTipResync(t, f, resyncCh)
	f.rollForwardWithTip(t, newTestBlockHeader(401, 500, 0xa6), peerTipAt(1000))

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"a second far-behind header must not request a second resync",
	)
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		200*time.Millisecond,
		"a far-behind header must not extend the peer's candidate",
	)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		200*time.Millisecond,
		"a far-behind header must not be queued for blockfetch",
	)
	testutil.RequireNoReceive(
		t,
		f.recycle,
		200*time.Millisecond,
		"a far-behind header is not a peer fault",
	)
}

// An observer promoted into an ingress slot with a cursor left far behind the
// local tip re-intersects at the moment it is promoted.
func TestChainsyncClientRollForwardResyncsPromotedObserverFarBehindTip(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.25:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(1000, 10)
	f.setBestPeer(false)
	require.True(t, f.o.chainsyncState.SetClientObservabilityOnly(f.conn, true))

	f.rollForwardWithTip(t, newTestBlockHeader(400, 500, 0xa7), peerTipAt(1000))

	requireBehindTipResync(t, f, resyncCh)
	observabilityOnly, exists := f.o.chainsyncState.ClientObservabilityOnly(
		f.conn,
	)
	require.True(t, exists)
	require.False(t, observabilityOnly, "the observer must have been promoted")
}

// While chain selection has a best peer, a far-behind eligible connection is
// a peer lagging the one feeding the ledger, not a stalled cursor: it is not
// resynced and its header still reaches chain selection.
func TestChainsyncClientRollForwardKeepsFarBehindClientWhileBestPeerExists(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.28:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(1000, 10)
	f.setBestPeer(true)

	f.rollForwardWithTip(t, newTestBlockHeader(400, 500, 0xaa), peerTipAt(1000))

	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"a far-behind header must still reach chain selection while a best peer exists",
	)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"a far-behind client must not be resynced while a best peer exists",
	)
}

// A fork or rollback within k is ordinary: the header is behind the local tip
// but not by more than k blocks, so the connection is not resynced and the
// header still reaches chain selection.
func TestChainsyncClientRollForwardKeepsEligibleClientWithinKOfTip(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.26:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(1000, 10)
	f.setBestPeer(false)

	// Exactly k behind is within bounds.
	f.rollForwardWithTip(t, newTestBlockHeader(990, 990, 0xa8), peerTipAt(1000))

	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"a header within k of the local tip must reach chain selection",
	)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"a header within k of the local tip must not request a resync",
	)
}

// A peer whose own tip trails the local tip by more than k is behind on the
// chain, not holding a stale cursor: re-intersecting cannot move it closer to
// the tip, and replacing its connection on every header would cut off a
// lagging upstream that is catching up. Its connection is kept.
func TestChainsyncClientRollForwardKeepsEligibleClientWhosePeerTipIsFarBehind(
	t *testing.T,
) {
	t.Parallel()

	f := newForecastHorizonFixture(t, "10.0.0.27:3001")
	_, resyncCh := f.o.eventBus.Subscribe(event.ChainsyncResyncEventType)
	f.setLocalTip(1000, 10)
	f.setBestPeer(false)

	f.rollForwardWithTip(t, newTestBlockHeader(400, 500, 0xa9), peerTipAt(500))

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"a peer whose own tip is far behind must not be resynced",
	)
	testutil.RequireReceive(
		t,
		f.tipCh,
		testutil.AsyncWait,
		"a header from a peer behind on the chain still reaches chain selection",
	)
}
