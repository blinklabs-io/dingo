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
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type promotionGapTimer struct {
	callback  func()
	cancelled atomic.Bool
}

func (timer *promotionGapTimer) fire() {
	if !timer.cancelled.Load() {
		timer.callback()
	}
}

type promotionGapFixture struct {
	o        *Ouroboros
	conn     ouroboros.ConnectionId
	fillers  []ouroboros.ConnectionId
	eligible atomic.Bool
	verified atomic.Int32
	timers   chan *promotionGapTimer
	ledgerCh <-chan event.Event
	resyncCh <-chan event.Event
}

func newPromotionGapFixture(t *testing.T) *promotionGapFixture {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	o, _, conn := newOutboundStartTestOuroboros(t, logger, bus)
	f := &promotionGapFixture{o: o, conn: conn, timers: make(chan *promotionGapTimer, 16)}
	_, f.ledgerCh = bus.Subscribe(ledger.ChainsyncEventType)
	_, f.resyncCh = bus.Subscribe(event.ChainsyncResyncEventType)
	f.eligible.Store(true)
	o.chainsyncState = dchainsync.NewStateWithConfig(bus, nil, dchainsync.Config{
		MaxClients: capacityTestSlots, StallTimeout: dchainsync.DefaultStallTimeout,
	})
	o.config.ChainsyncIngressEligible = func(ouroboros.ConnectionId) bool { return f.eligible.Load() }
	o.config.ChainsyncObservePeerTip = func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
		return chainselection.PeerTipUpdateAccepted
	}
	o.chainsyncHeaderAdmission = func(context.Context, ledger.ChainsyncEvent) (bool, error) { return true, nil }
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		f.verified.Add(1)
		return 77, nil
	}
	o.chainsyncScheduleAt = func(_ time.Time, callback func()) func() {
		timer := &promotionGapTimer{callback: callback}
		f.timers <- timer
		return func() { timer.cancelled.Store(true) }
	}
	o.futureHeaderResyncCtx = context.Background()
	t.Cleanup(o.stopFutureHeaderResyncs)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	o.SubscribeChainsyncResync(ctx)
	for i, remote := range []string{"10.0.0.11:3001", "10.0.0.12:3001", "10.0.0.13:3001"} {
		filler := newTestConnId("127.0.0.1:6000", remote)
		require.True(t, o.registerTrackedChainsyncClient(filler, true, true))
		f.fillers = append(f.fillers, filler)
		require.Equal(t, i+1, o.chainsyncState.ClientConnCount())
	}
	require.True(t, o.registerTrackedChainsyncClient(conn, true, true))
	observing, exists := o.chainsyncState.ClientObservabilityOnly(conn)
	require.True(t, exists)
	require.True(t, observing, "the real bearer must start in overflow observation")
	return f
}

func (f *promotionGapFixture) eligibility(eligible bool) {
	f.eligible.Store(eligible)
	f.o.HandlePeerEligibilityChangedEvent(event.NewEvent(peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{ConnectionId: f.conn, Eligible: eligible}))
}

func (f *promotionGapFixture) header(slot uint64, block uint64, hash byte) error {
	return f.o.chainsyncClientRollForwardAt(ochainsync.CallbackContext{ConnectionId: f.conn},
		0, newTestBlockHeader(slot, block, hash), ochainsync.Tip{
			Point: ocommon.NewPoint(slot+100, []byte("advertised")), BlockNumber: block + 1,
		}, time.Now())
}

func (f *promotionGapFixture) rollback(t *testing.T, point ocommon.Point) {
	t.Helper()
	require.NoError(t, f.o.chainsyncClientRollBackward(ochainsync.CallbackContext{ConnectionId: f.conn},
		point, ochainsync.Tip{Point: ocommon.NewPoint(1000, []byte("advertised")), BlockNumber: 10}))
}

func (f *promotionGapFixture) noTimer(t *testing.T) {
	t.Helper()
	select {
	case <-f.timers:
		t.Fatal("fresh capacity promotion must keep its bearer open without scheduling resync")
	default:
	}
	require.NotNil(t, f.o.connManager.GetConnectionById(f.conn), "fresh bearer must remain open")
}

func TestCapacityObserverFreshPromotionSurvivesMultipleTurns(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.rollback(t, ocommon.Point{})
	f.noTimer(t)
	for i := range 4 {
		require.NoError(t, f.header(uint64(100+i), uint64(1+i), byte(0xa0+i)))
		evt := testutil.RequireReceive(t, f.ledgerCh, time.Second, "fresh promoted header must reach ledger ingress")
		data := evt.Data.(ledger.ChainsyncEvent)
		// Rollback events share the channel; consume it before checking header.
		if data.Rollback {
			evt = testutil.RequireReceive(t, f.ledgerCh, time.Second, "header after initial rollback")
			data = evt.Data.(ledger.ChainsyncEvent)
		}
		require.EqualValues(t, 77, data.HeaderCryptoVerificationGeneration)
		require.NotNil(t, data.BlockHeader)
		f.eligibility(false)
		f.eligibility(true)
		f.noTimer(t)
		require.Equal(t, capacityTestSlots, f.o.chainsyncState.ClientConnCount())
	}
	require.EqualValues(t, 4, f.verified.Load(), "every current header must still pass producer authentication")
}

func TestCapacityObserverSkippedHeaderResyncsAndFreshReplayStaysOpen(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(chainselection.PeerTipUpdateEvent) {}
	require.NoError(t, f.header(100, 1, 0xaa))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn), "the production observer drop must mark a gap")
	f.noTimer(t)
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.eligibility(true)
	timer := testutil.RequireReceive(t, f.timers, time.Second, "gapped promotion must request replay")
	timer.fire()
	evt := testutil.RequireReceive(t, f.resyncCh, time.Second, "gapped observer must re-intersect")
	require.Equal(t, event.ChainsyncResyncReasonIngressPromotion, evt.Data.(event.ChainsyncResyncEvent).Reason)
	require.Eventually(t, func() bool { return f.o.connManager.GetConnectionById(f.conn) == nil }, time.Second, time.Millisecond)

	// A replacement's intersection comes from the applied ledger even when
	// the primary header queue is ahead. Its replay must not restart again.
	ls, _ := newTestLedgerStateWithChain(t, 5)
	r := newPromotionGapFixture(t)
	r.o.ledgerState = ls
	applied := ocommon.NewPoint(2, bytes.Repeat([]byte{2}, 32))
	ls.SetTipForTesting(ochainsync.Tip{Point: applied, BlockNumber: 2})
	r.o.config.ChainsyncIntersectAtLedgerTip = func() bool { return true }
	points, err := r.o.buildDefaultChainsyncIntersectPoints(context.Background(), r.conn)
	require.NoError(t, err)
	require.NotEmpty(t, points)
	require.Equal(t, applied, points[0])
	r.o.chainsyncState.RemoveClientConnId(r.fillers[0])
	r.rollback(t, points[0])
	require.NoError(t, r.header(3, 3, 0xab))
	rollbackEvent := testutil.RequireReceive(t, r.ledgerCh, time.Second, "fresh intersection rollback")
	require.True(t, rollbackEvent.Data.(ledger.ChainsyncEvent).Rollback)
	replayed := testutil.RequireReceive(t, r.ledgerCh, time.Second, "replacement must replay to ledger ingress")
	require.EqualValues(t, 3, replayed.Data.(ledger.ChainsyncEvent).Point.Slot)
	require.EqualValues(t, 77, replayed.Data.(ledger.ChainsyncEvent).HeaderCryptoVerificationGeneration)
	r.noTimer(t)
	require.False(t, r.o.chainsyncState.ClientObserverHeaderGap(r.conn))
}

func TestObserverDropPromotionRaceStillRequestsReplay(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(chainselection.PeerTipUpdateEvent) {
		close(arrived)
		<-release
	}
	result := make(chan error, 1)
	go func() { result <- f.header(100, 1, 0xaa) }()
	testutil.RequireReceive(t, arrived, time.Second, "observer decision precedes promotion")
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.eligibility(true)
	f.noTimer(t)
	unblock()
	require.NoError(t, testutil.RequireReceive(t, result, time.Second, "observer callback must finish"))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	timer := testutil.RequireReceive(t, f.timers, time.Second, "drop after promotion must still schedule replay")
	timer.fire()
	testutil.RequireReceive(t, f.resyncCh, time.Second, "raced gap must re-intersect")
}

func TestObserverDropCannotMarkReplacementSession(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	arrived, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(chainselection.PeerTipUpdateEvent) {
		close(arrived)
		<-release
	}
	result := make(chan error, 1)
	go func() { result <- f.header(100, 1, 0xaa) }()
	testutil.RequireReceive(t, arrived, time.Second, "old session observer decision")
	f.o.chainsyncState.RemoveClientConnId(f.conn)
	require.True(t, f.o.registerTrackedChainsyncClient(f.conn, true, true))
	unblock()
	require.NoError(t, testutil.RequireReceive(t, result, time.Second, "old callback must finish"))
	require.False(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn), "old callback must not poison replacement")
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.rollback(t, ocommon.Point{})
	f.noTimer(t)
}

func TestObserverGapDemotionAndSafeRollbackReset(t *testing.T) {
	t.Parallel()
	ls := newTestLedgerState(t)
	f := newPromotionGapFixture(t)
	f.o.ledgerState = ls
	applied := ocommon.NewPoint(50, []byte("applied"))
	f.o.ledgerState.SetTipForTesting(ochainsync.Tip{Point: applied, BlockNumber: 2})
	require.NoError(t, f.header(100, 3, 0xaa))
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.eligibility(true)
	timer := testutil.RequireReceive(t, f.timers, time.Second, "gap schedules recovery")
	f.eligibility(false)
	f.rollback(t, ocommon.NewPoint(applied.Slot, []byte("other-fork")))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn), "same slot on another fork cannot reset gap")
	require.False(t, timer.cancelled.Load())
	f.rollback(t, applied)
	require.False(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	require.True(t, timer.cancelled.Load(), "safe intersection cancels only pending promotion recovery")
	f.eligibility(true)
	f.noTimer(t)
}

func TestSelectorPendingDropDoesNotCreateObserverGap(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.rollback(t, ocommon.Point{})
	f.o.config.ChainsyncObservePeerTip = func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
		return chainselection.PeerTipUpdatePending
	}
	require.NoError(t, f.header(100, 1, 0xaa))
	require.False(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn), "pending far-tip admission is not an observer skip")
	f.eligibility(false)
	f.eligibility(true)
	f.noTimer(t)
}

func TestObserverGapOriginResetPreservesOtherRecovery(t *testing.T) {
	t.Parallel()
	f := newPromotionGapFixture(t)
	require.NoError(t, f.header(100, 1, 0xaa))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	require.True(t, f.o.scheduleHeaderResync(f.conn, time.Now().Add(time.Hour),
		event.ChainsyncResyncReasonDisconnectedHeaderAdmission))
	other := testutil.RequireReceive(t, f.timers, time.Second, "independent recovery must be armed")
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	f.rollback(t, ocommon.Point{})
	require.False(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn), "origin restarts the complete header stream")
	require.False(t, other.cancelled.Load(), "safe intersection cannot cancel another recovery reason")
	f.noTimer(t)
}

// A gapped observer promoted by a header that is the direct child of the
// applied tip (a local block producer's new block) must keep its connection:
// the skipped headers are already on the local chain, so the promotion resync
// would only close the connection under the new block (#5812).
func TestObserverGapPromotedByAppliedTipChildKeepsConnection(t *testing.T) {
	t.Parallel()
	ls := newTestLedgerState(t)
	f := newPromotionGapFixture(t)
	f.o.ledgerState = ls
	var appliedHash gledger.Blake2b256
	appliedHash[0] = 0xaa
	applied := ocommon.NewPoint(50, appliedHash.Bytes())
	ls.SetTipForTesting(ochainsync.Tip{Point: applied, BlockNumber: 2})
	var trimmed []ocommon.Point
	f.o.config.ChainsyncRecordDeliveredPeerRollback = func(e chainselection.PeerRollbackEvent) {
		trimmed = append(trimmed, e.Point)
	}
	// The peer re-sends the applied tip while it only observes: dropped, gap.
	require.NoError(t, f.header(50, 2, 0xaa))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	f.noTimer(t)
	// A slot frees; the next header promotes the observer.
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	child := &testBlockHeader{slotNumber: 60, blockNumber: 3, prevHash: appliedHash}
	child.hash[0] = 0xbb
	require.NoError(t, f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.conn}, 0, child,
		ochainsync.Tip{Point: ocommon.NewPoint(60, child.hash.Bytes()), BlockNumber: 3},
		time.Now(),
	))
	timer := testutil.RequireReceive(t, f.timers, time.Second, "promotion arms the fallback resync")
	require.True(t, timer.cancelled.Load(), "a child of the applied tip must cancel the promotion resync")
	require.False(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	require.Equal(t, []ocommon.Point{applied}, trimmed, "delivered history is trimmed to the applied tip")
	evt := testutil.RequireReceive(t, f.ledgerCh, time.Second, "the new block's header must reach ledger ingress")
	require.EqualValues(t, 60, evt.Data.(ledger.ChainsyncEvent).Point.Slot)
	timer.fire()
	select {
	case <-f.resyncCh:
		t.Fatal("the connection must not be resynced under the new block")
	case <-time.After(50 * time.Millisecond):
	}
	require.NotNil(t, f.o.connManager.GetConnectionById(f.conn), "connection must stay open")
}

// A promoting header that does not descend from the applied tip still needs
// the fresh intersection.
func TestObserverGapPromotedByNonChildStillResyncs(t *testing.T) {
	t.Parallel()
	ls := newTestLedgerState(t)
	f := newPromotionGapFixture(t)
	f.o.ledgerState = ls
	var appliedHash gledger.Blake2b256
	appliedHash[0] = 0xaa
	ls.SetTipForTesting(ochainsync.Tip{Point: ocommon.NewPoint(50, appliedHash.Bytes()), BlockNumber: 2})
	require.NoError(t, f.header(50, 2, 0xaa))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	var otherParent gledger.Blake2b256
	otherParent[0] = 0xcc
	child := &testBlockHeader{slotNumber: 60, blockNumber: 3, prevHash: otherParent}
	child.hash[0] = 0xbb
	require.NoError(t, f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.conn}, 0, child,
		ochainsync.Tip{Point: ocommon.NewPoint(60, child.hash.Bytes()), BlockNumber: 3},
		time.Now(),
	))
	timer := testutil.RequireReceive(t, f.timers, time.Second, "promotion arms the fallback resync")
	require.False(t, timer.cancelled.Load())
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	timer.fire()
	evt := testutil.RequireReceive(t, f.resyncCh, time.Second, "a gap off the applied chain must re-intersect")
	require.Equal(t, event.ChainsyncResyncReasonIngressPromotion, evt.Data.(event.ChainsyncResyncEvent).Reason)
}

// The promotion fallback timer is armed before the header backpressure wait.
// A child of the applied tip must cancel it before that wait, or a wait longer
// than the fallback delay lets it close the connection first.
func TestObserverGapAppliedTipChildSurvivesHeaderBackpressure(t *testing.T) {
	t.Parallel()
	ls := newTestLedgerState(t)
	f := newPromotionGapFixture(t)
	f.o.ledgerState = ls
	var appliedHash gledger.Blake2b256
	appliedHash[0] = 0xaa
	ls.SetTipForTesting(ochainsync.Tip{Point: ocommon.NewPoint(50, appliedHash.Bytes()), BlockNumber: 2})
	require.NoError(t, f.header(50, 2, 0xaa))
	require.True(t, f.o.chainsyncState.ClientObserverHeaderGap(f.conn))
	f.noTimer(t)
	waiting, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	f.o.chainsyncHeaderBackpressure = func(context.Context, int) error {
		close(waiting)
		<-release
		return nil
	}
	f.o.chainsyncState.RemoveClientConnId(f.fillers[0])
	child := &testBlockHeader{slotNumber: 60, blockNumber: 3, prevHash: appliedHash}
	child.hash[0] = 0xbb
	result := make(chan error, 1)
	go func() {
		result <- f.o.chainsyncClientRollForwardAt(
			ochainsync.CallbackContext{ConnectionId: f.conn}, 0, child,
			ochainsync.Tip{Point: ocommon.NewPoint(60, child.hash.Bytes()), BlockNumber: 3},
			time.Now(),
		)
	}()
	testutil.RequireReceive(t, waiting, time.Second, "the header must reach the backpressure wait")
	timer := testutil.RequireReceive(t, f.timers, time.Second, "promotion arms the fallback resync")
	// The fallback deadline passes while the header is still held.
	timer.fire()
	select {
	case <-f.resyncCh:
		t.Fatal("the fallback must not resync the connection during backpressure")
	case <-time.After(50 * time.Millisecond):
	}
	require.NotNil(t, f.o.connManager.GetConnectionById(f.conn), "connection must stay open")
	unblock()
	require.NoError(t, testutil.RequireReceive(t, result, time.Second, "header callback must finish"))
	evt := testutil.RequireReceive(t, f.ledgerCh, time.Second, "the new block's header must reach ledger ingress")
	require.EqualValues(t, 60, evt.Data.(ledger.ChainsyncEvent).Point.Slot)
	require.NotNil(t, f.o.connManager.GetConnectionById(f.conn))
}
