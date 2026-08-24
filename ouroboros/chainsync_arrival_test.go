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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// chainsyncClientRollForward retains an explicit decoded-handler entry point
// for package tests. Production registers the raw callback and records arrival
// before decoding; direct decoded tests timestamp at their own call boundary.
func (o *Ouroboros) chainsyncClientRollForward(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip ochainsync.Tip,
) error {
	return o.chainsyncClientRollForwardAt(
		ctx,
		blockType,
		blockData,
		tip,
		time.Now(),
	)
}

func TestChainsyncClientRollForwardRecordsHeaderArrival(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	before := time.Now()
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		tip,
	))
	after := time.Now()
	evt := testutil.RequireReceive(
		t,
		ledgerCh,
		2*time.Second,
		"roll-forward should publish a ledger ChainSync event",
	)
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.False(t, data.ArrivalTime.Before(before))
	require.False(t, data.ArrivalTime.After(after))
}

func TestChainsyncClientRollForwardRawRecordsArrivalBeforeDecodeWait(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	headerType, raw := conwayHeaderFixtureBytes(t)
	header, err := o.decodeChainsyncHeader(headerType, raw)
	require.NoError(t, err)
	key := hashDecodeInput(headerType, raw)

	// Claim this decode key so the real raw callback has to wait. The arrival
	// timestamp must already be captured before it joins that wait.
	o.headerDecodeCache.mu.Lock()
	o.headerDecodeCache.inFlight[key] = nil
	o.headerDecodeCache.mu.Unlock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			o.headerDecodeCache.finishDecode(key, header, nil)
		})
	}
	defer release()

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- o.chainsyncClientRollForwardRaw(
			ochainsync.CallbackContext{
				ConnectionId: newTestConnId(
					"127.0.0.1:6000",
					"1.1.1.1:3001",
				),
			},
			headerType,
			raw,
			ochainsync.Tip{},
		)
	}()
	testutil.WaitForCondition(t, func() bool {
		o.headerDecodeCache.mu.Lock()
		defer o.headerDecodeCache.mu.Unlock()
		return len(o.headerDecodeCache.inFlight[key]) == 1
	}, 2*time.Second, "raw callback should wait on the claimed decode")
	releasedAt := time.Now()
	release()
	require.NoError(t, testutil.RequireReceive(
		t,
		resultCh,
		2*time.Second,
		"raw callback should finish after decode release",
	))

	evt := testutil.RequireReceive(
		t,
		ledgerCh,
		2*time.Second,
		"raw roll-forward should publish a ledger ChainSync event",
	)
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.True(t, data.ArrivalTime.Before(releasedAt))
}

func TestChainsyncHeaderAdmissionIsPreObservationAndPeerLocal(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	connA := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))

	observed := make(chan ouroboros.ConnectionId, 2)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			e chainselection.PeerTipUpdateEvent,
		) bool {
			observed <- e.ConnectionId
			return true
		},
	})
	o.chainsyncState = state
	entered := make(chan struct{})
	release := make(chan struct{})
	o.chainsyncHeaderAdmission = func(
		ctx context.Context,
		e ledger.ChainsyncEvent,
	) (bool, error) {
		if e.ConnectionId != connA {
			return true, nil
		}
		close(entered)
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	headerA := newTestBlockHeader(100, 1, 0xaa)
	tipA := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, headerA.Hash().Bytes()),
		BlockNumber: 1,
	}
	headerB := newTestBlockHeader(101, 2, 0xbb)
	tipB := ochainsync.Tip{
		Point:       ocommon.NewPoint(101, headerB.Hash().Bytes()),
		BlockNumber: 2,
	}
	doneA := make(chan error, 1)
	go func() {
		doneA <- o.chainsyncClientRollForwardAt(
			ochainsync.CallbackContext{ConnectionId: connA},
			0,
			headerA,
			tipA,
			time.Now(),
		)
	}()
	<-entered
	trackedA := state.GetTrackedClient(connA)
	require.NotNil(t, trackedA)
	require.Equal(t, uint64(0), trackedA.Cursor.Slot)
	_, _, found := state.LookupObservedHeader(connA, headerA.Hash().Bytes())
	require.False(t, found)
	testutil.RequireNoReceive(
		t,
		observed,
		50*time.Millisecond,
		"waiting header must not update observed tip",
	)

	// A different peer must continue through admission and ledger publication
	// while connA waits; no node-wide ChainSync mutex is held by the wait.
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		headerB,
		tipB,
		time.Now(),
	))
	require.Equal(t, connB, testutil.RequireReceive(
		t,
		observed,
		time.Second,
		"second peer should be observed while first peer waits",
	))
	ledgerEvent := testutil.RequireReceive(
		t,
		ledgerCh,
		time.Second,
		"second peer should reach ledger while first peer waits",
	)
	require.Equal(
		t,
		connB,
		ledgerEvent.Data.(ledger.ChainsyncEvent).ConnectionId,
	)

	close(release)
	require.NoError(t, testutil.RequireReceive(
		t,
		doneA,
		time.Second,
		"first peer should resume after slot onset",
	))
}

func TestChainsyncFarFutureDropHasNoStateOrConnectionPenalty(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, observedCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)

	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	droppedConn := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	honestConn := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(droppedConn))
	require.True(t, state.AddClientConnId(honestConn))

	onset := time.Now().Add(time.Minute)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	dropFuture := true
	o.chainsyncHeaderAdmission = func(
		_ context.Context,
		e ledger.ChainsyncEvent,
	) (bool, error) {
		return e.ConnectionId != droppedConn || !dropFuture, nil
	}
	o.chainsyncHeaderSlotTime = func(uint64) (time.Time, error) {
		return onset, nil
	}
	var scheduled func()
	o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
		scheduled = fn
		return func() {}
	}

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: droppedConn},
		0,
		header,
		tip,
		time.Now(),
	))
	droppedClient := state.GetTrackedClient(droppedConn)
	require.NotNil(t, droppedClient)
	require.Equal(t, uint64(0), droppedClient.Cursor.Slot)
	_, _, found := state.LookupObservedHeader(
		droppedConn,
		header.Hash().Bytes(),
	)
	require.False(t, found)
	testutil.RequireNoReceive(t, observedCh, 50*time.Millisecond,
		"dropped header must not update observed tip")
	testutil.RequireNoReceive(t, ledgerCh, 50*time.Millisecond,
		"dropped header must not reach the ledger")
	testutil.RequireNoReceive(t, recycleCh, 50*time.Millisecond,
		"ambiguous clock skew must not recycle the peer")
	require.NotNil(t, scheduled)

	// The dropped point must not enter cross-peer dedup: another admitted peer
	// can still publish the same point as new.
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: honestConn},
		0,
		header,
		tip,
		time.Now(),
	))
	ledgerEvent := testutil.RequireReceive(
		t,
		ledgerCh,
		time.Second,
		"same point from admitted peer must not be deduplicated",
	)
	require.Equal(t, honestConn,
		ledgerEvent.Data.(ledger.ChainsyncEvent).ConnectionId)

	// Even after the clock recovers, withhold later headers until the timer's
	// re-intersection has stopped the old protocol. Otherwise its remote cursor
	// can advance across the deliberately dropped point and preserve the gap.
	dropFuture = false
	header101 := newTestBlockHeader(101, 2, 0xab)
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: droppedConn},
		0,
		header101,
		ochainsync.Tip{},
		time.Now(),
	))
	droppedClient = state.GetTrackedClient(droppedConn)
	require.NotNil(t, droppedClient)
	require.Equal(t, uint64(0), droppedClient.Cursor.Slot)
	scheduled()
	header102 := newTestBlockHeader(102, 3, 0xac)
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: droppedConn},
		0,
		header102,
		ochainsync.Tip{},
		time.Now(),
	))
	droppedClient = state.GetTrackedClient(droppedConn)
	require.NotNil(t, droppedClient)
	require.Equal(t, uint64(0), droppedClient.Cursor.Slot)

	// The production resync handler clears the marker only after Client.Stop
	// returns. Simulate that boundary and prove the restarted stream can advance.
	o.completeFutureHeaderResync(droppedConn)
	header103 := newTestBlockHeader(103, 4, 0xad)
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: droppedConn},
		0,
		header103,
		ochainsync.Tip{},
		time.Now(),
	))
	droppedClient = state.GetTrackedClient(droppedConn)
	require.NotNil(t, droppedClient)
	require.Equal(t, uint64(103), droppedClient.Cursor.Slot)
}

func TestFutureHeaderResyncCoalescesEarliestOnset(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	connID := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	base := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	type timerRecord struct {
		onset    time.Time
		fn       func()
		canceled bool
	}
	var timers []*timerRecord
	o.chainsyncScheduleAt = func(onset time.Time, fn func()) func() {
		record := &timerRecord{onset: onset, fn: fn}
		timers = append(timers, record)
		return func() { record.canceled = true }
	}

	o.scheduleFutureHeaderResync(connID, base.Add(10*time.Second))
	o.scheduleFutureHeaderResync(connID, base.Add(12*time.Second))
	o.scheduleFutureHeaderResync(connID, base.Add(5*time.Second))
	o.scheduleFutureHeaderResync(connID, base.Add(7*time.Second))
	require.Len(t, timers, 2)
	require.True(t, timers[0].canceled)
	require.False(t, timers[1].canceled)
	require.Equal(t, base.Add(5*time.Second), timers[1].onset)

	// A canceled superseded callback cannot publish; the active earliest timer
	// emits exactly one in-place, non-penalizing re-intersection request.
	timers[0].fn()
	testutil.RequireNoReceive(t, resyncCh, 50*time.Millisecond,
		"superseded timer must not publish")
	timers[1].fn()
	resyncEvent := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"earliest onset should request re-intersection",
	)
	data := resyncEvent.Data.(event.ChainsyncResyncEvent)
	require.Equal(t, connID, data.ConnectionId)
	require.Equal(t,
		event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery,
		data.Reason,
	)
	require.False(t, chainsyncResyncRequiresFreshConnection(data.Reason))
	require.False(t, chainsyncResyncDeniesPeer(data.Reason))
	testutil.RequireNoReceive(t, resyncCh, 50*time.Millisecond,
		"one onset must emit one recovery request")
}

func TestFutureHeaderResyncImmediateOnsetArmsBeforePublish(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	connID := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
		// Model time.AfterFunc observing an already-due onset before its
		// scheduling call returns.
		fn()
		return func() {}
	}

	scheduled := make(chan struct{})
	go func() {
		o.scheduleFutureHeaderResync(connID, time.Now().Add(-time.Second))
		close(scheduled)
	}()
	testutil.RequireReceive(t, scheduled, time.Second,
		"an immediate callback must not deadlock timer registration")
	evt := testutil.RequireReceive(t, resyncCh, time.Second,
		"an immediate callback must publish after timer registration")
	data := evt.Data.(event.ChainsyncResyncEvent)
	require.Equal(t, connID, data.ConnectionId)
	require.Equal(t,
		event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery,
		data.Reason,
	)
}

func TestFutureHeaderResyncSuppressesConnectionRemovedDuringArm(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	manager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{EventBus: bus},
	)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, manager.Stop(ctx))
	})
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		ouroboros_mock.ConversationKeepAlive,
	)
	conn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(keepalive.NewConfig(
			keepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
			keepalive.WithPeriod(30*time.Second),
			keepalive.WithTimeout(15*time.Second),
		)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.True(t, manager.AddConnection(conn, false, "127.0.0.1:1234"))

	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.connManager = manager
	var callback func()
	canceled := false
	o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
		callback = fn
		// Model connManager removing the connection after the optimistic
		// pre-lock lookup but before the timer marker is fully armed. Its
		// subsequent close event may already have observed no marker.
		require.True(t, manager.RemoveConnection(conn.Id(), conn))
		return func() { canceled = true }
	}

	o.scheduleFutureHeaderResync(conn.Id(), time.Now().Add(time.Minute))
	require.True(t, canceled)
	require.False(t, o.futureHeaderResyncPending(conn.Id()))
	require.NotNil(t, callback)
	callback()
	testutil.RequireNoReceive(t, resyncCh, 50*time.Millisecond,
		"a timer armed after connection removal must not publish recovery")
}

func TestFutureHeaderResyncSuppressedAfterConnectionCloseAndClose(
	t *testing.T,
) {
	for _, test := range []struct {
		name string
		stop func(*Ouroboros, ouroboros.ConnectionId)
	}{
		{
			name: "connection close",
			stop: func(o *Ouroboros, connID ouroboros.ConnectionId) {
				o.HandleConnClosedEvent(event.NewEvent(
					connmanager.ConnectionClosedEventType,
					connmanager.ConnectionClosedEvent{ConnectionId: connID},
				))
			},
		},
		{
			name: "ouroboros close",
			stop: func(o *Ouroboros, _ ouroboros.ConnectionId) {
				require.NoError(t, o.Close())
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Close)
			_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
			o := newOuroboros(OuroborosConfig{EventBus: bus})
			connID := newTestConnId(
				"127.0.0.1:6000",
				"10.0.0.1:3001",
			)
			var callback func()
			canceled := false
			scheduleCount := 0
			o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
				scheduleCount++
				callback = fn
				return func() { canceled = true }
			}
			o.scheduleFutureHeaderResync(connID, time.Now().Add(time.Minute))
			require.NotNil(t, callback)

			test.stop(o, connID)
			require.True(t, canceled)
			if test.name == "ouroboros close" {
				o.scheduleFutureHeaderResync(
					connID,
					time.Now().Add(2*time.Minute),
				)
				require.Equal(t, 1, scheduleCount,
					"Close must reject timers scheduled by racing callbacks")
			}
			callback()
			testutil.RequireNoReceive(t, resyncCh, 50*time.Millisecond,
				"stopped timer must not publish recovery")
		})
	}
}

func TestChainsyncHeaderAdmissionErrorFailsClosedBeforeObservation(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, observedCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	wantErr := errors.New("slot conversion failed")
	o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		return false, wantErr
	}
	header := newTestBlockHeader(100, 1, 0xaa)
	err := o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{
			ConnectionId: newTestConnId(
				"127.0.0.1:6000",
				"10.0.0.1:3001",
			),
		},
		0,
		header,
		ochainsync.Tip{},
		time.Now(),
	)
	require.ErrorIs(t, err, wantErr)
	testutil.RequireNoReceive(t, observedCh, 50*time.Millisecond,
		"failed-closed admission must precede observation")
}
