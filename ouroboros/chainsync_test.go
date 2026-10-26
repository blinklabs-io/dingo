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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gcbor "github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/protocol"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/keepalive"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"go.uber.org/goleak"
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
	t.Parallel()

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

func TestChainsyncClientRollForwardCarriesHeaderCryptoVerdict(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	o := newOuroboros(OuroborosConfig{
		EventBus:                 bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool { return true },
	})
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		return 42, nil
	}
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		tip,
	))
	evt := testutil.RequireReceive(t, ledgerCh, 2*time.Second, "ledger event")
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.EqualValues(t, 42, data.HeaderCryptoVerificationGeneration)
}

func TestChainsyncClientRollForwardRecordsObserverDelivery(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, false)
	delivered := make(chan chainselection.PeerTipUpdateEvent, 1)
	observeCalled := false
	f.o.config.ChainsyncRecordDeliveredPeerTip = func(
		update chainselection.PeerTipUpdateEvent,
	) {
		delivered <- update
	}
	f.o.config.ChainsyncObservePeerTip = func(
		chainselection.PeerTipUpdateEvent,
	) chainselection.PeerTipUpdateDisposition {
		observeCalled = true
		return chainselection.PeerTipUpdateAccepted
	}
	header := newTestBlockHeader(100, 1, 0xaa)

	f.rollForward(t, header)

	update := testutil.RequireReceive(t, delivered, time.Second,
		"observer header should be recorded for corroboration")
	require.Equal(t, f.conn, update.ConnectionId)
	require.Equal(t, header.Hash().Bytes(), update.ObservedTip.Point.Hash)
	require.Zero(t, update.AdmissionID,
		"observer history must not create an admission candidate")
	require.False(t, observeCalled,
		"an ineligible observer must not enter selector admission")
}

func TestChainsyncPeerTipAdmissionUsesRevalidatedGeneration(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.2:3001")
	require.True(t, state.AddClientConnId(connID))
	resolved := make(chan chainselection.PeerTipUpdateEvent, 1)
	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return chainselection.PeerTipUpdateAccepted
		},
		ChainsyncResolvePeerTip: func(
			update chainselection.PeerTipUpdateEvent,
			admitted bool,
		) {
			if admitted {
				resolved <- update
			}
		},
	})
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 1, nil
	}
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID},
		0,
		header,
		tip,
	))
	evt := testutil.RequireReceive(t, ledgerCh, 2*time.Second, "ledger event")
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.NotNil(t, data.PeerTipUpdate)
	require.NotNil(t, data.PeerTipAdmission)
	data.PeerTipUpdate.HeaderCryptoVerificationGeneration = 2
	data.PeerTipAdmission(true)
	assert.EqualValues(t, 2, testutil.RequireReceive(
		t,
		resolved,
		time.Second,
		"peer tip resolution should read the generation updated by ledger",
	).HeaderCryptoVerificationGeneration)
}

func TestChainsyncClientRollForwardCarriesPolicyTargetWithAdmittedEvent(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	connID := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	advertised := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, []byte("corroborated-target")),
		BlockNumber: 2,
	}
	o := newOuroboros(OuroborosConfig{
		EventBus:                 bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool { return true },
		ChainsyncSyncTarget: func(update chainselection.PeerTipUpdateEvent) (ochainsync.Tip, bool) {
			require.Equal(t, uint64(100), update.ObservedTip.Point.Slot)
			require.Equal(t, advertised, update.Tip)
			return advertised, true
		},
	})
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connID}, 0, header, advertised,
	))
	evt := testutil.RequireReceive(t, ledgerCh, 2*time.Second, "ledger event")
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.True(t, data.SyncTargetTrusted)
	require.Equal(t, uint64(100), data.Point.Slot)
	require.Equal(t, advertised, data.SyncTarget)
}

func TestChainsyncClientRollForwardRawRecordsArrivalBeforeDecodeWait(
	t *testing.T,
) {
	t.Parallel()

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
	expectedArrival := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	arrivalCaptured := make(chan struct{}, 1)
	o.chainsyncArrivalNow = func() time.Time {
		arrivalCaptured <- struct{}{}
		return expectedArrival
	}

	// Claim this decode key so the real raw callback has to wait. The arrival
	// timestamp must already be captured before it joins that wait.
	o.headerDecodeCache.mu.Lock()
	o.headerDecodeCache.inFlight[key] = nil
	o.headerDecodeCache.mu.Unlock()
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			o.headerDecodeCache.finishDecodeSized(key, 0, header, nil)
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
	testutil.RequireReceive(
		t,
		arrivalCaptured,
		2*time.Second,
		"raw callback should capture arrival before waiting on decode",
	)
	testutil.WaitForCondition(t, func() bool {
		o.headerDecodeCache.mu.Lock()
		defer o.headerDecodeCache.mu.Unlock()
		return len(o.headerDecodeCache.inFlight[key]) == 1
	}, 2*time.Second, "raw callback should wait on the claimed decode")
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
	require.Equal(t, expectedArrival, data.ArrivalTime)
}

func TestChainsyncHeaderAdmissionIsPreObservationAndPeerLocal(t *testing.T) {
	t.Parallel()

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
		) chainselection.PeerTipUpdateDisposition {
			observed <- e.ConnectionId
			return chainselection.PeerTipUpdateAccepted
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

func TestChainsyncRejectedSynchronousCandidateTerminatesIngress(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, observedCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	state := dchainsync.NewState(bus, nil)
	connId := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(connId))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return chainselection.PeerTipUpdateRejected
		},
	})
	o.chainsyncState = state
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: connId},
		0,
		header,
		tip,
		time.Now(),
	)
	require.ErrorIs(t, err, errChainsyncCandidateRejected)
	testutil.RequireNoReceive(
		t,
		ledgerCh,
		50*time.Millisecond,
		"a rejected synchronous candidate must not reach the ledger",
	)
	testutil.RequireNoReceive(
		t,
		observedCh,
		50*time.Millisecond,
		"a rejected synchronous candidate must not enter the async path",
	)
}

func TestChainsyncPendingFarTipRunRemainsConnectedUntilAccepted(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	const securityParam = uint64(2)
	const localBlock = uint64(1_000)
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			GenesisMode:   true,
			SecurityParam: securityParam,
		},
	)
	selector.SetLocalTip(ochainsync.Tip{
		Point:       ocommon.NewPoint(100, []byte("local")),
		BlockNumber: localBlock,
	})
	lagging := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, selector.UpdatePeerTip(lagging, ochainsync.Tip{
		Point:       ocommon.NewPoint(10_000, []byte("lagging")),
		BlockNumber: localBlock - 500,
	}, nil))

	state := dchainsync.NewState(bus, nil)
	connId := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(connId))
	var dispositions []chainselection.PeerTipUpdateDisposition
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: selector.ShouldApplyIngress,
		ChainsyncObservePeerTip: func(
			e chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			disposition := selector.PreparePeerTipAdmissionDisposition(e)
			dispositions = append(dispositions, disposition)
			return disposition
		},
		ChainsyncResolvePeerTip: selector.ResolvePeerTipAdmission,
	})
	o.chainsyncState = state
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(2_000_000, []byte("remote-tip")),
		BlockNumber: 100_000,
	}
	var previous gledger.Blake2b256
	for i := range securityParam + 1 {
		block := localBlock + 2*securityParam + 36 + i
		header := newTestBlockHeader(
			1_000+i,
			block,
			byte(0xa0+i),
		).(*testBlockHeader)
		header.prevHash = previous
		previous = header.hash
		require.NoError(t, o.chainsyncClientRollForwardAt(
			ochainsync.CallbackContext{ConnectionId: connId},
			gledger.BlockTypeByronMain,
			header,
			tip,
			time.Now(),
		))
		tracked := state.GetTrackedClient(connId)
		require.NotNil(t, tracked)
		assert.Zero(t, tracked.Cursor.Slot,
			"a staged header must not advance the admitted cursor")
		if i < securityParam {
			assert.Equal(t,
				chainselection.PeerTipUpdatePending,
				dispositions[i],
			)
			testutil.RequireNoReceive(
				t, ledgerCh, 20*time.Millisecond,
				"pending far-tip claim must not reach the ledger",
			)
		}
	}
	require.Equal(t,
		chainselection.PeerTipUpdateAccepted,
		dispositions[securityParam],
	)
	admitted := testutil.RequireReceive(
		t, ledgerCh, time.Second,
		"connected far-tip run must proceed after K+1 headers",
	)
	admission := admitted.Data.(ledger.ChainsyncEvent)
	require.NotNil(t, admission.PeerTipAdmission)
	admission.PeerTipAdmission(true)
	assert.Equal(t, uint64(1_000)+securityParam, state.GetTrackedClient(connId).Cursor.Slot)
}

func TestChainsyncHeaderBackpressurePrecedesAdmissionAndCrypto(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	entered := make(chan int, 1)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	o.chainsyncHeaderBackpressure = func(
		_ context.Context,
		admittedHeadroom int,
	) error {
		entered <- admittedHeadroom
		<-release
		return nil
	}
	var admissionCalls atomic.Int32
	o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		admissionCalls.Add(1)
		return false, nil
	}
	var cryptoCalls atomic.Int32
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		cryptoCalls.Add(1)
		return 0, nil
	}

	header := newTestBlockHeader(100, 1, 0xaa)
	done := make(chan error, 1)
	go func() {
		done <- o.chainsyncClientRollForwardAt(
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
	}()
	gotHeadroom := testutil.RequireReceive(
		t,
		entered,
		time.Second,
		"capacity gate should run before admission",
	)
	require.Equal(
		t,
		ledger.ChainsyncEventBufferSize+1+defaultMaxChainsyncClients,
		gotHeadroom,
	)
	require.Zero(t, admissionCalls.Load())
	require.Zero(t, cryptoCalls.Load())
	close(release)
	require.NoError(t, testutil.RequireReceive(
		t,
		done,
		time.Second,
		"roll forward should continue after capacity returns",
	))
	require.Equal(t, int32(1), admissionCalls.Load())
	require.Zero(t, cryptoCalls.Load())
}

func TestChainsyncHeaderBackpressureUsesCallbackTeardown(t *testing.T) {
	t.Parallel()

	// Keep an unrelated manager-owned connection at the callback's ID. The
	// callback teardown signal belongs to this protocol instance and must win
	// even when a replacement connection has reused the same addresses.
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	t.Cleanup(func() { _ = cm.Stop(context.Background()) })
	rawConn, err := ouroboros.New()
	require.NoError(t, err)
	t.Cleanup(func() { close(rawConn.ErrorChan()) })
	require.True(t, cm.AddConnection(rawConn, false, "127.0.0.1:1234"))
	_, managerDone := cm.GetConnectionWithDone(rawConn.Id())

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	o := newOuroboros(OuroborosConfig{
		EventBus:    bus,
		ConnManager: cm,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	entered := make(chan struct{})
	o.chainsyncHeaderBackpressure = func(
		ctx context.Context,
		_ int,
	) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}

	connectionDone := make(chan any)
	done := make(chan error, 1)
	go func() {
		done <- o.chainsyncClientRollForwardAt(
			ochainsync.CallbackContext{
				ConnectionId:       rawConn.Id(),
				ConnectionDoneChan: connectionDone,
			},
			0,
			newTestBlockHeader(100, 1, 0xaa),
			ochainsync.Tip{},
			time.Now(),
		)
	}()
	testutil.RequireReceive(
		t,
		entered,
		time.Second,
		"capacity gate did not begin waiting",
	)
	close(connectionDone)
	require.ErrorIs(
		t,
		testutil.RequireReceive(
			t,
			done,
			time.Second,
			"callback teardown did not cancel capacity wait",
		),
		context.Canceled,
	)
	rawConn.ErrorChan() <- nil
	testutil.RequireReceive(
		t,
		managerDone,
		time.Second,
		"foreign manager connection did not shut down",
	)
}

func TestChainsyncBacklogBoundsConcurrentPeerPublishers(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	handlerEntered := make(chan struct{})
	var handlerEnteredOnce sync.Once
	bus.SubscribeFuncWithBufferPolicy(
		ledger.ChainsyncEventType,
		ledger.ChainsyncEventBufferSize,
		event.SubscriberBackpressureBlock,
		func(event.Event) {
			handlerEnteredOnce.Do(func() { close(handlerEntered) })
			<-release
		},
	)
	primer := event.NewEvent(
		ledger.ChainsyncEventType,
		ledger.ChainsyncEvent{},
	)
	require.NoError(t, bus.PublishBlocking(ledger.ChainsyncEventType, primer))
	testutil.RequireReceive(
		t,
		handlerEntered,
		time.Second,
		"chainsync handler should consume the first event",
	)
	for range ledger.ChainsyncEventBufferSize {
		require.NoError(t, bus.PublishBlocking(ledger.ChainsyncEventType, primer))
	}

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	headrooms := make(chan int, defaultMaxChainsyncClients)
	o.chainsyncHeaderBackpressure = func(
		_ context.Context,
		admittedHeadroom int,
	) error {
		headrooms <- admittedHeadroom
		return nil
	}
	done := make(chan error, defaultMaxChainsyncClients)
	for i := range defaultMaxChainsyncClients {
		header := newTestBlockHeader(
			uint64(100+i),
			uint64(i+1),
			byte(i+1),
		)
		go func(peer int) {
			done <- o.chainsyncClientRollForwardAt(
				ochainsync.CallbackContext{
					ConnectionId: newTestConnId(
						"127.0.0.1:6000",
						fmt.Sprintf("10.0.0.%d:3001", peer+1),
					),
				},
				0,
				header,
				ochainsync.Tip{},
				time.Now(),
			)
		}(i)
	}
	for range defaultMaxChainsyncClients {
		require.Equal(
			t,
			ledger.ChainsyncEventBufferSize+1+defaultMaxChainsyncClients,
			testutil.RequireReceive(
				t,
				headrooms,
				time.Second,
				"each eligible peer should reach the capacity gate",
			),
		)
	}
	testutil.RequireNoReceive(
		t,
		done,
		50*time.Millisecond,
		"all concurrent peers should block behind the saturated ledger subscriber",
	)
	close(release)
	for range defaultMaxChainsyncClients {
		require.NoError(t, testutil.RequireReceive(
			t,
			done,
			time.Second,
			"draining ledger ingress should release each peer",
		))
	}
}

func TestChainsyncAwaitReplyPublishesDeliveredFrontier(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, awaitCh := bus.Subscribe(ledger.ChainsyncAwaitReplyEventType)
	state := dchainsync.NewState(bus, nil)
	connId := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(connId))
	point := ocommon.NewPoint(123, []byte("delivered"))
	require.True(t, state.UpdateClientTipWithoutDedup(
		connId,
		point,
		ochainsync.Tip{Point: point, BlockNumber: 10},
	))

	var observedPoint ocommon.Point
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncObserveAwaitReply: func(
			_ ouroboros.ConnectionId,
			point ocommon.Point,
		) {
			observedPoint = point
		},
	})
	o.chainsyncState = state
	cfg := ochainsync.NewConfig(o.chainsyncClientConnOpts()...)
	require.NotNil(t, cfg.AwaitReplyFunc)
	require.NoError(t, cfg.AwaitReplyFunc(ochainsync.CallbackContext{
		ConnectionId: connId,
	}))
	assert.Equal(t, point, observedPoint)

	evt := testutil.RequireReceive(
		t,
		awaitCh,
		time.Second,
		"AwaitReply callback should publish the delivered frontier",
	)
	data, ok := evt.Data.(ledger.ChainsyncAwaitReplyEvent)
	require.True(t, ok)
	assert.Equal(t, connId, data.ConnectionId)
}

func TestChainsyncRejectedStagedCandidateDoesNotReachLedger(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, observedCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	state := dchainsync.NewState(bus, nil)
	connId := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	require.True(t, state.AddClientConnId(connId))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return chainselection.PeerTipUpdateRejected
		},
		ChainsyncResolvePeerTip: func(
			chainselection.PeerTipUpdateEvent,
			bool,
		) {
		},
	})
	o.chainsyncState = state
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	require.ErrorIs(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: connId},
		0,
		header,
		tip,
		time.Now(),
	), errChainsyncCandidateRejected)
	testutil.RequireNoReceive(
		t,
		ledgerCh,
		50*time.Millisecond,
		"a rejected staged candidate must not reach the ledger",
	)
	testutil.RequireNoReceive(
		t,
		observedCh,
		50*time.Millisecond,
		"a rejected staged candidate must not enter the legacy async path",
	)
}

func TestChainsyncFarFutureDropHasNoStateOrConnectionPenalty(t *testing.T) {
	t.Parallel()

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

	// The production resync handler closes the connection, and connection
	// teardown clears the marker. Simulate that boundary and prove a
	// replacement stream can advance.
	o.cancelFutureHeaderResync(droppedConn)
	header103 := newTestBlockHeader(103, 4, 0xad)
	require.NoError(t, o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: droppedConn},
		0,
		header103,
		ochainsync.Tip{},
		time.Now(),
	))
	acceptedEvent := testutil.RequireReceive(
		t,
		ledgerCh,
		time.Second,
		"restarted stream header admission",
	)
	acceptedHeader := acceptedEvent.Data.(ledger.ChainsyncEvent)
	require.NotNil(t, acceptedHeader.PeerTipAdmission)
	acceptedHeader.PeerTipAdmission(true)
	droppedClient = state.GetTrackedClient(droppedConn)
	require.NotNil(t, droppedClient)
	require.Equal(t, uint64(103), droppedClient.Cursor.Slot)
}

func TestFutureHeaderResyncCoalescesEarliestOnset(t *testing.T) {
	t.Parallel()

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
	// emits exactly one non-penalizing re-intersection request.
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
	require.False(t, chainsyncResyncDeniesPeer(data.Reason))
	testutil.RequireNoReceive(t, resyncCh, 50*time.Millisecond,
		"one onset must emit one recovery request")
}

func TestFutureHeaderResyncImmediateOnsetArmsBeforePublish(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
// density or corroboration.
func TestChainsyncClientRollForwardExcludesHeaderFailingCryptoVerification(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	_, awaitCh := bus.Subscribe(ledger.ChainsyncAwaitReplyEventType)
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
	awaitObserved := false
	o.config.ChainsyncObserveAwaitReply = func(ouroboros.ConnectionId, ocommon.Point) {
		awaitObserved = true
	}
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	var verifiedConn ouroboros.ConnectionId
	verificationCalls := 0
	o.chainSelectionVerifyHeaderCrypto = func(
		connId ouroboros.ConnectionId,
		_ gledger.BlockHeader,
	) (uint64, error) {
		verifiedConn = connId
		verificationCalls++
		return 0, errors.New("boom: invalid VRF proof")
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
	require.Equal(t, conn, verifiedConn)

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
	cfg := ochainsync.NewConfig(o.chainsyncClientConnOpts()...)
	require.NotNil(t, cfg.AwaitReplyFunc)
	require.NoError(t, cfg.AwaitReplyFunc(
		ochainsync.CallbackContext{ConnectionId: conn},
	))
	require.False(t, awaitObserved)
	testutil.RequireNoReceive(
		t,
		awaitCh,
		200*time.Millisecond,
		"an excluded peer's AwaitReply must not reach the ledger",
	)

	secondHeader := newTestBlockHeader(201, 2, 0xcd)
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		secondHeader,
		ochainsync.Tip{
			Point: ocommon.NewPoint(
				secondHeader.SlotNumber(),
				secondHeader.Hash().Bytes(),
			),
			BlockNumber: 2,
		},
	))
	require.Equal(t, 1, verificationCalls,
		"a failed connection must not regain verification eligibility")
	client := state.GetTrackedClient(conn)
	require.NotNil(t, client)
	require.Equal(t, dchainsync.ClientStatusFailed, client.Status)
	require.True(t, client.ObservabilityOnly)
}

// TestChainsyncClientRollForwardWithholdsHeaderWithDeferredCryptoVerification
// proves that a header this node cannot yet confirm stays out of trusted
// history until a later strict verification succeeds.
//
// The verifier here is the real ledger.LedgerState.ValidateChainSelection-
// HeaderCrypto (not a fake), driven into its deferred path by a bare ledger
// with no cached epoch/nonce data -- proving the actual ledger method, not
// just the branching around it.
func TestChainsyncClientRollForwardWithholdsHeaderWithDeferredCryptoVerification(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6011", "1.1.1.3:3001")
	require.True(t, state.AddClientConnId(conn))

	ls := newTestLedgerState(t)

	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		LedgerState:    ls,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	// Force the readiness gate on and use the real verifier: a bare
	// LedgerState with no cached epoch/nonce data for this slot deterministically
	// returns a deferred error, matching what ShouldVerifyChainSelectionHeaderCrypto
	// would itself have skipped verification for -- so a caller relying only on
	// the real verifier's own error classification, without the readiness gate,
	// must still get fast-sync-safe (deferred, not excluded) behavior.
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(
		connId ouroboros.ConnectionId,
		header gledger.BlockHeader,
	) (uint64, error) {
		return ls.ValidatePeerChainSelectionHeaderCryptoWithGeneration(
			connId,
			header,
		)
	}

	var hash gledger.Blake2b256
	hash[0] = 0xdd
	header := nonByronTestHeader{&testBlockHeader{
		hash:        hash,
		blockNumber: 1,
		slotNumber:  200,
	}}
	deferredErr := ls.ValidateChainSelectionHeaderCrypto(header)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
	var verificationReady atomic.Bool
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		if !verificationReady.Load() {
			return 0, deferredErr
		}
		return 42, nil
	}
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

	testutil.RequireNoReceive(
		t,
		tipCh,
		200*time.Millisecond,
		"a deferred verification result must not enter chain selection",
	)
	select {
	case <-recycleCh:
		t.Fatal(
			"a deferred verification result must not recycle the connection",
		)
	case <-time.After(200 * time.Millisecond):
	}
	testutil.RequireNoReceive(
		t,
		ledgerCh,
		200*time.Millisecond,
		"a deferred header must not enter the ledger queue",
	)
	observed, _, retained := state.LookupObservedHeader(conn, header.Hash().Bytes())
	require.True(t, retained, "deferred header must remain available as raw ancestry")
	require.Zero(t, observed.HeaderCryptoVerificationGeneration)
	require.Contains(t, o.deferredHeaderRecovery, conn)

	baseline := ls.Tip().Point
	advancedPoint := ocommon.NewPoint(baseline.Slot+1, []byte("applied-block"))
	bus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(chain.ChainUpdateEventType, chain.ChainBlockEvent{
			Point: advancedPoint,
		}),
	)
	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"an applied chain update should re-intersect deferred peers",
	)
	resyncData, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, conn, resyncData.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		resyncData.Reason,
	)

	// The peer's re-intersection sends its linked suffix through the ordinary
	// callback parent-first. Once strict verification passes, both selector and
	// ledger admission resume on that fresh connection.
	verificationReady.Store(true)
	replayConn := newTestConnId("127.0.0.1:6012", "1.1.1.4:3001")
	require.True(t, state.AddClientConnId(replayConn))
	childHash := hash
	childHash[0]++
	childHeader := nonByronTestHeader{&testBlockHeader{
		hash:        childHash,
		prevHash:    header.Hash(),
		blockNumber: 2,
		slotNumber:  201,
	}}
	for _, replay := range []struct {
		header gledger.BlockHeader
		tip    ochainsync.Tip
	}{
		{header: header, tip: tip},
		{
			header: childHeader,
			tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(201, childHeader.Hash().Bytes()),
				BlockNumber: 2,
			},
		},
	} {
		require.NoError(t, o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: replayConn},
			0,
			replay.header,
			replay.tip,
		))
	}
	for _, replay := range []struct {
		header gledger.BlockHeader
	}{
		{header: header},
		{header: childHeader},
	} {
		observed := testutil.RequireReceive(
			t,
			tipCh,
			testutil.AsyncWait,
			"verified parent-first replay should re-enter selector admission",
		)
		require.IsType(t, chainselection.PeerTipUpdateEvent{}, observed.Data)
		ledgerEvent := testutil.RequireReceive(
			t,
			ledgerCh,
			testutil.AsyncWait,
			"verified parent-first replay should re-enter ledger admission",
		)
		ledgerData, ok := ledgerEvent.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, replay.header.Hash().Bytes(), ledgerData.Point.Hash)
		require.EqualValues(
			t,
			42,
			ledgerData.HeaderCryptoVerificationGeneration,
		)
	}
}

func TestDeferredHeaderRecoveryResyncsWhenAppliedTipAdvancesDuringVerification(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	ls := newTestLedgerState(t)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6013", "1.1.1.5:3001")
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		LedgerState:    ls,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	deferredErr := ls.ValidateChainSelectionHeaderCrypto(
		nonByronTestHeader{&testBlockHeader{slotNumber: 200}},
	)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
	advancedTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(1, []byte("applied-during-verification")),
		BlockNumber: 1,
	}
	var advanced atomic.Bool
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		if advanced.CompareAndSwap(false, true) {
			// Model a ledger apply during ancestry verification, before this
			// connection can register for the matching ChainUpdate event.
			ls.SetTipForTesting(advancedTip)
		}
		return 0, deferredErr
	}
	o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
		fn()
		return func() {}
	}

	header := nonByronTestHeader{&testBlockHeader{
		hash:       gledger.Blake2b256{0xee},
		slotNumber: 200,
	}}
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(200, header.Hash().Bytes()),
			BlockNumber: 1,
		},
	))

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"tip advancement during verification must trigger recovery without another block",
	)
	resyncData, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, conn, resyncData.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		resyncData.Reason,
	)
	require.NotContains(t, o.deferredHeaderRecovery, conn)
}

func TestDeferredHeaderRecoveryDoesNotRestoreClosedConnection(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	manager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	rawConn, err := ouroboros.New()
	require.NoError(t, err)
	conn := rawConn.Id()
	require.True(t, manager.AddConnection(rawConn, false, "127.0.0.1:6014"))

	o := newOuroboros(OuroborosConfig{
		EventBus:    bus,
		ConnManager: manager,
	})
	o.deferredHeaderRecovery = make(
		map[ouroboros.ConnectionId]deferredHeaderRecoveryState,
	)
	baseline := ochainsync.Tip{Point: ocommon.NewPoint(10, []byte("baseline"))}
	o.deferredHeaderRecovery[conn] = deferredHeaderRecoveryState{
		baseline: baseline,
		point:    ocommon.NewPoint(12, []byte("deferred")),
	}
	o.chainsyncScheduleAt = func(_ time.Time, _ func()) func() {
		// Reproduce close handling after retry removes the deferred marker but
		// before a failed resync restores it.
		require.True(t, manager.RemoveConnection(conn, rawConn))
		o.clearDeferredHeaderRecovery(conn)
		return func() {}
	}

	o.retryDeferredHeadersAfterLedgerAdvance(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{Point: ocommon.NewPoint(11, []byte("advanced"))},
	))

	require.NotContains(t, o.deferredHeaderRecovery, conn)
}

func TestDeferredHeaderRecoveryKeepsPeerWhenDeferredHeaderIsApplied(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	ls := newTestLedgerState(t)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6015", "1.1.1.6:3001")
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		LedgerState:    ls,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	header := nonByronTestHeader{&testBlockHeader{
		hash:       gledger.Blake2b256{0xef},
		slotNumber: 200,
	}}
	deferredErr := ls.ValidateChainSelectionHeaderCrypto(header)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 0, deferredErr
	}
	o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
		fn()
		return func() {}
	}
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
	require.Contains(t, o.deferredHeaderRecovery, conn)

	o.retryDeferredHeadersAfterLedgerAdvance(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{Point: tip.Point},
	))

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"an independently applied deferred header must not reconnect its peer",
	)
	require.NotContains(t, o.deferredHeaderRecovery, conn)
	tracked := state.GetTrackedClient(conn)
	require.NotNil(t, tracked)
	require.Equal(t, tip.Point, tracked.Cursor)
}

type deferredHeaderRecoveryFixture struct {
	o        *Ouroboros
	state    *dchainsync.State
	conn     ouroboros.ConnectionId
	resyncCh <-chan event.Event
}

// newDeferredHeaderRecoveryFixture builds a node whose producer authorization
// check defers every header, with one tracked chainsync client.
func newDeferredHeaderRecoveryFixture(
	t *testing.T,
	addr string,
) deferredHeaderRecoveryFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	ls := newTestLedgerState(t)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId(addr, "1.1.1.6:3001")
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		LedgerState:    ls,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	deferredErr := ls.ValidateChainSelectionHeaderCrypto(nonByronTestHeader{
		&testBlockHeader{hash: gledger.Blake2b256{0xee}, slotNumber: 200},
	})
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 0, deferredErr
	}
	o.chainsyncScheduleAt = func(_ time.Time, fn func()) func() {
		fn()
		return func() {}
	}
	return deferredHeaderRecoveryFixture{
		o:        o,
		state:    state,
		conn:     conn,
		resyncCh: resyncCh,
	}
}

// deferHeader delivers a header that the fixture defers and returns its tip.
func (f deferredHeaderRecoveryFixture) deferHeader(
	t *testing.T,
	hash byte,
	prevHash byte,
	slot uint64,
	blockNumber uint64,
) ochainsync.Tip {
	t.Helper()
	header := nonByronTestHeader{&testBlockHeader{
		hash:        gledger.Blake2b256{hash},
		prevHash:    gledger.Blake2b256{prevHash},
		slotNumber:  slot,
		blockNumber: blockNumber,
	}}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, header.Hash().Bytes()),
		BlockNumber: blockNumber,
	}
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		tip,
	))
	require.Contains(t, f.o.deferredHeaderRecovery, f.conn)
	return tip
}

func (f deferredHeaderRecoveryFixture) applyElsewhere(point ocommon.Point) {
	f.o.retryDeferredHeadersAfterLedgerAdvance(event.NewEvent(
		chain.ChainUpdateEventType,
		chain.ChainBlockEvent{Point: point},
	))
}

func TestDeferredHeaderRecoveryRecordsDeliveryWhenApplied(t *testing.T) {
	t.Parallel()
	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6016")
	tip := f.deferHeader(t, 0xef, 0x01, 200, 1)
	before := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, before)
	require.Zero(t, before.HeadersRecv)

	f.applyElsewhere(tip.Point)

	tracked := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tracked)
	require.Equal(t, tip.Point, tracked.Cursor)
	require.Equal(
		t,
		uint64(1),
		tracked.HeadersRecv,
		"a deferred header admitted after another peer applied it was delivered",
	)
}

func TestDeferredHeaderRecoveryFollowsNewestWithheldHeader(t *testing.T) {
	t.Parallel()
	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6017")
	f.deferHeader(t, 0xef, 0x01, 200, 1)
	newest := f.deferHeader(t, 0xf0, 0xef, 201, 2)

	f.applyElsewhere(newest.Point)

	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		200*time.Millisecond,
		"applying the newest withheld header must not reconnect its peer",
	)
	require.NotContains(t, f.o.deferredHeaderRecovery, f.conn)
	tracked := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tracked)
	require.Equal(t, newest.Point, tracked.Cursor)
	require.Equal(t, newest, tracked.Tip)
}

func TestDeferredHeaderRecoveryDoesNotAdmitAfterPeerRollback(t *testing.T) {
	t.Parallel()
	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6018")
	deferred := f.deferHeader(t, 0xef, 0x01, 200, 1)
	rollbackPoint := ocommon.NewPoint(100, []byte("rollback"))
	require.NoError(t, f.o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		rollbackPoint,
		deferred,
	))

	f.applyElsewhere(deferred.Point)

	tracked := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tracked)
	require.Equal(
		t,
		rollbackPoint,
		tracked.Cursor,
		"a header withheld before the peer rolled back is not its cursor",
	)
	testutil.RequireReceive(
		t,
		f.resyncCh,
		2*time.Second,
		"the rolled-back peer must still re-intersect",
	)
}

func TestDeferredHeaderRecoveryDoesNotRegressAdvancedCursor(t *testing.T) {
	t.Parallel()
	f := newDeferredHeaderRecoveryFixture(t, "127.0.0.1:6019")
	deferred := f.deferHeader(t, 0xef, 0x01, 200, 1)
	advanced := ochainsync.Tip{
		Point:       ocommon.NewPoint(201, []byte("advanced")),
		BlockNumber: 2,
	}
	require.True(t, f.state.RecordClientDelivery(f.conn, advanced.Point))
	require.True(t, f.state.AdmitClientTip(f.conn, advanced.Point, advanced))

	f.applyElsewhere(deferred.Point)

	require.NotContains(t, f.o.deferredHeaderRecovery, f.conn)
	tracked := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tracked)
	require.Equal(t, advanced.Point, tracked.Cursor)
	require.Equal(t, advanced, tracked.Tip)
	require.Equal(t, uint64(1), tracked.HeadersRecv)
}

func TestChainsyncClientRollForwardWithholdsUnauthenticatedCandidate(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, recycleCh := bus.Subscribe(ledger.ConnectionRecycleRequestedEventType)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6013", "1.1.1.4:3001")
	otherConn := newTestConnId("127.0.0.1:6014", "2.2.2.4:3001")
	require.True(t, state.AddClientConnId(conn))
	require.True(t, state.AddClientConnId(otherConn))
	corroborationWaitCalls := 0

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncAwaitCorroboration: func(
			context.Context,
			ouroboros.ConnectionId,
		) (bool, error) {
			corroborationWaitCalls++
			return true, nil
		},
	})
	o.chainsyncState = state
	o.eventBus = bus
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 0, ledger.ErrHeaderVerificationWithheld
	}

	header := newTestBlockHeader(400, 1, 0xab)
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	))
	if corroborationWaitCalls != 0 {
		t.Errorf(
			"withheld authentication reached Genesis corroboration wait %d times",
			corroborationWaitCalls,
		)
	}
	if !state.RecordHeaderForDedup(
		otherConn,
		ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes()),
	) {
		t.Error("withheld header was recorded in cross-peer dedup")
	}

	select {
	case <-tipCh:
		t.Fatal("an unauthenticated candidate must not be observed")
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case <-recycleCh:
		t.Fatal("missing local ancestry must not recycle the peer")
	case <-time.After(200 * time.Millisecond):
	}
	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"withheld ancestry must remain on the connection for later headers",
	)
	_, _, found := state.LookupObservedHeader(conn, header.Hash().Bytes())
	require.True(t, found, "withheld ancestry must remain reconstructable")
}

func TestChainsyncClientRollForwardRejectsVerdictStaleBeforeObservation(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6013", "1.1.1.5:3001")
	require.True(t, state.AddClientConnId(conn))

	var current atomic.Uint64
	current.Store(1)
	selector := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		HeaderCryptoVerificationGenerationCurrent: func(generation uint64) bool {
			return generation == current.Load()
		},
	})
	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			update chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			current.Store(2)
			return selector.PreparePeerTipAdmissionDisposition(update)
		},
	})
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(
		ouroboros.ConnectionId,
		gledger.BlockHeader,
	) (uint64, error) {
		return 1, nil
	}
	header := newTestBlockHeader(202, 1, 0xde)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(202, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	)
	require.ErrorIs(t, err, errChainsyncCandidateRejected)
	testutil.RequireNoReceive(t, ledgerCh, 200*time.Millisecond,
		"a stale verdict must be rejected before ledger publication")
	_, _, retained := state.LookupObservedHeader(conn, header.Hash().Bytes())
	require.False(t, retained)
}

// deferredHeaderFixture drives chainsyncClientRollForward with a verifier that
// always defers and a local chain whose block and header-queue tip is
// localTip.
type deferredHeaderFixture struct {
	o        *Ouroboros
	state    *dchainsync.State
	conn     ouroboros.ConnectionId
	tipCh    <-chan event.Event
	ledgerCh <-chan event.Event
	resyncCh <-chan event.Event
}

func newDeferredHeaderFixture(
	t *testing.T,
	localTip gledger.Blake2b256,
) deferredHeaderFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6021", "1.1.1.5:3001")
	require.True(t, state.AddClientConnId(conn))
	ls := newTestLedgerState(t)
	o := newOuroboros(OuroborosConfig{
		EventBus:       bus,
		LedgerState:    ls,
		ChainsyncState: state,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	deferredErr := ls.ValidateChainSelectionHeaderCrypto(
		nonByronTestHeader{&testBlockHeader{blockNumber: 1, slotNumber: 200}},
	)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))
	o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool { return true }
	o.chainSelectionVerifyHeaderCrypto = func(ouroboros.ConnectionId, gledger.BlockHeader) (uint64, error) {
		return 0, deferredErr
	}
	o.chainsyncLocalChainTips = func() ([]byte, []byte) {
		return localTip.Bytes(), localTip.Bytes()
	}
	return deferredHeaderFixture{
		o:        o,
		state:    state,
		conn:     conn,
		tipCh:    tipCh,
		ledgerCh: ledgerCh,
		resyncCh: resyncCh,
	}
}

func (f deferredHeaderFixture) rollForward(
	t *testing.T,
	header gledger.BlockHeader,
) {
	t.Helper()
	require.NoError(t, f.o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	))
}

// TestChainsyncClientRollForwardQueuesDeferredHeaderExtendingLocalChain is the
// Byron->Shelley regression: the first header of an epoch the local ledger has
// not entered defers producer authorization, and only applying its own block
// can clear that. Withholding it from block-fetch stalled sync for good, so a
// deferred header that extends the local chain, and its pipelined child, must
// reach the ledger queue and keep the selector following the peer. They remain
// in bounded ancestry history with generation zero until the ledger verifies
// their complete candidate path.
func TestChainsyncClientRollForwardQueuesDeferredHeaderExtendingLocalChain(
	t *testing.T,
) {
	t.Parallel()

	var localTip, firstHash, childHash gledger.Blake2b256
	localTip[0], firstHash[0], childHash[0] = 0x01, 0x02, 0x03
	f := newDeferredHeaderFixture(t, localTip)
	first := nonByronTestHeader{&testBlockHeader{
		hash:        firstHash,
		prevHash:    localTip,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	// The child arrives before the ledger has queued its parent: the local
	// tips still read localTip when it is evaluated.
	child := nonByronTestHeader{&testBlockHeader{
		hash:        childHash,
		prevHash:    firstHash,
		blockNumber: 11,
		slotNumber:  86420,
	}}

	for _, header := range []gledger.BlockHeader{first, child} {
		f.rollForward(t, header)
		tipEvt := testutil.RequireReceive(
			t,
			f.tipCh,
			5*time.Second,
			"a linear deferred header must keep the selector following the peer",
		)
		update, ok := tipEvt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(t, ok)
		require.Zero(t, update.HeaderCryptoVerificationGeneration)
		evt := testutil.RequireReceive(
			t,
			f.ledgerCh,
			5*time.Second,
			"a deferred header extending the local chain must reach the ledger queue",
		)
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, header.Hash().Bytes(), data.Point.Hash)
		require.Zero(t, data.HeaderCryptoVerificationGeneration)
		observed, _, retained := f.state.LookupObservedHeader(
			f.conn,
			header.Hash().Bytes(),
		)
		require.True(t, retained, "deferred ancestry must be retained")
		require.Zero(t, observed.HeaderCryptoVerificationGeneration)
	}
	f.o.deferredHeaderRecoveryMu.Lock()
	_, marked := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.False(t, marked)
}

// TestChainsyncClientRollForwardWithheldAncestryHeaderIsNotAcceptedTip pins
// that a header withheld because its fork ancestry cannot be authenticated
// does not become the connection's accepted tip. Otherwise its deferred child
// would pass as a linear extension of the local chain and reach the selector
// and the ledger queue although nothing below it was verified.
func TestChainsyncClientRollForwardWithheldAncestryHeaderIsNotAcceptedTip(
	t *testing.T,
) {
	t.Parallel()

	var localTip, forkParent, withheldHash, childHash gledger.Blake2b256
	localTip[0], forkParent[0], withheldHash[0], childHash[0] = 0x01, 0x0f, 0x20, 0x21
	f := newDeferredHeaderFixture(t, localTip)
	deferred := f.o.chainSelectionVerifyHeaderCrypto
	f.o.chainSelectionVerifyHeaderCrypto = func(
		conn ouroboros.ConnectionId,
		header gledger.BlockHeader,
	) (uint64, error) {
		if bytes.Equal(header.Hash().Bytes(), withheldHash.Bytes()) {
			return 0, ledger.ErrHeaderVerificationWithheld
		}
		return deferred(conn, header)
	}
	withheld := nonByronTestHeader{&testBlockHeader{
		hash:        withheldHash,
		prevHash:    forkParent,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	child := nonByronTestHeader{&testBlockHeader{
		hash:        childHash,
		prevHash:    withheldHash,
		blockNumber: 11,
		slotNumber:  86420,
	}}

	f.rollForward(t, withheld)
	f.o.acceptedHeaderMu.Lock()
	_, accepted := f.o.acceptedHeaderTips[f.conn]
	f.o.acceptedHeaderMu.Unlock()
	require.False(t, accepted, "a withheld header must not become the accepted tip")

	f.rollForward(t, child)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a deferred child of a withheld header must not enter the ledger queue",
	)
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		100*time.Millisecond,
		"a deferred child of a withheld header must not enter chain selection",
	)
}

// TestChainsyncClientRollForwardWithholdsDeferredForkHeader proves the
// linear-extension allowance does not reopen #5219: a deferred header whose
// parent is not on the local chain stays out of chain selection and the ledger.
// With nothing queued no applied-chain update can come to re-intersect the
// peer, so it is re-intersected at once, but only once per applied tip.
func TestChainsyncClientRollForwardWithholdsDeferredForkHeader(t *testing.T) {
	t.Parallel()

	var localTip, forkParent, forkHash gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0] = 0x01, 0x0f, 0x10
	f := newDeferredHeaderFixture(t, localTip)
	fork := nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 10,
		slotNumber:  86400,
	}}

	f.rollForward(t, fork)
	observed, _, retained := f.state.LookupObservedHeader(
		f.conn,
		forkHash.Bytes(),
	)
	require.True(t, retained, "deferred fork ancestry must be retained")
	require.Zero(t, observed.HeaderCryptoVerificationGeneration)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a deferred fork header must not enter the ledger queue",
	)
	testutil.RequireNoReceive(
		t,
		f.tipCh,
		100*time.Millisecond,
		"a deferred fork header must not enter chain selection",
	)
	f.o.deferredHeaderRecoveryMu.Lock()
	_, marked := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(t, marked)
	resync := testutil.RequireReceive(
		t,
		f.resyncCh,
		5*time.Second,
		"an idle queue must re-intersect the withheld peer",
	)
	resyncData, ok := resync.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	require.Equal(t, f.conn, resyncData.ConnectionId)
	require.Equal(
		t,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		resyncData.Reason,
	)

	f.o.cancelFutureHeaderResync(f.conn)
	f.rollForward(t, fork)
	testutil.RequireNoReceive(
		t,
		f.resyncCh,
		100*time.Millisecond,
		"a fork peer must not be re-intersected again at the same applied tip",
	)
}

// TestChainsyncClientRollForwardAcceptedHeaderTipIsRooted proves the
// connection's accepted header tip, which lets a pipelined deferred child
// count as a linear extension, names only a header that itself extends the
// local chain and that chain selection and admission accepted. Every other
// outcome clears it, so a later deferred header cannot reach the ledger queue
// by chaining onto a fork or onto a header that never reached the ledger.
func TestChainsyncClientRollForwardAcceptedHeaderTipIsRooted(t *testing.T) {
	t.Parallel()

	var localTip, firstHash, secondHash, forkParent gledger.Blake2b256
	localTip[0], firstHash[0], secondHash[0], forkParent[0] = 0x01, 0x02, 0x03, 0x0f
	first := nonByronTestHeader{&testBlockHeader{
		hash:        firstHash,
		prevHash:    localTip,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	onParent := func(parent gledger.Blake2b256) gledger.BlockHeader {
		return nonByronTestHeader{&testBlockHeader{
			hash:        secondHash,
			prevHash:    parent,
			blockNumber: 11,
			slotNumber:  86420,
		}}
	}
	isSecond := func(h []byte) bool { return bytes.Equal(h, secondHash.Bytes()) }

	type outcome struct {
		// parent of the second header; the zero value means first.
		parent gledger.Blake2b256
		// verify is the second header's verification result; nil defers it.
		verify   func() (uint64, error)
		observe  chainselection.PeerTipUpdateDisposition
		resolver bool
		// admit is the second header's future-header admission result.
		admit func() (bool, error)
		// ledgerRejects resolves the second header's ledger admission as
		// rejected.
		ledgerRejects bool
		wantErr       bool
		wantTip       bool
	}
	verified := func() (uint64, error) { return 1, nil }
	cases := map[string]outcome{
		"deferred header extending the accepted tip": {
			wantTip: true,
		},
		"verified header extending the accepted tip": {
			verify:  verified,
			wantTip: true,
		},
		"accepted by chain selection": {
			observe: chainselection.PeerTipUpdateAccepted,
			wantTip: true,
		},
		"verified header off the local chain": {
			parent: forkParent,
			verify: verified,
		},
		"deferred header off the local chain": {
			parent: forkParent,
		},
		"pending in chain selection": {
			observe: chainselection.PeerTipUpdatePending,
		},
		"rejected by chain selection": {
			observe: chainselection.PeerTipUpdateRejected,
			wantErr: true,
		},
		"unhandled with a resolver": {
			resolver: true,
		},
		"withheld by future-header admission": {
			admit: func() (bool, error) { return false, nil },
		},
		"future-header admission failed": {
			admit:   func() (bool, error) { return false, errors.New("admission failed") },
			wantErr: true,
		},
		"header verification failed": {
			verify: func() (uint64, error) { return 0, errors.New("bad KES signature") },
		},
		"past the forecast horizon": {
			verify: func() (uint64, error) { return 0, beyondForecastHorizonErr() },
		},
		"rejected by ledger admission": {
			verify:        verified,
			ledgerRejects: true,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newDeferredHeaderFixture(t, localTip)
			deferredVerify := f.o.chainSelectionVerifyHeaderCrypto
			f.o.chainSelectionVerifyHeaderCrypto = func(
				conn ouroboros.ConnectionId,
				h gledger.BlockHeader,
			) (uint64, error) {
				if tc.verify != nil && isSecond(h.Hash().Bytes()) {
					return tc.verify()
				}
				return deferredVerify(conn, h)
			}
			f.o.config.ChainsyncObservePeerTip = func(
				u chainselection.PeerTipUpdateEvent,
			) chainselection.PeerTipUpdateDisposition {
				if isSecond(u.ObservedTip.Point.Hash) {
					return tc.observe
				}
				return chainselection.PeerTipUpdateAccepted
			}
			if tc.resolver {
				f.o.config.ChainsyncResolvePeerTip = func(
					chainselection.PeerTipUpdateEvent,
					bool,
				) {
				}
			}
			admission := f.o.chainsyncHeaderAdmission
			f.o.chainsyncHeaderAdmission = func(
				ctx context.Context,
				e ledger.ChainsyncEvent,
			) (bool, error) {
				if tc.admit != nil && isSecond(e.Point.Hash) {
					return tc.admit()
				}
				if admission == nil {
					return true, nil
				}
				return admission(ctx, e)
			}

			f.rollForward(t, first)
			f.o.acceptedHeaderMu.Lock()
			primed := f.o.acceptedHeaderTips[f.conn]
			f.o.acceptedHeaderMu.Unlock()
			require.Equal(t, firstHash.Bytes(), primed)

			parent := tc.parent
			if parent == (gledger.Blake2b256{}) {
				parent = firstHash
			}
			second := onParent(parent)
			err := f.o.chainsyncClientRollForward(
				ochainsync.CallbackContext{ConnectionId: f.conn},
				0,
				second,
				ochainsync.Tip{
					Point: ocommon.NewPoint(
						second.SlotNumber(),
						second.Hash().Bytes(),
					),
					BlockNumber: second.BlockNumber(),
				},
			)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tc.ledgerRejects {
				for {
					evt := testutil.RequireReceive(
						t,
						f.ledgerCh,
						5*time.Second,
						"the verified header must reach the ledger queue",
					)
					data, ok := evt.Data.(ledger.ChainsyncEvent)
					require.True(t, ok)
					if !isSecond(data.Point.Hash) {
						continue
					}
					require.NotNil(t, data.PeerTipAdmission)
					data.PeerTipAdmission(false)
					break
				}
			}

			f.o.acceptedHeaderMu.Lock()
			got, recorded := f.o.acceptedHeaderTips[f.conn]
			f.o.acceptedHeaderMu.Unlock()
			if tc.wantTip {
				require.Equal(t, secondHash.Bytes(), got)
				return
			}
			require.False(
				t,
				recorded,
				"an outcome that does not accept a rooted header must clear the accepted tip",
			)
		})
	}
}

// TestChainsyncClientRollForwardWithholdsDeferredChildOfVerifiedFork proves a
// verified header that does not extend the local chain cannot become the
// parent that lets a deferred child through as a linear extension.
func TestChainsyncClientRollForwardWithholdsDeferredChildOfVerifiedFork(
	t *testing.T,
) {
	t.Parallel()

	var localTip, forkParent, forkHash, childHash gledger.Blake2b256
	localTip[0], forkParent[0], forkHash[0], childHash[0] = 0x01, 0x0f, 0x10, 0x11
	f := newDeferredHeaderFixture(t, localTip)
	fork := nonByronTestHeader{&testBlockHeader{
		hash:        forkHash,
		prevHash:    forkParent,
		blockNumber: 10,
		slotNumber:  86400,
	}}
	child := nonByronTestHeader{&testBlockHeader{
		hash:        childHash,
		prevHash:    forkHash,
		blockNumber: 11,
		slotNumber:  86420,
	}}
	deferredVerify := f.o.chainSelectionVerifyHeaderCrypto
	f.o.chainSelectionVerifyHeaderCrypto = func(
		conn ouroboros.ConnectionId,
		h gledger.BlockHeader,
	) (uint64, error) {
		if bytes.Equal(h.Hash().Bytes(), forkHash.Bytes()) {
			return 1, nil
		}
		return deferredVerify(conn, h)
	}

	f.rollForward(t, fork)
	evt := testutil.RequireReceive(
		t,
		f.ledgerCh,
		5*time.Second,
		"a verified fork header still reaches the ledger, which decides whether it fits",
	)
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, forkHash.Bytes(), data.Point.Hash)

	f.rollForward(t, child)
	testutil.RequireNoReceive(
		t,
		f.ledgerCh,
		100*time.Millisecond,
		"a deferred child of a fork header must not enter the ledger queue",
	)
	f.o.deferredHeaderRecoveryMu.Lock()
	_, marked := f.o.deferredHeaderRecovery[f.conn]
	f.o.deferredHeaderRecoveryMu.Unlock()
	require.True(t, marked)
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
	t.Parallel()

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
	o.chainSelectionVerifyHeaderCrypto = func(
		_ ouroboros.ConnectionId,
		h gledger.BlockHeader,
	) (uint64, error) {
		if h.Hash() == badHeader.Hash() {
			return 0, errors.New("boom: invalid VRF proof")
		}
		return 42, nil
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

// TestBuildDefaultChainsyncIntersectPointsSurvivesAnchorStorageFaultWhenPointsAreGood
// is the regression test for the rollback anchor being looked up on every
// chainsync client start rather than only when it can matter.
//
// The healthy path is served from the in-memory chain: with the primary chain
// tip at or ahead of the ledger tip, LedgerState.IntersectPoints answers out of
// chain.IntersectPoints and never reads the database. The anchor lookup always
// reads it. So an unconditional lookup gave a database fault a way to fail a
// chainsync start that had a full list of real points to offer -- and since
// this branch makes HandleOutboundConnEvent close the connection when the
// client fails to start, a transient storage fault would tear down a healthy
// peer over an answer that would have been discarded.
//
// The closed database here stands in for that transient fault. What the test
// pins is that the fault cannot reach a start whose intersect list is already
// good.
func TestBuildDefaultChainsyncIntersectPointsSurvivesAnchorStorageFaultWhenPointsAreGood(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, _, connId := newOutboundStartTestOuroboros(t, logger, bus)
	ls, db := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls

	// A healthy node: the ledger tip is a block the chain holds, so the
	// primary chain tip is not behind it and intersect points come from the
	// in-memory chain.
	chainTip := ls.PrimaryChainTip()
	require.Equal(t, uint64(5), chainTip.Point.Slot)
	ls.SetTipForTesting(ochainsync.Tip{
		Point:       chainTip.Point,
		BlockNumber: 5,
	})

	// Sanity: with the database up, the start builds real points.
	healthy, err := o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	require.NoError(t, err)
	require.True(
		t,
		intersectPointsHaveRealPoint(healthy),
		"fixture must produce a list with real points",
	)

	// The anchor lookup reads the database; the healthy path does not.
	require.NoError(t, dbtest.CloseDatabase(db))
	_, _, anchorErr := ls.RollbackWindowIntersectAnchor(context.Background())
	require.Error(
		t,
		anchorErr,
		"fixture must make the anchor lookup fail",
	)

	points, err := o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	require.NoError(
		t,
		err,
		"a storage fault in a lookup that cannot affect the result must not fail the chainsync start",
	)

	// The deeper points come from the database, so the fault legitimately
	// shortens the list. What must survive is the part that decides what
	// goes on the wire: a real leading point, and origin only as the last
	// resort rather than the whole request.
	require.True(
		t,
		intersectPointsHaveRealPoint(points),
		"the start must still offer a real point, not an origin-only request",
	)
	assert.Equal(t, chainTip.Point.Slot, points[0].Slot)
	assert.Equal(t, chainTip.Point.Hash, points[0].Hash)
	assert.True(
		t,
		isOriginPoint(points[len(points)-1]),
		"origin must remain the appended last resort",
	)
	assert.Equal(t, healthy[0], points[0])
}

// TestIntersectPointsHaveRealPointMatchesTheRescueCondition pins the predicate
// that the anchor-lookup gate and the rescue both use. They have to be the
// same test: a gate narrower than the rescue would skip the lookup for a list
// the rescue would have acted on, and the node would send the origin-only
// FindIntersect this path exists to prevent.
func TestIntersectPointsHaveRealPointMatchesTheRescueCondition(t *testing.T) {
	anchor := testAnchorPoint(1000)
	for _, tc := range []struct {
		name   string
		points []ocommon.Point
		real   bool
	}{
		{"empty", nil, false},
		{"origin only", []ocommon.Point{ocommon.NewPointOrigin()}, false},
		{
			"origin repeated",
			[]ocommon.Point{
				ocommon.NewPointOrigin(),
				ocommon.NewPointOrigin(),
			},
			false,
		},
		{"real point", []ocommon.Point{testAnchorPoint(42)}, true},
		{
			"real point then origin",
			[]ocommon.Point{
				testAnchorPoint(42),
				ocommon.NewPointOrigin(),
			},
			true,
		},
		{
			"origin then real point",
			[]ocommon.Point{
				ocommon.NewPointOrigin(),
				testAnchorPoint(42),
			},
			true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(
				t,
				tc.real,
				intersectPointsHaveRealPoint(tc.points),
			)
			// The rescue fires exactly when the predicate is false,
			// given a usable anchor. That equivalence is what lets the
			// gate skip the lookup whenever the predicate is true.
			_, rescued := finalizeChainsyncIntersectPoints(
				tc.points,
				anchor,
				true,
			)
			require.Equal(
				t,
				!tc.real,
				rescued,
				"the rescue must fire exactly when the gate would allow the lookup",
			)
		})
	}
}

func testAnchorPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, bytes.Repeat([]byte{0xab}, 32))
}

// TestFinalizeChainsyncIntersectPointsNeverOffersOriginAloneOnNonOriginChain is
// the regression test for the connection-recycle loop: while a rollback's
// metadata truncation is in flight the ledger can return no intersect points,
// and the "always append origin" fallback then made a fully synced node ask its
// peers to replay from genesis. Those genesis-era headers fail leader
// verification and recycle every connection until the truncation commits.
func TestFinalizeChainsyncIntersectPointsNeverOffersOriginAloneOnNonOriginChain(
	t *testing.T,
) {
	anchor := testAnchorPoint(2576716)

	for name, points := range map[string][]ocommon.Point{
		"nil points":      nil,
		"empty points":    {},
		"origin-only set": {ocommon.NewPointOrigin()},
	} {
		t.Run(name, func(t *testing.T) {
			got, rescued := finalizeChainsyncIntersectPoints(
				points,
				anchor,
				true,
			)

			require.NotEmpty(t, got)
			require.True(
				t,
				rescued,
				"expected the origin-only collapse to be reported",
			)
			require.False(
				t,
				len(got) == 1 && isOriginPoint(got[0]),
				"offered origin as the only intersect point while holding a rollback anchor at slot %d",
				anchor.Slot,
			)

			// The anchor leads, origin remains the last resort.
			assert.Equal(t, anchor.Slot, got[0].Slot)
			assert.Equal(t, anchor.Hash, got[0].Hash)
			assert.True(
				t,
				isOriginPoint(got[len(got)-1]),
				"origin must remain the final fallback point",
			)
		})
	}
}

// TestFinalizeChainsyncIntersectPointsKeepsOriginOnlyAtOrigin guards the other
// direction: a node that really is at origin (fresh sync) must still be allowed
// to ask for a full replay, otherwise it could never bootstrap.
func TestFinalizeChainsyncIntersectPointsKeepsOriginOnlyAtOrigin(t *testing.T) {
	got, rescued := finalizeChainsyncIntersectPoints(
		nil,
		ocommon.Point{},
		false,
	)

	require.Len(t, got, 1)
	assert.True(t, isOriginPoint(got[0]))
	assert.False(t, rescued)
}

// TestFinalizeChainsyncIntersectPointsPreservesRealPoints verifies the normal
// path is untouched: a healthy point list keeps its order and still gets origin
// appended as the final fallback for divergent-fork peers.
func TestFinalizeChainsyncIntersectPointsPreservesRealPoints(t *testing.T) {
	points := []ocommon.Point{
		ocommon.NewPoint(300, bytes.Repeat([]byte{0x03}, 32)),
		ocommon.NewPoint(200, bytes.Repeat([]byte{0x02}, 32)),
		ocommon.NewPoint(100, bytes.Repeat([]byte{0x01}, 32)),
	}

	got, rescued := finalizeChainsyncIntersectPoints(
		points,
		testAnchorPoint(300),
		true,
	)

	assert.False(t, rescued)
	require.Len(t, got, len(points)+1)
	for idx, point := range points {
		assert.Equal(t, point.Slot, got[idx].Slot)
		assert.Equal(t, point.Hash, got[idx].Hash)
	}
	assert.True(t, isOriginPoint(got[len(got)-1]))
}

// TestFinalizeChainsyncIntersectPointsDoesNotDoubleAppendOrigin verifies a list
// that already ends in origin is left alone.
func TestFinalizeChainsyncIntersectPointsDoesNotDoubleAppendOrigin(
	t *testing.T,
) {
	points := []ocommon.Point{
		ocommon.NewPoint(300, bytes.Repeat([]byte{0x03}, 32)),
		ocommon.NewPointOrigin(),
	}

	got, rescued := finalizeChainsyncIntersectPoints(
		points,
		testAnchorPoint(300),
		true,
	)

	assert.False(t, rescued)
	require.Len(t, got, 2)
	assert.True(t, isOriginPoint(got[1]))
}

// newTestLedgerStateWithChain builds a ledger whose primary chain holds
// blockCount blocks, so PrimaryChainTip reports a real (non-origin) tip.
func newTestLedgerStateWithChain(
	t *testing.T,
	blockCount uint64,
) (*ledger.LedgerState, *database.Database) {
	t.Helper()
	return newTestLedgerStateWithChainAt(t, blockCount, "")
}

// newTestLedgerStateWithChainAt is newTestLedgerStateWithChain over a database
// in dataDir. An empty dataDir makes dbtest.NewDatabase use a temporary
// file-backed SQLite database; pass a dataDir to control where it lives.
func newTestLedgerStateWithChainAt(
	t *testing.T,
	blockCount uint64,
	dataDir string,
) (*ledger.LedgerState, *database.Database) {
	return newTestLedgerStateWithChainAtAndConfig(t, blockCount, dataDir, nil)
}

func newTestLedgerStateWithChainAtAndConfig(
	t *testing.T,
	blockCount uint64,
	dataDir string,
	cardanoConfig *cardano.CardanoNodeConfig,
) (*ledger.LedgerState, *database.Database) {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: dataDir})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	var prevHash []byte
	for slot := uint64(1); slot <= blockCount; slot++ {
		hash := bytes.Repeat([]byte{byte(slot)}, 32)
		require.NoError(t, db.BlockCreate(models.Block{
			ID:       slot,
			Slot:     slot,
			Number:   slot,
			Hash:     hash,
			PrevHash: prevHash,
			Type:     1,
			Cbor:     []byte{0x80},
		}, nil))
		prevHash = hash
	}

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2160}),
	)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:          db,
		ChainManager:      cm,
		CardanoNodeConfig: cardanoConfig,
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return ls, db
}

// TestChainsyncNeverAsksPeerToReplayFromGenesisDuringRollback is the
// composition test for the incident: a node holding a real chain whose ledger
// tip block row has been removed (the window between the chain rewind and the
// end of the metadata truncation) must not build an origin-only FindIntersect
// list. It exercises both halves of the fix together -- the ledger anchoring
// its points on the chain tip, and chainsync refusing an origin-only list --
// because either one alone is sufficient to preserve the invariant.
func TestChainsyncNeverAsksPeerToReplayFromGenesisDuringRollback(t *testing.T) {
	o := &Ouroboros{}
	o.ledgerState, _ = newTestLedgerStateWithChain(t, 5)

	chainTip := o.ledgerState.PrimaryChainTip()
	require.False(
		t,
		isOriginPoint(chainTip.Point),
		"fixture must hold a non-origin chain",
	)

	// The ledger tip names a block that is not in the metadata database,
	// which is what a chain rewind leaves behind until ls.rollback's
	// truncation commits and reassigns ls.currentTip.
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ocommon.NewPoint(2576729, bytes.Repeat([]byte{0xe8}, 32)),
		BlockNumber: 112915,
	})

	points, err := o.ledgerState.IntersectPoints(
		context.Background(),
		chainsyncIntersectPointCount,
	)
	require.NoError(t, err)

	anchor, hasAnchor, err := o.ledgerState.RollbackWindowIntersectAnchor(
		context.Background(),
	)
	require.NoError(t, err)
	got, _ := finalizeChainsyncIntersectPoints(
		normalizeIntersectPoints(points),
		anchor,
		hasAnchor,
	)

	require.NotEmpty(t, got)
	require.False(
		t,
		len(got) == 1 && isOriginPoint(got[0]),
		"chainsync would have asked the peer to replay from genesis while holding a chain at slot %d",
		chainTip.Point.Slot,
	)
	assert.False(
		t,
		isOriginPoint(got[0]),
		"the leading intersect point must be a real chain point",
	)
	assert.True(
		t,
		isOriginPoint(got[len(got)-1]),
		"origin must remain the final fallback point",
	)
}

// TestFinalizeChainsyncIntersectPointsRefusesAheadForkAnchor is the regression
// test for the review finding that the origin-only rescue re-introduced the
// violation one layer up.
//
// When the point list is empty because the primary chain is AHEAD on a fork
// that does not contain the applied ledger tip, the ledger deliberately reports
// no rollback anchor. Seeding from the raw chain tip in that case would
// advertise unapplied fork state. The list must stay origin-only, exactly as
// before the rescue existed.
func TestFinalizeChainsyncIntersectPointsRefusesAheadForkAnchor(t *testing.T) {
	got, rescued := finalizeChainsyncIntersectPoints(
		nil,
		// A caller that wrongly passes a real ahead-fork point must still
		// be refused when the ledger says there is no anchor.
		testAnchorPoint(999999),
		false,
	)

	assert.False(t, rescued, "must not rescue without a ledger-approved anchor")
	require.Len(t, got, 1)
	assert.True(
		t,
		isOriginPoint(got[0]),
		"an ahead-fork chain tip must never seed the intersect list",
	)
}

// ledgerTipHashAbsentFromChain is a hash that deliberately matches no block
// built by newTestLedgerStateWithChain (whose block at slot N has hash
// bytes.Repeat([]byte{byte(N)}, 32)). Using it as the ledger tip point makes
// that tip's metadata row absent AND places it off the primary chain, which is
// the state a chain rewind leaves behind.
//
// This distinction is load-bearing: swapping this for the real block hash
// converts the test below into the ordinary chain-ahead test beside it,
// which asserts the opposite outcome.
var ledgerTipHashAbsentFromChain = bytes.Repeat([]byte{0xe8}, 32)

// TestIntersectPointsChainAheadWithLedgerTipRowMissingStaysOriginOnly is the
// case: the primary chain is AHEAD of the ledger tip, and the ledger
// tip's own block row is missing (so the tip is not an ancestor on the primary
// chain either). primaryChainTipAtOrAheadOfLedgerTip's ancestor check fails,
// the authoritative path finds no tip row, and the ahead-gate refuses to anchor
// on unapplied forward work.
//
// Nothing may be advertised: the list must stay origin-only, matching upstream
// TestIntersectPointsDoesNotUsePrimaryChainWhenLedgerTipMissing.
func TestIntersectPointsChainAheadWithLedgerTipRowMissingStaysOriginOnly(
	t *testing.T,
) {
	o := &Ouroboros{}
	o.ledgerState, _ = newTestLedgerStateWithChain(t, 5)

	ledgerTipPoint := ocommon.NewPoint(2, ledgerTipHashAbsentFromChain)
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ledgerTipPoint,
		BlockNumber: 2,
	})

	// Pin the preconditions, so this cannot silently become the
	// chain-ahead-with-a-real-tip case below.
	_, err := o.ledgerState.GetBlock(ledgerTipPoint)
	require.Error(t, err, "fixture requires the ledger tip row to be absent")
	require.Greater(
		t,
		o.ledgerState.PrimaryChainTip().Point.Slot,
		ledgerTipPoint.Slot,
		"fixture requires the primary chain to be ahead of the ledger tip",
	)

	points, err := o.ledgerState.IntersectPoints(
		context.Background(),
		chainsyncIntersectPointCount,
	)
	require.NoError(t, err)
	require.Empty(
		t,
		points,
		"unapplied ahead-fork state must not be advertised (#2309)",
	)

	anchor, hasAnchor, err := o.ledgerState.RollbackWindowIntersectAnchor(
		context.Background(),
	)
	require.NoError(t, err)
	require.False(
		t,
		hasAnchor,
		"ledger must refuse to anchor on a chain tip ahead of its own tip",
	)

	got, rescued := finalizeChainsyncIntersectPoints(
		normalizeIntersectPoints(points),
		anchor,
		hasAnchor,
	)
	assert.False(t, rescued)
	require.Len(t, got, 1)
	assert.True(t, isOriginPoint(got[0]))
}

// TestIntersectPointsChainAheadWithLedgerTipRowPresentAdvertisesChainPoints is
// the complementary case, and the one that must NOT be conflated with the
// test above: the primary chain is ahead of the ledger tip, but the
// ledger tip is a real block on that chain. The chain is then a valid forward
// extension, primaryChainTipAtOrAheadOfLedgerTip's ancestor check passes, and
// the chain's real points are advertised with origin appended as the usual
// last resort.
//
// No rescue is involved here: the ledger already returned real points, so the
// rollback anchor is absent and must stay absent.
func TestIntersectPointsChainAheadWithLedgerTipRowPresentAdvertisesChainPoints(
	t *testing.T,
) {
	o := &Ouroboros{}
	o.ledgerState, _ = newTestLedgerStateWithChain(t, 5)

	// The real block-2 hash produced by the fixture, so the row exists and
	// the tip is an ancestor on the primary chain.
	ledgerTipPoint := ocommon.NewPoint(2, bytes.Repeat([]byte{0x02}, 32))
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ledgerTipPoint,
		BlockNumber: 2,
	})

	_, err := o.ledgerState.GetBlock(ledgerTipPoint)
	require.NoError(t, err, "fixture requires the ledger tip row to be present")

	points, err := o.ledgerState.IntersectPoints(
		context.Background(),
		chainsyncIntersectPointCount,
	)
	require.NoError(t, err)
	require.Len(t, points, 5, "the chain's real points must be advertised")
	assert.Equal(t, uint64(5), points[0].Slot, "newest chain point leads")

	anchor, hasAnchor, err := o.ledgerState.RollbackWindowIntersectAnchor(
		context.Background(),
	)
	require.NoError(t, err)
	assert.False(
		t,
		hasAnchor,
		"no rollback is in flight, so no anchor may be offered",
	)

	got, rescued := finalizeChainsyncIntersectPoints(
		normalizeIntersectPoints(points),
		anchor,
		hasAnchor,
	)
	assert.False(t, rescued, "real points need no rescue")
	require.Len(t, got, len(points)+1)
	assert.Equal(t, uint64(5), got[0].Slot)
	assert.True(
		t,
		isOriginPoint(got[len(got)-1]),
		"origin remains the appended last resort",
	)
}

// TestRollbackWindowIntersectAnchorPropagatesStorageError verifies the anchor
// lookup surfaces a storage fault instead of reporting "no anchor". The
// chainsync call site fails the client start on this error; downgrading it
// would send the peer an origin-only intersect list, i.e. a request to replay
// the chain from genesis.
func TestRollbackWindowIntersectAnchorPropagatesStorageError(t *testing.T) {
	o := &Ouroboros{}
	ls, db := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls

	setTestLedgerTip(t, o, ochainsync.Tip{
		Point:       ocommon.NewPoint(2, ledgerTipHashAbsentFromChain),
		BlockNumber: 2,
	})

	// Sanity: healthy database answers without error.
	_, _, err := ls.RollbackWindowIntersectAnchor(context.Background())
	require.NoError(t, err)

	require.NoError(t, dbtest.CloseDatabase(db))

	_, hasAnchor, err := ls.RollbackWindowIntersectAnchor(context.Background())
	require.Error(t, err, "storage failure must not be reported as no anchor")
	assert.False(t, hasAnchor)
}

// TestBuildDefaultChainsyncIntersectPointsOffersRollbackPointInWindow is the
// end-to-end composition test for the rollback window, driven through the
// function the chainsync client actually calls rather than through
// finalizeChainsyncIntersectPoints directly.
//
// Every other test in this file supplies the intersect points and the anchor
// itself, so none of them pins the runtime path
// buildDefaultChainsyncIntersectPoints takes: LedgerState.IntersectPoints ->
// finalizeChainsyncIntersectPoints -> the list handed to MsgFindIntersect. This
// one drives a ledger genuinely in the window (ledger tip row absent, primary
// chain tip strictly below the ledger tip) and asserts what goes on the wire.
func TestBuildDefaultChainsyncIntersectPointsOffersRollbackPointInWindow(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, _, connId := newOutboundStartTestOuroboros(t, logger, bus)
	ls, _ := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls

	// The window: the chain has been rewound to slot 5 while the ledger tip
	// still names a block at slot 9 whose row the rewind removed.
	ls.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(9, ledgerTipHashAbsentFromChain),
		BlockNumber: 9,
	})

	chainTip := ls.PrimaryChainTip()
	require.Equal(t, uint64(5), chainTip.Point.Slot)
	_, err := ls.GetBlock(ocommon.NewPoint(9, ledgerTipHashAbsentFromChain))
	require.Error(t, err, "fixture requires the ledger tip row to be absent")

	points, err := o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	require.NoError(t, err)

	// What actually matters on the wire: we must not ask the peer to replay
	// from genesis, and the newest point offered must be the rollback point.
	require.NotEmpty(t, points)
	require.False(
		t,
		len(points) == 1 && isOriginPoint(points[0]),
		"a node holding a chain must never send an origin-only FindIntersect",
	)
	assert.Equal(t, chainTip.Point.Slot, points[0].Slot)
	assert.Equal(t, chainTip.Point.Hash, points[0].Hash)
	assert.True(
		t,
		isOriginPoint(points[len(points)-1]),
		"origin must remain the appended last resort",
	)

	// No point may name a block above the rollback point.
	for _, point := range points {
		if isOriginPoint(point) {
			continue
		}
		assert.LessOrEqual(t, point.Slot, chainTip.Point.Slot)
	}
}

// TestBuildDefaultChainsyncIntersectPointsStaysOriginOnlyOnAheadFork drives the
// shape through the real call site: the primary chain is ahead of the
// ledger tip on a fork that does not contain it, and the ledger tip row is
// missing. Nothing may be advertised, so the wire request is origin-only.
func TestBuildDefaultChainsyncIntersectPointsStaysOriginOnlyOnAheadFork(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, _, connId := newOutboundStartTestOuroboros(t, logger, bus)
	ls, _ := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls

	ls.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(2, ledgerTipHashAbsentFromChain),
		BlockNumber: 2,
	})
	require.Greater(t, ls.PrimaryChainTip().Point.Slot, uint64(2))

	points, err := o.buildDefaultChainsyncIntersectPoints(
		context.Background(),
		connId,
	)
	require.NoError(t, err)

	require.Len(t, points, 1)
	assert.True(
		t,
		isOriginPoint(points[0]),
		"unapplied ahead-fork state must not be advertised (#2309)",
	)
}

// newOutboundStartTestOuroboros builds an Ouroboros wired with every
// dependency hasDependencies() requires, so HandleOutboundConnEvent actually
// reaches the chainsync start rather than dropping the event as unwired.
func newOutboundStartTestOuroboros(
	t *testing.T,
	logger *slog.Logger,
	bus *event.EventBus,
) (*Ouroboros, *connmanager.ConnectionManager, ouroboros.ConnectionId) {
	t.Helper()

	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: bus,
			Logger:   logger,
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})

	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		ouroboros_mock.ConversationKeepAlive,
	)
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(mockConn),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithKeepAliveConfig(
			keepalive.NewConfig(
				keepalive.WithCookie(ouroboros_mock.MockKeepAliveCookie),
				keepalive.WithPeriod(30*time.Second),
				keepalive.WithTimeout(15*time.Second),
			),
		),
	)
	require.NoError(t, err)
	connManager.AddConnection(oConn, false, "127.0.0.1:1234")

	m, err := mempool.NewMempool(mempool.MempoolConfig{
		Logger:          logger,
		PromRegistry:    prometheus.NewRegistry(),
		Validator:       txsubmissionTestValidator{},
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, m.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, m.Stop(context.Background()))
	})

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.eventBus = bus
	o.connManager = connManager
	o.chainsyncState = dchainsync.NewState(bus, nil)
	o.mempool = &mempool.FIFO{Mempool: m}
	o.peerGov = peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	return o, connManager, oConn.Id()
}

// TestOutboundChainsyncStartFailureClosesConnection is the regression test for
// an outbound peer left half-connected.
//
// When chainsync fails to start, HandleOutboundConnEvent rolls back the
// registration and returns before starting txsubmission. It used to leave the
// TCP connection open, so peer governance still counted the peer as connected,
// nothing retried, and the peer was effectively lost for the lifetime of the
// connection. That matters for transient causes -- an intersect-point or
// rollback-anchor lookup hitting a storage fault -- which are recoverable on a
// reconnect but were never retried.
//
// The failure is induced here by closing the ledger's database, which makes
// the rollback-anchor lookup return a storage error, which fails the chainsync
// client start.
func TestOutboundChainsyncStartFailureClosesConnection(t *testing.T) {
	logBuf := &lockedBuffer{}
	logger := slog.New(
		slog.NewJSONHandler(
			logBuf,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, connManager, connId := newOutboundStartTestOuroboros(t, logger, bus)

	// Snapshot AFTER the harness has started its own workers (mempool,
	// event bus, connection manager, mock connection) so only goroutines
	// created from here on are attributed to the code under test.
	//
	// IgnoreCurrent is what makes goleak usable in this package: a bare
	// goleak call inspects the whole process and would trip on unrelated
	// pre-existing goroutines from other tests. A NumGoroutine baseline was
	// tried first and is not sensitive enough here -- tearing the connection
	// down frees several goroutines, so the total lands below the baseline
	// and a single stranded goroutine hides inside that slack.
	leakOpt := goleak.IgnoreCurrent()
	baselineGoroutines := runtime.NumGoroutine()

	// A ledger whose database is closed makes the rollback-anchor lookup
	// return a storage error rather than "no anchor".
	ls, db := newTestLedgerStateWithChain(t, 5)
	o.ledgerState = ls
	o.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       ocommon.NewPoint(2, ledgerTipHashAbsentFromChain),
		BlockNumber: 2,
	})
	require.NoError(t, dbtest.CloseDatabase(db))
	_, _, anchorErr := ls.RollbackWindowIntersectAnchor(context.Background())
	require.Error(t, anchorErr, "fixture must produce an anchor lookup error")

	o.HandleOutboundConnEvent(event.NewEvent(
		peergov.OutboundConnectionEventType,
		peergov.OutboundConnectionEvent{ConnectionId: connId},
	))

	// The connection must not be left open: peer governance has to observe
	// the failure to apply backoff and reconnect.
	require.Eventually(
		t,
		func() bool { return connManager.GetConnectionById(connId) == nil },
		2*time.Second,
		20*time.Millisecond,
		"outbound connection was left open after chainsync start failure",
	)

	// The tracked chainsync client registration must have been rolled back.
	require.False(
		t,
		o.chainsyncState.HasClientConnId(connId),
		"chainsync client registration must be rolled back",
	)
	require.Zero(t, o.chainsyncState.ClientConnCount())

	require.Contains(
		t,
		logBuf.String(),
		"failed to start chainsync client, closing outbound connection",
	)

	// The torn-down connection must not strand goroutines. The count must
	// come back to (or below) the post-setup baseline; teardown is
	// asynchronous, hence the poll.
	require.Eventually(
		t,
		func() bool { return runtime.NumGoroutine() <= baselineGoroutines },
		10*time.Second,
		50*time.Millisecond,
		"goroutines did not return to the post-setup baseline after the failed chainsync start (baseline %d, now %d)",
		baselineGoroutines,
		runtime.NumGoroutine(),
	)
	// And, unlike the count above, this catches a single stranded goroutine.
	goleak.VerifyNone(t, leakOpt)
}

// TestCloseOutboundConnAfterChainsyncFailureClosesStartedConn covers the
// ordinary case: the connection chainsync failed on is still the manager's
// current connection for its id, so it is closed and peer governance can
// reconnect.
func TestCloseOutboundConnAfterChainsyncFailureClosesStartedConn(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, connManager, connId := newOutboundStartTestOuroboros(t, logger, bus)
	startedConn := connManager.GetConnectionById(connId)
	require.NotNil(t, startedConn)

	o.closeOutboundConnAfterChainsyncFailure(connId, startedConn)

	require.Eventually(
		t,
		func() bool { return connManager.GetConnectionById(connId) == nil },
		2*time.Second,
		20*time.Millisecond,
		"the connection chainsync failed on must be closed",
	)
}

// TestCloseOutboundConnAfterChainsyncFailureSparesReplacement is the
// regression test for closing the wrong connection.
//
// ConnectionId is a (local addr, remote addr) pair, so a reconnect to the same
// peer can produce the same id. If the connection we started on has already
// been replaced by the time the failed start returns, closing whatever now
// holds the id would tear down a healthy peer for its predecessor's failure.
//
// The replacement is modelled by handing the helper a connection that is not
// the one the manager currently holds for that id, which is exactly the state
// the guard has to detect.
func TestCloseOutboundConnAfterChainsyncFailureSparesReplacement(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, connManager, connId := newOutboundStartTestOuroboros(t, logger, bus)
	replacement := connManager.GetConnectionById(connId)
	require.NotNil(t, replacement)

	// A different connection object, standing in for the one we started on
	// before it was replaced under the same id.
	staleConn, err := ouroboros.New(
		ouroboros.WithConnection(
			ouroboros_mock.NewConnection(
				ouroboros_mock.ProtocolRoleClient,
				ouroboros_mock.ConversationKeepAlive,
			),
		),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = staleConn.Close() })
	require.NotSame(t, replacement, staleConn)

	o.closeOutboundConnAfterChainsyncFailure(connId, staleConn)

	// The replacement must be left strictly alone. Removal from the manager
	// is asynchronous, so assert the connection never disappears over a
	// window rather than checking once: an immediate NotNil would pass even
	// if the helper had just closed it.
	require.Never(
		t,
		func() bool { return connManager.GetConnectionById(connId) == nil },
		2*time.Second,
		50*time.Millisecond,
		"the replacement connection must not be closed",
	)
	require.Same(t, replacement, connManager.GetConnectionById(connId))
}

// TestCloseOutboundConnAfterChainsyncFailureHandlesMissingConn verifies the
// helper is a no-op when there was no connection to start with, and when the
// connection has already gone away entirely.
func TestCloseOutboundConnAfterChainsyncFailureHandlesMissingConn(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	o, connManager, connId := newOutboundStartTestOuroboros(t, logger, bus)

	// Nil started connection: nothing to close, must not panic.
	require.NotPanics(t, func() {
		o.closeOutboundConnAfterChainsyncFailure(connId, nil)
	})
	require.NotNil(t, connManager.GetConnectionById(connId))

	// Connection already gone: the id no longer resolves, so the guard sees
	// current == nil != startedConn and leaves well alone.
	startedConn := connManager.GetConnectionById(connId)
	require.NoError(t, startedConn.Close())
	require.Eventually(
		t,
		func() bool { return connManager.GetConnectionById(connId) == nil },
		2*time.Second,
		20*time.Millisecond,
	)
	require.NotPanics(t, func() {
		o.closeOutboundConnAfterChainsyncFailure(connId, startedConn)
	})
}

type patienceRollForwardFixture struct {
	o     *Ouroboros
	state *dchainsync.State
	conn  ouroboros.ConnectionId
	now   time.Time
}

func newPatienceRollForwardFixture(
	t *testing.T,
	eligible bool,
) *patienceRollForwardFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	f := &patienceRollForwardFixture{
		conn: newTestConnId("127.0.0.1:6000", "10.0.0.1:3001"),
		now:  time.Unix(1_700_000_000, 0),
	}
	cfg := dchainsync.DefaultConfig()
	cfg.Patience = dchainsync.PatienceConfig{
		Enabled:  true,
		Capacity: 100,
		Rate:     1,
	}
	cfg.PatienceActiveFunc = func() bool { return true }
	cfg.Now = func() time.Time { return f.now }
	f.state = dchainsync.NewStateWithConfig(bus, nil, cfg)
	require.True(t, f.state.AddClientConnId(f.conn))
	// Start the leak as an earlier accepted header would have.
	f.state.PatienceHeaderAccepted(f.conn, 0, false)
	f.o = newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return eligible
		},
	})
	f.o.chainsyncState = f.state
	return f
}

func (f *patienceRollForwardFixture) rollForward(
	t *testing.T,
	header gledger.BlockHeader,
) {
	t.Helper()
	require.NoError(t, f.rollForwardError(header))
}

func (f *patienceRollForwardFixture) rollForwardError(
	header gledger.BlockHeader,
) error {
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(1_000_000, []byte("far")),
		BlockNumber: 1_000_000,
	}
	return f.o.chainsyncClientRollForwardAt(
		ochainsync.CallbackContext{ConnectionId: f.conn},
		0,
		header,
		tip,
		f.now,
	)
}

// TestRollForwardChargesPatienceOnlyUntilArrival pins the hook placement: the
// peer is charged up to the header's arrival, local work inside the callback
// is free, and an accepted header resumes the leak with a token.
func TestRollForwardChargesPatienceOnlyUntilArrival(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	f.o.chainsyncHeaderAdmission = func(
		context.Context,
		ledger.ChainsyncEvent,
	) (bool, error) {
		f.now = f.now.Add(time.Hour)
		return true, nil
	}

	f.now = f.now.Add(40 * time.Second)
	f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))

	tc := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tc)
	require.False(t, tc.Patience.Exhausted)
	require.False(t, tc.Patience.Paused, "an accepted header resumes the leak")
	require.Equal(t, uint64(1), tc.Patience.BestBlockNumber)
	require.InDelta(t, 61, tc.Patience.Tokens, 1e-9)
}

// A header held at the Limit on Eagerness waits inside the callback, after
// the peer was charged up to the header's arrival. The wait is this node's
// own, so an hour of it must not drain the bucket.
func TestRollForwardEagernessWaitIsNotChargedToPatience(t *testing.T) {
	t.Parallel()
	f := newPatienceRollForwardFixture(t, true)
	waited := false
	f.o.config.ChainsyncAwaitEagerness = func(
		context.Context,
		ouroboros.ConnectionId,
		uint64,
		func() ochainsync.Tip,
	) error {
		waited = true
		f.now = f.now.Add(time.Hour)
		return nil
	}

	f.now = f.now.Add(40 * time.Second)
	f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))

	require.True(t, waited, "the eagerness wait must be consulted")
	tc := f.state.GetTrackedClient(f.conn)
	require.NotNil(t, tc)
	require.False(t, tc.Patience.Exhausted)
	require.InDelta(t, 61, tc.Patience.Tokens, 1e-9)
}

func TestRollForwardGrantsNoPatienceForRejectedHeaders(t *testing.T) {
	t.Parallel()
	t.Run("verification failure", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, true)
		f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool {
			return true
		}
		f.o.chainSelectionVerifyHeaderCrypto = func(
			ouroboros.ConnectionId,
			gledger.BlockHeader,
		) (uint64, error) {
			return 0, errors.New("bad vrf")
		}
		f.now = f.now.Add(40 * time.Second)
		f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.InDelta(t, 60, tc.Patience.Tokens, 1e-9)
	})
	t.Run("verification withheld", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, true)
		f.o.chainSelectionShouldVerifyHeaderCrypto = func(uint64) bool {
			return true
		}
		f.o.chainSelectionVerifyHeaderCrypto = func(
			ouroboros.ConnectionId,
			gledger.BlockHeader,
		) (uint64, error) {
			return 0, ledger.ErrHeaderVerificationWithheld
		}
		f.now = f.now.Add(40 * time.Second)
		f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.False(t, tc.Patience.Paused)
		require.InDelta(t, 60, tc.Patience.Tokens, 1e-9)
	})
	t.Run("not ingress eligible", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, false)
		f.now = f.now.Add(40 * time.Second)
		f.rollForward(t, newTestBlockHeader(100, 1, 0xaa))
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.True(
			t,
			tc.Patience.Paused,
			"a peer whose headers are not verified is not held to patience",
		)
	})
	t.Run("synchronous selector rejection", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, true)
		f.o.config.ChainsyncObservePeerTip = func(
			chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return chainselection.PeerTipUpdateRejected
		}
		f.now = f.now.Add(40 * time.Second)
		require.ErrorIs(
			t,
			f.rollForwardError(newTestBlockHeader(100, 1, 0xaa)),
			errChainsyncCandidateRejected,
		)
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.InDelta(t, 60, tc.Patience.Tokens, 1e-9)
		require.False(t, tc.Patience.Paused,
			"a rejected eligible header resumes the patience leak")
	})
	t.Run("sender removed while awaiting eagerness", func(t *testing.T) {
		t.Parallel()
		f := newPatienceRollForwardFixture(t, true)
		f.o.config.ChainsyncAwaitEagerness = func(
			context.Context,
			ouroboros.ConnectionId,
			uint64,
			func() ochainsync.Tip,
		) error {
			return chainselection.ErrEagernessPeerUntracked
		}
		f.now = f.now.Add(40 * time.Second)
		require.ErrorIs(
			t,
			f.rollForwardError(newTestBlockHeader(100, 1, 0xaa)),
			chainselection.ErrEagernessPeerUntracked,
		)
		tc := f.state.GetTrackedClient(f.conn)
		require.Zero(t, tc.Patience.BestBlockNumber)
		require.InDelta(t, 60, tc.Patience.Tokens, 1e-9)
		require.False(t, tc.Patience.Paused,
			"a withheld eligible header resumes the patience leak")
	})
}

// The tests in this file drive Dingo's real ChainSync server callbacks over a
// real protocol connection using the shared ouroboros-mock harness,
// and assert the exact protocol messages the
// server emits back.
//
// This is the difference that matters versus calling the callbacks directly:
// a callback that consumes an iterator result but never sends the matching
// RollForward/RollBackward still satisfies a "did the iterator advance?"
// assertion, but fails here, because here the test only sees what actually
// went onto the wire.
//
// # Asynchronous paths
//
// Immediately after sending AwaitReply, chainsyncServerRequestNext resolves the
// peer with o.connManager.GetConnectionById(ctx.ConnectionId) so it can abandon
// the blocked iterator read if the peer goes away. The fixture therefore
// registers the harness's connection (ouroboros-mock v0.16.0's
// Harness.ServerConnection) with Dingo's ConnManager before driving anything.
// Without that registration the lookup fails and the callback tears the
// connection down instead of serving, which is why these paths previously
// needed a local harness.
//
// The only callbacks still invoked directly are the ones whose assertion *is*
// the returned error: the protocol layer converts a callback error into
// connection teardown rather than an observable message, so there is nothing
// on the wire to assert. Those tests still run against the fixture's real
// connection and server.

// chainsyncServerFixture pairs a Dingo Ouroboros instance with a shared
// ouroboros-mock ChainSync harness driving its server callbacks.
type chainsyncServerFixture struct {
	o       *Ouroboros
	h       *csmock.Harness
	limiter *chainsyncFindIntersectRateLimiter

	// conn is the harness's server-under-test connection, registered with
	// Dingo's ConnManager so callbacks that resolve their peer through it
	// (the post-AwaitReply async path) can run.
	conn *ouroboros.Connection

	// closedCh receives connmanager connection-closed events, so tests can
	// assert that an async send failure reached normal lifecycle handling.
	closedCh <-chan event.Event

	// connIdMu guards connId, which the server callbacks record from the
	// protocol goroutine while tests read it.
	connIdMu sync.Mutex
	connId   *ouroboros.ConnectionId
}

// callbackContext builds the callback context the protocol would deliver, for
// the few tests that must call a server callback directly because what they
// assert is the error it returns — something the protocol layer converts into
// connection teardown rather than an observable message.
func (f *chainsyncServerFixture) callbackContext() ochainsync.CallbackContext {
	return ochainsync.CallbackContext{
		ConnectionId: f.conn.Id(),
		Server:       f.h.Server(),
	}
}

// registerClientAtOrigin registers a downstream client that has already had
// its initial rollback, so the next RequestNext consults the iterator.
func (f *chainsyncServerFixture) registerClientAtOrigin(
	t *testing.T,
) *dchainsync.ChainsyncClientState {
	t.Helper()
	clientState, err := f.o.chainsyncState.AddClient(
		f.conn.Id(),
		ocommon.NewPointOrigin(),
	)
	require.NoError(t, err)
	clientState.NeedsInitialRollback = false
	return clientState
}

// recordConnId notes the connection ID the harness assigned, so tests can look
// up the server-side client state the callbacks registered for it.
func (f *chainsyncServerFixture) recordConnId(connId ouroboros.ConnectionId) {
	f.connIdMu.Lock()
	defer f.connIdMu.Unlock()
	f.connId = &connId
}

// observedConnId returns the recorded connection ID, or false if no server
// callback has run yet.
func (f *chainsyncServerFixture) observedConnId() (ouroboros.ConnectionId, bool) {
	f.connIdMu.Lock()
	defer f.connIdMu.Unlock()
	if f.connId == nil {
		return ouroboros.ConnectionId{}, false
	}
	return *f.connId, true
}

// newChainsyncServerFixture wires Dingo's chainsync server config into the
// shared harness. The config is built with the same chainsyncServerConnOpts
// helper production uses, so the instrumentation wrappers are exercised rather
// than bypassed.
func newChainsyncServerFixture(
	t *testing.T,
	mode csmock.Mode,
) *chainsyncServerFixture {
	t.Helper()
	return newChainsyncServerFixtureWithConfig(t, mode, OuroborosConfig{})
}

// newChainsyncServerFixtureWithConfig is newChainsyncServerFixture with extra
// OuroborosConfig fields (EnableLeios, LeiosClosureWaitTimeout, ...) folded in.
// ConnManager, EventBus and Logger are always supplied by the fixture and
// override anything set in cfg.
//
// The connection manager's ConnClosedFunc is wired to the same
// ReleaseLeiosServeWaiters call the node makes, so a disconnect releases a
// parked NtC serving wait exactly as it does in production. The callback
// closes over the o variable rather than a value because the manager has to
// exist before newOuroboros can be given it; it can only fire after
// AddConnection below, by which point o is assigned.
// tweaks are applied to the Ouroboros instance after it is constructed and
// before the harness exists, so a test can adjust an internal seam without
// racing the protocol goroutine that will read it.
func newChainsyncServerFixtureWithConfig(
	t *testing.T,
	mode csmock.Mode,
	cfg OuroborosConfig,
	tweaks ...func(*Ouroboros),
) *chainsyncServerFixture {
	t.Helper()
	return newChainsyncServerFixtureServing(t, mode, cfg, false, tweaks...)
}

// newChainsyncServerFixtureServing builds the fixture with the server's
// waitForLedger behaviour chosen explicitly. The other constructors leave it
// off, so their blocks are served as soon as the chain has them, without the
// ledger applying them.
func newChainsyncServerFixtureServing(
	t *testing.T,
	mode csmock.Mode,
	cfg OuroborosConfig,
	waitForLedger bool,
	tweaks ...func(*Ouroboros),
) *chainsyncServerFixture {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)

	var o *Ouroboros
	ledgerState := newTestLedgerState(t)
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: bus,
			Logger:   logger,
			ConnClosedFunc: func(
				connId ouroboros.ConnectionId,
				_ bool,
				_ error,
			) {
				if o != nil {
					o.ReleaseLeiosServeWaiters(connId)
				}
			},
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})

	cfg.ConnManager = connManager
	cfg.EventBus = bus
	cfg.Logger = logger
	o = newOuroboros(cfg)
	o.ledgerState = ledgerState
	o.chainsyncState = dchainsync.NewState(bus, ledgerState)
	for _, tweak := range tweaks {
		tweak(o)
	}

	limiter := newChainsyncFindIntersectRateLimiter(
		chainsyncFindIntersectBudgetRate,
		chainsyncFindIntersectBudgetBurst,
	)
	f := &chainsyncServerFixture{o: o, limiter: limiter}

	// The harness assigns the connection ID, so observe it as the production
	// callbacks run. These shims only record the ID and delegate; the real
	// callbacks (and their instrumentation wrappers) still do all the work.
	serverCfg := ochainsync.NewConfig(
		o.chainsyncServerConnOpts(
			context.Background(),
			limiter,
			waitForLedger,
		)...)
	findIntersect := serverCfg.FindIntersectFunc
	serverCfg.FindIntersectFunc = func(
		ctx ochainsync.CallbackContext,
		points []ocommon.Point,
	) (ocommon.Point, ochainsync.Tip, error) {
		f.recordConnId(ctx.ConnectionId)
		return findIntersect(ctx, points)
	}
	requestNext := serverCfg.RequestNextFunc
	serverCfg.RequestNextFunc = func(ctx ochainsync.CallbackContext) error {
		f.recordConnId(ctx.ConnectionId)
		return requestNext(ctx)
	}

	_, closedCh := bus.Subscribe(connmanager.ConnectionClosedEventType)
	f.closedCh = closedCh

	h, err := csmock.New(csmock.Config{
		Mode:      mode,
		ChainSync: serverCfg,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = h.Close() })

	f.h = h

	// Register the server-under-test connection the way a real peer connection
	// would be. chainsyncServerRequestNext resolves the peer through
	// ConnManager immediately after sending AwaitReply, so without this the
	// async serving path cannot run at all. Requires ouroboros-mock v0.16.0's
	// Harness.ServerConnection.
	f.conn = h.ServerConnection()
	require.NotNil(t, f.conn)
	require.True(
		t,
		connManager.AddConnection(
			f.conn,
			false,
			f.conn.Id().RemoteAddr.String(),
		),
	)

	return f
}

// observe returns the next message the server put on the wire, failing the
// test if none arrives within a bounded window. Synchronization is entirely
// channel-based; nothing in this file sleeps.
func (f *chainsyncServerFixture) observe(t *testing.T) csmock.ServerMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := f.h.Observe(ctx)
	require.NoError(t, err, "expected a chainsync message from the server")
	return msg
}

// appendBlock adds a block to the fixture's chain and returns it with its
// chainsync point.
func (f *chainsyncServerFixture) appendBlock(
	t *testing.T,
	slot, blockNumber uint64,
	hashByte byte,
) (*testBlock, ocommon.Point) {
	t.Helper()
	header, ok := newTestBlockHeader(
		slot,
		blockNumber,
		hashByte,
	).(*testBlockHeader)
	require.True(t, ok)
	block := &testBlock{
		BlockHeader: header,
		blockType:   1,
		cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		f.o.ledgerState.Chain().AddBlock(context.Background(), block, nil),
	)
	return block, ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
}

// setTip publishes a ledger tip so the server reports it to the peer.
func (f *chainsyncServerFixture) setTip(
	block *testBlock,
	point ocommon.Point,
) {
	f.o.ledgerState.SetTipForTesting(ochainsync.Tip{
		Point:       point,
		BlockNumber: block.BlockNumber(),
	})
}

// registeredClient returns the server-side chainsync client state the server
// registered for the harness connection, if any. It reads through
// LookupClient rather than AddClient so asking the question cannot create the
// state being asserted on.
func (f *chainsyncServerFixture) registeredClient(
	t *testing.T,
) (*dchainsync.ChainsyncClientStateSnapshot, bool) {
	t.Helper()
	connId, ok := f.observedConnId()
	if !ok {
		return nil, false
	}
	return f.o.chainsyncState.LookupClient(connId)
}

// requireConnectionClosed asserts the connection was closed through normal
// connmanager lifecycle handling. The event error is nil because the watcher
// is woken by the connection closing, not by an error being pushed onto its
// channel.
func (f *chainsyncServerFixture) requireConnectionClosed(
	t *testing.T,
	msg string,
) {
	t.Helper()
	evt := testutil.RequireReceive(t, f.closedCh, 5*time.Second, msg)
	closed, ok := evt.Data.(connmanager.ConnectionClosedEvent)
	require.True(t, ok)
	require.Equal(t, f.conn.Id(), closed.ConnectionId)
	require.NoError(
		t,
		closed.Error,
		"close must be published as a graceful lifecycle event, not an "+
			"error pushed onto the connection's error channel",
	)
}

// requireClientUnparked asserts the server dropped the transport, which is the
// only thing that releases a peer the server has parked in MustReply short of
// gouroboros' randomized 135-269s MustReply timer. The harness surfaces the
// drop by closing its observed-message stream, so Observe returns
// csmock.ErrClosed; a server that abandons the wait silently instead leaves
// Observe blocking until the deadline.
func (f *chainsyncServerFixture) requireClientUnparked(
	t *testing.T,
	msg string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := f.h.Observe(ctx)
	require.ErrorIs(t, err, csmock.ErrClosed, msg)
}

// parkInAwaitReply drives the fixture to the state this file's abandonment
// tests all start from: the downstream client has intersected at origin, taken
// its initial rollback, and asked for a block the server does not have yet, so
// the server has sent AwaitReply and armed the asynchronous waiter.
func (f *chainsyncServerFixture) parkInAwaitReply(t *testing.T) {
	t.Helper()
	f.drainInitialRollback(t, csmock.OriginPoint())
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")
}

// drainInitialRollback performs the intersect-then-rollback handshake every
// downstream client starts with, leaving the server ready to serve blocks.
func (f *chainsyncServerFixture) drainInitialRollback(
	t *testing.T,
	intersect ocommon.Point,
) {
	t.Helper()
	require.NoError(t, f.h.FindIntersect([]ocommon.Point{intersect}))
	found := f.observe(t)
	require.True(t, found.IsIntersectFound(), "expected IntersectFound")

	require.NoError(t, f.h.RequestNext())
	rollback := f.observe(t)
	require.True(t, rollback.IsRollBackward(), "expected initial RollBackward")
}

// =============================================================================
// FindIntersect
// =============================================================================

// TestChainsyncServerFindIntersectEmitsFoundAndRegistersClient verifies a
// matching point produces an IntersectFound carrying that exact point and the
// current tip, and that the server registered the downstream client at the
// intersect point as a result.
func TestChainsyncServerFindIntersectEmitsFoundAndRegistersClient(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	block, point := f.appendBlock(t, 1, 1, 0x01)
	f.setTip(block, point)

	// No client may exist before the peer has asked for an intersection.
	_, registered := f.registeredClient(t)
	require.False(t, registered, "no client should be registered yet")

	require.NoError(t, f.h.FindIntersect([]ocommon.Point{point}))

	msg := f.observe(t)
	require.True(t, msg.IsIntersectFound(), "expected IntersectFound")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, point, gotPoint, "IntersectFound point")
	gotTip, ok := msg.Tip()
	require.True(t, ok)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"IntersectFound tip",
	)

	// Returning a point without registering the client would leave the
	// server unable to serve the peer, so assert the registration happened
	// and is cursored at the intersection.
	clientState, registered := f.registeredClient(t)
	require.True(
		t,
		registered,
		"FindIntersect returned a point without registering the client",
	)
	require.Equal(t, point, clientState.Cursor)
	require.True(t, clientState.NeedsInitialRollback)
}

// TestChainsyncServerFindIntersectEmitsNotFoundForUnknownPoint verifies an
// in-range but unknown point produces IntersectNotFound carrying the current
// tip, and leaves no client registered.
func TestChainsyncServerFindIntersectEmitsNotFoundForUnknownPoint(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	block, point := f.appendBlock(t, 10, 1, 0x01)
	f.setTip(block, point)

	unknown := ocommon.NewPoint(10, make([]byte, 32))
	require.NoError(t, f.h.FindIntersect([]ocommon.Point{unknown}))

	msg := f.observe(t)
	require.True(t, msg.IsIntersectNotFound(), "expected IntersectNotFound")
	gotTip, ok := msg.Tip()
	require.True(t, ok)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"IntersectNotFound must still report the current tip",
	)

	_, registered := f.registeredClient(t)
	require.False(
		t,
		registered,
		"a failed intersection must not register a downstream client",
	)
}

// TestChainsyncServerFindIntersectAcceptsPointListAtLimit verifies a point
// list exactly at chainsyncMaxFindIntersectPoints is served normally. An empty
// chain intersects any in-bounds request at origin, so IntersectFound at
// origin proves the cap did not short-circuit the request.
func TestChainsyncServerFindIntersectAcceptsPointListAtLimit(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints)
	require.NoError(t, f.h.FindIntersect(points))

	msg := f.observe(t)
	require.True(
		t,
		msg.IsIntersectFound(),
		"a point list at the limit must be accepted",
	)
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, csmock.OriginPoint(), gotPoint)
}

// TestChainsyncServerFindIntersectRejectsPointListOverLimit verifies an
// over-limit list is rejected with IntersectNotFound before any intersection
// lookup, rather than tearing the connection down. On an empty chain the
// lookup would otherwise have matched origin, so IntersectNotFound here proves
// the cap short-circuited the request.
func TestChainsyncServerFindIntersectRejectsPointListOverLimit(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints + 1)
	require.NoError(t, f.h.FindIntersect(points))

	msg := f.observe(t)
	require.True(
		t,
		msg.IsIntersectNotFound(),
		"an over-limit point list must be rejected with IntersectNotFound",
	)

	_, registered := f.registeredClient(t)
	require.False(
		t,
		registered,
		"a rejected intersection must not register a downstream client",
	)
}

// TestChainsyncServerFindIntersectAcceptsNormalPointList verifies the point
// count a well-behaved client actually sends is served normally.
func TestChainsyncServerFindIntersectAcceptsNormalPointList(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	points := makeFindIntersectPoints(chainsyncIntersectPointCount)
	require.NoError(t, f.h.FindIntersect(points))

	msg := f.observe(t)
	require.True(t, msg.IsIntersectFound())
}

// TestChainsyncServerFindIntersectDeduplicatesRepeatedPointsForBudget verifies
// duplicate points within one request are deduplicated before the
// per-connection work budget is charged. A list of chainsyncMaxFindIntersectPoints
// copies of the same point is at the point-count limit but collapses to a
// single point after deduplication, so it must be charged as 1 point of work,
// not chainsyncMaxFindIntersectPoints.
func TestChainsyncServerFindIntersectDeduplicatesRepeatedPointsForBudget(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)

	// An empty chain intersects any in-bounds request at origin, so
	// IntersectFound here only tells us the request wasn't rejected — the
	// assertion that matters is the second request below.
	point := makeFindIntersectPoints(1)[0]
	dup := make([]ocommon.Point, chainsyncMaxFindIntersectPoints)
	for i := range dup {
		dup[i] = point
	}
	require.NoError(t, f.h.FindIntersect(dup))
	require.True(t, f.observe(t).IsIntersectFound())

	// Had the duplicate-heavy request above been charged its full
	// un-deduplicated size, it would have exhausted the entire work budget
	// on its own, and this distinct-point request — within both the
	// point-count and work-budget limits by itself — would be rejected too.
	require.NoError(
		t,
		f.h.FindIntersect(
			makeFindIntersectPoints(chainsyncMaxFindIntersectPoints-1),
		),
	)
	require.True(
		t,
		f.observe(t).IsIntersectFound(),
		"a duplicate-heavy request must not exhaust the work budget meant for distinct points",
	)
}

// TestChainsyncServerFindIntersectRateLimitsRepeatedRequests verifies the
// per-connection work budget bounds cumulative work across many in-bounds
// requests, not just the size of a single request: a second full-size
// request immediately following the first must be rejected even though
// each is within the point-count limit on its own. The limiter's clock is
// pinned so the assertion holds regardless of how long the wire round trips
// actually take, rather than relying on them staying under the 5s a full
// burst would need to refill at chainsyncFindIntersectBudgetRate.
func TestChainsyncServerFindIntersectRateLimitsRepeatedRequests(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.limiter.nowFunc = func() time.Time {
		return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}

	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints)
	require.NoError(t, f.h.FindIntersect(points))
	require.True(
		t,
		f.observe(t).IsIntersectFound(),
		"first full-size request should be within the work budget",
	)

	require.NoError(t, f.h.FindIntersect(points))
	require.True(
		t,
		f.observe(t).IsIntersectNotFound(),
		"a repeated full-size request over the per-connection work budget must be rejected",
	)
}

// =============================================================================
// RequestNext (synchronous replies)
// =============================================================================

// TestChainsyncServerRequestNextEmitsInitialRollbackToIntersect verifies the
// first RequestNext after an intersection replies with a RollBackward to the
// exact intersection point and current tip, and clears the pending-rollback
// flag so the next reply serves a block instead.
func TestChainsyncServerRequestNextEmitsInitialRollbackToIntersect(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	block, point := f.appendBlock(t, 1, 1, 0x01)
	f.setTip(block, point)

	require.NoError(t, f.h.FindIntersect([]ocommon.Point{point}))
	require.True(t, f.observe(t).IsIntersectFound())

	require.NoError(t, f.h.RequestNext())

	msg := f.observe(t)
	require.True(t, msg.IsRollBackward(), "expected RollBackward")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, point, gotPoint, "initial rollback must target intersect")
	gotTip, ok := msg.Tip()
	require.True(t, ok)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
	)

	clientState, registered := f.registeredClient(t)
	require.True(t, registered)
	require.False(
		t,
		clientState.NeedsInitialRollback,
		"initial rollback must clear the pending-rollback flag",
	)
}

// TestChainsyncServerRequestNextEmitsRollForwardWithExactBlock verifies an
// available iterator block is sent immediately as a RollForward carrying the
// exact block type, block CBOR and tip.
//
// The predecessor of this test asserted only that the iterator had advanced to
// the chain tip, which a callback that drained the iterator and sent nothing
// would also have satisfied.
func TestChainsyncServerRequestNextEmitsRollForwardWithExactBlock(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	block, point := f.appendBlock(t, 1, 1, 0x01)

	require.NoError(t, f.h.RequestNext())

	msg := f.observe(t)
	require.True(t, msg.IsRollForward(), "expected RollForward")
	blockType, blockCbor, gotTip, ok := msg.RollForwardNtC()
	require.True(t, ok)
	require.Equal(t, uint(block.Type()), blockType, "RollForward block type")
	require.Equal(t, block.Cbor(), blockCbor, "RollForward block CBOR")
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"RollForward tip must not lag the block being sent",
	)
}

// TestChainsyncServerRequestNextEmitsRollBackwardOnChainRollback verifies a
// pending iterator rollback is sent immediately as a RollBackward carrying the
// exact rollback point.
//
// As with the roll-forward case, the predecessor asserted only that the
// iterator had been drained.
func TestChainsyncServerRequestNextEmitsRollBackwardOnChainRollback(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	// Serve one block so the iterator is past origin, then roll the chain
	// back so the next synchronous iterator result is a rollback event.
	f.appendBlock(t, 1, 1, 0x01)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward())

	require.NoError(
		t,
		f.o.ledgerState.Chain().
			Rollback(context.Background(), ocommon.NewPointOrigin()),
	)

	require.NoError(t, f.h.RequestNext())

	msg := f.observe(t)
	require.True(t, msg.IsRollBackward(), "expected RollBackward")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(
		t,
		ocommon.NewPointOrigin(),
		gotPoint,
		"rollback must target the point the chain rolled back to",
	)
}

// =============================================================================
// RequestNext (AwaitReply and the asynchronous replies that follow)
//
// These paths require the peer connection to be resolvable through Dingo's
// ConnManager: chainsyncServerRequestNext looks it up immediately after
// sending AwaitReply so it can abandon the blocked iterator read if the peer
// goes away. The fixture registers the harness connection for exactly that
// reason.
// =============================================================================

type deadlineRecordingListener struct {
	net.Listener
	accepted chan *deadlineRecordingConn
}

func (l *deadlineRecordingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	wrapped := &deadlineRecordingConn{Conn: conn}
	select {
	case l.accepted <- wrapped:
	default:
	}
	return wrapped, nil
}

type deadlineRecordingConn struct {
	net.Conn
	mu           sync.Mutex
	readDeadline time.Time
}

func (c *deadlineRecordingConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.readDeadline = deadline
	c.mu.Unlock()
	return c.Conn.SetDeadline(deadline)
}

func (c *deadlineRecordingConn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.readDeadline = deadline
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(deadline)
}

func (c *deadlineRecordingConn) currentReadDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readDeadline
}

func TestTrustedNtCChainSyncAwaitKeepsIdleDeadlineSuspended(t *testing.T) {
	baseListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener := &deadlineRecordingListener{
		Listener: baseListener,
		accepted: make(chan *deadlineRecordingConn, 1),
	}
	t.Cleanup(func() { _ = listener.Close() })

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	ledgerState := newTestLedgerState(t)
	o := newOuroboros(OuroborosConfig{
		NetworkMagic: 1,
		EventBus:     bus,
		Logger:       logger,
	})
	o.ledgerState = ledgerState
	o.chainsyncState = dchainsync.NewState(bus, ledgerState)
	listeners := o.ConfigureListeners(t.Context(), []connmanager.ListenerConfig{{
		Listener: listener,
		UseNtC:   true,
	}})
	manager := connmanager.NewConnectionManager(connmanager.ConnectionManagerConfig{
		EventBus:  bus,
		Listeners: listeners,
		Logger:    logger,
	})
	o.connManager = manager
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, manager.Stop(ctx))
	})
	require.NoError(t, manager.Start(t.Context()))

	awaited := make(chan struct{}, 1)
	rollForward := make(chan struct{}, 1)
	clientConfig := ochainsync.NewConfig(
		ochainsync.WithPipelineLimit(1),
		ochainsync.WithAwaitReplyFunc(func(ochainsync.CallbackContext) error {
			awaited <- struct{}{}
			return nil
		}),
		ochainsync.WithRollBackwardFunc(func(
			ochainsync.CallbackContext,
			ocommon.Point,
			ochainsync.Tip,
		) error {
			return nil
		}),
		ochainsync.WithRollForwardRawFunc(func(
			ochainsync.CallbackContext,
			uint,
			[]byte,
			ochainsync.Tip,
		) error {
			rollForward <- struct{}{}
			return ochainsync.ErrStopSyncProcess
		}),
	)
	clientConn, err := net.Dial("tcp", listener.Addr().String())
	require.NoError(t, err)
	client, err := ouroboros.NewConnection(
		ouroboros.WithConnection(clientConn),
		ouroboros.WithNetworkMagic(1),
		ouroboros.WithNodeToNode(false),
		ouroboros.WithChainSyncConfig(clientConfig),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	serverConn := testutil.RequireReceive(
		t,
		listener.accepted,
		5*time.Second,
		"server did not accept the NtC connection",
	)
	require.Eventually(t, func() bool {
		return !serverConn.currentReadDeadline().IsZero()
	}, 5*time.Second, time.Millisecond, "trusted NtC idle deadline was not armed")

	require.NoError(t, client.ChainSync().Client.Sync(
		[]ocommon.Point{ocommon.NewPointOrigin()},
	))
	testutil.RequireReceive(
		t,
		awaited,
		5*time.Second,
		"ChainSync did not enter AwaitReply",
	)
	require.True(
		t,
		serverConn.currentReadDeadline().IsZero(),
		"idle deadline remained armed while ChainSync awaited a block",
	)

	header, ok := newTestBlockHeader(1, 1, 0x01).(*testBlockHeader)
	require.True(t, ok)
	block := &testBlock{
		BlockHeader: header,
		blockType:   1,
		cbor:        []byte{0x80},
	}
	require.NoError(t, ledgerState.Chain().AddBlock(t.Context(), block, nil))
	ledgerState.SetTipForTesting(ochainsync.Tip{
		Point: ocommon.NewPoint(
			block.SlotNumber(),
			block.Hash().Bytes(),
		),
		BlockNumber: block.BlockNumber(),
	})
	testutil.RequireReceive(
		t,
		rollForward,
		5*time.Second,
		"ChainSync did not deliver the deferred RollForward",
	)
	require.Eventually(t, func() bool {
		return !serverConn.currentReadDeadline().IsZero()
	}, 5*time.Second, time.Millisecond, "idle deadline was not re-armed")
}

// TestChainsyncServerRequestNextEmitsAwaitReplyThenAsyncRollForward verifies
// that an iterator sitting at the chain tip parks the peer with AwaitReply,
// and that a block appended afterwards is served asynchronously as a
// RollForward carrying the exact block type, block CBOR and tip.
//
// The predecessor of the AwaitReply half asserted only that the callback
// returned nil, which a callback that sent nothing at all would also satisfy.
// The async RollForward half could not be covered before at all: the
// post-AwaitReply ConnManager lookup failed, so the callback errored and tore
// the connection down instead of serving.
func TestChainsyncServerRequestNextEmitsAwaitReplyThenAsyncRollForward(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	// Iterator is at the chain tip, so the server parks the peer.
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	// A block arriving after the park must be served by the async goroutine.
	block, point := f.appendBlock(t, 1, 1, 0x01)

	msg := f.observe(t)
	require.True(t, msg.IsRollForward(), "expected async RollForward")
	blockType, blockCbor, gotTip, ok := msg.RollForwardNtC()
	require.True(t, ok)
	require.Equal(t, uint(block.Type()), blockType)
	require.Equal(t, block.Cbor(), blockCbor)
	require.Equal(
		t,
		ochainsync.Tip{Point: point, BlockNumber: block.BlockNumber()},
		gotTip,
		"async RollForward tip must not lag the block being sent",
	)
}

// TestChainsyncServerRequestNextEmitsAsyncRollBackward verifies a rollback
// that happens while the peer is parked in AwaitReply is served
// asynchronously as a RollBackward carrying the exact rollback point.
func TestChainsyncServerRequestNextEmitsAsyncRollBackward(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	// Serve one block so the iterator is past origin.
	f.appendBlock(t, 1, 1, 0x01)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward())

	// Park the peer, then roll the chain back underneath it.
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	require.NoError(
		t,
		f.o.ledgerState.Chain().
			Rollback(context.Background(), ocommon.NewPointOrigin()),
	)

	msg := f.observe(t)
	require.True(t, msg.IsRollBackward(), "expected async RollBackward")
	gotPoint, ok := msg.Point()
	require.True(t, ok)
	require.Equal(t, ocommon.NewPointOrigin(), gotPoint)
}

// TestChainsyncServerRequestNextAsyncRollForwardFailureClosesConnection
// verifies that when the asynchronous RollForward send fails — after
// chainsyncServerRequestNext has already returned, so the protocol layer can
// no longer turn an error into teardown — the connection is still closed
// through normal connmanager lifecycle handling rather than left silently
// open with the peer parked in AwaitReply.
func TestChainsyncServerRequestNextAsyncRollForwardFailureClosesConnection(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	// Stop the protocol so the async send fails, then wake the iterator.
	f.h.Server().Stop()
	f.appendBlock(t, 1, 1, 0x01)

	f.requireConnectionClosed(
		t,
		"async RollForward send failure should close the connection",
	)
}

// TestChainsyncServerRequestNextAsyncRollBackwardFailureClosesConnection is
// the rollback counterpart: a rollback send that fails after AwaitReply must
// not leave the downstream peer connection silently open either.
func TestChainsyncServerRequestNextAsyncRollBackwardFailureClosesConnection(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.drainInitialRollback(t, csmock.OriginPoint())

	f.appendBlock(t, 1, 1, 0x01)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward())

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	// Stop the protocol so the async send fails, then roll back.
	f.h.Server().Stop()
	require.NoError(
		t,
		f.o.ledgerState.Chain().
			Rollback(context.Background(), ocommon.NewPointOrigin()),
	)

	f.requireConnectionClosed(
		t,
		"async RollBackward send failure should close the connection",
	)
}

// TestChainsyncServerRequestNextIteratorCancelDoesNotCloseConnection verifies
// that ordinary iterator cancellation — which is how the async wait unwinds
// during normal connection teardown — is not mistaken for a failure worth
// recycling the connection over.
func TestChainsyncServerRequestNextIteratorCancelDoesNotCloseConnection(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	clientState := f.registerClientAtOrigin(t)

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")

	clientState.ChainIter.Cancel()

	testutil.RequireNoReceive(
		t,
		f.closedCh,
		100*time.Millisecond,
		"iterator cancellation should not close the connection",
	)
}

// TestChainsyncServerLocalClientNotServedAheadOfLedger verifies a
// node-to-client server holds back a block the chain has but the ledger has
// not applied, on both the synchronous and the post-AwaitReply path, and
// serves it once the ledger tip reaches it. A local client acquires the
// points chainsync delivers; one served ahead of the ledger failed Acquire
// with AcquireFailurePointNotOnChain until the ledger caught up.
func TestChainsyncServerLocalClientNotServedAheadOfLedger(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixtureServing(
		t,
		csmock.ModeNtC,
		OuroborosConfig{},
		true,
	)
	block1, point1 := f.appendBlock(t, 1, 1, 0x01)
	f.setTip(block1, point1)
	f.drainInitialRollback(t, csmock.OriginPoint())

	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsRollForward(), "applied block is served")

	requireServedOnceApplied := func(block *testBlock, point ocommon.Point) {
		t.Helper()
		testutil.RequireNoReceive(
			t,
			f.h.Observed(),
			100*time.Millisecond,
			"a block the ledger has not applied must not be served",
		)
		f.setTip(block, point)
		msg := f.observe(t)
		require.True(t, msg.IsRollForward(), "expected RollForward")
		tip, ok := msg.Tip()
		require.True(t, ok)
		require.GreaterOrEqual(t, tip.Point.Slot, point.Slot)
	}

	appendChild := func(
		parent *testBlock,
		hashByte byte,
	) (*testBlock, ocommon.Point) {
		t.Helper()
		header, ok := newTestBlockHeader(
			parent.SlotNumber()+1,
			parent.BlockNumber()+1,
			hashByte,
		).(*testBlockHeader)
		require.True(t, ok)
		header.prevHash = parent.Hash()
		block := &testBlock{BlockHeader: header, blockType: 1, cbor: []byte{0x80}}
		require.NoError(
			t,
			f.o.ledgerState.Chain().AddBlock(context.Background(), block, nil),
		)
		return block, ocommon.NewPoint(
			block.SlotNumber(),
			block.Hash().Bytes(),
		)
	}

	// The chain already has the block when the client asks for it.
	block2, point2 := appendChild(block1, 0x02)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")
	requireServedOnceApplied(block2, point2)

	// The client is parked when the chain gets the block.
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")
	block3, point3 := appendChild(block2, 0x03)
	requireServedOnceApplied(block3, point3)

	// A block rolled back before application must never be served. The
	// iterator's rollback is delivered even while the applied tip remains
	// below the abandoned block's slot.
	_, _ = appendChild(block3, 0x04)
	require.NoError(t, f.h.RequestNext())
	require.True(t, f.observe(t).IsAwaitReply(), "expected AwaitReply")
	require.NoError(t, f.o.ledgerState.Chain().Rollback(t.Context(), point3))
	rollback := f.observe(t)
	require.True(t, rollback.IsRollBackward(), "expected RollBackward")
	rollbackPoint, ok := rollback.Point()
	require.True(t, ok)
	require.Equal(t, point3, rollbackPoint)
}

// =============================================================================
// RequestNext error paths
//
// These assert the error the callback returns. The protocol layer converts a
// callback error into connection teardown rather than an observable message,
// so the callback is invoked directly; the fixture still supplies the real
// connection and server it runs against.
// =============================================================================

// TestChainsyncServerRequestNextSyncIteratorErrorPropagates verifies a real
// iterator failure is returned rather than being mistaken for the chain-tip
// sentinel (which would silently park the peer instead of surfacing the
// fault).
func TestChainsyncServerRequestNextSyncIteratorErrorPropagates(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.registerClientAtOrigin(t)

	// Break the backing store so the synchronous iterator returns a real
	// lookup error.
	require.NoError(t, dbtest.CloseDatabase(f.o.ledgerState.Database()))

	err := f.o.chainsyncServerRequestNext(f.callbackContext(), false)

	require.Error(t, err)
	require.NotErrorIs(t, err, chain.ErrIteratorChainTip)
}

// TestChainsyncServerRequestNextAwaitReplyErrorPropagates verifies an
// AwaitReply send failure is returned from the callback, so the protocol layer
// tears the connection down instead of arming an async wait on a dead peer.
func TestChainsyncServerRequestNextAwaitReplyErrorPropagates(t *testing.T) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.registerClientAtOrigin(t)

	// Stop the protocol so the AwaitReply send itself fails.
	f.h.Server().Stop()

	require.Error(t, f.o.chainsyncServerRequestNext(f.callbackContext(), false))
}

// TestChainsyncServerRequestNextErrorsNameTheFailedStep verifies the callback
// errors carry the step that failed while keeping the underlying error
// matchable, so a torn-down connection can be diagnosed from its log line.
func TestChainsyncServerRequestNextErrorsNameTheFailedStep(t *testing.T) {
	t.Parallel()

	t.Run("iterator", func(t *testing.T) {
		t.Parallel()

		f := newChainsyncServerFixture(t, csmock.ModeNtC)
		f.registerClientAtOrigin(t)
		require.NoError(t, dbtest.CloseDatabase(f.o.ledgerState.Database()))

		err := f.o.chainsyncServerRequestNext(f.callbackContext(), false)

		require.ErrorContains(t, err, "chainsync server: next chain event")
		require.NotErrorIs(t, err, chain.ErrIteratorChainTip)
	})

	t.Run("await reply", func(t *testing.T) {
		t.Parallel()

		f := newChainsyncServerFixture(t, csmock.ModeNtC)
		f.registerClientAtOrigin(t)
		f.h.Server().Stop()

		err := f.o.chainsyncServerRequestNext(f.callbackContext(), false)

		require.ErrorContains(t, err, "chainsync server: send AwaitReply")
		require.Error(t, errors.Unwrap(err), "the send error must stay wrapped")
	})
}

// TestChainsyncServerRequestNextMissingConnectionAfterAwaitReply verifies the
// post-AwaitReply connection lookup fails explicitly when the connection was
// already recycled, rather than arming an async wait with no way to notice the
// peer is gone.
func TestChainsyncServerRequestNextMissingConnectionAfterAwaitReply(
	t *testing.T,
) {
	t.Parallel()

	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.registerClientAtOrigin(t)

	require.True(t, f.o.connManager.RemoveConnection(f.conn.Id(), f.conn))

	err := f.o.chainsyncServerRequestNext(f.callbackContext(), false)

	require.ErrorContains(t, err, "not found")
}

// =============================================================================
// RequestNext: abandoning the peer after AwaitReply
//
// Once AwaitReply is on the wire the server holds agency in MustReply, so a
// waiter that gives up without a reply must drop the transport. Every test
// below asserts the client was released rather than left to gouroboros'
// randomized 135-269s MustReply timeout, which is what produced the observed
// reconnect-and-park loop.
// =============================================================================

// TestChainsyncServerRequestNextIteratorErrorAfterAwaitReplyUnparksClient
// covers the exit taken when the blocking chain iterator fails after the peer
// has been parked. Before the fix the goroutine logged at Debug and returned,
// leaving the peer parked with the server still holding agency.
func TestChainsyncServerRequestNextIteratorErrorAfterAwaitReplyUnparksClient(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.parkInAwaitReply(t)

	// Break the backing store, then wake the parked iterator so its next
	// lookup fails for real rather than returning the chain-tip sentinel.
	require.NoError(t, dbtest.CloseDatabase(f.o.ledgerState.Database()))
	f.o.ledgerState.Chain().NotifyIterators()

	f.requireClientUnparked(
		t,
		"an iterator failure after AwaitReply must drop the transport, not "+
			"leave the peer parked in MustReply",
	)
}

// TestChainsyncServerRequestNextNilBlockAfterAwaitReplyUnparksClient covers the
// nil-result exit. ChainIterator.Next maps an empty non-error result to
// ErrIteratorChainTip, so the real iterator cannot produce it and the handler
// is invoked directly against the fixture's real connection and server.
func TestChainsyncServerRequestNextNilBlockAfterAwaitReplyUnparksClient(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.parkInAwaitReply(t)

	f.o.chainsyncServerServeAwaited(f.callbackContext(), f.conn, nil, nil)

	f.requireClientUnparked(
		t,
		"a nil iterator result after AwaitReply must drop the transport, not "+
			"leave the peer parked in MustReply",
	)
}

// stubChainsyncServerConnection stands in for the connection the
// post-AwaitReply waiter watches: the test closes done to signal teardown,
// exactly as the connection manager's watcher does for a real connection.
//
// closeFn delegates to the real connection, so closing the stand-in still drops
// the actual transport and the harness can observe the parked peer being
// released.
type stubChainsyncServerConnection struct {
	done     chan struct{}
	closeFn  func() error
	closeErr error

	mu         sync.Mutex
	closeCalls int
}

func newStubChainsyncServerConnection(
	closeFn func() error,
) *stubChainsyncServerConnection {
	return &stubChainsyncServerConnection{
		done:    make(chan struct{}),
		closeFn: closeFn,
	}
}

func (c *stubChainsyncServerConnection) Done() <-chan struct{} {
	return c.done
}

func (c *stubChainsyncServerConnection) CloseError() error {
	return c.closeErr
}

func (c *stubChainsyncServerConnection) Close() error {
	c.mu.Lock()
	c.closeCalls++
	c.mu.Unlock()
	if c.closeFn != nil {
		return c.closeFn()
	}
	return nil
}

func (c *stubChainsyncServerConnection) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeCalls
}

// newChainsyncServerFixtureLogging is newChainsyncServerFixture with the
// Ouroboros logger redirected into a buffer, for the teardown test whose assertion
// includes the reason the waiter logged for a teardown.
func newChainsyncServerFixtureLogging(
	t *testing.T,
) (*chainsyncServerFixture, *lockedBuffer) {
	t.Helper()
	logBuf := &lockedBuffer{}
	f := newChainsyncServerFixtureWithConfig(
		t,
		csmock.ModeNtC,
		OuroborosConfig{},
		func(o *Ouroboros) {
			o.config.Logger = slog.New(slog.NewJSONHandler(logBuf, nil))
		},
	)
	return f, logBuf
}

// TestChainsyncServerAwaitedWaiterClosesOnConnectionTeardown covers the
// waiter's teardown exit: once the connection manager signals teardown the
// waiter must close the transport rather than return silently. Closing is the
// whole point of the exit -- once MsgAwaitReply is on the wire the server holds
// agency in MustReply and nothing else releases the peer: a silent return does
// not, and ConnectionManager.RemoveConnection only unregisters.
//
// The waiter is driven directly, over a stand-in whose Close is the real one;
// the test does not park a peer through the protocol. A peer parked by the
// protocol is covered by
// TestChainsyncServerRequestNextIteratorErrorAfterAwaitReplyUnparksClient and
// TestChainsyncServerRequestNextNilBlockAfterAwaitReplyUnparksClient, which
// park for real and drive the serve path on the real connection.
func TestChainsyncServerAwaitedWaiterClosesOnConnectionTeardown(
	t *testing.T,
) {
	f, logBuf := newChainsyncServerFixtureLogging(t)
	clientState := f.registerClientAtOrigin(t)

	conn := newStubChainsyncServerConnection(f.conn.Close)
	conn.closeErr = errors.New("peer reset")
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.o.chainsyncServerAwaitNext(f.callbackContext(), conn, clientState, nil, false)
	}()

	close(conn.done)

	testutil.RequireReceive(
		t,
		done,
		10*time.Second,
		"the waiter must return once the connection is torn down",
	)
	f.requireClientUnparked(
		t,
		"connection teardown must drop the transport, which is the only "+
			"thing that releases a peer the server has parked in MustReply",
	)
	require.Equal(
		t,
		1,
		conn.closeCount(),
		"the waiter itself must close the connection",
	)
	require.Contains(
		t,
		logBuf.String(),
		errChainsyncAwaitConnectionClosed.Error(),
		"the teardown must be logged as a closed connection",
	)
	require.Contains(t, logBuf.String(), conn.closeErr.Error())
}

// TestChainsyncServerServeAwaitedCancelledDoesNotCloseConnection pins the one
// exit that must stay silent: a context.Canceled iterator result. The server
// client's iterator is only cancelled from chainsync.State.RemoveClient, which
// connection-closed handling drives, so the transport is already gone and
// there is no parked peer to release. This is the invariant
// TestChainsyncServerRequestNextIteratorCancelDoesNotCloseConnection asserts
// end to end, restated at the handler so the abandonment fix cannot turn every
// ordinary disconnect into a connection-recycling warning.
func TestChainsyncServerServeAwaitedCancelledDoesNotCloseConnection(
	t *testing.T,
) {
	f := newChainsyncServerFixture(t, csmock.ModeNtC)
	f.parkInAwaitReply(t)

	f.o.chainsyncServerServeAwaited(
		f.callbackContext(),
		f.conn,
		nil,
		context.Canceled,
	)

	testutil.RequireNoReceive(
		t,
		f.closedCh,
		100*time.Millisecond,
		"a cancelled iterator is ordinary teardown and must not recycle the connection",
	)
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func TestEffectiveChainsyncBlockTimeoutUsesProtocolMaxAsFloor(t *testing.T) {
	t.Parallel()

	require.Equal(
		t,
		ochainsync.MustReplyTimeoutMax,
		effectiveChainsyncBlockTimeout(0),
	)
	require.Equal(
		t,
		ochainsync.MustReplyTimeoutMax,
		effectiveChainsyncBlockTimeout(time.Minute),
	)
	require.Equal(
		t,
		10*time.Minute,
		effectiveChainsyncBlockTimeout(10*time.Minute),
	)
}

func TestChainsyncByronEbbHeaderRoundTrip(t *testing.T) {
	t.Parallel()

	ebbCbor := byronEbbFixtureCbor(t)

	msg, err := ochainsync.NewMsgRollForwardNtN(
		gledger.BlockHeaderTypeByron,
		gledger.BlockTypeByronEbb,
		ebbCbor,
		ochainsync.Tip{},
	)
	require.NoError(t, err)
	wire, err := gcbor.Encode(msg)
	require.NoError(t, err)
	var received ochainsync.MsgRollForwardNtN
	_, err = gcbor.Decode(wire, &received)
	require.NoError(t, err)
	_, err = gledger.NewBlockHeaderFromCbor(
		received.WrappedHeader.ByronType(),
		received.WrappedHeader.HeaderCbor(),
	)
	require.NoError(t, err)
}

func TestDecodeChainsyncHeaderAcceptsFullByronEbb(t *testing.T) {
	t.Parallel()

	ebbCbor := byronEbbFixtureCbor(t)
	expected, err := gledger.NewBlockFromCbor(
		gledger.BlockTypeByronEbb,
		ebbCbor,
	)
	require.NoError(t, err)

	o := newOuroboros(OuroborosConfig{})
	header, err := o.decodeChainsyncHeader(gledger.BlockTypeByronEbb, ebbCbor)
	require.NoError(t, err)
	require.Equal(t, expected.Header().Hash(), header.Hash())
}

func byronEbbFixtureCbor(t *testing.T) []byte {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Block_Byron_EBB",
	)
	require.NoError(t, err)
	data, err := fixture.ConsensusLedgerBlockBytes()
	require.NoError(t, err)
	return data
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type testBlockHeader struct {
	hash        gledger.Blake2b256
	prevHash    gledger.Blake2b256
	blockNumber uint64
	slotNumber  uint64
	bodySize    uint64
	bodyHash    gledger.Blake2b256
}

// testBlock is the smallest block implementation needed to wake a server-side
// ChainIterator and drive the async RollForward path.
type testBlock struct {
	gledger.BlockHeader
	blockType int
	cbor      []byte
}

func (h *testBlockHeader) Hash() gledger.Blake2b256 {
	return h.hash
}

func (h *testBlockHeader) PrevHash() gledger.Blake2b256 {
	return h.prevHash
}

func (h *testBlockHeader) BlockNumber() uint64 {
	return h.blockNumber
}

func (h *testBlockHeader) SlotNumber() uint64 {
	return h.slotNumber
}

func (h *testBlockHeader) IssuerVkey() gledger.IssuerVkey {
	return gledger.IssuerVkey{}
}

func (h *testBlockHeader) BlockBodySize() uint64 {
	return h.bodySize
}

func (h *testBlockHeader) Era() gledger.Era {
	return gledger.Era{}
}

func (h *testBlockHeader) Cbor() []byte {
	return nil
}

func (h *testBlockHeader) BlockBodyHash() gledger.Blake2b256 {
	return h.bodyHash
}

func (b *testBlock) Header() gledger.BlockHeader {
	return b.BlockHeader
}

func (b *testBlock) Type() int {
	return b.blockType
}

func (b *testBlock) Transactions() []gledger.Transaction {
	return nil
}

func (b *testBlock) Utxorpc() (*utxorpc.Block, error) {
	return nil, nil
}

func (b *testBlock) Cbor() []byte {
	return b.cbor
}

func newTestBlockHeader(slot, block uint64, hashByte byte) gledger.BlockHeader {
	var hash gledger.Blake2b256
	hash[0] = hashByte
	return &testBlockHeader{
		hash:        hash,
		blockNumber: block,
		slotNumber:  slot,
	}
}

func newTestConnId(local, remote string) ouroboros.ConnectionId {
	localAddr, err := net.ResolveTCPAddr("tcp", local)
	if err != nil {
		panic(err)
	}
	remoteAddr, err := net.ResolveTCPAddr("tcp", remote)
	if err != nil {
		panic(err)
	}
	return ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}
}

func selectTrackedChainsyncClient(
	t testing.TB,
	state *dchainsync.State,
	connId ouroboros.ConnectionId,
) {
	t.Helper()
	point := ocommon.NewPoint(1, []byte("selected-client"))
	state.UpdateClientTipWithoutDedup(
		connId,
		point,
		ochainsync.Tip{Point: point},
	)
	require.True(t, state.TrySetClientConnId(connId))
}

type testSecurityParamLedger struct {
	securityParam int
}

func (l testSecurityParamLedger) SecurityParam() int {
	return l.securityParam
}

func newTestLedgerState(t *testing.T) *ledger.LedgerState {
	t.Helper()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) })

	cm, err := chain.NewManager(context.Background(), db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2160}),
	)

	ls, err := ledger.NewLedgerState(ledger.LedgerStateConfig{
		Database:     db,
		ChainManager: cm,
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
	})
	require.NoError(t, err)
	return ls
}

func setTestLedgerTip(
	t *testing.T,
	o *Ouroboros,
	tip ochainsync.Tip,
) {
	t.Helper()
	o.ledgerState.SetTipForTesting(tip)
}

func snapshotChainsyncNtNTimeouts() map[string]struct {
	timeout        time.Duration
	hasTimeoutFunc bool
} {
	snapshot := make(map[string]struct {
		timeout        time.Duration
		hasTimeoutFunc bool
	})
	for state, entry := range ochainsync.StateMapNtN {
		switch state.Name {
		case "CanAwait", "MustReply":
			snapshot[state.Name] = struct {
				timeout        time.Duration
				hasTimeoutFunc bool
			}{
				timeout:        entry.Timeout,
				hasTimeoutFunc: entry.TimeoutFunc != nil,
			}
		}
	}
	return snapshot
}

func TestNewOuroborosDoesNotMutateChainsyncNtNTimeouts(t *testing.T) {
	t.Parallel()

	before := snapshotChainsyncNtNTimeouts()

	_ = newOuroboros(OuroborosConfig{
		ChainsyncBlockTimeout: 10 * time.Minute,
	})
	require.Equal(t, before, snapshotChainsyncNtNTimeouts())

	_ = newOuroboros(OuroborosConfig{
		ChainsyncBlockTimeout: 20 * time.Minute,
	})
	require.Equal(t, before, snapshotChainsyncNtNTimeouts())
}

func TestChainsyncConnOptsUseConfiguredBlockTimeout(t *testing.T) {
	t.Parallel()

	const blockTimeout = 20 * time.Minute

	o := newOuroboros(OuroborosConfig{
		ChainsyncBlockTimeout: blockTimeout,
	})

	clientCfg := ochainsync.NewConfig(o.chainsyncClientConnOpts()...)
	serverCfg := ochainsync.NewConfig(
		o.chainsyncServerConnOpts(
			context.Background(),
			newChainsyncFindIntersectRateLimiter(200, 1000),
			false,
		)...)

	require.Equal(t, blockTimeout, clientCfg.BlockTimeout)
	require.Equal(t, blockTimeout, serverCfg.BlockTimeout)
	require.Equal(t, ochainsync.DefaultPipelineLimit, clientCfg.PipelineLimit)
	require.Equal(t, ochainsync.DefaultRecvQueueSize, clientCfg.RecvQueueSize)
}

// TestChainsyncConnectionConfigOptionCreatesPerConnectionBudget verifies that
// reusing the cached production option for two connections does not share the
// FindIntersect work budget. Each connection must be able to spend its own
// full burst, while a second request on the same connection is refused.
func TestChainsyncConnectionConfigOptionCreatesPerConnectionBudget(
	t *testing.T,
) {
	t.Parallel()

	o := newFindIntersectTestOuroboros(t)
	option := o.chainsyncConnectionConfigOption(context.Background(), false)
	points := makeFindIntersectPoints(chainsyncMaxFindIntersectPoints)

	for range 2 {
		mockConn := ouroboros_mock.NewConnection(
			ouroboros_mock.ProtocolRoleServer,
			[]ouroboros_mock.ConversationEntry{
				ouroboros_mock.ConversationEntryHandshakeRequestOutput,
				ouroboros_mock.ConversationEntryHandshakeNtCResponseInput,
				ouroboros_mock.ConversationEntryOutput{
					ProtocolId: ochainsync.ProtocolIdNtC,
					Messages: []protocol.Message{
						ochainsync.NewMsgFindIntersect(points),
					},
				},
				ouroboros_mock.ConversationEntryInput{
					ProtocolId:      ochainsync.ProtocolIdNtC,
					IsResponse:      true,
					MessageType:     ochainsync.MessageTypeIntersectFound,
					MsgFromCborFunc: ochainsync.NewMsgFromCborNtC,
				},
				ouroboros_mock.ConversationEntryOutput{
					ProtocolId: ochainsync.ProtocolIdNtC,
					Messages: []protocol.Message{
						ochainsync.NewMsgFindIntersect(points),
					},
				},
				ouroboros_mock.ConversationEntryInput{
					ProtocolId:      ochainsync.ProtocolIdNtC,
					IsResponse:      true,
					MessageType:     ochainsync.MessageTypeIntersectNotFound,
					MsgFromCborFunc: ochainsync.NewMsgFromCborNtC,
				},
				ouroboros_mock.ConversationEntryClose{},
			},
		)
		t.Cleanup(func() { _ = mockConn.Close() })
		conn, err := ouroboros.NewConnection(
			ouroboros.WithConnection(mockConn),
			ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			ouroboros.WithServer(true),
			option,
		)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		select {
		case err, ok := <-mockConn.(*ouroboros_mock.Connection).ErrorChan():
			require.NoError(t, err)
			require.False(t, ok)
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for scripted FindIntersect exchange")
		}
	}
}

// TestCloseChainsyncServerConnTearsDownTransport verifies that the async
// serving path's connection close actually tears down the bearer, not only the
// connmanager conn_closed event. The earlier error-channel-only path published
// conn_closed but left the transport open, so the NtC client stayed parked in
// AwaitReply; this asserts the transport itself closes (the client end's
// connection observes the bearer going away).
func TestCloseChainsyncServerConnTearsDownTransport(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	serverPipe, clientPipe := net.Pipe()
	t.Cleanup(func() {
		_ = serverPipe.Close()
		_ = clientPipe.Close()
	})

	serverConnCh := make(chan *ouroboros.Connection, 1)
	serverErrCh := make(chan error, 1)
	go func() {
		c, err := ouroboros.New(
			ouroboros.WithConnection(serverPipe),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(42),
			ouroboros.WithDelayProtocolStart(true),
			ouroboros.WithLogger(logger),
		)
		if err != nil {
			serverErrCh <- err
			return
		}
		serverConnCh <- c
	}()
	clientConn, err := ouroboros.New(
		ouroboros.WithConnection(clientPipe),
		ouroboros.WithNetworkMagic(42),
		ouroboros.WithDelayProtocolStart(true),
		ouroboros.WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.Close() })

	var serverConn *ouroboros.Connection
	select {
	case err := <-serverErrCh:
		t.Fatalf("server connection setup failed: %v", err)
	case serverConn = <-serverConnCh:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for server connection setup")
	}
	t.Cleanup(func() { _ = serverConn.Close() })

	o := newOuroboros(OuroborosConfig{Logger: logger})
	o.closeChainsyncServerConn(
		serverConn,
		serverConn.Id().String(),
		errLeiosClosureUnresolved,
	)

	// The client end observes the transport tearing down (its connection
	// ErrorChan fires) rather than staying connected. The earlier
	// error-channel-only server path left the bearer open, parking the client
	// in AwaitReply.
	testutil.RequireReceive(
		t,
		clientConn.ErrorChan(),
		2*time.Second,
		"client should observe the server transport closing",
	)
}

// TestChainsyncServerFindIntersect_LedgerErrorPropagates verifies ledger
// lookup failures are wrapped and returned to the protocol layer.
func TestChainsyncServerFindIntersect_LedgerErrorPropagates(
	t *testing.T,
) {
	t.Parallel()

	// Move the ledger past origin so malformed point data reaches the
	// database-backed intersection lookup.
	o := newFindIntersectTestOuroboros(t)
	connId := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	block := &testBlock{
		BlockHeader: &testBlockHeader{
			hash:        gledger.Blake2b256{0x01},
			blockNumber: 1,
			slotNumber:  10,
		},
		blockType: 1,
		cbor:      []byte{0x80},
	}
	require.NoError(
		t,
		o.ledgerState.Chain().AddBlock(context.Background(), block, nil),
	)
	setTestLedgerTip(t, o, ochainsync.Tip{
		Point: ocommon.NewPoint(
			block.SlotNumber(),
			block.Hash().Bytes(),
		),
		BlockNumber: block.BlockNumber(),
	})

	// Submit a malformed point hash that causes the ledger lookup to fail
	// while resolving the candidate block.
	limiter := newChainsyncFindIntersectRateLimiter(200, 1000)
	_, _, err := o.chainsyncServerFindIntersect(
		context.Background(),
		limiter,
		ochainsync.CallbackContext{ConnectionId: connId},
		[]ocommon.Point{ocommon.NewPoint(10, []byte{0xff})},
	)

	// The server wraps and returns the ledger error to the protocol layer
	// instead of hiding it as an ordinary miss.
	require.ErrorContains(t, err, "get intersect point")
	require.ErrorContains(t, err, "parsing block key")
}

// TestChainsyncServerFindIntersect_ClientRegistrationFailure verifies
// successful intersections still fail when server client state cannot register.
func TestChainsyncServerFindIntersect_ClientRegistrationFailure(
	t *testing.T,
) {
	t.Parallel()

	// Use a ledger that can intersect at origin, but a ChainsyncState without
	// a chain provider so client registration must fail.
	o := newFindIntersectTestOuroboros(t)
	o.chainsyncState = dchainsync.NewState(o.eventBus, nil)
	connId := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")

	// Perform FindIntersect with origin so registration is the first failing
	// operation after a successful intersection.
	limiter := newChainsyncFindIntersectRateLimiter(200, 1000)
	_, _, err := o.chainsyncServerFindIntersect(
		context.Background(),
		limiter,
		ochainsync.CallbackContext{ConnectionId: connId},
		[]ocommon.Point{ocommon.NewPointOrigin()},
	)

	// The registration error is surfaced to the caller.
	require.ErrorContains(t, err, "add chainsync client")
	require.ErrorContains(t, err, "no chain provider available")
}

// TestChainsyncServerFindIntersect_ServedActivityOnlyWhenServed verifies a
// rejected FindIntersect does not count as downstream consumption: a warm
// inbound peer must not dodge the idle prune with requests we refuse.
func TestChainsyncServerFindIntersect_ServedActivityOnlyWhenServed(
	t *testing.T,
) {
	t.Parallel()

	o := newFindIntersectTestOuroboros(t)
	var reports atomic.Int64
	o.servedActivityHook = func(ouroboros.ConnectionId) { reports.Add(1) }
	limiter := newChainsyncFindIntersectRateLimiter(200, 1000)

	// Oversized point list: rejected before any lookup.
	tooMany := make([]ocommon.Point, chainsyncMaxFindIntersectPoints+1)
	for i := range tooMany {
		tooMany[i] = ocommon.NewPointOrigin()
	}
	_, _, err := o.chainsyncServerFindIntersect(
		context.Background(),
		limiter,
		ochainsync.CallbackContext{
			ConnectionId: newTestConnId("127.0.0.1:6000", "1.1.1.1:3001"),
		},
		tooMany,
	)
	require.ErrorIs(t, err, ochainsync.ErrIntersectNotFound)
	assert.Zero(t, reports.Load(), "a rejected request is not served activity")

	// A served intersection is reported.
	_, _, err = o.chainsyncServerFindIntersect(
		context.Background(),
		limiter,
		ochainsync.CallbackContext{
			ConnectionId: newTestConnId("127.0.0.1:6000", "1.1.1.2:3001"),
		},
		[]ocommon.Point{ocommon.NewPointOrigin()},
	)
	require.NoError(t, err)
	assert.Equal(t, int64(1), reports.Load())
}

// TestChainsyncServerRequestNext_AddClientFailure verifies RequestNext returns
// registration errors before attempting any protocol response.
func TestChainsyncServerRequestNext_AddClientFailure(
	t *testing.T,
) {
	t.Parallel()

	// Configure RequestNext with ChainsyncState that cannot build a
	// server-side iterator for the downstream client.
	o := newFindIntersectTestOuroboros(t)
	o.chainsyncState = dchainsync.NewState(o.eventBus, nil)
	connId := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")

	// Enter RequestNext before any protocol response can be sent.
	err := o.chainsyncServerRequestNext(
		ochainsync.CallbackContext{ConnectionId: connId},
		false,
	)

	// AddClient failure is returned directly from the callback.
	require.ErrorContains(t, err, "add chainsync client")
	require.ErrorContains(t, err, "no chain provider available")
}

func TestNormalizeIntersectPoints(t *testing.T) {
	t.Parallel()

	points := []ocommon.Point{
		ocommon.NewPoint(20, []byte("b")),
		ocommon.NewPoint(30, []byte("c")),
		ocommon.NewPoint(20, []byte("b")),
		ocommon.NewPointOrigin(),
		ocommon.NewPointOrigin(),
	}

	normalized := normalizeIntersectPoints(points)

	require.Equal(
		t,
		[]ocommon.Point{
			ocommon.NewPoint(20, []byte("b")),
			ocommon.NewPoint(30, []byte("c")),
			ocommon.NewPointOrigin(),
		},
		normalized,
	)
}

// The apply gate (ChainsyncApplyEligible) withholds a peer's headers from the
// ledger while still observing its tips for chain selection: an uncorroborated
// Genesis fast source is seen but cannot steer the ledger (no post-denial
// ingress). This is the ouroboros-layer enforcement of the corroboration stall.
func TestChainsyncClientRollForwardApplyGateWithholdsLedgerButObservesTip(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	require.True(t, state.AddClientConnId(conn))

	applyEligible := false
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return applyEligible
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	// Apply denied: the tip is observed for chain selection, but the header is
	// NOT applied to the ledger.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	))
	select {
	case evt := <-tipCh:
		_, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(t, ok, "tip must be observed even when apply is denied")
	case <-time.After(time.Second):
		t.Fatal("expected PeerTipUpdateEvent (observation) while apply denied")
	}
	select {
	case <-ledgerCh:
		t.Fatal("ledger ingress must be withheld while apply is denied")
	case <-time.After(200 * time.Millisecond):
	}

	// Apply now allowed (peer corroborated): the same header is applied.
	applyEligible = true
	header2 := newTestBlockHeader(101, 2, 0xbb)
	tip2 := ochainsync.Tip{
		Point:       ocommon.NewPoint(101, header2.Hash().Bytes()),
		BlockNumber: 2,
	}
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header2,
		tip2,
	))
	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, conn, data.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal("expected ledger ingress once apply is allowed")
	}
}

// A header first seen from an uncorroborated (apply-denied) peer is withheld but
// must NOT be permanently deduplicated: the point is recorded without a dedup
// entry, so when a corroborated apply-eligible peer later delivers it the header
// is still published — even under the parallel strategy, which never replays
// duplicates.
func TestChainsyncClientRollForward_WithheldHeaderNotPermanentlyDeduped(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	// Distinct remote hosts so the two peers count as independent corroborators.
	connA := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(
			e chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return cs.ObservePeerTipUpdate(e)
		},
		ChainsyncApplyEligible: cs.ShouldApplyIngress,
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	// connA delivers the header while uncorroborated: withheld, and NOT recorded
	// in the cross-peer dedup cache.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	))
	select {
	case <-ledgerCh:
		t.Fatal("connA header must be withheld while uncorroborated")
	case <-time.After(200 * time.Millisecond):
	}

	// connB delivers the same header. connA and connB now corroborate each
	// other, so connB is apply-eligible. Because connA's delivery was not
	// deduplicated, the header is still "new" and the parallel strategy
	// publishes it — the point is not lost.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	))
	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connB, data.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal(
			"corroborated peer must be able to publish a point first seen " +
				"from an uncorroborated (withheld) peer",
		)
	}
}

// With the synchronous observe hook wired, the Genesis apply gate can inspect
// a candidate from this header without making it selectable before ledger
// admission. A header that establishes corroboration is therefore submitted
// in the same roll-forward, while rejected candidates remain invisible.
// The roll-backward apply gate must reflect the rollback currently being
// admitted: a rollback trims the peer's observed frontier (via ApplyRollback),
// which can change its corroboration status, so the observation must be applied
// to chain selection synchronously before the apply-eligibility check. Here a
// peer corroborated on its pre-rollback frontier rolls back below that frontier
// (trimming its observed points to empty), which makes it uncorroborated; the
// rollback must therefore be withheld from the ledger — decided in the same
// roll-backward call, with no async lag.
func TestChainsyncClientRollBackwardSyncObservationOrdersApplyGate(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	state := dchainsync.NewState(bus, nil)
	// Distinct remote hosts so the two peers count as independent corroborators.
	connP := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	connW := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(connP))
	require.True(t, state.AddClientConnId(connW))

	mkTip := func(slot uint64, hash string, block uint64) ochainsync.Tip {
		return ochainsync.Tip{
			Point:       ocommon.Point{Slot: slot, Hash: []byte(hash)},
			BlockNumber: block,
		}
	}
	// P and W corroborate each other on slots 100 and 105.
	for _, c := range []ouroboros.ConnectionId{connP, connW} {
		cs.UpdatePeerTip(c, mkTip(100, "h100", 100), nil)
		cs.UpdatePeerTip(c, mkTip(105, "h105", 105), nil)
	}
	require.True(t, cs.ShouldApplyIngress(connP),
		"P must be apply-eligible (corroborated) before the rollback")

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		// Synchronous observation, exactly like node.chainsyncObservePeerRollback.
		ChainsyncObservePeerRollback: func(
			e chainselection.PeerRollbackEvent,
		) bool {
			cs.HandlePeerRollbackEvent(
				event.NewEvent(chainselection.PeerRollbackEventType, e),
			)
			return true
		},
		ChainsyncApplyEligible: cs.ShouldApplyIngress,
	})
	o.chainsyncState = state
	o.eventBus = bus

	// P rolls back to slot 99, below its entire corroborated frontier, trimming
	// its observed points to empty. Its synchronous observation makes P
	// uncorroborated before the apply gate, so the rollback is withheld.
	rollbackPoint := ocommon.NewPoint(99, []byte("rb99"))
	require.NoError(t, o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: connP},
		rollbackPoint,
		mkTip(99, "rb99", 99),
	))
	testutil.RequireNoReceive(
		t,
		ledgerCh,
		200*time.Millisecond,
		"rollback must be withheld: the apply gate must reflect the "+
			"post-rollback (trimmed) corroboration state",
	)
	require.False(t, cs.ShouldApplyIngress(connP),
		"P must be uncorroborated after the rollback trims its frontier")
}

func TestChainsyncClientRollForwardSyncObservationOrdersApplyGate(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})

	state := dchainsync.NewState(bus, nil)
	// Distinct remote hosts so the two peers count as independent corroborators.
	connA := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	// Prefer connA at an equal corroborated frontier so its first delivered
	// header is also the first selectable switch to connA.
	cs.SetConnectionPriority(connA, 1)
	var switchAccepted bool

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		// Stage the candidate synchronously, as the node does before the
		// Genesis apply gate.
		ChainsyncObservePeerTip: func(
			e chainselection.PeerTipUpdateEvent,
		) chainselection.PeerTipUpdateDisposition {
			return cs.PreparePeerTipAdmissionDisposition(e)
		},
		ChainsyncResolvePeerTip: func(
			e chainselection.PeerTipUpdateEvent,
			admitted bool,
		) {
			if !admitted {
				cs.RejectPeerTipAdmission(e)
				return
			}
			previousBest := cs.GetBestPeer()
			cs.CommitPeerTipAdmission(e)
			best := cs.GetBestPeer()
			if previousBest == nil && best != nil {
				// Admission updates the tracked cursor before committing the
				// candidate, so the selected client is ready for the handoff.
				switchAccepted = state.TrySetClientConnId(*best)
			}
		},
		ChainsyncApplyEligible: cs.ShouldApplyIngress,
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	// connB delivers the header first. It is observed but uncorroborated (no
	// witness yet), so it is withheld from the ledger.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	))
	select {
	case <-ledgerCh:
		t.Fatal("connB header must be withheld while uncorroborated")
	case <-time.After(200 * time.Millisecond):
	}

	// connA (the driver) delivers the same header. Its synchronous observation
	// makes connA and connB corroborate each other, so by the time the apply
	// gate runs connA is corroborated and the header is applied — in the same
	// roll-forward, with no async lag.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	))
	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connA, data.ConnectionId)
		require.NotNil(t, data.PeerTipAdmission)
		data.PeerTipAdmission(true)
	case <-time.After(time.Second):
		t.Fatal(
			"corroborating header must be applied in the same roll-forward",
		)
	}
	require.True(t, switchAccepted,
		"the first corroborated switch must accept its delivered client")
	active := state.GetClientConnId()
	require.NotNil(t, active)
	require.Equal(t, connA, *active)
	trackedA := state.GetTrackedClient(connA)
	require.NotNil(t, trackedA)
	require.Equal(t, uint64(1), trackedA.HeadersRecv,
		"pre-selection tracking must not double-count the delivered header")
}

func TestChainsyncRejectedHeaderRemovesStagedPeerTip(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	conn := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(conn))
	prefixHeader := newTestBlockHeader(99, 0, 0xa9)
	prefixPoint := ocommon.NewPoint(99, prefixHeader.Hash().Bytes())
	state.RecordObservedHeader(dchainsync.ObservedHeader{
		ConnectionId: conn,
		BlockHeader:  prefixHeader,
		Point:        prefixPoint,
	})
	selector := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
		StaleTipThreshold:     time.Hour,
	})
	witnessConn := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}
	selector.UpdatePeerTip(witnessConn, tip, nil)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(update chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
			selector.PreparePeerTipAdmission(update)
			return chainselection.PeerTipUpdateAccepted
		},
		ChainsyncResolvePeerTip: selector.ResolvePeerTipAdmission,
	})
	o.eventBus = bus
	o.chainsyncState = state
	o.ledgerState = newTestLedgerState(t)

	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	))
	evt := testutil.RequireReceive(
		t,
		ledgerCh,
		time.Second,
		"header sent for ledger admission",
	)
	chainsyncEvent, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.NotNil(t, chainsyncEvent.PeerTipUpdate)
	_, _, found := state.LookupObservedHeader(conn, header.Hash().Bytes())
	require.True(t, found, "pending header should be available until admission resolves")
	require.NotContains(t, selector.GetAllPeerTips(), conn,
		"staged header must not be selector-visible before ledger admission")
	require.True(t, selector.ShouldApplyIngress(conn),
		"the staged candidate should be usable for Genesis corroboration")
	require.NotNil(t, chainsyncEvent.PeerTipAdmission)
	chainsyncEvent.PeerTipAdmission(false)
	_, _, found = state.LookupObservedHeader(conn, header.Hash().Bytes())
	require.True(t, found,
		"delivery rejection alone must not invalidate another delivery of the point")
	require.NotNil(t, chainsyncEvent.InvalidatePeerHeaderHistory)
	chainsyncEvent.InvalidatePeerHeaderHistory()
	_, _, found = state.LookupObservedHeader(conn, header.Hash().Bytes())
	require.False(t, found, "rejected header must leave no trusted ancestry")
	_, _, prefixFound := state.LookupObservedHeader(conn, prefixHeader.Hash().Bytes())
	require.True(t, prefixFound, "rejection must retain earlier verified ancestry")
	require.NotContains(t, selector.GetAllPeerTips(), conn,
		"rejected header must leave no selector-visible peer tip")
	require.False(t, selector.ShouldApplyIngress(conn),
		"rejection must synchronously remove the candidate from corroboration")
	require.Contains(t, o.deferredHeaderRecovery, conn)

	laterHeader := newTestBlockHeader(101, 2, 0xab)
	laterTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(101, laterHeader.Hash().Bytes()),
		BlockNumber: 2,
	}
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		laterHeader,
		laterTip,
	))
	laterEvent := testutil.RequireReceive(
		t,
		ledgerCh,
		time.Second,
		"later header sent for ledger admission",
	)
	laterChainsyncEvent, ok := laterEvent.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.NotNil(t, laterChainsyncEvent.PeerTipAdmission)
	laterChainsyncEvent.PeerTipAdmission(true)
	require.Contains(t, o.deferredHeaderRecovery, conn,
		"later admission must not cancel recovery for a rejected prefix")
}

func TestChainsyncClientRollForwardReplaysDuplicateFromSelectedPeerSeenElsewhere(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	selectTrackedChainsyncClient(t, state, connA)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	evt1 := <-ch
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connB, data1.ConnectionId)

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt2 := <-ch:
		data2, ok := evt2.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connA, data2.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal(
			"expected selected peer to replay duplicate header first seen elsewhere",
		)
	}
}

// A suppressed header at a new point is forward protocol activity, while
// replaying that point must not keep an otherwise stalled client eligible.
// Neither delivery moves the admitted cursor.
func TestChainsyncClientRollForwardSuppressedHeaderAdvancesActivityOnce(
	t *testing.T,
) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		applyEligible bool
	}{
		{name: "duplicate", applyEligible: true},
		{name: "withheld", applyEligible: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bus := event.NewEventBus(nil, nil)
			defer bus.Close()
			_, ch := bus.Subscribe(ledger.ChainsyncEventType)
			now := time.Unix(1, 0)
			state := dchainsync.NewStateWithConfig(
				bus,
				nil,
				dchainsync.Config{
					MaxClients:   3,
					StallTimeout: time.Minute,
					Now:          func() time.Time { return now },
				},
			)
			connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
			connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
			require.True(t, state.AddClientConnId(connA))
			require.True(t, state.AddClientConnId(connB))
			selectTrackedChainsyncClient(t, state, connA)
			o := newOuroboros(OuroborosConfig{
				EventBus: bus,
				ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
					return true
				},
				ChainsyncApplyEligible: func(
					connId ouroboros.ConnectionId,
				) bool {
					return connId == connA || tt.applyEligible
				},
			})
			o.chainsyncState = state
			o.eventBus = bus

			header := newTestBlockHeader(100, 1, 0xaa)
			tip := ochainsync.Tip{
				Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
				BlockNumber: 1,
			}
			require.NoError(t, o.chainsyncClientRollForward(
				ochainsync.CallbackContext{ConnectionId: connA},
				0,
				header,
				tip,
			))
			testutil.RequireReceive(
				t, ch, time.Second, "active peer header published",
			)

			before := state.GetTrackedClient(connB)
			require.NotNil(t, before)
			now = now.Add(time.Second)
			require.NoError(t, o.chainsyncClientRollForward(
				ochainsync.CallbackContext{ConnectionId: connB},
				0,
				header,
				tip,
			))
			after := state.GetTrackedClient(connB)
			require.NotNil(t, after)
			require.Equal(t, before.HeadersRecv+1, after.HeadersRecv,
				"suppressed delivery must count as client activity")
			require.Equal(t, now, after.LastActivity)
			require.Equal(t, before.Cursor, after.Cursor,
				"suppressed delivery must not move the cursor")

			now = now.Add(2 * time.Minute)
			require.NoError(t, o.chainsyncClientRollForward(
				ochainsync.CallbackContext{ConnectionId: connB},
				0,
				header,
				tip,
			))
			replayed := state.GetTrackedClient(connB)
			require.NotNil(t, replayed)
			require.Equal(t, after.LastActivity, replayed.LastActivity,
				"replayed suppressed header must not refresh activity")
			require.Contains(t, state.CheckStalledClients(), connB)
		})
	}
}

func TestChainsyncClientRollForwardRecordsDeliveryBeforeLedgerBackpressure(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, ledgerCh := bus.SubscribeWithBuffer(ledger.ChainsyncEventType, 1)
	bus.Publish(
		ledger.ChainsyncEventType,
		event.NewEvent(ledger.ChainsyncEventType, "occupy buffer"),
	)
	now := time.Unix(2, 0)
	state := dchainsync.NewStateWithConfig(
		bus,
		nil,
		dchainsync.Config{
			MaxClients:   1,
			StallTimeout: time.Minute,
			Now:          func() time.Time { return now },
		},
	)
	conn := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	require.True(t, state.AddClientConnId(conn))
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncObservePeerTip: func(chainselection.PeerTipUpdateEvent) chainselection.PeerTipUpdateDisposition {
			return chainselection.PeerTipUpdateAccepted
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	point := ocommon.NewPoint(100, header.Hash().Bytes())
	tip := ochainsync.Tip{Point: point, BlockNumber: 1}
	done := make(chan error, 1)
	go func() {
		done <- o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			header,
			tip,
		)
	}()

	require.Eventually(t, func() bool {
		client := state.GetTrackedClient(conn)
		return client != nil && client.HeadersRecv == 1 &&
			client.LastActivity.Equal(now)
	}, time.Second, time.Millisecond)
	blocked := state.GetTrackedClient(conn)
	require.NotNil(t, blocked)
	assert.Equal(t, ocommon.Point{}, blocked.Cursor)
	select {
	case err := <-done:
		require.Failf(t, "roll forward returned before ledger capacity", "%v", err)
	default:
	}

	<-ledgerCh
	var chainsyncEvent ledger.ChainsyncEvent
	select {
	case evt := <-ledgerCh:
		var ok bool
		chainsyncEvent, ok = evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
	case <-time.After(time.Second):
		t.Fatal("ledger event was not published after capacity became available")
	}
	require.NoError(t, <-done)
	require.NotNil(t, chainsyncEvent.PeerTipAdmission)
	chainsyncEvent.PeerTipAdmission(true)
	admitted := state.GetTrackedClient(conn)
	require.NotNil(t, admitted)
	assert.Equal(t, point, admitted.Cursor)
	assert.Equal(t, uint64(1), admitted.HeadersRecv)
	assert.Equal(t, now, admitted.LastActivity)
}

func TestChainsyncClientRollForwardReplaysDuplicateFromEquivalentSelectedPeerSeenElsewhere(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connADup := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connADup))
	require.True(t, state.AddClientConnId(connB))
	selectTrackedChainsyncClient(t, state, connA)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	evt1 := <-ch
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connB, data1.ConnectionId)

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connADup},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt2 := <-ch:
		data2, ok := evt2.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connADup, data2.ConnectionId)
	case <-time.After(time.Second):
		t.Fatal(
			"expected equivalent selected peer to replay duplicate header first seen elsewhere",
		)
	}
}

func TestChainsyncClientRollForwardDropsDuplicateFromSameSelectedPeer(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	require.True(t, state.AddClientConnId(connA))
	selectTrackedChainsyncClient(t, state, connA)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	evt1 := <-ch
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connA, data1.ConnectionId)

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt2 := <-ch:
		t.Fatalf(
			"expected same-connection duplicate to be dropped, got event: %#v",
			evt2,
		)
	case <-time.After(200 * time.Millisecond):
	}
}

// Under the parallel strategy, two eligible peers offering the same header
// must not push that header into ledger processing twice: the first reporter
// publishes it and the duplicate from the other peer is suppressed (no
// active-peer replay).
func TestChainsyncClientRollForward_ParallelMultiPeerNoDoubleIngress(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	selectTrackedChainsyncClient(t, state, connA)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	// First reporter (B) publishes the header.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connB},
		0,
		header,
		tip,
	))
	evt1 := testutil.RequireReceive(
		t, ch, time.Second, "expected first reporter to publish the header",
	)
	data1, ok := evt1.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, connB, data1.ConnectionId)

	// The active peer (A) reporting the same header must NOT replay it under
	// the parallel strategy.
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connA},
		0,
		header,
		tip,
	))
	testutil.RequireNoReceive(
		t,
		ch,
		200*time.Millisecond,
		"expected duplicate from second peer to be suppressed",
	)
}

// Under the parallel strategy, multiple eligible peers can supply different
// headers concurrently without corrupting ledger ingress ordering. Each
// distinct header enters the ledger queue exactly once, in arrival order,
// attributed to the peer that reported it first.
func TestChainsyncClientRollForward_ParallelMultiPeerOrdering(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	cfg := dchainsync.DefaultConfig()
	cfg.HeaderSyncStrategy = dchainsync.HeaderSyncStrategyParallel
	state := dchainsync.NewStateWithConfig(bus, nil, cfg)
	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	type step struct {
		conn   ouroboros.ConnectionId
		slot   uint64
		block  uint64
		hashID byte
	}
	// Interleave reporters; the duplicate (B re-reporting slot 100) must be
	// dropped, leaving an ordered, deduplicated ingress stream.
	steps := []step{
		{connA, 100, 1, 0xa0},
		{connB, 101, 2, 0xb1},
		{connB, 100, 1, 0xa0}, // duplicate of slot 100 -> suppressed
		{connA, 102, 3, 0xa2},
	}
	for _, s := range steps {
		header := newTestBlockHeader(s.slot, s.block, s.hashID)
		tip := ochainsync.Tip{
			Point:       ocommon.NewPoint(s.slot, header.Hash().Bytes()),
			BlockNumber: s.block,
		}
		require.NoError(t, o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: s.conn},
			0,
			header,
			tip,
		))
	}

	type ingress struct {
		slot uint64
		conn ouroboros.ConnectionId
	}
	want := []ingress{
		{100, connA},
		{101, connB},
		{102, connA},
	}
	for i, w := range want {
		evt := testutil.RequireReceive(
			t,
			ch,
			time.Second,
			fmt.Sprintf(
				"missing expected ingress event %d (slot %d)",
				i,
				w.slot,
			),
		)
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, w.slot, data.Point.Slot, "event %d slot", i)
		require.Equal(t, w.conn, data.ConnectionId, "event %d conn", i)
	}
	testutil.RequireNoReceive(
		t, ch, 200*time.Millisecond, "expected no extra ingress event",
	)
}

func TestChainsyncClientRollForward_IneligiblePeerDoesNotPoisonDedup(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connEligible := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connIneligible := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connEligible))
	require.True(t, state.AddClientConnId(connIneligible))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(connId ouroboros.ConnectionId) bool {
			return connId == connEligible
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	header := newTestBlockHeader(42, 7, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(42, header.Hash().Bytes()),
		BlockNumber: 7,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connIneligible},
		0,
		header,
		tip,
	)
	require.NoError(t, err)
	select {
	case evt := <-ledgerCh:
		t.Fatalf("unexpected ledger event from ineligible peer: %#v", evt)
	default:
	}

	err = o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connEligible},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connEligible, data.ConnectionId)
		require.Equal(t, tip.Point.Slot, data.Point.Slot)
	case <-time.After(2 * time.Second):
		t.Fatal("expected eligible peer header to feed the ledger")
	}
}

func TestRegisterTrackedChainsyncClient_ObservabilityOnlyDoesNotConsumePool(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connObserved := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	connEligible := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	state := dchainsync.NewStateWithConfig(bus, nil, dchainsync.Config{
		MaxClients:   1,
		StallTimeout: time.Minute,
	})
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = state

	require.True(t, o.registerTrackedChainsyncClient(connObserved, false, true))
	observabilityOnly, exists := state.ClientObservabilityOnly(connObserved)
	require.True(t, exists)
	require.True(t, observabilityOnly)
	outbound, exists := state.ClientStartedAsOutbound(connObserved)
	require.True(t, exists)
	require.True(t, outbound)
	require.False(t, o.isInboundChainsyncClient(connObserved))
	require.Equal(t, 0, state.ClientConnCount())

	require.True(t, o.registerTrackedChainsyncClient(connEligible, true, true))
	require.Equal(t, 1, state.ClientConnCount())

	active := state.GetClientConnId()
	require.Nil(t, active)
}

func TestRegisterTrackedChainsyncClient_PromotedObservedKeepsDirection(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connId := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewStateWithConfig(bus, nil, dchainsync.Config{
		MaxClients:   1,
		StallTimeout: time.Minute,
	})
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = state

	require.True(t, o.registerTrackedChainsyncClient(connId, false, true))
	observabilityOnly, exists := state.ClientObservabilityOnly(connId)
	require.True(t, exists)
	require.True(t, observabilityOnly)
	require.False(t, o.isInboundChainsyncClient(connId))

	require.True(t, o.registerTrackedChainsyncClient(connId, true, true))
	observabilityOnly, exists = state.ClientObservabilityOnly(connId)
	require.True(t, exists)
	require.False(t, observabilityOnly)
	outbound, exists := state.ClientStartedAsOutbound(connId)
	require.True(t, exists)
	require.True(t, outbound)
	require.False(t, o.isInboundChainsyncClient(connId))
}

func TestHandlePeerEligibilityChangedEvent_DemotesObservedIngress(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	selectTrackedChainsyncClient(t, state, connA)
	state.UpdateClientTip(
		connA,
		ocommon.NewPoint(200, []byte("ha")),
		ochainsync.Tip{Point: ocommon.NewPoint(200, []byte("ha"))},
	)
	state.UpdateClientTip(
		connB,
		ocommon.NewPoint(100, []byte("hb")),
		ochainsync.Tip{Point: ocommon.NewPoint(100, []byte("hb"))},
	)

	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = state
	o.HandlePeerEligibilityChangedEvent(event.NewEvent(
		peergov.PeerEligibilityChangedEventType,
		peergov.PeerEligibilityChangedEvent{
			ConnectionId: connA,
			Eligible:     false,
		},
	))

	observabilityOnly, exists := state.ClientObservabilityOnly(connA)
	require.True(t, exists)
	require.True(t, observabilityOnly)

	active := state.GetClientConnId()
	require.Nil(t, active)
}

func TestChainsyncClientRollForward_UntrackedPeerDoesNotPublishToLedger(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connId := newTestConnId("127.0.0.1:6000", "3.3.3.3:3001")
	state := dchainsync.NewState(bus, nil)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	header := newTestBlockHeader(42, 7, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(42, header.Hash().Bytes()),
		BlockNumber: 7,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connId},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		t.Fatalf("unexpected ledger event from untracked peer: %#v", evt)
	default:
	}
}

func TestSubscribeChainsyncResyncRewindsClientsWithoutRecycle(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connB := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	rollbackPoint := ocommon.NewPoint(90, []byte("rollback"))
	point := ocommon.NewPoint(100, []byte("hdr"))
	tip := ochainsync.Tip{Point: point}

	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	state.UpdateClientTip(
		connA,
		ocommon.NewPoint(120, []byte("ahead")),
		ochainsync.Tip{
			Point: ocommon.NewPoint(120, []byte("ahead")),
		},
	)
	state.UpdateClientTip(connB, point, tip)
	require.True(
		t,
		state.HeaderPreviouslySeenFromOtherConn(connA, point),
	)

	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = state
	o.eventBus = bus

	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)
	ctx := t.Context()
	o.SubscribeChainsyncResync(ctx)

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				Reason: event.ChainsyncResyncReasonLocalLedgerRollback,
				Point:  rollbackPoint,
			},
		),
	)

	select {
	case evt := <-recycleCh:
		t.Fatalf("unexpected recycle request: %#v", evt)
	case <-time.After(100 * time.Millisecond):
	}

	require.False(
		t,
		state.HeaderPreviouslySeenFromOtherConn(connA, point),
	)
	tc := state.GetTrackedClient(connA)
	require.NotNil(t, tc)
	require.Equal(t, rollbackPoint, tc.Cursor)
}

func TestSubscribeChainsyncResyncDoesNotRecycleOnLocalRollbackWithoutPeerHistory(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	rollbackPoint := ocommon.NewPoint(90, []byte("rollback"))

	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	// Keep the tracked cursor at the rollback point so
	// RewindTrackedClientsTo returns no connections. The local rollback
	// still needs to resynchronize the live tracked session.
	state.UpdateClientTip(
		connA,
		rollbackPoint,
		ochainsync.Tip{Point: rollbackPoint},
	)
	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = state
	o.eventBus = bus
	o.ledgerState = newTestLedgerState(t)

	_, recycleCh := bus.Subscribe(
		connmanager.ConnectionRecycleRequestedEventType,
	)
	ctx := t.Context()
	o.SubscribeChainsyncResync(ctx)

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				Reason: event.ChainsyncResyncReasonLocalLedgerRollback,
				Point:  rollbackPoint,
			},
		),
	)

	// The fallback path should not request peer-governance recycling here.
	// Recovery may close the connection for a fresh reconnect instead.
	select {
	case evt := <-recycleCh:
		t.Fatalf("unexpected recycle request: %#v", evt)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestSubscribeChainsyncResyncClosesConnectionForFreshSyncReasons(
	t *testing.T,
) {
	t.Parallel()

	reasons := []string{
		event.ChainsyncResyncReasonLocalTipPlateau,
		event.ChainsyncResyncReasonPostPlateauRealign,
		event.ChainsyncResyncReasonRollbackNotFound,
		event.ChainsyncResyncReasonPersistentFork,
		event.ChainsyncResyncReasonRollbackExceedsK,
		event.ChainsyncResyncReasonForkResolutionExceedsK,
		event.ChainsyncResyncReasonRollbackLoop,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		event.ChainsyncResyncReasonPeerTipBehindMithril,
		event.ChainsyncResyncReasonLiveTxValidationRecovery,
		event.ChainsyncResyncReasonDeterministicTxValidationRecovery,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
		event.ChainsyncResyncReasonReplayRecoveryNonConverging,
		event.ChainsyncResyncReasonChainSwitchCursorAhead,
		// Every re-sync reason replaces the connection: an in-place
		// re-intersect sends FindIntersect while RequestNext replies are
		// still in flight, which the peer's replies then violate.
		event.ChainsyncResyncReasonRollbackAhead,
		event.ChainsyncResyncReasonHeaderValidationRecovery,
		event.ChainsyncResyncReasonForecastCorroborationRecovery,
		event.ChainsyncResyncReasonBlockfetchRangeUnavailable,
		event.ChainsyncResyncReasonBlockfetchTimeoutRetryFailed,
		event.ChainsyncResyncReasonForkQueueOverflowRestartFailed,
		event.ChainsyncResyncReasonForkExtensionRestartFailed,
		event.ChainsyncResyncReasonFutureHeaderAdmissionRecovery,
		event.ChainsyncResyncReasonDisconnectedHeaderAdmission,
		event.ChainsyncResyncReasonForecastHorizonNoWork,
		event.ChainsyncResyncReasonBehindLocalTip,
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			logBuf := &lockedBuffer{}
			logger := slog.New(
				slog.NewJSONHandler(
					logBuf,
					&slog.HandlerOptions{Level: slog.LevelDebug},
				),
			)
			bus := event.NewEventBus(nil, logger)
			defer bus.Close()

			connManager := connmanager.NewConnectionManager(
				connmanager.ConnectionManagerConfig{
					EventBus: bus,
					Logger:   logger,
				},
			)
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stopCancel()
				_ = connManager.Stop(stopCtx)
			})

			mockConn := ouroboros_mock.NewConnection(
				ouroboros_mock.ProtocolRoleClient,
				ouroboros_mock.ConversationKeepAlive,
			)
			oConn, err := ouroboros.New(
				ouroboros.WithConnection(mockConn),
				ouroboros.WithNetworkMagic(
					ouroboros_mock.MockNetworkMagic,
				),
				ouroboros.WithNodeToNode(true),
				ouroboros.WithKeepAlive(true),
				ouroboros.WithKeepAliveConfig(
					keepalive.NewConfig(
						keepalive.WithCookie(
							ouroboros_mock.MockKeepAliveCookie,
						),
						keepalive.WithPeriod(30*time.Second),
						keepalive.WithTimeout(15*time.Second),
					),
				),
			)
			require.NoError(t, err)
			connManager.AddConnection(oConn, false, "127.0.0.1:1234")

			o := newOuroboros(OuroborosConfig{
				EventBus: bus,
				Logger:   logger,
			})
			o.eventBus = bus
			o.connManager = connManager

			ctx := t.Context()
			o.SubscribeChainsyncResync(ctx)

			connId := oConn.Id()
			bus.Publish(
				event.ChainsyncResyncEventType,
				event.NewEvent(
					event.ChainsyncResyncEventType,
					event.ChainsyncResyncEvent{
						ConnectionId: connId,
						Reason:       reason,
					},
				),
			)

			require.Eventually(
				t,
				func() bool {
					return connManager.GetConnectionById(connId) == nil
				},
				2*time.Second,
				20*time.Millisecond,
			)
			require.Eventually(
				t,
				func() bool {
					logs := logBuf.String()
					return strings.Contains(
						logs,
						`"msg":"closing connection for fresh chainsync"`,
					) && strings.Contains(
						logs,
						`"reason":"`+reason+`"`,
					)
				},
				2*time.Second,
				20*time.Millisecond,
			)
			require.NotContains(
				t,
				logBuf.String(),
				`"msg":"restarting chainsync client"`,
			)
		})
	}
}

func TestSubscribeChainsyncResyncDeniesDivergentPeer(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.eventBus = bus
	o.peerGov = peerGov
	o.SubscribeChainsyncResync(t.Context())

	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "10.0.0.1:3001")
	require.NoError(t, err)
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: connId,
				Reason:       event.ChainsyncResyncReasonRollbackExceedsK,
			},
		),
	)

	require.Eventually(
		t,
		func() bool {
			return peerGov.IsDenied(remoteAddr.String())
		},
		2*time.Second,
		20*time.Millisecond,
	)
}

func TestSubscribeChainsyncResyncDoesNotDenyRollbackLoop(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()

	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.eventBus = bus
	o.peerGov = peerGov
	o.SubscribeChainsyncResync(t.Context())

	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", "10.0.0.1:3001")
	require.NoError(t, err)
	connId := ouroboros.ConnectionId{
		LocalAddr:  localAddr,
		RemoteAddr: remoteAddr,
	}

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: connId,
				Reason:       event.ChainsyncResyncReasonRollbackLoop,
			},
		),
	)

	require.Never(
		t,
		func() bool {
			return peerGov.IsDenied(remoteAddr.String())
		},
		200*time.Millisecond,
		20*time.Millisecond,
	)
}

func TestHeaderPreviouslySeenFromOtherConnTreatsEquivalentConnIdsAsSame(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connA := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connADup := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	point := ocommon.NewPoint(100, []byte("hdr"))
	tip := ochainsync.Tip{Point: point}

	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connA))
	state.UpdateClientTip(connA, point, tip)

	require.False(
		t,
		state.HeaderPreviouslySeenFromOtherConn(connADup, point),
	)
}

// TestChainsyncClientRollForward_InboundUpstreamPublishesWhenEligible
// exercises a full-duplex inbound connection from a configured upstream peer
// (one that ChainsyncIngressEligible recognises as eligible). Even though the
// chainsync client is registered inbound (startedAsOutbound=false), headers
// should flow into the ledger and a PeerTipUpdateEvent should be emitted.
// This covers the single-relay block producer scenario where the relay wins
// the dial race after a crash.
func TestChainsyncClientRollForward_InboundUpstreamPublishesWhenEligible(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	state := dchainsync.NewState(bus, nil)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(connId ouroboros.ConnectionId) bool {
			return connId == connInbound
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	// Register as inbound + ingress-eligible to model a full-duplex inbound
	// from a trusted upstream peer.
	require.True(t, o.registerTrackedChainsyncClient(connInbound, true, false))
	observabilityOnly, exists := state.ClientObservabilityOnly(connInbound)
	require.True(t, exists)
	require.False(
		t,
		observabilityOnly,
		"eligible inbound should not be observability-only",
	)
	require.True(t, o.isInboundChainsyncClient(connInbound))

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connInbound},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, tip.Point.Slot, data.Point.Slot)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"expected eligible inbound header to feed the ledger; " +
				"single-relay producer would stay stuck at tip otherwise",
		)
	}

	select {
	case evt := <-tipCh:
		data, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, tip.Point.Slot, data.Tip.Point.Slot)
	case <-time.After(2 * time.Second):
		t.Fatal("expected PeerTipUpdateEvent for eligible inbound peer")
	}
}

// TestChainsyncClientRollForward_InboundIneligiblePeerStaysObservabilityOnly
// verifies the fix preserves the protection against inbound peers: when peergov
// reports the peer as ineligible (e.g. a random downstream client pulling
// data from us), its headers must not feed the ledger even though chainsync
// is running against it.
func TestChainsyncClientRollForward_InboundIneligiblePeerStaysObservabilityOnly(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return false
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	require.True(t, o.registerTrackedChainsyncClient(connInbound, false, false))
	observabilityOnly, exists := state.ClientObservabilityOnly(connInbound)
	require.True(t, exists)
	require.True(t, observabilityOnly)

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: connInbound},
		0,
		header,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-ledgerCh:
		t.Fatalf(
			"unexpected ledger event from ineligible inbound peer: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case evt := <-tipCh:
		t.Fatalf(
			"unexpected PeerTipUpdateEvent from ineligible inbound peer: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestShouldPublishChainsyncToLedger_InboundFailsClosedWithNilCallback
// verifies that when no ChainsyncIngressEligible policy is wired, an inbound
// full-duplex chainsync client is not treated as ingress-eligible. Outbound
// chainsync retains its legacy default of eligible so the fix does not
// regress existing callers that don't pass a policy. Regression guard.
func TestShouldPublishChainsyncToLedger_InboundFailsClosedWithNilCallback(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connOutbound := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)

	o := newOuroboros(OuroborosConfig{EventBus: bus})
	o.chainsyncState = state
	o.eventBus = bus
	require.Nil(t, o.config.ChainsyncIngressEligible)

	require.True(t, o.registerTrackedChainsyncClient(connOutbound, true, true))
	require.True(t, o.registerTrackedChainsyncClient(connInbound, false, false))

	require.True(
		t,
		o.shouldPublishChainsyncToLedger(connOutbound),
		"outbound default must remain eligible when no policy is wired",
	)
	require.False(
		t,
		o.shouldPublishChainsyncToLedger(connInbound),
		"inbound default must be observability-only when no policy is wired",
	)

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)

	require.NoError(
		t,
		o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: connInbound},
			0,
			header,
			tip,
		),
	)

	select {
	case evt := <-ledgerCh:
		t.Fatalf(
			"inbound peer with nil policy must not feed ledger: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case evt := <-tipCh:
		t.Fatalf(
			"inbound peer with nil policy must not emit PeerTipUpdateEvent: %#v",
			evt,
		)
	case <-time.After(200 * time.Millisecond):
	}

	observabilityOnly, exists := state.ClientObservabilityOnly(connInbound)
	require.True(t, exists)
	require.True(
		t,
		observabilityOnly,
		"reconcile must not upgrade inbound under nil policy",
	)
}

// TestChainsyncClientRollBackward_InboundUpstreamProcessesRollback verifies
// that rollbacks received on an eligible inbound chainsync client are
// forwarded to the ledger. Without the fix, isInboundChainsyncClient
// short-circuits before reconcileChainsyncIngressAdmission and rollbacks are
// silently dropped, so the node can't react to chain reorganisations reported
// by a configured upstream when the relay dialed first.
func TestChainsyncClientRollBackward_InboundUpstreamProcessesRollback(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connInbound := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	state := dchainsync.NewState(bus, nil)

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	require.True(t, o.registerTrackedChainsyncClient(connInbound, true, false))

	_, rollbackCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, chainSelectionRollbackCh := bus.Subscribe(
		chainselection.PeerRollbackEventType,
	)
	rollbackPoint := ocommon.NewPoint(90, []byte("rollback"))
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(95, []byte("tip")),
		BlockNumber: 5,
	}

	err := o.chainsyncClientRollBackward(
		ochainsync.CallbackContext{ConnectionId: connInbound},
		rollbackPoint,
		tip,
	)
	require.NoError(t, err)

	select {
	case evt := <-rollbackCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, rollbackPoint.Slot, data.Point.Slot)
		require.True(t, data.Rollback)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"expected rollback event from eligible inbound peer",
		)
	}

	select {
	case evt := <-chainSelectionRollbackCh:
		data, ok := evt.Data.(chainselection.PeerRollbackEvent)
		require.True(t, ok)
		require.Equal(t, connInbound, data.ConnectionId)
		require.Equal(t, rollbackPoint.Slot, data.Point.Slot)
		require.Equal(t, tip.BlockNumber, data.Tip.BlockNumber)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"expected chainselection rollback event from eligible inbound peer",
		)
	}
}

// newFindIntersectTestOuroboros builds an Ouroboros wired with a fresh,
// empty LedgerState (tip at origin) and ChainsyncState. With the chain at
// origin, GetIntersectPoint returns the origin point for any in-bounds point
// list, so a successful FindIntersect proves the cap did not reject the
// request.
func newFindIntersectTestOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	t.Cleanup(bus.Close)
	ledgerState := newTestLedgerState(t)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		Logger:   logger,
	})
	o.ledgerState = ledgerState
	o.chainsyncState = dchainsync.NewState(bus, ledgerState)
	return o
}

func makeFindIntersectPoints(n int) []ocommon.Point {
	points := make([]ocommon.Point, n)
	for i := range points {
		hash := make([]byte, 32)
		hash[0] = byte(i)
		hash[1] = byte(i >> 8)
		points[i] = ocommon.NewPoint(
			uint64(i+1),
			hash,
		)
	}
	return points
}

// Both Mithril-boundary rejection reasons must deny the peer for a cooldown.
// Without the deny, a peer whose chain is refused at the trust boundary is
// redialed roughly every backoff interval and rejected ~600ms later, forever.
// The connection close every reason triggers is covered by
// TestSubscribeChainsyncResyncClosesConnectionForFreshSyncReasons.
func TestChainsyncResyncDeniesPeerByReason(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		reason         string
		wantDeniesPeer bool
	}{
		{
			reason:         event.ChainsyncResyncReasonRollbackExceedsMithril,
			wantDeniesPeer: true,
		},
		{
			reason:         event.ChainsyncResyncReasonPeerTipBehindMithril,
			wantDeniesPeer: true,
		},
		// Existing behavior pins
		{
			reason:         event.ChainsyncResyncReasonRollbackExceedsK,
			wantDeniesPeer: true,
		},
		{
			reason:         event.ChainsyncResyncReasonLocalTipPlateau,
			wantDeniesPeer: false,
		},
		{
			reason:         event.ChainsyncResyncReasonLiveTxValidationRecovery,
			wantDeniesPeer: false,
		},
		{
			reason:         event.ChainsyncResyncReasonDeterministicTxValidationRecovery,
			wantDeniesPeer: false,
		},
		{
			reason: event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
			// The rollback cannot be crossed locally, so the stale bearer
			// must be replaced before the peer can retry its chain.
			wantDeniesPeer: false,
		},
		{
			reason: event.
				ChainsyncResyncReasonReplayRecoveryNonConverging,
			wantDeniesPeer: false,
		},
		{
			reason:         event.ChainsyncResyncReasonChainSwitchCursorAhead,
			wantDeniesPeer: false,
		},
		{
			reason:         event.ChainsyncResyncReasonForecastCorroborationRecovery,
			wantDeniesPeer: false,
		},
	}
	for _, tt := range tests {
		if got := chainsyncResyncDeniesPeer(tt.reason); got != tt.wantDeniesPeer {
			t.Errorf(
				"chainsyncResyncDeniesPeer(%q) = %v, want %v",
				tt.reason, got, tt.wantDeniesPeer,
			)
		}
	}
}

func TestChainsyncClientRollBackwardUpdatesTrackedClient(t *testing.T) {
	for _, origin := range []bool{false, true} {
		t.Run(fmt.Sprint(origin), func(t *testing.T) {
			bus := event.NewEventBus(nil, nil)
			defer bus.Close()
			state := dchainsync.NewState(bus, nil)
			connID := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
			require.True(t, state.AddClientConnId(connID))
			previous := ocommon.NewPoint(100, []byte("previous"))
			state.UpdateClientTip(
				connID,
				previous,
				ochainsync.Tip{Point: previous},
			)
			state.MarkClientSynced(connID)
			before := state.GetTrackedClient(connID)
			point := ocommon.NewPoint(90, []byte("rollback"))
			if origin {
				point = ocommon.NewPointOrigin()
			}
			tip := ochainsync.Tip{
				Point:       ocommon.NewPoint(110, []byte("tip")),
				BlockNumber: 10,
			}
			observed := false
			o := newOuroboros(OuroborosConfig{
				ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool { return true },
				ChainsyncApplyEligible:   func(ouroboros.ConnectionId) bool { return false },
				ChainsyncObservePeerRollback: func(chainselection.PeerRollbackEvent) bool {
					observed = true
					current := state.GetTrackedClient(connID)
					require.Equal(t, point, current.Cursor)
					require.Equal(t, tip, current.Tip)
					require.Equal(
						t,
						dchainsync.ClientStatusSyncing,
						current.Status,
					)
					require.False(
						t,
						current.LastActivity.Before(before.LastActivity),
					)
					require.Equal(t, before.HeadersRecv, current.HeadersRecv)
					return true
				},
			})
			o.chainsyncState = state
			o.eventBus = bus
			require.NoError(t, o.chainsyncClientRollBackward(
				ochainsync.CallbackContext{ConnectionId: connID}, point, tip,
			))
			require.True(t, observed)
			// Rollback points are not headers and must not enter the dedup cache.
			require.True(t, state.RecordHeaderForDedup(connID, point))
			state.RemoveClientConnId(connID)
			observed = false
			require.NoError(t, o.chainsyncClientRollBackward(
				ochainsync.CallbackContext{ConnectionId: connID}, point, tip,
			))
			require.False(t, observed)
			require.Nil(t, state.GetTrackedClient(connID))
		})
	}
}

// A header past the Limit on Eagerness is held in the roll-forward callback
// (the peer is paused) and reaches the ledger once the limit admits it. Its
// tip is observed for chain selection before the wait, so the limit is
// computed with the header included.
func TestChainsyncClientRollForwardWaitsForEagernessLimitBeforeLedger(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	require.True(t, state.AddClientConnId(conn))

	release := make(chan struct{})
	entered := make(chan uint64, 1)
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncAwaitEagerness: func(
			ctx context.Context,
			connId ouroboros.ConnectionId,
			blockNumber uint64,
			_ func() ochainsync.Tip,
		) error {
			require.Equal(t, conn, connId)
			entered <- blockNumber
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 7, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 7,
	}
	returned := make(chan error, 1)
	go func() {
		returned <- o.chainsyncClientRollForward(
			ochainsync.CallbackContext{ConnectionId: conn},
			0,
			header,
			tip,
		)
	}()

	select {
	case block := <-entered:
		require.Equal(t, uint64(7), block)
	case <-time.After(10 * time.Second):
		t.Fatal("eagerness wait was not consulted for the header")
	}
	select {
	case <-tipCh:
	case <-time.After(10 * time.Second):
		t.Fatal("tip must be observed before the eagerness wait")
	}
	select {
	case <-returned:
		t.Fatal("callback returned while the header is past the limit")
	case <-ledgerCh:
		t.Fatal("header reached the ledger while past the limit")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-returned:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("callback did not return after the limit admitted the header")
	}
	select {
	case evt := <-ledgerCh:
		data, ok := evt.Data.(ledger.ChainsyncEvent)
		require.True(t, ok)
		require.Equal(t, conn, data.ConnectionId)
	case <-time.After(10 * time.Second):
		t.Fatal("header must reach the ledger once admitted")
	}
}

// A wait that fails (the connection closed) must fail the callback without
// handing the header to the ledger.
func TestChainsyncClientRollForwardEagernessWaitErrorWithholdsHeader(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	require.True(t, state.AddClientConnId(conn))

	waitErr := errors.New("connection closed")
	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncAwaitEagerness: func(
			context.Context,
			ouroboros.ConnectionId,
			uint64,
			func() ochainsync.Tip,
		) error {
			return waitErr
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	header := newTestBlockHeader(100, 1, 0xaa)
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(100, header.Hash().Bytes()),
		BlockNumber: 1,
	}
	err := o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		0,
		header,
		tip,
	)
	require.ErrorIs(t, err, waitErr)
	otherConn := newTestConnId("127.0.0.1:6001", "1.1.1.2:3001")
	require.True(
		t,
		state.RecordHeaderForDedup(otherConn, tip.Point),
		"a withheld header must remain available from another peer",
	)
	testutil.RequireNoReceive(
		t,
		ledgerCh,
		200*time.Millisecond,
		"header must not reach the ledger when the eagerness wait fails",
	)
}

// TestSubscribeChainsyncResyncPenalizesOnlyTheResponsibleDeferredHeaderPeer
// pins that a deferred-header failure attributed to one of two connected peers
// closes and denies that peer only.
type chainsyncResyncTestConn struct {
	net.Conn
	localAddr  net.Addr
	remoteAddr net.Addr
}

func (c chainsyncResyncTestConn) LocalAddr() net.Addr  { return c.localAddr }
func (c chainsyncResyncTestConn) RemoteAddr() net.Addr { return c.remoteAddr }

func newChainsyncResyncTestConnection(
	t *testing.T,
	remote string,
) *ouroboros.Connection {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", remote)
	require.NoError(t, err)
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		ouroboros_mock.ConversationKeepAlive,
	)
	conn, err := ouroboros.New(
		ouroboros.WithConnection(chainsyncResyncTestConn{
			Conn:       mockConn,
			localAddr:  localAddr,
			remoteAddr: remoteAddr,
		}),
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
	return conn
}

func TestSubscribeChainsyncResyncPenalizesOnlyTheResponsibleDeferredHeaderPeer(
	t *testing.T,
) {
	t.Parallel()

	require.True(t, chainsyncResyncDeniesPeer(
		event.ChainsyncResyncReasonDeferredHeaderValidationFailure,
	))

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Close()
	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger: logger,
	})
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			EventBus: bus,
			Logger:   logger,
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer stopCancel()
		_ = connManager.Stop(stopCtx)
	})
	badConn := newChainsyncResyncTestConnection(t, "10.0.0.1:3001")
	honestConn := newChainsyncResyncTestConnection(t, "10.0.0.2:3001")
	require.True(t, connManager.AddConnection(
		badConn,
		false,
		"10.0.0.1:3001",
	))
	require.True(t, connManager.AddConnection(
		honestConn,
		false,
		"10.0.0.2:3001",
	))
	o := newOuroboros(OuroborosConfig{EventBus: bus, Logger: logger})
	o.eventBus = bus
	o.peerGov = peerGov
	o.connManager = connManager
	o.SubscribeChainsyncResync(t.Context())
	bad := badConn.Id()
	honest := honestConn.Id()

	bus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: bad,
				Reason: event.
					ChainsyncResyncReasonDeferredHeaderValidationFailure,
			},
		),
	)

	require.Eventually(
		t,
		func() bool { return peerGov.IsDenied(bad.RemoteAddr.String()) },
		2*time.Second,
		20*time.Millisecond,
	)
	require.Eventually(
		t,
		func() bool { return connManager.GetConnectionById(bad) == nil },
		2*time.Second,
		20*time.Millisecond,
	)
	require.NotNil(t, connManager.GetConnectionById(honest))
	require.Never(
		t,
		func() bool { return peerGov.IsDenied(honest.RemoteAddr.String()) },
		200*time.Millisecond,
		20*time.Millisecond,
	)
}

// A header from a peer whose advertised tip is a Genesis window older than the
// wall clock is observed for chain selection but never published to the ledger,
// so an abandoned fork cannot leave headers in the shared queue. A peer at the
// wall clock is published as before.
func TestChainsyncClientRollForwardWithholdsWallClockStalePeer(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()
	_, ledgerCh := bus.Subscribe(ledger.ChainsyncEventType)

	const wall = uint64(50000)
	cs := chainselection.NewChainSelector(chainselection.ChainSelectorConfig{
		GenesisMode:        true,
		SecurityParam:      20,
		GenesisWindowSlots: 100,
		WallClockSlot: func() (uint64, bool) {
			return wall, true
		},
		DisableEventSubscriptions: true,
	})
	state := dchainsync.NewState(bus, nil)
	stale := newTestConnId("127.0.0.1:6000", "10.0.0.1:3001")
	fresh := newTestConnId("127.0.0.1:6000", "10.0.0.2:3001")
	require.True(t, state.AddClientConnId(stale))
	require.True(t, state.AddClientConnId(fresh))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		// Models node.chainsyncObservePeerTip, which observes synchronously
		// throughout Genesis selection (the node-level tests pin that,
		// including with corroboration off).
		ChainsyncObservePeerTip: cs.ObservePeerTipUpdate,
		ChainsyncApplyEligible:  cs.ShouldApplyIngress,
	})
	o.chainsyncState = state
	o.eventBus = bus

	staleHeader := newTestBlockHeader(100, 1, 0xaa)
	staleTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(200, []byte("stale-tip")),
		BlockNumber: 2,
	}
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: stale},
		0,
		staleHeader,
		staleTip,
	))

	freshHeader := newTestBlockHeader(101, 1, 0xbb)
	freshTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(wall, []byte("fresh-tip")),
		BlockNumber: 3,
	}
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: fresh},
		0,
		freshHeader,
		freshTip,
	))

	// The stale peer's header was processed first, so the first event to
	// arrive must be the fresh peer's; a stale event would arrive before it.
	evt := testutil.RequireReceive(
		t, ledgerCh, time.Second,
		"the fresh peer's header must be published to the ledger",
	)
	data, ok := evt.Data.(ledger.ChainsyncEvent)
	require.True(t, ok)
	require.Equal(t, fresh, data.ConnectionId,
		"a wall-clock-stale peer's header was published to the ledger")
	testutil.RequireNoReceive(
		t, ledgerCh, 200*time.Millisecond,
		"no further header should be published",
	)
}
