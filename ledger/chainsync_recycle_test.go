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

package ledger

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRecycleConnId() ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
}

// TestChainsyncHeaderVerificationFailurePublishesRecycleEvent verifies that
// a header crypto verification failure on the chainsync path publishes a
// ledger.ConnectionRecycleRequestedEvent with reason "header_verification_failure".
func TestChainsyncHeaderVerificationFailurePublishesRecycleEvent(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		validationEnabled: true,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 2_000,
				Nonce:         make([]byte, 32),
			},
		},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  mockHeader{slot: 1000, blockNumber: 100},
		Point:        ocommon.Point{Slot: 1000},
	})
	require.Error(t, err)

	got := testutil.RequireReceive(t, recycled, 2*time.Second, "recycle event not published")
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "header_verification_failure", got.Reason)
}

func TestChainsyncHeaderVerificationMissingEpochDefersToBlockfetch(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	ls := &LedgerState{
		validationEnabled: true,
		chain:             testChain,
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) error {
				return nil
			},
		},
	}
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point:        point,
	})
	require.NoError(t, err)

	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"missing epoch should defer header verification, not recycle peer",
	)
	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
}

func TestChainsyncHeaderVerificationEmptyEpochNonceDefersToBlockfetch(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	requested := make(chan ocommon.Point, 1)
	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	ls := &LedgerState{
		validationEnabled: true,
		chain:             testChain,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 2_000,
			},
		},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				start ocommon.Point,
				_ ocommon.Point,
			) error {
				requested <- start
				return nil
			},
		},
	}
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point:        point,
	})
	require.NoError(t, err)

	gotStart := testutil.RequireReceive(
		t,
		requested,
		2*time.Second,
		"empty nonce should start blockfetch for deferred verification",
	)
	assert.Equal(t, point, gotStart)
	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"empty nonce should defer header verification, not recycle peer",
	)
	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
}

// TestBlockfetchHeaderVerificationFailurePublishesRecycleEvent verifies that
// a block header crypto verification failure on the blockfetch path publishes a
// ledger.ConnectionRecycleRequestedEvent with reason "block_header_verification_failure".
func TestBlockfetchHeaderVerificationFailurePublishesRecycleEvent(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		validationEnabled:            true,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}

	ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: connId,
			Block:        &mockBabbageBlock{slot: 500},
			Point:        ocommon.Point{Slot: 500, Hash: []byte("fake-hash")},
		},
	))

	got := testutil.RequireReceive(t, recycled, 2*time.Second, "recycle event not published")
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "block_header_verification_failure", got.Reason)
}

func TestBlockfetchStatefulHeaderVerificationDefersUntilLedgerApply(
	t *testing.T,
) {
	connId := testRecycleConnId()
	tb := createTestBlock(t, [32]byte{47}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chain = &chain.Chain{}

	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())
	err := ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point:        point,
	})
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(t, ls.consumeDeferredHeaderValidation(point))
	value, err := ls.db.GetSyncState(
		deferredHeaderValidationSyncStateKey(point),
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, deferredHeaderValidationSyncStateValue, value)
}
