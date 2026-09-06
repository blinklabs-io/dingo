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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testChainsyncConnId(localPort, remotePort int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: localPort,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: remotePort,
		},
	}
}

type mockHeader struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	blockNumber uint64
	slot        uint64
}

type sizedMockHeader struct {
	mockHeader
	cbor []byte
}

func (m sizedMockHeader) Cbor() []byte { return m.cbor }

func (m mockHeader) Hash() lcommon.Blake2b256     { return m.hash }
func (m mockHeader) PrevHash() lcommon.Blake2b256 { return m.prevHash }
func (m mockHeader) BlockNumber() uint64          { return m.blockNumber }
func (m mockHeader) SlotNumber() uint64           { return m.slot }

func (m mockHeader) IssuerVkey() lcommon.IssuerVkey { return lcommon.IssuerVkey{} }
func (m mockHeader) BlockBodySize() uint64          { return 0 }

func (m mockHeader) Era() lcommon.Era { return babbage.EraBabbage }
func (m mockHeader) Cbor() []byte     { return nil }

func (m mockHeader) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

func TestDetectConnectionSwitchHandsOffQueuedHeadersToNewActiveConnection(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, testChain.HeaderCount())

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	currentConn := connId2
	switchCalls := 0
	requestCount := 0

	ls := &LedgerState{
		chain:                        testChain,
		lastActiveConnId:             &connId1,
		activeBlockfetchConnId:       connId1,
		headerPipelineConnId:         connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		pendingBlockfetchEvents: []BlockfetchEvent{
			{
				ConnectionId: connId1,
				Block:        &mockBabbageBlock{slot: 2},
				Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &currentConn
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return nil
			},
			ConnectionSwitchFunc: func() {
				switchCalls++
			},
		},
	}

	activeConnId, configured := ls.detectConnectionSwitch(nil)
	require.True(t, configured)
	require.NotNil(t, activeConnId)
	assert.Equal(t, connId2, *activeConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
	// In-flight blockfetch is preserved across the switch so the current batch
	// can complete. selectedBlockfetchConnId is updated so the NEXT batch uses
	// the new connection. requestCount stays 0 — no immediate restart.
	assert.Equal(t, 0, requestCount)
	assert.Equal(t, connId1, ls.activeBlockfetchConnId)
	assert.Equal(t, connId2, ls.selectedBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.headerPipelineConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	assert.Equal(t, 1, len(ls.pendingBlockfetchEvents))
	assert.Equal(t, 1, switchCalls)

	ls.blockfetchRequestRangeCleanup()
}

func TestDetectConnectionSwitchRechecksLivenessBeforeReactivatingFrontier(
	t *testing.T,
) {
	previousConnId := testChainsyncConnId(6000, 3021)
	activeConnId := testChainsyncConnId(6000, 3022)
	callbackCalls := 0
	ls := &LedgerState{
		lastActiveConnId: &previousConnId,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				callbackCalls++
				if callbackCalls == 1 {
					return &activeConnId
				}
				return nil
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)

	var pending pendingPublishes
	got, configured := ls.detectConnectionSwitch(&pending)

	assert.True(t, configured)
	assert.Nil(t, got)
	assert.Zero(t, ls.UpstreamTipSlot())
	_, active := ls.UpstreamSyncStatus()
	assert.False(t, active)
}

func TestHandleConnectionClosedEventRetainsAdmittedUpstreamFrontier(
	t *testing.T,
) {
	closedConnId := testChainsyncConnId(6000, 3001)
	equivalentClosedConnId := testChainsyncConnId(6000, 3001)
	otherConnId := testChainsyncConnId(6000, 3002)

	tests := []struct {
		name        string
		activeConn  *ouroboros.ConnectionId
		wantVisible uint64
	}{
		{
			name:        "active connection closed",
			activeConn:  &equivalentClosedConnId,
			wantVisible: 0,
		},
		{
			name:        "no active connection",
			activeConn:  nil,
			wantVisible: 0,
		},
		{
			name:        "different active connection awaits admitted target",
			activeConn:  &otherConnId,
			wantVisible: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				config: LedgerStateConfig{
					GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
						return tc.activeConn
					},
				},
			}
			ls.syncUpstreamTipSlot.Store(114220800)
			var pending pendingPublishes
			ls.detectConnectionSwitch(&pending)

			ls.handleConnectionClosedEvent(event.NewEvent(
				ConnectionClosedEventType,
				ConnectionClosedEvent{
					ConnectionId: closedConnId,
				},
			))

			assert.Equal(t, uint64(114220800), ls.syncUpstreamTipSlot.Load())
			assert.Equal(t, tc.wantVisible, ls.UpstreamTipSlot())
		})
	}
}

func TestUpstreamTipSlotPreservesForgingGateAcrossStalePeerReconnect(
	t *testing.T,
) {
	closedConnId := testChainsyncConnId(6000, 3001)
	reconnectedConnId := testChainsyncConnId(6000, 3002)
	activeConnId := &closedConnId
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return activeConnId
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return !sameConnectionId(connId, closedConnId)
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)
	var pending pendingPublishes
	ls.detectConnectionSwitch(&pending)

	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: closedConnId},
	))
	activeConnId = nil
	require.Equal(t, uint64(0), ls.UpstreamTipSlot())

	activeConnId = &reconnectedConnId
	ls.lastActiveConnId = nil
	ls.detectConnectionSwitch(&pending)
	const stalePeerSlot uint64 = 114220700
	if stalePeerSlot > ls.syncUpstreamTipSlot.Load() {
		ls.syncUpstreamTipSlot.Store(stalePeerSlot)
	}
	assert.Equal(t, uint64(114220800), ls.syncUpstreamTipSlot.Load())
	assert.Zero(t, ls.UpstreamTipSlot())
	target, active := ls.UpstreamSyncStatus()
	assert.True(t, active)
	assert.Zero(t, target)
}

func TestAdvanceUpstreamTipSlotDoesNotPublishWithoutAdmittedTarget(
	t *testing.T,
) {
	activeConnID := testChainsyncConnId(6000, 3041)
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConnID
			},
		},
	}
	const admittedSlot uint64 = 114220801
	ls.advanceUpstreamTipSlot(admittedSlot)

	assert.Equal(t, admittedSlot, ls.syncUpstreamTipSlot.Load())
	assert.Zero(t, ls.UpstreamTipSlot())
	_, active := ls.UpstreamSyncStatus()
	assert.True(t, active)
}

func TestHandleChainSwitchAfterCloseRejectsDeadTargetKeepsFrontierHidden(
	t *testing.T,
) {
	closedConnId := testChainsyncConnId(6000, 3011)
	activeConnId := &closedConnId
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return activeConnId
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return !sameConnectionId(connId, closedConnId)
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)
	var pending pendingPublishes
	ls.detectConnectionSwitch(&pending)

	// Model the EventBus ordering: the close is applied before its already
	// queued chain-switch event, and the connection manager has no live peer.
	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: closedConnId},
	))
	activeConnId = nil
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{NewConnectionId: closedConnId},
	))

	assert.Zero(t, ls.UpstreamTipSlot())
	// A zero upstream frontier is the production forger's peerless state; a
	// dead queued switch must not re-enable the retained sync gate.
	_, active := ls.UpstreamSyncStatus()
	assert.False(t, active)
}

func TestHandleChainSwitchRetainsLiveTargetAcrossSubscriberOrdering(
	t *testing.T,
) {
	targetConnId := testChainsyncConnId(6000, 3031)
	activeConnId := testChainsyncConnId(6000, 3032)
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConnId
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return sameConnectionId(connId, targetConnId) ||
					sameConnectionId(connId, activeConnId)
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)
	ls.publishActiveUpstream(activeConnId)

	// A close subscriber can update the active pointer before the queued
	// chain-switch subscriber runs. The new connection has no admitted event
	// yet, so it must not inherit the prior connection's frontier.
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{NewConnectionId: targetConnId},
	))

	assert.Equal(t, targetConnId, ls.selectedBlockfetchConnId)
	assert.Zero(t, ls.UpstreamTipSlot())
}

func TestHandoffPipelineOnSwitchDropsStaleQueuedHeadersForNewBufferedPeer(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, testChain.HeaderCount())

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain:                testChain,
		headerPipelineConnId: connId1,
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connId2.String(): {
				{
					ConnectionId: connId2,
					Point:        ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
					Tip: ochainsync.Tip{
						Point: ocommon.Point{
							Slot: 2,
							Hash: []byte("hdr-2"),
						},
						BlockNumber: 2,
					},
					BlockHeader: mockHeader{
						hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
						prevHash:    lcommon.NewBlake2b256([]byte("hdr-1")),
						blockNumber: 2,
						slot:        2,
					},
				},
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	replayConnId, err := ls.handoffPipelineOnSwitchLocked(connId2, nil)
	require.NoError(t, err)
	assert.Equal(t, connId2, replayConnId)
	assert.Equal(t, 0, testChain.HeaderCount())
	assert.Equal(t, ouroboros.ConnectionId{}, ls.headerPipelineConnId)
	assert.Equal(t, connId2, ls.selectedBlockfetchConnId)
}

func TestHandleEventBlockfetchBlockAllowsBlocksFromActiveBatch(t *testing.T) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls := &LedgerState{
		activeBlockfetchConnId:       connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				return nil
			},
		},
	}

	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId1,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.Equal(t, connId1, ls.pendingBlockfetchEvents[0].ConnectionId)
}

func TestHandleEventChainsyncIgnoresClosedConnection(t *testing.T) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	testChain := &chain.Chain{}
	ls := &LedgerState{
		chain: testChain,
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connId.String(): {
				{ConnectionId: connId},
			},
		},
		peerHeaderHistory: map[string]*peerHeaderChain{
			connId.String(): {
				order: []string{"hdr-2"},
				byHash: map[string]peerHeaderRecord{
					"hdr-2": {event: ChainsyncEvent{ConnectionId: connId}},
				},
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			ConnectionLiveFunc: func(candidate ouroboros.ConnectionId) bool {
				return candidate != connId
			},
		},
	}

	ls.handleEventChainsync(event.NewEvent(
		ChainsyncEventType,
		ChainsyncEvent{
			ConnectionId: connId,
			Point:        ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
			Tip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
				BlockNumber: 2,
			},
			BlockHeader: mockHeader{
				hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 2,
				slot:        2,
			},
		},
	))

	assert.Equal(t, 0, testChain.HeaderCount())
	assert.Empty(t, ls.bufferedHeaderEvents[connId.String()])
	assert.Empty(t, ls.peerHeaderHistory[connId.String()])
}

func TestHandleEventBlockfetchBlockAllowsEquivalentConnectionId(t *testing.T) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId1Dup := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	require.True(t, sameConnectionId(connId1, connId1Dup))

	ls := &LedgerState{
		activeBlockfetchConnId:       connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				return nil
			},
		},
	}

	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId1Dup,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(
		t,
		sameConnectionId(connId1, ls.pendingBlockfetchEvents[0].ConnectionId),
	)
}

func TestHandleEventBlockfetchBlockDropsBlocksFromStaleConnection(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		activeBlockfetchConnId:       connId2,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				return nil
			},
		},
	}

	err := ls.handleEventBlockfetchBlockDeferred(BlockfetchEvent{
		ConnectionId: connId1,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	}, nil)
	require.NoError(t, err)
	require.Empty(t, ls.pendingBlockfetchEvents)
}

func TestHandleEventBlockfetchBatchDoneUsesSelectedConnectionAfterSwitch(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	requestedConnId := ouroboros.ConnectionId{}
	requestCount := 0

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId1,
		batchBlocksReceived:          1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				requestCount++
				requestedConnId = connId
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
		},
	))

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, connId2, requestedConnId)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventBlockfetchBatchDoneFallsBackToCurrentConnection(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestedConnId := ouroboros.ConnectionId{}
	requestCount := 0

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		batchBlocksReceived:          1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				requestCount++
				requestedConnId = connId
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, connId, requestedConnId)
	assert.Equal(t, connId, ls.activeBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

func TestHandleChainSwitchEventUpdatesSelectedBlockfetchConnId(t *testing.T) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls := &LedgerState{}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			NewConnectionId: connId,
		},
	))

	nextConnId, ok := ls.nextBlockfetchConnId()
	require.True(t, ok)
	assert.Equal(t, connId, nextConnId)
}

func TestHandleChainSwitchEventRequestsFreshCursorWhenPeerAheadWithoutHeaders(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
			NewTip: ochainsync.Tip{
				Point:       ocommon.NewPoint(200, []byte("peer-tip")),
				BlockNumber: 10,
			},
		},
	))

	evt := testutil.RequireReceive(
		t,
		resyncCh,
		2*time.Second,
		"expected chain-switch cursor resync event",
	)
	resync, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId2, resync.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonChainSwitchCursorAhead,
		resync.Reason,
	)
}

func TestChainSwitchNeedsFreshCursorUsesObservedTip(
	t *testing.T,
) {
	chainManager, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := chainManager.PrimaryChain()
	require.NoError(t, testChain.AddLocalBlock(&mockBabbageBlock{slot: 100}))
	require.Zero(t, testChain.HeaderCount())
	localTip := testChain.Tip()

	connId1 := testChainsyncConnId(6000, 3001)
	connId2 := testChainsyncConnId(6000, 3002)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	needsFreshCursor := ls.chainSwitchNeedsFreshCursorLocked(
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
			NewTip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					math.MaxUint64,
					[]byte("advertised-outlier"),
				),
				BlockNumber: math.MaxUint64,
			},
			NewObservedTip:    localTip,
			NewObservedTipSet: true,
		},
		connId2,
	)
	assert.False(
		t,
		needsFreshCursor,
		"an untrusted advertisement must not force a resync when the delivered frontier is at the local tip",
	)
}

func TestChainSwitchNeedsFreshCursorIgnoresFailedTargetFrontier(
	t *testing.T,
) {
	chainManager, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := chainManager.PrimaryChain()
	require.NoError(t, testChain.AddLocalBlock(&mockBabbageBlock{slot: 100}))
	require.Zero(t, testChain.HeaderCount())
	localTip := testChain.Tip()

	previousConnId := testChainsyncConnId(6000, 3001)
	failedTargetConnId := testChainsyncConnId(6000, 3002)
	fallbackConnId := testChainsyncConnId(6000, 3003)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetPeerObservedTipFunc: func(
				connId ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				if sameConnectionId(connId, fallbackConnId) {
					return localTip, true
				}
				return ochainsync.Tip{}, false
			},
		},
	}

	needsFreshCursor := ls.chainSwitchNeedsFreshCursorLocked(
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: previousConnId,
			NewConnectionId:      failedTargetConnId,
			NewObservedTip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					localTip.Point.Slot+100,
					[]byte("failed-target"),
				),
				BlockNumber: localTip.BlockNumber + 100,
			},
		},
		fallbackConnId,
	)
	assert.False(
		t,
		needsFreshCursor,
		"a failed target's observed frontier must not drive recovery for the fallback connection",
	)
}

type chainSwitchFallbackFixture struct {
	ls             *LedgerState
	resyncCh       <-chan event.Event
	previousConnId ouroboros.ConnectionId
	targetConnId   ouroboros.ConnectionId
	activeConnId   ouroboros.ConnectionId
}

func newChainSwitchFallbackFixture(
	t *testing.T,
) chainSwitchFallbackFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)

	connId1 := testChainsyncConnId(6000, 3001)
	connId2 := testChainsyncConnId(6000, 3002)
	connId3 := testChainsyncConnId(6000, 3003)
	currentConn := connId3
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("stale-hdr")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &currentConn
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return sameConnectionId(connId, connId3)
			},
			GetPeerObservedTipFunc: func(
				connId ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				if sameConnectionId(connId, connId3) {
					return ochainsync.Tip{
						Point: ocommon.NewPoint(
							200,
							[]byte("active-tip"),
						),
						BlockNumber: 10,
					}, true
				}
				return ochainsync.Tip{}, false
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = start
				_ = end
				if sameConnectionId(connId, connId2) {
					testChain.ClearHeaders()
					return errors.New("connection closed")
				}
				return nil
			},
		},
	}

	return chainSwitchFallbackFixture{
		ls:             ls,
		resyncCh:       resyncCh,
		previousConnId: connId1,
		targetConnId:   connId2,
		activeConnId:   connId3,
	}
}

func (f chainSwitchFallbackFixture) handleChainSwitchEvent() {
	f.ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: f.previousConnId,
			NewConnectionId:      f.targetConnId,
			NewTip: ochainsync.Tip{
				Point:       ocommon.NewPoint(200, []byte("peer-tip")),
				BlockNumber: 10,
			},
		},
	))
}

func TestHandleChainSwitchEventFallbackResyncUsesActiveConnection(
	t *testing.T,
) {
	fixture := newChainSwitchFallbackFixture(t)
	fixture.handleChainSwitchEvent()

	evt := testutil.RequireReceive(
		t,
		fixture.resyncCh,
		2*time.Second,
		"expected fallback chain-switch cursor resync event",
	)
	resync, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, fixture.activeConnId, resync.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonChainSwitchCursorAhead,
		resync.Reason,
	)
	assert.Equal(t, fixture.activeConnId, fixture.ls.selectedBlockfetchConnId)
}

func TestHandleChainSwitchEventFallbackReplaysBufferedActiveHeaders(
	t *testing.T,
) {
	bufferedHeaderHash := lcommon.NewBlake2b256([]byte("active-hdr"))
	fixture := newChainSwitchFallbackFixture(t)

	fixture.ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
		connIdKey(fixture.activeConnId): {{
			ConnectionId: fixture.activeConnId,
			BlockHeader: mockHeader{
				hash:        bufferedHeaderHash,
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 1,
				slot:        1,
			},
			Point: ocommon.NewPoint(1, bufferedHeaderHash.Bytes()),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(10, []byte("tip")),
				BlockNumber: 10,
			},
		}},
	}

	fixture.handleChainSwitchEvent()

	testutil.WaitForCondition(
		t,
		func() bool {
			fixture.ls.chainsyncMutex.Lock()
			defer fixture.ls.chainsyncMutex.Unlock()
			return sameConnectionId(
				fixture.ls.headerPipelineConnId,
				fixture.activeConnId,
			) &&
				fixture.ls.chain.HeaderCount() == 1 &&
				len(
					fixture.ls.bufferedHeaderEvents[connIdKey(fixture.activeConnId)],
				) == 0
		},
		2*time.Second,
		"expected buffered active headers to replay after fallback handoff",
	)
	testutil.RequireNoReceive(
		t,
		fixture.resyncCh,
		200*time.Millisecond,
		"active buffered headers should not trigger fresh cursor resync",
	)
}

func TestHandleChainSwitchEventDoesNotResyncInitialPeerSelection(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			NewConnectionId: connId,
			NewTip: ochainsync.Tip{
				Point:       ocommon.NewPoint(200, []byte("peer-tip")),
				BlockNumber: 10,
			},
		},
	))

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"initial peer selection should not trigger resync",
	)
}

func TestShouldBufferHeaderEventDoesNotPreserveIdleSelectedConnection(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain:                    &chain.Chain{},
		selectedBlockfetchConnId: connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId1
			},
		},
	}

	buffered := ls.shouldBufferHeaderEvent(ChainsyncEvent{
		ConnectionId: connId2,
		Point:        ocommon.NewPoint(1, []byte("hdr-1")),
	})
	require.False(t, buffered)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId2))
	assert.Equal(t, ouroboros.ConnectionId{}, ls.selectedBlockfetchConnId)
}

// TestShouldBufferHeaderEventDoesNotRaceDiscardBufferedPeerHeaders guards a
// real data race: shouldBufferHeaderEvent used to determine the current
// header pipeline owner (currentHeaderPipelineOwner, under
// chainsyncBlockfetchMutex) and then write headerPipelineConnId in a
// SEPARATE, unprotected step afterward, while discardBufferedPeerHeaders
// (and every other mutator) correctly holds chainsyncBlockfetchMutex
// around its own read/write of that same field -- so header admission and
// a concurrent batch-completion/clear could genuinely race on
// headerPipelineConnId. This runs both concurrently under go test -race,
// which fails the test outright if that race still exists; there is
// nothing else to assert; a clean run (no race detected) is the pass
// condition.
func TestShouldBufferHeaderEventDoesNotRaceDiscardBufferedPeerHeaders(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 200 {
			ls.shouldBufferHeaderEvent(ChainsyncEvent{
				ConnectionId: connId1,
				Point:        ocommon.NewPoint(uint64(i), []byte("hdr")),
			})
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			ls.discardBufferedPeerHeaders(connId2)
		}
	}()
	wg.Wait()
}

// TestDiscardBufferedPeerHeadersDoesNotRaceBufferedHeaderIteration guards the
// bufferedHeaderEvents map itself, which is a different field from the
// headerPipelineConnId race above.
//
// handleEventBlockfetch holds chainsyncBlockfetchMutex for its whole batch-done
// path, and nextBufferedHeaderConnId ranges over bufferedHeaderEvents inside
// it. discardBufferedPeerHeaders runs on handleEventChainsync's dispatch
// goroutine, which holds only chainsyncMutex, and used to delete from that same
// map before taking chainsyncBlockfetchMutex -- a concurrent map iteration and
// map write, which is fatal at runtime rather than merely racy. It was observed
// killing a mainnet block producer inside nextBufferedHeaderConnId.
//
// This runs both paths concurrently under go test -race; a clean run is the
// pass condition, so there is nothing else to assert.
func TestDiscardBufferedPeerHeadersDoesNotRaceBufferedHeaderIteration(
	t *testing.T,
) {
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Several connections so the range in nextBufferedHeaderConnId has real
	// work to do and overlaps the concurrent deletes.
	const conns = 50
	connIds := make([]ouroboros.ConnectionId, conns)
	for i := range connIds {
		connIds[i] = ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 3001 + i,
			},
		}
		ls.bufferHeaderEvent(ChainsyncEvent{
			ConnectionId: connIds[i],
			Point:        ocommon.NewPoint(uint64(i), []byte("hdr")),
		})
	}

	var wg sync.WaitGroup
	wg.Add(4)
	// Mirrors handleEventBlockfetch: the read side holds
	// chainsyncBlockfetchMutex across the iteration.
	go func() {
		defer wg.Done()
		for range 200 {
			ls.chainsyncBlockfetchMutex.Lock()
			ls.nextBufferedHeaderConnId()
			ls.chainsyncBlockfetchMutex.Unlock()
		}
	}()
	// Mirrors handleEventChainsync's dispatch goroutine.
	go func() {
		defer wg.Done()
		for i := range 200 {
			ls.discardBufferedPeerHeaders(connIds[i%conns])
		}
	}()
	// The buffering write path, which bufferedHeaderMutex alone protects.
	// claimHeaderPipelineOwnership releases chainsyncBlockfetchMutex on
	// return, so this write never holds that lock -- it is the case the
	// previous, narrower fix missed, and it fails here without the
	// dedicated mutex.
	go func() {
		defer wg.Done()
		for i := range 200 {
			ls.bufferHeaderEvent(ChainsyncEvent{
				ConnectionId: connIds[i%conns],
				Point:        ocommon.NewPoint(uint64(i), []byte("hdr")),
			})
		}
	}()
	// The resync delete path, which reaches the map from callers that do
	// not all hold chainsyncBlockfetchMutex.
	go func() {
		defer wg.Done()
		var pending pendingPublishes
		for i := range 200 {
			ls.requestChainsyncResync(
				connIds[i%conns],
				"race probe",
				&pending,
			)
		}
	}()
	wg.Wait()
}

func TestHandleChainSwitchEventReplaysBufferedHeadersForSelectedConnection(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	headerHash := lcommon.NewBlake2b256([]byte("hdr-1"))
	ls := &LedgerState{
		chain:                &chain.Chain{},
		headerPipelineConnId: connId1,
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connIdKey(connId2): {{
				ConnectionId: connId2,
				BlockHeader: mockHeader{
					hash:        headerHash,
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        1,
				},
				Point: ocommon.NewPoint(1, headerHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(10, []byte("tip")),
					BlockNumber: 10,
				},
			}},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				return nil
			},
		},
	}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
		},
	))

	require.Eventually(t, func() bool {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		return sameConnectionId(ls.headerPipelineConnId, connId2) &&
			ls.chain.HeaderCount() == 1 &&
			len(ls.bufferedHeaderEvents[connIdKey(connId2)]) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestHandleEventChainsyncBlockHeaderAcceptsCompatibleNonOwnerConnection(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    header1Hash,
		blockNumber: 2,
		slot:        2,
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader:  header1,
		Point:        ocommon.NewPoint(header1.slot, header1.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, connId1, ls.headerPipelineConnId)
	assert.Equal(t, 1, ls.chain.HeaderCount())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		BlockHeader:  header2,
		Point:        ocommon.NewPoint(header2.slot, header2.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60002, []byte("tip-2")),
			BlockNumber: 60002,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, connId2, ls.headerPipelineConnId)
	assert.Equal(t, connId2, ls.selectedBlockfetchConnId)
	assert.Equal(t, 2, ls.chain.HeaderCount())
	require.Empty(t, ls.bufferedHeaderEvents[connIdKey(connId2)])
}

func TestHandleEventChainsyncRecordsOnlyAdmittedHeaderFrontier(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	// Keep the test at header admission; no blockfetch worker is needed.
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	connID := fixture.connId
	ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &connID
	}
	ls.publishActiveUpstream(connID)
	assert.Zero(
		t,
		ls.UpstreamTipSlot(),
		"selection alone must not publish a target",
	)

	// This header is accepted and establishes the initial upstream tip.
	accepted := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("accepted-header-2")),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	advertisedSlot := ^uint64(0)
	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connID,
		BlockHeader:  accepted,
		Point:        ocommon.NewPoint(accepted.slot, accepted.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advertisedSlot,
				[]byte("unbound-advertised-tip"),
			),
			BlockNumber: advertisedSlot,
		},
		SyncTarget: ochainsync.Tip{
			Point: ocommon.NewPoint(accepted.slot, []byte("accepted-target")),
		},
		SyncTargetTrusted: true,
	}))
	require.Equal(t, accepted.slot, ls.syncUpstreamTipSlot.Load())
	assert.Equal(t, accepted.slot, ls.UpstreamTipSlot())

	// The next header does not extend the queued chain. Its advertised tip
	// must not advance shared progress state before fork handling rejects it.
	rejected := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("rejected-header")),
		prevHash:    lcommon.NewBlake2b256([]byte("unknown-parent")),
		blockNumber: 3,
		slot:        3,
	}
	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connID,
		BlockHeader:  rejected,
		Point:        ocommon.NewPoint(rejected.slot, rejected.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advertisedSlot-1,
				[]byte("rejected-tip"),
			),
			BlockNumber: advertisedSlot - 1,
		},
		SyncTarget: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advertisedSlot-1,
				[]byte("rejected-target"),
			),
		},
	}))
	assert.Equal(t, accepted.slot, ls.syncUpstreamTipSlot.Load())
	assert.Equal(t, accepted.slot, ls.UpstreamTipSlot(),
		"a rejected header must not publish its advertised target")
}

func TestHandleEventChainsyncBlockHeaderBuffersIncompatibleNonOwnerConnection(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    lcommon.NewBlake2b256([]byte("other-parent")),
		blockNumber: 2,
		slot:        2,
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader:  header1,
		Point:        ocommon.NewPoint(header1.slot, header1.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	require.Equal(t, header1.slot, ls.syncUpstreamTipSlot.Load())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		BlockHeader:  header2,
		Point:        ocommon.NewPoint(header2.slot, header2.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(^uint64(0), []byte("unbound-tip-2")),
			BlockNumber: ^uint64(0),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, connId1, ls.headerPipelineConnId)
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Equal(t, header1.slot, ls.syncUpstreamTipSlot.Load())
	events := ls.bufferedHeaderEvents[connIdKey(connId2)]
	require.Len(t, events, 1)
	assert.Equal(
		t,
		header2.slot,
		events[0].Point.Slot,
	)
}

func TestHandleEventChainsyncBlockHeader_ProcessesEligibleNonActivePeer(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	testChain := &chain.Chain{}
	var requestedConn ouroboros.ConnectionId
	ls := &LedgerState{
		chain:            testChain,
		lastActiveConnId: &connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = start
				_ = end
				requestedConn = connId
				return nil
			},
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		Point:        ocommon.NewPoint(1, hash1.Bytes()),
		BlockHeader: mockHeader{
			hash:        hash1,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1, hash1.Bytes()),
			BlockNumber: 1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, connId2, requestedConn)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
}

func TestHandleEventChainsyncBlockHeaderBuffersMinimumBatchWhenBehind(
	t *testing.T,
) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	requestCount := 0

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return nil
			},
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(1, hash1.Bytes()),
		BlockHeader: mockHeader{
			hash:        hash1,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("tip-200")),
			BlockNumber: 200,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Equal(t, 0, requestCount)
	assert.Equal(t, connId, ls.headerPipelineConnId)
}

func TestHandleEventChainsyncBlockHeaderScalesBatchWhenFarBehind(t *testing.T) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return nil
			},
		},
	}

	// gapBlocks > 1000 puts the runway in the deepest catchup bucket
	// (minHeaders = 256), so we send enough headers to cross that
	// threshold and trigger exactly one batch. Headers added past the
	// trigger queue against the in-flight batch (the test's
	// BlockfetchRequestRangeFunc mock never completes), so requestCount
	// stays at 1 — that's the "scales up but doesn't re-fire while
	// in-flight" guarantee this test pins.
	const totalHeaders = 260
	prevHash := lcommon.NewBlake2b256(nil)
	for i := 1; i <= totalHeaders; i++ {
		headerHash := lcommon.NewBlake2b256(fmt.Appendf(nil, "hdr-%d", i))
		err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: connId,
			Point:        ocommon.NewPoint(uint64(i), headerHash.Bytes()),
			BlockHeader: mockHeader{
				hash:        headerHash,
				prevHash:    prevHash,
				blockNumber: uint64(i),
				slot:        uint64(i),
			},
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(1280, []byte("tip-1280")),
				BlockNumber: 1280,
			},
		})
		require.NoError(t, err)
		prevHash = headerHash
		if i == 7 {
			assert.Equal(t, 0, requestCount,
				"no batch before runway accumulates")
		}
	}

	assert.Equal(t, 1, requestCount)
	assert.Equal(t, totalHeaders, ls.chain.HeaderCount())
}

func TestHandleEventChainsyncBlockHeaderAcceptsEquivalentOwnerConnectionId(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId1Dup := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	require.True(t, sameConnectionId(connId1, connId1Dup))

	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    header1Hash,
		blockNumber: 2,
		slot:        2,
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader:  header1,
		Point:        ocommon.NewPoint(header1.slot, header1.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1Dup,
		BlockHeader:  header2,
		Point:        ocommon.NewPoint(header2.slot, header2.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60002, []byte("tip-2")),
			BlockNumber: 60002,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1Dup))
	assert.Equal(t, 2, ls.chain.HeaderCount())
	assert.Empty(t, ls.bufferedHeaderEvents)
}

func TestHandleEventBlockfetchBatchDoneReplaysBufferedHeadersAfterDrain(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	headerHash := lcommon.NewBlake2b256([]byte("hdr-2"))
	ls := &LedgerState{
		chain:                        &chain.Chain{},
		activeBlockfetchConnId:       connId1,
		selectedBlockfetchConnId:     connId2,
		headerPipelineConnId:         connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connIdKey(connId2): {{
				ConnectionId: connId2,
				BlockHeader: mockHeader{
					hash:        headerHash,
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        1,
				},
				Point: ocommon.NewPoint(1, headerHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(60001, []byte("tip-2")),
					BlockNumber: 60001,
				},
			}},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		return sameConnectionId(ls.headerPipelineConnId, connId2) &&
			len(ls.bufferedHeaderEvents[connIdKey(connId2)]) == 0 &&
			ls.chain.HeaderCount() == 1 &&
			ls.syncUpstreamTipSlot.Load() == 1
	}, 2*time.Second, 10*time.Millisecond)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId2))
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Equal(t, uint64(1), ls.syncUpstreamTipSlot.Load())
}

func TestHandleEventChainsyncBlockHeaderKeepsActiveBatchOwner(t *testing.T) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		chain:                        &chain.Chain{},
		activeBlockfetchConnId:       connId1,
		selectedBlockfetchConnId:     connId2,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		BlockHeader: mockHeader{
			hash:        header1Hash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Point: ocommon.NewPoint(1, header1Hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1))
	assert.Equal(t, 0, ls.chain.HeaderCount())
	require.Len(t, ls.bufferedHeaderEvents[connIdKey(connId2)], 1)

	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader: mockHeader{
			hash:        header2Hash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 2,
			slot:        2,
		},
		Point: ocommon.NewPoint(2, header2Hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60002, []byte("tip-2")),
			BlockNumber: 60002,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1))
	assert.Equal(t, 1, ls.chain.HeaderCount())
}

func TestHandleEventChainsyncBlockHeaderIgnoresIdleSelectedOwner(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	headerHash := lcommon.NewBlake2b256([]byte("hdr-idle-selected"))
	ls := &LedgerState{
		chain:                    &chain.Chain{},
		selectedBlockfetchConnId: connId2,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader: mockHeader{
			hash:        headerHash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Point: ocommon.NewPoint(1, headerHash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1))
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Empty(t, ls.bufferedHeaderEvents)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.selectedBlockfetchConnId)
}

func TestHandleEventChainsyncBlockHeaderIgnoresStaleHeaderBehindChainTip(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.currentTip = fixture.ancestorTip
	staleHash := lcommon.NewBlake2b256([]byte("stale-header"))
	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		BlockHeader: mockHeader{
			hash:        staleHash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 2,
			slot:        fixture.ancestorTip.Point.Slot + 5,
		},
		Point: ocommon.NewPoint(
			fixture.ancestorTip.Point.Slot+5,
			staleHash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				fixture.currentTip.Point.Slot+10,
				[]byte("tip-30"),
			),
			BlockNumber: fixture.currentTip.BlockNumber + 1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fixture.ls.headerMismatchCount)
	assert.Equal(t, 0, fixture.ls.chain.HeaderCount())
	assert.Equal(
		t,
		fixture.currentTip.Point.Slot,
		fixture.ls.chain.Tip().Point.Slot,
	)
}

func TestHandleEventChainsyncBlockHeaderSkipsMithrilBoundaryHeaderVerification(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.validationEnabled = true
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot

	staleHash := lcommon.NewBlake2b256([]byte("mithril-stale-header"))
	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		BlockHeader: mockHeader{
			hash:        staleHash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 2,
			slot:        fixture.ancestorTip.Point.Slot + 5,
		},
		Point: ocommon.NewPoint(
			fixture.ancestorTip.Point.Slot+5,
			staleHash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				fixture.currentTip.Point.Slot+10,
				[]byte("tip-after-mithril"),
			),
			BlockNumber: fixture.currentTip.BlockNumber + 1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fixture.ls.headerMismatchCount)
	assert.Equal(t, 0, fixture.ls.chain.HeaderCount())
	assert.Equal(
		t,
		fixture.currentTip.Point.Slot,
		fixture.ls.chain.Tip().Point.Slot,
	)
}

func TestHandleEventChainsyncRollbackClearsBufferedHeadersForNonActivePeer(
	t *testing.T,
) {
	activeConn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	bufferedConn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	bufferedHash := lcommon.NewBlake2b256([]byte("buffered-header"))
	ls := &LedgerState{
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connIdKey(bufferedConn): {{
				ConnectionId: bufferedConn,
				BlockHeader: mockHeader{
					hash:        bufferedHash,
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        1,
				},
				Point: ocommon.NewPoint(1, bufferedHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(10, []byte("tip")),
					BlockNumber: 10,
				},
			}},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConn
			},
		},
	}

	err := ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: bufferedConn,
		Point:        ocommon.NewPoint(0, nil),
	}, nil)
	require.NoError(t, err)
	assert.Empty(t, ls.bufferedHeaderEvents[connIdKey(bufferedConn)])
}

func TestHandleEventChainsyncBlockHeaderStartsBlockfetchForSmallBlockGap(
	t *testing.T,
) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0
	ls := &LedgerState{
		chain: &chain.Chain{},
		currentTip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1000, []byte("local-tip")),
			BlockNumber: 100,
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				requestConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				requestCount++
				assert.Equal(t, connId, requestConnId)
				assert.Equal(t, uint64(1064), start.Slot)
				assert.Equal(t, uint64(1064), end.Slot)
				return nil
			},
		},
	}

	prevHash := lcommon.NewBlake2b256(nil)
	for i := range 5 {
		hash := lcommon.NewBlake2b256(fmt.Appendf(nil, "hdr-%d", i))
		slot := uint64(1064 + i*4)
		err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: connId,
			BlockHeader: mockHeader{
				hash:        hash,
				prevHash:    prevHash,
				blockNumber: 101 + uint64(i),
				slot:        slot,
			},
			Point: ocommon.NewPoint(slot, hash.Bytes()),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(1080, []byte("peer-tip")),
				BlockNumber: 105,
			},
		})
		require.NoError(t, err)
		prevHash = hash
	}

	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(ls.activeBlockfetchConnId, connId))
	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventChainsyncBlockHeaderStartsBlockfetchForSparseBlockGap(
	t *testing.T,
) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0
	ls := &LedgerState{
		chain: &chain.Chain{},
		currentTip: ochainsync.Tip{
			Point:       ocommon.NewPoint(107374005, []byte("local-tip")),
			BlockNumber: 4123854,
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				requestConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				requestCount++
				assert.Equal(t, connId, requestConnId)
				assert.Equal(t, uint64(107374026), start.Slot)
				assert.Equal(t, uint64(107374047), end.Slot)
				return nil
			},
		},
	}

	headerSlots := []uint64{
		107374026,
		107374033,
		107374047,
		107374530,
	}
	var prevHash lcommon.Blake2b256
	for i, slot := range headerSlots {
		hash := lcommon.NewBlake2b256(
			fmt.Appendf(nil, "sparse-hdr-%d", i),
		)
		err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: connId,
			BlockHeader: mockHeader{
				hash:        hash,
				prevHash:    prevHash,
				blockNumber: 4123855 + uint64(i),
				slot:        slot,
			},
			Point: ocommon.NewPoint(slot, hash.Bytes()),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(107374509, []byte("peer-tip")),
				BlockNumber: 4123873,
			},
		})
		require.NoError(t, err)
		prevHash = hash
	}

	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(ls.activeBlockfetchConnId, connId))
	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventChainsyncAwaitReplyStartsBlockfetchForActiveConnection(
	t *testing.T,
) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	testChain := &chain.Chain{}
	var prevHash lcommon.Blake2b256
	for i, slot := range []uint64{1001, 1002, 1003, 1004} {
		hash := lcommon.NewBlake2b256(
			fmt.Appendf(nil, "await-reply-hdr-%d", i),
		)
		err := testChain.AddBlockHeader(mockHeader{
			hash:        hash,
			prevHash:    prevHash,
			blockNumber: 200 + uint64(i),
			slot:        slot,
		})
		require.NoError(t, err)
		prevHash = hash
	}

	requestCount := 0
	activeConn := connId
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConn
			},
			BlockfetchRequestRangeFunc: func(
				requestConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				requestCount++
				assert.Equal(t, connId, requestConnId)
				assert.Equal(t, uint64(1001), start.Slot)
				assert.Equal(t, uint64(1004), end.Slot)
				return nil
			},
		},
	}

	ls.handleEventChainsyncAwaitReply(
		event.NewEvent(
			ChainsyncAwaitReplyEventType,
			ChainsyncAwaitReplyEvent{ConnectionId: connId},
		),
	)

	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(ls.activeBlockfetchConnId, connId))
	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventBlockfetchBatchDoneEmptyBatchRetriesAlternateConnection(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	requestedConnIds := make([]ouroboros.ConnectionId, 0, 1)

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId1,
		selectedBlockfetchConnId:     connId2,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = start
				_ = end
				requestedConnIds = append(requestedConnIds, connId)
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, []ouroboros.ConnectionId{connId2}, requestedConnIds)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)

	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventBlockfetchBatchDoneEmptyBatchNearTipRetries(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("near-tip-header")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        4,
	})
	require.NoError(t, err)

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		selectedBlockfetchConnId:     connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) error {
				requestCount++
				return nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, connId, ls.activeBlockfetchConnId)
	assert.NotNil(t, ls.chainsyncBlockfetchReadyChan)

	ls.blockfetchRequestRangeCleanup()
}

func TestHandleBlockfetchTimeoutLocked_RetriesQueuedRangeUsingActivePeer(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        hash1,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	var requestedConn ouroboros.ConnectionId
	ls := &LedgerState{
		chain:                  testChain,
		activeBlockfetchConnId: connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = start
				_ = end
				requestedConn = connId
				return nil
			},
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId1, nil)

	assert.Equal(t, connId2, requestedConn)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
}

// TestHandleBlockfetchTimeoutLocked_RetryRetargetsSelection asserts a timeout
// retry moves the blockfetch selection to the connection it retries on.
// nextBlockfetchConnId prefers selectedBlockfetchConnId, and the
// batch-completion continuation uses it to choose the next batch's connection,
// so a selection left on the timed-out peer recovers one batch from the working
// peer and sends the next straight back to the one that just timed out.
func TestHandleBlockfetchTimeoutLocked_RetryRetargetsSelection(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-retarget-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	var requestedConn ouroboros.ConnectionId
	ls := &LedgerState{
		chain:                  testChain,
		activeBlockfetchConnId: connId1,
		// Starts on the connection that is about to time out, so the
		// assertion below cannot pass without the retarget.
		selectedBlockfetchConnId: connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) error {
				requestedConn = connId
				return nil
			},
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId1, nil)

	require.Equal(t, connId2, requestedConn)
	assert.Equal(
		t, connId2, ls.selectedBlockfetchConnId,
		"the selection must follow the retry, so the next batch does not "+
			"return to the timed-out connection",
	)
}

func TestHandleBlockfetchTimeoutLocked_ClearsActiveConnectionWithoutHeaders(
	t *testing.T,
) {
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}

	ls := &LedgerState{
		chain:                        &chain.Chain{},
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId, nil)

	assert.Equal(t, ouroboros.ConnectionId{}, ls.activeBlockfetchConnId)
	assert.Nil(t, ls.chainsyncBlockfetchReadyChan)
	_, ok := ls.nextBlockfetchConnId()
	assert.False(t, ok)
}

func TestHandleBlockfetchTimeoutLocked_RetryFailureUsesAlternateSelectedPeer(
	t *testing.T,
) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	connId3 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3003},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        hash1,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	requestedConnIds := make([]ouroboros.ConnectionId, 0, 2)
	ls := &LedgerState{
		chain:                    testChain,
		activeBlockfetchConnId:   connId1,
		selectedBlockfetchConnId: connId3,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = start
				_ = end
				requestedConnIds = append(requestedConnIds, connId)
				if connId == connId2 {
					return errors.New("retry failed")
				}
				return nil
			},
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId1, nil)

	require.Equal(
		t,
		[]ouroboros.ConnectionId{connId2, connId3},
		requestedConnIds,
	)
	assert.Equal(t, connId3, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	assert.Equal(t, 1, testChain.HeaderCount())
}

// TestChainSwitchNewObservedTipKeysOnPresenceNotZeroValue covers the
// advertising-only peer this path exists to distrust.
//
// A zero delivered frontier is a real observation: the peer delivered nothing.
// Inferring "field absent" from it fell back to the advertised NewTip, which
// handed that peer's advertisement to ledger cursor recovery. The fallback now
// keys on NewObservedTipSet, which every producer in chainselection sets, so
// only a producer that never populated the field reaches the advertised tip.
func TestChainSwitchNewObservedTipKeysOnPresenceNotZeroValue(t *testing.T) {
	advertised := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 9_000, Hash: []byte{0xaa}},
		BlockNumber: 900,
	}
	delivered := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte{0xbb}},
		BlockNumber: 10,
	}

	t.Run("delivered frontier is used when set", func(t *testing.T) {
		got := chainSwitchNewObservedTip(chainselection.ChainSwitchEvent{
			NewTip:            advertised,
			NewObservedTip:    delivered,
			NewObservedTipSet: true,
		})
		assert.Equal(t, delivered, got)
	})

	t.Run(
		"zero delivered frontier is not the advertised tip",
		func(t *testing.T) {
			got := chainSwitchNewObservedTip(chainselection.ChainSwitchEvent{
				NewTip:            advertised,
				NewObservedTipSet: true,
			})
			assert.Equal(t, ochainsync.Tip{}, got)
			assert.NotEqual(t, advertised, got)
		},
	)

	t.Run("unset falls back to the advertised tip", func(t *testing.T) {
		// Older events and direct unit-test or integration constructors.
		got := chainSwitchNewObservedTip(chainselection.ChainSwitchEvent{
			NewTip: advertised,
		})
		assert.Equal(t, advertised, got)
	})
}
