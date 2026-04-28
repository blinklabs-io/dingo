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
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockHeader struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	blockNumber uint64
	slot        uint64
}

func (m mockHeader) Hash() lcommon.Blake2b256          { return m.hash }
func (m mockHeader) PrevHash() lcommon.Blake2b256      { return m.prevHash }
func (m mockHeader) BlockNumber() uint64               { return m.blockNumber }
func (m mockHeader) SlotNumber() uint64                { return m.slot }
func (m mockHeader) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (m mockHeader) BlockBodySize() uint64             { return 0 }
func (m mockHeader) Era() lcommon.Era                  { return babbage.EraBabbage }
func (m mockHeader) Cbor() []byte                      { return nil }
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
	requestedConnId := ouroboros.ConnectionId{}

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
				_ = start
				_ = end
				requestCount++
				requestedConnId = connId
				return nil
			},
			ConnectionSwitchFunc: func() {
				switchCalls++
			},
		},
	}

	activeConnId, configured := ls.detectConnectionSwitch()
	require.True(t, configured)
	require.NotNil(t, activeConnId)
	assert.Equal(t, connId2, *activeConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, connId2, requestedConnId)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.headerPipelineConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	require.Empty(t, ls.pendingBlockfetchEvents)
	assert.Equal(t, 1, switchCalls)

	ls.blockfetchRequestRangeCleanup()
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
						Point:       ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
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

	replayConnId, err := ls.handoffPipelineOnSwitchLocked(connId2)
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

	err := ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: connId1,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	})
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

	err := ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: connId1Dup,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	})
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(
		t,
		sameConnectionId(connId1, ls.pendingBlockfetchEvents[0].ConnectionId),
	)
}

func TestHandleEventBlockfetchBlockDropsBlocksFromStaleConnection(t *testing.T) {
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

	err := ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: connId1,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	})
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
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
		},
	))

	err = ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	})
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

	err = ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	})
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
	}, time.Second, 10*time.Millisecond)
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
	assert.Equal(t, connId1, ls.headerPipelineConnId)
	assert.Equal(t, 1, ls.chain.HeaderCount())
	require.Len(t, ls.bufferedHeaderEvents[connIdKey(connId2)], 1)
	assert.Equal(
		t,
		header2.slot,
		ls.bufferedHeaderEvents[connIdKey(connId2)][0].Point.Slot,
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

	prevHash := lcommon.NewBlake2b256(nil)
	for i := 1; i <= 20; i++ {
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
			assert.Equal(t, 0, requestCount)
		}
	}

	assert.Equal(t, 1, requestCount)
	assert.Equal(t, 20, ls.chain.HeaderCount())
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

	err := ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		return sameConnectionId(ls.headerPipelineConnId, connId2) &&
			len(ls.bufferedHeaderEvents[connIdKey(connId2)]) == 0 &&
			ls.chain.HeaderCount() == 1
	}, time.Second, 10*time.Millisecond)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId2))
	assert.Equal(t, 1, ls.chain.HeaderCount())
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
	})
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

	err = ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	})
	require.NoError(t, err)
	require.Equal(t, []ouroboros.ConnectionId{connId2}, requestedConnIds)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)

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

	ls.handleBlockfetchTimeoutLocked(connId1)

	assert.Equal(t, connId2, requestedConn)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
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

	ls.handleBlockfetchTimeoutLocked(connId)

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

	ls.handleBlockfetchTimeoutLocked(connId1)

	require.Equal(t, []ouroboros.ConnectionId{connId2, connId3}, requestedConnIds)
	assert.Equal(t, connId3, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	assert.Equal(t, 1, testChain.HeaderCount())
}
