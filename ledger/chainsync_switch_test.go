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

func TestDetectConnectionSwitchPreservesQueuedHeaders(t *testing.T) {
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

	ls := &LedgerState{
		chain:                        testChain,
		lastActiveConnId:             &connId1,
		activeBlockfetchConnId:       connId1,
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
			ConnectionSwitchFunc: func() {
				switchCalls++
			},
		},
	}

	activeConnId, configured, switched := ls.detectConnectionSwitch()
	require.True(t, configured)
	require.True(t, switched)
	require.NotNil(t, activeConnId)
	assert.Equal(t, connId2, *activeConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, connId1, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.Equal(t, 1, switchCalls)
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

func TestHandleEventChainsyncBlockHeaderBuffersNonOwnerConnection(
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
		headerHash := lcommon.NewBlake2b256([]byte(fmt.Sprintf("hdr-%d", i)))
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
		if i == 8 {
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
