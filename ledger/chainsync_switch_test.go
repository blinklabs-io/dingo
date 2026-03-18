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

	activeConnId, configured, _ := ls.detectConnectionSwitch()
	require.True(t, configured)
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
	require.Len(t, ls.bufferedHeaderEvents[connId2], 1)
	assert.Equal(
		t,
		header2.slot,
		ls.bufferedHeaderEvents[connId2][0].Point.Slot,
	)
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
		bufferedHeaderEvents: map[ouroboros.ConnectionId][]ChainsyncEvent{
			connId2: {
				{
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
				},
			},
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
		return ls.headerPipelineConnId == connId2 &&
			len(ls.bufferedHeaderEvents[connId2]) == 0 &&
			ls.chain.HeaderCount() == 1
	}, time.Second, 10*time.Millisecond)
	assert.Equal(t, connId2, ls.headerPipelineConnId)
	assert.Equal(t, 1, ls.chain.HeaderCount())
}
