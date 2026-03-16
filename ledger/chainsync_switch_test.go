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

	"github.com/blinklabs-io/dingo/chain"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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
		chain:            testChain,
		lastActiveConnId: &connId1,
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

	activeConnId, configured := ls.detectConnectionSwitch()
	require.True(t, configured)
	require.NotNil(t, activeConnId)
	assert.Equal(t, connId2, *activeConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, 1, switchCalls)
}

func TestHandleEventBlockfetchBlockAllowsBlocksFromPreviousConnection(t *testing.T) {
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		activeBlockfetchConnId: connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: connId2,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	})
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.Equal(t, connId2, ls.pendingBlockfetchEvents[0].ConnectionId)
}
