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
	"net"
	"testing"
	"time"

	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type testBlockHeader struct {
	hash        gledger.Blake2b256
	prevHash    gledger.Blake2b256
	blockNumber uint64
	slotNumber  uint64
	bodySize    uint64
	bodyHash    gledger.Blake2b256
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

func TestNormalizeIntersectPoints(t *testing.T) {
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

func TestShouldRestartChainsyncOnSwitch(t *testing.T) {
	tests := []struct {
		name          string
		localTip      ocommon.Point
		trackedClient *dchainsync.TrackedClient
		want          bool
	}{
		{
			name:     "nil tracked client",
			localTip: ocommon.NewPoint(100, []byte("tip")),
			want:     false,
		},
		{
			name:          "origin local tip",
			localTip:      ocommon.NewPointOrigin(),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(5, []byte("a"))},
			want:          false,
		},
		{
			name:          "tracked cursor behind local tip",
			localTip:      ocommon.NewPoint(100, []byte("tip")),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(90, []byte("a"))},
			want:          false,
		},
		{
			name:          "tracked cursor equal to local tip",
			localTip:      ocommon.NewPoint(100, []byte("tip")),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(100, []byte("tip"))},
			want:          false,
		},
		{
			name:          "tracked cursor ahead of local tip",
			localTip:      ocommon.NewPoint(100, []byte("tip")),
			trackedClient: &dchainsync.TrackedClient{Cursor: ocommon.NewPoint(110, []byte("ahead"))},
			want:          true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(
				t,
				test.want,
				shouldRestartChainsyncOnSwitch(
					test.localTip,
					test.trackedClient,
				),
			)
		})
	}
}

func TestChainsyncClientRollForward_IneligiblePeerDoesNotPoisonDedup(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	connEligible := newTestConnId("127.0.0.1:6000", "1.1.1.1:3001")
	connIneligible := newTestConnId("127.0.0.1:6000", "2.2.2.2:3001")
	state := dchainsync.NewState(bus, nil)
	require.True(t, state.AddClientConnId(connEligible))
	require.True(t, state.AddClientConnId(connIneligible))

	o := NewOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(connId ouroboros.ConnectionId) bool {
			return connId == connEligible
		},
	})
	o.ChainsyncState = state
	o.EventBus = bus

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
