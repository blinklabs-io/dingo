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
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type testBlockHeader struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	blockNumber uint64
	slot        uint64
}

func (h testBlockHeader) Hash() lcommon.Blake2b256          { return h.hash }
func (h testBlockHeader) PrevHash() lcommon.Blake2b256      { return h.prevHash }
func (h testBlockHeader) BlockNumber() uint64               { return h.blockNumber }
func (h testBlockHeader) SlotNumber() uint64                { return h.slot }
func (h testBlockHeader) IssuerVkey() lcommon.IssuerVkey    { return lcommon.IssuerVkey{} }
func (h testBlockHeader) BlockBodySize() uint64             { return 0 }
func (h testBlockHeader) Era() lcommon.Era                  { return babbage.EraBabbage }
func (h testBlockHeader) Cbor() []byte                      { return nil }
func (h testBlockHeader) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

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

func TestChainsyncClientRollForwardPublishesDuplicateFromSelectedPeer(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	bus := event.NewEventBus(nil, logger)
	defer bus.Stop()

	_, ch := bus.Subscribe(ledger.ChainsyncEventType)
	state := chainsync.NewState(bus, nil)
	connA := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connB := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	require.True(t, state.AddClientConnId(connA))
	require.True(t, state.AddClientConnId(connB))
	state.SetClientConnId(connA)

	o := &Ouroboros{
		EventBus:       bus,
		ChainsyncState: state,
		config: OuroborosConfig{
			Logger: logger,
			ChainsyncIngressEligible: func(
				ouroboros.ConnectionId,
			) bool {
				return true
			},
		},
	}
	header := testBlockHeader{
		hash:        lcommon.NewBlake2b256([]byte("header-1")),
		prevHash:    lcommon.NewBlake2b256([]byte("prev-0")),
		blockNumber: 1,
		slot:        100,
	}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(header.slot, header.hash.Bytes()),
		BlockNumber: header.blockNumber,
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
		t.Fatal("expected duplicate header from selected peer to publish")
	}
}
