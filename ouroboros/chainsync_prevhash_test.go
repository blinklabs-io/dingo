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
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	dchainsync "github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestChainsyncClientRollForwardPublishesParentHash proves the roll-forward
// handler hands the selector the parent hash the delivered header names, which
// a lone far frontier needs to be recognised as a connected header chain.
func TestChainsyncClientRollForwardPublishesParentHash(t *testing.T) {
	t.Parallel()

	for _, boundary := range []bool{false, true} {
		t.Run(fmt.Sprintf("boundary=%v", boundary), func(t *testing.T) {
			t.Parallel()
			testRollForwardPublishesParentHash(t, boundary)
		})
	}
}

func testRollForwardPublishesParentHash(t *testing.T, boundary bool) {
	t.Helper()

	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	_, tipCh := bus.Subscribe(chainselection.PeerTipUpdateEventType)
	state := dchainsync.NewState(bus, nil)
	conn := newTestConnId("127.0.0.1:6013", "10.0.0.12:3001")
	require.True(t, state.AddClientConnId(conn))

	o := newOuroboros(OuroborosConfig{
		EventBus: bus,
		ChainsyncIngressEligible: func(ouroboros.ConnectionId) bool {
			return true
		},
		ChainsyncApplyEligible: func(ouroboros.ConnectionId) bool {
			return false
		},
	})
	o.chainsyncState = state
	o.eventBus = bus

	blockType := uint(gledger.BlockTypeByronMain)
	if boundary {
		blockType = gledger.BlockTypeByronEbb
	}
	header, ok := newTestBlockHeader(300, 2, 0xaa).(*testBlockHeader)
	require.True(t, ok)
	header.prevHash[0] = 0xbb
	require.NoError(t, o.chainsyncClientRollForward(
		ochainsync.CallbackContext{ConnectionId: conn},
		blockType,
		header,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(300, header.Hash().Bytes()),
			BlockNumber: 2,
		},
	))

	select {
	case evt := <-tipCh:
		data, ok := evt.Data.(chainselection.PeerTipUpdateEvent)
		require.True(t, ok)
		require.Equal(t, header.PrevHash().Bytes(), data.ObservedPrevHash)
		require.Equal(t, byte(0xbb), data.ObservedPrevHash[0])
		require.Equal(t, boundary, data.ObservedBoundary)
	case <-time.After(5 * time.Second):
		t.Fatal("expected the header to be observed")
	}
}
