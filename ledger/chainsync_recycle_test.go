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
