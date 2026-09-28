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
	"testing"
	"time"

	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// blockfetchForwardTestBound is how long blockfetchClientBlock is given to
// return. It must return almost immediately regardless of ledger speed (the
// #4782 fix), so this is generous for a CI host but far below anything a
// genuine ledger-consumption wait would take (observed live at 2-5.4s per
// event).
const blockfetchForwardTestBound = 2 * time.Second

// stopEventBusBounded stops bus and fails the test if Stop does not return
// promptly. A forwarder goroutine deliberately left blocked in
// EventBus.Publish (this test's whole point) must be released by Stop, the
// same guarantee TestBlockfetchDrainDefersChainUpdatePastLedgerMutex relies
// on; an unbounded t.Cleanup(bus.Stop) would otherwise hang the test binary
// with no diagnostic if that guarantee ever regressed.
func stopEventBusBounded(t *testing.T, bus *event.EventBus) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		bus.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(blockfetchForwardTestBound):
		t.Error(
			"EventBus.Stop did not return: it failed to release the " +
				"forwarder goroutine parked on the stalled subscriber",
		)
	}
}

// TestBlockfetchClientBlockNeverBlocksOnStalledLedgerConsumer is the
// regression guard for blinklabs-io/dingo#4782: blockfetchClientBlock runs on
// the gouroboros blockfetch client's receive goroutine, which the shared
// muxer's read loop depends on to keep draining incoming socket data
// (including unrelated keep-alive pongs). Publishing to the ledger inline
// there let a slow or stalled ledger.blockfetch consumer (lossless,
// SubscriberBackpressureBlock) park that goroutine indefinitely, backing up
// the muxer and starving keep-alive until the peer's own timeout tore the
// connection down.
//
// This test puts the ledger.blockfetch subscriber into exactly that stalled
// state -- a lossless subscriber whose one buffer slot is filled and never
// drained -- then calls blockfetchClientBlock directly, twice. Under the
// original inline o.eventBus.Publish, the first call alone would block
// forever on the full buffer and the test would time out. The fix hands the
// event to a per-connection forward queue instead (blockfetch_forward.go),
// so both calls must return promptly even though neither the queue nor the
// ledger ever drains during the test.
func TestBlockfetchClientBlockNeverBlocksOnStalledLedgerConsumer(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { stopEventBusBounded(t, bus) })

	// Lossless subscriber, buffer 1, deliberately never drained -- mirrors
	// ls.subscribeBlockfetchEvents' real buffer/backpressure policy for
	// ledger.BlockfetchEventType.
	subId, ch := bus.SubscribeWithBufferPolicy(
		ledger.BlockfetchEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the single buffer slot so any further synchronous Publish blocks.
	// Confirm the stall is real, exactly as
	// TestBlockfetchDrainDefersChainUpdatePastLedgerMutex does for
	// chain.update: a broken test setup here would pass for the wrong
	// reason.
	bus.Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(ledger.BlockfetchEventType, ledger.BlockfetchEvent{}),
	)
	directBlocked := make(chan struct{})
	go func() {
		bus.Publish(
			ledger.BlockfetchEventType,
			event.NewEvent(
				ledger.BlockfetchEventType,
				ledger.BlockfetchEvent{},
			),
		)
		close(directBlocked)
	}()
	select {
	case <-directBlocked:
		t.Fatal(
			"inline Publish did not block on the stalled subscriber; " +
				"the test cannot exercise the deadlock condition",
		)
	case <-time.After(300 * time.Millisecond):
	}

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{EventBus: bus, PromRegistry: reg})

	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	connId := testConnId()
	ctx := blockfetch.CallbackContext{ConnectionId: connId, RequestId: 1}

	callReturned := func(blockIdx int) bool {
		done := make(chan error, 1)
		go func() {
			block := blocks[blockIdx]
			done <- o.blockfetchClientBlock(ctx, uint(block.Type()), block)
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
			return true
		case <-time.After(blockfetchForwardTestBound):
			return false
		}
	}

	require.True(
		t,
		callReturned(0),
		"blockfetchClientBlock blocked on a stalled ledger.blockfetch "+
			"consumer: it must enqueue and return, never publish inline",
	)
	require.True(
		t,
		callReturned(1),
		"a second blockfetchClientBlock call also blocked: the forward "+
			"queue must keep accepting while its single forwarder "+
			"goroutine is parked delivering the first event",
	)

	// Both blocks are accounted for as in-flight: the first is stuck mid
	// hand-off inside the forwarder's own (now-blocked) Publish call, the
	// second is still queued behind it. Neither has reached the ledger.
	wantBytes := float64(len(blocks[0].Cbor()) + len(blocks[1].Cbor()))
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(o.blockfetchMetrics.inFlightBlocks) == 2
	}, blockfetchForwardTestBound, 5*time.Millisecond,
		"expected 2 blockfetch events in flight",
	)
	require.Equal(
		t,
		wantBytes,
		testutil.ToFloat64(o.blockfetchMetrics.inFlightBytes),
		"in-flight bytes must account for both undelivered blocks",
	)

	// Drain the subscriber channel: the forwarder's blocked Publish call can
	// now complete, and it goes on to deliver the second queued event too.
	// In-flight bytes/blocks must converge back to zero once the ledger
	// consumer catches up -- proving the backlog this fix introduces is
	// bounded and self-resolving, not merely deferred forever. A continuous
	// blocking drain (not a fixed receive count) is required: the channel
	// also still holds the priming event from the stall check above and
	// contends with the still-parked directBlocked goroutine's own publish,
	// so the exact number of sends ahead of the two blockfetch events is not
	// fixed.
	drainDone := make(chan struct{})
	defer close(drainDone)
	go func() {
		for {
			select {
			case <-ch:
			case <-drainDone:
				return
			}
		}
	}()

	require.Eventually(
		t,
		func() bool {
			return testutil.ToFloat64(o.blockfetchMetrics.inFlightBlocks) == 0
		},
		blockfetchForwardTestBound,
		5*time.Millisecond,
		"in-flight blocks must drain to zero once the ledger consumer catches up",
	)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(o.blockfetchMetrics.inFlightBytes),
		"in-flight bytes must drain to zero alongside the blocks",
	)
}
