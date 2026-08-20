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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// buildOverflowForkPath constructs a chain of headerCount headers extending
// directly from the fixture's committed tip and records all but the last
// into peerHeaderHistory, exactly as recordPeerHeaderHistory does for every
// observed header regardless of whether it is ever queued (see
// handleEventChainsyncBlockHeaderWithPending, which records before the
// buffering/queuing decision). The last header is returned separately as
// the live trigger event that arrives after the rest of the path is already
// known -- mirroring a large Genesis-mode fork-path reconstruction
// (findPeerForkPath) where the peer's own recent header history resolves
// all the way back to the local committed tip.
//
// Genesis selection is reported active with a large window so
// peerHeaderHistoryLimit accepts a path longer than the default 256-entry
// cap -- required to build a path that exceeds MaxQueuedHeaders (the
// default floor is 10,000), matching the shape of the live-sync freeze this
// file regression-tests (a single reconciliation event with several
// thousand fork-path headers, issue #1894 phase 3).
func buildOverflowForkPath(
	fixture *chainsyncRollbackFixture,
	connId ouroboros.ConnectionId,
	headerCount int,
) mockHeader {
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, uint64(headerCount) * 10
	}
	prevHash := fixture.currentTip.Point.Hash
	prevBlockNumber := fixture.currentTip.BlockNumber
	var trigger mockHeader
	for i := range headerCount {
		h := mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("overflow-fork-%d", i)),
			),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: prevBlockNumber + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		if i == headerCount-1 {
			trigger = h
			break
		}
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: connId,
			Point: ocommon.NewPoint(
				h.SlotNumber(),
				h.Hash().Bytes(),
			),
			BlockHeader: h,
		})
		prevHash = h.Hash().Bytes()
		prevBlockNumber = h.BlockNumber()
	}
	return trigger
}

// TestTryResolveForkExtensionRestartsBlockfetchAfterQueueOverflow pins the
// fix for the #1894 phase 3 live-sync freeze: a fork-resolution path whose
// length exceeds the header queue's capacity fails partway through
// (chain.ErrHeaderQueueFull) appending onto the current chain tip. Before
// the fix, tryResolveFork's "fork extends from current tip" loop returned
// immediately on that failure without ever restarting blockfetch for the
// headers it DID manage to queue -- and because chain.AddBlockHeader's
// capacity check runs before any "should I start a fetch" decision, no
// later header event, from any peer, fork or not, could ever trigger a
// fresh blockfetch again: the queue would never drain and the node would
// stop advancing permanently.
func TestTryResolveForkExtensionRestartsBlockfetchAfterQueueOverflow(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	maxHeaders := fixture.ls.chain.MaxQueuedHeaders()
	connId := testChainsyncConnId(6201, 3001)
	trigger := buildOverflowForkPath(fixture, connId, maxHeaders+5)

	// Sanity-check: chain.AddBlockHeader must reject the trigger header as
	// not fitting the (empty) header queue's tip, i.e. the not-fit gate the
	// production handler needs to reach tryResolveFork -- not a capacity
	// rejection, since the queue is empty at this point.
	err := fixture.ls.chain.AddBlockHeader(trigger)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAsf(
		t, err, &notFitErr,
		"expected the trigger header to be rejected as not fitting the "+
			"chain tip so the handler reaches tryResolveFork; got err=%v",
		err,
	)

	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) error {
		requestCount++
		return nil
	}

	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point: ocommon.NewPoint(
			trigger.SlotNumber(),
			trigger.Hash().Bytes(),
		),
		BlockHeader: trigger,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				trigger.SlotNumber()+10,
				testHashBytes("overflow-peer-tip-ahead"),
			),
			BlockNumber: trigger.BlockNumber() + 1,
		},
	}

	resolved, err := fixture.ls.tryResolveFork(evt, notFitErr, nil)
	require.NoError(t, err)
	require.False(
		t,
		resolved,
		"a fork path longer than the queue's capacity must fail to fully "+
			"append",
	)

	assert.Equal(
		t,
		maxHeaders,
		fixture.ls.chain.HeaderCount(),
		"the loop must queue headers up to exactly the queue's capacity "+
			"before failing",
	)
	assert.Equal(
		t,
		1,
		requestCount,
		"a queue-full fork-extension failure must still restart "+
			"blockfetch for the headers it did manage to queue when "+
			"nothing is currently fetching them -- otherwise the queue "+
			"never drains and the node stops advancing permanently",
	)
	assert.NotNil(
		t,
		fixture.ls.chainsyncBlockfetchReadyChan,
		"a fresh blockfetch batch must be recorded as in progress",
	)
}

// TestEnsureBlockfetchDrainingAfterForkQueueFailureRecoversWhenStartFails is
// a regression test for ensureBlockfetchDrainingAfterForkQueueFailure
// previously only logging a warning when the restart it attempts
// (startQueuedBlockfetchLocked/startQueuedBlockfetchOnLocked) itself fails.
// With nothing already fetching and the header queue full, that left the
// queued headers permanently stranded: chain.AddBlockHeader's capacity check
// rejects every later header, from any peer, before it can ever reach the
// "should I start a fetch" logic, so nothing would ever retry. The fix
// clears the queued headers and requests a chainsync re-sync instead of
// just logging, matching noteBlockfetchRangeUnavailable's equivalent
// recovery for the same "stuck queue" shape.
func TestEnsureBlockfetchDrainingAfterForkQueueFailureRecoversWhenStartFails(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	maxHeaders := fixture.ls.chain.MaxQueuedHeaders()
	connId := testChainsyncConnId(6203, 3001)
	trigger := buildOverflowForkPath(fixture, connId, maxHeaders+5)

	err := fixture.ls.chain.AddBlockHeader(trigger)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	// Every restart attempt fails, exercising the failure branch this test
	// targets rather than the already-covered success branch.
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) error {
		return errors.New("simulated blockfetch request failure")
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	fixture.ls.config.EventBus = bus
	resyncSubID, resyncCh := bus.SubscribeWithBuffer(
		event.ChainsyncResyncEventType,
		4,
	)
	require.NotEqual(t, event.EventSubscriberId(0), resyncSubID)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, resyncSubID)
	})

	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point: ocommon.NewPoint(
			trigger.SlotNumber(),
			trigger.Hash().Bytes(),
		),
		BlockHeader: trigger,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				trigger.SlotNumber()+10,
				testHashBytes("overflow-peer-tip-ahead-start-fails"),
			),
			BlockNumber: trigger.BlockNumber() + 1,
		},
	}

	// nil pending: pendingPublishes.add publishes immediately on a nil
	// receiver, which is what lets the subscription above observe the
	// resync request synchronously.
	resolved, err := fixture.ls.tryResolveFork(evt, notFitErr, nil)
	require.NoError(t, err)
	require.False(t, resolved)

	assert.Equal(
		t,
		0,
		fixture.ls.chain.HeaderCount(),
		"a restart failure with nothing else able to ever retry must "+
			"clear the stranded queued headers rather than leave them "+
			"permanently stuck",
	)

	resyncEvt := testutil.RequireReceive(
		t, resyncCh, 2*time.Second,
		"a chainsync re-sync must be requested when the recovery "+
			"restart itself fails",
	)
	data, ok := resyncEvt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok, "unexpected event payload %T", resyncEvt.Data)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonForkQueueOverflowRestartFailed,
		data.Reason,
	)
}

// TestTryResolveForkExtensionDoesNotThrashAlreadyRunningBlockfetch guards
// the other side of the same fix: the identical queue-full failure can fire
// repeatedly (once per rejected header) while a healthy batch is already
// draining the existing backlog -- this is the common case in practice, as
// many peers race small forks at the live tip in quick succession.
// ensureBlockfetchDrainingAfterForkQueueFailure must not interrupt that
// batch on every such event, or a batch would never be allowed to complete.
func TestTryResolveForkExtensionDoesNotThrashAlreadyRunningBlockfetch(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	maxHeaders := fixture.ls.chain.MaxQueuedHeaders()
	connId := testChainsyncConnId(6202, 3001)
	trigger := buildOverflowForkPath(fixture, connId, maxHeaders+5)

	err := fixture.ls.chain.AddBlockHeader(trigger)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) error {
		requestCount++
		return nil
	}
	// Simulate a blockfetch batch already in flight.
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	// Captured before tryResolveFork runs so the assertion below can prove
	// the existing batch's channel specifically survived untouched, not
	// merely that *a* non-nil channel exists afterward -- a regression
	// that replaced it with a brand new channel (without ever calling
	// BlockfetchRequestRangeFunc) would pass requestCount==0 but still be
	// wrong.
	preExistingReadyChan := fixture.ls.chainsyncBlockfetchReadyChan

	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point: ocommon.NewPoint(
			trigger.SlotNumber(),
			trigger.Hash().Bytes(),
		),
		BlockHeader: trigger,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				trigger.SlotNumber()+10,
				testHashBytes("overflow-peer-tip-ahead-inflight"),
			),
			BlockNumber: trigger.BlockNumber() + 1,
		},
	}

	resolved, err := fixture.ls.tryResolveFork(evt, notFitErr, nil)
	require.NoError(t, err)
	require.False(t, resolved)

	assert.Zero(
		t,
		requestCount,
		"an already in-progress blockfetch batch must not be "+
			"interrupted/restarted by a queue-full fork-extension failure",
	)
	assert.True(
		t,
		preExistingReadyChan == fixture.ls.chainsyncBlockfetchReadyChan,
		"the existing batch's ready channel must be left untouched -- a "+
			"regression that replaced it with a new channel without ever "+
			"calling BlockfetchRequestRangeFunc would otherwise pass "+
			"undetected",
	)
}
