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
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ouroboros "github.com/blinklabs-io/gouroboros"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// fakeBlockfetchRangeRequester is a test double for blockfetchRangeRequester,
// standing in for a live *blockfetch.Client. It lets
// BlockfetchClientRequestRange's own dispatch and blockFetchStarts
// bookkeeping be exercised without a live connection registered in
// connManager: the production code path (blockfetchConnClientLive) requires
// exactly that, which no existing test stands up for this function.
type fakeBlockfetchRangeRequester struct {
	mu     sync.Mutex
	nextID uint64
}

func (f *fakeBlockfetchRangeRequester) RequestRange(
	_ context.Context,
	_ blockfetch.RangeRequest,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextID++
	return f.nextID, nil
}

// TestBlockfetchClientRequestRangeOverlappingRequestsDoNotClobberStartTimes
// dispatches two requests on the same connId with different request IDs and
// asserts blockFetchStarts keeps a distinct entry for each.
//
// gouroboros' nextRequestId is scoped per connection, so requestId alone is
// not globally unique, and connId alone is exactly today's clobber bug: prior
// to RequestRange replacing GetBlockRange, BlockfetchClientRequestRange kept
// blockFetchStarts keyed only by connId, so
// `o.blockFetchStarts[connId] = time.Now()` unconditionally overwrote
// whatever entry a still-outstanding request on the same connection had
// already recorded. Pipelining means more than one request can be
// outstanding on one connection at once, so the second dispatch's start time
// would silently replace the first's, corrupting that request's own latency
// accounting when it eventually resolves.
func TestBlockfetchClientRequestRangeOverlappingRequestsDoNotClobberStartTimes(
	t *testing.T,
) {
	t.Parallel()

	connId := testConnId()
	fake := &fakeBlockfetchRangeRequester{}
	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	firstId, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)
	secondId, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)
	require.NotEqual(
		t,
		firstId,
		secondId,
		"the fake requester must hand out distinct request IDs for this to prove anything",
	)

	o.blockFetchMutex.Lock()
	_, firstOk := o.blockFetchStarts[blockFetchKey{
		connId:    connId,
		requestId: firstId,
	}]
	_, secondOk := o.blockFetchStarts[blockFetchKey{
		connId:    connId,
		requestId: secondId,
	}]
	entryCount := len(o.blockFetchStarts)
	o.blockFetchMutex.Unlock()

	assert.True(
		t,
		firstOk,
		"the first request's start time must survive the second request's dispatch",
	)
	assert.True(
		t,
		secondOk,
		"the second request's start time must be recorded",
	)
	assert.Equal(
		t,
		2,
		entryCount,
		"both requests must have their own blockFetchStarts entry",
	)
}

// TestBlockfetchClientRequestRangeDepthTwoNeverBlocksOnCapacity dispatches two
// pipelined RequestRange calls back to back over dingo's real client wiring
// (WithRequestPipelining enabled, WithMaxInFlightBytes left at gouroboros'
// default), and asserts neither blocks waiting for in-flight-byte admission.
// A depth-2 pipeline's two default-sized reservations should be nowhere near
// the ~8.8MB default budget (100 * 88KiB) -- this pins that as an observed
// fact about the wiring, rather than trusting the arithmetic never changes
// underneath it.
func TestBlockfetchClientRequestRangeDepthTwoNeverBlocksOnCapacity(
	t *testing.T,
) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := newBlockfetchPeerWithOpts(t, o.blockfetchClientConnOpts()...)

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	for i := range 2 {
		done := make(chan error, 1)
		go func() {
			_, err := peer.client.RequestRange(
				context.Background(),
				blockfetch.RangeRequest{Start: start, End: end},
			)
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err, "request %d", i)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"RequestRange %d blocked on in-flight capacity at depth 2",
				i,
			)
		}
	}
}

// TestBlockfetchClientRequestRangeUnblocksOnStopWhileWaitingForCapacity
// verifies the safety argument BlockfetchClientRequestRange's use of
// context.Background() rests on: sendRequestRange's internal waits (the
// in-flight byte budget here) select on the connection's own protocol
// shutdown channel in addition to the caller's context, so a request parked
// waiting for capacity still unblocks when the protocol stops, even though
// this caller's own context is never canceled directly.
//
// This drives gouroboros' *blockfetch.Client directly (not through
// BlockfetchClientRequestRange), since it is gouroboros' own contract being
// verified, not dingo's wrapper of it.
func TestBlockfetchClientRequestRangeUnblocksOnStopWhileWaitingForCapacity(
	t *testing.T,
) {
	t.Parallel()

	peer := newBlockfetchPeerWithOpts(
		t,
		blockfetch.WithRequestPipelining(true),
		blockfetch.WithRangeDoneFunc(
			func(blockfetch.CallbackContext, error) error { return nil },
		),
		// A tiny budget forces the second request below to genuinely wait:
		// the first reservation is never released (its RangeDoneFunc never
		// resolves it -- nothing answers on the wire in this test), so the
		// queue never empties and the second call cannot bypass the check
		// through the "empty queue always admits" rule.
		blockfetch.WithMaxInFlightBytes(1),
	)

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	_, err := peer.client.RequestRange(context.Background(), blockfetch.RangeRequest{
		Start:         start,
		End:           end,
		ExpectedBytes: 1,
	})
	require.NoError(t, err)

	blocked := make(chan error, 1)
	go func() {
		_, err := peer.client.RequestRange(
			context.Background(),
			blockfetch.RangeRequest{Start: start, End: end, ExpectedBytes: 1},
		)
		blocked <- err
	}()

	// There is no exported hook for "the second call has reached the
	// capacity wait" (gouroboros' own beforeInFlightWait test seam is
	// unexported), so this is a bounded assertion that it has not returned
	// yet, not a proof it is parked in the wait specifically -- the
	// no-hook gap is real, and the remainder of the test still proves the
	// call unblocks specifically because of Stop(), not because it was
	// about to return anyway (see the timing below).
	testutil.RequireNoReceive(
		t,
		blocked,
		100*time.Millisecond,
		"second RequestRange returned before Stop() gave it a reason to",
	)

	// Client.Stop()'s own doc comment allows a delivery error here: this
	// client parked its RequestRange mid-batch, so ClientDone is not
	// pipelinable in Busy/Streaming (StateMap's PipelinedMessageTypes carries
	// only MessageTypeRequestRange there) and cannot go out until agency
	// returns -- which nothing in this test ever grants. Stop() still fully
	// tears down the protocol either way.
	stopErr := peer.client.Stop()
	if stopErr != nil {
		require.ErrorIs(t, stopErr, context.DeadlineExceeded)
	}

	select {
	case err := <-blocked:
		require.ErrorIs(t, err, protocol.ErrProtocolShuttingDown)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"RequestRange blocked on in-flight capacity did not unblock after Stop()",
		)
	}
}

// terminalBeforeIdRequester resolves each request terminally, through the
// RangeDoneFunc path, before handing its ID back to the dispatcher. That is
// the ordering RequestRange permits but a live peer only rarely produces:
// the request is on the wire when RequestRange returns, so the protocol's
// receive goroutine can run blockfetchClientRangeDone to completion while the
// dispatcher is still on its way to recording the start time.
type terminalBeforeIdRequester struct {
	o      *Ouroboros
	connId ouroboros.ConnectionId
	nextID uint64
}

func (f *terminalBeforeIdRequester) RequestRange(
	_ context.Context,
	_ blockfetch.RangeRequest,
) (uint64, error) {
	f.nextID++
	if err := f.o.blockfetchClientRangeDone(
		blockfetch.CallbackContext{
			ConnectionId: f.connId,
			RequestId:    f.nextID,
		},
		nil,
	); err != nil {
		return 0, err
	}
	return f.nextID, nil
}

// TestBlockfetchClientRequestRangeTerminalBeforeIdLeavesNoStartEntry asserts
// that a request whose terminal callback lands first records no start time.
// blockfetchClientRangeDone is a request's only deleter and runs exactly once,
// so an entry inserted after it has already fired is never removed: it sits in
// blockFetchStarts until the connection is torn down, and a peer that answers
// every request that quickly grows the map for the life of the connection.
func TestBlockfetchClientRequestRangeTerminalBeforeIdLeavesNoStartEntry(
	t *testing.T,
) {
	t.Parallel()

	connId := testConnId()
	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	fake := &terminalBeforeIdRequester{o: o, connId: connId}
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	const requests = 4
	for range requests {
		_, err := o.BlockfetchClientRequestRange(connId, start, end)
		require.NoError(t, err)
	}

	o.blockFetchMutex.Lock()
	startCount := len(o.blockFetchStarts)
	earlyCount := len(o.blockFetchDoneEarly)
	o.blockFetchMutex.Unlock()

	assert.Equal(
		t,
		0,
		startCount,
		"a request already reported terminal must leave no start-time entry",
	)
	assert.Equal(
		t,
		0,
		earlyCount,
		"each dispatch must consume its own terminal marker",
	)
}

// TestBlockfetchClientRequestRangeTerminalAfterIdStillTimes is the control
// for the test above: in the ordinary ordering the start time is recorded and
// the terminal callback removes it, leaving both maps empty by a different
// route. Without it, a fix that simply stopped recording start times would
// satisfy the race test while destroying the timing this map exists for.
func TestBlockfetchClientRequestRangeTerminalAfterIdStillTimes(t *testing.T) {
	t.Parallel()

	connId := testConnId()
	fake := &fakeBlockfetchRangeRequester{}
	o := newOuroboros(OuroborosConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	o.blockfetchConnClient = func(
		ouroboros.ConnectionId,
	) (blockfetchRangeRequester, error) {
		return fake, nil
	}

	start := ocommon.NewPoint(1, make([]byte, lcommon.Blake2b256Size))
	end := ocommon.NewPoint(2, make([]byte, lcommon.Blake2b256Size))

	requestId, err := o.BlockfetchClientRequestRange(connId, start, end)
	require.NoError(t, err)

	o.blockFetchMutex.Lock()
	_, recorded := o.blockFetchStarts[blockFetchKey{
		connId:    connId,
		requestId: requestId,
	}]
	o.blockFetchMutex.Unlock()
	require.True(
		t,
		recorded,
		"the ordinary ordering must still record a start time",
	)

	require.NoError(t, o.blockfetchClientRangeDone(
		blockfetch.CallbackContext{
			ConnectionId: connId,
			RequestId:    requestId,
		},
		nil,
	))

	o.blockFetchMutex.Lock()
	startCount := len(o.blockFetchStarts)
	earlyCount := len(o.blockFetchDoneEarly)
	o.blockFetchMutex.Unlock()

	assert.Equal(t, 0, startCount, "the terminal callback must clear the entry")
	assert.Equal(
		t,
		0,
		earlyCount,
		"a terminal callback that found its entry must leave no marker",
	)
}
