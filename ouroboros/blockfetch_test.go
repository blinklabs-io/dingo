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
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// syncLogBuffer is a mutex-guarded log sink. The blockfetch server processes
// requests on its own recvLoop goroutine, which logs concurrently with the
// test goroutine reading those logs back -- a plain bytes.Buffer would race.
type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncLogBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncLogBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncLogBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
}

// testConnId creates a ConnectionId with valid net.Addr values for testing.
func testConnId() ouroboros_conn.ConnectionId {
	return ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
}

type stubBlockfetchBatchServer struct {
	startBatchCalls int
	blockCalls      int
	batchDoneCalls  int
	startBatchErr   error
	blockErr        error
	batchDoneErr    error
}

func (s *stubBlockfetchBatchServer) StartBatch() error {
	s.startBatchCalls++
	return s.startBatchErr
}

func (s *stubBlockfetchBatchServer) Block(_ uint, _ []byte) error {
	s.blockCalls++
	return s.blockErr
}

func (s *stubBlockfetchBatchServer) BatchDone() error {
	s.batchDoneCalls++
	return s.batchDoneErr
}

type stubBlockfetchDrainBatchServer struct {
	stubBlockfetchBatchServer
	drainCalls    int
	drainResults  []bool
	drainTimeouts []time.Duration
}

func (s *stubBlockfetchDrainBatchServer) WaitSendQueueDrained(
	timeout time.Duration,
) bool {
	s.drainCalls++
	s.drainTimeouts = append(s.drainTimeouts, timeout)
	if len(s.drainResults) == 0 {
		return true
	}
	result := s.drainResults[0]
	s.drainResults = s.drainResults[1:]
	return result
}

type blockfetchIteratorStep struct {
	result *chain.ChainIteratorResult
	err    error
}

type stubBlockfetchIterator struct {
	steps       []blockfetchIteratorStep
	nextCalls   int
	cancelCalls int
}

func (i *stubBlockfetchIterator) Next(
	bool,
) (*chain.ChainIteratorResult, error) {
	if i.nextCalls >= len(i.steps) {
		return nil, chain.ErrIteratorChainTip
	}
	step := i.steps[i.nextCalls]
	i.nextCalls++
	return step.result, step.err
}

func (i *stubBlockfetchIterator) Cancel() {
	i.cancelCalls++
}

type stubBlockfetchConnection struct {
	errChan    chan error
	closeCalls int
	closeErr   error
}

func (c *stubBlockfetchConnection) ErrorChan() chan error {
	return c.errChan
}

func (c *stubBlockfetchConnection) Close() error {
	c.closeCalls++
	return c.closeErr
}

func testBlockfetchIteratorBlock(slot uint64) *chain.ChainIteratorResult {
	return &chain.ChainIteratorResult{
		Point: ocommon.NewPoint(slot, []byte{byte(slot)}),
		Block: models.Block{
			Slot: slot,
			Type: 1,
			Cbor: []byte{byte(slot), byte(slot + 1)},
		},
	}
}

func TestBlockfetchServerRequestRange_StartAfterEnd(t *testing.T) {
	// When start slot > end slot, blockfetchServerRequestRange should
	// log a warning and attempt to send NoBlocks. Since we don't have a
	// real protocol server wired up, the NoBlocks call will panic on the
	// nil server. We use assert.Panics to catch that, and then verify the
	// warning was logged BEFORE the NoBlocks call, proving the range check
	// was reached (not GetChainFromPoint, which would be a different panic).
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	// LedgerState is intentionally nil - if the range check works,
	// we never reach GetChainFromPoint and avoid a nil dereference on
	// LedgerState.

	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(50, []byte{0x02})
	ctx := blockfetch.CallbackContext{
		ConnectionId: testConnId(),
		// Server is nil, so NoBlocks() will panic after the log.
	}

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(ctx, start, end)
	}, "expected panic from nil Server.NoBlocks()")

	// Verify the warning was logged before the panic
	logOutput := logBuf.String()
	assert.Contains(
		t,
		logOutput,
		"start after end",
		"expected log message about start after end",
	)
}

func TestBlockfetchServerRequestRange_EqualPoints(t *testing.T) {
	// When start == end (same slot), this is a valid single-block range
	// and should NOT trigger the "start after end" check.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	// LedgerState is nil, so GetChainFromPoint will panic.
	// This verifies the range check does NOT reject equal slots.
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(100, []byte{0x01})

	// We expect a panic from nil LedgerState (not from range validation),
	// which proves that equal slots pass the range check.
	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(
			blockfetch.CallbackContext{
				ConnectionId: testConnId(),
			},
			start,
			end,
		)
	}, "equal slot range should pass validation and reach LedgerState call")
}

func TestBlockfetchServerRequestRange_OversizedRange(t *testing.T) {
	// When the slot range exceeds MaxBlockFetchRange, the server should
	// log a warning and attempt to send NoBlocks. Since Server is nil,
	// the NoBlocks call will panic. We verify the correct warning was
	// logged before the panic, proving the range size check was reached.
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	start := ocommon.NewPoint(0, []byte{0x01})
	end := ocommon.NewPoint(MaxBlockFetchRange+1, []byte{0x02})
	ctx := blockfetch.CallbackContext{
		ConnectionId: testConnId(),
	}

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(ctx, start, end)
	}, "expected panic from nil Server.NoBlocks()")

	logOutput := logBuf.String()
	assert.Contains(
		t,
		logOutput,
		"range exceeds maximum",
		"expected log message about oversized range",
	)
	assert.Contains(
		t,
		logOutput,
		`"level":"DEBUG"`,
		"oversized range NoBlocks should log at DEBUG, not WARN",
	)
	assert.NotContains(
		t,
		logOutput,
		`"level":"WARN"`,
		"oversized range NoBlocks should not produce a WARN line",
	)
}

func TestBlockfetchServerRequestRange_RangeWithinLimit(t *testing.T) {
	// A range within MaxBlockFetchRange should pass both validation
	// checks and proceed to GetChainFromPoint. Since LedgerState is nil,
	// this will panic at that call, proving the range check did not
	// reject it.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	start := ocommon.NewPoint(1000, []byte{0x01})
	end := ocommon.NewPoint(1000+MaxBlockFetchRange-1, []byte{0x02})

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(
			blockfetch.CallbackContext{
				ConnectionId: testConnId(),
			},
			start,
			end,
		)
	}, "range within limit should pass validation and reach LedgerState call")
}

func TestBlockfetchServerRequestRange_ExactlyAtLimit(t *testing.T) {
	// A range of exactly MaxBlockFetchRange slots should be accepted
	// (the check is > not >=). Since LedgerState is nil, this will
	// panic at GetChainFromPoint, proving the range check passed.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	start := ocommon.NewPoint(1000, []byte{0x01})
	end := ocommon.NewPoint(1000+MaxBlockFetchRange, []byte{0x02})

	assert.Panics(t, func() {
		_ = o.blockfetchServerRequestRange(
			blockfetch.CallbackContext{
				ConnectionId: testConnId(),
			},
			start,
			end,
		)
	}, "range exactly at limit should pass validation and reach LedgerState call")
}

func TestBlockfetchServerSendBatch_ClosesConnectionOnIteratorError(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{err: errors.New("iterator exploded")},
		},
	}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
	)

	assert.Error(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 0, server.batchDoneCalls)
	assert.Equal(t, 1, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

func TestBlockfetchServerSendBatch_BatchDoneAtChainTip(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(t, 0, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

func TestBlockfetchServerSendBatch_RollbackEndsBatchWithoutServingBlock(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	// The chain iterator surfaces a rollback as a sentinel result with
	// Rollback=true and a zero-value Block. Serving that zero block streams a
	// [0, null] block that a fetching peer decodes as a nil-header Byron EBB
	// and crashes dereferencing it in SlotNumber(). The server must end the
	// batch instead of serving the sentinel.
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{result: &chain.ChainIteratorResult{
				Point:    ocommon.NewPoint(150, []byte{0x03}),
				Rollback: true,
			}},
		},
	}
	server := &stubBlockfetchBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	// The rollback sentinel must NOT be streamed as a block.
	assert.Equal(t, 0, server.blockCalls,
		"rollback sentinel must not be streamed as a block")
	// The batch ends cleanly so the client re-requests against its updated
	// chain (blockfetch has no rollback message).
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(t, 0, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
}

func TestBlockfetchServerSendBatch_WaitsForSendDrainBetweenMessages(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{result: testBlockfetchIteratorBlock(100)},
			{result: testBlockfetchIteratorBlock(101)},
		},
	}
	server := &stubBlockfetchDrainBatchServer{}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(101, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
	)

	assert.NoError(t, err)
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 2, server.blockCalls)
	assert.Equal(t, 1, server.batchDoneCalls)
	assert.Equal(t, 0, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
	assert.Equal(t, 3, server.drainCalls)
	for _, timeout := range server.drainTimeouts {
		assert.Equal(t, blockfetchServerSendDrainTimeout, timeout)
	}
}

func TestBlockfetchServerSendBatch_ClosesConnectionWhenSendDrainStalls(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	iter := &stubBlockfetchIterator{
		steps: []blockfetchIteratorStep{
			{result: testBlockfetchIteratorBlock(100)},
			{result: testBlockfetchIteratorBlock(101)},
		},
	}
	server := &stubBlockfetchDrainBatchServer{
		drainResults: []bool{true, false},
	}
	conn := &stubBlockfetchConnection{
		errChan: make(chan error),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(101, []byte{0x02})

	err := o.blockfetchServerSendBatch(
		testConnId().String(),
		start,
		end,
		iter,
		server,
		conn,
	)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "send queue did not drain after Block")
	assert.Equal(t, 1, server.startBatchCalls)
	assert.Equal(t, 1, server.blockCalls)
	assert.Equal(t, 0, server.batchDoneCalls)
	assert.Equal(t, 1, conn.closeCalls)
	assert.Equal(t, 1, iter.cancelCalls)
	assert.Equal(t, 2, server.drainCalls)
}

func TestReportBlockfetchServerAsyncError_ClosesConnection(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	conn := &stubBlockfetchConnection{errChan: make(chan error)}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})
	o.reportBlockfetchServerAsyncError(
		conn,
		testConnId().String(),
		start,
		end,
		errors.New("async blockfetch failure"),
	)

	assert.Equal(t, 1, conn.closeCalls)
}

func TestReportBlockfetchServerAsyncError_ReportsCloseErrorWithoutPanic(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	conn := &stubBlockfetchConnection{
		errChan:  make(chan error),
		closeErr: errors.New("close failed"),
	}
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(200, []byte{0x02})

	assert.NotPanics(t, func() {
		o.reportBlockfetchServerAsyncError(
			conn,
			testConnId().String(),
			start,
			end,
			errors.New("closed channel test"),
		)
	})
	assert.Equal(t, 1, conn.closeCalls)
}

// TestBlockfetchRecordNoBlocks_BelowThreshold verifies repeated NoBlocks stay
// below the close threshold until the configured limit is reached.
func TestBlockfetchRecordNoBlocks_BelowThreshold(t *testing.T) {
	// Returns false for each of the first four identical NoBlocks requests.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(
			t,
			o.blockfetchRecordNoBlocks(connId, start),
			"should not trigger before threshold",
		)
	}
}

// TestBlockfetchRecordNoBlocks_ReachesThreshold verifies the stuck-peer
// detector triggers on the configured consecutive NoBlocks threshold.
func TestBlockfetchRecordNoBlocks_ReachesThreshold(t *testing.T) {
	// Returns true on the fifth consecutive NoBlocks for the same start point.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		o.blockfetchRecordNoBlocks(connId, start)
	}
	assert.True(
		t,
		o.blockfetchRecordNoBlocks(connId, start),
		"should trigger on 5th consecutive request",
	)
}

// TestBlockfetchRecordNoBlocks_ProgressResetsCounter verifies valid progress
// clears prior NoBlocks counts for the connection.
func TestBlockfetchRecordNoBlocks_ProgressResetsCounter(t *testing.T) {
	// Valid blockfetch progress clears prior NoBlocks counts for the connection.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(t, o.blockfetchRecordNoBlocks(connId, start))
	}

	o.blockfetchResetNoBlocks(connId)

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(
			t,
			o.blockfetchRecordNoBlocks(connId, start),
			"counter should reset after progress",
		)
	}
	assert.True(
		t,
		o.blockfetchRecordNoBlocks(connId, start),
		"should need another full sequence after progress",
	)
}

// TestBlockfetchRecordNoBlocks_IndependentPoints verifies changing start
// points resets the consecutive NoBlocks count on the same connection.
func TestBlockfetchRecordNoBlocks_IndependentPoints(t *testing.T) {
	// Only consecutive NoBlocks for the same start point should accumulate.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	startA := ocommon.NewPoint(100, []byte{0x01})
	startB := ocommon.NewPoint(200, []byte{0x02})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(t, o.blockfetchRecordNoBlocks(connId, startA))
	}
	assert.False(
		t,
		o.blockfetchRecordNoBlocks(connId, startB),
		"different start point should not inherit count",
	)
	assert.False(
		t,
		o.blockfetchRecordNoBlocks(connId, startA),
		"interleaved start point should reset consecutive count",
	)
}

// TestBlockfetchRecordNoBlocks_IndependentConns verifies NoBlocks counts are
// tracked separately for each connection.
func TestBlockfetchRecordNoBlocks_IndependentConns(t *testing.T) {
	// Tracks counters independently per connection for the same start point.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connA := ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	connB := ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3003},
	}
	start := ocommon.NewPoint(100, []byte{0x01})

	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		o.blockfetchRecordNoBlocks(connA, start)
	}
	// Different connId at the same point must have its own independent counter
	assert.False(
		t,
		o.blockfetchRecordNoBlocks(connB, start),
		"different connId should not inherit count",
	)
}

// TestBlockfetchRecordNoBlocks_CleanupResetsCounter verifies connection-close
// cleanup clears stuck-peer state before a reconnect starts fresh.
func TestBlockfetchRecordNoBlocks_CleanupResetsCounter(t *testing.T) {
	// Resets the counter after connection close so the peer gets a fresh count on reconnect.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	connId := testConnId()
	start := ocommon.NewPoint(100, []byte{0x01})

	// Drive counter to threshold
	for range blockfetchMaxConsecutiveNoBlocks {
		o.blockfetchRecordNoBlocks(connId, start)
	}

	// Simulate connection close cleanup
	o.blockFetchMutex.Lock()
	delete(o.blockfetchNoBlocksCounts, connId)
	o.blockFetchMutex.Unlock()

	// Counter should be reset — needs another full sequence to trigger
	for range blockfetchMaxConsecutiveNoBlocks - 1 {
		assert.False(
			t,
			o.blockfetchRecordNoBlocks(connId, start),
			"counter should reset after cleanup",
		)
	}
	assert.True(
		t,
		o.blockfetchRecordNoBlocks(connId, start),
		"should trigger again after reset",
	)
}

// newBlockfetchServerPeer builds a muxerServerPeer driving Dingo's real
// blockfetch server config (blockfetchServerConnOpts, instrumentation
// wrappers included), so requests reach blockfetchServerRequestRange exactly
// as a real peer's would and NoBlocks() actually goes out on the wire
// instead of panicking on a nil CallbackContext.Server the way the
// direct-call tests above do.
func newBlockfetchServerPeer(t *testing.T, o *Ouroboros) *muxerServerPeer {
	t.Helper()
	opts, peer := newMuxerServerPeer(t)
	cfg, err := blockfetch.NewConfig(o.blockfetchServerConnOpts()...)
	require.NoError(t, err)
	server := blockfetch.NewServer(opts, &cfg)
	peer.start(t, server)
	return peer
}

// TestBlockfetchServerRequestRange_RepeatedInvertedRangeReachesCloseThreshold
// is issue #3428: an inverted range (start after end) sent NoBlocks without
// calling blockfetchRecordNoBlocksAndMaybeClose, the same valve oversized and
// missing-point rejections use, so a peer repeating an inverted request never
// counted toward blockfetchMaxConsecutiveNoBlocks and was never closed.
//
// This drives real MsgRequestRange traffic over a real blockfetch.Server/
// muxer pair -- unlike TestBlockfetchServerRequestRange_StartAfterEnd above,
// which only proves the check is reached before its NoBlocks call panics on a
// nil Server -- so the shared valve's close-eligible WARN log fires for real
// once the configured threshold is reached. o.connManager is nil, so this
// only proves the inverted-range branch now feeds the valve and the valve
// reaches its close-eligible state; it does not assert an actual connection
// close, which blockfetchRecordNoBlocksAndMaybeClose only attempts when
// connManager is non-nil. closeBlockfetchConnection's Close() call is already
// covered generically by TestBlockfetchServerSendBatch_ClosesConnectionWhenSendDrainStalls
// and TestReportBlockfetchServerAsyncError_ClosesConnection above.
func TestBlockfetchServerRequestRange_RepeatedInvertedRangeReachesCloseThreshold(
	t *testing.T,
) {
	const closeWarnMsg = "closing stuck peer after repeated inverted range requests"

	logBuf := &syncLogBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	peer := newBlockfetchServerPeer(t, o)

	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(50, []byte{0x02})

	for i := 1; i <= blockfetchMaxConsecutiveNoBlocks; i++ {
		logBuf.Reset()
		peer.send(
			t,
			blockfetch.ProtocolId,
			blockfetch.NewMsgRequestRange(start, end),
		)

		segment := peer.readResponse(t, 5*time.Second)
		assert.Equal(t, blockfetch.ProtocolId, segment.GetProtocolId())
		assert.Equal(
			t,
			[]byte{0x81, blockfetch.MessageTypeNoBlocks},
			segment.Payload,
			"request %d should be answered with NoBlocks",
			i,
		)

		if i < blockfetchMaxConsecutiveNoBlocks {
			// Below threshold, blockfetchRecordNoBlocksAndMaybeClose logs
			// nothing at all -- the only log line for this request is the
			// "start after end" one written before NoBlocks() was even
			// enqueued, which readResponse above already happened-before.
			assert.False(
				t,
				strings.Contains(logBuf.String(), closeWarnMsg),
				"request %d should not yet reach the close threshold",
				i,
			)
		} else {
			// At threshold, the close-eligible WARN is logged after NoBlocks()
			// is enqueued, on the server's own goroutine, with no ordering
			// guarantee relative to the wire bytes readResponse observed --
			// poll instead of asserting immediately.
			testutil.WaitForCondition(
				t,
				func() bool {
					return strings.Contains(logBuf.String(), closeWarnMsg)
				},
				2*time.Second,
				"expected close-eligible WARN once the threshold is reached",
			)
		}
	}
}

// TestBlockfetchServerRequestRange_RepeatedValidRangeNeverClosesPeer is the
// control for the fix above: a valid (non-inverted, in-limit) range must
// never be treated as an inverted-range rejection, however many times it is
// repeated for the same connection and start point. LedgerState is nil, so a
// valid range reaches GetChainFromPoint and panics there -- proving it passed
// both the inverted-range and oversized-range checks without either
// rejecting it.
func TestBlockfetchServerRequestRange_RepeatedValidRangeNeverClosesPeer(
	t *testing.T,
) {
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})
	start := ocommon.NewPoint(100, []byte{0x01})
	end := ocommon.NewPoint(150, []byte{0x02})
	ctx := blockfetch.CallbackContext{ConnectionId: testConnId()}

	for range blockfetchMaxConsecutiveNoBlocks {
		assert.Panics(t, func() {
			_ = o.blockfetchServerRequestRange(ctx, start, end)
		}, "valid range should reach LedgerState call, not get rejected")
	}

	logOutput := logBuf.String()
	assert.NotContains(t, logOutput, "start after end")
	assert.NotContains(t, logOutput, "inverted range")
}

func BenchmarkBlockfetchClientBlockMetrics(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	o := newOuroboros(OuroborosConfig{
		Logger:       logger,
		EventBus:     eventBus,
		PromRegistry: prometheus.NewRegistry(),
	})

	immDb, err := immutable.New("../database/immutable/testdata")
	if err != nil {
		b.Fatal(err)
	}
	iterator, err := immDb.BlocksFromPoint(ocommon.NewPoint(0, nil))
	if err != nil {
		b.Fatal(err)
	}
	defer iterator.Close()

	const blockCount = 100
	blocks := make([]gledger.Block, 0, blockCount)
	for len(blocks) < blockCount {
		block, err := iterator.Next()
		if err != nil {
			b.Fatal(err)
		}
		if block == nil {
			break
		}
		decoded, err := gledger.NewBlockFromCbor(block.Type, block.Cbor)
		if err != nil {
			continue
		}
		blocks = append(blocks, decoded)
	}
	if len(blocks) == 0 {
		b.Skip("no decoded blocks available")
	}

	connId := testConnId()
	ctx := blockfetch.CallbackContext{ConnectionId: connId}
	o.blockFetchMutex.Lock()
	o.blockFetchStarts[connId] = time.Now().Add(-50 * time.Millisecond)
	o.blockFetchMutex.Unlock()

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Reset fetch start each iteration so delaySeconds is
		// consistent across all iterations.
		o.blockFetchMutex.Lock()
		o.blockFetchStarts[connId] = time.Now().Add(-50 * time.Millisecond)
		o.blockFetchMutex.Unlock()

		block := blocks[i%len(blocks)]
		if err := o.blockfetchClientBlock(ctx, uint(block.Type()), block); err != nil {
			b.Fatal(err)
		}
	}
}
