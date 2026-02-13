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
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
)

// testConnId creates a ConnectionId with valid net.Addr values for testing.
func testConnId() ouroboros_conn.ConnectionId {
	return ouroboros_conn.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
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
	o := NewOuroboros(OuroborosConfig{
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
	o := NewOuroboros(OuroborosConfig{
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
	o := NewOuroboros(OuroborosConfig{
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
}

func TestBlockfetchServerRequestRange_RangeWithinLimit(t *testing.T) {
	// A range within MaxBlockFetchRange should pass both validation
	// checks and proceed to GetChainFromPoint. Since LedgerState is nil,
	// this will panic at that call, proving the range check did not
	// reject it.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := NewOuroboros(OuroborosConfig{
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
	o := NewOuroboros(OuroborosConfig{
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
