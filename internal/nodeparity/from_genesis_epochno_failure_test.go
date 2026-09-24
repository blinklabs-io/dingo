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

package nodeparity

// This file covers what happens when currentEpochNo fails inside a chainsync
// callback: that the session's failure reaches RunFromGenesis's reconnect
// loop at all, and that the reconnect does not then skip the epoch the
// failing block was in.
//
// Both use genesisFakeServer (from_genesis_wiring_test.go), which answers
// ShelleyEpochNoQuery on the separately-dialed LocalStateQuery connection
// currentEpochNo opens. Failing that query server-side makes the client's
// GetEpochNo return an error wrapping protocol.ErrProtocolShuttingDown --
// the real shape of a connection dying mid-query, and the one that made
// these two defects reachable.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCallbackErr pins callbackErr's contract directly: an error carrying
// protocol.ErrProtocolShuttingDown must not come back still carrying it,
// because gouroboros' recvLoop reads that sentinel as a graceful stop and
// returns without SendError, stranding RunFromGenesis's session select.
// Every other error must keep its chain intact.
func TestCallbackErr(t *testing.T) {
	shuttingDown := fmt.Errorf(
		"GetEpochNo: %w", protocol.ErrProtocolShuttingDown,
	)
	got := callbackErr("determine current epoch at slot %d: %w", 42, shuttingDown)
	require.Error(t, got)
	assert.False(
		t,
		errors.Is(got, protocol.ErrProtocolShuttingDown),
		"callbackErr must not let ErrProtocolShuttingDown out: gouroboros "+
			"would end the session without ever signalling ErrorChan",
	)
	assert.Contains(t, got.Error(), "determine current epoch at slot 42")
	assert.Contains(t, got.Error(), protocol.ErrProtocolShuttingDown.Error())

	other := errors.New("some other failure")
	kept := callbackErr("determine current epoch at slot %d: %w", 7, other)
	assert.ErrorIs(
		t, kept, other,
		"an error that cannot be mistaken for a graceful stop keeps its chain",
	)
}

// logCollector accumulates RunFromGenesis's logf output and lets a test wait
// for a specific line, which is how these tests observe the reconnect loop
// without reaching into RunFromGenesis's closure state.
type logCollector struct {
	mu    sync.Mutex
	lines []string
	added chan struct{}
}

func newLogCollector() *logCollector {
	return &logCollector{added: make(chan struct{}, 64)}
}

func (l *logCollector) logf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
	select {
	case l.added <- struct{}{}:
	default:
	}
}

func (l *logCollector) all() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// waitFor blocks until some logged line contains substr, or timeout elapses.
func (l *logCollector) waitFor(substr string, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		for _, line := range l.all() {
			if strings.Contains(line, substr) {
				return true
			}
		}
		select {
		case <-l.added:
		case <-deadline:
			return false
		}
	}
}

// TestRunFromGenesis_EpochNoFailureDoesNotHangSession proves a failing
// currentEpochNo inside the roll-forward callback actually ends up at the
// reconnect loop.
//
// The callback's error is returned to gouroboros' recvLoop, which stops the
// protocol without calling SendError whenever the error satisfies
// errors.Is(err, protocol.ErrProtocolShuttingDown) -- exactly what
// GetEpochNo returns when its own connection dies. Wrapped with %w, that
// sentinel travels out of the callback and the session ends with nothing on
// csConn.ErrorChan(), leaving RunFromGenesis blocked in its session select
// forever: no reconnect, no further report, and no error out of the run.
//
// The observable consequence, and what this asserts, is the reconnect loop's
// own "chainsync session ended, resuming from slot N" log. Reverting
// callbackErr's flattening at the roll-forward call site (back to a plain
// fmt.Errorf with %w) makes this time out.
func TestRunFromGenesis_EpochNoFailureDoesNotHangSession(t *testing.T) {
	const magic = 42
	const blockCount = 3

	server := newGenesisFakeServer(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
	}
	// Block 1's own currentEpochNo call, in the roll-forward callback.
	server.failEpochNoOnce(server.chain.Points[1].Slot)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	logs := newLogCollector()
	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			logs.logf, nil,
		)
	}()

	// Block 0 shares its epoch with the mandatory initial rollback, so it
	// only captures the baseline and reports nothing.
	server.allowStep(t)
	// Block 1: its currentEpochNo fails, ending the session.
	server.allowStep(t)

	require.True(
		t,
		logs.waitFor("chainsync session ended, resuming from slot", 20*time.Second),
		"the failed session never reached the reconnect loop; logs: %v",
		logs.all(),
	)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_EpochNoFailureDoesNotSkipEpoch proves the reconnect
// after such a failure still checks the epoch the failing block was in.
//
// Four blocks in epochs 1, 2, 2, 3, with block 1 (the first epoch-2 block)
// failing currentEpochNo. lastPoint is already that block, so the reconnect's
// own initial RollBackward lands on it and the roll-backward callback
// resolves epoch 2 there. Assigning lastEpoch unconditionally at that point
// marks epoch 2 confirmed without ever having checked it, and the
// roll-forward callback's `epoch <= lastEpoch` guard then returns early for
// block 2 as well -- so epoch 2 is never reported at all and the run's next
// report is epoch 3.
//
// Retreating lastEpoch only when the rollback point is in an earlier epoch
// keeps the cross-boundary rollback case working while leaving epoch 2 open
// for block 2 to report. Reverting the roll-backward callback's guard to a
// bare `haveLastEpoch = true; lastEpoch = epoch` makes this fail with
// epoch 3 as the first report.
func TestRunFromGenesis_EpochNoFailureDoesNotSkipEpoch(t *testing.T) {
	const magic = 42
	const blockCount = 4

	server := newGenesisFakeServer(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 2,
		server.chain.Points[3].Slot: 3,
	}
	server.failEpochNoOnce(server.chain.Points[1].Slot)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	logs := newLogCollector()
	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			logs.logf, nil,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(20 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}

	// Block 0: baseline only, same epoch as the initial rollback.
	server.allowStep(t)
	// Block 1: first epoch-2 block, its currentEpochNo fails.
	server.allowStep(t)

	require.True(
		t,
		logs.waitFor("chainsync session ended, resuming from slot", 20*time.Second),
		"the failed session never reached the reconnect loop; logs: %v",
		logs.all(),
	)
	// The reconnect loop logs that line before its backoff sleep, so wait
	// for the replacement session to actually be feeding before stepping it.
	server.waitForSessions(t, 2)

	// Block 2 (the second epoch-2 block) and block 3 (epoch 3) are both
	// released, so a run that skips epoch 2 still produces a report rather
	// than merely stalling -- the skip then shows up as the wrong epoch
	// arriving first, which is what this asserts.
	server.allowStep(t)
	server.allowStep(t)

	first := recv()
	assert.Equal(
		t, uint64(2), first.Epoch,
		"epoch 2 was skipped: the reconnect's rollback onto the failing "+
			"block marked epoch 2 confirmed without ever checking it",
	)

	// Epoch 3 still reports, so the retreat guard did not stall reporting.
	second := recv()
	assert.Equal(t, uint64(3), second.Epoch)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}
