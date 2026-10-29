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

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	csmock "github.com/blinklabs-io/ouroboros-mock/chainsync"
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
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
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
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
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

// TestRunFromGenesis_DoesNotAcquireChainOrigin proves a from-genesis run does
// not depend on acquiring the chain origin. Every session opens with a
// RollBackward to origin, and Dingo refuses to acquire that point, so asking
// it which epoch the origin is in failed the session before any block was
// checked and the reconnect loop repeated that failure indefinitely.
func TestRunFromGenesis_DoesNotAcquireChainOrigin(t *testing.T) {
	const magic = 42
	const blockCount = 3

	server := newGenesisFakeServer(t, blockCount)
	server.refuseOriginAcquire = true
	server.initialRollbackToOrigin = true
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 0,
		server.chain.Points[1].Slot: 1,
		server.chain.Points[2].Slot: 2,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
		)
	}()

	require.False(
		t,
		logs.waitFor("point too old", 2*time.Second),
		"the session ended asking Dingo to acquire the chain origin; logs: %v",
		logs.all(),
	)

	server.allowStep(t)
	server.allowStep(t)
	server.allowStep(t)

	select {
	case r := <-results:
		assert.Equal(t, uint64(1), r.Epoch)
	case <-time.After(20 * time.Second):
		t.Fatalf(
			"no epoch was reported; the run never got past the origin; logs: %v",
			logs.all(),
		)
	}

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_StartsValidatingAtFirstAnswerablePoint covers a chain
// whose earliest points Dingo cannot answer for, as preprod's Byron epochs
// are: it has no protocol parameters or stake there and Koios holds none
// either. Those blocks are skipped, and validation starts at the first point
// Dingo answers for rather than the run failing on its first block.
func TestRunFromGenesis_StartsValidatingAtFirstAnswerablePoint(t *testing.T) {
	const magic = 42
	const blockCount = 5

	server := newGenesisFakeServer(t, blockCount)
	server.initialRollbackToOrigin = true
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 0,
		server.chain.Points[1].Slot: 1,
		server.chain.Points[2].Slot: 2,
		server.chain.Points[3].Slot: 3,
		server.chain.Points[4].Slot: 4,
	}
	firstAnswerable := server.chain.Points[3].Slot
	server.refuseAcquireFn = func(p pcommon.Point) bool {
		return p.Slot < firstAnswerable
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
		)
	}()

	for range blockCount {
		server.allowStep(t)
	}

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(20 * time.Second):
			t.Fatalf("timed out waiting for an epoch result; logs: %v", logs.all())
			return EpochResult{}
		}
	}
	assert.Equal(t, uint64(3), recv().Epoch,
		"validation must start at the first point Dingo answers for")
	assert.Equal(t, uint64(4), recv().Epoch)
	assert.True(t, logs.waitFor("validation starts at slot", 5*time.Second),
		"the skipped epochs must be reported; logs: %v", logs.all())
	assert.False(t, logs.waitFor("chainsync session ended", 500*time.Millisecond),
		"skipping must not end the session; logs: %v", logs.all())

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_RefusalAfterValidationStartsStillFails proves skipping
// is limited to the start of the run. Once Dingo has answered for a point, a
// later refusal is a real fault -- a retention floor overtaking the walk, for
// instance -- and must end the session, not be skipped silently.
func TestRunFromGenesis_RefusalAfterValidationStartsStillFails(t *testing.T) {
	const magic = 42
	const blockCount = 3

	server := newGenesisFakeServer(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
	}
	refused := server.chain.Points[2].Slot
	server.refuseAcquireFn = func(p pcommon.Point) bool {
		return p.Slot == refused
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
		)
	}()

	for range blockCount {
		server.allowStep(t)
	}
	select {
	case r := <-results:
		assert.Equal(t, uint64(2), r.Epoch)
	case <-time.After(20 * time.Second):
		t.Fatalf("no epoch was reported; logs: %v", logs.all())
	}
	require.True(
		t,
		logs.waitFor("chainsync session ended", 20*time.Second),
		"a refusal after validation started was skipped; logs: %v",
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

// TestRunFromGenesis_RefusalPastEraHistoryAfterValidationStartsFails covers
// a run whose epochs the era history resolved until a slot past its bounded
// end, where the refetch fails and the per-point fallback is refused as too
// old. Validation had already started, so the refusal must end the session
// rather than consume the block unchecked and shift its epoch's checks to a
// later block.
func TestRunFromGenesis_RefusalPastEraHistoryAfterValidationStartsFails(
	t *testing.T,
) {
	t.Parallel()
	const blockCount = 6
	// Slots 100..200 step 20. The cached history covers slots 100-140 as
	// epoch 2; 160 and 180 are epoch 3 and 200 epoch 4, answered per point.
	server := newGenesisFakeServer(t, blockCount)
	server.ungated = true
	server.eraHistory = []eraSpan{
		{startSlot: 0, startEpoch: 0, endSlot: 150, epochSize: 50},
	}
	server.eraHistoryAnswers = 1
	refused := server.chain.Points[3].Slot
	server.epochBySlot = map[uint64]int{
		refused:                     3,
		server.chain.Points[4].Slot: 3,
		server.chain.Points[5].Slot: 4,
	}
	var refusals atomic.Int32
	server.refuseAcquireFn = func(p pcommon.Point) bool {
		return p.Slot == refused && refusals.Add(1) == 1
	}
	run := startGenesisRun(t, server)

	// The first block's epoch is the baseline's, so 3 is the first reported.
	assert.Equal(t, uint64(3), run.recv(t).Epoch)
	assert.Equal(t, uint64(4), run.recv(t).Epoch)

	assert.Greater(t, refusals.Load(), int32(1),
		"the refused point must be acquired again after the session ends")
	server.mu.Lock()
	sessions := server.sessions
	server.mu.Unlock()
	assert.GreaterOrEqual(t, sessions, 2)
	assert.True(
		t,
		run.logs.waitFor("chainsync session ended", time.Second),
		"logs: %v", run.logs.all(),
	)
	for _, line := range run.logs.all() {
		assert.NotContains(t, line, "validation starts at slot",
			"the refusal was skipped as if validation had not started")
	}
}

// TestIsRetryableDingoConnErr pins the exact classification
// runProtocolParamsAndStake's retry decision depends on: a connection-death
// shaped error (protocol.ErrProtocolShuttingDown, bare or wrapped, or a raw
// EOF/closed-connection error) is retryable, while a generic query-level
// failure or a context cancellation is not -- see isRetryableDingoConnErr's
// own doc comment for why conflating the two would either waste the retry
// budget on an unfixable failure or mask a real bug as "just churn."
func TestIsRetryableDingoConnErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"protocol shutting down", protocol.ErrProtocolShuttingDown, true},
		{
			"wrapped protocol shutting down",
			fmt.Errorf("query: %w", protocol.ErrProtocolShuttingDown),
			true,
		},
		{"bare EOF", io.EOF, true},
		{"wrapped EOF", fmt.Errorf("read: %w", io.EOF), true},
		{"closed network connection", net.ErrClosed, true},
		{"generic error", errors.New("boom"), false},
		{"context canceled", context.Canceled, false},
		{"context deadline exceeded", context.DeadlineExceeded, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := isRetryableDingoConnErr(c.err)
			require.Equal(t, c.want, got)
		})
	}
}

// TestRunProtocolParamsAndStake_RecoversFromMidSequenceConnDeath drives
// runProtocolParamsAndStake itself over a real *localstatequery.Client
// against a real gouroboros LocalStateQuery server that kills the connection
// the moment CheckStakeDistribution's GetPoolDistr2 call arrives -- exactly
// the failure shape confirmed live on preview's from-genesis run starting at
// epoch 4: the connection shared between CheckProtocolParams
// and CheckStakeDistribution dies in the gap between the two calls.
//
// Reverting runProtocolParamsAndStake back to a single attempt (no retry --
// the shape the code had before this fix) makes this test fail: the killed
// connection's GetPoolDistr2 call returns a connection-death error and
// nothing redials, so stakeErr is non-nil and require.NoError below fails.
// Confirmed by temporarily hardcoding protocolParamsAndStakeRetries's loop
// bound to 1 attempt and re-running this test: it fails exactly this way.
func TestRunProtocolParamsAndStake_RecoversFromMidSequenceConnDeath(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(500)
	const dingoStake = uint64(1_000_000)
	const koiosStake = "1000000"

	var poolID ledger.PoolId
	poolID[0] = 0xAB
	poolID[27] = 0xCD

	lsq := newWiringFakeLSQServer()
	lsq.setEra(int(shelley.EraIdShelley))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})
	// Arm exactly one kill: the first attempt's CheckStakeDistribution call
	// dies mid-flight; the retry's fresh connection must succeed normally.
	lsq.killNextPoolDistr()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/epoch_params":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w, `[{"epoch_no":%d,"era":"Shelley"}]`, epoch,
				)
			case "/pool_history":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w, `[{"epoch_no":%d,"active_stake":"%s"}]`,
					epoch, koiosStake,
				)
			case "/epoch_info":
				// Agrees with the single pool above, so
				// compareTotalActiveStake reports nothing and the empty
				// mismatch list asserted below stays about the reconnect.
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w, `[{"epoch_no":%d,"active_stake":"%s"}]`,
					epoch, koiosStake,
				)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	point := pcommon.NewPointOrigin()

	_, ppErr, stakeMismatches, stakeErr := runProtocolParamsAndStake(
		ctx, listener.Addr().String(), magic, point, koios, nil, "preview", epoch,
	)
	require.NoError(t, ppErr,
		"protocol params must succeed on the attempt before the kill")
	require.NoError(t, stakeErr,
		"runProtocolParamsAndStake must recover once the retry redials a fresh connection")
	require.Empty(t, stakeMismatches,
		"dingo and koios report identical stake once the retry succeeds")
}

// TestRunProtocolParamsAndStake_ExhaustedRetriesReportsOwnErrorsSeparately
// proves the retry-exhaustion path does not misattribute one check's failure
// to the other: with the fake server killing every single
// ShelleyPoolDistr2Query (a churn level a bounded retry budget cannot
// outlast, unlike the one-off death above), CheckProtocolParams succeeds on
// every attempt while CheckStakeDistribution never does. Once
// protocolParamsAndStakeRetries is exhausted, ppErr must be nil (protocol
// params genuinely never failed) and stakeErr must carry the stake check's
// own error -- never the reverse.
//
// This pins a real regression: an earlier version of the exhausted-retries
// return path collapsed both ppErr and stakeErr into one conflated
// "lastErr" value, so a successful protocol-params result was silently
// replaced with the stake check's own error once retries ran out. Confirmed
// live on the from-genesis run this fix targets: epochs 4
// onward logged "protocol params check did not run" with the *stake*
// check's exact error text ("dingo stake distribution query: protocol is
// shutting down"), even though protocol params was never actually failing.
// Reverting to that conflated-return shape in place makes this test's
// ppErr assertion fail.
func TestRunProtocolParamsAndStake_ExhaustedRetriesReportsOwnErrorsSeparately(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(600)

	lsq := newWiringFakeLSQServer()
	lsq.setEra(int(shelley.EraIdShelley))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools:            map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{},
		TotalActiveStake: 0,
	})
	// Every attempt's stake query dies; protocol params never does.
	lsq.alwaysKillPoolDistr()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	koiosURL, _ := countingKoiosServer(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/epoch_params":
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `[{"epoch_no":%d,"era":"Shelley"}]`, epoch)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	// Bounded well under this test's own timeout: protocolParamsAndStakeRetries
	// attempts at protocolParamsAndStakeRetryDelay apart.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	point := pcommon.NewPointOrigin()

	ppMismatches, ppErr, stakeMismatches, stakeErr := runProtocolParamsAndStake(
		ctx, listener.Addr().String(), magic, point, koios, nil, "preview", epoch,
	)
	require.NoError(t, ppErr,
		"protocol params succeeded on every attempt and must not inherit the stake check's failure")
	require.NotNil(t, ppMismatches,
		"a genuinely successful protocol-params comparison must still return its own (possibly empty) result")
	require.Error(t, stakeErr,
		"stake distribution never recovered and must report its own real error")
	require.Nil(t, stakeMismatches)
}

// TestResolveStartPoint pins from-genesis's resume-from-point behavior
// A killed or restarted process has no on-disk
// checkpoint of its own, so from-genesis --at-slot/--at-hash lets a caller
// that already trusts a prior run's epochs resume from that point instead
// of Origin. Reverting resolveStartPoint to always return
// pcommon.NewPointOrigin() would make the second and third subtests fail.
func TestResolveStartPoint(t *testing.T) {
	t.Run("nil resumeFrom defaults to Origin", func(t *testing.T) {
		got, err := resolveStartPoint(nil)
		require.NoError(t, err)
		assert.Equal(t, pcommon.NewPointOrigin(), got)
	})

	t.Run("valid resumeFrom resolves to that exact point", func(t *testing.T) {
		got, err := resolveStartPoint(&Tip{
			Slot: 55784796,
			Hash: "e18e33065526668044300543396ef5259764dafe4159e7038b4148dd77c4bc91",
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(55784796), got.Slot)
		assert.NotEqual(t, pcommon.NewPointOrigin(), got)
	})

	t.Run("invalid hash is rejected immediately, not left to fail later", func(t *testing.T) {
		_, err := resolveStartPoint(&Tip{
			Slot: 100,
			Hash: "not-hex",
		})
		require.Error(t, err)
	})
}

// TestUTxOVerdict pins the fix for a Koios outage during tx_info
// reconstruction being reported as a false "utxo set match" instead of
// "not run": the taint flag was closure state inside RunFromGenesis's
// 500+ line function literal, with nothing asserting on it directly.
// utxoVerdict is that decision, lifted out so it is directly testable:
// reverting its tainted case in place (returning utxoVerdictCompare
// instead) would make the first subtest below fail.
func TestUTxOVerdict(t *testing.T) {
	someRefs := UTxOSet{"deadbeef#0": "addr|100|||"}

	t.Run("tainted always wins, even with refs available", func(t *testing.T) {
		mode, err := utxoVerdict(taintCauseTxInfo, someRefs)
		assert.Equal(t, utxoVerdictTainted, mode)
		assert.ErrorIs(t, err, errUTxOTainted)
		assert.ErrorContains(t, err, taintCauseTxInfo)
	})

	t.Run("refs available and not tainted: compare", func(t *testing.T) {
		mode, err := utxoVerdict("", someRefs)
		assert.Equal(t, utxoVerdictCompare, mode)
		assert.NoError(t, err)
	})

	t.Run("no refs, not tainted: no baseline", func(t *testing.T) {
		mode, err := utxoVerdict("", nil)
		assert.Equal(t, utxoVerdictNoBaseline, mode)
		assert.NoError(t, err)
	})
}

// TestApplyTxInfoResults pins flushPendingTxInfos's actual failure
// decision -- the one that sets utxoTaintCause -- not just
// utxoVerdict, which only reads that flag: TestUTxOVerdict alone doesn't
// prove a tx_info fetch failure is what makes utxoTaintCause non-empty in
// the first place. Reverting applyTxInfoResults to always return false (as
// if every chunk always succeeded) would make the "one chunk fails" subtest
// below fail.
func TestApplyTxInfoResults(t *testing.T) {
	noopLogf := func(string, ...any) {}

	t.Run("all chunks succeed: no failure, changes applied", func(t *testing.T) {
		refs := UTxOSet{"spent#0": "addr|100|||"}
		chunks := [][]string{{"tx1"}}
		results := [][]koiosparity.KoiosTxInfoItem{
			{{
				TxHash:  "tx1",
				Inputs:  []koiosparity.KoiosTxInfoUtxoRef{{TxHash: "spent", TxIndex: 0}},
				Outputs: []koiosparity.KoiosTxInfoOutput{{TxHash: "tx1", TxIndex: 0}},
			}},
		}
		errs := []error{nil}

		failed := applyTxInfoResults(refs, chunks, results, errs, noopLogf)
		assert.False(t, failed)
		_, stillPresent := refs["spent#0"]
		assert.False(t, stillPresent, "a successful chunk's spend must be applied")
		_, created := refs["tx1#0"]
		assert.True(t, created, "a successful chunk's new output must be applied")
	})

	t.Run("one chunk fails: reported failed, its own changes not applied, others still are", func(t *testing.T) {
		refs := UTxOSet{}
		chunks := [][]string{{"tx1"}, {"tx2"}}
		results := [][]koiosparity.KoiosTxInfoItem{
			nil, // tx1's chunk failed -- no results for it
			{{TxHash: "tx2", Outputs: []koiosparity.KoiosTxInfoOutput{{TxHash: "tx2", TxIndex: 0}}}},
		}
		errs := []error{errors.New("koios stalled"), nil}

		failed := applyTxInfoResults(refs, chunks, results, errs, noopLogf)
		assert.True(t, failed,
			"a single failed chunk must taint the whole flush, even if other chunks succeeded")
		_, created := refs["tx2#0"]
		assert.True(t, created, "an independently-succeeding chunk's changes must still be applied")
	})
}

// TestNextSessionRetryDelay pins RunFromGenesis's reconnect-loop backoff
// arithmetic: this is the fix for a from-genesis run that died outright,
// 129 clean epochs in and zero real mismatches, on a one-off transient
// "connection shutdown initiated: EOF" from currentEpochNo -- a session
// error unrelated to any real UTxO/stake/protocol-params divergence, which
// RunFromGenesis's reconnect loop now retries indefinitely (reconnecting
// and resuming from the last processed point) instead of treating as fatal.
func TestNextSessionRetryDelay(t *testing.T) {
	const (
		base = 1 * time.Second
		max  = 30 * time.Second
	)

	t.Run("no progress doubles the delay", func(t *testing.T) {
		sleepFor, next := nextSessionRetryDelay(base, false, base, max)
		assert.Equal(t, base, sleepFor,
			"the attempt that just failed should sleep for its own delay, "+
				"not the doubled one")
		assert.Equal(t, 2*base, next,
			"the following attempt should back off further")
	})

	t.Run("repeated failure without progress grows toward the cap", func(t *testing.T) {
		delay := base
		for range 10 {
			_, delay = nextSessionRetryDelay(delay, false, base, max)
		}
		assert.Equal(t, max, delay,
			"backoff must not exceed maxDelay however many times it doubles")
	})

	t.Run("progress resets the backoff to base", func(t *testing.T) {
		// A session that ran for a while (grown delay from prior failures)
		// but then made real progress before failing again must not keep
		// the grown delay -- an occasional hiccup in an otherwise-healthy
		// run should reconnect quickly, not slowly.
		grown := 16 * time.Second
		sleepFor, next := nextSessionRetryDelay(grown, true, base, max)
		assert.Equal(t, base, sleepFor,
			"progress must reset the delay actually slept for, not just "+
				"the following one")
		assert.Equal(t, 2*base, next)
	})

	t.Run("progress at the cap still resets to base", func(t *testing.T) {
		sleepFor, next := nextSessionRetryDelay(max, true, base, max)
		assert.Equal(t, base, sleepFor)
		assert.Equal(t, 2*base, next)
	})
}

// genesisFakeServer combines a real ChainSync block feed (mirroring
// incremental_test.go's fakeCardanoServer -- FindIntersect, an
// initial forced RollBackward matching a real session's own
// NeedsInitialRollback behavior, a step-gated real block feed, and one
// deliberate extra RollBackward once rollbackAfterCursor blocks have been
// delivered) with the exact LocalStateQuery answers RunFromGenesis needs
// (HardForkCurrentEraQuery, ShelleyCurrentProtocolParamsQuery,
// ShelleyPoolDistr2Query, ShelleyUtxoWholeQuery, ShelleyEpochNoQuery).
//
// GetEpochNo's answer is resolved from epochBySlot, keyed by the slot most
// recently Acquired, rather than a single mutable field a test flips
// between allowStep calls: RunFromGenesis calls currentEpochNo on its own,
// separately-dialed connection from both its roll-forward and
// roll-backward callbacks, and the deliberate RollBackward below fires
// automatically on the chainsync session's own background goroutine as
// soon as the prior callback returns -- not synchronized with the test
// goroutine at all. A shared mutable "current epoch" field would race that
// goroutine's own currentEpochNo call against the test's next setEpoch.
// Keying the answer to the Acquired point instead removes the race:
// epochBySlot is fixed once, before RunFromGenesis ever starts.
type genesisFakeServer struct {
	mu                 sync.Mutex
	chain              csmock.Chain
	cursor             int
	rolledBack         bool
	rollbackTo         *pcommon.Point
	firedExtraRollback bool
	// rollbackAfterCursor delays the deliberate RollBackward until cursor
	// has advanced past it (i.e. that many real blocks have been
	// delivered), instead of firing on the very next requestNext call --
	// letting a test observe at least one genuine clean epoch report
	// before the rollback, not just epochs after it.
	rollbackAfterCursor int
	// step gates every real RollForward reply (not either RollBackward
	// branch): a test calls allowStep once per block it wants delivered,
	// so a state change (nothing here yet, but see epochBySlot) lands at a
	// precise point instead of racing an ungated server.
	step chan struct{}
	// sessionGone is closed when a new session begins (findIntersect), and
	// releases any previous session's handler still parked on step.
	//
	// Without it, a session whose client abandoned it mid-feed leaves its
	// handler blocked on the shared step channel, where it silently consumes
	// the next allowStep meant for the reconnected session -- which then
	// waits forever for a block the test believes it already released. Only
	// a test that drives a reconnect (see
	// from_genesis_test.go) can reach that state; a
	// single-session test closes nothing.
	sessionGone chan struct{}
	// sessions counts FindIntersect calls, i.e. how many chainsync sessions
	// this fake has served. waitForSessions lets a test that drives a
	// reconnect resume stepping only once the NEW session is actually
	// feeding, rather than as soon as the client logged that the old one
	// ended -- the reconnect loop logs that before its backoff sleep, so a
	// step released on the log lands on the dead session's handler.
	sessions       int
	sessionStarted chan struct{}

	epochBySlot      map[uint64]int
	lastAcquiredSlot uint64

	// failUtxoWholeCount, when nonzero, makes the first failUtxoWholeCount
	// calls to ShelleyUtxoWholeQuery (i.e. every GetUTxOWhole call, whether
	// from captureGenesisBaseline's initial capture or a later
	// re-baseline/comparison call) fail with a synthetic transient error
	// instead of answering, then succeed from the next call onward --
	// simulating captureGenesisBaseline itself failing (as opposed to a
	// rollback/tx_info-triggered re-baseline, which
	// TestRunFromGenesis_UTxOTaintLifecycle and
	// TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch already cover), for
	// TestRunFromGenesis_UTxOBaselineRetryRecovers. Zero (the default)
	// never fails, leaving every other test's behavior unchanged.
	failUtxoWholeCount int
	utxoWholeCalls     int
	// failUtxoWholeSkip lets that many GetUTxOWhole calls succeed before
	// the failUtxoWholeCount failures begin, so a failure can land on a
	// comparison rather than the genesis capture.
	failUtxoWholeSkip int

	// failAcquiresAfterStake arms, at the next ShelleyPoolDistr2Query, that
	// many failures of the specific-point Acquires that follow it: the UTxO
	// comparison's Acquire, which runs right after the stake check. The
	// Acquire callback returns a generic error, which tears the connection
	// down as Dingo's did when its snapshot open hit its deadline.
	failAcquiresAfterStake int
	acquireFailuresArmed   int

	// failEpochNoAtSlot, when non-nil, makes ShelleyEpochNoQuery fail once
	// for each slot in it whose count is still above zero, decrementing as
	// it goes -- simulating currentEpochNo's own separately-dialed
	// LocalStateQuery connection dying mid-query at an exact block. The
	// resulting client-side error wraps protocol.ErrProtocolShuttingDown,
	// which is the whole point: see
	// TestRunFromGenesis_EpochNoFailureDoesNotHangSession.
	failEpochNoAtSlot map[uint64]int

	// refuseOriginAcquire makes an Acquire of the chain origin (slot 0, no
	// hash) fail with ErrAcquireFailurePointTooOld, as Dingo does: its query
	// model reads slot 0 as "live", so it refuses an origin point rather than
	// answer the live state for it.
	refuseOriginAcquire bool

	// initialRollbackToOrigin makes a session that intersected at the origin
	// open with a RollBackward to the origin, as Dingo's ChainSync server
	// does, instead of to the first block. Off by default so the other tests'
	// epoch bookkeeping, which counts the first block as that rollback's
	// point, is unchanged.
	initialRollbackToOrigin bool

	// refuseAcquireFn, when set, makes an Acquire of any point it returns
	// true for fail with ErrAcquireFailurePointTooOld, as Dingo does for a
	// point it cannot answer for.
	refuseAcquireFn func(pcommon.Point) bool

	// tooOldAfterFirstAcquire makes the Acquires of a slot after its first
	// one fail with ErrAcquireFailurePointTooOld, that many times, as Dingo
	// does when it cannot open a read snapshot within its own wait. The
	// first Acquire is the epoch lookup, so the refusals land on the epoch
	// checks that follow it.
	tooOldAfterFirstAcquire map[uint64]int

	// utxoWhole, when non-nil, is the answer to ShelleyUtxoWholeQuery in
	// place of the empty set.
	utxoWhole map[localstatequery.UtxoId]ledger.BabbageTransactionOutput

	// eraHistory, when non-nil, is the answer to HardForkEraHistoryQuery.
	// Left nil, that query fails like any other unexpected one, so
	// RunFromGenesis resolves every epoch through its per-point fallback.
	eraHistory []eraSpan
	// eraHistoryAnswers, when nonzero, answers only that many
	// HardForkEraHistoryQuery calls; later ones fail, as when Dingo cannot
	// serve a refetch.
	eraHistoryAnswers  int
	eraHistoryAnswered int

	// acquiredSlots records the slot of every specific-point Acquire
	// attempt, refused or not; volatileTipAcquires and epochNoQueries count
	// tip Acquires and ShelleyEpochNoQuery calls.
	acquiredSlots       []uint64
	volatileTipAcquires int
	epochNoQueries      int

	// ungated makes requestNext answer every RollForward at once instead of
	// waiting for allowStep.
	ungated bool
	// conns holds every accepted connection by its client address, and
	// chainSyncAddr the client address of the latest chainsync session.
	conns         map[string]net.Conn
	chainSyncAddr string
	// closeChainSyncAtSlot, when set, makes the ShelleyEpochNoQuery for a
	// point Acquired at this slot close the chainsync session's connection
	// from the server side, once, before answering.
	//
	// The query runs inside the client's roll-forward callback. gouroboros'
	// chainsync client sends RequestNext in batches of PipelineLimit and
	// sends no more until it has handled every reply, so during the callback
	// for the last reply of a batch it has nothing outstanding and is idle.
	// gouroboros reads a peer close in that state as graceful: it closes
	// ErrorChan without sending an error. A Dingo killed while node-parity
	// is busy in a callback leaves exactly that. Closing at any other block
	// leaves requests outstanding, and the close arrives as an error.
	closeChainSyncAtSlot *uint64
	// hangEpochNoAtSlot, when set, makes the ShelleyEpochNoQuery for a point
	// Acquired at this slot go unanswered, once, until hangRelease is
	// closed. It runs after closeChainSyncAtSlot, so the two together close
	// the session while its callback waits on the query.
	hangEpochNoAtSlot *uint64
	hangRelease       chan struct{}
	// stallUtxoWholeAtSlot and stallPoolDistrAtSlot, when set, make the
	// first ShelleyUtxoWholeQuery or ShelleyPoolDistr2Query for a point
	// Acquired at that slot close the chainsync session's connection and
	// then go unanswered until hangRelease is closed: Dingo dropping the
	// session while an epoch check waits on its answer.
	stallUtxoWholeAtSlot *uint64
	stallPoolDistrAtSlot *uint64
}

// takeQueryStall implements stallUtxoWholeAtSlot and stallPoolDistrAtSlot
// for the one at names, returning the channel to wait on, or nil when this
// query is answered.
func (s *genesisFakeServer) takeQueryStall(at **uint64) <-chan struct{} {
	s.mu.Lock()
	if *at == nil || **at != s.lastAcquiredSlot {
		s.mu.Unlock()
		return nil
	}
	*at = nil
	conn := s.conns[s.chainSyncAddr]
	release := s.hangRelease
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	return release
}

// takeEpochNoHang implements hangEpochNoAtSlot, returning the channel to
// wait on, or nil when this query is answered.
func (s *genesisFakeServer) takeEpochNoHang() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hangEpochNoAtSlot == nil ||
		*s.hangEpochNoAtSlot != s.lastAcquiredSlot {
		return nil
	}
	s.hangEpochNoAtSlot = nil
	return s.hangRelease
}

// closeChainSyncIfArmed implements closeChainSyncAtSlot.
func (s *genesisFakeServer) closeChainSyncIfArmed() {
	s.mu.Lock()
	if s.closeChainSyncAtSlot == nil ||
		*s.closeChainSyncAtSlot != s.lastAcquiredSlot {
		s.mu.Unlock()
		return
	}
	s.closeChainSyncAtSlot = nil
	conn := s.conns[s.chainSyncAddr]
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// queryCounts snapshots this fake's Acquire and GetEpochNo bookkeeping.
func (s *genesisFakeServer) queryCounts() (
	acquiredSlots []uint64, volatileTipAcquires, epochNoQueries int,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.acquiredSlots...),
		s.volatileTipAcquires, s.epochNoQueries
}

// eraHistoryAnswer encodes spans the way Dingo answers
// HardForkEraHistoryQuery.
func eraHistoryAnswer(spans []eraSpan) any {
	ret := make(cbor.IndefLengthList, 0, len(spans))
	for _, sp := range spans {
		var end any
		if !sp.unbounded {
			endEpoch := sp.startEpoch +
				(sp.endSlot-sp.startSlot)/sp.epochSize
			end = []any{big.NewInt(0), sp.endSlot, endEpoch}
		}
		ret = append(ret, []any{
			[]any{big.NewInt(0), sp.startSlot, sp.startEpoch},
			end,
			[]any{sp.epochSize, uint(1000), []any{0, 0, []any{0}}, 0},
		})
	}
	return ret
}

// failEpochNoOnce arranges for the next ShelleyEpochNoQuery Acquired at slot
// to fail, once.
func (s *genesisFakeServer) failEpochNoOnce(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failEpochNoAtSlot == nil {
		s.failEpochNoAtSlot = make(map[uint64]int, 1)
	}
	s.failEpochNoAtSlot[slot]++
}

// takeEpochNoFailure reports whether this ShelleyEpochNoQuery, for the most
// recently Acquired slot, is one of the failures failEpochNoOnce armed.
func (s *genesisFakeServer) takeEpochNoFailure() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failEpochNoAtSlot[s.lastAcquiredSlot] <= 0 {
		return false
	}
	s.failEpochNoAtSlot[s.lastAcquiredSlot]--
	return true
}

// takeTooOldAfterFirst reports whether this Acquire of slot is one of the
// refusals tooOldAfterFirstAcquire armed. acquiredSlots already holds it.
func (s *genesisFakeServer) takeTooOldAfterFirst(slot uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tooOldAfterFirstAcquire[slot] <= 0 {
		return false
	}
	if slices.Index(s.acquiredSlots, slot) == len(s.acquiredSlots)-1 {
		return false
	}
	s.tooOldAfterFirstAcquire[slot]--
	return true
}

func newGenesisFakeServer(t *testing.T, blockCount int) *genesisFakeServer {
	t.Helper()
	chain, err := csmock.BuildChain(1, ledger.Blake2b256{}, 100, 20, blockCount)
	require.NoError(t, err)
	return &genesisFakeServer{
		chain:          chain,
		step:           make(chan struct{}),
		sessionStarted: make(chan struct{}, 16),
	}
}

// waitForSessions blocks until this fake has served at least n chainsync
// sessions (FindIntersect calls).
func (s *genesisFakeServer) waitForSessions(t *testing.T, n int) {
	t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		s.mu.Lock()
		got := s.sessions
		s.mu.Unlock()
		if got >= n {
			return
		}
		select {
		case <-s.sessionStarted:
		case <-deadline:
			t.Fatalf("only %d chainsync sessions started, wanted %d", got, n)
		}
	}
}

// newGenesisFakeServerWithTransactions is like newGenesisFakeServer, but its
// chain carries one real transaction per block
// (internal/test/fixtures.GenerateConwayChainWithTransactions) instead of
// newGenesisFakeServer's empty-body csmock.BuildChain blocks. RunFromGenesis's
// roll-forward callback only ever appends to pendingTxHashes from a block's
// own Transactions() (see from_genesis.go), so an empty-body chain never
// gives flushPendingTxInfos anything to fetch at all -- its own
// tx_info-chunk-failure taint path (the "if failed" branch inside
// flushPendingTxInfos, distinct from the rollback-triggered taint
// TestRunFromGenesis_UTxOTaintLifecycle already covers) goes completely
// unexercised by every test built on newGenesisFakeServer. See
// TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch.
func newGenesisFakeServerWithTransactions(
	t *testing.T, blockCount int,
) *genesisFakeServer {
	t.Helper()
	blocks, err := fixtures.GenerateConwayChainWithTransactions(blockCount)
	require.NoError(t, err)
	chain := csmock.Chain{
		Blocks: blocks,
		Points: make([]pcommon.Point, len(blocks)),
		Tips:   make([]chainsync.Tip, len(blocks)),
	}
	for i, block := range blocks {
		chain.Points[i] = csmock.PointOf(block)
		chain.Tips[i] = csmock.TipOf(block)
	}
	return &genesisFakeServer{
		chain:          chain,
		step:           make(chan struct{}),
		sessionStarted: make(chan struct{}, 16),
	}
}

// allowStep permits this server's next gated RollForward reply to proceed.
func (s *genesisFakeServer) allowStep(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	step := s.step
	s.mu.Unlock()
	select {
	case step <- struct{}{}:
	case <-time.After(15 * time.Second):
		t.Fatal("server did not consume the step signal in time")
	}
}

// findIntersect resumes at an offered chain point or origin. Empty tip
// probes have no possible intersection and do not start a sync session.
func (s *genesisFakeServer) findIntersect(
	ctx chainsync.CallbackContext, points []pcommon.Point,
) (pcommon.Point, chainsync.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(points) == 0 {
		return pcommon.Point{}, s.chain.Tip(),
			chainsync.ErrIntersectNotFound
	}
	if ctx.ConnectionId.RemoteAddr != nil {
		s.chainSyncAddr = ctx.ConnectionId.RemoteAddr.String()
	}
	// FindIntersect starts a session, and a real server opens every session
	// with a RollBackward (NeedsInitialRollback), not just the first one.
	// Clearing the flag here is what makes a reconnect after a failed
	// session behave like a real one -- which is the whole subject of
	// TestRunFromGenesis_EpochNoFailureDoesNotSkipEpoch. A run that never
	// reconnects calls this exactly once, so nothing else changes.
	s.rolledBack = false
	if s.sessionGone != nil {
		close(s.sessionGone)
	}
	s.sessionGone = make(chan struct{})
	s.sessions++
	select {
	case s.sessionStarted <- struct{}{}:
	default:
	}
	for i, p := range s.chain.Points {
		if p.Slot == points[0].Slot {
			s.cursor = i + 1
			return points[0], s.chain.Tips[i], nil
		}
	}
	s.cursor = 0
	return intersectOrNotFound(points, s.chain.Tip())
}

// requestNext mirrors fakeCardanoServer.requestNext's three-branch shape
// (initial forced rollback, one deliberate extra rollback, then a gated
// real block feed) -- see that function's own doc comment in
// incremental_test.go for why the initial rollback exists
// unconditionally on every session.
func (s *genesisFakeServer) requestNext(ctx chainsync.CallbackContext) error {
	s.mu.Lock()
	if !s.rolledBack {
		s.rolledBack = true
		defer s.mu.Unlock()
		if s.initialRollbackToOrigin && s.cursor == 0 {
			return ctx.Server.RollBackward(
				csmock.OriginPoint(), s.chain.Tip(),
			)
		}
		return ctx.Server.RollBackward(
			s.chain.Points[max(s.cursor-1, 0)], s.chain.Tip(),
		)
	}
	if s.rollbackTo != nil && !s.firedExtraRollback &&
		s.cursor > s.rollbackAfterCursor && s.cursor < s.chain.Len() {
		s.firedExtraRollback = true
		defer s.mu.Unlock()
		return ctx.Server.RollBackward(*s.rollbackTo, s.chain.Tip())
	}
	if s.cursor >= s.chain.Len() {
		defer s.mu.Unlock()
		return ctx.Server.AwaitReply()
	}
	step := s.step
	gone := s.sessionGone
	ungated := s.ungated
	s.mu.Unlock()
	if !ungated {
		select {
		case <-step:
		case <-gone:
			// A newer session has started; stop feeding this one rather
			// than consuming a step meant for its successor.
			return errors.New("chainsync session superseded")
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	block := s.chain.Blocks[s.cursor]
	tip := s.chain.Tips[s.cursor]
	s.cursor++
	return ctx.Server.RollForward(uint(block.Type()), block.Cbor(), tip)
}

// epochForLastAcquired returns epochBySlot's answer for the most recently
// Acquired slot -- see this type's own doc comment for why this, not a
// single mutable field, is what GetEpochNo answers from.
func (s *genesisFakeServer) epochForLastAcquired() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.epochBySlot[s.lastAcquiredSlot]
}

// lsqConfig answers exactly the query types RunFromGenesis needs:
// HardForkCurrentEraQuery (always Conway -- this test has no need for the
// Shelley/Allegra ambiguity koios_check_test.go exercises),
// ShelleyCurrentProtocolParamsQuery, ShelleyPoolDistr2Query (no pools --
// CheckStakeDistribution then never calls Koios's /pool_history at all, see
// its own doc comment), ShelleyUtxoWholeQuery (always an empty set -- this
// file only cares whether the UTxO comparison ran, tainted or not, never
// about its content), and ShelleyEpochNoQuery.
func (s *genesisFakeServer) lsqConfig() localstatequery.Config {
	pp := newFakeProtocolParams()
	return localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(
			func(
				_ localstatequery.CallbackContext,
				target localstatequery.AcquireTarget,
				_ bool,
			) error {
				if _, ok := target.(localstatequery.AcquireVolatileTip); ok {
					s.mu.Lock()
					s.volatileTipAcquires++
					s.mu.Unlock()
				}
				if sp, ok := target.(localstatequery.AcquireSpecificPoint); ok {
					s.mu.Lock()
					s.acquiredSlots = append(s.acquiredSlots, sp.Point.Slot)
					s.mu.Unlock()
					if s.refuseOriginAcquire &&
						sp.Point.Slot == 0 && len(sp.Point.Hash) == 0 {
						return localstatequery.ErrAcquireFailurePointTooOld
					}
					if s.refuseAcquireFn != nil && s.refuseAcquireFn(sp.Point) {
						return localstatequery.ErrAcquireFailurePointTooOld
					}
					if s.takeTooOldAfterFirst(sp.Point.Slot) {
						return localstatequery.ErrAcquireFailurePointTooOld
					}
					s.mu.Lock()
					if s.acquireFailuresArmed > 0 {
						s.acquireFailuresArmed--
						s.mu.Unlock()
						return errors.New(
							"simulated open ledger snapshot: " +
								"context deadline exceeded",
						)
					}
					s.lastAcquiredSlot = sp.Point.Slot
					s.mu.Unlock()
				}
				return nil
			},
		),
		localstatequery.WithQueryFunc(
			func(
				_ localstatequery.CallbackContext,
				q localstatequery.QueryWrapper,
			) (any, error) {
				block, ok := q.Query.(*localstatequery.BlockQuery)
				if !ok {
					return nil, fmt.Errorf("unexpected top-level query %T", q.Query)
				}
				switch inner := block.Query.(type) {
				case *localstatequery.HardForkQuery:
					switch inner.Query.(type) {
					case *localstatequery.HardForkCurrentEraQuery:
						return int(ledger.EraIdConway), nil
					case *localstatequery.HardForkEraHistoryQuery:
						s.mu.Lock()
						spans := s.eraHistory
						if s.eraHistoryAnswers > 0 &&
							s.eraHistoryAnswered >= s.eraHistoryAnswers {
							spans = nil
						}
						s.eraHistoryAnswered++
						s.mu.Unlock()
						if spans == nil {
							return nil, errors.New(
								"era history not configured",
							)
						}
						return eraHistoryAnswer(spans), nil
					default:
						return nil, fmt.Errorf("unexpected hardfork query %T", inner.Query)
					}
				case *localstatequery.ShelleyQuery:
					switch inner.Query.(type) {
					case *localstatequery.ShelleyCurrentProtocolParamsQuery:
						return []any{pp}, nil
					case *localstatequery.ShelleyPoolDistr2Query:
						if release := s.takeQueryStall(
							&s.stallPoolDistrAtSlot,
						); release != nil {
							<-release
							return nil, errors.New(
								"simulated unanswered stake distribution query",
							)
						}
						s.mu.Lock()
						s.acquireFailuresArmed = s.failAcquiresAfterStake
						s.failAcquiresAfterStake = 0
						s.mu.Unlock()
						return localstatequery.StakeDistribution2Result{
							Distribution: localstatequery.PoolDistr2Result{
								Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{},
							},
						}, nil
					case *localstatequery.ShelleyUtxoWholeQuery:
						if release := s.takeQueryStall(
							&s.stallUtxoWholeAtSlot,
						); release != nil {
							<-release
							return nil, errors.New(
								"simulated unanswered UTxO query",
							)
						}
						s.mu.Lock()
						s.utxoWholeCalls++
						fail := s.utxoWholeCalls > s.failUtxoWholeSkip &&
							s.utxoWholeCalls <= s.failUtxoWholeSkip+s.failUtxoWholeCount
						s.mu.Unlock()
						if fail {
							return nil, errors.New(
								"simulated transient dial failure: " +
									"can't assign requested address",
							)
						}
						s.mu.Lock()
						whole := s.utxoWhole
						s.mu.Unlock()
						if whole == nil {
							whole = map[localstatequery.UtxoId]ledger.BabbageTransactionOutput{}
						}
						return localstatequery.UTxOsResult{Results: whole}, nil
					case *localstatequery.ShelleyEpochNoQuery:
						s.mu.Lock()
						s.epochNoQueries++
						s.mu.Unlock()
						s.closeChainSyncIfArmed()
						if release := s.takeEpochNoHang(); release != nil {
							<-release
							return nil, errors.New(
								"simulated unanswered epoch-number query",
							)
						}
						if s.takeEpochNoFailure() {
							return nil, errors.New(
								"simulated epoch-number query failure",
							)
						}
						return []any{s.epochForLastAcquired()}, nil
					default:
						return nil, fmt.Errorf("unexpected shelley query %T", inner.Query)
					}
				default:
					return nil, fmt.Errorf("unexpected block query %T", block.Query)
				}
			},
		),
		localstatequery.WithReleaseFunc(
			func(_ localstatequery.CallbackContext) error { return nil },
		),
	)
}

// serve starts this fake as a real NtC server on listener, answering both
// ChainSync (the one persistent session RunFromGenesis dials via dialRaw)
// and LocalStateQuery (the many short-lived Acquire-query-Release
// connections RunFromGenesis dials via Dial) on every accepted connection,
// exactly like a real Dingo node does.
func (s *genesisFakeServer) serve(t *testing.T, listener net.Listener, magic uint32) {
	t.Helper()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			if s.conns == nil {
				s.conns = make(map[string]net.Conn)
			}
			s.conns[conn.RemoteAddr().String()] = conn
			s.mu.Unlock()
			go func() {
				oconn, err := ouroboros.New(
					ouroboros.WithConnection(conn),
					ouroboros.WithServer(true),
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithNodeToNode(false),
					ouroboros.WithChainSyncConfig(chainsync.NewConfig(
						chainsync.WithFindIntersectFunc(s.findIntersect),
						chainsync.WithRequestNextFunc(s.requestNext),
					)),
					ouroboros.WithLocalStateQueryConfig(s.lsqConfig()),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				defer oconn.Close() //nolint:errcheck
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// TestRunFromGenesis_UTxOTaintLifecycle drives RunFromGenesis through a
// genuine RollBackward and three epoch boundaries, proving
// utxoTaintCause's real lifecycle inside RunFromGenesis's own
// closures -- not just utxoVerdict/applyTxInfoResults in isolation (see
// this file's own doc comment).
//
// Every session gets one forced initial RollBackward before any real block
// (NeedsInitialRollback -- see genesisFakeServer.requestNext), to the same
// point block 0 itself lands on, so block 0's own epoch always equals the
// epoch that rollback already established and never itself starts a new
// epoch boundary -- it only captures the genesis UTxO baseline. Block 1 is
// this test's first real reported epoch (clean). The deliberate rollback
// to block 0 then fires once block 1 has been delivered
// (rollbackAfterCursor), re-baselining utxoRefs from Dingo's own (fake) current answer -- which
// the fix must taint, or block 2's report would trivially compare the
// re-baselined set against itself and falsely read "clean". Block 3's
// report must NOT still be tainted, proving the post-report reset ran too,
// not just that the rollback's own assignment did.
//
// The rollback targets block 0, not block 1: a rollback to the block last
// processed invalidates nothing and must not re-baseline (see
// TestRunFromGenesis_ResumeKeepsUTxOComparison).
//
// Reverting the rollback callback's taint call in from_genesis.go, or its
// post-report utxoTaintCause = "" reset, in place leaves TestUTxOVerdict and TestApplyTxInfoResults green:
// neither drives RunFromGenesis's real callbacks at all.
func TestRunFromGenesis_UTxOTaintLifecycle(t *testing.T) {
	const magic = 42
	const blockCount = 5

	server := newGenesisFakeServer(t, blockCount)
	server.rollbackAfterCursor = 1
	rollbackTo := server.chain.Points[0]
	server.rollbackTo = &rollbackTo
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
		server.chain.Points[3].Slot: 4,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	// A 404-everything Koios fake is enough: CheckStakeDistribution never
	// calls Koios at all with zero pools (see lsqConfig's doc comment), and
	// CheckProtocolParams tolerates a Koios fetch failure as a mismatch, not
	// a ProtocolParamsErr -- this test only asserts on UTxOErr.
	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}

	// Block 0: genesis baseline capture only -- shares its epoch with the
	// mandatory initial rollback, so it never starts a new epoch boundary
	// and reports nothing.
	server.allowStep(t)

	// Block 1: first real epoch boundary, before any rollback -- must be
	// clean.
	server.allowStep(t)
	report1 := recv()
	assert.True(t, report1.UTxOAttempted)
	assert.NoError(t, report1.UTxOErr)

	// The deliberate rollback fires automatically on the chainsync
	// session's own next requestNext call, once rollbackAfterCursor blocks
	// have been delivered -- the step gate only applies to a genuine block
	// delivery, not either RollBackward branch, so nothing here needs to
	// wait for it explicitly before the next allowStep.
	server.allowStep(t)

	// Block 2: the epoch right after the rollback -- must be tainted, not
	// a false "clean" match against the just-re-baselined set.
	report2 := recv()
	assert.True(t, report2.UTxOAttempted)
	require.Error(t, report2.UTxOErr)
	assert.ErrorIs(t, report2.UTxOErr, errUTxOTainted)
	assert.ErrorContains(
		t, report2.UTxOErr, taintCauseRollback(rollbackTo.Slot),
	)

	server.allowStep(t)

	// Block 3: must NOT still be tainted -- proves the post-report reset
	// ran, not just that the rollback's own assignment did.
	report3 := recv()
	assert.True(t, report3.UTxOAttempted)
	assert.NoError(t, report3.UTxOErr)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch drives RunFromGenesis
// through a genuine Koios tx_info fetch failure -- no rollback at all --
// proving flushPendingTxInfos's own "if failed { taint(...) }" branch
// (from_genesis.go) actually taints the epoch it belongs to, and that the
// verdict names the tx_info failure as the cause.
// TestRunFromGenesis_UTxOTaintLifecycle above only exercises the rollback
// callback's separate taint call; its own chain (newGenesisFakeServer's empty-body blocks) never gives
// flushPendingTxInfos a single transaction hash to fetch, so that taint
// source's own wiring goes completely unexercised there. Reverting this
// assignment to a discarded no-op (`_ = true`) in place leaves
// TestRunFromGenesis_UTxOTaintLifecycle, TestUTxOVerdict, and
// TestApplyTxInfoResults all green.
//
// newGenesisFakeServerWithTransactions gives every block one real
// transaction. Block 0 is the mandatory genesis baseline (shares its epoch
// with the forced initial rollback, so it never itself starts a new epoch
// boundary or reports -- see TestRunFromGenesis_UTxOTaintLifecycle's own
// doc comment). Block 1 is the first real epoch boundary: its transaction
// hash is buffered into pendingTxHashes and flushed via flushPendingTxInfos
// right before that epoch's report is built. The Koios fake here 404s
// every request, including /tx_info, so that flush fails and this epoch
// must report errUTxOTainted rather than a false "clean" match against the
// re-baselined set flushPendingTxInfos falls back to on failure.
func TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch(t *testing.T) {
	const magic = 42
	const blockCount = 2

	server := newGenesisFakeServerWithTransactions(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	// A 404-everything Koios fake, same as
	// TestRunFromGenesis_UTxOTaintLifecycle -- CheckStakeDistribution never
	// calls Koios at all with zero pools, and CheckProtocolParams tolerates
	// a Koios fetch failure as a mismatch, not a ProtocolParamsErr; this
	// test only asserts on UTxOErr. The same 404 also fails GetTxInfos'
	// /tx_info call, which is the failure this test exists to taint on.
	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}

	// Block 0: genesis baseline capture only -- reports nothing.
	server.allowStep(t)

	// Block 1: first real epoch boundary, with its own transaction hash
	// flushed against a Koios server that 404s /tx_info -- must be
	// tainted, not a false "clean" match against the re-baselined set.
	server.allowStep(t)
	report := recv()
	assert.True(t, report.UTxOAttempted)
	require.Error(t, report.UTxOErr)
	assert.ErrorIs(t, report.UTxOErr, errUTxOTainted)
	assert.Equal(
		t,
		taintCauseTxInfo+": "+errUTxOTainted.Error(),
		report.UTxOErr.Error(),
	)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_UTxOBaselineRetryRecovers drives RunFromGenesis through
// a captureGenesisBaseline failure at the very first (genesis) capture --
// distinct from TestRunFromGenesis_UTxOTaintLifecycle's rollback-triggered
// re-baseline and TestRunFromGenesis_TxInfoChunkFailureTaintsEpoch's
// tx_info-chunk-triggered re-baseline, both of which already re-baseline
// successfully. This test fails the underlying GetUTxOWhole call itself
// (server.failUtxoWholeCount), simulating the live "can't assign
// requested address" transient dial error during a re-baseline attempt.
//
// Confirmed by reverting from_genesis.go's fix in place (deleting the
// `if utxoAttempted && utxoRefs == nil { ... }` retry block just before the
// UTxO verdict switch) and re-running this test: report1, report2, and
// report3 all come back with UTxOAttempted == false, proving the historical
// bug -- one failed captureGenesisBaseline call permanently disables UTxO
// comparison for every subsequent epoch of the run, never attempting to
// recover even though only the very first call was ever configured to fail.
//
// With the fix restored: block 0's genesis capture (GetUTxOWhole call #1)
// fails and reports nothing (shares its epoch with the mandatory initial
// rollback). Block 1's epoch boundary retries (call #2, which succeeds,
// since failUtxoWholeCount == 1) -- report1 must be tainted, not a false
// "clean" match against the just-recovered baseline. Block 2's epoch
// boundary finds utxoRefs already non-nil (no retry needed) and runs a real
// comparison (call #3) -- report2 must be clean, proving UTxO checking
// actually resumed rather than staying stuck. Block 3 confirms report3 is
// still clean, ruling out a one-shot fluke.
func TestRunFromGenesis_UTxOBaselineRetryRecovers(t *testing.T) {
	const magic = 42
	const blockCount = 5

	server := newGenesisFakeServer(t, blockCount)
	server.failUtxoWholeCount = 1
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
		server.chain.Points[3].Slot: 4,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	// A 404-everything Koios fake, same as the sibling tests above --
	// CheckStakeDistribution never calls Koios at all with zero pools, and
	// CheckProtocolParams tolerates a Koios fetch failure as a mismatch, not
	// a ProtocolParamsErr; this test only asserts on UTxOErr.
	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}

	// Block 0: genesis baseline capture attempted and fails (GetUTxOWhole
	// call #1, the only call configured to fail) -- shares its epoch with
	// the mandatory initial rollback, so it never starts a new epoch
	// boundary and reports nothing.
	server.allowStep(t)

	// Block 1: first real epoch boundary. utxoRefs is still nil from
	// block 0's failed capture, so the fix's retry fires here (call #2,
	// which succeeds) -- must be tainted, not a false "clean" match against
	// the just-recovered baseline.
	server.allowStep(t)
	report1 := recv()
	assert.True(t, report1.UTxOAttempted)
	require.Error(t, report1.UTxOErr)
	assert.ErrorIs(t, report1.UTxOErr, errUTxOTainted)
	assert.ErrorContains(t, report1.UTxOErr, taintCauseBaseline)

	// Block 2: utxoRefs is now non-nil and untainted -- a real comparison
	// (call #3) must run and come back clean, proving checking actually
	// resumed rather than staying permanently disabled.
	server.allowStep(t)
	report2 := recv()
	assert.True(t, report2.UTxOAttempted)
	assert.NoError(t, report2.UTxOErr)

	// Block 3: still clean -- rules out a one-shot fluke.
	server.allowStep(t)
	report3 := recv()
	assert.True(t, report3.UTxOAttempted)
	assert.NoError(t, report3.UTxOErr)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_UTxOComparisonRetriesConnectionFailure proves the UTxO
// comparison survives its Dingo connection dying, as the protocol-parameter
// and stake checks already do. Live, Dingo's Acquire missed its deadline at
// an epoch boundary, the client gave up in Acquiring, and the epoch was
// reported as "utxo check did not run" with "acquire: protocol is shutting
// down".
//
// Epochs 1-4 on blocks 0-3. Block 0 captures the baseline (GetUTxOWhole call
// 1). At block 1 the comparison's GetUTxOWhole (call 2) kills its
// connection; the retry (call 3) must report a clean comparison. At block 2
// the comparison's Acquire kills its connection, as in the live failure; the
// retry must again report clean. Block 3 checks that neither retry tainted
// or re-baselined the reconstruction: one more clean comparison, and exactly
// one GetUTxOWhole per comparison plus the retried one.
func TestRunFromGenesis_UTxOComparisonRetriesConnectionFailure(t *testing.T) {
	t.Parallel()
	const magic = 42
	const blockCount = 5

	server := newGenesisFakeServer(t, blockCount)
	server.failUtxoWholeSkip = 1
	server.failUtxoWholeCount = 1
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
		server.chain.Points[3].Slot: 4,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for an epoch result")
			return EpochResult{}
		}
	}
	utxoWholeCalls := func() int {
		server.mu.Lock()
		defer server.mu.Unlock()
		return server.utxoWholeCalls
	}

	server.allowStep(t)

	server.allowStep(t)
	report1 := recv()
	assert.Equal(t, uint64(2), report1.Epoch)
	assert.True(t, report1.UTxOAttempted)
	assert.NoError(t, report1.UTxOErr, "a dropped GetUTxOWhole must be retried")
	assert.Equal(t, 3, utxoWholeCalls())

	server.mu.Lock()
	server.failAcquiresAfterStake = 1
	server.mu.Unlock()
	server.allowStep(t)
	report2 := recv()
	assert.Equal(t, uint64(3), report2.Epoch)
	assert.True(t, report2.UTxOAttempted)
	assert.NoError(t, report2.UTxOErr, "a dropped Acquire must be retried")
	assert.Equal(t, 4, utxoWholeCalls())
	server.mu.Lock()
	assert.Zero(t, server.acquireFailuresArmed, "the armed Acquire failure must have fired")
	server.mu.Unlock()

	server.allowStep(t)
	report3 := recv()
	assert.Equal(t, uint64(4), report3.Epoch)
	assert.True(t, report3.UTxOAttempted)
	assert.NoError(t, report3.UTxOErr)
	assert.Equal(t, 5, utxoWholeCalls())

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_ResumeKeepsUTxOComparison drives a chainsync session
// that dies mid-epoch and resumes. The resumed session opens with a
// RollBackward to the block last processed, which invalidates nothing: the
// reconstruction is still exact there. That epoch must still get a real
// UTxO comparison, with no extra GetUTxOWhole, and every block's transaction
// must reach /tx_info exactly once.
//
// Four blocks with one transaction each, in epochs 1, 2, 2, 3. Block 2's
// epoch lookup fails after its transaction was buffered, ending the session
// with lastPoint at block 2. Treating the reconnect's rollback as a real one
// re-baselines from Dingo, drops block 2's buffered hash, and reports epoch
// 3 as not run.
func TestRunFromGenesis_ResumeKeepsUTxOComparison(t *testing.T) {
	t.Parallel()
	const magic = 42
	const blockCount = 4

	server := newGenesisFakeServerWithTransactions(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 2,
		server.chain.Points[3].Slot: 3,
	}
	server.failEpochNoOnce(server.chain.Points[2].Slot)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	// /tx_info answers every requested hash with a transaction that neither
	// spends nor creates anything, so the reconstruction stays equal to the
	// fake's empty UTxO set, and records how often each hash was asked for.
	// Everything else 404s, as in the sibling tests.
	var (
		txInfoMu       sync.Mutex
		txInfoRequests = map[string]int{}
	)
	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/tx_info") {
				http.NotFound(w, r)
				return
			}
			var body struct {
				TxHashes []string `json:"_tx_hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			items := make([]koiosparity.KoiosTxInfoItem, 0, len(body.TxHashes))
			txInfoMu.Lock()
			for _, h := range body.TxHashes {
				txInfoRequests[h]++
				items = append(items, koiosparity.KoiosTxInfoItem{TxHash: h})
			}
			txInfoMu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		},
	))
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
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

	// Block 0: baseline (GetUTxOWhole #1). Block 1: epoch 2 boundary, a real
	// comparison (#2).
	server.allowStep(t)
	server.allowStep(t)
	report2 := recv()
	require.Equal(t, uint64(2), report2.Epoch)
	require.True(t, report2.UTxOAttempted)
	require.NoError(t, report2.UTxOErr)

	// Block 2: its transaction is buffered, then its epoch lookup fails.
	server.allowStep(t)
	require.True(
		t,
		logs.waitFor("chainsync session ended, resuming from slot", 20*time.Second),
		"the failed session never reached the reconnect loop; logs: %v",
		logs.all(),
	)
	server.waitForSessions(t, 2)

	// Block 3: epoch 3 boundary, a real comparison (#3) covering block 2's
	// transaction, which must have survived the reconnect.
	server.allowStep(t)
	report3 := recv()
	assert.Equal(t, uint64(3), report3.Epoch)
	assert.True(t, report3.UTxOAttempted)
	assert.NoError(
		t, report3.UTxOErr,
		"the reconnect's rollback to the block last processed voided "+
			"this epoch's UTxO comparison",
	)
	assert.Empty(t, report3.UTxOMissing)
	assert.Empty(t, report3.UTxOExtra)
	assert.Empty(t, report3.UTxODiffers)

	server.mu.Lock()
	utxoWholeCalls := server.utxoWholeCalls
	server.mu.Unlock()
	// A re-baseline skips the tainted epoch's comparison, so the count
	// alone would not show it; the re-baseline's own log line does.
	assert.Equal(
		t, 3, utxoWholeCalls,
		"want one baseline and two comparisons, no re-baseline",
	)
	for _, line := range logs.all() {
		assert.NotContains(t, line, "re-baselined")
	}

	txInfoMu.Lock()
	gotRequests := maps.Clone(txInfoRequests)
	txInfoMu.Unlock()
	wantRequests := map[string]int{}
	// Block 0's transaction is inside the baseline, so it is never fetched.
	for _, block := range server.chain.Blocks[1:] {
		for _, tx := range block.Transactions() {
			wantRequests[tx.Hash().String()] = 1
		}
	}
	assert.Equal(t, wantRequests, gotRequests)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_SkipUTxOCapturesNoBaselineAndFetchesNoTxInfo proves
// skipUTxO turns the UTxO half off completely: on mainnet it would otherwise
// fetch /tx_info for every transaction from genesis. No GetUTxOWhole is
// issued, no /tx_info is requested, and each epoch reports UTxOSkipped rather
// than a missing baseline.
func TestRunFromGenesis_SkipUTxOCapturesNoBaselineAndFetchesNoTxInfo(t *testing.T) {
	t.Parallel()
	const magic = 42
	const blockCount = 3

	server := newGenesisFakeServerWithTransactions(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	var txInfoRequests atomic.Int64
	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if strings.HasSuffix(r.URL.Path, "/tx_info") {
				txInfoRequests.Add(1)
			}
			http.NotFound(w, r)
		},
	))
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, true,
		)
	}()

	for _, want := range []uint64{2, 3} {
		server.allowStep(t)
		if want == 2 {
			server.allowStep(t)
		}
		select {
		case r := <-results:
			assert.Equal(t, want, r.Epoch)
			assert.True(t, r.UTxOSkipped)
			assert.False(t, r.UTxOAttempted)
			assert.NoError(t, r.UTxOErr)
		case <-time.After(20 * time.Second):
			t.Fatalf("timed out waiting for epoch %d", want)
		}
	}

	server.mu.Lock()
	utxoWholeCalls := server.utxoWholeCalls
	server.mu.Unlock()
	assert.Zero(t, utxoWholeCalls, "no baseline or comparison may query the UTxO set")
	assert.Zero(t, txInfoRequests.Load(), "no /tx_info may be fetched")
	for _, line := range logs.all() {
		assert.NotContains(t, line, "baseline")
	}

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestRunFromGenesis_ReconnectsAfterDingoClosesConnection proves a chainsync
// connection Dingo closes while ctx is live ends only the session, not the
// run. See closeChainSyncAtSlot for how the fake reaches the close that
// gouroboros reports by closing ErrorChan rather than sending an error;
// RunFromGenesis read that as a clean stop and returned nil, so a Dingo
// restart ended the whole run.
func TestRunFromGenesis_ReconnectsAfterDingoClosesConnection(t *testing.T) {
	t.Parallel()
	const magic = 42
	// Client.Sync sends one RequestNext, answered by the initial
	// RollBackward; handling that sends the first batch, answered by blocks
	// 0 to closeBlock. A later block starts epoch 3.
	const closeBlock = chainsync.DefaultPipelineLimit - 1
	const blockCount = closeBlock + 10

	server := newGenesisFakeServer(t, blockCount)
	server.ungated = true
	server.epochBySlot = make(map[uint64]int, blockCount)
	for i, p := range server.chain.Points {
		switch {
		case i == 0:
			server.epochBySlot[p.Slot] = 1
		case i < blockCount-5:
			server.epochBySlot[p.Slot] = 2
		default:
			server.epochBySlot[p.Slot] = 3
		}
	}
	closeAt := server.chain.Points[closeBlock].Slot
	server.closeChainSyncAtSlot = &closeAt

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
		)
	}()

	recv := func() EpochResult {
		t.Helper()
		select {
		case r := <-results:
			return r
		case err := <-done:
			t.Fatalf(
				"RunFromGenesis returned %v before ctx was cancelled; logs: %v",
				err, logs.all(),
			)
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for an epoch result; logs: %v", logs.all())
		}
		return EpochResult{}
	}

	// Block 1 starts epoch 2. closeBlock's epoch lookup then closes the
	// connection, so epoch 3 can only be reached by a second session.
	require.Equal(t, uint64(2), recv().Epoch)
	require.Equal(t, uint64(3), recv().Epoch)

	server.mu.Lock()
	sessions := server.sessions
	server.mu.Unlock()
	assert.GreaterOrEqual(t, sessions, 2)
	assert.True(
		t,
		logs.waitFor("chainsync session ended, resuming from slot", time.Second),
		"logs: %v", logs.all(),
	)

	select {
	case err := <-done:
		t.Fatalf("RunFromGenesis returned %v before ctx was cancelled", err)
	default:
	}

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// newEpochLookupStallServer serves a chain whose epoch 3 starts after
// closeBlock and whose epoch-number query at closeBlock goes unanswered, so
// epoch 3 is reachable only if the run gives up on that query and reconnects.
func newEpochLookupStallServer(
	t *testing.T, magic uint32,
) (server *genesisFakeServer, addr string, closeAt uint64) {
	t.Helper()
	const closeBlock = chainsync.DefaultPipelineLimit - 1
	const blockCount = closeBlock + 10

	server = newGenesisFakeServer(t, blockCount)
	server.ungated = true
	server.epochBySlot = make(map[uint64]int, blockCount)
	for i, p := range server.chain.Points {
		switch {
		case i == 0:
			server.epochBySlot[p.Slot] = 1
		case i < blockCount-5:
			server.epochBySlot[p.Slot] = 2
		default:
			server.epochBySlot[p.Slot] = 3
		}
	}
	closeAt = server.chain.Points[closeBlock].Slot
	server.hangEpochNoAtSlot = &closeAt
	server.hangRelease = make(chan struct{})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)
	t.Cleanup(func() { close(server.hangRelease) })
	return server, listener.Addr().String(), closeAt
}

// runUntilEpoch3 runs RunFromGenesis against addr until it reports epochs 2
// and 3, then cancels it and returns its log.
func runUntilEpoch3(t *testing.T, addr string, magic uint32) []string {
	t.Helper()
	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	logs := newLogCollector()
	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, addr, "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			logs.logf, nil, false,
		)
	}()

	for _, want := range []uint64{2, 3} {
		select {
		case r := <-results:
			require.Equal(t, want, r.Epoch)
		case err := <-done:
			t.Fatalf(
				"RunFromGenesis returned %v before ctx was cancelled; logs: %v",
				err, logs.all(),
			)
		case <-time.After(30 * time.Second):
			t.Fatalf(
				"timed out waiting for epoch %d; logs: %v", want, logs.all(),
			)
		}
	}
	cancel()
	err = testutil.RequireReceive(
		t, done, 10*time.Second, "RunFromGenesis exit after ctx cancellation",
	)
	assert.True(t, err == nil || errors.Is(err, context.Canceled))
	return logs.all()
}

// TestRunFromGenesis_ReconnectsWhenSessionEndsDuringEpochLookup covers Dingo
// closing the chainsync connection while the roll-forward callback waits on
// an epoch-number query Dingo never answers. The reconnect loop waits for
// that callback before starting the next session, and the query's
// connection has no query timeout, so the run stayed up with no reconnect
// and no further epoch. Ending the session must close the lookup's
// connection.
func TestRunFromGenesis_ReconnectsWhenSessionEndsDuringEpochLookup(t *testing.T) {
	t.Parallel()
	const magic = 42
	server, addr, closeAt := newEpochLookupStallServer(t, magic)
	server.closeChainSyncAtSlot = &closeAt

	logs := runUntilEpoch3(t, addr, magic)

	server.mu.Lock()
	sessions := server.sessions
	server.mu.Unlock()
	assert.GreaterOrEqual(t, sessions, 2)
	assert.True(
		t,
		slices.ContainsFunc(logs, func(line string) bool {
			return strings.Contains(line, "chainsync session ended") &&
				strings.Contains(line, errDingoClosedConnection.Error())
		}),
		"logs: %v", logs,
	)
}

// TestRunFromGenesis_ReconnectsWhenEpochLookupStalls covers an epoch-number
// query Dingo never answers while the chainsync connection stays open. The
// session is idle with nothing outstanding, so the connection never reports
// an error: without a bound on the lookup the run made no progress and never
// reconnected.
//
// Not t.Parallel: it shortens the package-level epochLookupTimeout.
func TestRunFromGenesis_ReconnectsWhenEpochLookupStalls(t *testing.T) {
	prevTimeout := epochLookupTimeout
	epochLookupTimeout = 2 * time.Second
	t.Cleanup(func() { epochLookupTimeout = prevTimeout })

	const magic = 42
	server, addr, _ := newEpochLookupStallServer(t, magic)

	logs := runUntilEpoch3(t, addr, magic)

	server.mu.Lock()
	sessions := server.sessions
	server.mu.Unlock()
	assert.GreaterOrEqual(t, sessions, 2)
	assert.True(
		t,
		slices.ContainsFunc(logs, func(line string) bool {
			return strings.Contains(line, "chainsync session ended") &&
				strings.Contains(line, errEpochLookupTimedOut.Error())
		}),
		"logs: %v", logs,
	)
}

// runWithStalledEpochCheck runs RunFromGenesis against a chain with epochs
// 1, 2, 2, 3, 3, 3 on blocks 0-5, after stall arms one of the fake's query
// stalls at block 1, the first block of epoch 2. It returns every result
// reported, the number of chainsync sessions served, and the run's log.
func runWithStalledEpochCheck(
	t *testing.T, stall func(*genesisFakeServer, *uint64),
) (results []EpochResult, sessions int, logs []string) {
	t.Helper()
	const magic = 42
	const blockCount = 6

	server := newGenesisFakeServer(t, blockCount)
	server.ungated = true
	server.epochBySlot = make(map[uint64]int, blockCount)
	for i, p := range server.chain.Points {
		switch {
		case i == 0:
			server.epochBySlot[p.Slot] = 1
		case i < 3:
			server.epochBySlot[p.Slot] = 2
		default:
			server.epochBySlot[p.Slot] = 3
		}
	}
	stallAt := server.chain.Points[1].Slot
	server.hangRelease = make(chan struct{})
	stall(server, &stallAt)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)
	t.Cleanup(func() { close(server.hangRelease) })

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	logc := newLogCollector()
	reported := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { reported <- r },
			logc.logf, nil, false,
		)
	}()

	for _, want := range []uint64{2, 3} {
		select {
		case r := <-reported:
			require.Equal(t, want, r.Epoch, "logs: %v", logc.all())
			results = append(results, r)
		case err := <-done:
			t.Fatalf(
				"RunFromGenesis returned %v before ctx was cancelled; logs: %v",
				err, logc.all(),
			)
		case <-time.After(30 * time.Second):
			t.Fatalf(
				"timed out waiting for epoch %d; logs: %v", want, logc.all(),
			)
		}
	}
	// Every block has been delivered by now, so no further boundary can
	// produce a result: anything else would be a second report of one.
	testutil.RequireNoReceive(
		t, reported, 200*time.Millisecond, "a duplicate epoch report",
	)
	cancel()
	err = testutil.RequireReceive(
		t, done, 10*time.Second, "RunFromGenesis exit after ctx cancellation",
	)
	assert.True(t, err == nil || errors.Is(err, context.Canceled))

	server.mu.Lock()
	sessions = server.sessions
	assert.Nil(t, server.stallUtxoWholeAtSlot, "the UTxO stall must have fired")
	assert.Nil(t, server.stallPoolDistrAtSlot, "the stake stall must have fired")
	server.mu.Unlock()
	return results, sessions, logc.all()
}

// assertEpochChecksInterruptedAndRerun asserts that the session ending
// during epoch 2's checks reported neither a cancellation nor a dropped
// connection for any of them, and that the checks ran again: epoch 2 is
// reported once, with the same outcome as the uninterrupted epoch 3.
func assertEpochChecksInterruptedAndRerun(
	t *testing.T, results []EpochResult, sessions int, logs []string,
) {
	t.Helper()
	require.Len(t, results, 2)
	epoch2, epoch3 := results[0], results[1]
	assert.GreaterOrEqual(t, sessions, 2)
	assert.True(
		t,
		slices.ContainsFunc(logs, func(line string) bool {
			return strings.Contains(line, errEpochChecksInterrupted.Error()+
				": epoch 2 at slot ")
		}),
		"logs: %v", logs,
	)
	for _, err := range []error{
		epoch2.ProtocolParamsErr, epoch2.StakeErr, epoch2.UTxOErr,
	} {
		assert.NotErrorIs(t, err, context.Canceled)
		assert.False(t, isRetryableDingoConnErr(err), "error: %v", err)
	}
	assert.True(t, epoch2.UTxOAttempted)
	assert.NoError(t, epoch2.UTxOErr, "the UTxO comparison must match")
	assert.Empty(t, epoch2.UTxOMissing)
	assert.Empty(t, epoch2.UTxOExtra)
	assert.Empty(t, epoch2.UTxODiffers)
	assert.Equal(t, epoch3.ProtocolParamsErr == nil, epoch2.ProtocolParamsErr == nil)
	assert.Equal(t, epoch3.StakeErr == nil, epoch2.StakeErr == nil)
	assert.Equal(t, epoch3.UTxOAttempted, epoch2.UTxOAttempted)
	assert.NoError(t, epoch3.UTxOErr)
}

// TestRunFromGenesis_ReconnectsWhenSessionEndsDuringUTxOComparison covers
// Dingo closing the chainsync connection while the roll-forward callback
// waits on an epoch's whole-UTxO query Dingo never answers. The comparison
// ran under the run's context, which a session ending does not cancel, so
// the reconnect loop's wait for the callback never ended. The epoch must not
// be reported from the cut-off comparison, and must not be lost: the next
// session checks it again.
func TestRunFromGenesis_ReconnectsWhenSessionEndsDuringUTxOComparison(
	t *testing.T,
) {
	t.Parallel()
	results, sessions, logs := runWithStalledEpochCheck(
		t, func(s *genesisFakeServer, slot *uint64) {
			s.stallUtxoWholeAtSlot = slot
		},
	)
	assertEpochChecksInterruptedAndRerun(t, results, sessions, logs)
}

// TestRunFromGenesis_ReconnectsWhenSessionEndsDuringStakeCheck is the same
// for the stake distribution query, which shares the protocol-parameters
// check's connection.
func TestRunFromGenesis_ReconnectsWhenSessionEndsDuringStakeCheck(
	t *testing.T,
) {
	t.Parallel()
	results, sessions, logs := runWithStalledEpochCheck(
		t, func(s *genesisFakeServer, slot *uint64) {
			s.stallPoolDistrAtSlot = slot
		},
	)
	assertEpochChecksInterruptedAndRerun(t, results, sessions, logs)
}

// TestRunFromGenesis_EpochChecksRetryTooOldRefusal covers Dingo refusing an
// epoch check's Acquire as too old for the block it just delivered. Live,
// Dingo answers that when commit contention at an epoch boundary keeps it
// from opening a read snapshot within its wait, and every check of the epoch
// was reported as not run.
//
// Epochs 1-4 on blocks 0-3. At block 1 the checks' Acquires are refused three
// times and must then run. At block 2 they are refused for good, which must
// end as not run with a reason naming the exhausted retries, and the run must
// go on to check block 3.
func TestRunFromGenesis_EpochChecksRetryTooOldRefusal(t *testing.T) {
	t.Parallel()
	const magic = 42
	const blockCount = 4

	server := newGenesisFakeServer(t, blockCount)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
		server.chain.Points[2].Slot: 3,
		server.chain.Points[3].Slot: 4,
	}
	transient := server.chain.Points[1].Slot
	permanent := server.chain.Points[2].Slot
	server.tooOldAfterFirstAcquire = map[uint64]int{
		transient: 3,
		permanent: math.MaxInt,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	koiosSrv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	results := make(chan EpochResult, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- RunFromGenesis(
			ctx, listener.Addr().String(), "preview", magic, koios, nil,
			func(r EpochResult) { results <- r },
			nil, nil, false,
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
	tooOld := localstatequery.ErrAcquireFailurePointTooOld

	server.allowStep(t)

	server.allowStep(t)
	report := recv()
	assert.Equal(t, uint64(2), report.Epoch)
	assert.NotErrorIs(t, report.ProtocolParamsErr, tooOld)
	assert.NotErrorIs(t, report.StakeErr, tooOld)
	assert.True(t, report.UTxOAttempted)
	assert.NoError(t, report.UTxOErr)
	server.mu.Lock()
	assert.Zero(t, server.tooOldAfterFirstAcquire[transient],
		"the armed refusals must all have fired")
	server.mu.Unlock()

	server.allowStep(t)
	report = recv()
	assert.Equal(t, uint64(3), report.Epoch)
	for name, err := range map[string]error{
		"protocol params": report.ProtocolParamsErr,
		"stake":           report.StakeErr,
		"utxo":            report.UTxOErr,
	} {
		require.ErrorIs(t, err, tooOld, name)
		assert.ErrorContains(t, err,
			fmt.Sprintf("after %d attempts", acquireRetries), name)
	}

	server.allowStep(t)
	report = recv()
	assert.Equal(t, uint64(4), report.Epoch)
	assert.True(t, report.UTxOAttempted)
	assert.NoError(t, report.UTxOErr)

	cancel()
	select {
	case err := <-done:
		assert.True(t, err == nil || errors.Is(err, context.Canceled))
	case <-time.After(10 * time.Second):
		t.Fatal("RunFromGenesis did not exit after ctx cancellation")
	}
}

// TestDial_ReceivesUTxOWholeLargerThanIngressBudget covers a whole-UTxO-set
// reply larger than the muxer's connection-wide ingress budget. Live, the
// reply at preprod epoch 157 overran the 64MiB default and the muxer closed
// the connection with ErrIngressOverflow. The budget is shrunk to a fraction
// of one segment here, so the reply exceeds it many times over and any
// segment queued behind another overruns it.
func TestDial_ReceivesUTxOWholeLargerThanIngressBudget(t *testing.T) {
	t.Parallel()
	const magic = 42
	const utxoCount = 3000
	const ingressBudget = 1024

	server := newGenesisFakeServer(t, 1)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		make([]byte, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	server.utxoWhole = make(
		map[localstatequery.UtxoId]ledger.BabbageTransactionOutput, utxoCount,
	)
	for i := range utxoCount {
		var hash ledger.Blake2b256
		hash[0], hash[1] = byte(i>>8), byte(i)
		server.utxoWhole[localstatequery.UtxoId{Hash: hash, Idx: i}] =
			ledger.BabbageTransactionOutput{
				OutputAddress: addr,
				OutputAmount:  mary.MaryTransactionOutputValue{Amount: uint64(i) + 1},
			}
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, lsq, err := acquireWithRetry(
		ctx, listener.Addr().String(), magic, server.chain.Points[0], true,
	)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck
	conn.Muxer().SetIngressBudget(ingressBudget)

	utxos, err := lsq.Client.GetUTxOWhole()
	require.NoError(t, err)
	assert.Len(t, utxos.Results, utxoCount)
}

// txInfoChunkFake answers each chunk, by its first hash, with respond(attempt)
// and counts the attempts.
type txInfoChunkFake struct {
	mu       sync.Mutex
	attempts map[string]int
	respond  func(chunk []string, attempt int) error
}

func (f *txInfoChunkFake) fetch(
	_ context.Context, chunk []string,
) ([]koiosparity.KoiosTxInfoItem, error) {
	f.mu.Lock()
	f.attempts[chunk[0]]++
	attempt := f.attempts[chunk[0]]
	f.mu.Unlock()
	if err := f.respond(chunk, attempt); err != nil {
		return nil, err
	}
	items := make([]koiosparity.KoiosTxInfoItem, 0, len(chunk))
	for _, h := range chunk {
		items = append(items, koiosparity.KoiosTxInfoItem{TxHash: h})
	}
	return items, nil
}

func (f *txInfoChunkFake) attemptsFor(hash string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[hash]
}

func errTxNotIndexed(hash string) error {
	return fmt.Errorf("%w %s", koiosparity.ErrTxInfoMissing, hash)
}

// TestFetchTxInfoChunksRetriesATxKoiosHasNotIndexed pins the live preprod
// failure at the tip: Koios had no /tx_info result yet for a hash Dingo had
// just served, and the flush re-baselined the reconstruction. A chunk that
// fails only that way is fetched again after a wait and then applies.
func TestFetchTxInfoChunksRetriesATxKoiosHasNotIndexed(t *testing.T) {
	t.Parallel()
	chunks := [][]string{{"aa", "ab"}, {"ba", "bb"}}
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, attempt int) error {
			if chunk[0] == "ba" && attempt == 1 {
				return errTxNotIndexed("bb")
			}
			return nil
		},
	}
	waits := &recordingWait{}
	schedule := []time.Duration{time.Second, 2 * time.Second}

	results, errs, err := fetchTxInfoChunks(
		context.Background(), chunks, f.fetch, schedule, waits.wait,
		func(string, ...any) {},
	)
	require.NoError(t, err)
	assert.Equal(t, []error{nil, nil}, errs)
	assert.Equal(t, schedule[:1], waits.waits)
	assert.Equal(t, 1, f.attemptsFor("aa"), "a fetched chunk is not refetched")
	assert.Equal(t, 2, f.attemptsFor("ba"))

	refs := UTxOSet{}
	assert.False(t,
		applyTxInfoResults(refs, chunks, results, errs, t.Logf),
		"a tx Koios indexed within the waits must not taint the epoch",
	)
}

// TestFetchTxInfoChunksGivesUpAfterTheLastWait proves the waits are bounded:
// a hash still missing after the last one fails its chunk, which taints the
// epoch as before, and a failure of any other kind is not fetched again.
func TestFetchTxInfoChunksGivesUpAfterTheLastWait(t *testing.T) {
	t.Parallel()
	chunks := [][]string{{"aa"}, {"ba"}, {"ca"}}
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, _ int) error {
			switch chunk[0] {
			case "aa":
				return errTxNotIndexed("aa")
			case "ba":
				return errors.New("koios /tx_info: status 500")
			}
			return nil
		},
	}
	waits := &recordingWait{}
	schedule := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

	results, errs, err := fetchTxInfoChunks(
		context.Background(), chunks, f.fetch, schedule, waits.wait,
		func(string, ...any) {},
	)
	require.NoError(t, err)
	assert.ErrorIs(t, errs[0], koiosparity.ErrTxInfoMissing)
	assert.Error(t, errs[1])
	assert.NoError(t, errs[2])
	assert.Equal(t, schedule, waits.waits)
	assert.Equal(t, 1+len(schedule), f.attemptsFor("aa"))
	assert.Equal(t, 1, f.attemptsFor("ba"))
	assert.Equal(t, 1, f.attemptsFor("ca"))

	assert.True(t,
		applyTxInfoResults(UTxOSet{}, chunks, results, errs, t.Logf),
		"a hash still missing after the last wait must taint the epoch",
	)
}

// TestFetchTxInfoChunksStopsWaitingWhenCancelled proves a session end during
// a wait returns the cancellation at once, with no further fetch.
func TestFetchTxInfoChunksStopsWaitingWhenCancelled(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, _ int) error {
			return errTxNotIndexed(chunk[0])
		},
	}
	cancelThenWait := func(ctx context.Context, d time.Duration) error {
		cancel()
		return waitStakeRetry(ctx, d)
	}

	type result struct {
		results [][]koiosparity.KoiosTxInfoItem
		errs    []error
		err     error
	}
	done := make(chan result, 1)
	go func() {
		r, e, err := fetchTxInfoChunks(
			ctx, [][]string{{"aa"}}, f.fetch, []time.Duration{time.Hour},
			cancelThenWait, func(string, ...any) {},
		)
		done <- result{r, e, err}
	}()
	got := testutil.RequireReceive(
		t, done, 5*time.Second, "fetchTxInfoChunks after cancellation",
	)
	require.ErrorIs(t, got.err, context.Canceled)
	assert.Nil(t, got.results)
	assert.Nil(t, got.errs)
	assert.Equal(t, 1, f.attemptsFor("aa"), "no fetch runs after cancellation")
}

// TestRunFromGenesis_TxInfoIndexLagDoesNotTaintEpoch drives the whole flush:
// /tx_info first answers without the boundary block's transaction, as Koios
// does before it has indexed the block, then with it. The epoch's UTxO
// comparison must run rather than be tainted by a re-baseline.
//
// Not t.Parallel: it shortens the package-level txInfoIndexWaits.
func TestRunFromGenesis_TxInfoIndexLagDoesNotTaintEpoch(t *testing.T) {
	prevWaits := txInfoIndexWaits
	txInfoIndexWaits = []time.Duration{time.Millisecond}
	t.Cleanup(func() { txInfoIndexWaits = prevWaits })

	const magic = 42
	server := newGenesisFakeServerWithTransactions(t, 2)
	server.epochBySlot = map[uint64]int{
		server.chain.Points[0].Slot: 1,
		server.chain.Points[1].Slot: 2,
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	server.serve(t, listener, magic)

	var txInfoCalls atomic.Int32
	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if !strings.HasSuffix(r.URL.Path, "/tx_info") {
				http.NotFound(w, r)
				return
			}
			var body struct {
				TxHashes []string `json:"_tx_hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			items := []koiosparity.KoiosTxInfoItem{}
			if txInfoCalls.Add(1) > 1 {
				for _, h := range body.TxHashes {
					items = append(
						items, koiosparity.KoiosTxInfoItem{TxHash: h},
					)
				}
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(items)
		},
	))
	t.Cleanup(koiosSrv.Close)
	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
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
			logs.logf, nil, false,
		)
	}()

	server.allowStep(t)
	server.allowStep(t)
	report := testutil.RequireReceive(
		t, results, 10*time.Second, "epoch 2 result",
	)
	assert.True(t, report.UTxOAttempted)
	assert.NoError(t, report.UTxOErr,
		"a tx Koios indexed after one wait must not taint the epoch")
	assert.Equal(t, int32(2), txInfoCalls.Load())
	for _, line := range logs.all() {
		assert.NotContains(t, line, "re-baselined")
	}

	cancel()
	err = testutil.RequireReceive(
		t, done, 10*time.Second, "RunFromGenesis exit after cancellation",
	)
	assert.True(t, err == nil || errors.Is(err, context.Canceled))
}
