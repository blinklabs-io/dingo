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
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/test/fixtures"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
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
// epoch 4 (dingo#1900): the connection shared between CheckProtocolParams
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
// live on the from-genesis run this fix targets (dingo#1900): epochs 4
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
// (dingo#1900 follow-up): a killed or restarted process has no on-disk
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
// "not run": utxoTaintedThisEpoch was closure state inside RunFromGenesis's
// 500+ line function literal, with nothing asserting on it directly.
// utxoVerdict is that decision, lifted out so it is directly testable:
// reverting its tainted case in place (returning utxoVerdictCompare
// instead) would make the first subtest below fail.
func TestUTxOVerdict(t *testing.T) {
	someRefs := UTxOSet{"deadbeef#0": "addr|100|||"}

	t.Run("tainted always wins, even with refs available", func(t *testing.T) {
		mode, err := utxoVerdict(true, someRefs)
		assert.Equal(t, utxoVerdictTainted, mode)
		assert.ErrorIs(t, err, errUTxOTainted)
	})

	t.Run("refs available and not tainted: compare", func(t *testing.T) {
		mode, err := utxoVerdict(false, someRefs)
		assert.Equal(t, utxoVerdictCompare, mode)
		assert.NoError(t, err)
	})

	t.Run("no refs, not tainted: no baseline", func(t *testing.T) {
		mode, err := utxoVerdict(false, nil)
		assert.Equal(t, utxoVerdictNoBaseline, mode)
		assert.NoError(t, err)
	})
}

// TestApplyTxInfoResults pins flushPendingTxInfos's actual failure
// decision -- the one that sets utxoTaintedThisEpoch -- not just
// utxoVerdict, which only reads that flag: TestUTxOVerdict alone doesn't
// prove a tx_info fetch failure is what makes utxoTaintedThisEpoch true in
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

	// failEpochNoAtSlot, when non-nil, makes ShelleyEpochNoQuery fail once
	// for each slot in it whose count is still above zero, decrementing as
	// it goes -- simulating currentEpochNo's own separately-dialed
	// LocalStateQuery connection dying mid-query at an exact block. The
	// resulting client-side error wraps protocol.ErrProtocolShuttingDown,
	// which is the whole point: see
	// TestRunFromGenesis_EpochNoFailureDoesNotHangSession.
	failEpochNoAtSlot map[uint64]int
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

// findIntersect always succeeds at whatever point the client asks for (its
// own cursor), matching a fresh from-genesis session's actual FindIntersect
// call ([]pcommon.Point{startPoint}, resolveStartPoint's Origin by
// default), or at Origin if the requested point isn't on this fake's chain
// at all.
func (s *genesisFakeServer) findIntersect(
	_ chainsync.CallbackContext, points []pcommon.Point,
) (pcommon.Point, chainsync.Tip, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if len(points) > 0 {
		for i, p := range s.chain.Points {
			if p.Slot == points[0].Slot {
				s.cursor = i + 1
				return points[0], s.chain.Tips[i], nil
			}
		}
	}
	s.cursor = 0
	return csmock.OriginPoint(), s.chain.Tip(), nil
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
	s.mu.Unlock()
	select {
	case <-step:
	case <-gone:
		// A newer session has started; stop feeding this one rather than
		// consuming a step meant for its successor.
		return errors.New("chainsync session superseded")
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
				if sp, ok := target.(localstatequery.AcquireSpecificPoint); ok {
					s.mu.Lock()
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
					default:
						return nil, fmt.Errorf("unexpected hardfork query %T", inner.Query)
					}
				case *localstatequery.ShelleyQuery:
					switch inner.Query.(type) {
					case *localstatequery.ShelleyCurrentProtocolParamsQuery:
						return []any{pp}, nil
					case *localstatequery.ShelleyPoolDistr2Query:
						return localstatequery.PoolDistr2Result{
							Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{},
						}, nil
					case *localstatequery.ShelleyUtxoWholeQuery:
						s.mu.Lock()
						s.utxoWholeCalls++
						fail := s.utxoWholeCalls <= s.failUtxoWholeCount
						s.mu.Unlock()
						if fail {
							return nil, errors.New(
								"simulated transient dial failure: " +
									"can't assign requested address",
							)
						}
						return localstatequery.UTxOsResult{
							Results: map[localstatequery.UtxoId]ledger.BabbageTransactionOutput{},
						}, nil
					case *localstatequery.ShelleyEpochNoQuery:
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
// utxoTaintedThisEpoch's real lifecycle inside RunFromGenesis's own
// closures -- not just utxoVerdict/applyTxInfoResults in isolation (see
// this file's own doc comment, and dingo#4365).
//
// Every session gets one forced initial RollBackward before any real block
// (NeedsInitialRollback -- see genesisFakeServer.requestNext), to the same
// point block 0 itself lands on, so block 0's own epoch always equals the
// epoch that rollback already established and never itself starts a new
// epoch boundary -- it only captures the genesis UTxO baseline. Block 1 is
// this test's first real reported epoch (clean). The deliberate rollback
// then fires once block 1 has been delivered (rollbackAfterCursor),
// re-baselining utxoRefs from Dingo's own (fake) current answer -- which
// this PR's fix must taint, or block 2's report would trivially compare the
// re-baselined set against itself and falsely read "clean". Block 3's
// report must NOT still be tainted, proving the post-report reset ran too,
// not just that the rollback's own assignment did.
//
// Reverting the rollback callback's utxoTaintedThisEpoch = true assignment
// in from_genesis.go, or its post-report utxoTaintedThisEpoch = false
// reset, in place leaves TestUTxOVerdict and TestApplyTxInfoResults green:
// neither drives RunFromGenesis's real callbacks at all.
func TestRunFromGenesis_UTxOTaintLifecycle(t *testing.T) {
	const magic = 42
	const blockCount = 5

	server := newGenesisFakeServer(t, blockCount)
	server.rollbackAfterCursor = 1
	rollbackTo := server.chain.Points[1]
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
			nil, nil,
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
// proving flushPendingTxInfos's own "if failed { utxoTaintedThisEpoch =
// true ... }" branch (from_genesis.go) actually taints the epoch it
// belongs to. TestRunFromGenesis_UTxOTaintLifecycle above only exercises
// the rollback callback's separate utxoTaintedThisEpoch = true assignment;
// its own chain (newGenesisFakeServer's empty-body blocks) never gives
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
			nil, nil,
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
// (server.failUtxoWholeCount), simulating dingo#1900's live "can't assign
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
			nil, nil,
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
