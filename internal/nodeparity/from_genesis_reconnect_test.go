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
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

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
