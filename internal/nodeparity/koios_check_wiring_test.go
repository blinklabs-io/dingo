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

// This file exercises CheckProtocolParams and CheckStakeDistribution
// themselves -- the real production entry points RunFromGenesis and
// cmd/node-parity call -- rather than only the extracted helpers
// (applyResolvedEra, evaluatePoolStake) koios_check_test.go already pins.
//
// Neither function is reached by any other test: nothing in either package
// calls CheckStakeDistribution, and the same is true for
// CheckProtocolParams. Reverting either function's call to its extracted
// helper in place (not the helper itself) would leave every existing test
// green, including TestApplyResolvedEra and TestEvaluatePoolStake, because
// those drive the helpers directly with synthetic inputs and never go
// through the functions that actually wire them into a real comparison.
//
// Closing that gap means driving both functions through a real
// *localstatequery.Client (GetCurrentProtocolParams and GetPoolDistr2 are
// concrete gouroboros methods; CheckStakeDistribution takes the concrete
// client type and this file substitutes no fake for it) talking to a real
// gouroboros LocalStateQuery server over a real wire connection, plus a real
// Koios client talking to an httptest.NewServer fake -- reusing the two
// conventions this repo already has for each half: incremental_harness_test.go's
// ouroboros.New(WithServer(true), WithLocalStateQueryConfig(...)) pattern for
// the LocalStateQuery side, and internal/koiosparity's httptest.NewServer
// pattern (see e.g. fetch_test.go) for the Koios side.
//
// CheckProtocolParams's client parameter is the narrower protocolParamsClient
// interface (koios_check.go), specifically so
// TestCheckProtocolParams_PropagatesExplicitEraQueryError below can
// substitute fakeProtocolParamsClient and decouple GetCurrentProtocolParams
// succeeding from the later, explicit GetCurrentEra call failing -- a
// combination no real *localstatequery.Client can ever produce, because it
// caches its resolved era on first success and never re-queries the wire
// afterward (see protocolParamsClient's own doc comment). The other two
// CheckProtocolParams tests below still exercise it over a real wire
// connection, since neither needs that decoupling.
//
// wiringFakeLSQServer is a smaller, purpose-built fake rather than a reuse of
// incremental_harness_test.go's fakeLSQState: that one always answers
// GetCurrentProtocolParams with a *conway.ConwayProtocolParameters and has no
// GetPoolDistr2 support at all, neither of which fits what these two tests
// need (a Shelley-shaped protocol-parameter reply to exercise the
// Shelley/Allegra type-alias ambiguity, and a GetPoolDistr2 reply for the
// stake side).

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wiringFakeLSQServer is a minimal real gouroboros LocalStateQuery server
// (no ChainSync configured -- neither test under it ever calls ChainSync,
// matching dial.go's own client-side omission) answering exactly the three
// query types CheckProtocolParams and CheckStakeDistribution need:
// HardForkCurrentEraQuery, ShelleyCurrentProtocolParamsQuery, and
// ShelleyPoolDistr2Query.
type wiringFakeLSQServer struct {
	mu    sync.Mutex
	eraID int
	// eraErr, once set (setEraErr), replaces every future
	// HardForkCurrentEraQuery reply with itself -- see
	// TestCheckProtocolParams_FailsWhenEraQueryFails for why this
	// necessarily also fails GetCurrentProtocolParams's own embedded era
	// lookup, not only CheckProtocolParams's later explicit GetCurrentEra
	// call.
	eraErr         error
	protocolParams *shelley.ShelleyProtocolParameters
	poolDistr      *localstatequery.PoolDistr2Result

	// killConnOnNextPoolDistr, when set (killNextPoolDistr), closes the
	// connection a ShelleyPoolDistr2Query arrives on instead of answering it
	// -- reproducing dingo#1900's confirmed live failure shape (the shared
	// connection between CheckProtocolParams and CheckStakeDistribution
	// dying mid-sequence) directly, rather than fabricating an
	// application-level error a real server could never actually send this
	// way: gouroboros's client only ever returns protocol.ErrProtocolShuttingDown
	// (or a raw EOF/closed-connection error) once its own connection is
	// already gone (dial.go's doc comment), never as a decoded query reply.
	// activeConn is the connection the most recent query arrived on, so the
	// handler can close exactly that one.
	killConnOnNextPoolDistr atomic.Bool
	// killAllPoolDistr, when true, closes the connection on every single
	// ShelleyPoolDistr2Query -- unlike killConnOnNextPoolDistr, this never
	// self-clears, simulating sustained connection churn a bounded retry
	// budget cannot outlast (as opposed to the one-off death
	// killNextPoolDistr models).
	killAllPoolDistr atomic.Bool
	activeConn       atomic.Pointer[ouroboros.Connection]
}

// killNextPoolDistr arms killConnOnNextPoolDistr -- see that field's doc
// comment.
func (s *wiringFakeLSQServer) killNextPoolDistr() {
	s.killConnOnNextPoolDistr.Store(true)
}

// alwaysKillPoolDistr arms killAllPoolDistr -- see that field's doc comment.
func (s *wiringFakeLSQServer) alwaysKillPoolDistr() {
	s.killAllPoolDistr.Store(true)
}

// newWiringFakeLSQServer defaults eraID to Conway, matching koios_check.go's
// own doc comment that stake distribution has no era-specific decode path --
// only the protocol-params test needs to override it to exercise the
// Shelley/Allegra ambiguity.
func newWiringFakeLSQServer() *wiringFakeLSQServer {
	return &wiringFakeLSQServer{eraID: int(conway.EraIdConway)}
}

func (s *wiringFakeLSQServer) setEra(eraID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eraID = eraID
}

func (s *wiringFakeLSQServer) setEraErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eraErr = err
}

func (s *wiringFakeLSQServer) setProtocolParams(pp *shelley.ShelleyProtocolParameters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocolParams = pp
}

func (s *wiringFakeLSQServer) setPoolDistr(pd *localstatequery.PoolDistr2Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poolDistr = pd
}

func (s *wiringFakeLSQServer) snapshot() (
	int, error, *shelley.ShelleyProtocolParameters, *localstatequery.PoolDistr2Result,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eraID, s.eraErr, s.protocolParams, s.poolDistr
}

// config builds the localstatequery.Config a real gouroboros server uses to
// answer GetCurrentEra, GetCurrentProtocolParams, and GetPoolDistr2 --
// mirroring incremental_harness_test.go's fakeLSQState.config() dispatch
// convention (BlockQuery -> HardForkQuery/ShelleyQuery -> leaf query type).
func (s *wiringFakeLSQServer) config() localstatequery.Config {
	return localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(
			func(
				_ localstatequery.CallbackContext,
				_ localstatequery.AcquireTarget,
				_ bool,
			) error {
				return nil
			},
		),
		localstatequery.WithQueryFunc(
			func(
				_ localstatequery.CallbackContext,
				q localstatequery.QueryWrapper,
			) (any, error) {
				eraID, eraErr, pp, poolDistr := s.snapshot()
				block, ok := q.Query.(*localstatequery.BlockQuery)
				if !ok {
					return nil, fmt.Errorf("unexpected top-level query %T", q.Query)
				}
				switch inner := block.Query.(type) {
				case *localstatequery.HardForkQuery:
					switch inner.Query.(type) {
					case *localstatequery.HardForkCurrentEraQuery:
						if eraErr != nil {
							return nil, eraErr
						}
						return eraID, nil
					default:
						return nil, fmt.Errorf("unexpected hardfork query %T", inner.Query)
					}
				case *localstatequery.ShelleyQuery:
					switch inner.Query.(type) {
					case *localstatequery.ShelleyCurrentProtocolParamsQuery:
						if pp == nil {
							return nil, fmt.Errorf("wiringFakeLSQServer: no protocol params configured")
						}
						return []any{pp}, nil
					case *localstatequery.ShelleyPoolDistr2Query:
						if s.killAllPoolDistr.Load() || s.killConnOnNextPoolDistr.CompareAndSwap(true, false) {
							if conn := s.activeConn.Load(); conn != nil {
								_ = conn.Close()
							}
							return nil, errors.New(
								"wiringFakeLSQServer: connection killed for test",
							)
						}
						if poolDistr == nil {
							return localstatequery.PoolDistr2Result{
								Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{},
							}, nil
						}
						return *poolDistr, nil
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

// serve starts this fake as a real NtC LocalStateQuery server on listener.
func (s *wiringFakeLSQServer) serve(t *testing.T, listener net.Listener, magic uint32) {
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
					ouroboros.WithLocalStateQueryConfig(s.config()),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				s.activeConn.Store(oconn)
				defer oconn.Close() //nolint:errcheck
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// dialWiringClient dials addr and Acquires the volatile tip, returning a
// real *localstatequery.Client ready for CheckProtocolParams/
// CheckStakeDistribution -- both documented as needing an already-Acquired
// client.
func dialWiringClient(
	t *testing.T, ctx context.Context, addr string, magic uint32,
) *localstatequery.Client {
	t.Helper()
	conn, err := Dial(ctx, addr, magic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	lsq := conn.LocalStateQuery()
	require.NotNil(t, lsq)
	require.NotNil(t, lsq.Client)
	require.NoError(t, lsq.Client.AcquireVolatileTip())
	return lsq.Client
}

// newWiringShelleyProtocolParams returns a fully-populated
// *shelley.ShelleyProtocolParameters -- every *cbor.Rat field non-nil, since
// cbor.Rat.MarshalCBOR panics on a nil underlying *big.Rat rather than
// encoding it as CBOR null (matching incremental_harness_test.go's own
// newFakeProtocolParams doc comment on the identical hazard for Conway).
func newWiringShelleyProtocolParams() *shelley.ShelleyProtocolParameters {
	return &shelley.ShelleyProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               100,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		Decentralization:   &cbor.Rat{Rat: big.NewRat(0, 1)},
		ProtocolMajor:      3,
		ProtocolMinor:      0,
		MinUtxoValue:       1000000,
		MinPoolCost:        340000000,
	}
}

// TestCheckProtocolParams_AppliesWireResolvedEraOverAmbiguousGuess drives
// CheckProtocolParams itself -- not applyResolvedEra directly -- over a real
// *localstatequery.Client against a real gouroboros LocalStateQuery server,
// and a real *koiosparity.KoiosClient against an httptest Koios fake,
// exploiting the exact ambiguity applyResolvedEra exists to resolve:
// allegra.AllegraProtocolParameters is a type alias for
// shelley.ShelleyProtocolParameters, so a client decoding Allegra-era
// protocol params gets back a value ProtocolParamsFromNative's type switch
// alone cannot distinguish from genuine Shelley -- it always guesses
// "Shelley" (see koios_check.go's own doc comment on this exact scenario).
//
// The fake LocalStateQuery server reports the wire-authoritative era as
// Allegra (HardForkCurrentEraQuery) while replying to
// GetCurrentProtocolParams with Shelley-shaped params (the same value
// gouroboros would decode for either era). The fake Koios server reports
// "Allegra" for /epoch_params. If CheckProtocolParams's call to
// applyResolvedEra actually wires the authoritative era into the comparison,
// dingoParams.EraName becomes "Allegra" and CompareEpochProtocolParams finds
// no pparams_era disagreement. If that call site is bypassed, the ambiguous
// "Shelley" guess survives untouched and a real pparams_era
// CategoryValueMismatch appears, which this test fails on.
func TestCheckProtocolParams_AppliesWireResolvedEraOverAmbiguousGuess(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(107)

	lsq := newWiringFakeLSQServer()
	lsq.setEra(int(allegra.EraIdAllegra))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/epoch_params" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(
				w, `[{"epoch_no":%d,"era":"Allegra"}]`, epoch,
			)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(ctx, client, koios, nil, "preview", epoch)
	require.NoError(t, err)
	for _, m := range mismatches {
		if m.Field == "pparams_era" {
			t.Fatalf(
				"unexpected pparams_era mismatch: dingo=%q koios=%q -- "+
					"CheckProtocolParams did not apply the wire-resolved "+
					"era over ProtocolParamsFromNative's ambiguous "+
					"type-inferred guess",
				m.DingoValue, m.KoiosValue,
			)
		}
	}
}

// TestCheckStakeDistribution_DetectsRealPoolStakeDivergence drives
// CheckStakeDistribution itself -- not evaluatePoolStake directly -- over a
// real *localstatequery.Client's GetPoolDistr2 reply and a real
// *koiosparity.KoiosClient's /pool_history reply, with the two sides
// deliberately disagreeing on one pool's active stake. If
// CheckStakeDistribution's per-pool goroutine still calls evaluatePoolStake
// on the real GetPoolDistr2/pool_history data, the disagreement surfaces as
// a StakeMismatch with the exact expected fields. If that call site is
// bypassed (the reverted call site this test proves against), the real
// divergence is silently dropped and this test fails on an empty result.
func TestCheckStakeDistribution_DetectsRealPoolStakeDivergence(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(500)
	const dingoStake = uint64(1_000_000)
	const koiosStake = "400000"

	var poolID ledger.PoolId
	poolID[0] = 0xAB
	poolID[27] = 0xCD
	bech32 := poolID.String()

	lsq := newWiringFakeLSQServer()
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 2)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/pool_history" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(
				w,
				`[{"epoch_no":%d,"active_stake":"%s"}]`,
				epoch, koiosStake,
			)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	mismatches, err := CheckStakeDistribution(ctx, client, koios, nil, "preview", epoch)
	require.NoError(t, err)
	require.Len(
		t, mismatches, 1,
		"expected exactly the injected dingo/koios stake divergence, got %+v",
		mismatches,
	)
	got := mismatches[0]
	assert.Equal(t, bech32, got.PoolIDBech32)
	assert.Equal(t, dingoStake, got.DingoStake)
	assert.Equal(t, koiosStake, got.KoiosStake)
	assert.Equal(t, int64(600_000), got.DiffLovelace)
	assert.Empty(t, got.Reason)
	assert.False(t, got.KoiosFault)
}

// TestCheckProtocolParams_FailsWhenEraQueryFails drives CheckProtocolParams
// itself over a real *localstatequery.Client whose wire-level
// HardForkCurrentEraQuery always fails, proving CheckProtocolParams returns
// an error with nil mismatches -- never a result built on
// ProtocolParamsFromNative's ambiguous type-inferred guess -- when Dingo's
// era cannot be resolved at all. TestApplyResolvedEra already pins
// applyResolvedEra itself (the helper `if err := applyResolvedEra(...); err
// != nil` at koios_check.go delegates to) with plain values.
//
// This test does NOT isolate that call site specifically: a wire-level
// HardForkCurrentEraQuery failure necessarily fails
// client.GetCurrentProtocolParams's own embedded era lookup first
// (gouroboros's Client.getCurrentEra caches the resolved era in c.currentEra
// only after a successful lookup, and GetCurrentProtocolParams calls it
// internally before CheckProtocolParams ever reaches its own explicit
// GetCurrentEra call), so this test actually observes CheckProtocolParams
// failing at "dingo protocol params query", one line above applyResolvedEra's
// own call site. Confirmed by temporarily deleting that call site's error
// check (`_ = eraID; _ = eraErr` in place of it): this test still passed
// unchanged, proving it does not regression-guard that specific line.
//
// There is no way to make only the second, explicit call fail over a real
// wire connection, and this is not merely a limitation of this test's own
// server setup: gouroboros's Client.getCurrentEra returns its cached
// c.currentEra immediately, with no wire round trip, whenever
// c.currentEra > -1, and sets it only after a query that succeeds -- never
// on failure, and never reset afterward. CheckProtocolParams's explicit
// GetCurrentEra call is only ever reached once GetCurrentProtocolParams has
// already returned successfully on that same client, which is only
// possible once its own internal getCurrentEra call has already succeeded
// and cached a value. So whenever the explicit call executes, it is
// provably a cache hit -- it cannot fail on a real client, full stop, not
// just in this test's fake-server configuration. Reaching this call site's
// error branch at all requires decoupling the two outcomes with a test
// double; see TestCheckProtocolParams_PropagatesExplicitEraQueryError,
// which does that via protocolParamsClient/fakeProtocolParamsClient and is
// the test that actually regression-guards this specific line. This test
// is kept anyway because it still pins a real, adjacent contract
// (CheckProtocolParams never fabricates a result once era resolution is
// broken) that nothing else exercises over a real wire connection.
func TestCheckProtocolParams_FailsWhenEraQueryFails(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(107)

	lsq := newWiringFakeLSQServer()
	lsq.setEraErr(errors.New("boom: era query failed"))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(ctx, client, koios, nil, "preview", epoch)
	require.Error(t, err)
	assert.Nil(t, mismatches)
}

// fakeProtocolParamsClient implements protocolParamsClient with fully
// independent GetCurrentProtocolParams/GetCurrentEra outcomes -- something
// no real *localstatequery.Client can offer, since it only ever resolves
// its era once per connection and caches it on success (see
// protocolParamsClient's own doc comment in koios_check.go). It exists
// solely for TestCheckProtocolParams_PropagatesExplicitEraQueryError.
type fakeProtocolParamsClient struct {
	pp     lcommon.ProtocolParameters
	eraID  int
	eraErr error
}

func (f *fakeProtocolParamsClient) GetCurrentProtocolParams() (lcommon.ProtocolParameters, error) {
	return f.pp, nil
}

func (f *fakeProtocolParamsClient) GetCurrentEra() (int, error) {
	return f.eraID, f.eraErr
}

// TestCheckProtocolParams_PropagatesExplicitEraQueryError pins
// koios_check.go's `eraID, eraErr := client.GetCurrentEra()` /
// `if err := applyResolvedEra(dingoParams, eraID, eraErr); err != nil`
// call site directly -- the exact thing
// TestCheckProtocolParams_FailsWhenEraQueryFails's own doc comment proves it
// cannot reach over a real wire connection, because a real
// *localstatequery.Client can never let GetCurrentProtocolParams succeed
// while a later GetCurrentEra on that same client fails (its era cache is
// set only on success and never re-queries the wire once set).
// fakeProtocolParamsClient breaks that coupling: GetCurrentProtocolParams
// always succeeds here, independent of eraErr.
//
// Reverting the call site to ignore eraErr (for example replacing it with
// a hardcoded nil while keeping a `_ = eraErr` no-op so it still compiles)
// makes applyResolvedEra apply the fake's eraID unconditionally.
// CheckProtocolParams then proceeds past era resolution, and Koios's
// /epoch_params 404 is recorded as an ordinary KoiosFault mismatch rather
// than a hard error -- CheckProtocolParams returns (mismatches, nil)
// instead of (nil, err), and require.Error below fails.
func TestCheckProtocolParams_PropagatesExplicitEraQueryError(t *testing.T) {
	const epoch = uint64(107)

	client := &fakeProtocolParamsClient{
		pp:     newWiringShelleyProtocolParams(),
		eraID:  int(conway.EraIdConway),
		eraErr: errors.New("boom: explicit era query failed"),
	}

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(
		context.Background(), client, koios, nil, "preview", epoch,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "explicit era query failed")
	assert.Nil(t, mismatches)
}
