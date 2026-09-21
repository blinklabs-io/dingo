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
// Human review (Chris Guiney, dingo#4319) found that neither function was
// reached by any test: "nothing in either package calls
// CheckStakeDistribution at all" and the same for CheckProtocolParams. A
// follow-up verification confirmed this concretely -- reverting either
// function's call to its extracted helper in place (not the helper itself)
// left every existing test green, including TestApplyResolvedEra and
// TestEvaluatePoolStake, because those drive the helpers directly with
// synthetic inputs and never go through the functions that actually wire
// them into a real comparison.
//
// Closing that gap means driving both functions through a real
// *localstatequery.Client (GetCurrentEra, GetCurrentProtocolParams,
// GetPoolDistr2 are concrete gouroboros methods, not an interface this
// package could substitute a hand-written fake for) talking to a real
// gouroboros LocalStateQuery server over a real wire connection, plus a real
// Koios client talking to an httptest.NewServer fake -- reusing the two
// conventions this repo already has for each half: incremental_harness_test.go's
// ouroboros.New(WithServer(true), WithLocalStateQueryConfig(...)) pattern for
// the LocalStateQuery side, and internal/koiosparity's httptest.NewServer
// pattern (see e.g. fetch_test.go) for the Koios side.
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
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
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
	mu             sync.Mutex
	eraID          int
	protocolParams *shelley.ShelleyProtocolParameters
	poolDistr      *localstatequery.PoolDistr2Result
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
	int, *shelley.ShelleyProtocolParameters, *localstatequery.PoolDistr2Result,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eraID, s.protocolParams, s.poolDistr
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
				eraID, pp, poolDistr := s.snapshot()
				block, ok := q.Query.(*localstatequery.BlockQuery)
				if !ok {
					return nil, fmt.Errorf("unexpected top-level query %T", q.Query)
				}
				switch inner := block.Query.(type) {
				case *localstatequery.HardForkQuery:
					switch inner.Query.(type) {
					case *localstatequery.HardForkCurrentEraQuery:
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
// "Shelley" (human review, Chris Guiney, dingo#4319; koios_check.go's own
// doc comment on this exact scenario).
//
// The fake LocalStateQuery server reports the wire-authoritative era as
// Allegra (HardForkCurrentEraQuery) while replying to
// GetCurrentProtocolParams with Shelley-shaped params (the same value
// gouroboros would decode for either era). The fake Koios server reports
// "Allegra" for /epoch_params. If CheckProtocolParams's call to
// applyResolvedEra actually wires the authoritative era into the comparison,
// dingoParams.EraName becomes "Allegra" and CompareEpochProtocolParams finds
// no pparams_era disagreement. If that call site is bypassed (the reverted
// call site this test proves against -- see this package's PR discussion),
// the ambiguous "Shelley" guess survives untouched and a real pparams_era
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

	mismatches, err := CheckProtocolParams(ctx, client, koios, "preview", epoch)
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

	mismatches, err := CheckStakeDistribution(ctx, client, koios, epoch)
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
