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

package utxorpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	betacardano "github.com/utxorpc/go-codegen/utxorpc/v1beta/cardano"
	betaquery "github.com/utxorpc/go-codegen/utxorpc/v1beta/query"
)

// readStateLedgerStub implements only the UtxorpcLedgerState methods ReadState
// reaches. The interface is embedded rather than implemented in full so that a
// method ReadState should not be calling panics instead of silently returning a
// zero value.
type readStateLedgerStub struct {
	UtxorpcLedgerState

	tip   ochainsync.Tip
	block models.Block
	// blockErr drives the height-lookup fallback.
	blockErr error

	dist    *ledger.PoolStakeDistribution
	distErr error
	// gotFilter records what the handler passed down, including whether it was
	// nil, which is what separates "every pool" from "no pools".
	gotFilter    []lcommon.PoolKeyHash
	gotFilterNil bool
	calls        int
}

func (s *readStateLedgerStub) Tip() ochainsync.Tip { return s.tip }

func (s *readStateLedgerStub) GetBlock(ocommon.Point) (models.Block, error) {
	if s.blockErr != nil {
		return models.Block{}, s.blockErr
	}
	return s.block, nil
}

func (s *readStateLedgerStub) PoolStakeDistribution(
	poolFilter []lcommon.PoolKeyHash,
) (*ledger.PoolStakeDistribution, error) {
	s.calls++
	s.gotFilter = poolFilter
	s.gotFilterNil = poolFilter == nil
	if s.distErr != nil {
		return nil, s.distErr
	}
	return s.dist, nil
}

func newReadStateServer(
	t *testing.T,
	ls UtxorpcLedgerState,
) *betaQueryServiceServer {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState: ls,
	})
	return &betaQueryServiceServer{utxorpc: u}
}

func testPoolKeyHash(b byte) lcommon.PoolKeyHash {
	raw := make([]byte, 28)
	for i := range raw {
		raw[i] = b
	}
	return lcommon.PoolKeyHash(lcommon.NewBlake2b224(raw))
}

func testVrfKeyHash(b byte) gledger.Blake2b256 {
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = b
	}
	return gledger.Blake2b256(gledger.NewBlake2b256(raw))
}

func stakePoolDistributionRequest(
	poolKeyHashes ...[]byte,
) *connect.Request[betaquery.ReadStateRequest] {
	return connect.NewRequest(&betaquery.ReadStateRequest{
		Query: &betaquery.AnyChainStateQuery{
			Query: &betaquery.AnyChainStateQuery_Cardano{
				Cardano: &betacardano.StateQuery{
					Query: &betacardano.StateQuery_StakePoolDistribution{
						StakePoolDistribution: &betacardano.GetStakePoolDistribution{
							PoolKeyhashes: poolKeyHashes,
						},
					},
				},
			},
		},
	})
}

// TestReadState_ReportsStakePoolDistribution covers the one Cardano state query
// v1beta defines. Every field a caller acts on -- the pool it names, its share,
// and the VRF key that share is claimed for -- has to survive the mapping, so
// each is asserted rather than just the pool count.
func TestReadState_ReportsStakePoolDistribution(t *testing.T) {
	pkhA := testPoolKeyHash(0xAA)
	pkhB := testPoolKeyHash(0xBB)
	vrfA := testVrfKeyHash(0x01)
	vrfB := testVrfKeyHash(0x02)

	stub := &readStateLedgerStub{
		tip: ochainsync.Tip{
			Point: ocommon.NewPoint(1234, []byte{0xDE, 0xAD}),
		},
		block: models.Block{Slot: 1234, Hash: []byte{0xDE, 0xAD}, Number: 77},
		dist: &ledger.PoolStakeDistribution{
			SnapshotEpoch:    9,
			TotalActiveStake: 4_000_000,
			Pools: []ledger.PoolStakeShare{
				{
					PoolKeyHash:   pkhA,
					Stake:         3_000_000,
					StakeFraction: &cbor.Rat{Rat: big.NewRat(3, 4)},
					VrfKeyHash:    vrfA,
				},
				{
					PoolKeyHash:   pkhB,
					Stake:         1_000_000,
					StakeFraction: &cbor.Rat{Rat: big.NewRat(1, 4)},
					VrfKeyHash:    vrfB,
				},
			},
		},
	}

	srv := newReadStateServer(t, stub)
	resp, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest(),
	)
	require.NoError(t, err)

	pools := resp.Msg.GetResult().
		GetCardano().
		GetStakePoolDistribution().
		GetPools()
	require.Len(t, pools, 2)

	assert.Equal(t, pkhA.Bytes(), pools[0].GetPoolKeyhash())
	assert.Equal(t, vrfA.Bytes(), pools[0].GetVrfKeyhash())
	assert.Equal(t, int32(3), pools[0].GetStakeFraction().GetNumerator())
	assert.Equal(t, uint32(4), pools[0].GetStakeFraction().GetDenominator())

	assert.Equal(t, pkhB.Bytes(), pools[1].GetPoolKeyhash())
	assert.Equal(t, vrfB.Bytes(), pools[1].GetVrfKeyhash())

	// The ledger tip names the snapshot the query was evaluated against, so a
	// caller can tell two replies apart when the chain moved between them.
	tip := resp.Msg.GetLedgerTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(1234), tip.GetSlot())
	assert.Equal(t, []byte{0xDE, 0xAD}, tip.GetHash())
	assert.Equal(t, uint64(77), tip.GetHeight())
}

// TestReadState_EmptyPoolFilterMeansEveryPool pins the proto's own rule:
// "If empty, return the distribution for every pool." The ledger draws that
// distinction between a nil filter and an empty non-nil one, so passing the
// request's empty slice straight through would silently ask for no pools.
func TestReadState_EmptyPoolFilterMeansEveryPool(t *testing.T) {
	stub := &readStateLedgerStub{
		dist: &ledger.PoolStakeDistribution{},
	}
	srv := newReadStateServer(t, stub)

	_, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest(),
	)
	require.NoError(t, err)
	require.Equal(t, 1, stub.calls)
	assert.True(t, stub.gotFilterNil,
		"an empty request filter must reach the ledger as nil, which is "+
			"what it reads as every pool")
}

// TestReadState_PoolFilterIsForwarded covers the bounded form of the request.
func TestReadState_PoolFilterIsForwarded(t *testing.T) {
	pkhA := testPoolKeyHash(0xAA)
	stub := &readStateLedgerStub{dist: &ledger.PoolStakeDistribution{}}
	srv := newReadStateServer(t, stub)

	_, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest(pkhA.Bytes()),
	)
	require.NoError(t, err)
	require.False(t, stub.gotFilterNil,
		"a filter that names pools must not be flattened into every pool")
	assert.Equal(t, []lcommon.PoolKeyHash{pkhA}, stub.gotFilter)
}

// TestReadState_RejectsMalformedPoolKeyHash covers a filter entry that is not a
// pool key hash. Truncating or zero-padding it would silently query a different
// pool than the caller named.
func TestReadState_RejectsMalformedPoolKeyHash(t *testing.T) {
	stub := &readStateLedgerStub{dist: &ledger.PoolStakeDistribution{}}
	srv := newReadStateServer(t, stub)

	_, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest([]byte{0x01, 0x02, 0x03}),
	)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	assert.Zero(t, stub.calls, "a malformed request must not reach the ledger")
}

// TestReadState_RejectsMissingQuery covers a request that names no query at
// all, which cannot be answered but is not an unimplemented feature either.
func TestReadState_RejectsMissingQuery(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *betaquery.ReadStateRequest
	}{
		{
			name: "no query",
			req:  &betaquery.ReadStateRequest{},
		},
		{
			name: "no chain",
			req: &betaquery.ReadStateRequest{
				Query: &betaquery.AnyChainStateQuery{},
			},
		},
		{
			name: "no cardano query",
			req: &betaquery.ReadStateRequest{
				Query: &betaquery.AnyChainStateQuery{
					Query: &betaquery.AnyChainStateQuery_Cardano{
						Cardano: &betacardano.StateQuery{},
					},
				},
			},
		},
		{
			// The caller did select Cardano, so this is a malformed request
			// rather than a request for a chain this node does not serve.
			// GetCardano() returns nil for both, which is why the handler
			// switches on the oneof wrapper instead.
			name: "cardano selected with no message",
			req: &betaquery.ReadStateRequest{
				Query: &betaquery.AnyChainStateQuery{
					Query: &betaquery.AnyChainStateQuery_Cardano{},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stub := &readStateLedgerStub{dist: &ledger.PoolStakeDistribution{}}
			srv := newReadStateServer(t, stub)
			_, err := srv.ReadState(
				context.Background(),
				connect.NewRequest(tc.req),
			)
			require.Error(t, err)
			assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			assert.Zero(t, stub.calls)
		})
	}
}

// TestReadState_NilDistributionIsInternal covers the ledger returning neither a
// distribution nor an error. That is a ledger bug, but this handler serves a
// network listener, so it has to report it rather than dereference it.
func TestReadState_NilDistributionIsInternal(t *testing.T) {
	stub := &readStateLedgerStub{}
	srv := newReadStateServer(t, stub)

	_, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest(),
	)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

// TestReadState_NilStakePoolDistributionMessageMeansEveryPool covers the
// stake-pool-distribution variant selected with no message. proto3 reads an
// absent message as its default, so this asks for every pool rather than being
// malformed.
func TestReadState_NilStakePoolDistributionMessageMeansEveryPool(t *testing.T) {
	stub := &readStateLedgerStub{dist: &ledger.PoolStakeDistribution{}}
	srv := newReadStateServer(t, stub)

	_, err := srv.ReadState(
		context.Background(),
		connect.NewRequest(&betaquery.ReadStateRequest{
			Query: &betaquery.AnyChainStateQuery{
				Query: &betaquery.AnyChainStateQuery_Cardano{
					Cardano: &betacardano.StateQuery{
						Query: &betacardano.StateQuery_StakePoolDistribution{},
					},
				},
			},
		}),
	)
	require.NoError(t, err)
	require.Equal(t, 1, stub.calls)
	assert.True(t, stub.gotFilterNil)
}

// TestReadState_LedgerErrorIsInternal covers the ledger read failing. The
// caller's request was well formed, so this is not InvalidArgument.
func TestReadState_LedgerErrorIsInternal(t *testing.T) {
	stub := &readStateLedgerStub{distErr: errors.New("boom")}
	srv := newReadStateServer(t, stub)

	_, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest(),
	)
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}

// TestReadState_TipHeightFallsBackWhenBlockMissing covers the tip block being
// unreadable. The distribution is still the answer to the question asked, so
// the reply is served with an unknown height rather than failed.
func TestReadState_TipHeightFallsBackWhenBlockMissing(t *testing.T) {
	stub := &readStateLedgerStub{
		tip: ochainsync.Tip{
			Point: ocommon.NewPoint(4321, []byte{0xBE, 0xEF}),
		},
		blockErr: errors.New("no such block"),
		dist:     &ledger.PoolStakeDistribution{},
	}
	srv := newReadStateServer(t, stub)

	resp, err := srv.ReadState(
		context.Background(),
		stakePoolDistributionRequest(),
	)
	require.NoError(t, err)
	tip := resp.Msg.GetLedgerTip()
	require.NotNil(t, tip)
	assert.Equal(t, uint64(4321), tip.GetSlot())
	assert.Equal(t, []byte{0xBE, 0xEF}, tip.GetHash())
	assert.Equal(t, uint64(0), tip.GetHeight())
}

// TestStakeFractionToBetaRational covers the encoding of a stake fraction into
// the protobuf RationalNumber, whose numerator is an int32 and denominator a
// uint32.
//
// An exact ratio of two lovelace amounts does not generally fit: mainnet's
// active stake is on the order of 2e16 lovelace, so a reduced denominator
// routinely exceeds uint32 by six orders of magnitude. The encoder therefore
// keeps the exact ratio when it fits and otherwise rescales to a fixed
// denominator, which bounds the error at one part in a billion -- far below the
// resolution any stake decision is made at.
func TestStakeFractionToBetaRational(t *testing.T) {
	for _, tc := range []struct {
		name    string
		in      *big.Rat
		wantNum int32
		wantDen uint32
	}{
		{
			name:    "exact ratio is preserved",
			in:      big.NewRat(3, 4),
			wantNum: 3,
			wantDen: 4,
		},
		{
			name:    "zero",
			in:      big.NewRat(0, 1),
			wantNum: 0,
			wantDen: 1,
		},
		{
			name:    "whole",
			in:      big.NewRat(1, 1),
			wantNum: 1,
			wantDen: 1,
		},
		{
			name: "denominator beyond uint32 is rescaled",
			in: new(big.Rat).SetFrac(
				big.NewInt(1),
				big.NewInt(8_000_000_001),
			),
			wantNum: 0,
			wantDen: stakeFractionScale,
		},
		{
			name: "mainnet-scale ratio keeps its magnitude",
			// 5% of a 20e15 lovelace active stake, with a numerator chosen so
			// the reduced denominator cannot fit in a uint32.
			in: new(big.Rat).SetFrac(
				big.NewInt(1_000_000_000_000_001),
				big.NewInt(20_000_000_000_000_000),
			),
			wantNum: 50_000_000,
			wantDen: stakeFractionScale,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := stakeFractionToBetaRational(tc.in)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.wantNum, got.GetNumerator())
			assert.Equal(t, tc.wantDen, got.GetDenominator())
		})
	}
}

// TestStakeFractionToBetaRational_RejectsUnusable covers the inputs that cannot
// be encoded at all. A nil fraction is a ledger bug rather than a caller error,
// and reporting it as a zero share would be indistinguishable from a real pool
// with no stake.
func TestStakeFractionToBetaRational_RejectsUnusable(t *testing.T) {
	_, err := stakeFractionToBetaRational(nil)
	require.Error(t, err)

	_, err = stakeFractionToBetaRational(big.NewRat(-1, 2))
	require.Error(t, err, "a negative share is not a share")
}
