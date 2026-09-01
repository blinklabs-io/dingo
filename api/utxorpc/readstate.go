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
	"fmt"
	"math"
	"math/big"

	"connectrpc.com/connect"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	betacardano "github.com/utxorpc/go-codegen/utxorpc/v1beta/cardano"
	betaquery "github.com/utxorpc/go-codegen/utxorpc/v1beta/query"
)

// betaQueryServiceServer serves the v1beta-only QueryService methods. Every
// other method of that service is wire-compatible with v1alpha and is served by
// rewriting the path onto the alpha handler (see betaVersionedQueryHandler), so
// this type deliberately implements only ReadState rather than the whole
// service interface.
type betaQueryServiceServer struct {
	utxorpc *Utxorpc
}

// stakeFractionScale is the denominator used when a stake fraction's exact
// ratio does not fit the protobuf RationalNumber. See
// stakeFractionToBetaRational.
const stakeFractionScale = 1_000_000_000

// ReadState answers utxorpc.v1beta.query.QueryService.ReadState.
//
// v1beta defines exactly one Cardano state query, the stake pool distribution,
// so that is the only variant handled here. A variant added by a later codegen
// bump falls through to Unimplemented rather than being answered wrongly.
func (s *betaQueryServiceServer) ReadState(
	_ context.Context,
	req *connect.Request[betaquery.ReadStateRequest],
) (*connect.Response[betaquery.ReadStateResponse], error) {
	fieldMask := req.Msg.GetFieldMask()
	s.utxorpc.config.Logger.Info(
		fmt.Sprintf("Got a ReadState request with fieldMask %v", fieldMask),
	)

	chainQuery := req.Msg.GetQuery()
	if chainQuery == nil || chainQuery.GetQuery() == nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("no chain state query supplied"),
		)
	}
	// Switched on the oneof wrapper rather than on GetCardano(), which returns
	// nil both for a chain this node does not serve and for a Cardano query
	// carrying no message. Those are different answers -- Unimplemented and
	// InvalidArgument -- and GetCardano() cannot tell a caller which it got.
	chain, ok := chainQuery.GetQuery().(*betaquery.AnyChainStateQuery_Cardano)
	if !ok {
		return nil, connect.NewError(
			connect.CodeUnimplemented,
			fmt.Errorf(
				"unsupported chain state query %T",
				chainQuery.GetQuery(),
			),
		)
	}
	if chain.Cardano.GetQuery() == nil {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("no Cardano state query supplied"),
		)
	}

	switch query := chain.Cardano.GetQuery().(type) {
	case *betacardano.StateQuery_StakePoolDistribution:
		// A nil inner message is a query with no pool filter rather than a
		// malformed one: the generated getters are nil-safe and proto3 reads an
		// absent message as its default.
		return s.readStakePoolDistribution(query.StakePoolDistribution)
	default:
		return nil, connect.NewError(
			connect.CodeUnimplemented,
			fmt.Errorf(
				"unsupported Cardano state query %T",
				chain.Cardano.GetQuery(),
			),
		)
	}
}

// readStakePoolDistribution answers GetStakePoolDistribution from the same
// snapshot the node elects leaders from, by way of the shared
// ledger.PoolStakeDistribution that also answers the node-to-client
// GetPoolDistr2 query.
func (s *betaQueryServiceServer) readStakePoolDistribution(
	query *betacardano.GetStakePoolDistribution,
) (*connect.Response[betaquery.ReadStateResponse], error) {
	// LedgerState is an optional dependency: Utxorpc.Start admits an untyped
	// nil and documents that handlers check per request (it rejects only a
	// typed nil, which would slip past a check like this one). Without this a
	// node configured with no ledger state answers ReadState with a panicking
	// listener rather than a status a client can act on.
	if s.utxorpc.config.LedgerState == nil {
		return nil, connect.NewError(
			connect.CodeUnavailable,
			errors.New("ledger state not available"),
		)
	}

	poolFilter, err := poolKeyHashFilter(
		query.GetPoolKeyhashes(),
		s.utxorpc.config.MaxPoolFilter,
	)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	dist, err := s.utxorpc.config.LedgerState.PoolStakeDistribution(poolFilter)
	if err != nil {
		return nil, connect.NewError(
			connect.CodeInternal,
			fmt.Errorf("read stake pool distribution: %w", err),
		)
	}
	if dist == nil {
		// Reported rather than dereferenced: a nil distribution with no error
		// is a ledger bug, and this handler is a network listener, so the cost
		// of the assumption being wrong is a panicking server rather than one
		// failed request.
		return nil, connect.NewError(
			connect.CodeInternal,
			errors.New("stake pool distribution is nil"),
		)
	}

	pools := make([]*betacardano.PoolStakeShare, 0, len(dist.Pools))
	for _, pool := range dist.Pools {
		var fraction *big.Rat
		if pool.StakeFraction != nil {
			fraction = pool.StakeFraction.Rat
		}
		stakeFraction, err := stakeFractionToBetaRational(fraction)
		if err != nil {
			return nil, connect.NewError(
				connect.CodeInternal,
				fmt.Errorf(
					"encode stake fraction for pool %x: %w",
					pool.PoolKeyHash.Bytes(),
					err,
				),
			)
		}
		pools = append(pools, &betacardano.PoolStakeShare{
			PoolKeyhash:   pool.PoolKeyHash.Bytes(),
			StakeFraction: stakeFraction,
			VrfKeyhash:    pool.VrfKeyHash.Bytes(),
		})
	}

	tip := s.utxorpc.betaChainPoint(dist.Tip)
	return connect.NewResponse(&betaquery.ReadStateResponse{
		Result: &betaquery.AnyChainStateData{
			Result: &betaquery.AnyChainStateData_Cardano{
				Cardano: &betacardano.StateData{
					Result: &betacardano.StateData_StakePoolDistribution{
						StakePoolDistribution: &betacardano.StakePoolDistribution{
							Pools: pools,
						},
					},
				},
			},
		},
		LedgerTip: tip,
	}), nil
}

// poolKeyHashFilter converts the request's pool filter into the form
// ledger.PoolStakeDistribution takes.
//
// An empty request filter becomes a nil filter, because the two sides spell
// "every pool" differently: the proto documents an empty pool_keyhashes as
// "return the distribution for every pool", while the ledger reads an empty
// non-nil filter as a request for no pools at all (that spelling exists for
// GetPoolDistr2, whose wire form can carry an explicit empty set). Passing the
// request's empty slice straight through would return nothing.
func poolKeyHashFilter(
	poolKeyHashes [][]byte,
	maxPoolFilter int,
) ([]lcommon.PoolKeyHash, error) {
	if len(poolKeyHashes) == 0 {
		return nil, nil
	}
	// Bounded like ReadUtxos' and ReadData's key lists: the filter is client
	// supplied and sizes both the allocation here and the snapshot and
	// registration reads it drives, so an unbounded one lets a single request
	// choose how much work the node does. A caller wanting every pool sends an
	// empty filter, which costs one bulk read regardless of pool count.
	if len(poolKeyHashes) > maxPoolFilter {
		return nil, fmt.Errorf(
			"too many pool key hashes: %d exceeds maximum of %d",
			len(poolKeyHashes),
			maxPoolFilter,
		)
	}
	filter := make([]lcommon.PoolKeyHash, 0, len(poolKeyHashes))
	for _, raw := range poolKeyHashes {
		// Length is checked rather than letting NewBlake2b224 pad or truncate:
		// either would query a pool the caller did not name and report the
		// answer as though it had.
		if len(raw) != lcommon.Blake2b224Size {
			return nil, fmt.Errorf(
				"pool key hash %x is %d bytes, want %d",
				raw,
				len(raw),
				lcommon.Blake2b224Size,
			)
		}
		filter = append(
			filter,
			lcommon.PoolKeyHash(lcommon.NewBlake2b224(raw)),
		)
	}
	return filter, nil
}

// betaChainPoint renders a point as a v1beta ChainPoint, looking up the block
// height that completes it.
//
// The point is passed in rather than sampled from the ledger here, so a caller
// can name the point its answer was actually read at. Sampling Tip() at
// response-building time would report a tip the chain had moved on to, which
// across an epoch boundary names an epoch whose stake snapshot is not the one
// the reply carries.
//
// The whole tip is passed rather than just its point because it already
// carries the block number. Re-reading the block to recover a height the
// caller already holds can fail, and `height` is a plain proto3 uint64 with no
// encoding for "unknown": a zero beside a non-origin slot and hash asserts
// that the point is the origin block rather than admitting the height is
// unknown, and a client cannot tell the two apart.
func (u *Utxorpc) betaChainPoint(
	tip ochainsync.Tip,
) *betaquery.ChainPoint {
	br := blockRefFromTip(tip)
	return &betaquery.ChainPoint{
		Slot:   br.Slot,
		Hash:   br.Hash,
		Height: br.Height,
	}
}

// stakeFractionToBetaRational encodes a stake fraction as a protobuf
// RationalNumber, whose numerator is an int32 and denominator a uint32.
//
// The exact ratio is kept whenever it fits, which covers small and test
// networks and any ratio that happens to reduce. It generally does not fit on a
// real network: the fraction is a ratio of two lovelace amounts and mainnet's
// active stake is on the order of 2e16, so a reduced denominator routinely
// exceeds uint32 by six orders of magnitude. Rather than fail, the fraction is
// rescaled onto stakeFractionScale, bounding the error at one part in a
// billion. That is far finer than any decision made from a stake share, and a
// caller reading numerator/denominator as a ratio does not need to know which
// path produced it.
func stakeFractionToBetaRational(
	rat *big.Rat,
) (*betacardano.RationalNumber, error) {
	if rat == nil {
		return nil, errors.New("stake fraction is nil")
	}
	if rat.Sign() < 0 {
		return nil, fmt.Errorf("stake fraction %s is negative", rat)
	}
	num := rat.Num()
	den := rat.Denom()
	if den.Sign() <= 0 {
		return nil, fmt.Errorf(
			"stake fraction %s has a non-positive denominator", rat,
		)
	}
	if num.IsInt64() && num.Int64() <= math.MaxInt32 &&
		den.IsInt64() && den.Int64() <= math.MaxUint32 {
		// #nosec G115 -- both bounds are checked immediately above
		return &betacardano.RationalNumber{
			Numerator:   int32(num.Int64()),
			Denominator: uint32(den.Int64()),
		}, nil
	}
	// Rescale with round-half-up: (num*scale + den/2) / den.
	scaled := new(big.Int).Mul(num, big.NewInt(stakeFractionScale))
	scaled.Add(scaled, new(big.Int).Rsh(den, 1))
	scaled.Quo(scaled, den)
	if !scaled.IsInt64() || scaled.Int64() > math.MaxInt32 {
		// Only reachable for a fraction well above one, which would mean a
		// pool holding more stake than the snapshot's total. Report it rather
		// than wrap it into a small number.
		return nil, fmt.Errorf("stake fraction %s is out of range", rat)
	}
	// #nosec G115 -- the range is checked immediately above
	return &betacardano.RationalNumber{
		Numerator:   int32(scaled.Int64()),
		Denominator: stakeFractionScale,
	}, nil
}
