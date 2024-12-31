// Copyright 2024 Blink Labs Software
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
	"fmt"

	"connectrpc.com/connect"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/ledger"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
)

// watchServiceServer implements the WatchService API
type watchServiceServer struct {
	watchconnect.UnimplementedWatchServiceHandler
	utxorpc *Utxorpc
}

// WatchTx
func (s *watchServiceServer) WatchTx(
	ctx context.Context,
	req *connect.Request[watch.WatchTxRequest],
	stream *connect.ServerStream[watch.WatchTxResponse],
) error {
	predicate := req.Msg.GetPredicate() // Predicate
	fieldMask := req.Msg.GetFieldMask()
	intersect := req.Msg.GetIntersect() // []*BlockRef

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a WatchTx request with predicate %v and fieldMask %v and intersect %v",
			predicate,
			fieldMask,
			intersect,
		),
	)

	// Get our points
	var points []ocommon.Point
	if len(intersect) > 0 {
		for _, blockRef := range intersect {
			blockIdx := blockRef.GetIndex()
			blockHash := blockRef.GetHash()
			slot := uint64(blockIdx)
			point := ocommon.NewPoint(slot, blockHash)
			points = append(points, point)
		}
	} else {
		point := s.utxorpc.config.LedgerState.Tip().Point
		points = append(points, point)
	}

	// Get our starting point matching our chain
	point, err := s.utxorpc.config.LedgerState.GetIntersectPoint(points)
	if err != nil {
		s.utxorpc.config.Logger.Error(
			"failed to get points",
			"error", err,
		)
		return err
	}

	// Create our chain iterator
	chainIter, err := s.utxorpc.config.LedgerState.GetChainFromPoint(*point, false)
	if err != nil {
		s.utxorpc.config.Logger.Error(
			"failed to get chain iterator",
			"error", err,
		)
		return err
	}

	for {
		// Check for available block
		next, err := chainIter.Next(false)
		if err != nil {
			s.utxorpc.config.Logger.Error(
				"failed to iterate chain",
				"error", err,
			)
			return err
		}
		if next != nil {
			// Get ledger.Block from bytes
			blockBytes := next.Block.Cbor[:]
			blockType, err := ledger.DetermineBlockType(blockBytes)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to get block type",
					"error", err,
				)
				return err
			}
			block, err := ledger.NewBlockFromCbor(blockType, blockBytes)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to get block",
					"error", err,
				)
				return err
			}

			// Loop through transactions
			for _, tx := range block.Transactions() {
				var act watch.AnyChainTx
				actc := watch.AnyChainTx_Cardano{
					Cardano: tx.Utxorpc(),
				}
				act.Chain = &actc
				resp := &watch.WatchTxResponse{
					Action: &watch.WatchTxResponse_Apply{
						Apply: &act,
					},
				}
				if predicate == nil {
					err := stream.Send(resp)
					if err != nil {
						return err
					}
				} else {
					// TODO: filter from all Predicate types
					err := stream.Send(resp)
					if err != nil {
						return err
					}
				}
			}
		}
	}
}
