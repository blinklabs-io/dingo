// Copyright 2025 Blink Labs Software
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

	"connectrpc.com/connect"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
			blockIdx := blockRef.GetSlot()
			blockHash := blockRef.GetHash()
			slot := blockIdx
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
	if point == nil {
		s.utxorpc.config.Logger.Error(
			"nil point returned",
		)
		return errors.New("nil point returned")
	}

	// Create our chain iterator
	chainIter, err := s.utxorpc.config.LedgerState.GetChainFromPoint(
		*point,
		false,
	)
	if err != nil {
		s.utxorpc.config.Logger.Error(
			"failed to get chain iterator",
			"error", err,
		)
		return err
	}

	defer chainIter.Cancel()

	// Cancel the chain iterator when the gRPC stream context is
	// done so that blocking Next() calls unblock immediately.
	go func() {
		<-ctx.Done()
		chainIter.Cancel()
	}()

	for {
		// Check for available block
		next, err := chainIter.Next(true)
		if err != nil {
			// Check if it was a context cancellation
			if ctx.Err() != nil {
				s.utxorpc.config.Logger.Debug(
					"WatchTx client disconnected",
				)
				return ctx.Err()
			}
			s.utxorpc.config.Logger.Error(
				"failed to iterate chain",
				"error", err,
			)
			return err
		}
		if next != nil {
			// Get ledger.Block from bytes
			block, err := ledger.NewBlockFromCbor(
				next.Block.Type,
				next.Block.Cbor,
			)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to get block",
					"error", err,
				)
				return err
			}

			// Loop through transactions
			for _, tx := range block.Transactions() {
				tmpTx, err := tx.Utxorpc()
				if err != nil {
					return fmt.Errorf("convert transaction: %w", err)
				}
				var act watch.AnyChainTx
				actc := watch.AnyChainTx_Cardano{
					Cardano: tmpTx,
				}
				act.Chain = &actc
				resp := &watch.WatchTxResponse{
					Action: &watch.WatchTxResponse_Apply{
						Apply: &act,
					},
				}
				shouldSend := predicate == nil ||
					s.utxorpc.matchesTxPattern(
						tx,
						predicate.GetMatch().GetCardano(),
					)
				if shouldSend {
					err := stream.Send(resp)
					if err != nil {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						return err
					}
				}
			}
		}
	}
}
