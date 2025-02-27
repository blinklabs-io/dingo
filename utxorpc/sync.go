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
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
)

// syncServiceServer implements the SyncService API
type syncServiceServer struct {
	syncconnect.UnimplementedSyncServiceHandler
	utxorpc *Utxorpc
}

// FetchBlock
func (s *syncServiceServer) FetchBlock(
	ctx context.Context,
	req *connect.Request[sync.FetchBlockRequest],
) (*connect.Response[sync.FetchBlockResponse], error) {
	ref := req.Msg.GetRef() // []*BlockRef
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a FetchBlock request with ref %v and fieldMask %v",
			ref,
			fieldMask,
		),
	)
	resp := &sync.FetchBlockResponse{}

	// Get our points
	var points []ocommon.Point
	if len(ref) > 0 {
		for _, blockRef := range ref {
			blockIdx := blockRef.GetIndex()
			blockHash := blockRef.GetHash()
			slot := blockIdx
			point := ocommon.NewPoint(slot, blockHash)
			points = append(points, point)
		}
	} else {
		point := s.utxorpc.config.LedgerState.Tip().Point
		points = append(points, point)
	}

	for _, point := range points {
		block, err := s.utxorpc.config.LedgerState.GetBlock(point)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, fmt.Errorf("block returned nil")
		}
		var acb sync.AnyChainBlock
		ret, err := block.Decode()
		if err != nil {
			return nil, err
		}
		acbc := sync.AnyChainBlock_Cardano{
			Cardano: ret.Utxorpc(),
		}
		acb.Chain = &acbc
		resp.Block = append(resp.Block, &acb)
	}

	return connect.NewResponse(resp), nil
}

// DumpHistory
func (s *syncServiceServer) DumpHistory(
	ctx context.Context,
	req *connect.Request[sync.DumpHistoryRequest],
) (*connect.Response[sync.DumpHistoryResponse], error) {
	startToken := req.Msg.GetStartToken() // *BlockRef
	maxItems := req.Msg.GetMaxItems()     // uint32
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a DumpHistory request with token %v and maxItems %d and fieldMask %v",
			startToken,
			maxItems,
			fieldMask,
		),
	)
	resp := &sync.DumpHistoryResponse{}

	// Get our points
	var points []ocommon.Point
	if maxItems > 0 {
		tmpPoints, err := s.utxorpc.config.LedgerState.RecentChainPoints(
			int(maxItems),
		)
		if err != nil {
			return nil, err
		}
		points = tmpPoints
	} else {
		point := s.utxorpc.config.LedgerState.Tip().Point
		points = append(points, point)
	}
	// TODO: make this work (#401)
	// if startToken != nil {
	// 	blockIdx := startToken.GetIndex()
	// 	blockHash := startToken.GetHash()
	// 	slot := uint64(blockIdx)
	// 	point = ocommon.NewPoint(slot, blockHash)
	// }

	for _, point := range points {
		block, err := s.utxorpc.config.LedgerState.GetBlock(point)
		if err != nil {
			return nil, err
		}
		if block == nil {
			return nil, fmt.Errorf("block returned nil")
		}
		var acb sync.AnyChainBlock
		ret, err := block.Decode()
		if err != nil {
			return nil, err
		}
		acbc := sync.AnyChainBlock_Cardano{
			Cardano: ret.Utxorpc(),
		}
		acb.Chain = &acbc
		resp.Block = append(resp.Block, &acb)
	}

	return connect.NewResponse(resp), nil
}

// FollowTip
func (s *syncServiceServer) FollowTip(
	ctx context.Context,
	req *connect.Request[sync.FollowTipRequest],
	stream *connect.ServerStream[sync.FollowTipResponse],
) error {
	intersect := req.Msg.GetIntersect() // []*BlockRef

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a FollowTip request with intersect %v",
			intersect,
		),
	)

	// Get our points
	var points []ocommon.Point
	if len(intersect) > 0 {
		for _, blockRef := range intersect {
			blockIdx := blockRef.GetIndex()
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
		return fmt.Errorf("nil point returned")
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

	for {
		// Check for available block
		next, err := chainIter.Next(true)
		if err != nil {
			s.utxorpc.config.Logger.Error(
				"failed to iterate chain",
				"error", err,
			)
			return err
		}
		if next != nil {
			// Send block response
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
			var acb sync.AnyChainBlock
			acbc := sync.AnyChainBlock_Cardano{
				Cardano: block.Utxorpc(),
			}
			acb.Chain = &acbc
			resp := &sync.FollowTipResponse{
				Action: &sync.FollowTipResponse_Apply{
					Apply: &acb,
				},
			}
			err = stream.Send(resp)
			if err != nil {
				s.utxorpc.config.Logger.Error(
					"failed to send message to client",
					"error", err,
				)
				return err
			}
		}
	}
}

// ReadTip
func (s *syncServiceServer) ReadTip(
	ctx context.Context,
	req *connect.Request[sync.ReadTipRequest],
) (*connect.Response[sync.ReadTipResponse], error) {

	s.utxorpc.config.Logger.Info("Got a ReadTip request")
	resp := &sync.ReadTipResponse{}

	point := s.utxorpc.config.LedgerState.Tip().Point
	resp.Tip = &sync.BlockRef{Index: point.Slot, Hash: point.Hash}

	return connect.NewResponse(resp), nil
}
