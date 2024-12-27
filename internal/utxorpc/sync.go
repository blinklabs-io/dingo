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
	"log"

	"connectrpc.com/connect"
	sync "github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
)

// syncServiceServer implements the SyncService API
type syncServiceServer struct {
	syncconnect.UnimplementedSyncServiceHandler
}

// FetchBlock
func (s *syncServiceServer) FetchBlock(
	ctx context.Context,
	req *connect.Request[sync.FetchBlockRequest],
) (*connect.Response[sync.FetchBlockResponse], error) {
	ref := req.Msg.GetRef() // BlockRef
	fieldMask := req.Msg.GetFieldMask()

	log.Printf(
		"Got a FetchBlock request with ref %v and fieldMask %v",
		ref,
		fieldMask,
	)

	resp := &sync.FetchBlockResponse{}
	// TODO: replace with something that works NtC
	// for _, point := range points {
	// 	log.Printf("Point Slot: %d, Hash: %x\n", point.Slot, point.Hash)
	// 	block, err := oConn.BlockFetch().Client.GetBlock(
	// 		ocommon.NewPoint(point.Slot, point.Hash),
	// 	)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	var acb sync.AnyChainBlock
	// 	var acbc sync.AnyChainBlock_Cardano
	// 	ret := NewBlockFromBlock(block)
	// 	acbc.Cardano = &ret
	// 	acb.Chain = &acbc
	// 	resp.Block = append(resp.Block, &acb)
	// }

	return connect.NewResponse(resp), nil
}

// DumpHistory
func (s *syncServiceServer) DumpHistory(
	ctx context.Context,
	req *connect.Request[sync.DumpHistoryRequest],
) (*connect.Response[sync.DumpHistoryResponse], error) {
	startToken := req.Msg.GetStartToken() // BlockRef
	maxItems := req.Msg.GetMaxItems()
	fieldMask := req.Msg.GetFieldMask()

	log.Printf(
		"Got a DumpHistory request with token %v and maxItems %d and fieldMask %v",
		startToken,
		maxItems,
		fieldMask,
	)

	resp := &sync.DumpHistoryResponse{}
	// TODO: the thing
	return connect.NewResponse(resp), nil
}

// FollowTip
func (s *syncServiceServer) FollowTip(
	ctx context.Context,
	req *connect.Request[sync.FollowTipRequest],
	stream *connect.ServerStream[sync.FollowTipResponse],
) error {
	intersect := req.Msg.GetIntersect() // []*BlockRef

	log.Printf("Got a FollowTip request with intersect %v", intersect)

	// TODO: the thing
	return nil
}
