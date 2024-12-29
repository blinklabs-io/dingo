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
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
)

// submitServiceServer implements the SubmitService API
type submitServiceServer struct {
	submitconnect.UnimplementedSubmitServiceHandler
	utxorpc *Utxorpc
}

// SubmitTx
func (s *submitServiceServer) SubmitTx(
	ctx context.Context,
	req *connect.Request[submit.SubmitTxRequest],
) (*connect.Response[submit.SubmitTxResponse], error) {
	txRawList := req.Msg.GetTx() // []*AnyChainTx

	log.Printf("Got a SubmitTx request with %d transactions", len(txRawList))
	resp := &submit.SubmitTxResponse{}

	return connect.NewResponse(resp), nil
}

func (s *submitServiceServer) WaitForTx(
	ctx context.Context,
	req *connect.Request[submit.WaitForTxRequest],
	stream *connect.ServerStream[submit.WaitForTxResponse],
) error {
	ref := req.Msg.GetRef() // [][]byte

	log.Printf("Received WaitForTx request with %d transactions", len(ref))
	return nil
}

// ReadMempool
func (s *submitServiceServer) ReadMempool(
	ctx context.Context,
	req *connect.Request[submit.ReadMempoolRequest],
) (*connect.Response[submit.ReadMempoolResponse], error) {

	log.Printf("Got a ReadMempool request")
	resp := &submit.ReadMempoolResponse{}

	return connect.NewResponse(resp), nil
}

// WatchMempool
func (s *submitServiceServer) WatchMempool(
	ctx context.Context,
	req *connect.Request[submit.WatchMempoolRequest],
	stream *connect.ServerStream[submit.WatchMempoolResponse],
) error {

	predicate := req.Msg.GetPredicate() // Predicate
	fieldMask := req.Msg.GetFieldMask()

	log.Printf(
		"Got a WatchMempool request with predicate %v and fieldMask %v",
		predicate,
		fieldMask,
	)
	return nil
}
