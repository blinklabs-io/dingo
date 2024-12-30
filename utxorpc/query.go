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
	query "github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
)

// queryServiceServer implements the QueryService API
type queryServiceServer struct {
	queryconnect.UnimplementedQueryServiceHandler
	utxorpc *Utxorpc
}

// ReadParams
func (s *queryServiceServer) ReadParams(
	ctx context.Context,
	req *connect.Request[query.ReadParamsRequest],
) (*connect.Response[query.ReadParamsResponse], error) {
	fieldMask := req.Msg.GetFieldMask()

	log.Printf("Got a ReadParams request with fieldMask %v", fieldMask)
	resp := &query.ReadParamsResponse{}

	return connect.NewResponse(resp), nil
}

// ReadUtxos
func (s *queryServiceServer) ReadUtxos(
	ctx context.Context,
	req *connect.Request[query.ReadUtxosRequest],
) (*connect.Response[query.ReadUtxosResponse], error) {
	keys := req.Msg.GetKeys() // []*TxoRef

	log.Printf("Got a ReadUtxos request with keys %v", keys)
	resp := &query.ReadUtxosResponse{}

	return connect.NewResponse(resp), nil
}

// SearchUtxos
func (s *queryServiceServer) SearchUtxos(
	ctx context.Context,
	req *connect.Request[query.SearchUtxosRequest],
) (*connect.Response[query.SearchUtxosResponse], error) {
	predicate := req.Msg.GetPredicate() // UtxoPredicate

	log.Printf("Got a SearchUtxos request with predicate %v", predicate)
	resp := &query.SearchUtxosResponse{}

	return connect.NewResponse(resp), nil
}

// StreamUtxos
