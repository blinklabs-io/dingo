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
	"encoding/hex"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	submit "github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/state"
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

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a SubmitTx request with %d transactions",
			len(txRawList),
		),
	)
	resp := &submit.SubmitTxResponse{}

	// Loop through the transactions and add each to the mempool
	errorList := make([]error, len(txRawList))
	hasError := false
	for i, txi := range txRawList {
		txRawBytes := txi.GetRaw() // raw bytes
		txHash := lcommon.Blake2b256Hash(txRawBytes)
		txType, err := ledger.DetermineTransactionType(txRawBytes)
		placeholderRef := []byte{}
		if err != nil {
			resp.Ref = append(resp.Ref, placeholderRef)
			errorList[i] = err
			s.utxorpc.config.Logger.Error(
				fmt.Sprintf(
					"failed decoding tx %d: %v",
					i,
					err,
				),
			)
			hasError = true
			continue
		}
		tx := mempool.MempoolTransaction{
			Hash:     txHash.String(),
			Type:     uint(txType),
			Cbor:     txRawBytes,
			LastSeen: time.Now(),
		}
		// Add transaction to mempool
		err = s.utxorpc.config.Mempool.AddTransaction(tx)
		if err != nil {
			resp.Ref = append(resp.Ref, placeholderRef)
			errorList[i] = fmt.Errorf("%s", err.Error())
			s.utxorpc.config.Logger.Error(
				fmt.Sprintf(
					"failed to add tx %s to mempool: %s",
					txHash.String(),
					err,
				),
			)
			hasError = true
			continue
		}
		txHexBytes, err := hex.DecodeString(tx.Hash)
		if err != nil {
			resp.Ref = append(resp.Ref, placeholderRef)
			errorList[i] = err
			hasError = true
			continue
		}
		resp.Ref = append(resp.Ref, txHexBytes)
	}
	if hasError {
		return connect.NewResponse(resp), fmt.Errorf("%v", errorList)
	}

	return connect.NewResponse(resp), nil
}

// WaitForTx
func (s *submitServiceServer) WaitForTx(
	ctx context.Context,
	req *connect.Request[submit.WaitForTxRequest],
	stream *connect.ServerStream[submit.WaitForTxResponse],
) error {
	ref := req.Msg.GetRef() // [][]byte

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Received WaitForTx request with %d transactions",
			len(ref),
		),
	)
	s.utxorpc.config.EventBus.SubscribeFunc(
		state.BlockfetchEventType,
		func(evt event.Event) {
			e := evt.Data.(state.BlockfetchEvent)
			for _, tx := range e.Block.Transactions() {
				for _, r := range ref {
					refHash := hex.EncodeToString(r)
					// Compare our hashes
					if refHash == tx.Hash() {
						// Send confirmation response
						err := stream.Send(&submit.WaitForTxResponse{
							Ref:   r,
							Stage: submit.Stage_STAGE_CONFIRMED,
						})
						if err != nil {
							if ctx.Err() != nil {
								s.utxorpc.config.Logger.Warn(
									"Client disconnected while sending response",
									"error", ctx.Err(),
								)
								return
							}
							s.utxorpc.config.Logger.Error(
								"Error sending response to client",
								"transaction_hash", tx.Hash(),
								"error", err,
							)
							return
						}
						s.utxorpc.config.Logger.Debug(
							"Confirmation response sent",
							"transaction_hash", tx.Hash(),
						)
						return // Stop processing after confirming the transaction
					}
				}
			}
		},
	)
	return nil
}

// ReadMempool
func (s *submitServiceServer) ReadMempool(
	ctx context.Context,
	req *connect.Request[submit.ReadMempoolRequest],
) (*connect.Response[submit.ReadMempoolResponse], error) {

	s.utxorpc.config.Logger.Info("Got a ReadMempool request")
	resp := &submit.ReadMempoolResponse{}

	mempool := []*submit.TxInMempool{}
	for _, tx := range s.utxorpc.config.Mempool.Transactions() {
		record := &submit.TxInMempool{
			NativeBytes: tx.Cbor,
			Stage:       submit.Stage_STAGE_MEMPOOL,
		}
		mempool = append(mempool, record)
	}
	resp.Items = mempool

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

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a WatchMempool request with predicate %v and fieldMask %v",
			predicate,
			fieldMask,
		),
	)

	// Start our forever loop
	for {
		// Match against mempool transactions
		for _, memTx := range s.utxorpc.config.Mempool.Transactions() {
			txRawBytes := memTx.Cbor
			txType, err := ledger.DetermineTransactionType(txRawBytes)
			if err != nil {
				return err
			}
			tx, err := ledger.NewTransactionFromCbor(txType, txRawBytes)
			if err != nil {
				return err
			}
			cTx := tx.Utxorpc() // *cardano.Tx
			resp := &submit.WatchMempoolResponse{}
			record := &submit.TxInMempool{
				NativeBytes: txRawBytes,
				Stage:       submit.Stage_STAGE_MEMPOOL,
			}
			resp.Tx = record
			if string(record.NativeBytes) == cTx.String() {
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
