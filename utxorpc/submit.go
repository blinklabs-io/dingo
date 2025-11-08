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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
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
	placeholderRef := []byte{}
	for i, txi := range txRawList {
		txRawBytes := txi.GetRaw() // raw bytes
		txType, err := gledger.DetermineTransactionType(txRawBytes)
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
		tx, err := gledger.NewTransactionFromCbor(txType, txRawBytes)
		if err != nil {
			resp.Ref = append(resp.Ref, placeholderRef)
			errorList[i] = err
			s.utxorpc.config.Logger.Error(
				fmt.Sprint(fmt.Errorf("failed to decode transaction from CBOR: %w", err)),
			)
			hasError = true
			continue
		}
		if tx == nil {
			resp.Ref = append(resp.Ref, placeholderRef)
			errorList[i] = errors.New("decoded transaction is nil")
			s.utxorpc.config.Logger.Error("decoded transaction is nil")
			hasError = true
			continue
		}
		txHash := tx.Hash()
		// Add transaction to mempool
		err = s.utxorpc.config.Mempool.AddTransaction(txType, txRawBytes)
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
		if err != nil {
			resp.Ref = append(resp.Ref, placeholderRef)
			errorList[i] = err
			hasError = true
			continue
		}
		resp.Ref = append(resp.Ref, txHash.Bytes())
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
		ledger.BlockfetchEventType,
		func(evt event.Event) {
			e := evt.Data.(ledger.BlockfetchEvent)
			for _, tx := range e.Block.Transactions() {
				for _, r := range ref {
					refHash := hex.EncodeToString(r)
					// Compare our hashes
					if refHash == tx.Hash().String() {
						// Send confirmation response
						err := stream.Send(&submit.WaitForTxResponse{
							Ref:   r,
							Stage: submit.Stage_STAGE_CONFIRMED,
						})
						if err != nil {
							if ctx.Err() != nil {
								s.utxorpc.config.Logger.Warn(
									"Client disconnected while sending response",
									"error",
									ctx.Err(),
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

// EvalTx
func (s *submitServiceServer) EvalTx(
	ctx context.Context,
	req *connect.Request[submit.EvalTxRequest],
) (*connect.Response[submit.EvalTxResponse], error) {
	s.utxorpc.config.Logger.Info("Got an EvalTx request")
	txRawList := req.Msg.GetTx() // []*AnyChainTx
	resp := &submit.EvalTxResponse{}
	for _, txi := range txRawList {
		txRawBytes := txi.GetRaw()
		// Decode TX
		txType, err := gledger.DetermineTransactionType(txRawBytes)
		if err != nil {
			return nil, fmt.Errorf(
				"could not parse transaction to determine type: %w",
				err,
			)
		}
		tx, err := gledger.NewTransactionFromCbor(txType, txRawBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction CBOR: %w", err)
		}
		// Evaluate TX
		fee, totalExUnits, redeemerExUnits, err := s.utxorpc.config.LedgerState.EvaluateTx(
			tx,
		)
		// Populate response
		tmpRedeemers := make([]*cardano.Redeemer, 0, len(redeemerExUnits))
		for key, val := range redeemerExUnits {
			tmpRedeemers = append(
				tmpRedeemers,
				&cardano.Redeemer{
					Purpose: cardano.RedeemerPurpose(key.Tag),
					Index:   key.Index,
					ExUnits: &cardano.ExUnits{
						Steps:  uint64(val.Steps),  // nolint:gosec
						Memory: uint64(val.Memory), // nolint:gosec
					},
					// TODO: Payload
				},
			)
		}
		var txEval *cardano.TxEval
		if err != nil {
			txEval = &cardano.TxEval{
				Errors: []*cardano.EvalError{
					{
						Msg: err.Error(),
					},
				},
			}
		} else {
			txEval = &cardano.TxEval{
				Fee: fee,
				ExUnits: &cardano.ExUnits{
					Steps:  uint64(totalExUnits.Steps),  // nolint:gosec
					Memory: uint64(totalExUnits.Memory), // nolint:gosec
				},
				Redeemers: tmpRedeemers,
			}
		}
		resp.Report = append(
			resp.Report,
			&submit.AnyChainEval{
				Chain: &submit.AnyChainEval_Cardano{
					Cardano: txEval,
				},
			},
		)
	}
	return connect.NewResponse(resp), nil
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
			txType, err := gledger.DetermineTransactionType(txRawBytes)
			if err != nil {
				return err
			}
			tx, err := gledger.NewTransactionFromCbor(txType, txRawBytes)
			if err != nil {
				return err
			}
			cTx, err := tx.Utxorpc() // *cardano.Tx
			if err != nil {
				return fmt.Errorf("convert transaction: %w", err)
			}
			resp := &submit.WatchMempoolResponse{}
			record := &submit.TxInMempool{
				NativeBytes: txRawBytes,
				Stage:       submit.Stage_STAGE_MEMPOOL,
			}
			resp.Tx = record
			if string(record.GetNativeBytes()) == cTx.String() {
				if predicate == nil {
					err := stream.Send(resp)
					if err != nil {
						return err
					}
				} else {
					found := false
					assetFound := false

					// Check Predicate
					addressPattern := predicate.GetMatch().GetCardano().GetHasAddress()
					mintAssetPattern := predicate.GetMatch().GetCardano().GetMintsAsset()
					moveAssetPattern := predicate.GetMatch().GetCardano().GetMovesAsset()

					var addresses []gledger.Address
					if addressPattern != nil {
						// Handle Exact Address
						exactAddressBytes := addressPattern.GetExactAddress()
						if exactAddressBytes != nil {
							var addr lcommon.Address
							err := addr.UnmarshalCBOR(exactAddressBytes)
							if err != nil {
								return fmt.Errorf(
									"failed to decode exact address: %w",
									err,
								)
							}
							addresses = append(addresses, addr)
						}

						// Handle Payment Part
						paymentPart := addressPattern.GetPaymentPart()
						if paymentPart != nil {
							s.utxorpc.config.Logger.Info("PaymentPart is present, decoding...")
							var paymentAddr lcommon.Address
							err := paymentAddr.UnmarshalCBOR(paymentPart)
							if err != nil {
								return fmt.Errorf("failed to decode payment part: %w", err)
							}
							addresses = append(addresses, paymentAddr)
						}

						// Handle Delegation Part
						delegationPart := addressPattern.GetDelegationPart()
						if delegationPart != nil {
							s.utxorpc.config.Logger.Info(
								"DelegationPart is present, decoding...",
							)
							var delegationAddr lcommon.Address
							err := delegationAddr.UnmarshalCBOR(delegationPart)
							if err != nil {
								return fmt.Errorf(
									"failed to decode delegation part: %w",
									err,
								)
							}
							addresses = append(addresses, delegationAddr)
						}
					}

					var assetPatterns []*cardano.AssetPattern
					if mintAssetPattern != nil {
						assetPatterns = append(assetPatterns, mintAssetPattern)
					}
					if moveAssetPattern != nil {
						assetPatterns = append(assetPatterns, moveAssetPattern)
					}

					// Convert everything to utxos (gledger.TransactionOutput) for matching
					var utxos []gledger.TransactionOutput
					utxos = append(tx.Outputs(), tx.CollateralReturn())
					var inputs []gledger.TransactionInput
					inputs = append(tx.Inputs(), tx.ReferenceInputs()...)
					inputs = append(inputs, tx.Collateral()...)
					for _, input := range inputs {
						utxo, err := s.utxorpc.config.LedgerState.UtxoByRef(
							input.Id().Bytes(),
							input.Index(),
						)
						if err != nil {
							return fmt.Errorf(
								"failed to look up input: %w",
								err,
							)
						}
						ret, err := utxo.Decode() // gledger.TransactionOutput
						if err != nil {
							return err
						}
						if ret == nil {
							return errors.New("decode returned empty utxo")
						}
						utxos = append(utxos, ret)
					}

					// Check UTxOs for addresses
					for _, address := range addresses {
						if found {
							break
						}
						if assetFound {
							found = true
							break
						}
						for _, utxo := range utxos {
							if found {
								break
							}
							if assetFound {
								found = true
								break
							}
							if utxo.Address().String() == address.String() {
								if found {
									break
								}
								if assetFound {
									found = true
									break
								}
								// We matched address, check assetPatterns
								for _, assetPattern := range assetPatterns {
									// Address found, no assetPattern
									if assetPattern == nil {
										found = true
										break
									}
									// Filter on assetPattern
									for _, policyId := range utxo.Assets().Policies() {
										if assetFound {
											found = true
											break
										}
										if bytes.Equal(
											policyId.Bytes(),
											assetPattern.GetPolicyId(),
										) {
											for _, asset := range utxo.Assets().Assets(
												policyId,
											) {
												if bytes.Equal(
													asset,
													assetPattern.GetAssetName(),
												) {
													found = true
													assetFound = true
													break
												}
											}
										}
									}
								}
								if found {
									break
								}
								// Asset not found; skip this UTxO
								if !assetFound {
									continue
								}
								found = true
							}
						}
					}
					if found {
						err := stream.Send(resp)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}
}
