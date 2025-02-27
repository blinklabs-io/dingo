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
	"fmt"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
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
					found := false
					assetFound := false

					// Check Predicate
					addressPattern := predicate.GetMatch().GetCardano().GetHasAddress()
					mintAssetPattern := predicate.GetMatch().GetCardano().GetMintsAsset()
					moveAssetPattern := predicate.GetMatch().GetCardano().GetMovesAsset()

					var addresses []ledger.Address
					if addressPattern != nil {
						// Handle Exact Address
						exactAddressBytes := addressPattern.GetExactAddress()
						if exactAddressBytes != nil {
							var addr ledger.Address
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
							var paymentAddr ledger.Address
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
							var delegationAddr ledger.Address
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

					// Convert everything to utxos (ledger.TransactionOutput) for matching
					var utxos []ledger.TransactionOutput
					utxos = append(tx.Outputs(), tx.CollateralReturn())
					var inputs []ledger.TransactionInput
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
						ret, err := utxo.Decode() // ledger.TransactionOutput
						if err != nil {
							return err
						}
						if ret == nil {
							return fmt.Errorf("decode returned empty utxo")
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
											assetPattern.PolicyId,
										) {
											for _, asset := range utxo.Assets().Assets(policyId) {
												if bytes.Equal(asset, assetPattern.AssetName) {
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
