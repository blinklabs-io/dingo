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
	"errors"
	"fmt"
	"sort"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
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

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadParams request with fieldMask %v",
			fieldMask,
		),
	)
	resp := &query.ReadParamsResponse{}

	protoParams := s.utxorpc.config.LedgerState.GetCurrentPParams()
	if protoParams == nil {
		return nil, errors.New("current protocol parameters empty")
	}

	// Get chain point (slot and hash)
	point := s.utxorpc.config.LedgerState.Tip().Point

	// Set up response parameters
	tmpPparams, err := protoParams.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("convert pparams: %w", err)
	}
	acpc := &query.AnyChainParams_Cardano{
		Cardano: tmpPparams,
	}
	resp.LedgerTip = &query.ChainPoint{
		Slot: point.Slot,
		Hash: point.Hash,
	}
	resp.Values = &query.AnyChainParams{
		Params: acpc,
	}
	return connect.NewResponse(resp), nil
}

// ReadEraSummary
func (s *queryServiceServer) ReadEraSummary(
	ctx context.Context,
	req *connect.Request[query.ReadEraSummaryRequest],
) (*connect.Response[query.ReadEraSummaryResponse], error) {
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadEraSummary request with fieldMask %v",
			fieldMask,
		),
	)

	// Fetched chain system start time from shelley genesis
	systemStart, err := s.utxorpc.config.LedgerState.SystemStart()
	if err != nil {
		return nil, fmt.Errorf("get system start: %w", err)
	}
	// converts system start time to milliseconds
	systemStartMs := systemStart.UnixMilli()
	if systemStartMs < 0 {
		return nil, errors.New("system start is before unix epoch")
	}
	// Load all epochs from the database
	epochs, err := s.utxorpc.config.LedgerState.GetEpochs()
	if err != nil {
		return nil, fmt.Errorf("get epochs: %w", err)
	}
	if len(epochs) == 0 {
		return nil, errors.New("no epochs available for era summary")
	}
	// rearrange the order of epochs by start slot
	sort.Slice(epochs, func(i, j int) bool {
		return epochs[i].StartSlot < epochs[j].StartSlot
	})

	summaries := make([]*cardano.EraSummary, 0, len(epochs))
	summaryByEra := map[uint]*cardano.EraSummary{}
	timespanMs := uint64(0)
	baseMs := uint64(systemStartMs)
	var lastEraId uint
	var hasLastEra bool

	for _, epoch := range epochs {
		if !hasLastEra || epoch.EraId != lastEraId {
			eraDescriptor := eras.GetEraById(epoch.EraId)
			if eraDescriptor == nil {
				return nil, fmt.Errorf("unknown era ID %d", epoch.EraId)
			}
			// Build the start boundary for the era using current accumulated time and epoch metadata
			startBoundary := &cardano.EraBoundary{
				Time:  baseMs + timespanMs,
				Slot:  epoch.StartSlot,
				Epoch: epoch.EpochId,
			}
			// Create a new era summary when era changes
			summary := &cardano.EraSummary{
				Name:  eraDescriptor.Name,
				Start: startBoundary,
			}
			// Get the protocol params for the era epoch
			pparams, err := s.utxorpc.config.LedgerState.GetPParamsForEpoch(
				epoch.EpochId,
				*eraDescriptor,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"get protocol params for era %s epoch %d: %w",
					eraDescriptor.Name,
					epoch.EpochId,
					err,
				)
			}
			// Converts params into utxorpc form
			if pparams != nil {
				tmpParams, err := pparams.Utxorpc()
				if err != nil {
					return nil, fmt.Errorf(
						"convert protocol params for era %s: %w",
						eraDescriptor.Name,
						err,
					)
				}
				summary.ProtocolParams = tmpParams
			}
			// Sets the previous era end boundary to current era start boundary
			if hasLastEra {
				prevSummary := summaryByEra[lastEraId]
				if prevSummary != nil && prevSummary.GetEnd() == nil {
					prevSummary.End = &cardano.EraBoundary{
						Time:  startBoundary.GetTime(),
						Slot:  startBoundary.GetSlot(),
						Epoch: startBoundary.GetEpoch(),
					}
				}
			}
			summaries = append(summaries, summary)
			summaryByEra[epoch.EraId] = summary
			lastEraId = epoch.EraId
			hasLastEra = true
		}
		epochDurationMs := uint64(epoch.SlotLength) * uint64(epoch.LengthInSlots)
		timespanMs += epochDurationMs
	}

	resp := &query.ReadEraSummaryResponse{
		Summary: &query.ReadEraSummaryResponse_Cardano{
			Cardano: &cardano.EraSummaries{
				Summaries: summaries,
			},
		},
	}
	return connect.NewResponse(resp), nil
}

// ReadUtxos
func (s *queryServiceServer) ReadUtxos(
	ctx context.Context,
	req *connect.Request[query.ReadUtxosRequest],
) (*connect.Response[query.ReadUtxosResponse], error) {
	keys := req.Msg.GetKeys() // []*TxoRef

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf("Got a ReadUtxos request with keys %v", keys),
	)
	resp := &query.ReadUtxosResponse{}

	// Get UTxOs from ledger
	for _, txo := range keys {
		utxo, err := s.utxorpc.config.LedgerState.UtxoByRef(
			txo.GetHash(),
			txo.GetIndex(),
		)
		if err != nil {
			return nil, err
		}
		var aud query.AnyUtxoData
		ret, err := utxo.Decode()
		if err != nil {
			return nil, err
		}
		if ret == nil {
			return nil, errors.New("decode returned empty utxo")
		}
		tmpUtxo, err := ret.Utxorpc()
		if err != nil {
			return nil, fmt.Errorf("failed to convert UTxO: %w", err)
		}
		audc := query.AnyUtxoData_Cardano{
			Cardano: tmpUtxo,
		}
		aud.NativeBytes = utxo.Cbor
		aud.TxoRef = txo

		if audc.Cardano.GetDatum() != nil {
			// Check if Datum.Hash is all zeroes
			isAllZeroes := true
			for _, b := range audc.Cardano.GetDatum().GetHash() {
				if b != 0 {
					isAllZeroes = false
					break
				}
			}
			if isAllZeroes {
				// No actual datum; set Datum to nil to omit it
				audc.Cardano.Datum = nil
			}
		}
		aud.ParsedState = &audc
		resp.Items = append(resp.Items, &aud)
	}

	// Get chain point (slot and hash)
	point := s.utxorpc.config.LedgerState.Tip().Point

	// Set up response utxos
	resp.LedgerTip = &query.ChainPoint{
		Slot: point.Slot,
		Hash: point.Hash,
	}

	return connect.NewResponse(resp), nil
}

// SearchUtxos
func (s *queryServiceServer) SearchUtxos(
	ctx context.Context,
	req *connect.Request[query.SearchUtxosRequest],
) (*connect.Response[query.SearchUtxosResponse], error) {
	predicate := req.Msg.GetPredicate()   // *UtxoPredicate
	startToken := req.Msg.GetStartToken() // string
	maxItems := req.Msg.GetMaxItems()     // int32
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a SearchUtxos request with predicate %v, startToken %s, maxItems %d, and fieldMask %v",
			predicate,
			startToken,
			maxItems,
			fieldMask,
		),
	)
	resp := &query.SearchUtxosResponse{}

	// TODO: make this optional and create separate code paths
	if predicate == nil {
		return nil, fmt.Errorf("empty predicate: %v", predicate)
	}

	addressPattern := predicate.GetMatch().GetCardano().GetAddress()
	assetPattern := predicate.GetMatch().GetCardano().GetAsset()

	var addresses []ledger.Address
	if addressPattern != nil {
		// Handle Exact Address
		exactAddressBytes := addressPattern.GetExactAddress()
		if exactAddressBytes != nil {
			var addr ledger.Address
			err := addr.UnmarshalCBOR(exactAddressBytes)
			if err != nil {
				return nil, fmt.Errorf(
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
				return nil, fmt.Errorf("failed to decode payment part: %w", err)
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
				return nil, fmt.Errorf(
					"failed to decode delegation part: %w",
					err,
				)
			}
			addresses = append(addresses, delegationAddr)
		}
	}

	// Get UTxOs from ledger
	for _, address := range addresses {
		utxos, err := s.utxorpc.config.LedgerState.UtxosByAddress(address)
		if err != nil {
			return nil, err
		}
		for _, utxo := range utxos {
			var aud query.AnyUtxoData
			ret, err := utxo.Decode()
			if err != nil {
				return nil, err
			}
			if ret == nil {
				return nil, errors.New("decode returned empty utxo")
			}
			tmpUtxo, err := ret.Utxorpc()
			if err != nil {
				return nil, fmt.Errorf("failed to convert UTxO: %w", err)
			}
			audc := query.AnyUtxoData_Cardano{
				Cardano: tmpUtxo,
			}
			aud.NativeBytes = utxo.Cbor
			aud.TxoRef = &query.TxoRef{
				Hash:  utxo.TxId,
				Index: utxo.OutputIdx,
			}
			if audc.Cardano.GetDatum() != nil {
				// Check if Datum.Hash is all zeroes
				isAllZeroes := true
				for _, b := range audc.Cardano.GetDatum().GetHash() {
					if b != 0 {
						isAllZeroes = false
						break
					}
				}
				if isAllZeroes {
					// No actual datum; set Datum to nil to omit it
					audc.Cardano.Datum = nil
				}
			}
			aud.ParsedState = &audc

			// If AssetPattern is specified, filter based on it
			if assetPattern != nil {
				assetFound := false
				for _, multiasset := range audc.Cardano.GetAssets() {
					if bytes.Equal(
						multiasset.GetPolicyId(),
						assetPattern.GetPolicyId(),
					) {
						for _, asset := range multiasset.GetAssets() {
							if bytes.Equal(
								asset.GetName(),
								assetPattern.GetAssetName(),
							) {
								assetFound = true
								break
							}
						}
					}
					if assetFound {
						break
					}
				}

				// Asset not found; skip this UTxO
				if !assetFound {
					continue
				}
			}
			resp.Items = append(resp.Items, &aud)
		}
	}
	// Get chain point (slot and hash)
	point := s.utxorpc.config.LedgerState.Tip().Point

	resp.LedgerTip = &query.ChainPoint{
		Slot: point.Slot,
		Hash: point.Hash,
	}
	return connect.NewResponse(resp), nil
}

// ReadData
func (s *queryServiceServer) ReadData(
	ctx context.Context,
	req *connect.Request[query.ReadDataRequest],
) (*connect.Response[query.ReadDataResponse], error) {
	keys := req.Msg.GetKeys() // [][]byte
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadData request with keys %v and fieldMask %v",
			keys,
			fieldMask,
		),
	)
	resp := &query.ReadDataResponse{}

	for _, key := range keys {
		datum, err := s.utxorpc.config.LedgerState.Datum(key)
		if err != nil {
			if errors.Is(err, database.ErrDatumNotFound) {
				return nil, connect.NewError(
					connect.CodeNotFound,
					fmt.Errorf("datum not found: %x", key),
				)
			}
			return nil, fmt.Errorf("get datum %x: %w", key, err)
		}
		resp.Values = append(
			resp.Values,
			&query.AnyChainDatum{
				Key:         datum.Hash,
				NativeBytes: datum.RawDatum,
			},
		)
	}

	// Get chain point (slot and hash)
	point := s.utxorpc.config.LedgerState.Tip().Point

	// Set up response utxos
	resp.LedgerTip = &query.ChainPoint{
		Slot: point.Slot,
		Hash: point.Hash,
	}

	return connect.NewResponse(resp), nil
}

// ReadTx
func (s *queryServiceServer) ReadTx(
	ctx context.Context,
	req *connect.Request[query.ReadTxRequest],
) (*connect.Response[query.ReadTxResponse], error) {
	hash := req.Msg.GetHash()
	fieldMask := req.Msg.GetFieldMask()

	s.utxorpc.config.Logger.Info(
		fmt.Sprintf(
			"Got a ReadTx request with hash %x and fieldMask %v",
			hash,
			fieldMask,
		),
	)

	if len(hash) == 0 {
		return nil, connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("hash is required"),
		)
	}

	// Resolve the transaction metadata to find it's containing block.
	txRecord, err := s.utxorpc.config.LedgerState.TransactionByHash(hash)
	if err != nil {
		return nil, fmt.Errorf("lookup transaction: %w", err)
	}
	if txRecord == nil {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf("transaction not found: %x", hash),
		)
	}

	// Find the block blob to decode and extract the transaction.
	block, err := s.utxorpc.config.LedgerState.BlockByHash(txRecord.BlockHash)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, connect.NewError(
				connect.CodeNotFound,
				fmt.Errorf("block not found: %x", txRecord.BlockHash),
			)
		}
		return nil, fmt.Errorf("lookup block: %w", err)
	}

	// Decode the block CBOR into a ledger block.
	ledgerBlock, err := ledger.NewBlockFromCbor(block.Type, block.Cbor)
	if err != nil {
		return nil, fmt.Errorf("decode block: %w", err)
	}

	// Find the transaction by index first, then fall back to a hash scan.
	var tx lcommon.Transaction
	transactions := ledgerBlock.Transactions()
	if int(txRecord.BlockIndex) < len(transactions) {
		value := transactions[txRecord.BlockIndex]
		if bytes.Equal(value.Hash().Bytes(), hash) {
			tx = value
		}
	}
	if tx == nil {
		for _, candidate := range transactions {
			if bytes.Equal(candidate.Hash().Bytes(), hash) {
				tx = candidate
				break
			}
		}
	}
	if tx == nil {
		return nil, connect.NewError(
			connect.CodeNotFound,
			fmt.Errorf("transaction not found in block: %x", hash),
		)
	}

	tmpTx, err := tx.Utxorpc()
	if err != nil {
		return nil, fmt.Errorf("convert transaction: %w", err)
	}

	anyTx := &query.AnyChainTx{
		NativeBytes: tx.Cbor(),
		Chain: &query.AnyChainTx_Cardano{
			Cardano: tmpTx,
		},
		BlockRef: &query.ChainPoint{
			Slot: block.Slot,
			Hash: block.Hash,
		},
	}

	// Get chain point (slot and hash)
	point := s.utxorpc.config.LedgerState.Tip().Point

	// Set up response utxos
	resp := &query.ReadTxResponse{
		Tx: anyTx,
		LedgerTip: &query.ChainPoint{
			Slot: point.Slot,
			Hash: point.Hash,
		},
	}

	return connect.NewResponse(resp), nil
}
