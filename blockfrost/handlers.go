// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
)

const apiVersion = "0.1.0"

// writeJSON writes a JSON response with the given status
// code. If encoding fails, it logs the error for
// diagnostics (the status code cannot be changed since
// headers have already been sent).
func writeJSON(
	w http.ResponseWriter,
	status int,
	v any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Header is already sent so we cannot change the
		// status code, but we log the failure for
		// diagnostics.
		slog.Error(
			"failed to encode JSON response",
			"component", "blockfrost",
			"error", err,
		)
	}
}

// writeError writes a Blockfrost-format error response.
//
//nolint:unparam
func writeError(
	w http.ResponseWriter,
	status int,
	errStr string,
	message string,
) {
	writeJSON(w, status, ErrorResponse{
		StatusCode: status,
		Error:      errStr,
		Message:    message,
	})
}

// handleRoot handles GET / and returns API metadata.
func (b *Blockfrost) handleRoot(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(w, http.StatusOK, RootResponse{
		URL:     "https://blockfrost.io/",
		Version: apiVersion,
	})
}

// handleHealth handles GET /health and returns node health
// status.
func (b *Blockfrost) handleHealth(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(w, http.StatusOK, HealthResponse{
		IsHealthy: true,
	})
}

// handleLatestBlock handles GET /api/v0/blocks/latest and
// returns the latest block.
func (b *Blockfrost) handleLatestBlock(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.LatestBlock()
	if err != nil {
		b.logger.Error(
			"failed to get latest block",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve latest block",
		)
		return
	}
	writeJSON(w, http.StatusOK, blockResponse(info))
}

// handleBlock handles GET /api/v0/blocks/{hash_or_number}.
func (b *Blockfrost) handleBlock(
	w http.ResponseWriter,
	r *http.Request,
) {
	info, err := b.node.BlockByHashOrNumber(
		r.PathValue("hash_or_number"),
	)
	if err != nil {
		if errors.Is(err, ErrInvalidBlockID) {
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"Invalid block hash or number.",
			)
			return
		}
		if errors.Is(err, ErrBlockNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested block could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get block",
			"block", r.PathValue("hash_or_number"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve block",
		)
		return
	}
	writeJSON(w, http.StatusOK, blockResponse(info))
}

// handleLatestBlockTxs handles
// GET /api/v0/blocks/latest/txs and returns transaction
// hashes from the latest block.
func (b *Blockfrost) handleLatestBlockTxs(
	w http.ResponseWriter,
	_ *http.Request,
) {
	hashes, err := b.node.LatestBlockTxHashes()
	if err != nil {
		b.logger.Error(
			"failed to get latest block txs",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve latest block transactions",
		)
		return
	}
	if hashes == nil {
		hashes = []string{}
	}
	writeJSON(w, http.StatusOK, hashes)
}

// handleLatestEpoch handles GET /api/v0/epochs/latest and
// returns the current epoch info.
func (b *Blockfrost) handleLatestEpoch(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.CurrentEpoch()
	if err != nil {
		b.logger.Error(
			"failed to get current epoch",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve current epoch",
		)
		return
	}
	activeStake := "0"
	writeJSON(w, http.StatusOK, EpochResponse{
		Epoch:          info.Epoch,
		StartTime:      info.StartTime,
		EndTime:        info.EndTime,
		FirstBlockTime: info.FirstBlockTime,
		LastBlockTime:  info.LastBlockTime,
		BlockCount:     info.BlockCount,
		TxCount:        info.TxCount,
		Output:         "0",
		Fees:           "0",
		ActiveStake:    &activeStake,
	})
}

// handleLatestEpochParams handles
// GET /api/v0/epochs/latest/parameters and returns the
// current protocol parameters.
func (b *Blockfrost) handleLatestEpochParams(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.CurrentProtocolParams()
	if err != nil {
		b.logger.Error(
			"failed to get protocol params",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve protocol parameters",
		)
		return
	}
	writeJSON(w, http.StatusOK, protocolParamsResponse(info))
}

// handleEpochParams handles GET /api/v0/epochs/{number}/parameters and
// returns the protocol parameters for a specific epoch.
func (b *Blockfrost) handleEpochParams(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, err := strconv.ParseUint(
		r.PathValue("number"),
		10,
		64,
	)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid epoch number.",
		)
		return
	}
	info, err := b.node.EpochProtocolParams(epoch)
	if err != nil {
		b.logger.Error(
			"failed to get protocol params for epoch",
			"epoch", epoch,
			"error", err,
		)
		if errors.Is(err, ErrEpochNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested epoch could not be found.",
			)
			return
		}
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve protocol parameters",
		)
		return
	}
	writeJSON(w, http.StatusOK, protocolParamsResponse(info))
}

// handleNetwork handles GET /api/v0/network and returns
// network supply and stake information.
func (b *Blockfrost) handleNetwork(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.Network()
	if err != nil {
		b.logger.Error(
			"failed to get network info",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve network information",
		)
		return
	}
	writeJSON(w, http.StatusOK, NetworkResponse{
		Supply: NetworkSupply{
			Max:         info.Supply.Max,
			Total:       info.Supply.Total,
			Circulating: info.Supply.Circulating,
			Locked:      info.Supply.Locked,
			Treasury:    info.Supply.Treasury,
			Reserves:    info.Supply.Reserves,
		},
		Stake: NetworkStake{
			Live:   info.Stake.Live,
			Active: info.Stake.Active,
		},
	})
}

// handleNetworkEras handles GET /api/v0/network/eras.
func (b *Blockfrost) handleNetworkEras(
	w http.ResponseWriter,
	_ *http.Request,
) {
	eras, err := b.node.NetworkEras()
	if err != nil {
		b.logger.Error(
			"failed to get network eras",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve network eras",
		)
		return
	}
	resp := make([]NetworkEraResponse, 0, len(eras))
	for _, era := range eras {
		var end *NetworkEraBound
		if era.End != nil {
			end = &NetworkEraBound{
				Time:  era.End.Time,
				Slot:  era.End.Slot,
				Epoch: era.End.Epoch,
			}
		}
		resp = append(resp, NetworkEraResponse{
			Era: era.Era,
			Start: NetworkEraBound{
				Time:  era.Start.Time,
				Slot:  era.Start.Slot,
				Epoch: era.Start.Epoch,
			},
			End: end,
			Parameters: NetworkEraParameters{
				EpochLength: era.Params.EpochLength,
				SlotLength:  era.Params.SlotLength,
				SafeZone:    era.Params.SafeZone,
			},
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleGenesis handles GET /api/v0/genesis.
func (b *Blockfrost) handleGenesis(
	w http.ResponseWriter,
	_ *http.Request,
) {
	info, err := b.node.Genesis()
	if err != nil {
		b.logger.Error(
			"failed to get genesis info",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve genesis information",
		)
		return
	}
	writeJSON(w, http.StatusOK, GenesisResponse(info))
}

// handleAsset handles GET /api/v0/assets/{asset} and
// returns native asset information.
func (b *Blockfrost) handleAsset(
	w http.ResponseWriter,
	r *http.Request,
) {
	policyID, assetName, err := parseAssetIdentifier(
		r.PathValue("asset"),
	)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid asset identifier.",
		)
		return
	}

	asset, err := b.node.Asset(policyID, assetName)
	if err != nil {
		if errors.Is(err, ErrAssetNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested asset could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get asset",
			"asset", r.PathValue("asset"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve asset",
		)
		return
	}

	writeJSON(w, http.StatusOK, AssetResponse{
		Asset:             asset.Asset,
		PolicyID:          asset.PolicyID,
		AssetName:         asset.AssetName,
		AssetNameASCII:    asset.AssetNameASCII,
		Fingerprint:       asset.Fingerprint,
		Quantity:          asset.Quantity,
		InitialMintTxHash: asset.InitialMintTxHash,
		MintOrBurnCount:   asset.MintOrBurnCount,
		OnchainMetadata:   asset.OnchainMetadata,
	})
}

// handlePoolsExtended handles GET /api/v0/pools/extended
// and returns active pools with extended details.
func (b *Blockfrost) handlePoolsExtended(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, err := ParsePagination(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid pagination parameters.",
		)
		return
	}

	pools, err := b.node.PoolsExtended()
	if err != nil {
		b.logger.Error(
			"failed to get extended pools",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve pools",
		)
		return
	}

	slices.SortFunc(
		pools,
		func(a, b PoolExtendedInfo) int {
			switch {
			case a.Hex < b.Hex:
				return -1
			case a.Hex > b.Hex:
				return 1
			default:
				return 0
			}
		},
	)
	if params.Order == PaginationOrderDesc {
		slices.Reverse(pools)
	}

	totalItems := len(pools)
	SetPaginationHeaders(w, totalItems, params)

	start := (params.Page - 1) * params.Count
	if start >= totalItems {
		writeJSON(w, http.StatusOK, []PoolExtendedResponse{})
		return
	}
	end := min(start+params.Count, totalItems)

	resp := make([]PoolExtendedResponse, 0, end-start)
	for _, pool := range pools[start:end] {
		relays := make([]PoolRelayResponse, 0, len(pool.Relays))
		for _, relay := range pool.Relays {
			tmpRelay := PoolRelayResponse{}
			if relay.IPv4 != "" {
				tmpRelay.IPv4 = &relay.IPv4
			}
			if relay.IPv6 != "" {
				tmpRelay.IPv6 = &relay.IPv6
			}
			if relay.DNS != "" {
				tmpRelay.DNS = &relay.DNS
			}
			if relay.Port != nil {
				tmpRelay.Port = relay.Port
			}
			relays = append(relays, tmpRelay)
		}
		resp = append(resp, PoolExtendedResponse{
			PoolID:         pool.PoolID,
			Hex:            pool.Hex,
			VrfKey:         pool.VrfKey,
			ActiveStake:    pool.ActiveStake,
			LiveStake:      pool.LiveStake,
			DeclaredPledge: pool.DeclaredPledge,
			FixedCost:      pool.FixedCost,
			MarginCost:     pool.MarginCost,
			Relays:         relays,
		})
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleAddressUTXOs handles GET /api/v0/addresses/{address}/utxos
// and returns the current UTxOs for an address.
func (b *Blockfrost) handleAddressUTXOs(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	address := r.PathValue("address")
	utxos, total, err := b.node.AddressUTXOs(address, params)
	if err != nil {
		b.logger.Error(
			"failed to get address utxos",
			"address", address,
			"error", err,
		)
		writeNodeQueryError(
			w,
			err,
			"failed to retrieve address UTxOs",
		)
		return
	}

	SetPaginationHeaders(w, total, params)
	resp := make([]AddressUTXOResponse, 0, len(utxos))
	for _, utxo := range utxos {
		resp = append(resp, AddressUTXOResponse{
			Address:             utxo.Address,
			TxHash:              utxo.TxHash,
			OutputIndex:         int(utxo.OutputIndex),
			Amount:              convertAddressAmounts(utxo.Amount),
			Block:               utxo.Block,
			DataHash:            utxo.DataHash,
			InlineDatum:         utxo.InlineDatum,
			ReferenceScriptHash: utxo.ReferenceScriptHash,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAddressTransactions handles GET /api/v0/addresses/{address}/transactions
// and returns transaction history for an address.
func (b *Blockfrost) handleAddressTransactions(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	address := r.PathValue("address")
	txs, total, err := b.node.AddressTransactions(address, params)
	if err != nil {
		b.logger.Error(
			"failed to get address transactions",
			"address", address,
			"error", err,
		)
		writeNodeQueryError(
			w,
			err,
			"failed to retrieve address transactions",
		)
		return
	}

	SetPaginationHeaders(w, total, params)
	resp := make([]AddressTransactionResponse, 0, len(txs))
	for _, tx := range txs {
		resp = append(resp, AddressTransactionResponse{
			TxHash:      tx.TxHash,
			TxIndex:     int(tx.TxIndex),
			BlockHeight: tx.BlockHeight,
			BlockTime:   int(tx.BlockTime),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleMetadataTransactions handles
// GET /api/v0/metadata/txs/labels/{label} and returns metadata values for the
// requested label in JSON form.
func (b *Blockfrost) handleMetadataTransactions(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	label, ok := parseMetadataLabelOrWriteError(w, r)
	if !ok {
		return
	}

	rows, total, err := b.node.MetadataTransactions(label, params)
	if err != nil {
		b.logger.Error(
			"failed to get metadata transactions",
			"label", label,
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction metadata",
		)
		return
	}

	SetPaginationHeaders(w, total, params)
	resp := make([]MetadataTransactionJSONResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, MetadataTransactionJSONResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleMetadataTransactionsCBOR handles
// GET /api/v0/metadata/txs/labels/{label}/cbor and returns metadata values
// for the requested label in CBOR-hex form.
func (b *Blockfrost) handleMetadataTransactionsCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	label, ok := parseMetadataLabelOrWriteError(w, r)
	if !ok {
		return
	}

	rows, total, err := b.node.MetadataTransactionsCBOR(label, params)
	if err != nil {
		b.logger.Error(
			"failed to get CBOR metadata transactions",
			"label", label,
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction metadata",
		)
		return
	}

	SetPaginationHeaders(w, total, params)
	resp := make([]MetadataTransactionCBORResponse, 0, len(rows))
	for _, row := range rows {
		metadata := row.Metadata
		resp = append(resp, MetadataTransactionCBORResponse{
			TxHash:       row.TxHash,
			CborMetadata: &metadata,
			Metadata:     metadata,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func parsePaginationOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
) (PaginationParams, bool) {
	params, err := ParsePagination(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid pagination parameters.",
		)
		return PaginationParams{}, false
	}
	return params, true
}

func parseMetadataLabelOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
) (uint64, bool) {
	label, err := strconv.ParseUint(r.PathValue("label"), 10, 64)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid metadata label.",
		)
		return 0, false
	}
	return label, true
}

func parseAssetIdentifier(
	asset string,
) (string, []byte, error) {
	const (
		policyIDHexLen        = 56
		maxAssetNameHexLen    = 64
		maxAssetIdentifierLen = policyIDHexLen + maxAssetNameHexLen
	)

	if len(asset) < policyIDHexLen {
		return "", nil, errors.New("asset ID too short")
	}
	if len(asset) > maxAssetIdentifierLen {
		return "", nil, errors.New("asset ID too long")
	}
	policyID := asset[:policyIDHexLen]
	assetNameHex := asset[policyIDHexLen:]
	if len(assetNameHex) > maxAssetNameHexLen {
		return "", nil, errors.New("asset name too long")
	}

	if _, err := hex.DecodeString(policyID); err != nil {
		return "", nil, err
	}
	assetName, err := hex.DecodeString(assetNameHex)
	if err != nil {
		return "", nil, err
	}
	return policyID, assetName, nil
}

func writeNodeQueryError(
	w http.ResponseWriter,
	err error,
	message string,
) {
	if errors.Is(err, ErrInvalidAddress) {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid address provided.",
		)
		return
	}
	writeError(
		w,
		http.StatusInternalServerError,
		"Internal Server Error",
		message,
	)
}

func convertAddressAmounts(
	amounts []AddressAmountInfo,
) []AddressAmountResponse {
	ret := make([]AddressAmountResponse, 0, len(amounts))
	for _, amount := range amounts {
		ret = append(ret, AddressAmountResponse(amount))
	}
	return ret
}

func assetNameASCII(
	assetName []byte,
) string {
	ret := make([]byte, 0, len(assetName))
	for _, b := range assetName {
		if b < 32 || b > 126 {
			return ""
		}
		ret = append(ret, b)
	}
	return string(ret)
}

func blockResponse(info BlockInfo) BlockResponse {
	output := "0"
	fees := "0"
	return BlockResponse{
		Hash:          info.Hash,
		Slot:          info.Slot,
		Epoch:         info.Epoch,
		EpochSlot:     info.EpochSlot,
		Height:        info.Height,
		Time:          info.Time,
		Size:          info.Size,
		TxCount:       info.TxCount,
		SlotLeader:    info.SlotLeader,
		PreviousBlock: info.PreviousBlock,
		Confirmations: info.Confirmations,
		Output:        &output,
		Fees:          &fees,
		BlockVRF:      nil,
		NextBlock:     nil,
	}
}

func protocolParamsResponse(
	info ProtocolParamsInfo,
) ProtocolParamsResponse {
	return ProtocolParamsResponse{
		Epoch:                 info.Epoch,
		MinFeeA:               info.MinFeeA,
		MinFeeB:               info.MinFeeB,
		MaxBlockSize:          info.MaxBlockSize,
		MaxTxSize:             info.MaxTxSize,
		MaxBlockHeaderSize:    info.MaxBlockHeaderSize,
		KeyDeposit:            info.KeyDeposit,
		PoolDeposit:           info.PoolDeposit,
		EMax:                  info.EMax,
		NOpt:                  info.NOpt,
		A0:                    info.A0,
		Rho:                   info.Rho,
		Tau:                   info.Tau,
		ProtocolMajorVer:      info.ProtocolMajorVer,
		ProtocolMinorVer:      info.ProtocolMinorVer,
		MinPoolCost:           info.MinPoolCost,
		CoinsPerUtxoSize:      &info.CoinsPerUtxoSize,
		CoinsPerUtxoWord:      info.CoinsPerUtxoSize,
		PriceMem:              &info.PriceMem,
		PriceStep:             &info.PriceStep,
		MaxTxExMem:            &info.MaxTxExMem,
		MaxTxExSteps:          &info.MaxTxExSteps,
		MaxBlockExMem:         &info.MaxBlockExMem,
		MaxBlockExSteps:       &info.MaxBlockExSteps,
		MaxValSize:            &info.MaxValSize,
		CollateralPercent:     &info.CollateralPercent,
		MaxCollateralInputs:   &info.MaxCollateralInputs,
		MinUtxo:               "0",
		Nonce:                 "",
		DecentralisationParam: 0,
		ExtraEntropy:          nil,
	}
}
