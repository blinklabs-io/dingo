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
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/btcsuite/btcd/btcutil/bech32"
)

const (
	apiVersion = "0.1.0"

	// DRep credentials are Blake2b-224 hashes: 224 bits = 28 bytes.
	drepCredentialHashLen = 28
	// CIP-129 DRep identifiers include a one-byte voter header followed
	// by the 28-byte credential hash.
	drepCIP129PayloadLen = drepCredentialHashLen + 1
	// Hex encodes each byte as two characters, so a 28-byte credential
	// hash is represented by 56 hex characters.
	drepCredentialHexLen = drepCredentialHashLen * 2
	maxTxBodySize        = 64 * 1024
)

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

// handleAssetAddresses handles GET /api/v0/assets/{asset}/addresses and
// returns paginated addresses currently holding the given asset.
func (b *Blockfrost) handleAssetAddresses(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
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
	holders, total, err := b.node.AssetAddresses(policyID, assetName, params)
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
			"failed to get asset addresses",
			"asset", r.PathValue("asset"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve asset addresses",
		)
		return
	}
	if total == 0 {
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested asset could not be found.",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]AssetAddressResponse, 0, len(holders))
	for _, h := range holders {
		resp = append(resp, AssetAddressResponse(h))
	}
	writeJSON(w, http.StatusOK, resp)
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

// handleDRep handles GET /api/v0/governance/dreps/{drep_id}
// and returns DRep governance information.
func (b *Blockfrost) handleDRep(
	w http.ResponseWriter,
	r *http.Request,
) {
	credential, err := parseDRepIdentifier(r.PathValue("drep_id"))
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid DRep identifier.",
		)
		return
	}

	drep, err := b.node.DRep(credential)
	if err != nil {
		if errors.Is(err, ErrDRepNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested DRep could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get drep",
			"drep_id", r.PathValue("drep_id"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve DRep",
		)
		return
	}

	writeJSON(w, http.StatusOK, DRepResponse(drep))
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
			TxIndex:             int(utxo.TxIndex),
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

// handleTransaction handles GET /api/v0/txs/{hash} and returns
// transaction summary details.
func (b *Blockfrost) handleTransaction(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	tx, err := b.node.Transaction(hash)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested transaction could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get transaction",
			"hash", r.PathValue("hash"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction",
		)
		return
	}

	treasuryDonation := tx.TreasuryDonation
	if treasuryDonation == "" {
		treasuryDonation = "0"
	}

	writeJSON(w, http.StatusOK, TransactionResponse{
		Hash:               tx.Hash,
		Block:              tx.Block,
		Slot:               tx.Slot,
		BlockHeight:        tx.BlockHeight,
		BlockTime:          tx.BlockTime,
		Index:              int(tx.Index),
		OutputAmount:       convertAddressAmounts(tx.OutputAmount),
		Fees:               tx.Fees,
		Deposit:            tx.Deposit,
		TreasuryDonation:   treasuryDonation,
		Size:               tx.Size,
		UtxoCount:          tx.UtxoCount,
		WithdrawalCount:    tx.WithdrawalCount,
		MirCertCount:       tx.MirCertCount,
		DelegationCount:    tx.DelegationCount,
		StakeCertCount:     tx.StakeCertCount,
		PoolUpdateCount:    tx.PoolUpdateCount,
		PoolRetireCount:    tx.PoolRetireCount,
		AssetMintBurnCount: tx.AssetMintBurnCount,
		RedeemerCount:      tx.RedeemerCount,
		ValidContract:      tx.ValidContract,
		InvalidBefore:      tx.InvalidBefore,
		InvalidHereafter:   tx.InvalidHereafter,
	})
}

// handleTransactionSubmit handles POST /api/v0/tx/submit and submits raw
// signed transaction CBOR to the mempool.
func (b *Blockfrost) handleTransactionSubmit(
	w http.ResponseWriter,
	r *http.Request,
) {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/cbor" {
		writeError(
			w,
			http.StatusUnsupportedMediaType,
			"Unsupported Media Type",
			"Content-Type must be application/cbor.",
		)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxTxBodySize)
	txCbor, err := io.ReadAll(r.Body)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeError(
				w,
				http.StatusRequestEntityTooLarge,
				"Request Entity Too Large",
				"transaction body exceeds maximum allowed size",
			)
			return
		}
		b.logger.Error(
			"failed to read transaction submit body",
			"error", err,
		)
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"failed to read transaction body",
		)
		return
	}
	if len(txCbor) == 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"transaction body is empty",
		)
		return
	}

	hash, err := b.node.TransactionSubmit(txCbor)
	if err != nil {
		if errors.Is(err, ErrInvalidTransaction) {
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"Invalid transaction CBOR.",
			)
			return
		}
		if errors.Is(err, ErrMempoolUnavailable) {
			writeError(
				w,
				http.StatusServiceUnavailable,
				"Service Unavailable",
				"mempool unavailable",
			)
			return
		}
		if errors.Is(err, ErrMempoolFull) {
			writeError(
				w,
				425,
				"Mempool Full",
				"mempool is full, try again later",
			)
			return
		}
		b.logger.Error(
			"failed to submit transaction",
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to submit transaction",
		)
		return
	}

	writeJSON(w, http.StatusOK, hash)
}

// handleTransactionCBOR handles GET /api/v0/txs/{hash}/cbor and returns
// the raw signed transaction CBOR as a hex string.
func (b *Blockfrost) handleTransactionCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	txCbor, err := b.node.TransactionCBOR(hash)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested transaction could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get transaction CBOR",
			"hash", r.PathValue("hash"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction CBOR",
		)
		return
	}

	writeJSON(w, http.StatusOK, TransactionCBORResponse{
		CBOR: hex.EncodeToString(txCbor),
	})
}

// handleTransactionMetadata handles GET /api/v0/txs/{hash}/metadata and
// returns transaction metadata labels as JSON values.
func (b *Blockfrost) handleTransactionMetadata(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionMetadata(hash)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested transaction could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get transaction metadata",
			"hash", r.PathValue("hash"),
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

	resp := make([]TransactionMetadataResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionMetadataResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionMetadataCBOR handles GET /api/v0/txs/{hash}/metadata/cbor
// and returns transaction metadata labels as CBOR values.
func (b *Blockfrost) handleTransactionMetadataCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionMetadataCBOR(hash)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested transaction could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get transaction metadata CBOR",
			"hash", r.PathValue("hash"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction metadata CBOR",
		)
		return
	}

	resp := make([]TransactionMetadataCBORResponse, 0, len(rows))
	for _, row := range rows {
		metadata := row.CBORMetadata
		resp = append(resp, TransactionMetadataCBORResponse{
			Label:        row.Label,
			CborMetadata: &metadata,
			Metadata:     metadata,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionUTXOs handles GET /api/v0/txs/{hash}/utxos and returns
// transaction inputs and outputs.
func (b *Blockfrost) handleTransactionUTXOs(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	utxos, err := b.node.TransactionUTXOs(hash)
	if err != nil {
		if errors.Is(err, ErrTransactionNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested transaction could not be found.",
			)
			return
		}
		b.logger.Error(
			"failed to get transaction UTxOs",
			"hash", r.PathValue("hash"),
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction UTxOs",
		)
		return
	}

	resp := TransactionUTXOsResponse{
		Hash:    utxos.Hash,
		Inputs:  make([]TransactionInputResponse, 0, len(utxos.Inputs)),
		Outputs: make([]TransactionOutputResponse, 0, len(utxos.Outputs)),
	}
	for _, input := range utxos.Inputs {
		resp.Inputs = append(resp.Inputs, TransactionInputResponse{
			Address:             input.Address,
			Amount:              convertAddressAmounts(input.Amount),
			TxHash:              input.TxHash,
			OutputIndex:         int(input.OutputIndex),
			DataHash:            input.DataHash,
			Collateral:          input.Collateral,
			InlineDatum:         input.InlineDatum,
			ReferenceScriptHash: input.ReferenceScriptHash,
			Reference:           input.Reference,
		})
	}
	for _, output := range utxos.Outputs {
		resp.Outputs = append(resp.Outputs, TransactionOutputResponse{
			Address:             output.Address,
			Amount:              convertAddressAmounts(output.Amount),
			OutputIndex:         int(output.OutputIndex),
			DataHash:            output.DataHash,
			InlineDatum:         output.InlineDatum,
			ReferenceScriptHash: output.ReferenceScriptHash,
			Collateral:          output.Collateral,
			ConsumedByTx:        output.ConsumedByTx,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionDelegations handles GET /api/v0/txs/{hash}/delegations and
// returns delegation certificates in the transaction.
func (b *Blockfrost) handleTransactionDelegations(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionDelegations(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction delegations",
			"failed to retrieve transaction delegations",
		)
		return
	}

	resp := make([]TransactionDelegationResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionDelegationResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionStakeAddresses handles GET /api/v0/txs/{hash}/stakes and
// returns stake registration/deregistration certificates in the transaction.
func (b *Blockfrost) handleTransactionStakeAddresses(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionStakeAddresses(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction stake addresses",
			"failed to retrieve transaction stake addresses",
		)
		return
	}

	resp := make([]TransactionStakeAddressResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionStakeAddressResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionWithdrawals handles GET /api/v0/txs/{hash}/withdrawals
// and returns reward withdrawals in the transaction.
func (b *Blockfrost) handleTransactionWithdrawals(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionWithdrawals(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction withdrawals",
			"failed to retrieve transaction withdrawals",
		)
		return
	}

	resp := make([]TransactionWithdrawalResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionWithdrawalResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionMIRs handles GET /api/v0/txs/{hash}/mirs and returns MIR
// certificates in the transaction.
func (b *Blockfrost) handleTransactionMIRs(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionMIRs(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction MIR certificates",
			"failed to retrieve transaction MIR certificates",
		)
		return
	}

	resp := make([]TransactionMIRResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionMIRResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionPoolUpdates handles GET /api/v0/txs/{hash}/pool_updates
// and returns pool registration certificates in the transaction.
func (b *Blockfrost) handleTransactionPoolUpdates(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionPoolUpdates(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction pool updates",
			"failed to retrieve transaction pool updates",
		)
		return
	}

	resp := make([]TransactionPoolUpdateResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionPoolUpdateResponse{
			ActiveEpoch:   row.ActiveEpoch,
			CertIndex:     row.CertIndex,
			FixedCost:     row.FixedCost,
			MarginCost:    row.MarginCost,
			Owners:        row.Owners,
			Pledge:        row.Pledge,
			PoolID:        row.PoolID,
			Relays:        transactionPoolRelayResponses(row.Relays),
			RewardAccount: row.RewardAccount,
			VrfKey:        row.VrfKey,
			Metadata:      transactionPoolMetadataResponse(row.Metadata),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionPoolRetires handles GET /api/v0/txs/{hash}/pool_retires
// and returns pool retirement certificates in the transaction.
func (b *Blockfrost) handleTransactionPoolRetires(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionPoolRetires(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction pool retires",
			"failed to retrieve transaction pool retires",
		)
		return
	}

	resp := make([]TransactionPoolRetireResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionPoolRetireResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionRedeemers handles GET /api/v0/txs/{hash}/redeemers and
// returns Plutus redeemers in the transaction.
func (b *Blockfrost) handleTransactionRedeemers(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionRedeemers(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction redeemers",
			"failed to retrieve transaction redeemers",
		)
		return
	}

	resp := make([]TransactionRedeemerResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionRedeemerResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleTransactionRequiredSigners handles GET /api/v0/txs/{hash}/required_signers
// and returns the required signing key hashes in the transaction.
func (b *Blockfrost) handleTransactionRequiredSigners(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}

	rows, err := b.node.TransactionRequiredSigners(hash)
	if err != nil {
		b.writeTransactionEndpointError(
			w,
			r,
			err,
			"failed to get transaction required signers",
			"failed to retrieve transaction required signers",
		)
		return
	}

	resp := make([]TransactionRequiredSignerResponse, 0, len(rows))
	for _, row := range rows {
		resp = append(resp, TransactionRequiredSignerResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (b *Blockfrost) writeTransactionEndpointError(
	w http.ResponseWriter,
	r *http.Request,
	err error,
	logMessage string,
	clientMessage string,
) {
	if errors.Is(err, ErrTransactionNotFound) {
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested transaction could not be found.",
		)
		return
	}
	b.logger.Error(
		logMessage,
		"hash", r.PathValue("hash"),
		"error", err,
	)
	writeError(
		w,
		http.StatusInternalServerError,
		"Internal Server Error",
		clientMessage,
	)
}

func parseTransactionHashOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
) ([]byte, bool) {
	hash, err := hex.DecodeString(r.PathValue("hash"))
	if err != nil || len(hash) != 32 {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid transaction hash.",
		)
		return nil, false
	}
	return hash, true
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

func parseDRepIdentifier(
	id string,
) (DRepCredential, error) {
	if id == "" {
		return DRepCredential{}, errors.New("empty DRep identifier")
	}
	// Blockfrost accepts the raw credential hash as hex. Storage uses the
	// raw 28-byte hash for lookup.
	if len(id) == drepCredentialHexLen {
		hash, err := hex.DecodeString(id)
		if err == nil {
			return DRepCredential{
				ID:        id,
				Hash:      hash,
				HasScript: false,
			}, nil
		}
	}

	// Non-hex input must be bech32. The HRP is the readable prefix before
	// the separator "1" (for example, "drep" in "drep1...").
	hrp, data, err := bech32.Decode(id)
	if err != nil {
		return DRepCredential{}, fmt.Errorf("decode DRep bech32: %w", err)
	}
	hrp = strings.ToLower(hrp)
	if hrp != "drep" {
		return DRepCredential{}, fmt.Errorf("invalid DRep prefix %q", hrp)
	}
	// Bech32 stores payload data as 5-bit groups. Convert it back to
	// normal 8-bit bytes before checking the credential payload.
	payload, err := bech32.ConvertBits(data, 5, 8, false)
	if err != nil {
		return DRepCredential{}, fmt.Errorf("decode DRep payload: %w", err)
	}

	switch len(payload) {
	case drepCredentialHashLen:
		return DRepCredential{
			ID:        id,
			Hash:      payload,
			HasScript: false,
		}, nil
	case drepCIP129PayloadLen:
		hasScript, err := parseCIP129DRepScriptFlag(payload[0])
		if err != nil {
			return DRepCredential{}, err
		}
		return DRepCredential{
			ID:                 id,
			Hash:               payload[1:],
			HasScript:          hasScript,
			CredentialTagKnown: true,
		}, nil
	default:
		return DRepCredential{}, fmt.Errorf(
			"invalid DRep credential length %d",
			len(payload),
		)
	}
}

func parseCIP129DRepScriptFlag(header byte) (bool, error) {
	voterType := header >> 4
	credentialNibble := header & 0x0f
	switch voterType {
	case 2:
		if credentialNibble != 0x2 {
			return false, fmt.Errorf(
				"invalid DRep key credential nibble 0x%x",
				credentialNibble,
			)
		}
		return false, nil
	case 3:
		if credentialNibble != 0x3 {
			return false, fmt.Errorf(
				"invalid DRep script credential nibble 0x%x",
				credentialNibble,
			)
		}
		return true, nil
	default:
		return false, fmt.Errorf("invalid DRep voter type %d", voterType)
	}
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

func transactionPoolMetadataResponse(
	metadata *TransactionPoolMetadataInfo,
) *TransactionPoolMetadataResponse {
	if metadata == nil {
		return nil
	}
	return &TransactionPoolMetadataResponse{
		Hash: metadata.Hash,
		URL:  metadata.URL,
	}
}

func transactionPoolRelayResponses(
	relays []TransactionPoolRelayInfo,
) []TransactionPoolRelayResponse {
	ret := make([]TransactionPoolRelayResponse, 0, len(relays))
	for _, relay := range relays {
		ret = append(ret, TransactionPoolRelayResponse{
			DNS:    optionalResponseString(relay.DNS),
			DNSSrv: optionalResponseString(relay.DNSSrv),
			IPv4:   optionalResponseString(relay.IPv4),
			IPv6:   optionalResponseString(relay.IPv6),
			Port:   relay.Port,
		})
	}
	return ret
}

func optionalResponseString(v string) *string {
	if v == "" {
		return nil
	}
	return &v
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
	output := info.Output
	fees := info.Fees
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
		BlockVRF:      info.BlockVRF,
		OPCert:        info.OPCert,
		OPCertCounter: info.OPCertCounter,
		NextBlock:     info.NextBlock,
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
		// Conway-era governance and reference-script parameters. nil for
		// pre-Conway eras, where they serialize as JSON null.
		PvtMotionNoConfidence:    info.PvtMotionNoConfidence,
		PvtCommitteeNormal:       info.PvtCommitteeNormal,
		PvtCommitteeNoConfidence: info.PvtCommitteeNoConfidence,
		PvtHardForkInitiation:    info.PvtHardForkInitiation,
		DvtMotionNoConfidence:    info.DvtMotionNoConfidence,
		DvtCommitteeNormal:       info.DvtCommitteeNormal,
		DvtCommitteeNoConfidence: info.DvtCommitteeNoConfidence,
		DvtUpdateToConstitution:  info.DvtUpdateToConstitution,
		DvtHardForkInitiation:    info.DvtHardForkInitiation,
		DvtPPNetworkGroup:        info.DvtPPNetworkGroup,
		DvtPPEconomicGroup:       info.DvtPPEconomicGroup,
		DvtPPTechnicalGroup:      info.DvtPPTechnicalGroup,
		DvtPPGovGroup:            info.DvtPPGovGroup,
		DvtTreasuryWithdrawal:    info.DvtTreasuryWithdrawal,
		CommitteeMinSize:         info.CommitteeMinSize,
		CommitteeMaxTermLength:   info.CommitteeMaxTermLength,
		GovActionLifetime:        info.GovActionLifetime,
		GovActionDeposit:         info.GovActionDeposit,
		DrepDeposit:              info.DRepDeposit,
		DrepActivity:             info.DRepActivity,
		// Blockfrost exposes the pool-voting security-group threshold under
		// both the legacy "pvtpp_security_group" key and the current
		// "pvt_p_p_security_group" key; populate both from the same value.
		PvtppSecurityGroup:         info.PvtPPSecurityGroup,
		PvtPPSecurityGroup:         info.PvtPPSecurityGroup,
		MinFeeRefScriptCostPerByte: info.MinFeeRefScriptCostPerByte,
		CostModelsRaw:              info.CostModelsRaw,
	}
}

func (b *Blockfrost) handleAccount(
	w http.ResponseWriter,
	r *http.Request,
) {
	account, err := b.node.Account(r.PathValue("stake_address"))
	if err != nil {
		b.writeAccountError(w, err, "failed to retrieve account")
		return
	}
	writeJSON(w, http.StatusOK, AccountResponse(account))
}

func (b *Blockfrost) handleAccountAssociatedAddresses(
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
	items, total, err := b.node.AccountAssociatedAddresses(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(
			w, err, "failed to retrieve account addresses",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make(
		[]AccountAssociatedAddressResponse,
		0,
		len(items),
	)
	for _, item := range items {
		resp = append(
			resp,
			AccountAssociatedAddressResponse(item),
		)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (b *Blockfrost) handleAccountDelegationHistory(
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
	items, total, err := b.node.AccountDelegationHistory(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(
			w, err, "failed to retrieve account delegation history",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make(
		[]AccountDelegationHistoryResponse,
		0,
		len(items),
	)
	for _, item := range items {
		resp = append(
			resp,
			AccountDelegationHistoryResponse(item),
		)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (b *Blockfrost) handleAccountRegistrationHistory(
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
	items, total, err := b.node.AccountRegistrationHistory(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(
			w, err, "failed to retrieve account registration history",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make(
		[]AccountRegistrationHistoryResponse,
		0,
		len(items),
	)
	for _, item := range items {
		resp = append(
			resp,
			AccountRegistrationHistoryResponse(item),
		)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (b *Blockfrost) handleAccountRewardHistory(
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
	items, total, err := b.node.AccountRewardHistory(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(
			w, err, "failed to retrieve account reward history",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]AccountRewardHistoryResponse, 0, len(items))
	for _, item := range items {
		resp = append(
			resp,
			AccountRewardHistoryResponse(item),
		)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (b *Blockfrost) writeAccountError(
	w http.ResponseWriter,
	err error,
	internalMessage string,
) {
	switch {
	case errors.Is(err, ErrInvalidStakeAddress):
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid stake address.",
		)
	case errors.Is(err, models.ErrAccountNotFound):
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested component has not been found.",
		)
	default:
		b.logger.Error(internalMessage, "error", err)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			internalMessage,
		)
	}
}
