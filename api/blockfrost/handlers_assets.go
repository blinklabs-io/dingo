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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package blockfrost

import (
	"context"
	"encoding/hex"
	"errors"
	"net/http"
)

// policyIDHexLen is the length of a hex-encoded minting policy ID.
const policyIDHexLen = 56

// handleAssetSection serves GET /api/v0/assets/{asset}/{section} and
// GET /api/v0/assets/policy/{policy_id}. The literal "policy" segment and the
// asset wildcard overlap, which ServeMux refuses to register as separate
// patterns, so one pattern dispatches both.
func (b *Blockfrost) handleAssetSection(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.PathValue("asset") == "policy" {
		b.handleAssetsByPolicy(w, r)
		return
	}
	switch r.PathValue("section") {
	case "addresses":
		b.handleAssetAddresses(w, r)
	case "history":
		b.handleAssetHistory(w, r)
	case "txs":
		b.handleAssetTransactionHashes(w, r)
	case "transactions":
		b.handleAssetTransactions(w, r)
	case "utxos":
		b.handleAssetUTXOs(w, r)
	default:
		b.handleNotFound(w, r)
	}
}

func (b *Blockfrost) writeAssetQueryError(
	w http.ResponseWriter,
	err error,
	what string,
	attrs ...any,
) {
	if b.writeAssetLookupLimitError(w, err) {
		return
	}
	if errors.Is(err, ErrAssetNotFound) {
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested asset could not be found.",
		)
		return
	}
	b.logger.Error("failed to get "+what, append(attrs, "error", err)...)
	writeError(
		w,
		http.StatusInternalServerError,
		"Internal Server Error",
		"failed to retrieve "+what,
	)
}

func writeAssetList(
	w http.ResponseWriter,
	items []AssetListItemInfo,
	total int,
	params PaginationParams,
) {
	SetPaginationHeaders(w, total, params)
	resp := make([]AssetListItemResponse, 0, len(items))
	for _, item := range items {
		resp = append(resp, AssetListItemResponse(item))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAssets handles GET /api/v0/assets.
func (b *Blockfrost) handleAssets(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	release, ok := b.admitAssetLookup(w, r)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), assetDetailTimeout)
	defer cancel()
	items, total, err := b.node.Assets(ctx, params)
	if err != nil {
		b.writeAssetQueryError(w, err, "assets")
		return
	}
	writeAssetList(w, items, total, params)
}

// handleAssetsByPolicy handles GET /api/v0/assets/policy/{policy_id}.
func (b *Blockfrost) handleAssetsByPolicy(
	w http.ResponseWriter,
	r *http.Request,
) {
	policyID := r.PathValue("section")
	if raw, err := hex.DecodeString(policyID); err != nil ||
		len(policyID) != policyIDHexLen || len(raw) == 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid policy ID.",
		)
		return
	}
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	release, ok := b.admitAssetLookup(w, r)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), assetDetailTimeout)
	defer cancel()
	items, total, err := b.node.AssetsByPolicy(ctx, policyID, params)
	if err != nil {
		b.writeAssetQueryError(w, err, "policy assets", "policy", policyID)
		return
	}
	writeAssetList(w, items, total, params)
}

// handleAssetHistory handles GET /api/v0/assets/{asset}/history.
func (b *Blockfrost) handleAssetHistory(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	policyID, assetName, ok := parseAssetOrWriteError(w, r.PathValue("asset"))
	if !ok {
		return
	}
	items, total, err := b.node.AssetHistory(
		r.Context(), policyID, assetName, params,
	)
	if err != nil {
		b.writeAssetQueryError(
			w, err, "asset history", "asset", r.PathValue("asset"),
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]AssetHistoryResponse, 0, len(items))
	for _, item := range items {
		resp = append(resp, AssetHistoryResponse(item))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAssetTransactions handles GET /api/v0/assets/{asset}/transactions.
func (b *Blockfrost) handleAssetTransactions(
	w http.ResponseWriter,
	r *http.Request,
) {
	pagination, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	policyID, assetName, ok := parseAssetOrWriteError(w, r.PathValue("asset"))
	if !ok {
		return
	}
	params, ok := parseTransactionRangeOrWriteError(w, r, pagination)
	if !ok {
		return
	}
	txs, total, err := b.node.AssetTransactions(
		r.Context(), policyID, assetName, params,
	)
	if err != nil {
		b.writeAssetQueryError(
			w, err, "asset transactions", "asset", r.PathValue("asset"),
		)
		return
	}
	SetPaginationHeaders(w, total, pagination)
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

// handleAssetTransactionHashes handles the deprecated
// GET /api/v0/assets/{asset}/txs, which lists transaction hashes without a
// block range.
func (b *Blockfrost) handleAssetTransactionHashes(
	w http.ResponseWriter,
	r *http.Request,
) {
	pagination, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	policyID, assetName, ok := parseAssetOrWriteError(w, r.PathValue("asset"))
	if !ok {
		return
	}
	txs, total, err := b.node.AssetTransactions(
		r.Context(),
		policyID,
		assetName,
		TransactionRangeParams{Pagination: pagination},
	)
	if err != nil {
		b.writeAssetQueryError(
			w, err, "asset transactions", "asset", r.PathValue("asset"),
		)
		return
	}
	SetPaginationHeaders(w, total, pagination)
	resp := make([]string, 0, len(txs))
	for _, tx := range txs {
		resp = append(resp, tx.TxHash)
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAssetUTXOs handles GET /api/v0/assets/{asset}/utxos.
func (b *Blockfrost) handleAssetUTXOs(
	w http.ResponseWriter,
	r *http.Request,
) {
	pagination, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	policyID, assetName, ok := parseAssetOrWriteError(w, r.PathValue("asset"))
	if !ok {
		return
	}
	params, ok := parseTransactionRangeOrWriteError(w, r, pagination)
	if !ok {
		return
	}
	utxos, total, err := b.node.AssetUTXOs(
		r.Context(), policyID, assetName, params,
	)
	if err != nil {
		b.writeAssetQueryError(
			w, err, "asset UTxOs", "asset", r.PathValue("asset"),
		)
		return
	}
	SetPaginationHeaders(w, total, pagination)
	resp := make([]AssetUTXOResponse, 0, len(utxos))
	for _, utxo := range utxos {
		resp = append(resp, AssetUTXOResponse{
			Address:             utxo.Address,
			TxHash:              utxo.TxHash,
			OutputIndex:         int(utxo.OutputIndex),
			Amount:              convertAddressAmounts(utxo.Amount),
			Block:               utxo.Block,
			BlockHeight:         utxo.BlockHeight,
			BlockTime:           utxo.BlockTime,
			DataHash:            utxo.DataHash,
			InlineDatum:         utxo.InlineDatum,
			InlineDatumJSON:     utxo.InlineDatumJSON,
			ReferenceScriptHash: utxo.ReferenceScriptHash,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
