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
	"errors"
	"net/http"
)

// writeAddressQueryError maps the errors of the address aggregate handlers.
func (b *Blockfrost) writeAddressQueryError(
	w http.ResponseWriter,
	err error,
	address string,
	what string,
) {
	if errors.Is(err, ErrAddressNotFound) {
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested component has not been found.",
		)
		return
	}
	b.logger.Error(
		"failed to get "+what,
		"address", address,
		"error", err,
	)
	writeNodeQueryError(w, err, "failed to retrieve "+what)
}

// handleAddressTransactionHashes handles the deprecated
// GET /api/v0/addresses/{address}/txs, which lists transaction hashes without
// a block range.
func (b *Blockfrost) handleAddressTransactionHashes(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	address := r.PathValue("address")
	txs, total, err := b.node.AddressTransactions(
		r.Context(),
		address,
		TransactionRangeParams{Pagination: params},
	)
	if err != nil {
		b.writeAddressQueryError(w, err, address, "address transactions")
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]string, 0, len(txs))
	for _, tx := range txs {
		resp = append(resp, tx.TxHash)
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAddressExtended handles GET /api/v0/addresses/{address}/extended.
func (b *Blockfrost) handleAddressExtended(
	w http.ResponseWriter,
	r *http.Request,
) {
	address := r.PathValue("address")
	info, err := b.node.AddressExtended(r.Context(), address)
	if err != nil {
		b.writeAddressQueryError(w, err, address, "extended address")
		return
	}
	amounts := make([]AddressExtendedAmountResponse, 0, len(info.Amount))
	for _, amount := range info.Amount {
		amounts = append(amounts, AddressExtendedAmountResponse(amount))
	}
	writeJSON(w, http.StatusOK, AddressExtendedResponse{
		Address:      info.Address,
		Amount:       amounts,
		StakeAddress: info.StakeAddress,
		Type:         info.Type,
		Script:       info.Script,
	})
}

// handleAddressTotal handles GET /api/v0/addresses/{address}/total.
func (b *Blockfrost) handleAddressTotal(
	w http.ResponseWriter,
	r *http.Request,
) {
	address := r.PathValue("address")
	info, err := b.node.AddressTotal(r.Context(), address)
	if err != nil {
		b.writeAddressQueryError(w, err, address, "address total")
		return
	}
	writeJSON(w, http.StatusOK, AddressTotalResponse{
		Address:     info.Address,
		ReceivedSum: convertAddressAmounts(info.ReceivedSum),
		SentSum:     convertAddressAmounts(info.SentSum),
		TxCount:     info.TxCount,
	})
}

// handleAddressAssetUTXOs handles
// GET /api/v0/addresses/{address}/utxos/{asset}.
func (b *Blockfrost) handleAddressAssetUTXOs(
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
	address := r.PathValue("address")
	utxos, total, err := b.node.AddressAssetUTXOs(
		r.Context(), address, policyID, assetName, params,
	)
	if err != nil {
		b.writeAddressQueryError(w, err, address, "address asset UTxOs")
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]AddressUTXOResponse, 0, len(utxos))
	for _, utxo := range utxos {
		resp = append(resp, addressUTXOResponse(utxo))
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseAssetOrWriteError(
	w http.ResponseWriter,
	asset string,
) (string, []byte, bool) {
	policyID, assetName, err := parseAssetIdentifier(asset)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid asset identifier.",
		)
		return "", nil, false
	}
	return policyID, assetName, true
}

func addressUTXOResponse(utxo AddressUTXOInfo) AddressUTXOResponse {
	return AddressUTXOResponse{
		Address:             utxo.Address,
		TxHash:              utxo.TxHash,
		TxIndex:             int(utxo.TxIndex),
		OutputIndex:         int(utxo.OutputIndex),
		Amount:              convertAddressAmounts(utxo.Amount),
		Block:               utxo.Block,
		DataHash:            utxo.DataHash,
		InlineDatum:         utxo.InlineDatum,
		ReferenceScriptHash: utxo.ReferenceScriptHash,
	}
}
