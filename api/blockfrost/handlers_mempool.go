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

func (b *Blockfrost) writeMempoolQueryError(
	w http.ResponseWriter,
	err error,
	what string,
) {
	switch {
	case errors.Is(err, ErrMempoolUnavailable):
		writeError(
			w,
			http.StatusServiceUnavailable,
			"Service Unavailable",
			"mempool unavailable",
		)
	case errors.Is(err, ErrTransactionNotFound):
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested component has not been found.",
		)
	case errors.Is(err, ErrInvalidAddress):
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid address for this network or malformed address format.",
		)
	default:
		b.logger.Error("failed to get "+what, "error", err)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve "+what,
		)
	}
}

func writeMempoolHashes(
	w http.ResponseWriter,
	hashes []string,
	total int,
	params PaginationParams,
) {
	SetPaginationHeaders(w, total, params)
	resp := make([]MempoolHashResponse, 0, len(hashes))
	for _, hash := range hashes {
		resp = append(resp, MempoolHashResponse{TxHash: hash})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleMempool handles GET /api/v0/mempool.
func (b *Blockfrost) handleMempool(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	hashes, total, err := b.node.MempoolTransactions(r.Context(), params)
	if err != nil {
		b.writeMempoolQueryError(w, err, "mempool")
		return
	}
	writeMempoolHashes(w, hashes, total, params)
}

// handleMempoolAddress handles GET /api/v0/mempool/addresses/{address}.
func (b *Blockfrost) handleMempoolAddress(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	release, ok := b.mempoolAddressAdmission.acquire(
		r.Context(),
		requestClient(r),
	)
	if !ok {
		w.Header().Set("Retry-After", "1")
		writeError(
			w,
			http.StatusTooManyRequests,
			"Too Many Requests",
			"mempool address capacity reached, try again later",
		)
		return
	}
	defer release()
	hashes, total, err := b.node.MempoolAddressTransactions(
		r.Context(), r.PathValue("address"), params,
	)
	if err != nil {
		b.writeMempoolQueryError(w, err, "mempool address transactions")
		return
	}
	writeMempoolHashes(w, hashes, total, params)
}

// handleMempoolTransaction handles GET /api/v0/mempool/{hash}.
func (b *Blockfrost) handleMempoolTransaction(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := parseTransactionHashOrWriteError(w, r)
	if !ok {
		return
	}
	info, err := b.node.MempoolTransaction(r.Context(), hash)
	if err != nil {
		b.writeMempoolQueryError(w, err, "mempool transaction")
		return
	}
	tx := info.Tx
	resp := MempoolTransactionResponse{
		Tx: MempoolTransactionSummaryResponse{
			Hash:               tx.Hash,
			OutputAmount:       convertAddressAmounts(tx.OutputAmount),
			Fees:               tx.Fees,
			Deposit:            tx.Deposit,
			Size:               tx.Size,
			InvalidBefore:      tx.InvalidBefore,
			InvalidHereafter:   tx.InvalidHereafter,
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
		},
		Inputs:  make([]MempoolInputResponse, 0, len(info.Inputs)),
		Outputs: make([]MempoolOutputResponse, 0, len(info.Outputs)),
	}
	for _, input := range info.Inputs {
		resp.Inputs = append(resp.Inputs, MempoolInputResponse{
			Address:     optionalResponseString(input.Address),
			TxHash:      input.TxHash,
			OutputIndex: int(input.OutputIndex),
			Collateral:  input.Collateral,
			Reference:   input.Reference,
		})
	}
	for _, output := range info.Outputs {
		resp.Outputs = append(resp.Outputs, MempoolOutputResponse{
			Address:             output.Address,
			Amount:              convertAddressAmounts(output.Amount),
			OutputIndex:         int(output.OutputIndex),
			DataHash:            output.DataHash,
			InlineDatum:         output.InlineDatum,
			Collateral:          output.Collateral,
			ReferenceScriptHash: output.ReferenceScriptHash,
		})
	}
	for _, redeemer := range info.Redeemers {
		resp.Redeemers = append(
			resp.Redeemers,
			MempoolRedeemerResponse(redeemer),
		)
	}
	writeJSON(w, http.StatusOK, resp)
}
