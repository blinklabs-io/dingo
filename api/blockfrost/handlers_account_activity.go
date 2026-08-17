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
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// ErrInvalidBlockRange is returned when a from/to query value is not a
// valid block number or "block:index" position.
var ErrInvalidBlockRange = errors.New("invalid block range position")

// handleAccountUTXOs handles GET /api/v0/accounts/{stake_address}/utxos
// and returns the current UTxOs controlled by the stake credential.
func (b *Blockfrost) handleAccountUTXOs(
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
	items, total, err := b.node.AccountUTXOs(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(w, err, "failed to retrieve account UTxOs")
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]AccountUTXOResponse, 0, len(items))
	for _, item := range items {
		resp = append(resp, AccountUTXOResponse{
			Address:             item.Address,
			TxHash:              item.TxHash,
			TxIndex:             int(item.TxIndex),
			OutputIndex:         int(item.OutputIndex),
			Amount:              convertAddressAmounts(item.Amount),
			Block:               item.Block,
			DataHash:            item.DataHash,
			InlineDatum:         item.InlineDatum,
			ReferenceScriptHash: item.ReferenceScriptHash,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAccountWithdrawals handles
// GET /api/v0/accounts/{stake_address}/withdrawals and returns
// withdrawal history for the stake credential.
func (b *Blockfrost) handleAccountWithdrawals(
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
	items, total, err := b.node.AccountWithdrawals(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(
			w, err, "failed to retrieve account withdrawals",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]AccountWithdrawalResponse, 0, len(items))
	for _, item := range items {
		resp = append(resp, AccountWithdrawalResponse(item))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleAccountTransactions handles
// GET /api/v0/accounts/{stake_address}/transactions and returns
// transactions associated with addresses controlled by the stake
// credential, optionally filtered by an inclusive from/to block range.
func (b *Blockfrost) handleAccountTransactions(
	w http.ResponseWriter,
	r *http.Request,
) {
	pagination, err := ParsePagination(r)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid pagination parameters.",
		)
		return
	}
	params := AccountTransactionsParams{Pagination: pagination}

	query := r.URL.Query()
	if raw := query.Get("from"); raw != "" {
		pos, err := parseBlockRangePosition(raw)
		if err != nil {
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"querystring/from must be a block number, "+
					`optionally suffixed with ":index".`,
			)
			return
		}
		params.From = &pos
	}
	if raw := query.Get("to"); raw != "" {
		pos, err := parseBlockRangePosition(raw)
		if err != nil {
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"querystring/to must be a block number, "+
					`optionally suffixed with ":index".`,
			)
			return
		}
		params.To = &pos
	}
	if params.From != nil && params.To != nil &&
		blockRangeInverted(*params.From, *params.To) {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"querystring/from must be lower than or equal to querystring/to.",
		)
		return
	}

	items, total, err := b.node.AccountTransactions(
		r.PathValue("stake_address"),
		params,
	)
	if err != nil {
		b.writeAccountError(
			w, err, "failed to retrieve account transactions",
		)
		return
	}
	SetPaginationHeaders(w, total, pagination)
	resp := make([]AccountTransactionResponse, 0, len(items))
	for _, item := range items {
		resp = append(resp, AccountTransactionResponse{
			Address:     item.Address,
			TxHash:      item.TxHash,
			TxIndex:     int(item.TxIndex),
			BlockHeight: item.BlockHeight,
			BlockTime:   int(item.BlockTime),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

// parseBlockRangePosition parses the Blockfrost account-transactions
// from/to query value: a block number, optionally suffixed with
// ":index" giving the transaction's index within that block.
func parseBlockRangePosition(raw string) (BlockRangePosition, error) {
	block, indexPart, hasIndex := strings.Cut(raw, ":")
	blockNumber, err := strconv.ParseUint(block, 10, 64)
	if err != nil {
		return BlockRangePosition{}, ErrInvalidBlockRange
	}
	if !hasIndex {
		return BlockRangePosition{Block: blockNumber}, nil
	}
	index, err := strconv.ParseUint(indexPart, 10, 32)
	if err != nil {
		return BlockRangePosition{}, ErrInvalidBlockRange
	}
	idx := uint32(index)
	return BlockRangePosition{Block: blockNumber, Index: &idx}, nil
}

// blockRangeInverted reports whether from is unambiguously after to,
// i.e. the requested range is empty by construction. Same-block
// comparisons where only one side carries an explicit index are treated
// as valid (not inverted): the adapter-level filter resolves the exact
// boundary in that case.
func blockRangeInverted(from, to BlockRangePosition) bool {
	if from.Block > to.Block {
		return true
	}
	if from.Block < to.Block {
		return false
	}
	return from.Index != nil && to.Index != nil && *from.Index > *to.Index
}
