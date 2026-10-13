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
	"strconv"
)

// orderedPage returns the page of items selected by params. A descending page
// is taken from the end of items, so page 1 holds the last item first.
func orderedPage[T any](items []T, params PaginationParams) []T {
	offset, ok := paginationOffset(params)
	if !ok || offset >= len(items) {
		return []T{}
	}
	count := min(params.Count, len(items)-offset)
	page := make([]T, count)
	for i := range count {
		index := offset + i
		if params.Order == PaginationOrderDesc {
			index = len(items) - 1 - index
		}
		page[i] = items[index]
	}
	return page
}

func parseUintPathOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
	name string,
	message string,
) (uint64, bool) {
	value, err := strconv.ParseUint(r.PathValue(name), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Bad Request", message)
		return 0, false
	}
	return value, true
}

// writeBlockQueryError maps the block lookup errors shared by the block
// navigation and block content handlers.
func (b *Blockfrost) writeBlockQueryError(
	w http.ResponseWriter,
	err error,
	what string,
	attrs ...any,
) {
	switch {
	case errors.Is(err, ErrInvalidBlockID):
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid block hash or number.",
		)
	case errors.Is(err, ErrBlockNotFound):
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested block could not be found.",
		)
	default:
		b.logger.Error(
			"failed to get "+what,
			append(attrs, "error", err)...,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve block",
		)
	}
}

// handleBlockSection serves the three-segment block routes. The literal
// "slot" segment of /blocks/slot/{slot_number} and the block identifier
// wildcard of /blocks/{hash_or_number}/{section} overlap, which ServeMux
// refuses to register as two patterns, so one pattern dispatches both.
func (b *Blockfrost) handleBlockSection(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.PathValue("hash_or_number") == "slot" {
		b.handleBlockBySlot(w, r)
		return
	}
	switch r.PathValue("section") {
	case "next":
		b.handleBlockNeighbors(w, r, true)
	case "previous":
		b.handleBlockNeighbors(w, r, false)
	case "txs":
		b.handleBlockTransactions(w, r)
	case "addresses":
		b.handleBlockAddresses(w, r)
	default:
		b.handleNotFound(w, r)
	}
}

// handleBlockNeighbors handles GET /api/v0/blocks/{hash_or_number}/next and
// /previous.
func (b *Blockfrost) handleBlockNeighbors(
	w http.ResponseWriter,
	r *http.Request,
	forward bool,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	id := r.PathValue("hash_or_number")
	var (
		infos []BlockInfo
		err   error
	)
	if forward {
		infos, err = b.node.BlockNext(r.Context(), id, params)
	} else {
		infos, err = b.node.BlockPrevious(r.Context(), id, params)
	}
	if err != nil {
		b.writeBlockQueryError(w, err, "neighboring blocks", "block", id)
		return
	}
	resp := make([]BlockResponse, 0, len(infos))
	for _, info := range infos {
		resp = append(resp, blockResponse(info))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleBlockBySlot handles GET /api/v0/blocks/slot/{slot_number}.
func (b *Blockfrost) handleBlockBySlot(
	w http.ResponseWriter,
	r *http.Request,
) {
	slot, err := strconv.ParseUint(r.PathValue("section"), 10, 64)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid slot number.",
		)
		return
	}
	info, err := b.node.BlockBySlot(r.Context(), slot)
	if err != nil {
		b.writeBlockQueryError(w, err, "block by slot", "slot", slot)
		return
	}
	writeJSON(w, http.StatusOK, blockResponse(info))
}

// handleBlockByEpochSlot handles
// GET /api/v0/blocks/epoch/{epoch_number}/slot/{slot_number}.
func (b *Blockfrost) handleBlockByEpochSlot(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := parseUintPathOrWriteError(
		w, r, "epoch_number", "Invalid epoch number.",
	)
	if !ok {
		return
	}
	slot, ok := parseUintPathOrWriteError(
		w, r, "slot_number", "Invalid slot number.",
	)
	if !ok {
		return
	}
	info, err := b.node.BlockByEpochSlot(r.Context(), epoch, slot)
	if err != nil {
		b.writeBlockQueryError(
			w, err, "block by epoch slot", "epoch", epoch, "slot", slot,
		)
		return
	}
	writeJSON(w, http.StatusOK, blockResponse(info))
}

// handleBlockTransactions handles GET /api/v0/blocks/{hash_or_number}/txs.
func (b *Blockfrost) handleBlockTransactions(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	id := r.PathValue("hash_or_number")
	hashes, err := b.node.BlockTransactionHashes(r.Context(), id)
	if err != nil {
		b.writeBlockQueryError(w, err, "block transactions", "block", id)
		return
	}
	SetPaginationHeaders(w, len(hashes), params)
	writeJSON(w, http.StatusOK, orderedPage(hashes, params))
}

// handleBlockTransactionsCBOR handles
// GET /api/v0/blocks/{hash_or_number}/txs/cbor.
func (b *Blockfrost) handleBlockTransactionsCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	id := r.PathValue("hash_or_number")
	rows, err := b.node.BlockTransactionCBOR(r.Context(), id)
	if err != nil {
		b.writeBlockQueryError(w, err, "block transaction CBOR", "block", id)
		return
	}
	b.writeTransactionCBORPage(w, rows, params)
}

// handleLatestBlockTransactionsCBOR handles
// GET /api/v0/blocks/latest/txs/cbor.
func (b *Blockfrost) handleLatestBlockTransactionsCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	rows, err := b.node.LatestBlockTransactionCBOR(r.Context())
	if err != nil {
		b.logger.Error(
			"failed to get latest block transaction CBOR",
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
	b.writeTransactionCBORPage(w, rows, params)
}

func (b *Blockfrost) writeTransactionCBORPage(
	w http.ResponseWriter,
	rows []BlockTransactionCBORInfo,
	params PaginationParams,
) {
	SetPaginationHeaders(w, len(rows), params)
	page := orderedPage(rows, params)
	resp := make([]BlockTransactionCBORResponse, 0, len(page))
	for _, row := range page {
		resp = append(resp, BlockTransactionCBORResponse(row))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleBlockAddresses handles
// GET /api/v0/blocks/{hash_or_number}/addresses.
func (b *Blockfrost) handleBlockAddresses(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	// The listing has no order parameter: addresses are always ascending.
	params.Order = PaginationOrderAsc
	id := r.PathValue("hash_or_number")
	rows, err := b.node.BlockAddresses(r.Context(), id)
	if err != nil {
		b.writeBlockQueryError(w, err, "block addresses", "block", id)
		return
	}
	SetPaginationHeaders(w, len(rows), params)
	page := orderedPage(rows, params)
	resp := make([]BlockAddressResponse, 0, len(page))
	for _, row := range page {
		txs := make([]BlockAddressTransactionResponse, 0, len(row.TxHashes))
		for _, hash := range row.TxHashes {
			txs = append(txs, BlockAddressTransactionResponse{TxHash: hash})
		}
		resp = append(resp, BlockAddressResponse{
			Address:      row.Address,
			Transactions: txs,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
