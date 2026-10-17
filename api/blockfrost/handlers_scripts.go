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
	"encoding/hex"
	"errors"
	"net/http"
)

const (
	scriptHashHexLen = 56
	datumHashHexLen  = 64
)

// decodeHashOrWriteError decodes a path value that must be a hex hash of the
// given length, answering 400 with message otherwise.
func decodeHashOrWriteError(
	w http.ResponseWriter,
	value string,
	hexLen int,
	message string,
) ([]byte, bool) {
	hash, err := hex.DecodeString(value)
	if err != nil || len(value) != hexLen {
		writeError(w, http.StatusBadRequest, "Bad Request", message)
		return nil, false
	}
	return hash, true
}

func (b *Blockfrost) writeScriptQueryError(
	w http.ResponseWriter,
	err error,
	what string,
	hash []byte,
) {
	if errors.Is(err, ErrScriptNotFound) || errors.Is(err, ErrDatumNotFound) {
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
		"hash", hex.EncodeToString(hash),
		"error", err,
	)
	writeError(
		w,
		http.StatusInternalServerError,
		"Internal Server Error",
		"failed to retrieve "+what,
	)
}

// handleScriptSection serves GET /api/v0/scripts/{script_hash}/{section} and
// GET /api/v0/scripts/datum/{datum_hash}. The literal "datum" segment and the
// script hash wildcard overlap, which ServeMux refuses to register as separate
// patterns, so one pattern dispatches both.
func (b *Blockfrost) handleScriptSection(
	w http.ResponseWriter,
	r *http.Request,
) {
	if r.PathValue("script_hash") == "datum" {
		b.handleDatum(w, r, r.PathValue("section"), false)
		return
	}
	switch r.PathValue("section") {
	case "json":
		b.handleScriptJSON(w, r)
	case "cbor":
		b.handleScriptCBOR(w, r)
	case "redeemers":
		b.handleScriptRedeemers(w, r)
	case "utxos":
		b.handleScriptUTXOs(w, r)
	default:
		b.handleNotFound(w, r)
	}
}

// handleDatumCBOR handles GET /api/v0/scripts/datum/{datum_hash}/cbor.
func (b *Blockfrost) handleDatumCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	b.handleDatum(w, r, r.PathValue("datum_hash"), true)
}

func (b *Blockfrost) handleDatum(
	w http.ResponseWriter,
	r *http.Request,
	datumHash string,
	asCBOR bool,
) {
	hash, ok := decodeHashOrWriteError(
		w, datumHash, datumHashHexLen, "Invalid datum hash.",
	)
	if !ok {
		return
	}
	if asCBOR {
		cborHex, err := b.node.DatumCBOR(r.Context(), hash)
		if err != nil {
			b.writeScriptQueryError(w, err, "datum CBOR", hash)
			return
		}
		writeJSON(w, http.StatusOK, DatumCBORResponse{CBOR: cborHex})
		return
	}
	value, err := b.node.DatumJSON(r.Context(), hash)
	if err != nil {
		b.writeScriptQueryError(w, err, "datum", hash)
		return
	}
	writeJSON(w, http.StatusOK, DatumResponse{JSONValue: value})
}

// handleScripts handles GET /api/v0/scripts.
func (b *Blockfrost) handleScripts(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	hashes, total, err := b.node.Scripts(r.Context(), params)
	if err != nil {
		b.writeScriptQueryError(w, err, "scripts", nil)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]ScriptListItemResponse, 0, len(hashes))
	for _, hash := range hashes {
		resp = append(resp, ScriptListItemResponse{ScriptHash: hash})
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleScript handles GET /api/v0/scripts/{script_hash}.
func (b *Blockfrost) handleScript(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := decodeHashOrWriteError(
		w, r.PathValue("script_hash"), scriptHashHexLen, "Invalid script hash.",
	)
	if !ok {
		return
	}
	info, err := b.node.Script(r.Context(), hash)
	if err != nil {
		b.writeScriptQueryError(w, err, "script", hash)
		return
	}
	writeJSON(w, http.StatusOK, ScriptResponse{
		ScriptHash:     info.Hash,
		Type:           info.Type,
		SerialisedSize: info.SerialisedSize,
	})
}

// handleScriptJSON handles GET /api/v0/scripts/{script_hash}/json.
func (b *Blockfrost) handleScriptJSON(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := decodeHashOrWriteError(
		w, r.PathValue("script_hash"), scriptHashHexLen, "Invalid script hash.",
	)
	if !ok {
		return
	}
	value, err := b.node.ScriptJSON(r.Context(), hash)
	if err != nil {
		b.writeScriptQueryError(w, err, "script JSON", hash)
		return
	}
	writeJSON(w, http.StatusOK, ScriptJSONResponse{JSON: value})
}

// handleScriptCBOR handles GET /api/v0/scripts/{script_hash}/cbor.
func (b *Blockfrost) handleScriptCBOR(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := decodeHashOrWriteError(
		w, r.PathValue("script_hash"), scriptHashHexLen, "Invalid script hash.",
	)
	if !ok {
		return
	}
	value, err := b.node.ScriptCBOR(r.Context(), hash)
	if err != nil {
		b.writeScriptQueryError(w, err, "script CBOR", hash)
		return
	}
	writeJSON(w, http.StatusOK, ScriptCBORResponse{CBOR: value})
}

// handleScriptRedeemers handles GET /api/v0/scripts/{script_hash}/redeemers.
func (b *Blockfrost) handleScriptRedeemers(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := decodeHashOrWriteError(
		w, r.PathValue("script_hash"), scriptHashHexLen, "Invalid script hash.",
	)
	if !ok {
		return
	}
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	redeemers, total, err := b.node.ScriptRedeemers(r.Context(), hash, params)
	if err != nil {
		b.writeScriptQueryError(w, err, "script redeemers", hash)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]ScriptRedeemerResponse, 0, len(redeemers))
	for _, redeemer := range redeemers {
		resp = append(resp, ScriptRedeemerResponse(redeemer))
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleScriptUTXOs handles GET /api/v0/scripts/{script_hash}/utxos.
func (b *Blockfrost) handleScriptUTXOs(
	w http.ResponseWriter,
	r *http.Request,
) {
	hash, ok := decodeHashOrWriteError(
		w, r.PathValue("script_hash"), scriptHashHexLen, "Invalid script hash.",
	)
	if !ok {
		return
	}
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	utxos, total, err := b.node.ScriptUTXOs(r.Context(), hash, params)
	if err != nil {
		b.writeScriptQueryError(w, err, "script UTxOs", hash)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]ScriptUTXOResponse, 0, len(utxos))
	for _, utxo := range utxos {
		resp = append(resp, ScriptUTXOResponse{
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
