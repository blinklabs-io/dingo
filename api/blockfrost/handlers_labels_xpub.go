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
	"strconv"
	"time"
)

// xpubHexLength is the length Blockfrost requires of an xpub path parameter:
// a 32-byte public key and 32-byte chain code in hex.
const xpubHexLength = 128

// handleHealthClock handles GET /health/clock and GET /api/v0/health/clock
// and returns the server's UNIX time in milliseconds.
func (b *Blockfrost) handleHealthClock(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(w, http.StatusOK, HealthClockResponse{
		ServerTime: time.Now().UnixMilli(),
	})
}

// handleMetadataLabels handles GET /api/v0/metadata/txs/labels and returns
// the transaction metadata labels in use with their transaction counts.
func (b *Blockfrost) handleMetadataLabels(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	rows, total, err := b.node.MetadataLabels(r.Context(), params)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		b.logger.Error("failed to list metadata labels", "error", err)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve transaction metadata labels",
		)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]MetadataLabelResponse, 0, len(rows))
	for _, row := range rows {
		entry := MetadataLabelResponse{
			Label: strconv.FormatUint(row.Label, 10),
			Count: strconv.FormatUint(row.Count, 10),
		}
		if description, ok := cip10Labels[row.Label]; ok {
			entry.CIP10 = &description
		}
		resp = append(resp, entry)
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleXpubAddress handles
// GET /api/v0/utils/addresses/xpub/{xpub}/{role}/{index} and returns the
// address derived from an account extended public key.
func (b *Blockfrost) handleXpubAddress(
	w http.ResponseWriter,
	r *http.Request,
) {
	xpubHex := r.PathValue("xpub")
	xpub, err := hex.DecodeString(xpubHex)
	if len(xpubHex) != xpubHexLength || err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid or malformed xpub format. Has to be hex of length 128.",
		)
		return
	}
	role, err := strconv.ParseUint(r.PathValue("role"), 10, 32)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Missing, out of range or malformed role.",
		)
		return
	}
	index, err := strconv.ParseUint(r.PathValue("index"), 10, 32)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Missing, out of range or malformed index.",
		)
		return
	}
	address, err := b.node.XpubAddress(xpub, uint32(role), uint32(index))
	if err != nil {
		// Every failure here comes from the input: a hardened role or
		// index, or an xpub whose key is not a valid curve point.
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Unable to derive an address: "+err.Error(),
		)
		return
	}
	writeJSON(w, http.StatusOK, XpubAddressResponse{
		Xpub:    xpubHex,
		Role:    uint32(role),
		Index:   uint32(index),
		Address: address,
	})
}
