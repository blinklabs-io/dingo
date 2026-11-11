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
	"errors"
	"net/http"
	"time"
)

// blockListTimeout bounds how long one block list page may run.
const blockListTimeout = 15 * time.Second

// serveBlockPage serves a block hash list under the block list admission
// pool and deadline. The pagination query is validated first, and a pool id
// the node rejects before any database read does not take a slot, so neither
// reads as capacity. A full pool answers 429 and an overrun answers 503, both
// with Retry-After.
func serveBlockPage(
	b *Blockfrost,
	w http.ResponseWriter,
	r *http.Request,
	fetch func(context.Context, PaginationParams) ([]string, int, error),
	convert func(string) string,
	internalMessage string,
) {
	params, ok := parsePaginationOrWriteError(w, r)
	if !ok {
		return
	}
	poolID := r.PathValue("pool_id")
	if _, err := parsePoolID(poolID); err == nil || poolID == "" {
		release, ok := b.blockListAdmission.acquire(
			r.Context(),
			requestClient(r),
		)
		if !ok {
			w.Header().Set("Retry-After", "1")
			writeError(
				w,
				http.StatusTooManyRequests,
				"Too Many Requests",
				"block list capacity reached, try again later",
			)
			return
		}
		defer release()
	}
	ctx, cancel := context.WithTimeout(r.Context(), blockListTimeout)
	defer cancel()
	items, total, err := fetch(ctx, params)
	if errors.Is(err, context.DeadlineExceeded) {
		b.logger.Warn(internalMessage, "error", err)
		w.Header().Set("Retry-After", "1")
		writeError(
			w,
			http.StatusServiceUnavailable,
			"Service Unavailable",
			"block list deadline exceeded, try again later",
		)
		return
	}
	if err != nil {
		b.writeHistoryError(w, err, internalMessage)
		return
	}
	SetPaginationHeaders(w, total, params)
	resp := make([]string, len(items))
	for i, item := range items {
		resp[i] = convert(item)
	}
	writeJSON(w, http.StatusOK, resp)
}
