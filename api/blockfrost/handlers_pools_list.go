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

// poolListTimeout bounds how long one pool list request may run.
const poolListTimeout = 15 * time.Second

// admitPoolList reserves a slot in the pool list pool for the request's
// client. When the pool is full it writes the 429 response and reports
// false.
func (b *Blockfrost) admitPoolList(
	w http.ResponseWriter,
	r *http.Request,
) (func(), bool) {
	release, ok := b.poolListAdmission.acquire(r.Context(), requestClient(r))
	if !ok {
		w.Header().Set("Retry-After", "1")
		writeError(
			w,
			http.StatusTooManyRequests,
			"Too Many Requests",
			"pool list capacity reached, try again later",
		)
		return nil, false
	}
	return release, true
}

// writePoolListDeadlineError answers a request that ran past its deadline
// and reports whether it did.
func (b *Blockfrost) writePoolListDeadlineError(
	w http.ResponseWriter,
	err error,
) bool {
	if !errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	b.logger.Warn("pool list exceeded its deadline", "error", err)
	w.Header().Set("Retry-After", "1")
	writeError(
		w,
		http.StatusServiceUnavailable,
		"Service Unavailable",
		"pool list deadline exceeded, try again later",
	)
	return true
}

// handlePoolsList handles GET /api/v0/pools and returns the paginated
// list of registered stake pool IDs (pool_list): a flat array of bech32
// pool ID strings, nothing more. See PoolsList for the ordering and
// query-cost rationale.
func (b *Blockfrost) handlePoolsList(
	w http.ResponseWriter,
	r *http.Request,
) {
	params, errMsg := ParsePaginationStrict(r)
	if errMsg != "" {
		writeError(w, http.StatusBadRequest, "Bad Request", errMsg)
		return
	}

	release, ok := b.admitPoolList(w, r)
	if !ok {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(r.Context(), poolListTimeout)
	defer cancel()
	pools, total, err := b.node.PoolsList(ctx, params)
	if err != nil {
		if b.writePoolListDeadlineError(w, err) ||
			errors.Is(err, context.Canceled) {
			return
		}
		b.logger.Error(
			"failed to list pools",
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

	SetPaginationHeaders(w, total, params)
	if pools == nil {
		pools = []string{}
	}
	writeJSON(w, http.StatusOK, pools)
}
