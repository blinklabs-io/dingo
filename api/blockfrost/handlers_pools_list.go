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

import "net/http"

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

	pools, total, err := b.node.PoolsList(params)
	if err != nil {
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
	writeJSON(w, http.StatusOK, pools)
}
