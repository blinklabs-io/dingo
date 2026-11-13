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
	"net/http"
)

// serveBlockPage serves a block hash list.
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
	items, total, err := fetch(r.Context(), params)
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
