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

	"github.com/blinklabs-io/dingo/database/models"
)

// writeHistoryError maps an error from a historical-query adapter method to
// the Blockfrost response for it. Anything unrecognized is an internal error
// whose detail stays in the log.
func (b *Blockfrost) writeHistoryError(
	w http.ResponseWriter,
	err error,
	internalMessage string,
) {
	switch {
	case errors.Is(err, ErrInvalidPoolID):
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid or malformed pool id format.",
		)
	case errors.Is(err, ErrInvalidStakeAddress):
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid stake address.",
		)
	case errors.Is(err, ErrHistoryNotRetained):
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested history is not retained by this node.",
		)
	case errors.Is(err, ErrAddressSummaryTooLarge):
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"The account holds too many outputs or assets to summarize.",
		)
	case errors.Is(err, context.Canceled):
		// The client is gone, so there is nobody to answer.
	case errors.Is(err, ErrEpochNotFound),
		errors.Is(err, ErrDRepNotFound),
		errors.Is(err, models.ErrGovernanceProposalNotFound),
		errors.Is(err, models.ErrPoolNotFound),
		errors.Is(err, models.ErrAccountNotFound):
		writeError(
			w,
			http.StatusNotFound,
			"Not Found",
			"The requested component has not been found.",
		)
	default:
		b.logger.Error(internalMessage, "error", err)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			internalMessage,
		)
	}
}

// withoutOrder drops the order parameter from a list whose Blockfrost schema
// defines only count and page, so it is ignored like any other undefined
// query parameter rather than validated or applied.
func withoutOrder(r *http.Request) *http.Request {
	query := r.URL.Query()
	if !query.Has("order") {
		return r
	}
	query.Del("order")
	clone := r.Clone(r.Context())
	clone.URL.RawQuery = query.Encode()
	return clone
}

// servePage serves a paginated list: it parses the pagination query, fetches
// one page, sets the pagination headers from the reported total and writes
// the converted items. A nil item slice encodes as an empty array.
func servePage[Item, Response any](
	b *Blockfrost,
	w http.ResponseWriter,
	r *http.Request,
	fetch func(context.Context, PaginationParams) ([]Item, int, error),
	convert func(Item) Response,
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
	resp := make([]Response, len(items))
	for i, item := range items {
		resp[i] = convert(item)
	}
	writeJSON(w, http.StatusOK, resp)
}
