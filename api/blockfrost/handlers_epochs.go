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
	"strconv"
)

func epochNumberOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
) (uint64, bool) {
	epoch, err := strconv.ParseUint(r.PathValue("number"), 10, 64)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid epoch number.",
		)
		return 0, false
	}
	return epoch, true
}

func epochResponse(info EpochInfo) EpochResponse {
	return EpochResponse(info)
}

// handleEpoch handles GET /api/v0/epochs/{number}.
func (b *Blockfrost) handleEpoch(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	release, ok := b.admitChainSummary(w, r)
	if !ok {
		return
	}
	defer release()
	info, err := b.node.Epoch(r.Context(), epoch)
	if err != nil {
		b.writeHistoryError(w, err, "failed to retrieve epoch")
		return
	}
	writeJSON(w, http.StatusOK, epochResponse(info))
}

// handleEpochNext handles GET /api/v0/epochs/{number}/next.
func (b *Blockfrost) handleEpochNext(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	req := withoutOrder(r)
	params, ok := parsePaginationOrWriteError(w, req)
	if !ok {
		return
	}
	release, ok := b.admitChainSummary(w, r)
	if !ok {
		return
	}
	defer release()
	serveParsedPage(
		b,
		w,
		req,
		params,
		func(ctx context.Context, params PaginationParams) ([]EpochInfo, int, error) {
			return b.node.EpochsAfter(ctx, epoch, params)
		},
		epochResponse,
		"failed to retrieve following epochs",
	)
}

// handleEpochPrevious handles GET /api/v0/epochs/{number}/previous.
func (b *Blockfrost) handleEpochPrevious(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	req := withoutOrder(r)
	params, ok := parsePaginationOrWriteError(w, req)
	if !ok {
		return
	}
	release, ok := b.admitChainSummary(w, r)
	if !ok {
		return
	}
	defer release()
	serveParsedPage(
		b,
		w,
		req,
		params,
		func(ctx context.Context, params PaginationParams) ([]EpochInfo, int, error) {
			return b.node.EpochsBefore(ctx, epoch, params)
		},
		epochResponse,
		"failed to retrieve preceding epochs",
	)
}

// handleEpochStakes handles GET /api/v0/epochs/{number}/stakes.
func (b *Blockfrost) handleEpochStakes(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		withoutOrder(r),
		func(ctx context.Context, params PaginationParams) ([]EpochStakeInfo, int, error) {
			return b.node.EpochStakes(ctx, epoch, params)
		},
		func(item EpochStakeInfo) EpochStakeResponse {
			return EpochStakeResponse(item)
		},
		"failed to retrieve epoch stakes",
	)
}

// handleEpochPoolStakes handles
// GET /api/v0/epochs/{number}/stakes/{pool_id}.
func (b *Blockfrost) handleEpochPoolStakes(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		withoutOrder(r),
		func(ctx context.Context, params PaginationParams) ([]EpochPoolStakeInfo, int, error) {
			return b.node.EpochPoolStakes(
				ctx, epoch, r.PathValue("pool_id"), params,
			)
		},
		func(item EpochPoolStakeInfo) EpochPoolStakeResponse {
			return EpochPoolStakeResponse(item)
		},
		"failed to retrieve epoch pool stakes",
	)
}

// handleEpochBlocks handles GET /api/v0/epochs/{number}/blocks.
func (b *Blockfrost) handleEpochBlocks(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]string, int, error) {
			return b.node.EpochBlocks(ctx, epoch, params)
		},
		func(hash string) string { return hash },
		"failed to retrieve epoch blocks",
	)
}

// handleEpochPoolBlocks handles
// GET /api/v0/epochs/{number}/blocks/{pool_id}.
func (b *Blockfrost) handleEpochPoolBlocks(
	w http.ResponseWriter,
	r *http.Request,
) {
	epoch, ok := epochNumberOrWriteError(w, r)
	if !ok {
		return
	}
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]string, int, error) {
			return b.node.EpochPoolBlocks(
				ctx, epoch, r.PathValue("pool_id"), params,
			)
		},
		func(hash string) string { return hash },
		"failed to retrieve epoch pool blocks",
	)
}
