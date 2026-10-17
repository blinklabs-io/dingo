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

// handlePoolsRetired handles GET /api/v0/pools/retired.
func (b *Blockfrost) handlePoolsRetired(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]PoolRetiringInfo, int, error) {
			return b.node.PoolsRetired(ctx, params)
		},
		func(item PoolRetiringInfo) PoolRetiringResponse {
			return PoolRetiringResponse(item)
		},
		"failed to retrieve retired pools",
	)
}

// handlePoolHistory handles GET /api/v0/pools/{pool_id}/history.
func (b *Blockfrost) handlePoolHistory(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]PoolHistoryInfo, int, error) {
			return b.node.PoolHistory(ctx, r.PathValue("pool_id"), params)
		},
		func(item PoolHistoryInfo) PoolHistoryResponse {
			return PoolHistoryResponse(item)
		},
		"failed to retrieve pool history",
	)
}

// handlePoolRelays handles GET /api/v0/pools/{pool_id}/relays.
func (b *Blockfrost) handlePoolRelays(
	w http.ResponseWriter,
	r *http.Request,
) {
	relays, err := b.node.PoolRelays(r.Context(), r.PathValue("pool_id"))
	if err != nil {
		b.writeHistoryError(w, err, "failed to retrieve pool relays")
		return
	}
	resp := make([]PoolRelayResponse, len(relays))
	for i, relay := range relays {
		resp[i] = PoolRelayResponse{
			IPv4:   optionalResponseString(relay.IPv4),
			IPv6:   optionalResponseString(relay.IPv6),
			DNS:    optionalResponseString(relay.DNS),
			DNSSrv: optionalResponseString(relay.DNSSrv),
			Port:   relay.Port,
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// handlePoolDelegators handles GET /api/v0/pools/{pool_id}/delegators.
func (b *Blockfrost) handlePoolDelegators(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]PoolDelegatorInfo, int, error) {
			return b.node.PoolDelegators(ctx, r.PathValue("pool_id"), params)
		},
		func(item PoolDelegatorInfo) PoolDelegatorResponse {
			return PoolDelegatorResponse(item)
		},
		"failed to retrieve pool delegators",
	)
}

// handlePoolBlocks handles GET /api/v0/pools/{pool_id}/blocks.
func (b *Blockfrost) handlePoolBlocks(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]string, int, error) {
			return b.node.PoolBlocks(ctx, r.PathValue("pool_id"), params)
		},
		func(hash string) string { return hash },
		"failed to retrieve pool blocks",
	)
}

// handlePoolUpdates handles GET /api/v0/pools/{pool_id}/updates.
func (b *Blockfrost) handlePoolUpdates(
	w http.ResponseWriter,
	r *http.Request,
) {
	servePage(
		b,
		w,
		r,
		func(ctx context.Context, params PaginationParams) ([]PoolUpdateInfo, int, error) {
			return b.node.PoolUpdates(ctx, r.PathValue("pool_id"), params)
		},
		func(item PoolUpdateInfo) PoolUpdateResponse {
			return PoolUpdateResponse(item)
		},
		"failed to retrieve pool updates",
	)
}
