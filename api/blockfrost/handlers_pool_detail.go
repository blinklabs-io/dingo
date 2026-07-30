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

import (
	"errors"
	"net/http"

	"github.com/blinklabs-io/dingo/database/models"
)

// handlePoolDetail handles GET /api/v0/pools/{pool_id} and returns the
// OpenAPI pool detail object for the requested pool.
func (b *Blockfrost) handlePoolDetail(
	w http.ResponseWriter,
	r *http.Request,
) {
	poolID := r.PathValue("pool_id")
	info, err := b.node.PoolDetail(poolID)
	if err != nil {
		if errors.Is(err, ErrInvalidPoolID) {
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"Invalid or malformed pool id format.",
			)
			return
		}
		if errors.Is(err, models.ErrPoolNotFound) {
			writeError(
				w,
				http.StatusNotFound,
				"Not Found",
				"The requested component has not been found.",
			)
			return
		}
		b.logger.Error(
			"failed to get pool detail",
			"pool_id", poolID,
			"error", err,
		)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to retrieve pool detail",
		)
		return
	}
	writeJSON(w, http.StatusOK, poolDetailResponse(info))
}

// poolDetailResponse converts a PoolDetailInfo into its Blockfrost wire
// shape. Owners, Registration, and Retirement are non-nullable arrays in
// the OpenAPI schema, so a nil slice is normalized to an empty one rather
// than encoding as JSON null.
func poolDetailResponse(info PoolDetailInfo) PoolDetailResponse {
	// info.CalidusKey is always nil: dingo does not currently ingest
	// CIP-0088 Calidus key registrations from any source (see
	// PoolCalidusKeyInfo's doc comment in node_interface.go), so there is
	// no producer that could set it, and no PoolCalidusKeyInfo ->
	// PoolCalidusKeyResponse conversion to reach. PoolCalidusKeyResponse
	// itself stays, since nullable: true in the OpenAPI schema makes null
	// the correct wire representation and the type still documents that
	// shape.
	var calidusKey *PoolCalidusKeyResponse
	owners := info.Owners
	if owners == nil {
		owners = []string{}
	}
	registration := info.Registration
	if registration == nil {
		registration = []string{}
	}
	retirement := info.Retirement
	if retirement == nil {
		retirement = []string{}
	}
	return PoolDetailResponse{
		PoolID:         info.PoolID,
		Hex:            info.Hex,
		VrfKey:         info.VrfKey,
		BlocksMinted:   info.BlocksMinted,
		BlocksEpoch:    info.BlocksEpoch,
		LiveStake:      info.LiveStake,
		LiveSize:       info.LiveSize,
		LiveSaturation: info.LiveSaturation,
		LiveDelegators: info.LiveDelegators,
		ActiveStake:    info.ActiveStake,
		ActiveSize:     info.ActiveSize,
		DeclaredPledge: info.DeclaredPledge,
		LivePledge:     info.LivePledge,
		MarginCost:     info.MarginCost,
		FixedCost:      info.FixedCost,
		RewardAccount:  info.RewardAccount,
		Owners:         owners,
		Registration:   registration,
		Retirement:     retirement,
		CalidusKey:     calidusKey,
	}
}
