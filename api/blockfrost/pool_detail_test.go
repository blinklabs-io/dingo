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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandlePoolDetail(t *testing.T) {
	mock := &mockNode{
		poolDetail: PoolDetailInfo{
			PoolID:         "pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
			Hex:            "6080b9c76b4b19c21d17b4091aa205417cb80a5449f638c27eec0946",
			VrfKey:         "0b5245f9934ec2151116fb8ec00f35fd00e0aa3b075c4ed12cce440f999d823",
			BlocksMinted:   69,
			BlocksEpoch:    4,
			LiveStake:      "6900000000",
			LiveSize:       0.42,
			LiveSaturation: 0.93,
			LiveDelegators: 127,
			ActiveStake:    "4200000000",
			ActiveSize:     0.43,
			DeclaredPledge: "5000000000",
			LivePledge:     "5000000001",
			MarginCost:     0.05,
			FixedCost:      "340000000",
			RewardAccount:  "stake1uxkptsa4lkr55jleztw43t37vgdn88l6ghclfwuxld2eykgpgvg3f",
			Owners:         []string{"stake1u98nnlkvkk23vtvf9273uq7cph5ww6u2yq2389psuqet90sv4xv9v"},
			Registration:   []string{"9f83e5484f543e05b52e99988272a31da373f3aab4c064c76db96643a355d9dc"},
			Retirement:     []string{},
			CalidusKey:     nil,
		},
	}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
		nil,
	)
	req.SetPathValue("pool_id", "pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	// Every field name and type must match the OpenAPI 0.1.90 pool schema
	// exactly: string amounts, float ratios, integer counts, and a
	// nullable calidus_key that must be present in the payload as null,
	// not omitted.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	calidusKey, ok := raw["calidus_key"]
	require.True(t, ok, "calidus_key must be present in the response")
	assert.Nil(t, calidusKey)

	var resp PoolDetailResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, mock.poolDetail.PoolID, resp.PoolID)
	assert.Equal(t, mock.poolDetail.Hex, resp.Hex)
	assert.Equal(t, mock.poolDetail.VrfKey, resp.VrfKey)
	assert.Equal(t, uint64(69), resp.BlocksMinted)
	assert.Equal(t, uint64(4), resp.BlocksEpoch)
	assert.Equal(t, "6900000000", resp.LiveStake)
	assert.InDelta(t, 0.42, resp.LiveSize, 0.0001)
	assert.InDelta(t, 0.93, resp.LiveSaturation, 0.0001)
	assert.Equal(t, uint64(127), resp.LiveDelegators)
	assert.Equal(t, "4200000000", resp.ActiveStake)
	assert.InDelta(t, 0.43, resp.ActiveSize, 0.0001)
	assert.Equal(t, "5000000000", resp.DeclaredPledge)
	assert.Equal(t, "5000000001", resp.LivePledge)
	assert.InDelta(t, 0.05, resp.MarginCost, 0.0001)
	assert.Equal(t, "340000000", resp.FixedCost)
	assert.Equal(t, mock.poolDetail.RewardAccount, resp.RewardAccount)
	assert.Equal(t, mock.poolDetail.Owners, resp.Owners)
	assert.Equal(t, mock.poolDetail.Registration, resp.Registration)
	assert.Empty(t, resp.Retirement)
	assert.NotNil(t, resp.Retirement)
	assert.Nil(t, resp.CalidusKey)
}

// TestHandlePoolDetailEmptyArraysNotNull guards the non-nullable owners,
// registration, and retirement arrays: a zero-value PoolDetailInfo (nil
// slices) must still encode as "[]", never JSON null, since the OpenAPI
// schema marks them required arrays without nullable: true.
func TestHandlePoolDetailEmptyArraysNotNull(t *testing.T) {
	mock := &mockNode{poolDetail: PoolDetailInfo{PoolID: "pool1empty"}}
	b := newTestBlockfrost(mock)
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/pool1empty", nil)
	req.SetPathValue("pool_id", "pool1empty")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &raw))
	for _, field := range []string{"owners", "registration", "retirement"} {
		v, ok := raw[field]
		require.True(t, ok, "%s must be present", field)
		arr, isArray := v.([]any)
		require.True(t, isArray, "%s must encode as a JSON array, got %T", field, v)
		assert.Empty(t, arr)
	}
}

func TestHandlePoolDetailInvalidID(t *testing.T) {
	b := newTestBlockfrost(&mockNode{poolDetailErr: ErrInvalidPoolID})
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/pool1stonks", nil)
	req.SetPathValue("pool_id", "pool1stonks")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Invalid or malformed pool id format.", resp.Message)
}

func TestHandlePoolDetailNotFound(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		poolDetailErr: fmt.Errorf("get pool: %w", models.ErrPoolNotFound),
	})
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq8a7a2d",
		nil,
	)
	req.SetPathValue("pool_id", "pool1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq8a7a2d")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// TestHandlePoolDetailDatabaseFailure covers an opaque backing-store error
// (neither the invalid-ID nor not-found sentinels): it must surface as a
// generic 500 rather than being misclassified as a 400/404.
func TestHandlePoolDetailDatabaseFailure(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		poolDetailErr: errors.New("database is closed"),
	})
	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/pool1whatever", nil)
	req.SetPathValue("pool_id", "pool1whatever")
	w := httptest.NewRecorder()
	b.handlePoolDetail(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "failed to retrieve pool detail", resp.Message)
}

// TestPoolsRouteOrderingPoolDetailDoesNotSwallowSiblings is the acceptance
// test for route registration: even though "/pools/{pool_id}" is
// registered, requests for the literal sibling paths "/pools/retiring" and
// "/pools/extended" must still resolve to their own handlers, not be
// captured by the pool-detail wildcard. This exercises the real
// http.ServeMux built by (*Blockfrost).handler(), not a direct method
// call, so it verifies Go's actual pattern-specificity resolution rather
// than assuming it.
func TestPoolsRouteOrderingPoolDetailDoesNotSwallowSiblings(t *testing.T) {
	mock := &mockNode{
		poolsRetiringTotal: 1,
		poolsRetiring: []PoolRetiringInfo{
			{PoolID: "pool1retiring", Epoch: 10},
		},
		pools: []PoolExtendedInfo{
			{PoolID: "pool1extended"},
		},
		poolDetail: PoolDetailInfo{PoolID: "pool1detail"},
	}
	b := newTestBlockfrost(mock)
	handler := b.handler()

	req := httptest.NewRequest(http.MethodGet, "/api/v0/pools/retiring", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var retiringResp []PoolRetiringResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&retiringResp))
	require.Len(t, retiringResp, 1)
	assert.Equal(t, "pool1retiring", retiringResp[0].PoolID)

	req = httptest.NewRequest(http.MethodGet, "/api/v0/pools/extended", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var extendedResp []PoolExtendedResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&extendedResp))
	require.Len(t, extendedResp, 1)
	assert.Equal(t, "pool1extended", extendedResp[0].PoolID)

	req = httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1notretiringnorextended", nil,
	)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var detailResp PoolDetailResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&detailResp))
	assert.Equal(t, "pool1detail", detailResp.PoolID)

	req = httptest.NewRequest(
		http.MethodGet, "/api/v0/pools/pool1detail/metadata", nil,
	)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestPoolSizeSaturation(t *testing.T) {
	tests := []struct {
		name                                             string
		liveStake, activeStake, totalLive, totalActive   uint64
		nOpt                                             int
		wantLiveSize, wantActiveSize, wantLiveSaturation float64
	}{
		{
			name:               "normal pool",
			liveStake:          6_900_000_000,
			activeStake:        4_200_000_000,
			totalLive:          16_428_571_428,
			totalActive:        420_000_000_000,
			nOpt:               500,
			wantLiveSize:       6_900_000_000.0 / 16_428_571_428.0,
			wantActiveSize:     4_200_000_000.0 / 420_000_000_000.0,
			wantLiveSaturation: 6_900_000_000.0 / (420_000_000_000.0 / 500.0),
		},
		{
			name:               "zero total live stake",
			liveStake:          1000,
			totalLive:          0,
			totalActive:        1_000_000,
			activeStake:        500,
			nOpt:               100,
			wantLiveSize:       0,
			wantActiveSize:     500.0 / 1_000_000.0,
			wantLiveSaturation: 1000.0 / (1_000_000.0 / 100.0),
		},
		{
			name:               "zero total active stake",
			liveStake:          1000,
			totalLive:          10_000,
			activeStake:        0,
			totalActive:        0,
			nOpt:               100,
			wantLiveSize:       1000.0 / 10_000.0,
			wantActiveSize:     0,
			wantLiveSaturation: 0,
		},
		{
			name:               "nOpt not yet available",
			liveStake:          1000,
			totalLive:          10_000,
			activeStake:        500,
			totalActive:        1_000_000,
			nOpt:               0,
			wantLiveSize:       1000.0 / 10_000.0,
			wantActiveSize:     500.0 / 1_000_000.0,
			wantLiveSaturation: 0,
		},
		{
			name: "all zero",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			liveSize, activeSize, liveSaturation := poolSizeSaturation(
				tt.liveStake, tt.activeStake, tt.totalLive, tt.totalActive, tt.nOpt,
			)
			assert.InDelta(t, tt.wantLiveSize, liveSize, 1e-9)
			assert.InDelta(t, tt.wantActiveSize, activeSize, 1e-9)
			assert.InDelta(t, tt.wantLiveSaturation, liveSaturation, 1e-9)
		})
	}
}
