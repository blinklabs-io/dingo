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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandlePoolsExtendedSchemaFields is the shape/pagination/order and
// metadata success/absence/failure acceptance test for #2489: it exercises
// handlePoolsExtended (not the adapter) against a mock that returns the
// full pool_list_extended field set, including all three metadata states,
// and does not reference vrf_key or relays, which are not part of the
// current OpenAPI schema.
func TestHandlePoolsExtendedSchemaFields(t *testing.T) {
	ticker := "ABC"
	name := "Pool ABC"
	description := "A pool."
	homepage := "https://example.com"
	url := "https://example.com/pool.json"
	hash := "deadbeef"

	mock := &mockNode{
		pools: []PoolExtendedInfo{
			{
				// No registered metadata anchor -> metadata: null.
				PoolID:         "pool1zzz",
				Hex:            "ff",
				ActiveStake:    "200",
				LiveStake:      "300",
				BlocksMinted:   7,
				LiveSaturation: 0.42,
				DeclaredPledge: "400",
				MarginCost:     0.2,
				FixedCost:      "500",
				Metadata:       nil,
			},
			{
				// Anchor present, fetch failed -> metadata is a non-null
				// object with the error object and every off-chain field
				// left null.
				PoolID:         "pool1mmm",
				Hex:            "80",
				ActiveStake:    "150",
				LiveStake:      "250",
				BlocksMinted:   1,
				LiveSaturation: 0.1,
				DeclaredPledge: "350",
				MarginCost:     0.15,
				FixedCost:      "450",
				Metadata: &PoolExtendedMetadataInfo{
					URL:  &url,
					Hash: &hash,
					Error: &OffchainFetchErrorInfo{
						Code:    "HASH_MISMATCH",
						Message: "hash mismatch",
					},
				},
			},
			{
				// Anchor present, fetch succeeded -> metadata fully
				// populated with no error.
				PoolID:         "pool1aaa",
				Hex:            "01",
				ActiveStake:    "20",
				LiveStake:      "30",
				BlocksMinted:   3,
				LiveSaturation: 0.93,
				DeclaredPledge: "40",
				MarginCost:     0.1,
				FixedCost:      "50",
				Metadata: &PoolExtendedMetadataInfo{
					URL:         &url,
					Hash:        &hash,
					Ticker:      &ticker,
					Name:        &name,
					Description: &description,
					Homepage:    &homepage,
				},
			},
		},
	}
	b := newTestBlockfrost(mock)

	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/extended?count=2&page=1&order=asc",
		nil,
	)
	w := httptest.NewRecorder()
	b.handlePoolsExtended(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "3", w.Header().Get("X-Pagination-Count-Total"))
	assert.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	var resp []PoolExtendedResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	require.Len(t, resp, 2)

	// Ascending hex order: "01" (pool1aaa) then "80" (pool1mmm), matching
	// hosted Blockfrost's ordering semantics for this endpoint.
	assert.Equal(t, "pool1aaa", resp[0].PoolID)
	assert.Equal(t, "01", resp[0].Hex)
	assert.Equal(t, "20", resp[0].ActiveStake)
	assert.Equal(t, "30", resp[0].LiveStake)
	assert.Equal(t, uint64(3), resp[0].BlocksMinted)
	assert.InDelta(t, 0.93, resp[0].LiveSaturation, 0.0001)
	assert.Equal(t, "40", resp[0].DeclaredPledge)
	assert.Equal(t, "50", resp[0].FixedCost)
	assert.InDelta(t, 0.1, resp[0].MarginCost, 0.0001)
	require.NotNil(t, resp[0].Metadata)
	assert.Nil(t, resp[0].Metadata.Error)
	require.NotNil(t, resp[0].Metadata.Ticker)
	assert.Equal(t, "ABC", *resp[0].Metadata.Ticker)
	require.NotNil(t, resp[0].Metadata.Name)
	assert.Equal(t, "Pool ABC", *resp[0].Metadata.Name)

	assert.Equal(t, "pool1mmm", resp[1].PoolID)
	assert.Equal(t, "80", resp[1].Hex)
	require.NotNil(t, resp[1].Metadata)
	require.NotNil(t, resp[1].Metadata.URL)
	assert.Equal(t, url, *resp[1].Metadata.URL)
	assert.Nil(t, resp[1].Metadata.Name)
	require.NotNil(t, resp[1].Metadata.Error)
	assert.Equal(t, "HASH_MISMATCH", resp[1].Metadata.Error.Code)

	// Page 2 holds the pool with no metadata anchor at all.
	req = httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/extended?count=2&page=2&order=asc",
		nil,
	)
	w = httptest.NewRecorder()
	b.handlePoolsExtended(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var page2 []PoolExtendedResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&page2))
	require.Len(t, page2, 1)
	assert.Equal(t, "pool1zzz", page2[0].PoolID)
	assert.Equal(t, uint64(7), page2[0].BlocksMinted)
	assert.InDelta(t, 0.42, page2[0].LiveSaturation, 0.0001)
	assert.Nil(t, page2[0].Metadata)

	// Descending order reverses the page-1 ordering.
	req = httptest.NewRequest(
		http.MethodGet,
		"/api/v0/pools/extended?count=2&page=1&order=desc",
		nil,
	)
	w = httptest.NewRecorder()
	b.handlePoolsExtended(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	var descResp []PoolExtendedResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&descResp))
	require.Len(t, descResp, 2)
	assert.Equal(t, "pool1zzz", descResp[0].PoolID)
	assert.Equal(t, "pool1mmm", descResp[1].PoolID)
}
