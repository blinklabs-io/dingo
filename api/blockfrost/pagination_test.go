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
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePaginationDefaultValues(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/v0/test", nil)
	params, err := ParsePagination(req)
	require.NoError(t, err)

	assert.Equal(t, DefaultPaginationCount, params.Count)
	assert.Equal(t, DefaultPaginationPage, params.Page)
	assert.Equal(t, PaginationOrderAsc, params.Order)
}

func TestParsePaginationValid(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/test?count=25&page=3&order=DESC",
		nil,
	)
	params, err := ParsePagination(req)
	require.NoError(t, err)

	assert.Equal(t, 25, params.Count)
	assert.Equal(t, 3, params.Page)
	assert.Equal(t, PaginationOrderDesc, params.Order)
}

func TestParsePaginationClampBounds(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/test?count=999&page=0",
		nil,
	)
	params, err := ParsePagination(req)
	require.NoError(t, err)

	assert.Equal(t, MaxPaginationCount, params.Count)
	assert.Equal(t, 1, params.Page)
	assert.Equal(t, PaginationOrderAsc, params.Order)
}

func TestParsePaginationClampsUnboundedPage(t *testing.T) {
	// The largest value strconv.Atoi accepts on a 64-bit platform: large
	// enough to have overflowed a naive offset calculation (page-1)*count
	// before this was bounded, without itself overflowing int parsing.
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/test?page=9223372036854775807",
		nil,
	)
	params, err := ParsePagination(req)
	require.NoError(t, err)

	assert.Equal(t, MaxPaginationPage, params.Page)
}

func TestParsePaginationClampsPageAboveMax(t *testing.T) {
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v0/test?page=21474837",
		nil,
	)
	params, err := ParsePagination(req)
	require.NoError(t, err)

	assert.Equal(t, MaxPaginationPage, params.Page)
}

func TestPaginationOffset(t *testing.T) {
	tests := []struct {
		name       string
		params     PaginationParams
		wantOffset int
		wantOK     bool
	}{
		{
			name:       "first page",
			params:     PaginationParams{Page: 1, Count: 100},
			wantOffset: 0,
			wantOK:     true,
		},
		{
			name:       "third page",
			params:     PaginationParams{Page: 3, Count: 25},
			wantOffset: 50,
			wantOK:     true,
		},
		{
			name: "max page and count does not overflow",
			params: PaginationParams{
				Page:  MaxPaginationPage,
				Count: MaxPaginationCount,
			},
			wantOffset: (MaxPaginationPage - 1) * MaxPaginationCount,
			wantOK:     true,
		},
		{
			name:   "zero page is invalid",
			params: PaginationParams{Page: 0, Count: 100},
			wantOK: false,
		},
		{
			name:   "zero count is invalid",
			params: PaginationParams{Page: 1, Count: 0},
			wantOK: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			offset, ok := paginationOffset(test.params)
			assert.Equal(t, test.wantOK, ok)
			if test.wantOK {
				assert.Equal(t, test.wantOffset, offset)
			}
		})
	}
}

func TestParsePaginationInvalid(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "non-numeric count", url: "/api/v0/test?count=abc"},
		{name: "non-numeric page", url: "/api/v0/test?page=abc"},
		{name: "invalid order", url: "/api/v0/test?order=sideways"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodGet,
				test.url,
				nil,
			)
			params, err := ParsePagination(req)
			require.Error(t, err)
			assert.True(
				t,
				errors.Is(err, ErrInvalidPaginationParameters),
			)
			assert.Equal(t, PaginationParams{}, params)
		})
	}
}

func TestSetPaginationHeaders(t *testing.T) {
	recorder := httptest.NewRecorder()
	SetPaginationHeaders(
		recorder,
		250,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	assert.Equal(
		t,
		"250",
		recorder.Header().Get("X-Pagination-Count-Total"),
	)
	assert.Equal(
		t,
		"3",
		recorder.Header().Get("X-Pagination-Page-Total"),
	)
}

func TestSetPaginationHeadersZeroTotals(t *testing.T) {
	recorder := httptest.NewRecorder()
	SetPaginationHeaders(
		recorder,
		-1,
		PaginationParams{Count: 0, Page: 1, Order: "asc"},
	)
	assert.Equal(
		t,
		"0",
		recorder.Header().Get("X-Pagination-Count-Total"),
	)
	assert.Equal(
		t,
		"0",
		recorder.Header().Get("X-Pagination-Page-Total"),
	)
}
