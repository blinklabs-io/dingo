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
	"strconv"
	"strings"
)

const (
	DefaultPaginationCount    = 100
	MaxPaginationCount        = 100
	DefaultPaginationPage     = 1
	DefaultPaginationOrderAsc = "asc"
	PaginationOrderDesc       = "desc"
)

var ErrInvalidPaginationParameters = errors.New(
	"invalid pagination parameters",
)

// PaginationParams contains parsed pagination query values.
type PaginationParams struct {
	Count int
	Page  int
	Order string
}

// ParsePagination parses pagination query parameters and
// applies defaults and bounds clamping.
func ParsePagination(r *http.Request) (PaginationParams, error) {
	// Defined params with default values
	params := PaginationParams{
		Count: DefaultPaginationCount,
		Page:  DefaultPaginationPage,
		Order: DefaultPaginationOrderAsc,
	}

	// Parse params value from the request query
	query := r.URL.Query()
	if countParam := query.Get("count"); countParam != "" {
		count, err := strconv.Atoi(countParam)
		if err != nil {
			return PaginationParams{},
				ErrInvalidPaginationParameters
		}
		params.Count = count
	}

	if pageParam := query.Get("page"); pageParam != "" {
		page, err := strconv.Atoi(pageParam)
		if err != nil {
			return PaginationParams{},
				ErrInvalidPaginationParameters
		}
		params.Page = page
	}

	if orderParam := query.Get("order"); orderParam != "" {
		convertedOrder := strings.ToLower(orderParam)
		switch convertedOrder {
		case DefaultPaginationOrderAsc, PaginationOrderDesc:
			params.Order = convertedOrder
		default:
			return PaginationParams{},
				ErrInvalidPaginationParameters
		}
	}

	// Bounds clamping
	if params.Count < 1 {
		params.Count = 1
	}
	if params.Count > MaxPaginationCount {
		params.Count = MaxPaginationCount
	}
	if params.Page < 1 {
		params.Page = 1
	}

	return params, nil
}

// SetPaginationHeaders sets Blockfrost pagination headers.
func SetPaginationHeaders(
	w http.ResponseWriter,
	totalItems int,
	params PaginationParams,
) {
	if totalItems < 0 {
		totalItems = 0
	}
	if params.Count < 1 {
		params.Count = DefaultPaginationCount
	}
	totalPages := 0
	if totalItems > 0 {
		// Equivalent to ceil(totalItems/params.count)
		totalPages = (totalItems + params.Count - 1) / params.Count
	}
	w.Header().Set(
		"X-Pagination-Count-Total",
		strconv.Itoa(totalItems),
	)
	w.Header().Set(
		"X-Pagination-Page-Total",
		strconv.Itoa(totalPages),
	)
}
