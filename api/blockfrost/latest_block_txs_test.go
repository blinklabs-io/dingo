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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type latestBlockPaginationNode struct {
	*mockNode
	calls int
}

func (n *latestBlockPaginationNode) LatestBlockTxHashes() ([]string, error) {
	n.calls++
	return n.mockNode.LatestBlockTxHashes()
}

func TestLatestBlockTransactionsPagesInBlockOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, query, want, pages string
	}{
		{"first", "count=2", `["tx0","tx1"]`, "3"},
		{"second", "count=2&page=2", `["tx2","tx3"]`, "3"},
		{"last", "count=2&page=3", `["tx4"]`, "3"},
		{"past last", "count=2&page=4", `[]`, "3"},
		{"descending first", "count=2&order=desc", `["tx4","tx3"]`, "3"},
		{"descending second", "count=2&page=2&order=desc", `["tx2","tx1"]`, "3"},
		{"descending last", "count=2&page=3&order=desc", `["tx0"]`, "3"},
		{"minimum count", "count=1&page=2", `["tx1"]`, "5"},
		{"maximum count", "count=100", `["tx0","tx1","tx2","tx3","tx4"]`, "1"},
		{"maximum page", "count=100&page=21474836", `[]`, "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			hashes := []string{"tx0", "tx1", "tx2", "tx3", "tx4"}
			node := &latestBlockPaginationNode{
				mockNode: &mockNode{txHashes: hashes},
			}
			b := newTestBlockfrost(node)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(
				http.MethodGet,
				"/api/v0/blocks/latest/txs?"+tc.query,
				nil,
			)
			b.handler().ServeHTTP(w, r)
			require.Equal(t, http.StatusOK, w.Code)
			require.JSONEq(t, tc.want, w.Body.String())
			require.Equal(t, "5", w.Header().Get("X-Pagination-Count-Total"))
			require.Equal(
				t,
				tc.pages,
				w.Header().Get("X-Pagination-Page-Total"),
			)
			require.Equal(t, 1, node.calls)
			require.Equal(
				t,
				[]string{"tx0", "tx1", "tx2", "tx3", "tx4"},
				hashes,
			)
		})
	}
}

func TestLatestBlockTransactionsDefaultPageIsBounded(t *testing.T) {
	t.Parallel()
	hashes := make([]string, 101)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("tx%d", i)
	}
	b := newTestBlockfrost(&mockNode{txHashes: hashes})
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/api/v0/blocks/latest/txs", nil)
	b.handler().ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var page []string
	require.NoError(t, json.NewDecoder(w.Body).Decode(&page))
	require.Equal(t, hashes[:100], page)
	require.Equal(t, "101", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))
}

func TestLatestBlockTransactionsRejectInvalidPaginationBeforeQuery(
	t *testing.T,
) {
	t.Parallel()
	for _, query := range []string{
		"count=0", "count=-1", "count=101", "count=abc",
		"page=0", "page=-1", "page=21474837", "page=abc", "order=sideways",
	} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			node := &latestBlockPaginationNode{mockNode: &mockNode{}}
			b := newTestBlockfrost(node)
			w := httptest.NewRecorder()
			r := httptest.NewRequest(
				http.MethodGet,
				"/api/v0/blocks/latest/txs?"+query,
				nil,
			)
			b.handler().ServeHTTP(w, r)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.JSONEq(
				t,
				`{"status_code":400,"error":"Bad Request","message":"Invalid pagination parameters."}`,
				w.Body.String(),
			)
			require.Zero(t, node.calls)
		})
	}
}

func TestLatestBlockTransactionsEmptyAndQueryFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		status int
		want   string
	}{
		{"empty", nil, http.StatusOK, `[]`},
		{"query failure", errors.New("query failed"), http.StatusInternalServerError, `{"status_code":500,"error":"Internal Server Error","message":"failed to retrieve latest block transactions"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBlockfrost(&mockNode{txHashesErr: tc.err})
			w := httptest.NewRecorder()
			r := httptest.NewRequest(
				http.MethodGet,
				"/api/v0/blocks/latest/txs?count=2&page=2&order=desc",
				nil,
			)
			b.handler().ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code)
			require.JSONEq(t, tc.want, w.Body.String())
			if tc.err == nil {
				require.Equal(
					t,
					"0",
					w.Header().Get("X-Pagination-Count-Total"),
				)
				require.Equal(t, "0", w.Header().Get("X-Pagination-Page-Total"))
			} else {
				require.Empty(t, w.Header().Get("X-Pagination-Count-Total"))
				require.Empty(t, w.Header().Get("X-Pagination-Page-Total"))
			}
		})
	}
}
