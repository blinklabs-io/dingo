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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func serve(
	t *testing.T,
	node BlockfrostNode,
	path string,
) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	newTestBlockfrost(node).handler().ServeHTTP(
		w,
		httptest.NewRequest(http.MethodGet, path, nil),
	)
	return w
}

func testBlockInfo(height uint64) BlockInfo {
	return BlockInfo{
		Hash:          fmt.Sprintf("hash%d", height),
		Slot:          height * 10,
		Height:        height,
		Epoch:         1,
		EpochSlot:     height,
		Time:          1700000000 + int64(height),
		Size:          512,
		TxCount:       2,
		SlotLeader:    "pool1leader",
		PreviousBlock: fmt.Sprintf("hash%d", height-1),
		Confirmations: 3,
		Output:        "1000",
		Fees:          "10",
	}
}

func TestHandleBlockNextAndPrevious(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"next", "previous"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			node := &mockNode{}
			node.navBlocks = []BlockInfo{testBlockInfo(8), testBlockInfo(9)}
			w := serve(t, node, "/api/v0/blocks/7/"+route+"?count=2&page=3")
			require.Equal(t, http.StatusOK, w.Code)
			require.JSONEq(t, `[
{"time":1700000008,"height":8,"hash":"hash8","slot":80,"epoch":1,"epoch_slot":8,"slot_leader":"pool1leader","size":512,"tx_count":2,"output":"1000","fees":"10","block_vrf":null,"op_cert":null,"op_cert_counter":null,"previous_block":"hash7","next_block":null,"confirmations":3},
{"time":1700000009,"height":9,"hash":"hash9","slot":90,"epoch":1,"epoch_slot":9,"slot_leader":"pool1leader","size":512,"tx_count":2,"output":"1000","fees":"10","block_vrf":null,"op_cert":null,"op_cert_counter":null,"previous_block":"hash8","next_block":null,"confirmations":3}
]`, w.Body.String())
			node.mu.Lock()
			defer node.mu.Unlock()
			require.Equal(t, "7", node.lastBlockID)
			require.Equal(t, 2, node.lastBlockParams.Count)
			require.Equal(t, 3, node.lastBlockParams.Page)
		})
	}
}

func TestHandleBlockNextAndPreviousEmptyIsArray(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"next", "previous"} {
		w := serve(t, &mockNode{}, "/api/v0/blocks/7/"+route)
		require.Equal(t, http.StatusOK, w.Code)
		require.JSONEq(t, `[]`, w.Body.String())
	}
}

func TestHandleBlockNavigationErrors(t *testing.T) {
	t.Parallel()
	for _, route := range []string{
		"next", "previous", "txs", "txs/cbor", "addresses",
	} {
		for _, tc := range []struct {
			name   string
			err    error
			status int
			msg    string
		}{
			{"invalid id", ErrInvalidBlockID, 400, "Invalid block hash or number."},
			{"not found", ErrBlockNotFound, 404, "The requested block could not be found."},
			{"failure", errors.New("db down"), 500, "failed to retrieve block"},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				node.navErr = tc.err
				w := serve(t, node, "/api/v0/blocks/7/"+route)
				require.Equal(t, tc.status, w.Code)
				require.Contains(t, w.Body.String(), tc.msg)
			})
		}
	}
}

func TestHandleBlockNavigationRejectsInvalidPagination(t *testing.T) {
	t.Parallel()
	for _, route := range []string{
		"next", "previous", "txs", "txs/cbor", "addresses",
	} {
		for _, query := range []string{
			"count=0", "count=101", "page=0", "page=21474837", "order=up",
		} {
			t.Run(route+"?"+query, func(t *testing.T) {
				t.Parallel()
				node := &mockNode{}
				w := serve(t, node, "/api/v0/blocks/7/"+route+"?"+query)
				require.Equal(t, http.StatusBadRequest, w.Code)
				require.Contains(
					t,
					w.Body.String(),
					"Invalid pagination parameters.",
				)
				node.mu.Lock()
				defer node.mu.Unlock()
				require.Zero(t, node.lastBlockCalls)
			})
		}
	}
}

func TestHandleBlockBySlot(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.navBlock = testBlockInfo(5)
	w := serve(t, node, "/api/v0/blocks/slot/50")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"hash":"hash5"`)
	node.mu.Lock()
	require.Equal(t, uint64(50), node.lastBlockSlot)
	node.mu.Unlock()

	for _, bad := range []string{"abc", "-1", "1.5", "18446744073709551616"} {
		w := serve(t, &mockNode{}, "/api/v0/blocks/slot/"+bad)
		require.Equal(t, http.StatusBadRequest, w.Code, bad)
		require.Contains(t, w.Body.String(), "Invalid slot number.")
	}

	notFound := &mockNode{}
	notFound.navErr = ErrBlockNotFound
	w = serve(t, notFound, "/api/v0/blocks/slot/99")
	require.Equal(t, http.StatusNotFound, w.Code)

	failing := &mockNode{}
	failing.navErr = errors.New("db down")
	w = serve(t, failing, "/api/v0/blocks/slot/99")
	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleBlockByEpochSlot(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.navBlock = testBlockInfo(6)
	w := serve(t, node, "/api/v0/blocks/epoch/12/slot/345")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"hash":"hash6"`)
	node.mu.Lock()
	require.Equal(t, uint64(12), node.lastBlockEpoch)
	require.Equal(t, uint64(345), node.lastBlockEpSlot)
	node.mu.Unlock()

	for _, path := range []string{
		"/api/v0/blocks/epoch/x/slot/1",
		"/api/v0/blocks/epoch/1/slot/x",
		"/api/v0/blocks/epoch/-1/slot/1",
	} {
		w := serve(t, &mockNode{}, path)
		require.Equal(t, http.StatusBadRequest, w.Code, path)
	}

	notFound := &mockNode{}
	notFound.navErr = ErrBlockNotFound
	w = serve(t, notFound, "/api/v0/blocks/epoch/1/slot/1")
	require.Equal(t, http.StatusNotFound, w.Code)
}

// The "slot" literal and the block identifier wildcard overlap on paths such
// as /blocks/slot/next, so the router must resolve each to one handler.
func TestHandleBlockRoutesDoNotShadowEachOther(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.navBlock = testBlockInfo(5)
	node.navHashes = []string{"a"}
	w := serve(t, node, "/api/v0/blocks/slot/next")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "Invalid slot number.")
	w = serve(t, node, "/api/v0/blocks/abc/unknown")
	require.Equal(t, http.StatusNotFound, w.Code)
	w = serve(t, node, "/api/v0/blocks/latest/txs")
	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleBlockTransactionHashesPages(t *testing.T) {
	t.Parallel()
	hashes := []string{"tx0", "tx1", "tx2", "tx3", "tx4"}
	for _, tc := range []struct {
		name, query, want, pages string
	}{
		{"first", "count=2", `["tx0","tx1"]`, "3"},
		{"last", "count=2&page=3", `["tx4"]`, "3"},
		{"past last", "count=2&page=4", `[]`, "3"},
		{"descending", "count=2&order=desc", `["tx4","tx3"]`, "3"},
		{"descending last", "count=2&page=3&order=desc", `["tx0"]`, "3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := &mockNode{}
			node.navHashes = hashes
			w := serve(t, node, "/api/v0/blocks/abc/txs?"+tc.query)
			require.Equal(t, http.StatusOK, w.Code)
			require.JSONEq(t, tc.want, w.Body.String())
			require.Equal(t, "5", w.Header().Get("X-Pagination-Count-Total"))
			require.Equal(
				t,
				tc.pages,
				w.Header().Get("X-Pagination-Page-Total"),
			)
			node.mu.Lock()
			defer node.mu.Unlock()
			require.Equal(t, "abc", node.lastBlockID)
		})
	}
}

func TestHandleBlockTransactionCBOR(t *testing.T) {
	t.Parallel()
	rows := []BlockTransactionCBORInfo{
		{TxHash: "t0", CBOR: "aa"}, {TxHash: "t1", CBOR: "bb"},
		{TxHash: "t2", CBOR: "cc"},
	}
	for _, path := range []string{
		"/api/v0/blocks/abc/txs/cbor",
		"/api/v0/blocks/latest/txs/cbor",
	} {
		node := &mockNode{}
		node.navTxCBOR = rows
		w := serve(t, node, path+"?count=2&order=desc")
		require.Equal(t, http.StatusOK, w.Code, path)
		require.JSONEq(
			t,
			`[{"tx_hash":"t2","cbor":"cc"},{"tx_hash":"t1","cbor":"bb"}]`,
			w.Body.String(),
		)
		require.Equal(t, "3", w.Header().Get("X-Pagination-Count-Total"))
		require.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))
	}
	node := &mockNode{}
	w := serve(t, node, "/api/v0/blocks/latest/txs/cbor")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[]`, w.Body.String())
	node.mu.Lock()
	require.Equal(t, "latest", node.lastBlockID)
	node.mu.Unlock()
}

func TestHandleBlockAddresses(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.navAddresses = []BlockAddressInfo{
		{Address: "addr1", TxHashes: []string{"t0", "t1"}},
		{Address: "addr2", TxHashes: []string{"t1"}},
		{Address: "addr3", TxHashes: []string{"t2"}},
	}
	w := serve(t, node, "/api/v0/blocks/abc/addresses?count=2&page=2")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(
		t,
		`[{"address":"addr3","transactions":[{"tx_hash":"t2"}]}]`,
		w.Body.String(),
	)
	require.Equal(t, "3", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "2", w.Header().Get("X-Pagination-Page-Total"))

	w = serve(t, node, "/api/v0/blocks/abc/addresses")
	require.JSONEq(t, `[
{"address":"addr1","transactions":[{"tx_hash":"t0"},{"tx_hash":"t1"}]},
{"address":"addr2","transactions":[{"tx_hash":"t1"}]},
{"address":"addr3","transactions":[{"tx_hash":"t2"}]}]`, w.Body.String())
}
