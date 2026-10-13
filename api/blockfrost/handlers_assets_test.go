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
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHandleAssetsList(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, path string }{
		{"all", "/api/v0/assets"},
		{"policy", "/api/v0/assets/policy/" + testPolicy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			node := &mockNode{}
			node.assetList = []AssetListItemInfo{
				{Asset: testAsset, Quantity: "10"},
				{Asset: testPolicy, Quantity: "0"},
			}
			node.assetTotal = 205
			w := serve(t, node, tc.path+"?count=2&page=3&order=desc")
			require.Equal(t, http.StatusOK, w.Code)
			require.JSONEq(t, `[
{"asset":"`+testAsset+`","quantity":"10"},
{"asset":"`+testPolicy+`","quantity":"0"}]`, w.Body.String())
			require.Equal(t, "205", w.Header().Get("X-Pagination-Count-Total"))
			require.Equal(t, "103", w.Header().Get("X-Pagination-Page-Total"))
			node.mu.Lock()
			defer node.mu.Unlock()
			require.Equal(t, PaginationParams{Count: 2, Page: 3, Order: "desc"}, node.lastAssetParams)
			if tc.name == "policy" {
				require.Equal(t, testPolicy, node.lastPolicyID)
			}
		})
	}
}

func TestHandleAssetsListEmptyAndInvalid(t *testing.T) {
	t.Parallel()
	w := serve(t, &mockNode{}, "/api/v0/assets")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[]`, w.Body.String())
	require.Equal(t, "0", w.Header().Get("X-Pagination-Count-Total"))

	for _, path := range []string{
		"/api/v0/assets?count=101",
		"/api/v0/assets/policy/" + testPolicy + "?page=0",
	} {
		w := serve(t, &mockNode{}, path)
		require.Equal(t, http.StatusBadRequest, w.Code, path)
		require.Contains(t, w.Body.String(), "Invalid pagination parameters.")
	}
	for _, policy := range []string{"abcd", strings.Repeat("z", 56), testAsset} {
		w := serve(t, &mockNode{}, "/api/v0/assets/policy/"+policy)
		require.Equal(t, http.StatusBadRequest, w.Code, policy)
		require.Contains(t, w.Body.String(), "Invalid policy ID.")
	}
	node := &mockNode{}
	node.assetListErr = errors.New("boom")
	w = serve(t, node, "/api/v0/assets")
	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAssetHistory(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.assetHistory = []AssetHistoryItemInfo{
		{TxHash: "aa", Action: "minted", Amount: "10"},
		{TxHash: "bb", Action: "burned", Amount: "3"},
	}
	node.assetTotal = 2
	w := serve(t, node, "/api/v0/assets/"+testAsset+"/history?order=desc")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[
{"tx_hash":"aa","action":"minted","amount":"10"},
{"tx_hash":"bb","action":"burned","amount":"3"}]`, w.Body.String())
	node.mu.Lock()
	require.Equal(t, testPolicy, node.lastAssetPolicy)
	require.Equal(t, []byte("nutcoin"), node.lastAssetName)
	require.Equal(t, "desc", node.lastAssetParams.Order)
	node.mu.Unlock()
	require.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
}

func TestHandleAssetListingsMapErrors(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"history", "txs", "transactions", "utxos"} {
		path := "/api/v0/assets/" + testAsset + "/" + route
		node := &mockNode{}
		node.assetListErr = ErrAssetNotFound
		w := serve(t, node, path)
		require.Equal(t, http.StatusNotFound, w.Code, route)
		require.Contains(t, w.Body.String(), "The requested asset could not be found.")

		node = &mockNode{}
		node.assetListErr = errors.New("boom")
		w = serve(t, node, path)
		require.Equal(t, http.StatusInternalServerError, w.Code, route)

		w = serve(t, &mockNode{}, "/api/v0/assets/abcd/"+route)
		require.Equal(t, http.StatusBadRequest, w.Code, route)
		require.Contains(t, w.Body.String(), "Invalid asset identifier.")

		w = serve(t, &mockNode{}, path+"?count=0")
		require.Equal(t, http.StatusBadRequest, w.Code, route)
		require.Contains(t, w.Body.String(), "Invalid pagination parameters.")
	}
}

func TestHandleAssetTransactions(t *testing.T) {
	t.Parallel()
	rows := []AssetTransactionInfo{
		{TxHash: "aa", TxIndex: 2, BlockHeight: 7, BlockTime: 70},
		{TxHash: "bb", TxIndex: 0, BlockHeight: 8, BlockTime: 80},
	}
	node := &mockNode{}
	node.assetTxs = rows
	node.assetTotal = 9
	w := serve(
		t, node,
		"/api/v0/assets/"+testAsset+"/transactions?from=5:1&to=9&count=2",
	)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[
{"tx_hash":"aa","tx_index":2,"block_height":7,"block_time":70},
{"tx_hash":"bb","tx_index":0,"block_height":8,"block_time":80}]`, w.Body.String())
	require.Equal(t, "9", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "5", w.Header().Get("X-Pagination-Page-Total"))
	node.mu.Lock()
	got := node.lastAssetRange
	node.mu.Unlock()
	require.NotNil(t, got.From)
	require.Equal(t, uint64(5), got.From.Block)
	require.NotNil(t, got.From.Index)
	require.Equal(t, uint32(1), *got.From.Index)
	require.NotNil(t, got.To)
	require.Equal(t, uint64(9), got.To.Block)

	w = serve(t, &mockNode{}, "/api/v0/assets/"+testAsset+"/transactions?from=x")
	require.Equal(t, http.StatusBadRequest, w.Code)
	w = serve(t, &mockNode{}, "/api/v0/assets/"+testAsset+"/transactions?from=9&to=3")
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAssetLegacyTransactionsReturnsHashes(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.assetTxs = []AssetTransactionInfo{{TxHash: "aa"}, {TxHash: "bb"}}
	node.assetTotal = 2
	w := serve(t, node, "/api/v0/assets/"+testAsset+"/txs?from=5&order=desc")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `["aa","bb"]`, w.Body.String())
	require.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	node.mu.Lock()
	defer node.mu.Unlock()
	// The deprecated listing takes no block range.
	require.Nil(t, node.lastAssetRange.From)
	require.Equal(t, "desc", node.lastAssetRange.Pagination.Order)
}

func TestHandleAssetUTXOs(t *testing.T) {
	t.Parallel()
	datum := "d87980"
	hash := "0b"
	node := &mockNode{}
	node.assetUTXOs = []AssetUTXOInfo{
		{
			Address:         "addr1",
			TxHash:          "aa",
			OutputIndex:     1,
			Amount:          []AddressAmountInfo{{Unit: "lovelace", Quantity: "2"}},
			Block:           "bb",
			BlockHeight:     7,
			BlockTime:       70,
			InlineDatum:     &datum,
			InlineDatumJSON: json.RawMessage(`{"constructor":0,"fields":[]}`),
		},
		{
			Address:     "addr2",
			TxHash:      "cc",
			OutputIndex: 0,
			Amount:      []AddressAmountInfo{},
			Block:       "dd",
			DataHash:    &hash,
		},
	}
	node.assetTotal = 2
	w := serve(t, node, "/api/v0/assets/"+testAsset+"/utxos?from=3")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[
{"address":"addr1","tx_hash":"aa","output_index":1,
 "amount":[{"unit":"lovelace","quantity":"2"}],"block":"bb",
 "block_height":7,"block_time":70,"data_hash":null,
 "inline_datum":"d87980","inline_datum_json":{"constructor":0,"fields":[]},
 "reference_script_hash":null},
{"address":"addr2","tx_hash":"cc","output_index":0,"amount":[],"block":"dd",
 "block_height":0,"block_time":0,"data_hash":"0b","inline_datum":null,
 "inline_datum_json":null,"reference_script_hash":null}]`, w.Body.String())
	node.mu.Lock()
	defer node.mu.Unlock()
	require.NotNil(t, node.lastAssetRange.From)
	require.Equal(t, uint64(3), node.lastAssetRange.From.Block)
}

// "policy" and a real asset id both live under /assets/{x}/..., and the
// literal must not capture an asset's own sub-resources or vice versa.
func TestHandleAssetRoutesDoNotShadowEachOther(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.assetHolders = []AssetHolderInfo{{Address: "addr1", Quantity: "1"}}
	node.assetHoldersTotal = 1
	w := serve(t, node, "/api/v0/assets/"+testAsset+"/addresses")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "addr1")

	w = serve(t, node, "/api/v0/assets/policy/addresses")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "Invalid policy ID.")

	w = serve(t, node, "/api/v0/assets/"+testAsset+"/unknown")
	require.Equal(t, http.StatusNotFound, w.Code)

	w = serve(t, &mockNode{asset: AssetInfo{Asset: testAsset}}, "/api/v0/assets/"+testAsset)
	require.Equal(t, http.StatusOK, w.Code)
}
