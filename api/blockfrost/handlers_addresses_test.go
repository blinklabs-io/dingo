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
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testPolicy = "b0d07d45fe9514f80213f4020e5a61241458be626841cde717cb38a7"
	testAsset  = testPolicy + "6e7574636f696e"
)

func TestHandleAddressLegacyTransactionsReturnsHashes(t *testing.T) {
	t.Parallel()
	node := &mockNode{
		addressTransactions: []AddressTransactionInfo{
			{TxHash: "aa", TxIndex: 1, BlockHeight: 7, BlockTime: 9},
			{TxHash: "bb", TxIndex: 0, BlockHeight: 8, BlockTime: 10},
		},
		addressTxsTotal: 5,
	}
	w := serve(
		t, node,
		"/api/v0/addresses/addr1/txs?count=2&page=2&order=desc&from=3&to=9",
	)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `["aa","bb"]`, w.Body.String())
	require.Equal(t, "5", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "3", w.Header().Get("X-Pagination-Page-Total"))
	got := node.lastAddressTransactionsParams
	require.Equal(
		t,
		PaginationParams{Count: 2, Page: 2, Order: "desc"},
		got.Pagination,
	)
	// The deprecated listing takes no block range.
	require.Nil(t, got.From)
	require.Nil(t, got.To)
}

func TestHandleAddressLegacyTransactionsEmptyAndErrors(t *testing.T) {
	t.Parallel()
	w := serve(t, &mockNode{}, "/api/v0/addresses/addr1/txs")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[]`, w.Body.String())

	w = serve(t, &mockNode{}, "/api/v0/addresses/addr1/txs?count=0")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "Invalid pagination parameters.")

	w = serve(t, &mockNode{addressTransactionsErr: ErrInvalidAddress},
		"/api/v0/addresses/bad/txs")
	require.Equal(t, http.StatusBadRequest, w.Code)

	w = serve(t, &mockNode{addressTransactionsErr: errors.New("boom")},
		"/api/v0/addresses/addr1/txs")
	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleAddressExtended(t *testing.T) {
	t.Parallel()
	six := 6
	stake := "stake1u9"
	node := &mockNode{}
	node.addressExtended = AddressExtendedInfo{
		Address: "addr1",
		Amount: []AddressExtendedAmountInfo{
			{Unit: "lovelace", Quantity: "100", Decimals: &six},
			{Unit: testAsset, Quantity: "5", HasNFTOnchainMetadata: true},
		},
		StakeAddress: &stake,
		Type:         "shelley",
	}
	w := serve(t, node, "/api/v0/addresses/addr1/extended")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{
"address":"addr1",
"amount":[
 {"unit":"lovelace","quantity":"100","decimals":6,"has_nft_onchain_metadata":false},
 {"unit":"`+testAsset+`","quantity":"5","decimals":null,"has_nft_onchain_metadata":true}],
"stake_address":"stake1u9","type":"shelley","script":false}`, w.Body.String())
	require.Equal(t, "addr1", node.lastAddress)
}

func TestHandleAddressTotal(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.addressTotal = AddressTotalInfo{
		Address:     "addr1",
		ReceivedSum: []AddressAmountInfo{{Unit: "lovelace", Quantity: "30"}},
		SentSum:     []AddressAmountInfo{},
		TxCount:     4,
	}
	w := serve(t, node, "/api/v0/addresses/addr1/total")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"address":"addr1",
"received_sum":[{"unit":"lovelace","quantity":"30"}],
"sent_sum":[],"tx_count":4}`, w.Body.String())
}

func TestHandleAddressExtendedAndTotalErrors(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"extended", "total"} {
		for _, tc := range []struct {
			name   string
			err    error
			status int
		}{
			{"invalid", ErrInvalidAddress, http.StatusBadRequest},
			{"not found", ErrAddressNotFound, http.StatusNotFound},
			{"failure", errors.New("boom"), http.StatusInternalServerError},
		} {
			node := &mockNode{}
			node.addressErr = tc.err
			w := serve(t, node, "/api/v0/addresses/addr1/"+route)
			require.Equal(t, tc.status, w.Code, route+" "+tc.name)
		}
	}
}

func TestHandleAddressAssetUTXOs(t *testing.T) {
	t.Parallel()
	datum := "d8799f00ff"
	node := &mockNode{}
	node.addressAssetUTXO = []AddressUTXOInfo{{
		Address:     "addr1",
		TxHash:      "aa",
		TxIndex:     1,
		OutputIndex: 1,
		Amount: []AddressAmountInfo{
			{Unit: "lovelace", Quantity: "2"},
			{Unit: testAsset, Quantity: "1"},
		},
		Block:       "bb",
		InlineDatum: &datum,
	}}
	node.addressTotalN = 7
	w := serve(
		t, node,
		"/api/v0/addresses/addr1/utxos/"+testAsset+"?count=1&page=3&order=desc",
	)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[{
"address":"addr1","tx_hash":"aa","tx_index":1,"output_index":1,
"amount":[{"unit":"lovelace","quantity":"2"},{"unit":"`+testAsset+`","quantity":"1"}],
"block":"bb","data_hash":null,"inline_datum":"d8799f00ff","reference_script_hash":null}]`,
		w.Body.String())
	require.Equal(t, "7", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "7", w.Header().Get("X-Pagination-Page-Total"))
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Equal(t, "addr1", node.lastAddress)
	require.Equal(t, testPolicy, node.lastAssetPolicy)
	require.Equal(t, []byte("nutcoin"), node.lastAssetName)
	require.Equal(
		t,
		PaginationParams{Count: 1, Page: 3, Order: "desc"},
		node.lastAddressPage,
	)
}

func TestHandleAddressAssetUTXOsRejectsBadInput(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, message string
	}{
		{"short asset", "/api/v0/addresses/a/utxos/abcd", "Invalid asset identifier."},
		{"non hex asset", "/api/v0/addresses/a/utxos/" + strings.Repeat("zz", 28), "Invalid asset identifier."},
		{"lovelace", "/api/v0/addresses/a/utxos/lovelace", "Invalid asset identifier."},
		{"bad count", "/api/v0/addresses/a/utxos/" + testAsset + "?count=0", "Invalid pagination parameters."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := serve(t, &mockNode{}, tc.path)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), tc.message)
		})
	}
	node := &mockNode{}
	node.addressErr = ErrInvalidAddress
	w := serve(t, node, "/api/v0/addresses/a/utxos/"+testAsset)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "Invalid address")
}
