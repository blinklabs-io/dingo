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
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var testMempoolHash = strings.Repeat("ab", 32)

func TestHandleMempool(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.mempoolHashes = []string{"aa", "bb"}
	node.mempoolTotal = 7
	w := serve(t, node, "/api/v0/mempool?count=2&page=2&order=desc")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[{"tx_hash":"aa"},{"tx_hash":"bb"}]`, w.Body.String())
	require.Equal(t, "7", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "4", w.Header().Get("X-Pagination-Page-Total"))
	node.mu.Lock()
	require.Equal(
		t,
		PaginationParams{Count: 2, Page: 2, Order: "desc"},
		node.lastMempool,
	)
	node.mu.Unlock()

	w = serve(t, &mockNode{}, "/api/v0/mempool")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[]`, w.Body.String())
	w = serve(t, &mockNode{}, "/api/v0/mempool?order=sideways")
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleMempoolAddress(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.mempoolHashes = []string{"aa"}
	node.mempoolTotal = 1
	w := serve(t, node, "/api/v0/mempool/addresses/addr1?count=5")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[{"tx_hash":"aa"}]`, w.Body.String())
	node.mu.Lock()
	require.Equal(t, "addr1", node.lastAddress)
	require.Equal(t, 5, node.lastMempool.Count)
	node.mu.Unlock()

	node = &mockNode{}
	node.mempoolErr = ErrInvalidAddress
	w = serve(t, node, "/api/v0/mempool/addresses/bad")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "Invalid address")
}

func TestHandleMempoolTransaction(t *testing.T) {
	t.Parallel()
	inline := "d87980"
	datumHash := "0b"
	address := "addr1in"
	node := &mockNode{}
	node.mempoolTx = MempoolTransactionInfo{
		Tx: TransactionInfo{
			Hash: testMempoolHash,
			OutputAmount: []AddressAmountInfo{
				{Unit: "lovelace", Quantity: "9"},
			},
			Fees:          "170000",
			Deposit:       "0",
			Size:          300,
			UtxoCount:     3,
			ValidContract: true,
		},
		Inputs: []TransactionInputInfo{
			{TxHash: "cc", OutputIndex: 1, Address: address},
			{TxHash: "dd", OutputIndex: 0, Collateral: true},
		},
		Outputs: []TransactionOutputInfo{{
			Address:     "addr1out",
			Amount:      []AddressAmountInfo{{Unit: "lovelace", Quantity: "9"}},
			OutputIndex: 0,
			DataHash:    &datumHash,
			InlineDatum: &inline,
		}},
		Redeemers: []MempoolRedeemerInfo{
			{TxIndex: 0, Purpose: "spend", UnitMem: "1", UnitSteps: "2"},
		},
	}
	w := serve(t, node, "/api/v0/mempool/"+testMempoolHash)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{
"tx":{"hash":"`+testMempoolHash+`","output_amount":[{"unit":"lovelace","quantity":"9"}],
 "fees":"170000","deposit":"0","size":300,"invalid_before":null,"invalid_hereafter":null,
 "utxo_count":3,"withdrawal_count":0,"mir_cert_count":0,"delegation_count":0,
 "stake_cert_count":0,"pool_update_count":0,"pool_retire_count":0,
 "asset_mint_or_burn_count":0,"redeemer_count":0,"valid_contract":true},
"inputs":[
 {"address":"addr1in","tx_hash":"cc","output_index":1,"collateral":false},
 {"tx_hash":"dd","output_index":0,"collateral":true}],
"outputs":[{"address":"addr1out","amount":[{"unit":"lovelace","quantity":"9"}],
 "output_index":0,"data_hash":"0b","inline_datum":"d87980","collateral":false,
 "reference_script_hash":null}],
"redeemers":[{"tx_index":0,"purpose":"spend","unit_mem":"1","unit_steps":"2"}]}`, w.Body.String())
	want, _ := hex.DecodeString(testMempoolHash)
	node.mu.Lock()
	require.Equal(t, want, node.lastMempoolTx)
	node.mu.Unlock()
}

func TestHandleMempoolErrors(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"/api/v0/mempool",
		"/api/v0/mempool/" + testMempoolHash,
		"/api/v0/mempool/addresses/addr1",
	} {
		node := &mockNode{}
		node.mempoolErr = ErrMempoolUnavailable
		w := serve(t, node, path)
		require.Equal(t, http.StatusServiceUnavailable, w.Code, path)

		node = &mockNode{}
		node.mempoolErr = errors.New("boom")
		w = serve(t, node, path)
		require.Equal(t, http.StatusInternalServerError, w.Code, path)
	}
	node := &mockNode{}
	node.mempoolErr = ErrTransactionNotFound
	w := serve(t, node, "/api/v0/mempool/"+testMempoolHash)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Contains(
		t,
		w.Body.String(),
		"The requested component has not been found.",
	)

	for _, hash := range []string{"abcd", strings.Repeat("z", 64), testScriptHash} {
		w := serve(t, &mockNode{}, "/api/v0/mempool/"+hash)
		require.Equal(t, http.StatusBadRequest, w.Code, hash)
		require.Contains(t, w.Body.String(), "Invalid transaction hash.")
	}
	w = serve(t, &mockNode{}, "/api/v0/mempool/addresses")
	require.Equal(
		t,
		http.StatusBadRequest,
		w.Code,
		"addresses is not a transaction hash",
	)
}
