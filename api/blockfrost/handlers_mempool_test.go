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
	"context"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
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

type blockingMempoolAddressNode struct {
	*mockNode
	entered chan struct{}
	release chan struct{}
}

func (n *blockingMempoolAddressNode) MempoolAddressTransactions(
	ctx context.Context,
	_ string,
	_ PaginationParams,
) ([]string, int, error) {
	n.entered <- struct{}{}
	select {
	case <-n.release:
		return []string{}, 0, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func TestHandleMempoolAddressAdmissionCap(t *testing.T) {
	t.Parallel()
	node := &blockingMempoolAddressNode{
		mockNode: &mockNode{},
		entered:  make(chan struct{}, 8),
		release:  make(chan struct{}),
	}
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()
	const path = "/api/v0/mempool/addresses/addr1"

	var wg sync.WaitGroup
	wg.Go(func() {
		b.handler().ServeHTTP(
			httptest.NewRecorder(),
			addressSummaryRequest(context.Background(), path),
		)
	})
	testutil.RequireReceive(t, node.entered, 5*time.Second, "admitted")

	// The deadline only keeps a wrongly admitted request from parking the
	// test; a rejected one returns at once.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
	require.Equal(t, http.StatusTooManyRequests, w.Code)
	require.Equal(t, "1", w.Header().Get("Retry-After"))

	release()
	wg.Wait()
	w = httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(context.Background(), path),
	)
	require.Equal(t, http.StatusOK, w.Code)
}
