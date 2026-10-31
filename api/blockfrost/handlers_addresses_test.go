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

// blockingAddressNode parks both summary routes until released or until the
// request context ends.
type blockingAddressNode struct {
	*mockNode
	entered chan struct{}
	release chan struct{}
}

func (n *blockingAddressNode) wait(ctx context.Context) error {
	n.entered <- struct{}{}
	select {
	case <-n.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *blockingAddressNode) Address(
	ctx context.Context,
	_ string,
) (AddressInfo, error) {
	return AddressInfo{Address: "addr1"}, n.wait(ctx)
}

func (n *blockingAddressNode) AddressExtended(
	ctx context.Context,
	_ string,
) (AddressExtendedInfo, error) {
	return AddressExtendedInfo{Address: "addr1"}, n.wait(ctx)
}

func (n *blockingAddressNode) AddressTransactions(
	ctx context.Context,
	_ string,
	_ TransactionRangeParams,
) ([]AddressTransactionInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

func newBlockingAddressNode() *blockingAddressNode {
	return &blockingAddressNode{
		mockNode: &mockNode{},
		entered:  make(chan struct{}, 8),
		release:  make(chan struct{}),
	}
}

func addressSummaryRequest(
	ctx context.Context,
	path string,
) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	req.RemoteAddr = "192.0.2.7:4000"
	return req
}

func TestHandleAddressPropagatesRequestContext(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"/api/v0/addresses/addr1",
		"/api/v0/addresses/addr1/extended",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			node := newBlockingAddressNode()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var releaseOnce sync.Once
			release := func() {
				releaseOnce.Do(func() { close(node.release) })
			}
			defer release()
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				newTestBlockfrost(node).handler().ServeHTTP(
					w, addressSummaryRequest(ctx, path),
				)
				done <- w
			}()
			testutil.RequireReceive(
				t, node.entered, 5*time.Second, "request reached node",
			)
			cancel()
			w := testutil.RequireReceive(
				t, done, 5*time.Second,
				"handler returns once the client cancels",
			)
			// A cancelled request has no reader, so nothing is written.
			require.Equal(t, 0, w.Body.Len())
		})
	}
}

func TestHandleAddressSummaryAdmissionCap(t *testing.T) {
	t.Parallel()
	node := newBlockingAddressNode()
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	serveAsync := func(path string) {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	// One base and one extended request fill the per-client cap, so the
	// cap is shared between the two routes.
	serveAsync("/api/v0/addresses/addr1")
	serveAsync("/api/v0/addresses/addr1/extended")
	for range 2 {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}

	for _, path := range []string{
		"/api/v0/addresses/addr1",
		"/api/v0/addresses/addr1/extended",
	} {
		// A rejected request returns at once; the deadline only keeps a
		// wrongly admitted one from parking the test.
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
		cancel()
		require.Equal(t, http.StatusTooManyRequests, w.Code, path)
		require.Equal(t, "1", w.Header().Get("Retry-After"))
	}

	// A different client is not affected by the first one's cap.
	other := addressSummaryRequest(
		context.Background(), "/api/v0/addresses/addr1",
	)
	other.RemoteAddr = "192.0.2.8:4000"
	go b.handler().ServeHTTP(httptest.NewRecorder(), other)
	testutil.RequireReceive(
		t, node.entered, 5*time.Second, "other client admitted",
	)

	release()
	wg.Wait()

	// The slots are free again once the requests finish.
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w, addressSummaryRequest(
			context.Background(), "/api/v0/addresses/addr1",
		),
	)
	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleAddressTransactionRoutesShareAdmission(t *testing.T) {
	t.Parallel()
	node := newBlockingAddressNode()
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	for _, path := range []string{
		"/api/v0/addresses/addr1/transactions",
		"/api/v0/addresses/addr1/txs",
	} {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	for range 2 {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}

	for _, path := range []string{
		"/api/v0/addresses/addr1",
		"/api/v0/addresses/addr1/transactions",
		"/api/v0/addresses/addr1/txs",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(
			w,
			addressSummaryRequest(ctx, path),
		)
		cancel()
		require.Equal(t, http.StatusTooManyRequests, w.Code, path)
		require.Equal(t, "1", w.Header().Get("Retry-After"), path)
	}

	// Invalid pagination is rejected before admission, preserving the route's
	// public validation behavior when the shared pool is full.
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(
		w,
		addressSummaryRequest(
			context.Background(),
			"/api/v0/addresses/addr1/transactions?count=0",
		),
	)
	require.Equal(t, http.StatusBadRequest, w.Code)

	release()
	wg.Wait()
}

func TestHandleAddressSummaryTooLarge(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"/api/v0/addresses/addr1",
		"/api/v0/addresses/addr1/extended",
	} {
		node := &mockNode{addressInfoErr: ErrAddressSummaryTooLarge}
		node.addressErr = ErrAddressSummaryTooLarge
		w := serve(t, node, path)
		require.Equal(t, http.StatusBadRequest, w.Code, path)
		require.Contains(t, w.Body.String(), "too many outputs or assets")
	}
}

// blockingAggregateNode parks the address and account aggregate routes until
// released or until the request context ends.
type blockingAggregateNode struct {
	*blockingAddressNode
}

func (n *blockingAggregateNode) AddressTotal(
	ctx context.Context,
	_ string,
) (AddressTotalInfo, error) {
	return AddressTotalInfo{Address: "addr1"}, n.wait(ctx)
}

func (n *blockingAggregateNode) AccountAddressTotal(
	ctx context.Context,
	_ string,
) (AccountAddressTotalInfo, error) {
	return AccountAddressTotalInfo{StakeAddress: "stake1abc"}, n.wait(ctx)
}

func (n *blockingAggregateNode) AccountAddressAssets(
	ctx context.Context,
	_ string,
	_ PaginationParams,
) ([]AddressAmountInfo, int, error) {
	return nil, 0, n.wait(ctx)
}

func TestHandleAggregateRoutesShareAdmission(t *testing.T) {
	t.Parallel()
	routes := []string{
		"/api/v0/addresses/addr1/total",
		"/api/v0/accounts/stake1abc/addresses/total",
		"/api/v0/accounts/stake1abc/addresses/assets",
	}
	node := &blockingAggregateNode{newBlockingAddressNode()}
	b := newTestBlockfrost(node)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(node.release) }) }
	defer release()

	var wg sync.WaitGroup
	// Two different aggregate routes fill the per-client cap.
	for _, path := range routes[:2] {
		wg.Go(func() {
			b.handler().ServeHTTP(
				httptest.NewRecorder(),
				addressSummaryRequest(context.Background(), path),
			)
		})
	}
	for range 2 {
		testutil.RequireReceive(
			t, node.entered, 5*time.Second, "request admitted",
		)
	}
	for _, path := range append(routes, "/api/v0/addresses/addr1") {
		// A rejected request returns at once; the deadline only keeps a
		// wrongly admitted one from parking the test.
		ctx, cancel := context.WithTimeout(
			context.Background(), 2*time.Second,
		)
		w := httptest.NewRecorder()
		b.handler().ServeHTTP(w, addressSummaryRequest(ctx, path))
		cancel()
		require.Equal(t, http.StatusTooManyRequests, w.Code, path)
		require.Equal(t, "1", w.Header().Get("Retry-After"), path)
	}
	release()
	wg.Wait()
}

func TestHandleAggregateRoutesAnswer400WhenOverBudget(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"/api/v0/addresses/addr1/total",
		"/api/v0/accounts/stake1abc/addresses/total",
		"/api/v0/accounts/stake1abc/addresses/assets",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			node := &mockNode{}
			node.addressErr = ErrAddressSummaryTooLarge
			node.accountTotal.err = ErrAddressSummaryTooLarge
			node.accountAssets.err = ErrAddressSummaryTooLarge
			w := serve(t, node, path)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "too many")
		})
	}
}
