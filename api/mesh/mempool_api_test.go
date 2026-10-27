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

package mesh

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/mempool"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func networkReq() NetworkRequest {
	return NetworkRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
	}
}

func TestMempool(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	deps.mempool.txs = []mempool.MempoolTransaction{
		{Hash: hexString(testHash(0xe1))},
		{Hash: hexString(testHash(0xe2))},
	}
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/mempool", networkReq())

	resp := decodeResponse[MempoolResponse](t, rec)
	require.Equal(
		t,
		[]*TransactionIdentifier{
			{Hash: hexString(testHash(0xe1))},
			{Hash: hexString(testHash(0xe2))},
		},
		resp.TransactionIdentifiers,
	)
}

// TestMempoolEmpty asserts an empty mempool serializes as an empty list
// rather than JSON null, which Mesh clients reject.
func TestMempoolEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t, newTestDeps())

	rec := postJSON(t, h, "/mempool", networkReq())

	resp := decodeResponse[MempoolResponse](t, rec)
	require.NotNil(t, resp.TransactionIdentifiers)
	require.Empty(t, resp.TransactionIdentifiers)
	require.Contains(
		t, rec.Body.String(), `"transaction_identifiers":[]`,
	)
}

func TestMempoolTransaction(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x0d), nil,
	)
	txCbor, tx := testSimpleSignedTx(t, addr)
	deps.mempool.txs = []mempool.MempoolTransaction{
		{
			Hash: tx.Hash().String(),
			Cbor: txCbor,
			Type: gledger.TxTypeConway,
		},
	}
	h := newTestHandler(t, deps)

	req := MempoolTransactionRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		TransactionIdentifier: &TransactionIdentifier{
			Hash: tx.Hash().String(),
		},
	}
	rec := postJSON(t, h, "/mempool/transaction", req)

	resp := decodeResponse[MempoolTransactionResponse](t, rec)
	require.Equal(
		t,
		tx.Hash().String(),
		resp.Transaction.TransactionIdentifier.Hash,
	)
	require.Len(t, resp.Transaction.Operations, 2)
	require.Equal(t, OpInput, resp.Transaction.Operations[0].Type)
	require.Equal(t, OpOutput, resp.Transaction.Operations[1].Type)
	require.Equal(
		t, addr, resp.Transaction.Operations[1].Account.Address,
	)
	require.Equal(
		t, "1000000", resp.Transaction.Operations[1].Amount.Value,
	)
}

func TestMempoolTransactionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t, newTestDeps())

	req := MempoolTransactionRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		TransactionIdentifier: &TransactionIdentifier{
			Hash: hexString(testHash(0xe3)),
		},
	}
	rec := postJSON(t, h, "/mempool/transaction", req)

	requireMeshError(
		t, rec, ErrTransactionNotFound, http.StatusInternalServerError,
	)
}

func TestMempoolTransactionMissingIdentifier(t *testing.T) {
	t.Parallel()

	for name, req := range map[string]MempoolTransactionRequest{
		"nil identifier": {
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
		},
		"empty hash": {
			networkIdentifierField: networkIdentifierField{
				NetworkIdentifier: testNetworkID(),
			},
			TransactionIdentifier: &TransactionIdentifier{},
		},
	} {
		t.Run(name, func(t *testing.T) {
			h := newTestHandler(t, newTestDeps())

			rec := postJSON(t, h, "/mempool/transaction", req)

			requireMeshError(
				t, rec, ErrInvalidRequest,
				http.StatusInternalServerError,
			)
		})
	}
}

// TestMempoolTransactionUndecodable covers a mempool entry whose CBOR
// cannot be parsed: the endpoint must report an internal error rather
// than panicking or returning an empty transaction.
func TestMempoolTransactionUndecodable(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	hash := hexString(testHash(0xe4))
	deps.mempool.txs = []mempool.MempoolTransaction{
		{
			Hash: hash,
			Cbor: []byte{0xff, 0xff, 0xff},
			Type: gledger.TxTypeConway,
		},
	}
	h := newTestHandler(t, deps)

	req := MempoolTransactionRequest{
		networkIdentifierField: networkIdentifierField{
			NetworkIdentifier: testNetworkID(),
		},
		TransactionIdentifier: &TransactionIdentifier{Hash: hash},
	}
	rec := postJSON(t, h, "/mempool/transaction", req)

	requireMeshError(
		t, rec, ErrInternal, http.StatusInternalServerError,
	)
}

// TestMempoolListingIsBounded asserts the handler asks the mempool for at
// most the configured number of identifiers, and that a pool which fits the
// bound exactly is listed in full.
func TestMempoolListingIsBounded(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	for i := range 3 {
		deps.mempool.txs = append(
			deps.mempool.txs,
			mempool.MempoolTransaction{Hash: hexString(testHash(byte(i + 1)))},
		)
	}
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.maxMempoolListing = 3
	})

	rec := postJSON(t, h, "/mempool", networkReq())

	resp := decodeResponse[MempoolResponse](t, rec)
	require.Len(t, resp.TransactionIdentifiers, 3)
	require.Equal(
		t, hexString(testHash(1)), resp.TransactionIdentifiers[0].Hash,
	)
	require.Equal(t, []int{3}, deps.mempool.listCalls)
}

// TestMempoolListingRejectsOversizedPool asserts a pool larger than the bound
// is refused with a retriable error rather than answered with a prefix: the
// Mesh response must name every identifier and has no cursor, so a prefix
// would silently hide transactions that are still pending.
func TestMempoolListingRejectsOversizedPool(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	for i := range 5 {
		deps.mempool.txs = append(
			deps.mempool.txs,
			mempool.MempoolTransaction{Hash: hexString(testHash(byte(i + 1)))},
		)
	}
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.maxMempoolListing = 3
	})

	rec := postJSON(t, h, "/mempool", networkReq())

	got := requireMeshError(
		t, rec, ErrUnavailable, http.StatusInternalServerError,
	)
	require.Contains(t, got.Details["error"], "5 transactions")
	require.Equal(t, []int{3}, deps.mempool.listCalls)
}

// TestMempoolListingDefaultBound asserts a server built without an explicit
// bound still passes one to the mempool.
func TestMempoolListingDefaultBound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	h := newTestHandler(t, deps)

	rec := postJSON(t, h, "/mempool", networkReq())

	decodeResponse[MempoolResponse](t, rec)
	require.Equal(t, []int{defaultMaxMempoolListing}, deps.mempool.listCalls)
}

// TestMempoolConcurrencyBudget parks one listing inside the mempool and
// asserts a second request is refused with the retriable Mesh error, then
// that the slot is returned once the first request finishes.
func TestMempoolConcurrencyBudget(t *testing.T) {
	t.Parallel()

	deps := newTestDeps()
	listEntered := make(chan struct{})
	deps.mempool.listEntered = listEntered
	listRelease := make(chan struct{})
	deps.mempool.listRelease = listRelease
	h := newTestHandler(t, deps, func(c *ServerConfig) {
		c.maxConcurrentMempoolListings = 1
	})

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(listRelease) })
	}
	defer release()

	body, err := json.Marshal(networkReq())
	require.NoError(t, err)
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(
			http.MethodPost, "/mempool", bytes.NewReader(body),
		)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		first <- rec
	}()
	testutil.RequireReceive(
		t, listEntered, 10*time.Second,
		"first listing reaches the mempool",
	)

	rec := postJSON(t, h, "/mempool", networkReq())
	got := requireMeshError(
		t, rec, ErrUnavailable, http.StatusInternalServerError,
	)
	require.True(t, got.Retriable)

	release()
	firstRec := testutil.RequireReceive(
		t, first, 10*time.Second, "first listing completes",
	)
	require.Equal(t, http.StatusOK, firstRec.Code)

	// The slot is free again.
	decodeResponse[MempoolResponse](
		t, postJSON(t, h, "/mempool", networkReq()),
	)
}
