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

package mesh

import (
	"net/http"
	"testing"

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
		t, rec, ErrTransactionNotFound, http.StatusNotFound,
	)
}

func TestMempoolTransactionMissingIdentifier(t *testing.T) {
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
				http.StatusBadRequest,
			)
		})
	}
}

// TestMempoolTransactionUndecodable covers a mempool entry whose CBOR
// cannot be parsed: the endpoint must report an internal error rather
// than panicking or returning an empty transaction.
func TestMempoolTransactionUndecodable(t *testing.T) {
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
