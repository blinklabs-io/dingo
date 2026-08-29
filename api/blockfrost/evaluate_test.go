// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package blockfrost

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawEvaluateTxCbor stands in for a serialized transaction. Like every real
// one it starts with a CBOR array header, which is what keeps a raw body
// distinguishable from its base16/base64 encodings.
var rawEvaluateTxCbor = []byte{0x84, 0xa0, 0x00, 0xff}

func evaluateTestNode() *mockNode {
	return &mockNode{
		transactionEvaluation: TransactionEvaluationResponse{
			"spend:0": {Memory: 1700, Steps: 476468},
			"mint:0":  {Memory: 42, Steps: 99},
		},
	}
}

func postEvaluate(
	t *testing.T,
	node *mockNode,
	target string,
	contentType string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	b := newTestBlockfrost(node)
	req := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, req)
	return w
}

// requireEvaluationEnvelope decodes the Ogmios-format response Blockfrost
// clients read execution units from.
func requireEvaluationEnvelope(
	t *testing.T,
	w *httptest.ResponseRecorder,
) TransactionEvaluationEnvelope {
	t.Helper()
	var got TransactionEvaluationEnvelope
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	return got
}

// TestHandleTransactionEvaluateReturnsOgmiosEnvelope pins the response shape
// off-chain SDKs parse: they read result.EvaluationResult, not a bare map.
func TestHandleTransactionEvaluateReturnsOgmiosEnvelope(t *testing.T) {
	node := evaluateTestNode()
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate",
		"application/cbor",
		hex.EncodeToString(rawEvaluateTxCbor),
	)

	assert.Equal(t, http.StatusOK, w.Code)
	got := requireEvaluationEnvelope(t, w)
	assert.Equal(t, "jsonwsp/response", got.Type)
	assert.Equal(t, "ogmios", got.ServiceName)
	assert.Equal(t, "EvaluateTx", got.MethodName)
	assert.Equal(t, TransactionEvaluationResponse{
		"spend:0": {Memory: 1700, Steps: 476468},
		"mint:0":  {Memory: 42, Steps: 99},
	}, got.Result.EvaluationResult)
}

// TestHandleTransactionEvaluateAcceptsEncodedPayloads covers the encodings
// Blockfrost documents for this endpoint (base16 and base64) alongside the raw
// CBOR bytes Dingo accepted first. Each must reach the ledger as the same
// transaction.
func TestHandleTransactionEvaluateAcceptsEncodedPayloads(t *testing.T) {
	for name, body := range map[string]string{
		"base16": hex.EncodeToString(rawEvaluateTxCbor),
		"base16 upper": strings.ToUpper(
			hex.EncodeToString(rawEvaluateTxCbor),
		),
		"base64": base64.StdEncoding.EncodeToString(rawEvaluateTxCbor),
		"base64 raw": base64.RawStdEncoding.EncodeToString(
			rawEvaluateTxCbor,
		),
		"raw cbor": string(rawEvaluateTxCbor),
	} {
		t.Run(name, func(t *testing.T) {
			node := evaluateTestNode()
			w := postEvaluate(
				t,
				node,
				"/api/v0/utils/txs/evaluate",
				"application/cbor",
				body,
			)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, rawEvaluateTxCbor, node.transactionEvaluateCbor)
		})
	}
}

func TestHandleTransactionEvaluateRejectsInvalidTransaction(t *testing.T) {
	node := &mockNode{transactionEvaluationErr: ErrInvalidTransaction}
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate",
		"application/cbor",
		hex.EncodeToString(rawEvaluateTxCbor),
	)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var got ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "Invalid transaction CBOR.", got.Message)
}

func TestHandleTransactionEvaluateRequiresCBOR(t *testing.T) {
	w := postEvaluate(
		t,
		&mockNode{},
		"/api/v0/utils/txs/evaluate",
		"application/json",
		"{}",
	)

	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}

func TestHandleTransactionEvaluateRejectsEmptyBody(t *testing.T) {
	w := postEvaluate(
		t,
		&mockNode{},
		"/api/v0/utils/txs/evaluate",
		"application/cbor",
		"",
	)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleTransactionEvaluateRejectsOtherOgmiosVersion keeps the endpoint
// from answering a caller that asked for a response format it does not serve.
func TestHandleTransactionEvaluateRejectsOtherOgmiosVersion(t *testing.T) {
	node := evaluateTestNode()
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate?version=6",
		"application/cbor",
		hex.EncodeToString(rawEvaluateTxCbor),
	)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, node.transactionEvaluateCbor)
}

func TestHandleTransactionEvaluateAcceptsDefaultOgmiosVersion(t *testing.T) {
	w := postEvaluate(
		t,
		evaluateTestNode(),
		"/api/v0/utils/txs/evaluate?version=5",
		"application/cbor",
		hex.EncodeToString(rawEvaluateTxCbor),
	)

	assert.Equal(t, http.StatusOK, w.Code)
}

// TestHandleTransactionEvaluateUtxos covers the JSON form of the endpoint,
// which is the one MeshJS's Blockfrost provider calls.
func TestHandleTransactionEvaluateUtxos(t *testing.T) {
	node := evaluateTestNode()
	body, err := json.Marshal(map[string]any{
		"cbor":              hex.EncodeToString(rawEvaluateTxCbor),
		"additionalUtxoSet": []any{},
	})
	require.NoError(t, err)
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate/utxos",
		"application/json",
		string(body),
	)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, rawEvaluateTxCbor, node.transactionEvaluateCbor)
	got := requireEvaluationEnvelope(t, w)
	assert.Equal(t, TransactionEvaluationResponse{
		"spend:0": {Memory: 1700, Steps: 476468},
		"mint:0":  {Memory: 42, Steps: 99},
	}, got.Result.EvaluationResult)
}

// TestHandleTransactionEvaluateUtxosRejectsAdditionalUtxoSet refuses a set the
// ledger evaluator cannot honor, rather than returning execution units
// computed without it.
func TestHandleTransactionEvaluateUtxosRejectsAdditionalUtxoSet(t *testing.T) {
	node := evaluateTestNode()
	body, err := json.Marshal(map[string]any{
		"cbor": hex.EncodeToString(rawEvaluateTxCbor),
		"additionalUtxoSet": []any{
			[]any{
				map[string]any{"txId": "00", "index": 0},
				map[string]any{
					"address": "addr_test1",
					"value":   map[string]any{"coins": 1000000},
				},
			},
		},
	})
	require.NoError(t, err)
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate/utxos",
		"application/json",
		string(body),
	)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, node.transactionEvaluateCbor)
	var got ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "additionalUtxoSet is not supported.", got.Message)
}

func TestHandleTransactionEvaluateUtxosRequiresJSON(t *testing.T) {
	w := postEvaluate(
		t,
		&mockNode{},
		"/api/v0/utils/txs/evaluate/utxos",
		"application/cbor",
		hex.EncodeToString(rawEvaluateTxCbor),
	)

	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}

func TestHandleTransactionEvaluateUtxosRejectsInvalidBody(t *testing.T) {
	w := postEvaluate(
		t,
		&mockNode{},
		"/api/v0/utils/txs/evaluate/utxos",
		"application/json",
		"not json",
	)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleTransactionEvaluateUtxosRejectsEmptyCbor(t *testing.T) {
	node := evaluateTestNode()
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate/utxos",
		"application/json",
		`{"cbor":""}`,
	)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Nil(t, node.transactionEvaluateCbor)
}

func TestDecodeTransactionPayload(t *testing.T) {
	for name, tc := range map[string]struct {
		payload []byte
		want    []byte
		wantErr bool
	}{
		"base16": {
			payload: []byte(hex.EncodeToString(rawEvaluateTxCbor)),
			want:    rawEvaluateTxCbor,
		},
		"base16 surrounded by whitespace": {
			payload: []byte(
				"\n" + hex.EncodeToString(rawEvaluateTxCbor) + "\n",
			),
			want: rawEvaluateTxCbor,
		},
		"base64": {
			payload: []byte(
				base64.StdEncoding.EncodeToString(rawEvaluateTxCbor),
			),
			want: rawEvaluateTxCbor,
		},
		"raw cbor": {
			payload: rawEvaluateTxCbor,
			want:    rawEvaluateTxCbor,
		},
		"empty": {
			payload: []byte("   "),
			wantErr: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeTransactionPayload(tc.payload)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
