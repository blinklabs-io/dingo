// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package blockfrost

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

// TestHandleTransactionSubmitRejectedIsNotReportedAsMalformed covers a
// transaction the mempool declines. Both that and a genuinely undecodable body
// used to be wrapped in ErrInvalidTransaction, so a well-formed transaction
// rejected for, say, a script data hash mismatch came back as
// "Invalid transaction CBOR." with nothing logged. That sends the caller to
// inspect their serialization instead of the rejection, which is the one thing
// the response could have told them.
func TestHandleTransactionSubmitRejectedIsNotReportedAsMalformed(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionSubmitErr: fmt.Errorf(
			"%w: validate transaction: script data hash mismatch",
			ErrTransactionRejected,
		),
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		strings.NewReader("\x84\x00"),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Bad Request", resp.Error)
	assert.NotEqual(
		t,
		"Invalid transaction CBOR.",
		resp.Message,
		"a rejected transaction is not malformed CBOR",
	)
	assert.Equal(
		t,
		"Transaction rejected: validate transaction: script data hash mismatch",
		resp.Message,
		"the rejection reason is what the caller needs",
	)
}

// TestHandleTransactionSubmitStillReportsMalformedCbor pins the other side of
// that split: a body that genuinely cannot be decoded keeps its message.
func TestHandleTransactionSubmitStillReportsMalformedCbor(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionSubmitErr: fmt.Errorf(
			"%w: determine transaction type",
			ErrInvalidTransaction,
		),
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		strings.NewReader("\x84\x00"),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Invalid transaction CBOR.", resp.Message)
}

// TestHandleTransactionEvaluateFailureIsNotReportedAsMalformed is the same
// split on the evaluation endpoints: a transaction that decoded but could not
// be evaluated is not malformed CBOR.
func TestHandleTransactionEvaluateFailureIsNotReportedAsMalformed(t *testing.T) {
	node := evaluateTestNode()
	node.transactionEvaluationErr = fmt.Errorf(
		"%w: resolve inputs",
		ErrTransactionEvaluation,
	)
	b := newTestBlockfrost(node)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/utils/txs/evaluate",
		strings.NewReader("\x84\x00"),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionEvaluate(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Transaction could not be evaluated.", resp.Message)
}

// stubEvaluator stands in for the ledger at the evaluation boundary, so a
// single EvaluateTx result can be classified in isolation.
type stubEvaluator struct {
	exUnits map[lcommon.RedeemerKey]lcommon.ExUnits
	err     error
	calls   int
}

func (s *stubEvaluator) EvaluateTx(tx lcommon.Transaction) (
	uint64,
	lcommon.ExUnits,
	map[lcommon.RedeemerKey]lcommon.ExUnits,
	error,
) {
	s.calls++
	return 0, lcommon.ExUnits{}, s.exUnits, s.err
}

// TestTransactionEvaluateStorageFailureIsNotAnEvaluationFailure pins that a
// node that cannot read its own UTxO set does not report the caller's
// transaction as unevaluable. Labelled ErrTransactionEvaluation it answers
// 400, which tells the caller to change a transaction that was never
// evaluated and hides the outage from anything deciding whether to retry.
func TestTransactionEvaluateStorageFailureIsNotAnEvaluationFailure(
	t *testing.T,
) {
	for name, cause := range map[string]error{
		"blob store unavailable": dbtypes.ErrBlobStoreUnavailable,
		"utxo cbor unavailable":  database.ErrUtxoCborUnavailable,
	} {
		t.Run(name, func(t *testing.T) {
			evaluator := &stubEvaluator{
				err: fmt.Errorf(
					"TX abcd failed evaluation: %w",
					cause,
				),
			}
			adapter := &NodeAdapter{evaluator: evaluator}

			result, err := adapter.TransactionEvaluate(submitTestTxCbor(t))

			require.Error(t, err)
			assert.Nil(t, result)
			assert.Equal(t, 1, evaluator.calls)
			assert.ErrorIs(t, err, ErrLedgerUnavailable)
			assert.NotErrorIs(
				t,
				err,
				ErrTransactionEvaluation,
				"the transaction was never evaluated",
			)
			assert.ErrorIs(t, err, cause, "the cause is preserved")
		})
	}
}

// TestTransactionEvaluateScriptFailureStaysAnEvaluationFailure is the control
// for that split. Without it, mapping every EvaluateTx error to
// ErrLedgerUnavailable would satisfy the test above.
func TestTransactionEvaluateScriptFailureStaysAnEvaluationFailure(
	t *testing.T,
) {
	evalErr := errors.New(
		"TX abcd failed evaluation: the machine terminated part way " +
			"through evaluation due to overspending the budget",
	)
	evaluator := &stubEvaluator{err: evalErr}
	adapter := &NodeAdapter{evaluator: evaluator}

	_, err := adapter.TransactionEvaluate(submitTestTxCbor(t))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransactionEvaluation)
	assert.NotErrorIs(t, err, ErrLedgerUnavailable)
	assert.ErrorIs(t, err, evalErr)
}

// TestTransactionEvaluateReturnsExecutionUnits pins that the classification
// does not swallow the success path, and that the evaluator seam is the one
// the execution units come from.
func TestTransactionEvaluateReturnsExecutionUnits(t *testing.T) {
	evaluator := &stubEvaluator{
		exUnits: map[lcommon.RedeemerKey]lcommon.ExUnits{
			{Tag: lcommon.RedeemerTagSpend, Index: 0}: {
				Memory: 1700,
				Steps:  476468,
			},
		},
	}
	adapter := &NodeAdapter{evaluator: evaluator}

	result, err := adapter.TransactionEvaluate(submitTestTxCbor(t))

	require.NoError(t, err)
	assert.Equal(t, 1, evaluator.calls)
	assert.Equal(t, TransactionEvaluationResponse{
		"spend:0": {Memory: 1700, Steps: 476468},
	}, result)
}

// TestTransactionEvaluateWithoutEvaluatorIsUnavailable covers the adapter
// built without a ledger, the evaluation counterpart of the nil submitter.
func TestTransactionEvaluateWithoutEvaluatorIsUnavailable(t *testing.T) {
	adapter := &NodeAdapter{}

	_, err := adapter.TransactionEvaluate(submitTestTxCbor(t))

	require.ErrorIs(t, err, ErrLedgerUnavailable)
}

// TestNewNodeAdapterWiresEvaluator pins that the production constructor fills
// the seam, so the branch above is not reachable from a real node.
func TestNewNodeAdapterWiresEvaluator(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	assert.NotNil(t, adapter.evaluator)
}

// TestHandleTransactionEvaluateStorageFailureReturns503 carries the
// classification through the HTTP layer.
func TestHandleTransactionEvaluateStorageFailureReturns503(t *testing.T) {
	node := evaluateTestNode()
	node.transactionEvaluationErr = fmt.Errorf(
		"%w: %w",
		ErrLedgerUnavailable,
		dbtypes.ErrBlobStoreUnavailable,
	)
	w := postEvaluate(
		t,
		node,
		"/api/v0/utils/txs/evaluate",
		"application/cbor",
		hex.EncodeToString(rawEvaluateTxCbor),
	)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Service Unavailable", resp.Error)
	assert.Equal(t, "ledger state unavailable", resp.Message)
}
