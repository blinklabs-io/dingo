// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package blockfrost

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleTransactionEvaluate(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionEvaluation: TransactionEvaluationResponse{
			"spend:0": {Memory: 1700, Steps: 476468},
			"mint:0":  {Memory: 42, Steps: 99},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v0/utils/txs/evaluate", strings.NewReader("\x84\x00"))
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	var got TransactionEvaluationResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, TransactionEvaluationResponse{
		"spend:0": {Memory: 1700, Steps: 476468},
		"mint:0":  {Memory: 42, Steps: 99},
	}, got)
}

func TestHandleTransactionEvaluateRejectsInvalidTransaction(t *testing.T) {
	b := newTestBlockfrost(&mockNode{transactionEvaluationErr: ErrInvalidTransaction})
	req := httptest.NewRequest(http.MethodPost, "/api/v0/utils/txs/evaluate", strings.NewReader("\x84\x00"))
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var got ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&got))
	assert.Equal(t, "Invalid transaction CBOR.", got.Message)
}

func TestHandleTransactionEvaluateRequiresCBOR(t *testing.T) {
	b := newTestBlockfrost(&mockNode{})
	req := httptest.NewRequest(http.MethodPost, "/api/v0/utils/txs/evaluate", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	b.handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnsupportedMediaType, w.Code)
}
