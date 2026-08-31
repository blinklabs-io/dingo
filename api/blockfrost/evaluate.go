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

package blockfrost

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
)

const (
	// maxTxPayloadSize bounds the evaluation request bodies. Both endpoints
	// carry the transaction encoded as base16 or base64 rather than as raw
	// bytes, and the /utxos variant wraps it in JSON, so the encoded body is
	// larger than the transaction it carries.
	maxTxPayloadSize = 4 * maxTxBodySize

	// ogmiosEvaluateVersion is the Ogmios major version whose response
	// format these endpoints serve. Blockfrost defaults its `version` query
	// parameter to the same value.
	ogmiosEvaluateVersion = "5"
)

// errEmptyTransactionPayload reports an evaluation request that carried no
// transaction.
var errEmptyTransactionPayload = errors.New("empty transaction payload")

// transactionEvaluateUtxosRequest is the JSON body of
// POST /api/v0/utils/txs/evaluate/utxos.
type transactionEvaluateUtxosRequest struct {
	Cbor              string            `json:"cbor"`
	AdditionalUtxoSet []json.RawMessage `json:"additionalUtxoSet"`
}

// TransactionEvaluationEnvelope is the response body both evaluation
// endpoints return: the Ogmios v5 EvaluateTx response that Blockfrost passes
// through verbatim. Off-chain SDKs read the execution units from
// result.EvaluationResult, so the envelope is part of the wire contract
// rather than decoration.
type TransactionEvaluationEnvelope struct {
	Type        string                      `json:"type"`
	Version     string                      `json:"version"`
	ServiceName string                      `json:"servicename"`
	MethodName  string                      `json:"methodname"`
	Result      TransactionEvaluationResult `json:"result"`
	Reflection  any                         `json:"reflection"`
}

// TransactionEvaluationResult holds the execution units of a successful
// evaluation under the key Ogmios uses for them.
type TransactionEvaluationResult struct {
	EvaluationResult TransactionEvaluationResponse `json:"EvaluationResult"`
}

// newTransactionEvaluationEnvelope wraps an execution-unit map in the Ogmios
// v5 response envelope.
func newTransactionEvaluationEnvelope(
	result TransactionEvaluationResponse,
) TransactionEvaluationEnvelope {
	return TransactionEvaluationEnvelope{
		Type:        "jsonwsp/response",
		Version:     "1.0",
		ServiceName: "ogmios",
		MethodName:  "EvaluateTx",
		Result: TransactionEvaluationResult{
			EvaluationResult: result,
		},
		Reflection: nil,
	}
}

// decodeTransactionPayload decodes the transaction an evaluation request
// carries. Blockfrost documents both evaluation endpoints as taking the
// transaction CBOR encoded in base16 or base64, and that is what off-chain
// SDKs send. Raw CBOR bytes are also accepted, because Dingo's first release
// of POST /api/v0/utils/txs/evaluate took only that form.
//
// A transaction always begins with a CBOR array header (0x80-0x9f), which is
// outside printable ASCII, so a raw body can never be mistaken for one of the
// text encodings.
func decodeTransactionPayload(payload []byte) ([]byte, error) {
	trimmed := strings.TrimSpace(string(payload))
	if trimmed == "" {
		return nil, errEmptyTransactionPayload
	}
	if !isPrintableASCII(trimmed) {
		return payload, nil
	}
	if decoded, err := hex.DecodeString(trimmed); err == nil {
		return decoded, nil
	}
	for _, encoding := range []*base64.Encoding{
		base64.StdEncoding,
		base64.RawStdEncoding,
		base64.URLEncoding,
		base64.RawURLEncoding,
	} {
		if decoded, err := encoding.DecodeString(trimmed); err == nil {
			return decoded, nil
		}
	}
	return payload, nil
}

// isPrintableASCII reports whether every byte of s is printable ASCII, which
// every base16 and base64 encoding of a transaction is.
func isPrintableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < 0x20 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// handleTransactionEvaluate handles POST /api/v0/utils/txs/evaluate and
// returns the execution units each redeemer in the transaction consumes.
func (b *Blockfrost) handleTransactionEvaluate(
	w http.ResponseWriter,
	r *http.Request,
) {
	if !requireContentType(w, r, "application/cbor") {
		return
	}
	if !requireOgmiosVersion(w, r) {
		return
	}
	payload, ok := readEvaluationBody(w, r)
	if !ok {
		return
	}
	b.evaluateTransaction(w, payload)
}

// handleTransactionEvaluateUtxos handles
// POST /api/v0/utils/txs/evaluate/utxos, the JSON form of the evaluation
// endpoint that off-chain SDKs use when they carry an additional UTxO set.
func (b *Blockfrost) handleTransactionEvaluateUtxos(
	w http.ResponseWriter,
	r *http.Request,
) {
	if !requireContentType(w, r, "application/json") {
		return
	}
	if !requireOgmiosVersion(w, r) {
		return
	}
	body, ok := readEvaluationBody(w, r)
	if !ok {
		return
	}
	var req transactionEvaluateUtxosRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"Invalid request body.",
		)
		return
	}
	// Evaluation resolves inputs from the ledger's own UTxO set, so a
	// caller-supplied set would silently not be honored. Rejecting it keeps
	// the execution units the endpoint returns truthful.
	if len(req.AdditionalUtxoSet) > 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"additionalUtxoSet is not supported.",
		)
		return
	}
	b.evaluateTransaction(w, []byte(req.Cbor))
}

// evaluateTransaction decodes a transaction payload, evaluates it, and writes
// the Ogmios-format response shared by both evaluation endpoints.
func (b *Blockfrost) evaluateTransaction(
	w http.ResponseWriter,
	payload []byte,
) {
	txCbor, err := decodeTransactionPayload(payload)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"transaction body is empty",
		)
		return
	}
	result, err := b.node.TransactionEvaluate(txCbor)
	if err != nil {
		if errors.Is(err, ErrInvalidTransaction) {
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"Invalid transaction CBOR.",
			)
			return
		}
		// A transaction that decoded but could not be evaluated is a
		// different failure, and reporting it as malformed CBOR sends
		// callers looking in the wrong place. Log the cause: it is the
		// only record of why evaluation failed, and the response body
		// deliberately does not leak ledger internals.
		if errors.Is(err, ErrTransactionEvaluation) {
			b.logger.Error(
				"failed to evaluate transaction",
				"error", err,
			)
			writeError(
				w,
				http.StatusBadRequest,
				"Bad Request",
				"Transaction could not be evaluated.",
			)
			return
		}
		b.logger.Error("failed to evaluate transaction", "error", err)
		writeError(
			w,
			http.StatusInternalServerError,
			"Internal Server Error",
			"failed to evaluate transaction",
		)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionEvaluationEnvelope(result))
}

// requireContentType enforces the media type an endpoint accepts, writing the
// error response itself when the request carries anything else.
func requireContentType(
	w http.ResponseWriter,
	r *http.Request,
	want string,
) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != want {
		writeError(
			w,
			http.StatusUnsupportedMediaType,
			"Unsupported Media Type",
			"Content-Type must be "+want+".",
		)
		return false
	}
	return true
}

// requireOgmiosVersion enforces the `version` query parameter Blockfrost
// accepts on the evaluation endpoints. Only the default response format is
// served, so any other version is refused rather than answered in a format
// the caller did not ask for.
func requireOgmiosVersion(w http.ResponseWriter, r *http.Request) bool {
	version := r.URL.Query().Get("version")
	if version == "" || version == ogmiosEvaluateVersion {
		return true
	}
	writeError(
		w,
		http.StatusBadRequest,
		"Bad Request",
		"Only Ogmios version "+ogmiosEvaluateVersion+" responses are supported.",
	)
	return false
}

// readEvaluationBody reads a size-limited evaluation request body, writing
// the error response itself when the body cannot be used.
func readEvaluationBody(
	w http.ResponseWriter,
	r *http.Request,
) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxTxPayloadSize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			writeError(
				w,
				http.StatusRequestEntityTooLarge,
				"Request Entity Too Large",
				"transaction body exceeds maximum allowed size",
			)
			return nil, false
		}
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"failed to read transaction body",
		)
		return nil, false
	}
	if len(body) == 0 {
		writeError(
			w,
			http.StatusBadRequest,
			"Bad Request",
			"transaction body is empty",
		)
		return nil, false
	}
	return body, true
}
