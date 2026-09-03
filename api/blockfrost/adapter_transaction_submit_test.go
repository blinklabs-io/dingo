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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// submitTxHex is a signed transaction that decodes cleanly. TransactionSubmit
// decodes before it reaches the submitter, so classifying what the submitter
// returns requires a body that survives that decode.
const submitTxHex = "84a700818258200c07395aed88bdddc6de0518d1462dd0ec7e52e1" +
	"e3a53599f7cdb24dc80237f8010181a20058390073a817bb425cbe179af824529d96ce" +
	"b93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebeef022245944e391" +
	"4f5bcca3a793011a02dc6c00021a001e84800b5820192d0c0c2c2320e843e080b5f91a" +
	"9ca35155bc50f3ef3bfdbc72c1711b86367e0d818258203af629a5cd75f76d0cc21172" +
	"e1193b85f199ca78e837c3965d77d7d6bc90206b0010a20058390073a817bb425cbe17" +
	"9af824529d96ceb93c41c3ab507380095d1be4ebd64c93ef0094f5c179e5380109ebee" +
	"f022245944e3914f5bcca3a793011a006acfc0111a002dc6c0a40081825820" +
	"25fcacade3fffc096b53bdaf4c7d012bded303c9edbee686d24b372dae60aa1b58409d" +
	"a928a064ff9f795110bdcb8ab05d2a7a023dd15ebc42044f102ce366c0c9077024c795" +
	"1c2d63584b7d2eea7bf1da4a7453bde4c99dd083889c1e2e2e3db804048119077a0581" +
	"840000187b820a0a06814746010000222601f4f6"

func submitTestTxCbor(t *testing.T) []byte {
	t.Helper()
	txCbor, err := hex.DecodeString(submitTxHex)
	require.NoError(t, err)
	return txCbor
}

// stubSubmitter stands in for the mempool at the TransactionSubmitter
// boundary, so a single AddTransaction result can be classified in isolation.
type stubSubmitter struct {
	err   error
	calls int
}

func (s *stubSubmitter) AddTransaction(txType uint, txBytes []byte) error {
	s.calls++
	return s.err
}

// TestTransactionSubmitUnavailableMempoolIsNotARejection pins that the two
// conditions meaning "the mempool cannot accept anything right now" are
// reported as ErrMempoolUnavailable, the same class the nil-submitter branch
// reports, rather than as a rejection of the caller's transaction. Both are
// members: a mempool stopped by shutdown, and one built without a validator.
func TestTransactionSubmitUnavailableMempoolIsNotARejection(t *testing.T) {
	for name, submitErr := range map[string]error{
		"stopped": mempool.ErrMempoolStopped,
		"stopped wrapped": fmt.Errorf(
			"submit: %w",
			mempool.ErrMempoolStopped,
		),
		"nil validator": fmt.Errorf(
			"%w in AddTransaction",
			mempool.ErrNilValidator,
		),
	} {
		t.Run(name, func(t *testing.T) {
			submitter := &stubSubmitter{err: submitErr}
			adapter := &NodeAdapter{submitter: submitter}

			hash, err := adapter.TransactionSubmit(submitTestTxCbor(t))

			require.Error(t, err)
			assert.Empty(t, hash)
			assert.Equal(t, 1, submitter.calls)
			assert.ErrorIs(
				t,
				err,
				ErrMempoolUnavailable,
				"an unusable mempool is the same condition the nil "+
					"submitter reports",
			)
			assert.NotErrorIs(
				t,
				err,
				ErrTransactionRejected,
				"the transaction was never judged, so it was not rejected",
			)
			assert.ErrorIs(t, err, submitErr, "the cause is preserved")
		})
	}
}

// TestTransactionSubmitRejectionStaysARejection is the other half of the
// split. Without it, mapping every AddTransaction error to
// ErrMempoolUnavailable would satisfy the test above.
func TestTransactionSubmitRejectionStaysARejection(t *testing.T) {
	submitErr := errors.New(
		"validate transaction: script data hash mismatch",
	)
	submitter := &stubSubmitter{err: submitErr}
	adapter := &NodeAdapter{submitter: submitter}

	hash, err := adapter.TransactionSubmit(submitTestTxCbor(t))

	require.Error(t, err)
	assert.Empty(t, hash)
	assert.ErrorIs(
		t,
		err,
		ErrTransactionRejected,
		"a mempool that judged the transaction and declined it rejected it",
	)
	assert.NotErrorIs(t, err, ErrMempoolUnavailable)
	assert.ErrorIs(t, err, submitErr)
}

// TestTransactionSubmitAcceptedReturnsHash pins that the added classification
// does not swallow the success path.
func TestTransactionSubmitAcceptedReturnsHash(t *testing.T) {
	submitter := &stubSubmitter{}
	adapter := &NodeAdapter{submitter: submitter}

	hash, err := adapter.TransactionSubmit(submitTestTxCbor(t))

	require.NoError(t, err)
	assert.NotEmpty(t, hash)
	assert.Equal(t, 1, submitter.calls)
}

// TestHandleTransactionSubmitStoppedMempoolReturns503 carries the
// classification through the HTTP layer. A stopped mempool answered
// 400 "Transaction rejected: mempool: stopped", telling the client its
// transaction was at fault for a node-side condition that the nil-submitter
// path already answered 503.
func TestHandleTransactionSubmitStoppedMempoolReturns503(t *testing.T) {
	for name, submitErr := range map[string]error{
		"stopped": mempool.ErrMempoolStopped,
		"nil validator": fmt.Errorf(
			"%w in AddTransaction",
			mempool.ErrNilValidator,
		),
	} {
		t.Run(name, func(t *testing.T) {
			b := newTestBlockfrost(&NodeAdapter{
				submitter: &stubSubmitter{err: submitErr},
			})
			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v0/tx/submit",
				bytes.NewReader(submitTestTxCbor(t)),
			)
			req.Header.Set("Content-Type", "application/cbor")
			w := httptest.NewRecorder()
			b.handleTransactionSubmit(w, req)

			assert.Equal(
				t,
				http.StatusServiceUnavailable,
				w.Code,
				"an unusable mempool is a node condition, not a bad request",
			)
			var resp ErrorResponse
			require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
			assert.Equal(t, "Service Unavailable", resp.Error)
			assert.Equal(t, "mempool unavailable", resp.Message)
		})
	}
}

// TestHandleTransactionSubmitRejectionReturns400 keeps the other side of the
// HTTP split pinned against the same real adapter.
func TestHandleTransactionSubmitRejectionReturns400(t *testing.T) {
	b := newTestBlockfrost(&NodeAdapter{
		submitter: &stubSubmitter{
			err: errors.New("validate transaction: fee too small"),
		},
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		bytes.NewReader(submitTestTxCbor(t)),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(
		t,
		"Transaction rejected: validate transaction: fee too small",
		resp.Message,
	)
}

// TestTransactionSubmitStorageFailureIsNotARejection carries the same split
// one level deeper. Mempool admission runs ledger validation, which resolves
// the transaction's inputs from storage, so a storage fault returns through
// AddTransaction next to genuine verdicts. Reported as a rejection it becomes
// a 400 telling the caller to fix a transaction the node never judged.
func TestTransactionSubmitStorageFailureIsNotARejection(t *testing.T) {
	for name, cause := range map[string]error{
		"blob store unavailable": dbtypes.ErrBlobStoreUnavailable,
		"utxo cbor unavailable":  database.ErrUtxoCborUnavailable,
	} {
		t.Run(name, func(t *testing.T) {
			submitErr := fmt.Errorf(
				"validate transaction: TX abcd failed validation: %w",
				cause,
			)
			submitter := &stubSubmitter{err: submitErr}
			adapter := &NodeAdapter{submitter: submitter}

			hash, err := adapter.TransactionSubmit(submitTestTxCbor(t))

			require.Error(t, err)
			assert.Empty(t, hash)
			assert.ErrorIs(
				t,
				err,
				ErrLedgerUnavailable,
				"storage that cannot answer is a node condition",
			)
			assert.NotErrorIs(
				t,
				err,
				ErrTransactionRejected,
				"the transaction was never judged, so it was not rejected",
			)
			assert.ErrorIs(t, err, cause, "the cause is preserved")
		})
	}
}

// TestTransactionSubmitUnresolvableInputStaysARejection is the control for
// the split above: not every error that reaches the adapter through storage
// is a node fault. A UTxO the ledger does not hold is the caller's input,
// and it stays a rejection.
func TestTransactionSubmitUnresolvableInputStaysARejection(t *testing.T) {
	submitErr := fmt.Errorf(
		"validate transaction: bad inputs: %w",
		database.ErrUtxoNotFound,
	)
	submitter := &stubSubmitter{err: submitErr}
	adapter := &NodeAdapter{submitter: submitter}

	_, err := adapter.TransactionSubmit(submitTestTxCbor(t))

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTransactionRejected)
	assert.NotErrorIs(t, err, ErrLedgerUnavailable)
}

// TestHandleTransactionSubmitStorageFailureReturns503 carries the deeper
// classification through the HTTP layer against the real adapter.
func TestHandleTransactionSubmitStorageFailureReturns503(t *testing.T) {
	b := newTestBlockfrost(&NodeAdapter{
		submitter: &stubSubmitter{
			err: fmt.Errorf(
				"validate transaction: %w",
				dbtypes.ErrBlobStoreUnavailable,
			),
		},
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		bytes.NewReader(submitTestTxCbor(t)),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(
		t,
		http.StatusServiceUnavailable,
		w.Code,
		"storage the node cannot read is not a bad request",
	)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(t, "Service Unavailable", resp.Error)
	assert.Equal(t, "ledger state unavailable", resp.Message)
}

// TestHandleTransactionSubmitReportsWrappedRejectionCause pins that the
// client-facing reason survives a rejection wrapped with further context.
// Recovering the cause by trimming the sentinel off the front of the message
// works only while the sentinel is the outermost prefix; anything wrapped
// around it leaves "transaction rejected" in the message and buries the
// reason behind the wrapper's own text.
func TestHandleTransactionSubmitReportsWrappedRejectionCause(t *testing.T) {
	b := newTestBlockfrost(&mockNode{
		transactionSubmitErr: fmt.Errorf(
			"submit transaction to mempool: %w",
			&TransactionRejectedError{
				Cause: errors.New("fee too small: supplied 100000"),
			},
		),
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		bytes.NewReader(submitTestTxCbor(t)),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	var resp ErrorResponse
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	assert.Equal(
		t,
		"Transaction rejected: fee too small: supplied 100000",
		resp.Message,
	)
}

// recordingHandler collects the level and message of every log record, so a
// test can assert the level a handler chose for an expected condition.
type recordingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *recordingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *recordingHandler) WithGroup(name string) slog.Handler { return h }

// levelFor returns the level the named message was logged at.
func (h *recordingHandler) levelFor(msg string) (slog.Level, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Message == msg {
			return r.Level, true
		}
	}
	return 0, false
}

// TestHandleTransactionSubmitLogsRejectionAtDebug pins the level a rejection
// is logged at. A fee too small or a script that failed is an ordinary
// client-side condition, and handleEpochParams already documents the
// convention: expected client-facing answers log at Debug, because at Error
// level ordinary traffic fills the log with alerts. A caller probing
// submissions would otherwise raise one alert per attempt.
func TestHandleTransactionSubmitLogsRejectionAtDebug(t *testing.T) {
	handler := &recordingHandler{}
	b := New(
		BlockfrostConfig{ListenAddress: ":0"},
		&NodeAdapter{
			submitter: &stubSubmitter{
				err: errors.New("validate transaction: fee too small"),
			},
		},
		slog.New(handler),
	)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		bytes.NewReader(submitTestTxCbor(t)),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	level, ok := handler.levelFor("transaction rejected by the mempool")
	require.True(t, ok, "the rejection reason is still recorded")
	assert.Equal(t, slog.LevelDebug, level)
}

// TestHandleTransactionSubmitLogsStorageFailureAtError is the other side of
// that convention: a node that cannot read its own storage is the unexpected
// path Error level is reserved for.
func TestHandleTransactionSubmitLogsStorageFailureAtError(t *testing.T) {
	handler := &recordingHandler{}
	b := New(
		BlockfrostConfig{ListenAddress: ":0"},
		&NodeAdapter{
			submitter: &stubSubmitter{
				err: fmt.Errorf(
					"validate transaction: %w",
					dbtypes.ErrBlobStoreUnavailable,
				),
			},
		},
		slog.New(handler),
	)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/tx/submit",
		bytes.NewReader(submitTestTxCbor(t)),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionSubmit(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	level, ok := handler.levelFor(
		"transaction submit failed on ledger storage",
	)
	require.True(t, ok)
	assert.Equal(t, slog.LevelError, level)
}

// TestHandleTransactionEvaluateLogsFailureAtDebug applies the same convention
// to the evaluation endpoints, where a failing script is likewise an ordinary
// client-facing answer.
func TestHandleTransactionEvaluateLogsFailureAtDebug(t *testing.T) {
	handler := &recordingHandler{}
	b := New(
		BlockfrostConfig{ListenAddress: ":0"},
		&mockNode{
			transactionEvaluationErr: fmt.Errorf(
				"%w: script failed",
				ErrTransactionEvaluation,
			),
		},
		slog.New(handler),
	)
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v0/utils/txs/evaluate",
		bytes.NewReader(submitTestTxCbor(t)),
	)
	req.Header.Set("Content-Type", "application/cbor")
	w := httptest.NewRecorder()
	b.handleTransactionEvaluate(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	level, ok := handler.levelFor("failed to evaluate transaction")
	require.True(t, ok, "the evaluation failure is still recorded")
	assert.Equal(t, slog.LevelDebug, level)
}
