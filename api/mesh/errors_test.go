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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestErrorCodesAreStable pins the wire values of every Mesh error.
// Clients branch on these codes, so a change here is a breaking API
// change and must be deliberate.
func TestErrorCodesAreStable(t *testing.T) {
	want := map[int32]struct {
		message   string
		retriable bool
	}{
		1:  {"network not supported", false},
		2:  {"block not found", false},
		3:  {"transaction not found", false},
		4:  {"account not found", false},
		5:  {"invalid request", false},
		6:  {"internal error", true},
		7:  {"not implemented", false},
		8:  {"invalid public key", false},
		9:  {"invalid transaction", false},
		10: {"transaction submit failed", true},
		11: {"service unavailable", true},
	}

	all := AllErrors()
	require.Len(t, all, len(want))
	seen := make(map[int32]struct{}, len(all))
	for _, err := range all {
		spec, ok := want[err.Code]
		require.True(t, ok, "unexpected error code %d", err.Code)
		require.Equal(t, spec.message, err.Message)
		require.Equal(t, spec.retriable, err.Retriable)
		require.NotEmpty(t, err.Description)
		_, dup := seen[err.Code]
		require.False(t, dup, "duplicate error code %d", err.Code)
		seen[err.Code] = struct{}{}
	}
}

// TestWriteErrorStatusMapping pins the HTTP status each Mesh error maps
// to. Rosetta clients and proxies key retry behavior off the status as
// well as the code.
func TestWriteErrorStatusMapping(t *testing.T) {
	want := map[*Error]int{
		ErrNetworkNotSupported: http.StatusNotFound,
		ErrBlockNotFound:       http.StatusNotFound,
		ErrTransactionNotFound: http.StatusNotFound,
		ErrAccountNotFound:     http.StatusNotFound,
		ErrInvalidRequest:      http.StatusBadRequest,
		ErrInvalidPublicKey:    http.StatusBadRequest,
		ErrInvalidTransaction:  http.StatusBadRequest,
		ErrSubmitFailed:        http.StatusBadRequest,
		ErrNotImplemented:      http.StatusNotImplemented,
		ErrUnavailable:         http.StatusServiceUnavailable,
		ErrInternal:            http.StatusInternalServerError,
	}
	// Every defined error must have a pinned status.
	require.Len(t, want, len(AllErrors()))

	for meshErr, status := range want {
		rec := httptest.NewRecorder()

		writeError(rec, meshErr)

		require.Equal(
			t, status, rec.Code,
			"error code %d", meshErr.Code,
		)
		require.Equal(
			t,
			"application/json",
			rec.Header().Get("Content-Type"),
		)
		var got Error
		require.NoError(
			t, json.Unmarshal(rec.Body.Bytes(), &got),
		)
		require.Equal(t, meshErr.Code, got.Code)
	}
}

// TestWriteErrorUnknownCodeIsInternal covers an error outside the
// defined set, which must fail closed as a server error rather than
// being reported as success.
func TestWriteErrorUnknownCodeIsInternal(t *testing.T) {
	rec := httptest.NewRecorder()

	writeError(rec, &Error{Code: 999, Message: "unknown"})

	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestWrapErr(t *testing.T) {
	t.Run("nil detail returns the base error", func(t *testing.T) {
		require.Same(
			t, ErrInternal, wrapErr(ErrInternal, nil),
		)
	})

	t.Run("detail is attached", func(t *testing.T) {
		wrapped := wrapErr(
			ErrInternal, errors.New("disk on fire"),
		)

		require.Equal(t, ErrInternal.Code, wrapped.Code)
		require.Equal(t, ErrInternal.Message, wrapped.Message)
		require.Equal(
			t, ErrInternal.Description, wrapped.Description,
		)
		require.Equal(
			t, ErrInternal.Retriable, wrapped.Retriable,
		)
		require.Equal(t, "disk on fire", wrapped.Details["error"])
	})

	t.Run("base error is not mutated", func(t *testing.T) {
		wrapErr(ErrBlockNotFound, errors.New("detail"))

		require.Nil(t, ErrBlockNotFound.Details)
	})
}

// TestErrorJSONShape pins the serialized field names, which are part of
// the Rosetta response schema.
func TestErrorJSONShape(t *testing.T) {
	raw, err := json.Marshal(
		wrapErr(ErrInvalidRequest, errors.New("bad")),
	)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, float64(5), decoded["code"])
	require.Equal(t, "invalid request", decoded["message"])
	require.Equal(t, false, decoded["retriable"])
	require.NotEmpty(t, decoded["description"])
	require.Equal(
		t,
		map[string]any{"error": "bad"},
		decoded["details"],
	)

	// Optional fields are omitted when unset, while retriable is
	// always present so clients never have to infer it.
	raw, err = json.Marshal(&Error{Code: 1, Message: "m"})
	require.NoError(t, err)
	minimal := map[string]any{}
	require.NoError(t, json.Unmarshal(raw, &minimal))
	require.NotContains(t, minimal, "description")
	require.NotContains(t, minimal, "details")
	require.Contains(t, minimal, "retriable")
}
