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

package apiready_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/apiready"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func serve(
	t *testing.T,
	check apiready.Check,
	classify apiready.Classifier,
) *httptest.ResponseRecorder {
	t.Helper()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	h := apiready.Middleware(
		check, classify,
		func(w http.ResponseWriter, status int, _ string) {
			w.WriteHeader(status)
		},
		next,
	)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	return rec
}

func TestMiddlewareGatesByKind(t *testing.T) {
	t.Parallel()
	gated := func(kind indexerdb.Kind) apiready.Classifier {
		return func(*http.Request) (indexerdb.Kind, bool) { return kind, true }
	}
	ungated := func(*http.Request) (indexerdb.Kind, bool) { return 0, false }
	onlyHistory := func(kind indexerdb.Kind) (bool, error) {
		return kind == indexerdb.KindHistory, nil
	}

	rec := serve(t, onlyHistory, gated(indexerdb.KindCurrent))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Equal(t, apiready.RetryAfterSeconds, rec.Header().Get("Retry-After"))

	assert.Equal(t, http.StatusOK,
		serve(t, onlyHistory, gated(indexerdb.KindHistory)).Code)
	assert.Equal(t, http.StatusOK, serve(t, onlyHistory, ungated).Code)
}

func TestMiddlewareRefusesOnCheckError(t *testing.T) {
	t.Parallel()
	failing := func(indexerdb.Kind) (bool, error) {
		return false, errors.New("tip unreadable")
	}
	rec := serve(t, failing, func(*http.Request) (indexerdb.Kind, bool) {
		return indexerdb.KindHistory, true
	})
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestMiddlewareNilCheckServesEverything(t *testing.T) {
	t.Parallel()
	rec := serve(t, nil, func(*http.Request) (indexerdb.Kind, bool) {
		return indexerdb.KindCurrent, true
	})
	require.Equal(t, http.StatusOK, rec.Code)
}
