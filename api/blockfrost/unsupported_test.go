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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRouterUnsupportedOperationsReturnExplicitError(t *testing.T) {
	t.Parallel()
	handler := newTestBlockfrost(&mockNode{}).handler()
	// Requests enumerate the unimplemented operations from the pinned upstream
	// specification independently of the production route table.
	requests := []struct{ method, path string }{
		{"GET", "/api/v0/"},
		{"GET", "/health/clock"},
		{"GET", "/api/v0/blocks/latest/txs/cbor"},
		{"GET", "/api/v0/blocks/testvalue/next"},
		{"GET", "/api/v0/blocks/testvalue/previous"},
		{"GET", "/api/v0/blocks/slot/testvalue"},
		{"GET", "/api/v0/blocks/epoch/testvalue/slot/testvalue"},
		{"GET", "/api/v0/blocks/testvalue/txs"},
		{"GET", "/api/v0/blocks/testvalue/txs/cbor"},
		{"GET", "/api/v0/blocks/testvalue/addresses"},
		{"GET", "/api/v0/governance/committee"},
		{"GET", "/api/v0/governance/committee/votes"},
		{"GET", "/api/v0/governance/committee/testvalue/votes"},
		{"GET", "/api/v0/governance/dreps/testvalue/delegators"},
		{"GET", "/api/v0/governance/dreps/testvalue/metadata"},
		{"GET", "/api/v0/governance/dreps/testvalue/updates"},
		{"GET", "/api/v0/governance/dreps/testvalue/votes"},
		{"GET", "/api/v0/governance/proposals"},
		{"GET", "/api/v0/governance/proposals/testvalue/testvalue"},
		{"GET", "/api/v0/governance/proposals/testvalue/testvalue/parameters"},
		{"GET", "/api/v0/governance/proposals/testvalue/testvalue/withdrawals"},
		{"GET", "/api/v0/governance/proposals/testvalue/testvalue/votes"},
		{"GET", "/api/v0/governance/proposals/testvalue/testvalue/metadata"},
		{"GET", "/api/v0/governance/proposals/testvalue"},
		{"GET", "/api/v0/governance/proposals/testvalue/parameters"},
		{"GET", "/api/v0/governance/proposals/testvalue/withdrawals"},
		{"GET", "/api/v0/governance/proposals/testvalue/votes"},
		{"GET", "/api/v0/governance/proposals/testvalue/metadata"},
		{"GET", "/api/v0/epochs/testvalue"},
		{"GET", "/api/v0/epochs/testvalue/next"},
		{"GET", "/api/v0/epochs/testvalue/previous"},
		{"GET", "/api/v0/epochs/testvalue/stakes"},
		{"GET", "/api/v0/epochs/testvalue/stakes/testvalue"},
		{"GET", "/api/v0/epochs/testvalue/blocks"},
		{"GET", "/api/v0/epochs/testvalue/blocks/testvalue"},
		{"GET", "/api/v0/accounts/testvalue/history"},
		{"GET", "/api/v0/accounts/testvalue/mirs"},
		{"GET", "/api/v0/accounts/testvalue/addresses/assets"},
		{"GET", "/api/v0/accounts/testvalue/addresses/total"},
		{"GET", "/api/v0/mempool"},
		{"GET", "/api/v0/mempool/testvalue"},
		{"GET", "/api/v0/mempool/addresses/testvalue"},
		{"GET", "/api/v0/metadata/txs/labels"},
		{"GET", "/api/v0/addresses/testvalue/extended"},
		{"GET", "/api/v0/addresses/testvalue/total"},
		{"GET", "/api/v0/addresses/testvalue/utxos/testvalue"},
		{"GET", "/api/v0/addresses/testvalue/txs"},
		{"GET", "/api/v0/pools/retired"},
		{"GET", "/api/v0/pools/testvalue/history"},
		{"GET", "/api/v0/pools/testvalue/relays"},
		{"GET", "/api/v0/pools/testvalue/delegators"},
		{"GET", "/api/v0/pools/testvalue/blocks"},
		{"GET", "/api/v0/pools/testvalue/updates"},
		{"GET", "/api/v0/pools/testvalue/votes"},
		{"GET", "/api/v0/assets"},
		{"GET", "/api/v0/assets/testvalue/history"},
		{"GET", "/api/v0/assets/testvalue/txs"},
		{"GET", "/api/v0/assets/testvalue/transactions"},
		{"GET", "/api/v0/assets/testvalue/utxos"},
		{"GET", "/api/v0/assets/policy/testvalue"},
		{"GET", "/api/v0/scripts"},
		{"GET", "/api/v0/scripts/testvalue"},
		{"GET", "/api/v0/scripts/testvalue/json"},
		{"GET", "/api/v0/scripts/testvalue/cbor"},
		{"GET", "/api/v0/scripts/testvalue/redeemers"},
		{"GET", "/api/v0/scripts/testvalue/utxos"},
		{"GET", "/api/v0/scripts/datum/testvalue"},
		{"GET", "/api/v0/scripts/datum/testvalue/cbor"},
		{"GET", "/api/v0/utils/addresses/xpub/testvalue/testvalue/testvalue"},
		{"POST", "/api/v0/ipfs/add"},
		{"GET", "/api/v0/ipfs/gateway/testvalue"},
		{"POST", "/api/v0/ipfs/pin/add/testvalue"},
		{"GET", "/api/v0/ipfs/pin/list"},
		{"GET", "/api/v0/ipfs/pin/list/testvalue"},
		{"POST", "/api/v0/ipfs/pin/remove/testvalue"},
		{"GET", "/api/v0/metrics"},
		{"GET", "/api/v0/metrics/endpoints"},
		{"GET", "/api/v0/nutlink/testvalue"},
		{"GET", "/api/v0/nutlink/testvalue/tickers"},
		{"GET", "/api/v0/nutlink/testvalue/tickers/testvalue"},
		{"GET", "/api/v0/nutlink/tickers/testvalue"},
	}
	for _, request := range requests {
		t.Run(request.method+" "+request.path, func(t *testing.T) {
			t.Parallel()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(request.method, request.path, nil))
			require.Equal(t, http.StatusNotImplemented, recorder.Code)
			require.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
			require.JSONEq(t, `{"status_code":501,"error":"Not Implemented","message":"The requested endpoint is not implemented."}`, recorder.Body.String())
		})
	}
}

func TestRouterUnsupportedMethodAndUnknownPath(t *testing.T) {
	t.Parallel()
	handler := newTestBlockfrost(&mockNode{}).handler()
	for _, tc := range []struct {
		method, path, allow string
		status              int
	}{
		{http.MethodPost, "/api/v0/pools/retired", "GET, HEAD", http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/v0/scripts/hash/cbor", "GET, HEAD", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v0/ipfs/pin/add/hash", "POST", http.StatusMethodNotAllowed},
		{http.MethodHead, "/api/v0/scripts/hash", "", http.StatusNotImplemented},
		{http.MethodGet, "/api/v0", "", http.StatusNotFound},
		{http.MethodGet, "/api/v0/scripts/hash/unknown", "", http.StatusNotFound},
		{http.MethodPost, "/api/v0/ipfs/pin/add/hash/extra", "", http.StatusNotImplemented},
		{http.MethodGet, "/api/v0/ipfs/gateway/hash/dir/file.png", "", http.StatusNotImplemented},
		{http.MethodGet, "/api/v0/ipfs/pin/list/hash/dir", "", http.StatusNotImplemented},
		{http.MethodPost, "/api/v0/ipfs/pin/remove/hash/dir", "", http.StatusNotImplemented},
		{http.MethodGet, "/api/v0/ipfs/pin/add/hash/dir", "POST", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v0/ipfs/gateway/", "", http.StatusNotFound},
		{http.MethodPost, "/api/v0/ipfs/add/extra", "", http.StatusNotFound},
		{http.MethodGet, "/api/v0/scripts/", "", http.StatusNotFound},
		{http.MethodGet, "/api/v0/%73cripts/datum/hash", "", http.StatusNotImplemented},
		{http.MethodGet, "/api/v0/ipfs/gateway/hash%2Ffile", "", http.StatusNotImplemented},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			t.Parallel()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(tc.method, tc.path, nil))
			require.Equal(t, tc.status, recorder.Code)
			require.Equal(t, tc.allow, recorder.Header().Get("Allow"))
			var response ErrorResponse
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
			require.Equal(t, tc.status, response.StatusCode)
		})
	}
}

func TestRouterImplementedOperationsKeepPrecedence(t *testing.T) {
	t.Parallel()
	handler := newTestBlockfrost(&mockNode{}).handler()
	for _, path := range []string{
		"/", "/health", "/api/v0/blocks/latest", "/api/v0/blocks/latest/txs",
		"/api/v0/epochs/latest", "/api/v0/epochs/latest/parameters",
		"/api/v0/pools/extended", "/api/v0/pools/retiring",
		"/api/v0/metadata/txs/labels/721", "/api/v0/governance/dreps",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, http.StatusOK, recorder.Code)
		})
	}
}

func TestRouterUnsupportedHEADResponsesHaveNoBody(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(newTestBlockfrost(&mockNode{}).handler())
	t.Cleanup(server.Close)
	for _, path := range []string{"/api/v0/scripts", "/api/v0/scripts/hash"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			response, err := server.Client().Head(server.URL + path)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusNotImplemented, response.StatusCode)
			require.Equal(t, "application/json", response.Header.Get("Content-Type"))
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Empty(t, body)
		})
	}
}
