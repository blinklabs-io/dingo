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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/stretchr/testify/require"
)

// doRequest performs req and asserts it completed without a transport
// error, returning a guaranteed non-nil response. Centralizing the nil
// guard here (rather than relying on require.NoError alone before
// dereferencing resp at each call site) keeps every call site provably
// safe.
func doRequest(
	t *testing.T,
	client *http.Client,
	req *http.Request,
) *http.Response {
	t.Helper()
	resp, err := client.Do(req)
	require.NoError(t, err)
	if resp == nil {
		t.Fatal("http.Client.Do returned a nil response with a nil error")
	}
	return resp
}

// TestHandlerAuthModeNone covers the default (no in-process
// authentication) behavior: requests reach the mux unauthenticated.
func TestHandlerAuthModeNone(t *testing.T) {
	verifier, err := apiauth.NewVerifier(apiauth.Policy{})
	require.NoError(t, err)
	b := newTestBlockfrost(&mockNode{})
	srv := httptest.NewServer(b.handler(verifier))
	defer srv.Close()

	req, err := http.NewRequest( //nolint:noctx
		http.MethodGet,
		srv.URL+"/health",
		nil,
	)
	require.NoError(t, err)
	resp := doRequest(t, http.DefaultClient, req)
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestHandlerAuthModeToken covers dingo #2996's fail-closed token
// enforcement at the actual listener level (real TCP connection through
// net/http, not a direct handler call), including the Blockfrost-specific
// project_id header alias.
func TestHandlerAuthModeToken(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("s3cret"), 0o600))
	verifier, err := apiauth.NewVerifier(apiauth.Policy{
		Mode:          apiauth.ModeToken,
		TokenFilePath: tokenPath,
	})
	require.NoError(t, err)
	b := newTestBlockfrost(&mockNode{})
	srv := httptest.NewServer(b.handler(verifier))
	defer srv.Close()

	newHealthRequest := func(headers map[string]string) *http.Request {
		req, reqErr := http.NewRequest( //nolint:noctx
			http.MethodGet,
			srv.URL+"/health",
			nil,
		)
		require.NoError(t, reqErr)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		return req
	}

	// No credential: fails closed with 401.
	resp := doRequest(t, http.DefaultClient, newHealthRequest(nil))
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.NotEmpty(t, resp.Header.Get("WWW-Authenticate"))

	// Correct bearer credential: reaches the mux.
	resp = doRequest(t, http.DefaultClient, newHealthRequest(map[string]string{
		"Authorization": "Bearer s3cret",
	}))
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Blockfrost-compatible project_id header alias for the same token.
	resp = doRequest(t, http.DefaultClient, newHealthRequest(map[string]string{
		"project_id": "s3cret",
	}))
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Wrong credential: still fails closed.
	resp = doRequest(t, http.DefaultClient, newHealthRequest(map[string]string{
		"Authorization": "Bearer wrong",
	}))
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
