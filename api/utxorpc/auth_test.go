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

package utxorpc

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiauth"
	"github.com/stretchr/testify/require"
)

// newAuthTestServer wires the production Connect routing table behind the
// same auth.Middleware(mux) chain Start builds, without going through
// Start's TLS/listener bootstrap -- matching newReflectionTestServer's
// pattern (reflection_test.go).
func newAuthTestServer(
	t *testing.T,
	auth *apiauth.Verifier,
) (*httptest.Server, *http.Client) {
	t.Helper()
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
	})
	srv := httptest.NewUnstartedServer(auth.Middleware(u.newServeMux()))
	srv.Config.Protocols = unencryptedHTTP2Protocols()
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, newConnectH2CClient()
}

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

func newRootRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url+"/", nil) //nolint:noctx
	require.NoError(t, err)
	return req
}

// TestUtxorpcHandlerAuthModeNone covers the default (no in-process
// authentication) behavior: requests reach the mux unauthenticated.
func TestUtxorpcHandlerAuthModeNone(t *testing.T) {
	verifier, err := apiauth.NewVerifier(apiauth.Policy{})
	require.NoError(t, err)
	srv, client := newAuthTestServer(t, verifier)

	resp := doRequest(t, client, newRootRequest(t, srv.URL))
	defer resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestUtxorpcHandlerAuthModeToken covers dingo #2996's fail-closed token
// enforcement at the actual listener level for UTxO RPC's Connect/h2c
// transport, which is served as an ordinary net/http.Handler.
func TestUtxorpcHandlerAuthModeToken(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenPath, []byte("s3cret"), 0o600))
	verifier, err := apiauth.NewVerifier(apiauth.Policy{
		Mode:          apiauth.ModeToken,
		TokenFilePath: tokenPath,
	})
	require.NoError(t, err)
	srv, client := newAuthTestServer(t, verifier)

	// No credential: fails closed with 401, which Connect's own client
	// protocol implementation maps to CodeUnauthenticated for Connect/
	// gRPC-Web callers.
	resp := doRequest(t, client, newRootRequest(t, srv.URL))
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Correct bearer credential: reaches the mux.
	req := newRootRequest(t, srv.URL)
	req.Header.Set("Authorization", "Bearer s3cret")
	resp = doRequest(t, client, req)
	resp.Body.Close()
	require.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)

	// Wrong credential: still fails closed.
	req2 := newRootRequest(t, srv.URL)
	req2.Header.Set("Authorization", "Bearer wrong")
	resp = doRequest(t, client, req2)
	resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
