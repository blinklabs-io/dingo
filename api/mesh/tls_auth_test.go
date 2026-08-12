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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// startTestServerTLSAuth starts a server on a free loopback port with the
// given TLS/Auth policy and returns it with a scheme-appropriate base URL.
func startTestServerTLSAuth(
	t *testing.T,
	tlsCfg apiconfig.EffectiveTLS,
	auth apiconfig.EffectiveAuth,
) (*Server, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	srv, addr := startOnFreePort(
		t, ctx, newTestDeps(),
		func(c *ServerConfig) {
			c.TLS = tlsCfg
			c.Auth = auth
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})
	scheme := "http://"
	if tlsCfg.Enabled {
		scheme = "https://"
	}
	return srv, scheme + addr
}

func insecureHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // test-only, throwaway self-signed server cert
			},
		},
	}
}

func postNetworkList(
	t *testing.T,
	client *http.Client,
	baseURL string,
	authHeader string,
) *http.Response {
	t.Helper()
	body, err := json.Marshal(MetadataRequest{})
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodPost, baseURL+"/network/list",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

// TestServerPlaintextNoAuth is the baseline: no TLS, no auth, matching
// existing pre-dingo#2996 deployments exactly.
func TestServerPlaintextNoAuth(t *testing.T) {
	_, baseURL := startTestServerTLSAuth(
		t, apiconfig.EffectiveTLS{}, apiconfig.EffectiveAuth{},
	)
	resp := postNetworkList(t, http.DefaultClient, baseURL, "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestServerTLSNoAuth covers a TLS-enabled, unauthenticated listener: the
// handshake succeeds and the request is served.
func TestServerTLSNoAuth(t *testing.T) {
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)
	_, baseURL := startTestServerTLSAuth(
		t,
		apiconfig.EffectiveTLS{
			Enabled: true, CertFilePath: certPath, KeyFilePath: keyPath,
		},
		apiconfig.EffectiveAuth{},
	)
	resp := postNetworkList(t, insecureHTTPClient(), baseURL, "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestServerTLSAuth covers a TLS-and-token-authenticated listener: a
// request with no credential and one with the wrong credential are both
// rejected with 401; the correct bearer credential is accepted, over TLS.
func TestServerTLSAuth(t *testing.T) {
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)
	_, baseURL := startTestServerTLSAuth(
		t,
		apiconfig.EffectiveTLS{
			Enabled: true, CertFilePath: certPath, KeyFilePath: keyPath,
		},
		apiconfig.EffectiveAuth{Enabled: true, Token: "shared-secret"},
	)
	client := insecureHTTPClient()

	t.Run("missing credential", func(t *testing.T) {
		resp := postNetworkList(t, client, baseURL, "")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("wrong credential", func(t *testing.T) {
		resp := postNetworkList(t, client, baseURL, "Bearer wrong")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("valid credential", func(t *testing.T) {
		resp := postNetworkList(t, client, baseURL, "Bearer shared-secret")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestServerPlaintextAuth covers auth enabled without TLS -- reverse-proxy
// deployments that terminate TLS upstream must still be able to enable the
// shared token check on the plaintext listener behind them.
func TestServerPlaintextAuth(t *testing.T) {
	_, baseURL := startTestServerTLSAuth(
		t,
		apiconfig.EffectiveTLS{},
		apiconfig.EffectiveAuth{Enabled: true, Token: "shared-secret"},
	)
	resp := postNetworkList(t, http.DefaultClient, baseURL, "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	resp2 := postNetworkList(
		t, http.DefaultClient, baseURL, "Bearer shared-secret",
	)
	t.Cleanup(func() { _ = resp2.Body.Close() })
	require.Equal(t, http.StatusOK, resp2.StatusCode)
}

// TestServerCORSPreflightBypassesAuth documents and tests the decision
// that an OPTIONS CORS preflight never needs a credential -- browsers
// never attach Authorization to a preflight request, so requiring one
// would make every browser client's cross-origin access impossible
// regardless of what credential it later sends with the real request.
// Every non-preflight request, including one with no credential at all,
// still authenticates normally.
func TestServerCORSPreflightBypassesAuth(t *testing.T) {
	const allowed = "https://wallet.example"
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	srv, addr := startOnFreePort(
		t, ctx, newTestDeps(),
		func(c *ServerConfig) {
			c.Auth = apiconfig.EffectiveAuth{
				Enabled: true,
				Token:   "shared-secret",
			}
			c.CORSAllowedOrigins = []string{allowed}
		},
	)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})
	baseURL := "http://" + addr

	t.Run("preflight needs no credential", func(t *testing.T) {
		resp := preflight(t, baseURL, allowed)
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusNoContent, resp.StatusCode)
	})

	t.Run("real request without credential still rejected", func(t *testing.T) {
		resp := postNetworkList(t, http.DefaultClient, baseURL, "")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("real request with credential accepted", func(t *testing.T) {
		resp := postNetworkList(
			t, http.DefaultClient, baseURL, "Bearer shared-secret",
		)
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
