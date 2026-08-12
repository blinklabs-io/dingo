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
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// bindAttempts bounds how many ports a test tries before giving up,
// matching api/mesh's identical helper.
const bindAttempts = 8

// freePort reserves a loopback port and releases it. Not guaranteed to
// still be free when the caller binds it -- see bindAttempts.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// startOnFreePort starts a Blockfrost server with cfg (ListenAddress
// overwritten to a free loopback port) and returns it with the address it
// bound, retrying on a lost race for the port. The caller owns shutdown.
func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	cfg BlockfrostConfig,
) (*Blockfrost, string) {
	t.Helper()
	var lastErr error
	for range bindAttempts {
		addr := freePort(t)
		attemptCfg := cfg
		attemptCfg.ListenAddress = addr
		srv := New(attemptCfg, &mockNode{}, nil)
		attemptCtx, cancel := context.WithCancel(ctx)
		if err := srv.Start(attemptCtx); err != nil {
			cancel()
			lastErr = err
			continue
		}
		t.Cleanup(cancel)
		return srv, addr
	}
	t.Fatalf(
		"could not bind a free loopback port in %d attempts: %v",
		bindAttempts, lastErr,
	)
	return nil, ""
}

func startTestServerTLSAuth(
	t *testing.T,
	tlsCfg apiconfig.EffectiveTLS,
	auth apiconfig.EffectiveAuth,
) (*Blockfrost, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	srv, addr := startOnFreePort(
		t, ctx, BlockfrostConfig{TLS: tlsCfg, Auth: auth},
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

func getHealth(
	t *testing.T,
	client *http.Client,
	baseURL string,
	headerName, headerValue string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodGet, baseURL+"/health", nil,
	)
	require.NoError(t, err)
	if headerName != "" {
		req.Header.Set(headerName, headerValue)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func TestBlockfrostPlaintextNoAuth(t *testing.T) {
	_, baseURL := startTestServerTLSAuth(
		t, apiconfig.EffectiveTLS{}, apiconfig.EffectiveAuth{},
	)
	resp := getHealth(t, http.DefaultClient, baseURL, "", "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestBlockfrostTLSNoAuth(t *testing.T) {
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)
	_, baseURL := startTestServerTLSAuth(
		t,
		apiconfig.EffectiveTLS{
			Enabled: true, CertFilePath: certPath, KeyFilePath: keyPath,
		},
		apiconfig.EffectiveAuth{},
	)
	resp := getHealth(t, insecureHTTPClient(), baseURL, "", "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestBlockfrostTLSAuth covers a TLS-and-token-authenticated listener,
// both via the standard Authorization header and via the Blockfrost-
// compatible project_id alias header.
func TestBlockfrostTLSAuth(t *testing.T) {
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
		resp := getHealth(t, client, baseURL, "", "")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("wrong credential", func(t *testing.T) {
		resp := getHealth(t, client, baseURL, "Authorization", "Bearer wrong")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("valid bearer credential", func(t *testing.T) {
		resp := getHealth(
			t, client, baseURL, "Authorization", "Bearer shared-secret",
		)
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("valid project_id alias credential", func(t *testing.T) {
		resp := getHealth(t, client, baseURL, "project_id", "shared-secret")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("wrong project_id alias credential", func(t *testing.T) {
		resp := getHealth(t, client, baseURL, "project_id", "wrong")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})
}

// TestBlockfrostCORSPreflightBypassesAuth documents and tests the
// decision that an OPTIONS CORS preflight never needs a credential.
func TestBlockfrostCORSPreflightBypassesAuth(t *testing.T) {
	const allowed = "https://wallet.example"
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	srv, addr := startOnFreePort(t, ctx, BlockfrostConfig{
		Auth: apiconfig.EffectiveAuth{
			Enabled: true,
			Token:   "shared-secret",
		},
		CORSAllowedOrigins: []string{allowed},
	})
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})
	baseURL := "http://" + addr

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodOptions, baseURL+"/health", nil,
	)
	require.NoError(t, err)
	req.Header.Set("Origin", allowed)
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// A real (non-preflight) request without a credential is still
	// rejected.
	realResp := getHealth(t, http.DefaultClient, baseURL, "", "")
	t.Cleanup(func() { _ = realResp.Body.Close() })
	require.Equal(t, http.StatusUnauthorized, realResp.StatusCode)
}
