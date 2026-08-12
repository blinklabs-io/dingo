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
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// bindAttempts bounds how many ports a test tries before giving up,
// matching api/mesh's and api/blockfrost's identical helper.
const bindAttempts = 8

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// startOnFreePort starts a Utxorpc server on a free loopback port, retrying
// on a lost race for the port, and returns it with the address it bound.
// The caller owns shutdown.
func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	tlsCfg apiconfig.EffectiveTLS,
	auth apiconfig.EffectiveAuth,
) (*Utxorpc, string) {
	t.Helper()
	var lastErr error
	for range bindAttempts {
		addr := freePort(t)
		host, port, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		portNum, err := strconv.ParseUint(port, 10, 16)
		require.NoError(t, err)
		u := NewUtxorpc(UtxorpcConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: event.NewEventBus(nil, nil),
			Host:     host,
			Port:     uint(portNum),
			TLS:      tlsCfg,
			Auth:     auth,
		})
		attemptCtx, cancel := context.WithCancel(ctx)
		if err := u.Start(attemptCtx); err != nil {
			cancel()
			lastErr = err
			continue
		}
		t.Cleanup(cancel)
		return u, addr
	}
	t.Fatalf(
		"could not bind a free loopback port in %d attempts: %v",
		bindAttempts, lastErr,
	)
	return nil, ""
}

func stopUtxorpc(t *testing.T, u *Utxorpc) {
	t.Helper()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, u.Stop(stopCtx))
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

// healthCheck issues a Connect-protocol unary health check (works over
// plain HTTP/1.1 or TLS, no HTTP/2 client machinery required) and returns
// the response.
func healthCheck(
	t *testing.T,
	client *http.Client,
	baseURL, authHeader string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		baseURL+"/grpc.health.v1.Health/Check",
		bytes.NewReader([]byte("{}")),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

func TestUtxorpcPlaintextNoAuth(t *testing.T) {
	u, addr := startOnFreePort(
		t, t.Context(), apiconfig.EffectiveTLS{}, apiconfig.EffectiveAuth{},
	)
	t.Cleanup(func() { stopUtxorpc(t, u) })

	resp := healthCheck(t, http.DefaultClient, "http://"+addr, "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestUtxorpcTLSNoAuth(t *testing.T) {
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)
	u, addr := startOnFreePort(
		t, t.Context(),
		apiconfig.EffectiveTLS{
			Enabled: true, CertFilePath: certPath, KeyFilePath: keyPath,
		},
		apiconfig.EffectiveAuth{},
	)
	t.Cleanup(func() { stopUtxorpc(t, u) })

	resp := healthCheck(t, insecureHTTPClient(), "https://"+addr, "")
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// TestUtxorpcTLSAuth covers a TLS-and-token-authenticated listener: a
// missing or wrong credential is rejected with the Connect/gRPC
// Unauthenticated code (surfaced over HTTP as 401 by the Connect
// protocol), and the correct bearer credential is accepted.
func TestUtxorpcTLSAuth(t *testing.T) {
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)
	u, addr := startOnFreePort(
		t, t.Context(),
		apiconfig.EffectiveTLS{
			Enabled: true, CertFilePath: certPath, KeyFilePath: keyPath,
		},
		apiconfig.EffectiveAuth{Enabled: true, Token: "shared-secret"},
	)
	t.Cleanup(func() { stopUtxorpc(t, u) })
	client := insecureHTTPClient()
	baseURL := "https://" + addr

	t.Run("missing credential", func(t *testing.T) {
		resp := healthCheck(t, client, baseURL, "")
		t.Cleanup(func() { _ = resp.Body.Close() })
		// The Connect protocol maps CodeUnauthenticated to HTTP 401.
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("wrong credential", func(t *testing.T) {
		resp := healthCheck(t, client, baseURL, "Bearer wrong")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	})

	t.Run("valid credential", func(t *testing.T) {
		resp := healthCheck(t, client, baseURL, "Bearer shared-secret")
		t.Cleanup(func() { _ = resp.Body.Close() })
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestUtxorpcCORSPreflightBypassesAuth documents and tests the decision
// that an OPTIONS CORS preflight never needs a credential.
func TestUtxorpcCORSPreflightBypassesAuth(t *testing.T) {
	const allowed = "https://wallet.example"
	addr := freePort(t)
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	portNum, err := strconv.ParseUint(port, 10, 16)
	require.NoError(t, err)
	u := NewUtxorpc(UtxorpcConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: event.NewEventBus(nil, nil),
		Host:     host,
		Port:     uint(portNum),
		Auth: apiconfig.EffectiveAuth{
			Enabled: true,
			Token:   "shared-secret",
		},
		CORSAllowedOrigins: []string{allowed},
	})
	require.NoError(t, u.Start(t.Context()))
	t.Cleanup(func() { stopUtxorpc(t, u) })
	baseURL := "http://" + addr

	req, err := http.NewRequestWithContext(
		t.Context(), http.MethodOptions,
		baseURL+"/grpc.health.v1.Health/Check", nil,
	)
	require.NoError(t, err)
	req.Header.Set("Origin", allowed)
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	realResp := healthCheck(t, http.DefaultClient, baseURL, "")
	t.Cleanup(func() { _ = realResp.Body.Close() })
	require.Equal(t, http.StatusUnauthorized, realResp.StatusCode)
}
