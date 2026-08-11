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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeTestTLSCertKey generates a throwaway self-signed cert/key pair valid
// for 127.0.0.1 and writes them as PEM files under a fresh t.TempDir(),
// returning their paths. Mirrors bark/tls_test.go's helper of the same
// name/shape; kept local rather than shared since it's a generic test
// fixture, not a domain mock.
func writeTestTLSCertKey(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(
		rand.Reader, template, template, &priv.PublicKey, priv,
	)
	require.NoError(t, err)

	dir := t.TempDir()
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")

	certOut, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}),
	)
	require.NoError(t, certOut.Close())

	keyBytes, err := x509.MarshalECPrivateKey(priv)
	require.NoError(t, err)
	keyOut, err := os.OpenFile(
		keyPath,
		os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		0o600,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}),
	)
	require.NoError(t, keyOut.Close())

	return certPath, keyPath
}

// reserveFreePort binds an ephemeral TCP port, closes the listener
// immediately, and returns the port number, for tests that need a
// concrete port number up front (Start's ListenAddress is a static
// string, so ":0" alone can't be resolved back to the OS-assigned port).
func reserveFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	return port
}

// TestStartTLSHandshake covers dingo #2996/#2998's built-in TLS listener
// support for Blockfrost (previously only UTxO RPC had it): with
// TLSCertFilePath/TLSKeyFilePath configured, Start must serve HTTPS, and a
// plaintext HTTP client must fail against it.
func TestStartTLSHandshake(t *testing.T) {
	certPath, keyPath := writeTestTLSCertKey(t)
	port := reserveFreePort(t)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	b := New(BlockfrostConfig{
		ListenAddress:   addr,
		TLSCertFilePath: certPath,
		TLSKeyFilePath:  keyPath,
	}, &mockNode{}, nil)

	require.NoError(t, b.Start(t.Context()))
	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()
		_ = b.Stop(ctx)
	}()

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			}, //nolint:gosec // test-only, self-signed cert
		},
		Timeout: 5 * time.Second,
	}
	req, err := http.NewRequest( //nolint:noctx
		http.MethodGet,
		"https://"+addr+"/health",
		nil,
	)
	require.NoError(t, err)
	resp := doRequest(t, client, req)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// A plaintext client must not get a successful response from a TLS
	// listener. Go's net/http server detects a non-TLS ClientHello and
	// replies with a plaintext "Client sent an HTTP request to an HTTPS
	// server" 400 over the same connection rather than failing the
	// transport outright, so the assertion is on the status code, not a
	// transport-level error.
	plainClient := &http.Client{Timeout: 2 * time.Second}
	plainReq, err := http.NewRequest( //nolint:noctx
		http.MethodGet,
		"http://"+addr+"/health",
		nil,
	)
	require.NoError(t, err)
	plainResp := doRequest(t, plainClient, plainReq)
	defer plainResp.Body.Close()
	require.NotEqual(t, http.StatusOK, plainResp.StatusCode)
}
