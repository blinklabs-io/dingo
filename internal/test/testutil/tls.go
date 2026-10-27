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

package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	mathrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// BindAttempts bounds how many ports a test tries before giving up when
// racing another process for a loopback port (see FreePort). Shared by
// every built-in API provider's TLS test suite (Blockfrost, Kupo, Mesh,
// UTxO RPC).
const BindAttempts = 8

// FreePort draws a loopback port no other process is using and returns its
// address ("host:port"). The port is not guaranteed to still be free when
// the caller binds it -- retry up to BindAttempts times instead of treating
// one bind failure as a test failure.
//
// The port comes from below the host's ephemeral range rather than from the
// kernel's port-0 assignment. Every process on the host draws ports from the
// ephemeral range for its outgoing connections and port-0 listeners, so a
// port from there can be taken the moment it is released: a test that stops
// a server and then rebinds or probes the same port would fail on another
// package's traffic rather than on its own code.
func FreePort(t *testing.T) string {
	t.Helper()
	high := min(freePortHigh, firstEphemeralPort())
	require.Greater(
		t, high, freePortLow,
		"the host's ephemeral port range leaves no ports below it",
	)
	addr, err := pickFreePort(freePortLow, high)
	require.NoError(t, err)
	return addr
}

// pickFreePort returns a loopback address on a port in [low, high) that no
// listener holds and FreePort has not handed out before in this process.
func pickFreePort(low, high int) (string, error) {
	freePortMu.Lock()
	defer freePortMu.Unlock()
	// A random start spreads concurrent test binaries across the range; the
	// scan from there finds a free port whenever one is left.
	size := high - low
	start := mathrand.IntN(size) //nolint:gosec // not a secret
	for i := range size {
		port := low + (start+i)%size
		if _, issued := freePortIssued[port]; issued {
			continue
		}
		if !portUnheld(port) {
			continue
		}
		freePortIssued[port] = struct{}{}
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
	}
	return "", fmt.Errorf("no free loopback port in [%d, %d)", low, high)
}

// portUnheld reports whether both the loopback and the wildcard address can
// bind port. The wildcard bind is the one that finds another process serving
// the port on every interface: macOS lets a loopback bind succeed alongside
// such a listener, and a dial of the loopback address then reaches it.
func portUnheld(port int) bool {
	for _, host := range []string{"127.0.0.1", ""} {
		ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
		if err != nil {
			return false
		}
		_ = ln.Close()
	}
	return true
}

// FreePort draws from [freePortLow, freePortHigh): below the first ephemeral
// port of every supported host (32768 on Linux by default, 49152 on macOS and
// Windows), and above the registered ports a test host's services listen on.
const (
	freePortLow  = 20000
	freePortHigh = 32768
)

// freePortIssued records every port FreePort has handed out in this process,
// so two parallel tests cannot both be given a port neither has bound yet.
var (
	freePortMu     sync.Mutex
	freePortIssued = make(map[int]struct{})
)

// firstEphemeralPort returns the lowest port Linux assigns to an unbound
// socket, which an administrator can lower below the default. Elsewhere it
// returns freePortHigh, which the defaults already sit above.
func firstEphemeralPort() int {
	raw, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return freePortHigh
	}
	fields := strings.Fields(string(raw))
	if len(fields) == 0 {
		return freePortHigh
	}
	first, err := strconv.Atoi(fields[0])
	if err != nil {
		return freePortHigh
	}
	return first
}

// InsecureHTTPClient returns an *http.Client that skips TLS certificate
// verification, for exercising a listener's throwaway self-signed test
// certificate (see GenerateTestTLSCertKey).
func InsecureHTTPClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, //nolint:gosec // test-only, throwaway self-signed server cert
			},
		},
	}
}

// GenerateTestTLSCertKey generates a throwaway self-signed certificate/key
// pair valid for 127.0.0.1 and writes them as PEM files under a fresh
// t.TempDir(), returning their paths. Used to exercise a listener's TLS
// startup path (Blockfrost, Kupo, Mesh, UTxORPC, Bark) without depending on any
// fixed certificate checked into the repo.
func GenerateTestTLSCertKey(t *testing.T) (certPath, keyPath string) {
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
