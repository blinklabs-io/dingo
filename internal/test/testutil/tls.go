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
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// GenerateTestTLSCertKey generates a throwaway self-signed certificate/key
// pair valid for 127.0.0.1 and writes them as PEM files under a fresh
// t.TempDir(), returning their paths. Used to exercise a listener's TLS
// startup path (Blockfrost, Mesh, UTxORPC, Bark) without depending on any
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
