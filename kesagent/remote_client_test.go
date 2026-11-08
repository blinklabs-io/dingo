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

package kesagent

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/kes"
	"github.com/stretchr/testify/require"
)

// testCert is a throwaway Ed25519 certificate and key.
type testCert struct {
	tls  tls.Certificate
	x509 *x509.Certificate
	spki string
}

func newTestCert(t *testing.T, name string) testCert {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
			x509.ExtKeyUsageClientAuth,
		},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	require.NoError(t, err)
	parsed, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return testCert{
		tls:  tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv},
		x509: parsed,
		spki: SPKIFingerprint(parsed.RawSubjectPublicKeyInfo),
	}
}

// writePEM writes c's certificate and key to files and returns their paths.
func (c testCert) writePEM(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	dir := t.TempDir()
	certFile = filepath.Join(dir, "client.crt")
	keyFile = filepath.Join(dir, "client.key")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: c.tls.Certificate[0],
	}), 0o600))
	keyDER, err := x509.MarshalPKCS8PrivateKey(c.tls.PrivateKey)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{
		Type: "PRIVATE KEY", Bytes: keyDER,
	}), 0o600))
	return certFile, keyFile
}

// fakeRemoteAgent is a bursa attested sign listener with the mtls provider:
// TLS 1.3 requiring the client certificate, the Hello, the admission
// exchange, then sign requests answered with real KES signatures.
type fakeRemoteAgent struct {
	addr     string
	server   testCert
	accepts  atomic.Int32
	signs    atomic.Int32
	admitFix func(*attestationResponse)
	// dropAfterRequest closes the connection after reading a sign request
	// without answering it.
	dropAfterRequest atomic.Bool
}

func startFakeRemoteAgent(
	t *testing.T,
	client testCert,
	skeyData, vkey, opCert []byte,
) *fakeRemoteAgent {
	t.Helper()
	a := &fakeRemoteAgent{server: newTestCert(t, "agent")}
	pool := x509.NewCertPool()
	pool.AddCert(client.x509)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{a.server.tls},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	a.addr = ln.Addr().String()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			a.accepts.Add(1)
			go a.serve(conn, skeyData, vkey, opCert)
		}
	}()
	return a
}

func (a *fakeRemoteAgent) serve(conn net.Conn, skeyData, vkey, opCert []byte) {
	defer conn.Close()
	if err := writeFrame(conn, MaxSignFrameLen, Hello{
		Protocol: AttestedProtocolID, Mode: ModeSign,
	}); err != nil {
		return
	}
	var req attestationRequest
	if err := readFrame(conn, MaxSignFrameLen, &req); err != nil {
		return
	}
	resp := attestationResponse{
		Type:     "attestation_response",
		Protocol: AttestedProtocolID,
		Nonce:    req.Nonce,
		Provider: ProviderMTLS,
		KESVKey:  vkey,
		OpCert:   opCert,
		Period:   0,
	}
	if a.admitFix != nil {
		a.admitFix(&resp)
	}
	if err := writeFrame(conn, maxAttestationFrameLen, resp); err != nil {
		return
	}
	for {
		var sreq SignRequest
		if err := readFrame(conn, MaxSignFrameLen, &sreq); err != nil {
			return
		}
		if a.dropAfterRequest.Load() {
			return
		}
		sk := &kes.SecretKey{
			Depth:  kes.CardanoKesDepth,
			Period: sreq.Period,
			Data:   append([]byte(nil), skeyData...),
		}
		sig, err := kes.Sign(sk, sreq.Period, sreq.Message)
		if err != nil {
			return
		}
		a.signs.Add(1)
		if err := writeFrame(conn, MaxSignFrameLen, SignResponse{
			Type: "sign_response", Period: sreq.Period, Signature: sig,
		}); err != nil {
			return
		}
	}
}

func newTestRemoteClient(
	t *testing.T,
	a *fakeRemoteAgent,
	client testCert,
	vkey, opCert []byte,
	pin string,
) *RemoteClient {
	t.Helper()
	certFile, keyFile := client.writePEM(t)
	c, err := NewRemoteClient(RemoteConfig{
		Address:          a.addr,
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: pin,
		KESVKey:          vkey,
		OpCert:           opCert,
		SignTimeout:      2 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestRemoteClientPrepareThenSignReusesSession(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)

	require.NoError(t, c.Prepare())
	for _, msg := range []string{"header-1", "header-2"} {
		sig, err := c.Sign(0, []byte(msg))
		require.NoError(t, err)
		require.True(t, kes.VerifySignedKES(vkey, 0, []byte(msg), sig))
	}
	require.Equal(t, int32(1), a.accepts.Load(),
		"Prepare's session must carry both signs")
}

func TestRemoteClientRefusesWrongOrMissingPin(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)

	other := newTestCert(t, "impostor")
	c := newTestRemoteClient(t, a, client, vkey, opCert, other.spki)
	err := c.Prepare()
	require.ErrorIs(t, err, ErrServerPinMismatch)
	_, err = c.Sign(0, []byte("header"))
	require.Error(t, err)
	require.Zero(t, a.signs.Load())

	certFile, keyFile := client.writePEM(t)
	_, err = NewRemoteClient(RemoteConfig{
		Address:        a.addr,
		ClientCertFile: certFile,
		ClientKeyFile:  keyFile,
		KESVKey:        vkey,
		OpCert:         opCert,
	})
	require.ErrorContains(t, err, "server SPKI SHA-256 pin")
}

func TestRemoteClientAdmissionChecks(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	for _, tc := range []struct {
		name string
		fix  func(*attestationResponse)
	}{
		{"wrong nonce", func(r *attestationResponse) { r.Nonce = make([]byte, 32) }},
		{"other provider", func(r *attestationResponse) { r.Provider = "nitro" }},
		{"evidence present", func(r *attestationResponse) { r.Attestation = []byte{1} }},
		{"other opcert", func(r *attestationResponse) {
			r.OpCert = append(append([]byte(nil), opCert...), 0)
		}},
		{"other KES key", func(r *attestationResponse) { r.KESVKey = make([]byte, 32) }},
		{"agent error", func(r *attestationResponse) { r.Error = "attestation failed" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := newTestCert(t, "node")
			a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
			a.admitFix = tc.fix
			c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)
			require.ErrorIs(t, c.Prepare(), ErrRemoteAdmission)
			require.Zero(t, a.signs.Load())
		})
	}
}

func TestRemoteClientDoesNotResendAfterADroppedRequest(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	c := newTestRemoteClient(t, a, client, vkey, opCert, a.server.spki)

	require.NoError(t, c.Prepare())
	a.dropAfterRequest.Store(true)
	_, err := c.Sign(0, []byte("header"))
	require.Error(t, err)
	require.Equal(t, int32(1), a.accepts.Load(),
		"a request that may have reached the agent must not be resent")

	// The next call opens a new session after the reconnect backoff.
	a.dropAfterRequest.Store(false)
	require.Eventually(t, func() bool {
		_, err := c.Sign(0, []byte("next-header"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(2), a.accepts.Load())
}

func TestRemoteClientRedialsAStaleSession(t *testing.T) {
	t.Parallel()
	skeyData, vkey, opCert := testKESMaterial(t)
	client := newTestCert(t, "node")
	a := startFakeRemoteAgent(t, client, skeyData, vkey, opCert)
	certFile, keyFile := client.writePEM(t)
	c, err := NewRemoteClient(RemoteConfig{
		Address:          a.addr,
		ClientCertFile:   certFile,
		ClientKeyFile:    keyFile,
		ServerSPKISHA256: a.server.spki,
		KESVKey:          vkey,
		OpCert:           opCert,
		SignTimeout:      2 * time.Second,
		SessionFreshFor:  time.Nanosecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	require.NoError(t, c.Prepare())
	_, err = c.Sign(0, []byte("header"))
	require.NoError(t, err)
	require.Equal(t, int32(2), a.accepts.Load(),
		"a session past SessionFreshFor must be replaced before signing")
}
